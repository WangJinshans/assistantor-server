package network

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"github.com/BurnishCN/gateway-go/common"
	"github.com/BurnishCN/gateway-go/global"
	"github.com/BurnishCN/gateway-go/util"
	"github.com/go-redis/redis"
	"net"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

var (
	mapMutex = sync.RWMutex{}
	hostName = ""
)

// Connection holds info about connection
type Connection struct {
	conn           *net.TCPConn
	Server         Server
	ResidueBytes   []byte
	id             string
	peerAddress    string
	MessageChan    chan []byte
	ExitChan       chan struct{}
	IsFirstMessage bool // 是否是第一次发送消息
}

// ServerConfig involve server's configurations
type ServerConfig struct {
	Address       string
	Timeout       int
	MaxQps        int64
	MaxConnection int
}

// 处理离线删除key后又更新key的问题
type WarpedConnectionInfo struct {
	Data map[string]interface{}
	Conn *Connection
}

// NaiveServer represent a server
type NaiveServer struct {
	Config *ServerConfig

	funcOnConnectionMade   func(c *Connection, vin string)
	funcOnConnectionClosed func(c *Connection, err error)
	funcOnMessageReceived  func(c *Connection, message []byte)

	started           bool
	ConnectionChan    chan *Connection // 缓存VIN -- 登录时间 -- TTL
	RequestCount      int64            // 访问数
	QpsChan           chan struct{}    // qps数据通道
	QpsStartTimeStamp int64            // 开始采集qps指标的时间
	MaxQps            int64            // 配置最大qps,超出报警

	MaxConnection int // 最大连接数 超过拒绝连接

	idMap map[string]*Connection
}

func NewNativeServer(config *ServerConfig) (server *NaiveServer) {
	server = &NaiveServer{
		Config:         config,
		idMap:          make(map[string]*Connection),
		ConnectionChan: make(chan *Connection, 2000),
		QpsChan:        make(chan struct{}, 1000),
		MaxQps:         config.MaxQps,
		MaxConnection:  config.MaxConnection,
	}
	return
}

func init() {
	hostName = util.GetHost()
}

func (c *Connection) String() string {
	return c.peerAddress
}

// SetID set id for connection
func (c *Connection) SetID(id string) {
	c.id = id
	c.Server.addConn(id, c)
}

// GetID return a connection's id
func (c *Connection) GetID() string {
	return c.id
}

func (c *Connection) listen() {
	reader := bufio.NewReader(c.conn)
	buffer := make([]byte, 4096, 4096)
	var read int
	var err error
	var buffers bytes.Buffer
	for {
		err = c.conn.SetDeadline(time.Now().Add(c.Server.GetTimeout()))
		if err != nil {
			log.Error().Msg(err.Error())
			return
		}
		read, err = reader.Read(buffer)
		if err != nil {
			// We should defer the close routine of connection, since it's blocked
			// and the blocking time can be as long as two hours in cause of
			// CLOSE_WAIT status is encountered
			// Btw, don't modify kernal parameters to try to shorten the time
			// spent in the CLOSE_WAIT state
			log.Error().Msgf("close connection: %s", c.id)
			c.ExitChan <- struct{}{}
			c.Server.OnConnectionClosed(c, err)
			c.conn.Close()
			c.Server.removeConn(c.id)
			c.conn = nil
			return
		}
		// go transfer byte slice argument as point, so we copy
		// bytes here to prevent race conditions
		buffers.Write(buffer[:read])
		// It turns out that all the goroutines with a CLOSE_WAIT connection will
		// hang on here
		// c.Server.onMessageReceived(c, segment)
		c.MessageChan <- buffers.Bytes()
		buffers.Reset()
	}
}

func (c *Connection) dispatchMessage() {
	for {
		select {
		case <-c.ExitChan:
			log.Info().Msgf("stop working, connection:%s", c.id)
			return
		case message := <-c.MessageChan:
			c.Server.OnMessageReceived(c, message)
		}
	}
}

// Send send massage to peer
func (c *Connection) Send(message []byte) error {
	_, err := c.conn.Write(message)
	return err
}

// RemoteAddr return peer's address
func (c *Connection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// RegisteCallbacks registe three callbacks
func (s *NaiveServer) RegisterCallbacks(onConnectionMade func(c *Connection, vin string), onConnectionClosed func(c *Connection, err error), onMessageReceived func(c *Connection, message []byte)) {
	s.funcOnConnectionMade = onConnectionMade
	s.funcOnConnectionClosed = onConnectionClosed
	s.funcOnMessageReceived = onMessageReceived
}

func (s *NaiveServer) OnConnectionClosed(c *Connection, err error) {
	s.funcOnConnectionClosed(c, err)
}

func (s *NaiveServer) OnConnectionMade(c *Connection, vin string) {
	s.funcOnConnectionMade(c, vin)
}

func (s *NaiveServer) OnMessageReceived(c *Connection, message []byte) {
	s.funcOnMessageReceived(c, message)
}

// GetTimeout get timeout
func (s *NaiveServer) GetTimeout() time.Duration {
	return time.Duration(s.Config.Timeout) * time.Second
}

// addConn add a id-conn pair to connections
// called on package arrived
func (s *NaiveServer) addConn(id string, conn *Connection) {
	if id == "" {
		return
	}

	mapMutex.Lock()
	defer mapMutex.Unlock()

	s.idMap[id] = conn
}

// removeConn remove a named connection to connections
// usually called on connection lost
func (s *NaiveServer) removeConn(id string) {
	if id == "" {
		return
	}

	mapMutex.Lock()
	defer mapMutex.Unlock()

	delete(s.idMap, id)
}

// GetConn get connection object via id
// usually needed when application have to handle underlay connections
// use case: send command to peer
func (s *NaiveServer) GetConn(id string) *Connection {
	mapMutex.RLock()
	defer mapMutex.RUnlock()

	return s.idMap[id]
}

// GetIDSet get id set for connection report
// this operation should copy the entire map to prevent race condition
// we should always handle mutex by ourself
func (s *NaiveServer) GetIDSet() map[string]bool {
	mapMutex.RLock()
	defer mapMutex.RUnlock()

	log.Debug().Msgf("id map: %v", s.idMap)
	m := make(map[string]bool, len(s.idMap))
	for id := range s.idMap {
		m[id] = true
	}
	return m
}

// Listen start server's running loop
func (s *NaiveServer) Listen() {
	s.started = true
	defer func() { s.started = false }()

	addr, err := net.ResolveTCPAddr("tcp", s.Config.Address)
	if err != nil {
		log.Panic().Err(err)
	}

	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		log.Panic().Err(err)
	}
	defer listener.Close()
	//netutil.LimitListener(listener, s.MaxConnection) // 最大连接数

	for {
		if !s.started {
			log.Info().Msg("server is going down")
			return
		}
		err = listener.SetDeadline(time.Now().Add(3 * time.Second))
		if err != nil {
			log.Error().Msg(err.Error())
			return
		}
		var conn *net.TCPConn
		conn, err = listener.AcceptTCP()
		if err != nil {
			continue
		}

		c := Connection{
			conn:           conn,
			Server:         s,
			MessageChan:    make(chan []byte, 20),
			ExitChan:       make(chan struct{}),
			IsFirstMessage: true,
		}
		// set peer address at start to avoid frequently system calls
		c.peerAddress = c.RemoteAddr().String()

		go c.dispatchMessage()
		go c.listen()
		//s.OnConnectionMade(&c)
	}
}

// 空结构体不占内存
func (s *NaiveServer) SubmitRequest() {
	s.QpsChan <- struct{}{}
}

func (s *NaiveServer) CalculateQps(ctx context.Context) {
	log.Info().Msg("start to calculate qps...")
	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("qps server is about to quit...")
			return
		case <-s.QpsChan:
			s.RequestCount += 1
			if s.QpsStartTimeStamp != 0 {
				s.QpsStartTimeStamp = time.Now().Unix()
			} else {
				timeStamp := time.Now().Unix()
				gap := timeStamp - s.QpsStartTimeStamp
				if gap > 1000 {
					qps := s.RequestCount
					if s.MaxQps > 0 && (qps > s.MaxQps) {
						log.Info().Msgf("warning, qps now: %d exceeded the max qps: %d", qps, s.MaxQps)
						s.QpsStartTimeStamp = 0
						s.RequestCount = 0 // 清空
					}
				}
			}
		}
	}
}

func (s *NaiveServer) Stop() {
	s.started = false
}

// 更新时间以及ttl由外部传递
func (s *NaiveServer) ConnectionStore(redisClient *redis.Client, ctx context.Context) {
	timer := time.NewTicker(10 * time.Second)
	defer timer.Stop()
	vinSet := make(map[string]bool, 5000)
	var dataList []WarpedConnectionInfo
	for {
		select {
		case <-ctx.Done():
			return
		case conn := <-s.ConnectionChan:
			var info WarpedConnectionInfo
			data := make(map[string]interface{})
			vin := conn.GetID()
			_, ok := vinSet[vin]
			if !ok {
				vinSet[vin] = true
				data["vin"] = vin
				data["host"] = hostName
				data["last_updated"] = time.Now().Unix()
				info.Data = data
				info.Conn = conn
				dataList = append(dataList, info)
			}
		case <-timer.C:
			if len(dataList) > 0 {
				data := make([]WarpedConnectionInfo, len(dataList))
				copy(data, dataList)
				dataList = nil
				vinSet = make(map[string]bool, 5000)
				updateConnections(redisClient, data)
			}
		}
	}
}

// TODO 确认TTL过期时间以及数据更新时间 避免造成缺口
// FIXME 理论上来说存在被删除之后再次写入的可能,毫秒级别的误差,写入之后只能自然过期
// 添加connection_host 存储host信息
func updateConnections(redisClient *redis.Client, dataList []WarpedConnectionInfo) {
	pipe := redisClient.Pipeline()
	for _, info := range dataList {
		conn := info.Conn
		item := info.Data
		vin := item["vin"]
		h := item["host"]
		hostStr := h.(string)

		vinKey := fmt.Sprintf("%s_%s", common.ConnectionKey, vin)
		dataMap := make(map[string]interface{})
		dataMap["host"] = hostStr
		dataMap["address"] = util.GetLocalIP()

		if conn.conn != nil { // 离线后被置为nil 防止离线删除后又因更新写入redis,而实则已经离线直到过期后才会被删除
			pipe.HMSet(vinKey, item)
			pipe.ExpireAt(vinKey, time.Now().Add(time.Duration(global.TTL)*time.Second))
		} else {
			log.Info().Msg("connection is nil....")
		}
	}
	if _, err := pipe.Exec(); err != nil {
		log.Error().Msgf("failed to push connections to redis: %s", err.Error())
		return
	}
	return
}
