package gateway

import (
	"github.com/imserver/gateway-go/pkg"
	"github.com/rs/zerolog/log"
	"net"
	"sync"
)

var protocol string

// EpollConnection holds info about EpollConnection
type EpollConnection struct {
	conn          *net.TCPConn
	Server        *EpollServer
	ResidueBytes  []byte
	PackageBuffer [][]byte
	rwLock        sync.RWMutex
	messageCh     chan []byte
	working       bool
	uid           string // 存储避免遍历
	//stopped chan struct{}
}

func (c *EpollConnection) GetResidueBytes() []byte {
	c.rwLock.RLock()
	defer c.rwLock.RUnlock()
	data := c.ResidueBytes[:]
	c.ResidueBytes = c.ResidueBytes[:0]
	return data
}

func (c *EpollConnection) AppendResidueBytes(data []byte) {
	c.rwLock.Lock()
	defer c.rwLock.Unlock()
	c.ResidueBytes = append(c.ResidueBytes, data...)
}

func (c *EpollConnection) Send(data []byte) {
	c.messageCh <- data
}

func (c *EpollConnection) stopWork() {
	c.rwLock.Lock()
	c.working = false
	c.rwLock.Unlock()
}

// 串行处理同一个连接的消息，防止多个gorun同时处理消息导致未读完的的消息错位
func (c *EpollConnection) Work() {
	c.rwLock.Lock()
	defer c.rwLock.Unlock()

	if !c.working {
		c.working = true
		for {
			select {
			case segment := <-c.messageCh:
				c.ParseMessage(segment)
			default:
				if len(c.messageCh) == 0 {
					c.working = false
					return
				}
			}
		}
	}
}

// ServerConfig involve server's configurations
type ServerConfig struct {
	Address string
	Timeout int
}

// EpollServer represent a server
type EpollServer struct {
	Poller                 *epoll
	Config                 *ServerConfig
	rwLock                 sync.RWMutex
	Protocol               string // 数据解析协议
	ConnectionMap          map[string]*EpollConnection
	funcOnConnectionMade   func(c *EpollConnection)
	funcOnConnectionClosed func(c *EpollConnection)
	funcOnMessageReceived  func(c *EpollConnection, message []byte)
}

// RegisterCallbacks register three callbacks
func (s *EpollServer) RegisterCallbacks(onConnectionMade func(c *EpollConnection), onConnectionClosed func(c *EpollConnection), onMessageReceived func(c *EpollConnection, message []byte)) {
	s.funcOnConnectionMade = onConnectionMade
	s.funcOnConnectionClosed = onConnectionClosed
	s.funcOnMessageReceived = onMessageReceived
}

func (s *EpollServer) FeedConnectMap(c *EpollConnection) {
	uid := c.uid
	if uid == "" {
		return
	}
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	s.ConnectionMap[uid] = c
}

func (s *EpollServer) RemoveConnection(c *EpollConnection) {
	uid := c.uid
	if uid == "" {
		return
	}
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	delete(s.ConnectionMap, uid) // 删除连接
}

func (s *EpollServer) onConnectionClosed(c *EpollConnection) {
	log.Error().Msgf("%x", c.ResidueBytes)
	if len(c.ResidueBytes) == 0 {
		return
	}
	messages, _, _ := SplitMessage(c.ResidueBytes)
	for _, m := range messages {
		err := Produce(globalProducer, globalTopic, m)
		if err != nil {
			log.Error().Msgf("err: %v, queue size: %v", err, globalProducer.Len())
		} else {
			verifySuccess.Inc()
		}
	}

	//s.funcOnConnectionClosed(c)
}

func (s *EpollServer) onConnectionMade(c *EpollConnection) {
	s.funcOnConnectionMade(c)
}

func (s *EpollServer) onMessageReceived(c *EpollConnection, message []byte) {
	s.funcOnMessageReceived(c, message)
}

// Listen start server's running loop
func (s *EpollServer) Listen() {
	protocol = s.Protocol
	addr, err := net.ResolveTCPAddr("tcp", s.Config.Address)
	if err != nil {
		log.Panic().Err(err)
	}

	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		panic(err)
	}
	defer listener.Close()

	s.Poller, err = MkEpoll()
	if err != nil {
		panic(err)
	}

	go s.start()

	for {
		conn, e := listener.AcceptTCP()
		if e != nil {
			if ne, ok := e.(net.Error); ok && ne.Temporary() {
				log.Error().Msgf("accept temp err: %v", ne)
				continue
			}
			log.Error().Msgf("accept err: %v", e)
			return
		}

		c := EpollConnection{
			conn:      conn,
			Server:    s,
			messageCh: make(chan []byte, 1000),
		}

		err := s.Poller.Add(&c)
		if err != nil {
			log.Error().Msgf("failed to add EpollConnection %v", err)
			conn.Close()
		} else {
			//s.onConnectionMade(&c)
		}
	}
}

func (s *EpollServer) start() {
	for {
		connections, err := s.Poller.Wait()
		if err != nil {
			log.Printf("failed to epoll wait %v", err)
			continue
		}
		for _, conn := range connections {
			if conn.conn == nil {
				continue
			}
			var buf = make([]byte, 4096) // the data which was left behind will be retrieved on the next time
			read, err := conn.conn.Read(buf)
			if err != nil {
				if err := s.Poller.Remove(conn); err != nil {
					log.Printf("failed to remove %v", err)
				}
				conn.conn.Close()
				s.onConnectionClosed(conn)
				continue
			}
			go conn.Work()
			conn.Send(buf[:read]) // flush buffer to channel so that function "Work" can parse
		}
	}
}

func (*EpollServer) GetConn(id string) *EpollConnection {
	return nil
}

// NewEpollServer creates new tcp EpollServer instance
func NewEpollServer(config *ServerConfig) *EpollServer {
	log.Debug().Msgf("creating Server with address %v", config.Address)
	server := &EpollServer{
		Config:        config,
		ConnectionMap: make(map[string]*EpollConnection, 1000),
	}
	return server
}

func (c *EpollConnection) ParseMessage(segment []byte) {

	log.Info().Msgf("receive segment: %x from %v", segment, c)
	if len(c.ResidueBytes) > 0 {
		segment = append(c.GetResidueBytes(), segment...)
	}
	messages, residueBytes, invalidMessages := SplitMessage(segment)
	if len(residueBytes) > 0 {
		c.AppendResidueBytes(residueBytes)
	}

	if len(invalidMessages) > 0 {
		for _, iMessage := range invalidMessages {
			err := Produce(globalProducer, errorTopic, iMessage)
			log.Error().Msgf("receive invalid message %x", iMessage)
			if err != nil {
				log.Error().Msgf("err: %v, queue size: %v", err, globalProducer.Len())
			}
		}
	}

	for _, message := range messages {
		p, err := pkg.DecodePackage(message, protocol)
		if err != nil {
			log.Error().Msgf("error: %v", err)
		}

		uid := string(p.UniqueCode())
		err = pkg.VerifyUid(p.UniqueCode())
		if err != nil {
			// 错误消息
			err = Produce(globalProducer, errorTopic, message)
			if err != nil {
				log.Error().Msgf("err: %v, queue size: %v", err, globalProducer.Len())
			}
			log.Info().Msgf("topic %s: uid: %s %x", errorTopic, uid, message)
			continue
		}
		c.uid = uid // 建立连接到uid的对应
		c.Server.FeedConnectMap(c)
		err = Produce(globalProducer, globalTopic, message)
		if err != nil {
			log.Error().Msgf("err: %v, queue size: %v", err, globalProducer.Len())
		} else {
			log.Info().Msgf("topic %s: uid: %s %x", globalTopic, uid, message)
			verifySuccess.Inc()
		}
	}
}
