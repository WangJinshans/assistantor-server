package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/BurnishCN/gateway-go/command_sender"
	"github.com/BurnishCN/gateway-go/common"
	"github.com/BurnishCN/gateway-go/connection_manager"
	"github.com/BurnishCN/gateway-go/hbase1"
	"github.com/BurnishCN/gateway-go/util"
	"github.com/go-basic/uuid"
	"github.com/go-redis/redis"
	consulapi "github.com/hashicorp/consul/api"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"net"
	"net/http"
	"strconv"
	"time"
)

const (
	ConfigFileName = "config.yaml"
)

// 考虑做成异步的情况, 发送失败的缓存到redis, 设置TTL, 循环拉取并发送并记录发送结果

var (
	hConfig      *hbase1.HConfig
	consulClient *consulapi.Client
	config       *ManagerConfig
	redisClient  *redis.Client
	hbaseClient  *hbase1.BatchClient
)

type ManagerConfig struct {
	LogLevel                 string
	ConsulAddress            string
	CommandSenderServerPort  int    // grpc 监听端口
	GatewayGrpcPort          int    // 网关grpc 端口
	ConnectionManagerAddress string // 长链接管理器地址
	ConnectionManagerPort    int    // 长链接管理器端口
	HealthCheckPort          int    // 健康检查端口
	RedisAddress             string
	RedisPort                int
	RedisDB                  int
	RedisPassWord            string
	HbaseTableName           string // 结果缓存表名
	ThriftAddress            string // thrift 地址
	BatchSize                int    // 批量写入大小
	PlatFormName             string
}

type UpInfo struct {
	Result string // 结果
	Key    string // key
}

type Manager struct {
	MessageChan chan UpInfo
}

func getConnectionInfo(vin string) (*connection_manager.Connection, error) {

	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", config.ConnectionManagerAddress, config.ConnectionManagerPort), grpc.WithInsecure())
	if err != nil {
		log.Fatal().Msgf("did not connect: %v", err)
	}
	defer conn.Close()
	queryClient := connection_manager.NewQueryClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	result, err := queryClient.QueryConnection(ctx, &connection_manager.QueryConnectionParameter{
		Id: vin,
	})
	return result, err
}

func (m *Manager) SendCommand(ctx context.Context, parameter *command_sender.SendParameter) (*command_sender.SendResult, error) {

	if config.PlatFormName == common.PlatFormJmc {
		return m.JmcSendCommand(parameter)
	}
	return &command_sender.SendResult{
		Success:      false,
		ErrorMessage: common.DefaultError,
	}, nil
}

// 江玲下发
func (m *Manager) JmcSendCommand(parameter *command_sender.SendParameter) (*command_sender.SendResult, error) {
	info := make(map[string]interface{})
	err := json.Unmarshal(parameter.Extra, &info)
	if err != nil {
		log.Error().Msgf("json unmarshall error: %v", err)
		return &command_sender.SendResult{
			Success:      false,
			ErrorMessage: common.ParameterError,
		}, nil
	}
	commandType := info["command_type"].(string)
	timeString := time.Unix(parameter.Timestamp, 0).Format("2006-01-02 15:04:05")
	commandSendKey := fmt.Sprintf("%s__%s__%s", parameter.Vin, commandType, timeString)
	log.Info().Msgf("connection key is: %s", fmt.Sprintf("%s_%s", common.ConnectionKey, parameter.Vin))
	log.Info().Msgf("command Key is: %s", commandSendKey)
	connectionInfo, err := getConnectionInfo(parameter.Vin)
	if err != nil || connectionInfo == nil {
		log.Error().Msgf("get connection gateway address error: %v", err)
		m.commitSendResult(false, commandSendKey)
		cacheRecord(parameter, commandType)
		return &command_sender.SendResult{
			Success:      false,
			ErrorMessage: common.EmptyConnection,
		}, nil
	}
	//gatewayAddress, err := redisClient.HGet(fmt.Sprintf("%s_%s", common.ConnectionKey, parameter.Vin), "address").Result()
	gatewayAddress := connectionInfo.Address
	//log.Info().Msgf("gateway address is: %s", connectionInfo.Address)
	if gatewayAddress == "" {
		log.Error().Msgf("get gateway address error: %v", err)
		m.commitSendResult(false, commandSendKey)
		cacheRecord(parameter, commandType)
		return &command_sender.SendResult{
			Success:      false,
			ErrorMessage: common.EmptyConnection,
		}, nil
	}
	result := new(command_sender.SendResult)
	gatewayAddress = fmt.Sprintf("%s:%d", gatewayAddress, config.GatewayGrpcPort)
	log.Info().Msgf("gateway address is: %v", gatewayAddress)
	var flag bool
	for index := 0; index < 3; index++ {
		result, err = grpcRequest(gatewayAddress, parameter)
		if err == nil && result.Success {
			log.Error().Msgf("command key is: %s, send command success", commandSendKey)
			flag = true
			break
		}
		log.Error().Msgf("command key is: %s, send command failed, error is: %v, retry %d", commandSendKey, err, index+1)
	}

	if flag {
		m.commitSendResult(true, commandSendKey)
	} else {
		m.commitSendResult(false, commandSendKey)
		cacheRecord(parameter, commandType)
	}
	return result, err
}

func cacheRecord(parameter *command_sender.SendParameter, commandType string) {
	commandCacheKey := fmt.Sprintf("%s:%s:%s:%d", common.CommandCacheKey, parameter.Vin, commandType, parameter.Timestamp)
	val, err := redisClient.Exists(commandCacheKey).Result()
	if err != nil {
		log.Error().Msgf("query key %s error: %v...", commandCacheKey, err)
		return
	}
	if val == 1 {
		log.Error().Msgf("key %s is already existed...", commandCacheKey)
		return
	}
	dataMap := make(map[string]interface{})
	dataMap["vin"] = parameter.Vin
	dataMap["timestamp"] = fmt.Sprintf("%d", parameter.Timestamp)
	dataMap["command"] = hex.EncodeToString(parameter.Command)
	dataMap["extra"] = string(parameter.Extra)
	_, err = redisClient.HMSet(commandCacheKey, dataMap).Result()
	if err != nil {
		log.Error().Msgf("cache record error: %v", err)
	}
	redisClient.ExpireAt(commandCacheKey, time.Now().Add(time.Duration(60)*time.Second))
}

func (m *Manager) commitSendResult(success bool, commandSendKey string) {
	var status string
	if success {
		status = common.CommandSendSuccess
	} else {
		status = common.CommandSendFailed
	}

	info := UpInfo{
		Result: status,
		Key:    commandSendKey,
	}

	m.MessageChan <- info

	return
}

func (m *Manager) asyncUpdateResult(ctx context.Context) {

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if hbaseClient.BatchSize > 0 {
				hbaseClient.BatchForceSave()
			}
			timer.Reset(5 * time.Second)
		case info := <-m.MessageChan:
			hbaseClient.Handle(info.Key, info.Result)
		}
	}
}

func grpcRequest(addr string, arg *command_sender.SendParameter) (result *command_sender.SendResult, err error) {
	result = new(command_sender.SendResult)
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Fatal().Msgf("did not connect: %v", err)
		return
	}
	defer conn.Close()
	sendClient := command_sender.NewSendClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err = sendClient.SendCommand(ctx, &command_sender.SendParameter{
		Vin:     arg.Vin,
		Command: arg.Command,
	})
	return
}

func initConfigFromFile(filename string) (err error) {
	viper.SetConfigFile(filename)
	viper.AddConfigPath(".")
	err = viper.ReadInConfig()
	if err != nil {
		log.Error().Msgf("read config error: %v", err)
		return
	}
	redisHost := viper.GetString("redis.host")
	redisPort := viper.GetInt("redis.port")
	redisPassword := viper.GetString("redis.password")
	redisDB := viper.GetInt("redis.db")
	logLevel := viper.GetString("logLevel")
	consulAddress := viper.GetString("consulAddress")
	serverPort := viper.GetInt("commandSenderServerPort")
	connectionManagerAddress := viper.GetString("connectionManagerAddress")
	connectionManagerPort := viper.GetInt("connectionManagerServerPort")
	gatewayGrpcPort := viper.GetInt("gatewayGrpcPort")
	healthCheckPort := viper.GetInt("healthCheckPort")
	hbaseTableName := viper.GetString("hbaseTableName")
	thriftAddress := viper.GetString("thriftAddress")
	batchSize := viper.GetInt("batchSize")
	platFormName := viper.GetString("platFormName")

	config = &ManagerConfig{
		LogLevel:                 logLevel,
		ConsulAddress:            consulAddress,
		CommandSenderServerPort:  serverPort,
		ConnectionManagerAddress: connectionManagerAddress,
		ConnectionManagerPort:    connectionManagerPort,
		GatewayGrpcPort:          gatewayGrpcPort,
		HealthCheckPort:          healthCheckPort,
		RedisAddress:             redisHost,
		RedisPort:                redisPort,
		RedisDB:                  redisDB,
		RedisPassWord:            redisPassword,
		HbaseTableName:           hbaseTableName,
		ThriftAddress:            thriftAddress,
		BatchSize:                batchSize,
		PlatFormName:             platFormName,
	}
	log.Info().Msgf("conf is: %v", consulAddress)
	return
}

func init() {
	err := initConfigFromFile(ConfigFileName)
	if err != nil {
		log.Panic().Err(err)
	}
	log.Info().Msgf("config is: %v", config)
	initConsul()

	hConfig = &hbase1.HConfig{
		TableName: config.HbaseTableName,
		Addr:      config.ThriftAddress,
		BatchSize: config.BatchSize,
	}
	hbaseClient, err = hbase1.NewBatchClient(hConfig)
	if err != nil {
		panic(err)
	}

	redisClient = util.GetRedisClient(config.RedisAddress, config.RedisPort, config.RedisPassWord, config.RedisDB)
}

func PrepareService() (err error) {
	go func() {
		err = StartCheckServer()
		if err != nil {
			log.Error().Err(err)
		}
	}()
	err = registerService()
	if err != nil {
		return
	}
	return
}

// Health Check Server
func StartCheckServer() (err error) {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("pong"))
	})
	err = http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", config.HealthCheckPort), nil)
	return
}

// GRPC Server
func StartCommandServer(port int) (err error) {
	addr := fmt.Sprintf("0.0.0.0:%d", port)
	ctx := context.Background()
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Error().Msgf("start grpc listener error: %s", err.Error())
		return
	}
	s := grpc.NewServer()
	c := make(chan UpInfo, 200)
	manager := &Manager{
		MessageChan: c,
	}
	command_sender.RegisterSendServer(s, manager)
	reflection.Register(s)
	log.Info().Msgf("command sender server at %v", addr)
	go manager.StartCacheLoop(ctx)
	go manager.asyncUpdateResult(ctx)

	err = s.Serve(listener)
	if err != nil {
		log.Error().Msgf("start grpc server error: %s", err.Error())
		return
	}
	return
}

func (m *Manager) StartCacheLoop(ctx context.Context) {
	timer := time.NewTimer(time.Second * 10)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			commandList := m.getCacheCommandList()
			log.Info().Msgf("command list is: %v", commandList)
			if len(commandList) > 0 {
				m.sendCacheCommand(commandList)
			}
			timer.Reset(time.Second * 10)
		}
	}
}

func (m *Manager) getCacheCommandList() (commandCacheList []string) {
	commandCacheKey := fmt.Sprintf("%s*", common.CommandCacheKey)
	//commandCacheList, err := redisClient.Keys(commandCacheKey).Result()    // keys * 会阻塞其他查询
	commandCacheList, err := util.ScanKeys(commandCacheKey, redisClient)
	if err != nil {
		log.Error().Msgf("get connection error: %v", err)
	}

	return
}

// 缓存下发方式
func (m *Manager) sendCacheCommand(commandCacheList []string) {
	ctx := context.Background()
	if len(commandCacheList) <= 0 {
		log.Info().Msg("no cache command...")
		return
	}
	pipe := redisClient.Pipeline()
	for _, commandKey := range commandCacheList {
		pipe.HGetAll(commandKey).Result()
	}
	resultList, err := pipe.Exec()
	if err != nil {
		log.Error().Msgf("pipe get data error: %v", err)
		return
	}
	for index, cmder := range resultList {
		var commandSendKey string
		res, err := cmder.(*redis.StringStringMapCmd).Result()
		if err != nil {
			log.Error().Msgf("query connection by vin: %v error: %v", commandCacheList[index], err)
			continue
		}
		vin := res["vin"]
		timeStamp := res["timestamp"]
		command := res["command"] // hex
		extra := res["extra"]
		ts, err := strconv.ParseInt(timeStamp, 10, 64)
		if err != nil {
			log.Error().Msgf("parse timestamp error: %v， timestamp is: %s", err, timeStamp)
			continue
		}
		commandData, err := hex.DecodeString(command)
		if err != nil {
			log.Error().Msgf("parse hex data error: %v", err)
			continue
		}
		param := &command_sender.SendParameter{
			Vin:       vin,
			Timestamp: ts,
			Command:   commandData,
			Extra:     []byte(extra),
		}
		rs, _ := m.SendCommand(ctx, param)
		if rs == nil || !rs.Success {
			continue
		}

		info := make(map[string]interface{})
		err = json.Unmarshal(param.Extra, &info)
		if err != nil {
			log.Error().Msgf("json unmarshall error: %v", err)
			continue
		}
		commandType := info["command_type"].(string)
		timeString := time.Unix(param.Timestamp, 0).Format("2006-01-02 15:04:05")
		commandSendKey = fmt.Sprintf("%s__%s__%s", param.Vin, commandType, timeString)
		commandCacheKey := fmt.Sprintf("%s:%s:%d", common.CommandCacheKey, param.Vin, param.Timestamp)
		_, err = redisClient.Del(commandCacheKey).Result()
		if err != nil {
			log.Error().Msgf("delete cache key error: %v", err)
		}
		m.commitSendResult(true, commandSendKey)
	}
}

func initConsul() {
	var err error
	c := consulapi.DefaultConfig()
	c.Address = config.ConsulAddress
	consulClient, err = consulapi.NewClient(c)
	if err != nil {
		panic(err)
	}
}

// 注册到consul
func registerService() (err error) {

	// 创建注册到consul的服务到
	uuid := uuid.New()
	registration := new(consulapi.AgentServiceRegistration)
	registration.ID = uuid
	registration.Name = common.CommandSendServiceName
	registration.Port = config.CommandSenderServerPort // grpc 包解析端口
	registration.Address = util.GetLocalIP()

	// 增加consul健康检查回调函数
	check := new(consulapi.AgentServiceCheck)
	checkAddress := fmt.Sprintf("http://%s:%d", registration.Address, config.HealthCheckPort)
	log.Info().Msgf("check address is: %s", checkAddress)
	check.HTTP = checkAddress
	check.Timeout = "5s"                         //超时
	check.Interval = "5s"                        //健康检查频率
	check.DeregisterCriticalServiceAfter = "30s" // 故障检查失败30s后 consul自动将注册服务删除
	registration.Check = check

	// 注册服务到consul
	err = consulClient.Agent().ServiceRegister(registration)
	return
}

func main() {
	err := PrepareService()
	if err != nil {
		log.Panic().Err(err)
	}

	err = StartCommandServer(config.CommandSenderServerPort)
	if err != nil {
		log.Error().Err(err)
	}
}
