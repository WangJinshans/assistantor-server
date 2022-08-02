package main

import (
	"context"
	"fmt"
	"github.com/BurnishCN/gateway-go/common"
	"github.com/BurnishCN/gateway-go/connection_manager"
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
	"strings"
)

const (
	QueryConnection  = 1
	QueryConnections = 2
	ConfigFileName   = "config.yaml"
)

var (
	consulClient *consulapi.Client
	config       *ManagerConfig
	redisClient  *redis.Client
)

type ManagerConfig struct {
	LogLevel                    string
	ConsulAddress               string
	ConnectionManagerServerPort int // grpc 监听端口
	HealthCheckPort             int // 健康检查端口
	RedisAddress                string
	RedisPort                   int
	RedisDB                     int
	RedisPassWord               string
}

type Manager struct {
}

// 查询单个连接
func (*Manager) QueryConnection(ctx context.Context, parameter *connection_manager.QueryConnectionParameter) (connection *connection_manager.Connection, err error) {
	vin := parameter.Id
	connection = new(connection_manager.Connection)
	// 从redis中获取信息
	vinKey := fmt.Sprintf("%s_%s", common.ConnectionKey, vin)
	connectionInfo, err := redisClient.HGetAll(vinKey).Result()
	if err != nil {
		log.Error().Msgf("get connection [%s] info error: %v", vinKey, err)
		return connection, nil
	}

	_, ok := connectionInfo["vin"]
	if !ok {
		log.Error().Msgf("query connection error, vin: %s, message: %s", vin, common.EmptyConnection)
		return connection, nil
	}
	connection.Id = connectionInfo["vin"]
	connection.Host = connectionInfo["host"]
	connection.Address = connectionInfo["address"]
	connection.LastUpdated = connectionInfo["last_updated"]
	log.Info().Msgf("connection is: %v", connection)
	return
}

// 获取所有连接
func (*Manager) QueryAllConnections(ctx context.Context, e *connection_manager.Empty) (connections *connection_manager.Connections, err error) {
	queryKey := fmt.Sprintf("%s*", common.ConnectionKey)
	//vinList, err := redisClient.Keys(queryKey).Result()
	vinList, err := util.ScanKeys(queryKey, redisClient)
	if err != nil {
		log.Error().Msgf("get connection error: %v", err)
		return connections, nil
	}
	if len(vinList) <= 0 {
		log.Info().Msg("no car is online")
		return connections, nil
	}
	connections = new(connection_manager.Connections)
	var connectionList []*connection_manager.Connection
	pipe := redisClient.Pipeline()
	for _, vinKey := range vinList {
		pipe.HGetAll(vinKey).Result()
	}
	resultList, err := pipe.Exec()
	if err != nil {
		log.Error().Msgf("pipe get data error: %v", err)
		return connections, nil
	}
	for index, cmder := range resultList {
		res, err1 := cmder.(*redis.StringStringMapCmd).Result()
		if err1 != nil {
			log.Error().Msgf("query connection by vin: %v error: %v", vinList[index], err1)
			continue
		}
		connection := &connection_manager.Connection{
			Id:          res["vin"],
			LastUpdated: res["last_updated"],
			Address:     res["address"],
			Host:        res["host"],
		}
		connectionList = append(connectionList, connection)
	}
	connections.Connections = connectionList
	return
}

// 查询host列表
func (*Manager) QueryHosts(ctx context.Context, e *connection_manager.Empty) (lst *connection_manager.HostInfoList, err error) {

	lst = new(connection_manager.HostInfoList)
	hostKey := fmt.Sprintf("%s*", common.ConnectionHostKey)
	//hostInfoList, err := redisClient.Keys(hostKey).Result()
	hostInfoList, err := util.ScanKeys(hostKey, redisClient)
	if err != nil {
		log.Error().Msgf("get connection error: %v", err)
		return lst, nil
	}
	if len(hostInfoList) <= 0 {
		log.Info().Msg("no car is online")
		return lst, nil
	}
	var hostList []*connection_manager.HostInfo

	for _, hostInfo := range hostInfoList {
		hostAddress := strings.Replace(hostInfo, fmt.Sprintf("%s_", common.ConnectionHostKey), "", -1)
		h := &connection_manager.HostInfo{
			HostName: hostAddress,
		}
		hostList = append(hostList, h)
	}
	lst.HostList = hostList
	return
}

// 根据host 查询该节点的所有连接, host 来自后端
func (*Manager) QueryConnectionsByHost(ctx context.Context, parameter *connection_manager.QueryConnectionsParameter) (connections *connection_manager.Connections, err error) {
	hostName := parameter.Host
	vinKey := fmt.Sprintf("%s_%s", common.ConnectionHostKey, hostName)

	connections = new(connection_manager.Connections)
	var connectionList []*connection_manager.Connection
	resultList, err := redisClient.LRange(vinKey, 0, -1).Result()

	for _, vin := range resultList {
		connection := &connection_manager.Connection{
			Id: vin,
		}
		connectionList = append(connectionList, connection)
	}
	connections.Connections = connectionList
	return
}

func StartCommandServer(port int) (err error) {
	addr := fmt.Sprintf("0.0.0.0:%d", port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Error().Msgf("start grpc listener error: %s", err.Error())
		return
	}
	s := grpc.NewServer()
	manager := Manager{}
	connection_manager.RegisterQueryServer(s, &manager)
	reflection.Register(s)
	log.Info().Msgf("connection manager server at %v", addr)
	err = s.Serve(listener)
	if err != nil {
		log.Error().Msgf("start grpc server error: %s", err.Error())
		return
	}
	return
}

func Handler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("pong"))
}

func StartCheckServer() (err error) {
	http.HandleFunc("/", Handler)
	err = http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", config.HealthCheckPort), nil)
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
	serverPort := viper.GetInt("connectionManagerServerPort")
	healthCheckPort := viper.GetInt("healthCheckPort")
	config = &ManagerConfig{
		LogLevel:                    logLevel,
		ConsulAddress:               consulAddress,
		ConnectionManagerServerPort: serverPort,
		HealthCheckPort:             healthCheckPort,
		RedisAddress:                redisHost,
		RedisPort:                   redisPort,
		RedisDB:                     redisDB,
		RedisPassWord:               redisPassword,
	}
	log.Info().Msgf("conf is: %v", consulAddress)
	return
}

func init() {
	err := initConfigFromFile(ConfigFileName)
	if err != nil {
		log.Error().Msgf("error is: %v", err)
	}
	log.Info().Msgf("config is: %v", config)
	initConsul()
	initRedisWithConfig()
}

func initRedisWithConfig() {
	redisClient = util.GetRedisClient(config.RedisAddress, config.RedisPort, config.RedisPassWord, config.RedisDB)
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

func PrepareService() (err error) {
	go StartCheckServer()
	err = registerService()
	if err != nil {
		return
	}
	return
}

// 注册到consul
func registerService() (err error) {

	// 创建注册到consul的服务到
	uuid := uuid.New()
	registration := new(consulapi.AgentServiceRegistration)
	registration.ID = uuid
	registration.Name = common.CommandQueryServiceName
	registration.Port = config.ConnectionManagerServerPort // grpc 包解析端口
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
	err = StartCommandServer(config.ConnectionManagerServerPort)
	if err != nil {
		log.Panic().Err(err)
	}
}
