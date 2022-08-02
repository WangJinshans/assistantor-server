package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/BurnishCN/gateway-go/common"
	"github.com/BurnishCN/gateway-go/gateway"
	"github.com/BurnishCN/gateway-go/global"
	"github.com/BurnishCN/gateway-go/network"
	"github.com/BurnishCN/gateway-go/pkg"
	"github.com/BurnishCN/gateway-go/util"
	"github.com/go-basic/uuid"
	"github.com/go-redis/redis"
	consulapi "github.com/hashicorp/consul/api"
	"github.com/imroc/biu"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	confluentKafka "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"io/ioutil"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"
)

const (
	EpollServerType  = "epoll"
	NormalServerType = "normal"
	LinuxPlatform    = "linux"
	HealthyCheckPort = 9111 // consul 健康检查端口
)

var (
	old7e = []byte{0x7e}
	new7e = []byte{0x7d, 0x02}
	old7d = []byte{0x7d}
	new7d = []byte{0x7d, 0x01}
)

var (
	// env
	env         string
	app         string
	signals     = make(chan os.Signal)
	host        string
	hostAddress string

	// features toggle
	enableConfigFromFile bool
	enableConsul         bool
	consulClient         *consulapi.Client

	enableLogToFile bool // 输出到文件
	commandPort     int  // grpc 命令下行服务端口
	serverType      string

	nativeServer *network.NaiveServer

	// protocol
	outputFile *os.File
	writer     *bufio.Writer
	// redis config
	redisClient *redis.Client

	// kafka config
	producer *confluentKafka.Producer

	// log config
	enableLogLine bool
	logLevel      string

	platForm string // 平台

	enableMonitoring bool
	enableConnReport bool

	consulAddr string // consul
	protocol   string // protocol

	// redis config
	redisHost        string
	redisPort        int
	redisPassword    string
	redisDB          int
	redisReadTimeout int

	// kafka config
	kafkaBrokers          []string
	kafkaSecurityProtocol string
	producerBufferSize    = 200000
	producerThreshold     = 300000
	normalTopic           string
	errorTopic            string

	// network config
	port            int
	socketTimeout   int
	sendCommandPort int
	maxQps          int64
	maxConnection   int
)

var (
	// variables for monitoring
	upstreamBytes = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "gateway",
			Subsystem: "traffic",
			Name:      "upstream_bytes",
			Help:      "upstream bytes",
		},
	)
	downstreamBytes = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "gateway",
			Subsystem: "traffic",
			Name:      "downstream_bytes",
			Help:      "downstream bytes",
		},
	)
	enqueuedPackages = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "gateway",
			Subsystem: "kafka",
			Name:      "enqueued_packages",
			Help:      "",
		},
	)
	producedPackages = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "gateway",
			Subsystem: "kafka",
			Name:      "produced_packages",
			Help:      "",
		},
	)
	errorPackages = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "gateway",
			Subsystem: "kafka",
			Name:      "error_packages",
			Help:      "",
		},
	)

	connectionCount = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "gateway",
			Subsystem: "connection",
			Name:      "connection_count",
			Help:      "connection count",
		},
	)
)

type consulKV struct {
	Key   string `json:"key"`
	Flags int    `json:"flags"`
	Value string `json:"value"`
}

type LogHook struct{}

func (hook LogHook) Run(e *zerolog.Event, level zerolog.Level, msg string) {
	if writer != nil {
		writer.WriteString(fmt.Sprintf("%s\n", msg))
	}
}

func init() {
	gateway.ConfigLogFromEnv()

	env = os.Getenv("ENV")
	app = os.Getenv("APP")
	if env == "" {
		env = "local"
	}
	if app == "" {
		app = "gateway"
	}

	// Trap SIGINT and SIGTERM to trigger a graceful shutdown
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	host = util.GetHost()
	if host == "" {
		log.Error().Msg("failed to get host name...")
	}
}

func readConfigFromFile() {
	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	err := viper.ReadInConfig()
	if err != nil {
		panic(err)
	}

	protocol = viper.GetString("protocol")

	kafkaBrokers = viper.GetStringSlice("kafka.brokerUrls")
	kafkaSecurityProtocol = viper.GetString("kafka.securityProtocol")
	normalTopic = viper.GetString("kafka.normalTopic")
	errorTopic = viper.GetString("kafka.errorTopic")

	redisHost = viper.GetString("redis.host")
	redisPort = viper.GetInt("redis.port")
	redisPassword = viper.GetString("redis.password")
	redisDB = viper.GetInt("redis.db")

	commandPort = viper.GetInt("commandPort") // grpc
	enableLogToFile = viper.GetBool("enableLogToFile")
	serverType = viper.GetString("serverType")
	socketTimeout = viper.GetInt("socketTimeout")
	maxQps = viper.GetInt64("maxQps")
	maxConnection = viper.GetInt("maxConnection")

	enableMonitoring = viper.GetBool("enableMonitoring")
	logLevel = viper.GetString("logLevel")
	platForm = viper.GetString("platForm")
	switch logLevel {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	}
	if enableLogToFile {
		log.Logger = log.Hook(LogHook{})
		outputFile, err = os.Create(fmt.Sprintf("./%d-gateway.log", time.Now().Unix()))
		if err != nil {
			log.Error().Msgf("fail to create gateway.log,err:%v", err)
		} else {
			writer = bufio.NewWriter(outputFile)
		}
	}
}

//func initConsulClient() {
//	consul.SetEnv(env)
//	consul.SetApp(app)
//	consul.InitConsul(consulAddr)
//}
//
//func readConfigFromConsul() {
//	// kafka
//	kafkaBrokers = consul.GetStringSlice("kafka.brokerUrls")
//	kafkaSecurityProtocol = consul.GetString("kafka.securityProtocol")
//	normalTopic = consul.GetString("kafka.normalTopic")
//	errorTopic = consul.GetString("kafka.errorTopic")
//
//	// redis
//	redisHost = consul.GetString("redis.host")
//	redisPort = consul.GetInt("redis.port")
//	redisPassword = consul.GetString("redis.password")
//	redisDB = consul.GetInt("redis.db")
//	redisReadTimeout = consul.GetInt("redis.readTimeout")
//
//	socketTimeout = consul.GetInt("socketTimeout")
//
//	protocol = consul.GetString("protocol")
//
//	enableLogLine = consul.GetBool("enableLogLine")
//	logLevel = consul.GetString("logLevel")
//
//	enableMonitoring = consul.GetBool("enableMonitoring")
//	enableConnReport = consul.GetBool("enableConnReport")
//}

// 注册到consul
func registerService() (err error) {

	// 创建注册到consul的服务到
	uuid := uuid.New()
	registration := new(consulapi.AgentServiceRegistration)
	registration.ID = uuid
	registration.Name = common.CommandSendServiceName // 根据这个名称来找这个服务
	registration.Port = commandPort                   // grpc 命令下行端口
	registration.Address = hostAddress

	// 增加consul健康检查回调函数
	check := new(consulapi.AgentServiceCheck)
	checkAddress := fmt.Sprintf("http://%s:%d", registration.Address, HealthyCheckPort)
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

func initConsulKV(env, app string) {
	prefix := env + "/" + app + "/"
	kvList := []consulKV{
		{Key: env + "/"},
		{Key: prefix},
		{Key: prefix + "kafka.brokerUrls"},
		{Key: prefix + "kafka.securityProtocol"},
		{Key: prefix + "kafka.normalTopic"},
		{Key: prefix + "kafka.errorTopic"},
		{Key: prefix + "redis.host"},
		{Key: prefix + "redis.port"},
		{Key: prefix + "redis.password"},
		{Key: prefix + "redis.db"},
		{Key: prefix + "redis.readTimeout"},
		{Key: prefix + "socketTimeout"},
		{Key: prefix + "protocol"},
		{Key: prefix + "logLevel"},
		{Key: prefix + "enableLogLine"},
		{Key: prefix + "enableMonitoring"},
		{Key: prefix + "enableConnReport"},
	}

	// serialize
	res1B, _ := json.Marshal(kvList)
	ioutil.WriteFile("kv.json", res1B, 0644)
}

// ===========================================================================================

func main() {
	var serverCmd = &cobra.Command{
		Use: "server",
		Run: func(cmd *cobra.Command, args []string) {
			startServer()
		},
	}

	// fix bug: https://zentao.burnish.cn:8200/zentao/task-view-852.html
	// 即使是未运行的 sub command, 为其设定的默认值也会对变量进行覆盖，
	// 使得程序运行异常
	var envTmp string
	var appTmp string
	var initConsulKVCmd = &cobra.Command{
		Use: "init-consul-kv",
		Run: func(cmd *cobra.Command, args []string) {
			log.Info().Msgf("init consul kv...")
			initConsulKV(envTmp, appTmp)
		},
	}

	serverCmd.Flags().BoolVarP(&enableConfigFromFile, "enable-config-from-file", "", true, "read config from file")
	serverCmd.Flags().BoolVarP(&enableConsul, "enable-config-from-consul", "", false, "read config from consul")
	serverCmd.Flags().StringVarP(&consulAddr, "consul", "", "", "consul address")
	serverCmd.Flags().IntVarP(&port, "port", "p", 11000, "port to listen")

	initConsulKVCmd.Flags().StringVarP(&envTmp, "env", "", "local", "primary prefix")
	initConsulKVCmd.Flags().StringVarP(&appTmp, "app", "", "gateway", "secondary prefix")

	serverCmd.AddCommand(initConsulKVCmd)
	serverCmd.Execute()
}

func initConsul() {
	var err error
	config := consulapi.DefaultConfig()
	config.Address = consulAddr
	log.Info().Msgf("consul address is: %s", consulAddr)
	consulClient, err = consulapi.NewClient(config)
	if err != nil {
		panic(err)
	}
}

func startServer() {
	hostAddress = util.GetLocalIP()
	if enableConfigFromFile {
		readConfigFromFile()
	}
	//if enableConsul {
	//	initConsulClient()
	//	readConfigFromConsul()
	//}
	initConsul()
	configMonitorFromEnv()

	global.CommandPort = commandPort
	global.TTL = socketTimeout
	log.Info().Msgf("enableConnReport: %v", enableConfigFromFile)
	log.Info().Msgf("enableConsul: %v", enableConsul)
	log.Info().Msgf("enableMonitoring: %v", enableMonitoring)
	log.Info().Msgf("protocol: %v", protocol)
	log.Info().Msgf("host address is: %s", hostAddress)

	global.Protocol = protocol
	if enableMonitoring {
		prometheusRegister()

		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		AttachProfiler(mux)

		s := &http.Server{
			Addr:           ":8080",
			Handler:        mux,
			ReadTimeout:    30 * time.Second,
			WriteTimeout:   30 * time.Second,
			MaxHeaderBytes: 1 << 20,
		}

		go func() {
			logrus.Fatal(s.ListenAndServe())
			//http.ListenAndServe("127.0.0.1:8090", nil)
		}()
	}
	// 配置启动epoll
	log.Info().Msgf("serverType: %s", serverType)
	if serverType == EpollServerType && runtime.GOOS == LinuxPlatform {
		makeEpollServer()
	} else {
		makeServer()
	}
}

func makeEpollServer() {

	redisClient = util.GetRedisClientWithTimeOut(redisHost, redisPort, redisPassword, redisDB, redisReadTimeout)

	// 从环境变量中读取kafka配置并覆盖
	configKafkaFromEnv()
	producer = gateway.MakeProducer(
		kafkaBrokers,
		producerBufferSize,
	)

	stopContext, cancel := context.WithCancel(context.Background())
	gatewayServerConfig := gateway.GatewayServerConfig{
		GatewayPort:       port,
		SocketTimeout:     socketTimeout,
		CommandSenderPort: sendCommandPort,
		RedisClient:       redisClient,
		Protocol:          protocol,
		Producer:          producer,
		NormalTopic:       normalTopic,
		ErrorTopic:        errorTopic,
		EnableMonitoring:  enableMonitoring,
		StopContext:       stopContext,
		NetworkMode:       gateway.ConfigNetworkFromEnv(),
	}

	registerService() // 注册服务
	go func() {
		gateway.Serve(gatewayServerConfig)
	}()

	sig := <-signals
	cancel()
	log.Info().Msgf("exit signal: %v", sig)
}

func configKafkaFromEnv() {
	kafkaString := os.Getenv("KAFKA_ADDRS")
	if kafkaString != "" {
		kafkaBrokers = kafkaBrokers[:0]
		for _, addr := range strings.Split(kafkaString, ",") {
			if addr != "" {
				kafkaBrokers = append(kafkaBrokers, addr)
			}
		}
	}
}

func configMonitorFromEnv() {
	enable := os.Getenv("ENABLE_MONITOR")
	if enable == "true" {
		enableMonitoring = true
	}
}

func prometheusRegister() {
	// monitoring
	prometheus.Register(upstreamBytes)
	prometheus.Register(downstreamBytes)
	prometheus.Register(enqueuedPackages)
	prometheus.Register(producedPackages)
	prometheus.Register(errorPackages)
	prometheus.Register(connectionCount)
}

func AttachProfiler(router *http.ServeMux) {
	router.HandleFunc("/debug/pprof/", pprof.Index)
	router.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	router.HandleFunc("/debug/pprof/profile", pprof.Profile)
	router.HandleFunc("/debug/pprof/symbol", pprof.Symbol)

	// Manually add support for paths linked to by index page at /debug/pprof/
	router.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	router.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	router.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
	router.Handle("/debug/pprof/block", pprof.Handler("block"))
}

func connectKafka() {
	var err error
	var kafkaBrokerString string
	if len(kafkaBrokers) == 1 {
		kafkaBrokerString = kafkaBrokers[0]
	} else {
		kafkaBrokerString = strings.Join(kafkaBrokers, ",")
	}
	log.Info().Msgf("kafkaBrokerString is: %s", kafkaBrokerString)
	producer, err = confluentKafka.NewProducer(&confluentKafka.ConfigMap{
		"bootstrap.servers": kafkaBrokerString,
		// "security.protocol":            kafkaSecurityProtocol,
		"security.protocol":            "plaintext",
		"ssl.ca.location":              "./ca-cert",
		"queue.buffering.max.messages": producerBufferSize,
		"go.batch.producer":            true,
		"linger.ms":                    1000,
		"request.timeout.ms":           100000,
		"compression.type":             "snappy",
		"retries":                      20,
		"retry.backoff.ms":             1000,
		//"go.events.channel.size":       producerBufferSize,
		//"go.produce.channel.size":      producerBufferSize,
	})
	if err != nil {
		panic(err)
	}
	log.Info().Msgf("Connect to kafka at %v", kafkaBrokers)

	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *confluentKafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Error().Msgf("ev: %v", ev.TopicPartition.Error)
					errorPackages.Observe(1)
				} else {
					producedPackages.Observe(1)
				}
			}
		}
	}()
}

func makeServer() {

	ctx := context.Background()
	redisClient = util.GetRedisClientWithTimeOut(redisHost, redisPort, redisPassword, redisDB, redisReadTimeout)
	connectKafka()

	serverConfig := network.ServerConfig{
		Address:       fmt.Sprintf("0.0.0.0:%d", port),
		Timeout:       socketTimeout,
		MaxQps:        maxQps,
		MaxConnection: maxConnection,
	}
	nativeServer = network.NewNativeServer(&serverConfig)
	if protocol == pkg.ProtocolJTT808 {
		nativeServer.RegisterCallbacks(jtt808ConnectionMade, jtt808ConnectionLost, jtt808MessageReceived)
	} else if protocol == pkg.ProtocolXNY32960 {
		nativeServer.RegisterCallbacks(xny32960ConnectionMade, xny32960ConnectionLost, xny32960MessageReceived)
	} else {
		nativeServer.RegisterCallbacks(connectionMade, connectionLost, messageReceived)
	}
	done := make(chan bool, 1)

	go func() {
		sig := <-signals
		log.Info().Msgf("signal: %v", sig)
		nativeServer.Stop()
		done <- true
	}()
	if commandPort != 0 {
		go gateway.StartCommandServer(commandPort, nativeServer, redisClient) // 下行
	}
	//registerService()
	//go startCheckServer()
	go nativeServer.ConnectionStore(redisClient, ctx) // 长链接定时更新
	//go nativeServer.CalculateQps(ctx)                 // 统计qps
	nativeServer.Listen()
	<-done

	hostKey := fmt.Sprintf("%s_%s", common.ConnectionHostKey, host)
	redisClient.Del(hostKey) // 退出后清除该节点上连接key的缓存信息
}

func Handler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("pong"))
}

func startCheckServer() {
	//定义一个http接口
	http.HandleFunc("/", Handler)
	err := http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", HealthyCheckPort), nil)
	if err != nil {
		fmt.Println("error: ", err.Error())
	}
}

func connectionMade(c *network.Connection, vin string) {
	log.Info().Msgf("Receive new connection from %v, vin: %s", c.RemoteAddr(), vin)
	c.SetID(vin) // 设置当前连接Id
	// 更新redis 连接状态
	dataSet := make(map[string]interface{})

	hostKey := fmt.Sprintf("%s_%s", common.ConnectionHostKey, host)
	redisClient.LPush(hostKey, vin)

	dataSet["vin"] = vin
	dataSet["host"] = host
	dataSet["address"] = hostAddress
	dataSet["last_updated"] = time.Now().Unix()
	vinKey := fmt.Sprintf("%s_%s", common.ConnectionKey, vin)
	redisClient.HMSet(vinKey, dataSet)
	redisClient.ExpireAt(vinKey, time.Now().Add(time.Duration(socketTimeout)*time.Second))
	connectionCount.Inc()
}

func messageReceived(c *network.Connection, segment []byte) {

	//c.Server.SubmitRequest()
	log.Info().Msgf("Receive segment: %x from %v", segment, c)
	if len(c.ResidueBytes) > 0 {
		segment = append(c.ResidueBytes, segment...)
	}
	messages, residueBytes, invalidMessages := gateway.Split(segment)
	c.ResidueBytes = residueBytes
	for _, message := range messages {
		// deconstract packages
		p, err := pkg.DeconstractPackage(message, protocol)
		if err != nil {
			log.Error().Msgf("error: %v", err)
		}

		// check vin
		vin := string(p.UniqueCode())
		err = pkg.VerifyVINascii(p.UniqueCode())
		if err != nil {
			errorMsg := gateway.FormatErrorMsg("gateway", fmt.Sprintf("%x", message), "", "invaild vin")
			err = gateway.ProduceWithKey(producer, errorTopic, errorMsg, []byte{0})
			if err != nil {
				log.Error().Msgf("error: %v, queue size: %v", err, producer.Len())
			} else {
				log.Info().Msgf("topic %s: vin: %s %x", errorTopic, vin, message)
				enqueuedPackages.Observe(1)
			}
			continue
		}

		err = gateway.Produce(producer, normalTopic, message)
		if err != nil {
			log.Error().Msgf("error: %v, queue size: %v", err, producer.Len())
		} else {
			log.Info().Msgf("topic %s: vin: %s %x", normalTopic, vin, message)
			enqueuedPackages.Observe(1)
		}

		//generate response to the package which does not depend on the business
		response := gateway.QuickResponse(p)
		if response != nil {
			err = c.Send(response)
			if err != nil {
				log.Error().Msgf("send back error: %v", err)
			}
			downstreamBytes.Observe(float64(len(response)))
		}

		if vin != pkg.PlaceholderVIN && vin != "" {
			if c.IsFirstMessage {
				c.IsFirstMessage = false
				c.Server.OnConnectionMade(c, vin)
			} else {
				nativeServer.ConnectionChan <- c
			}
		}
	}

	// producer invalid messages
	for _, message := range invalidMessages {
		log.Info().Msgf("checksum-invalid: %x", message)
		errorMsg := gateway.FormatErrorMsg("gateway", fmt.Sprintf("%x", message), "", "checksum-invalid")
		err := gateway.ProduceWithKey(producer, errorTopic, errorMsg, []byte{0})
		if err != nil {
			log.Info().Msgf("err: %v, queue size: %v", err, producer.Len())
		} else {
			log.Info().Msgf("topic %s: %x", errorTopic, message)
			enqueuedPackages.Observe(1)
		}
	}
	upstreamBytes.Observe(float64(len(segment)))
}

func connectionLost(c *network.Connection, err error) {
	log.Info().Msgf("Connection lost with client %v, vin: %s, err: %v", c.RemoteAddr(), c.GetID(), err)
	// 从redis中删除
	vin := c.GetID()
	if vin != "" {
		vinKey := fmt.Sprintf("%s_%s", common.ConnectionKey, vin)
		redisClient.Del(vinKey)

		hostKey := fmt.Sprintf("%s_%s", common.ConnectionHostKey, host)
		redisClient.LRem(hostKey, 0, vin) // 移除列表中所有与vin相等的值
	}
	connectionCount.Dec()
}

func jtt808ConnectionMade(c *network.Connection, vin string) {
	log.Info().Msgf("receive new connection from client...")
	c.SetID(vin) // 设置当前连接Id
	// 更新redis 连接状态
	dataSet := make(map[string]interface{})

	hostKey := fmt.Sprintf("%s_%s", common.ConnectionHostKey, host)
	redisClient.LPush(hostKey, vin)

	dataSet["vin"] = vin
	dataSet["host"] = host
	dataSet["address"] = hostAddress
	dataSet["last_updated"] = time.Now().Unix()
	vinKey := fmt.Sprintf("%s_%s", common.ConnectionKey, vin)
	redisClient.HMSet(vinKey, dataSet)
	redisClient.ExpireAt(vinKey, time.Now().Add(time.Duration(socketTimeout)*time.Second))
}

func jtt808ConnectionLost(c *network.Connection, err error) {
	log.Info().Msgf("connection was lost...")
	vin := c.GetID()
	if vin != "" {
		vinKey := fmt.Sprintf("%s_%s", common.ConnectionKey, vin)
		redisClient.Del(vinKey)

		hostKey := fmt.Sprintf("%s_%s", common.ConnectionHostKey, host)
		redisClient.LRem(hostKey, 0, vin) // 移除列表中所有与vin相等的值
	}
}

func jtt808MessageReceived(c *network.Connection, segment []byte) {

	log.Info().Msgf("Receive segment: %x from %v", segment, c)
	if len(c.ResidueBytes) > 0 {
		segment = append(c.ResidueBytes, segment...)
	}
	messages, residueBytes, invalidMessages := gateway.Split808(segment)
	c.ResidueBytes = residueBytes
	for _, message := range messages {

		vin := hex.EncodeToString(message[5:11])
		err := pkg.CheckBCCV1(message)
		if err != nil {
			err = gateway.ProduceWithKey(producer, errorTopic, message, []byte{0})
			if err != nil {
				log.Error().Msgf("error: %v, queue size: %v", err, producer.Len())
			} else {
				log.Info().Msgf("topic %s: data: %x", errorTopic, message)
			}
			continue
		}

		err = gateway.Produce(producer, normalTopic, message)
		if err != nil {
			log.Error().Msgf("error: %v, queue size: %v", err, producer.Len())
		} else {
			log.Info().Msgf("topic %s: data: %x", normalTopic, message)
			enqueuedPackages.Observe(1)
		}
		if protocol == pkg.ProtocolJTT808 {
			makeResponse(c, message)
		}

		if vin != pkg.PlaceholderVIN && vin != "" {
			if c.IsFirstMessage {
				c.IsFirstMessage = false
				c.Server.OnConnectionMade(c, vin)
			} else {
				nativeServer.ConnectionChan <- c
			}
		}
	}

	// producer invalid messages
	for _, message := range invalidMessages {
		if len(message) <= 0 {
			continue
		}
		log.Info().Msgf("checksum-invalid: %x", message)
		errorMsg := gateway.FormatErrorMsg("gateway", fmt.Sprintf("%x", message), "", "checksum-invalid")
		err := gateway.ProduceWithKey(producer, errorTopic, errorMsg, []byte{0})
		if err != nil {
			log.Info().Msgf("err: %v, queue size: %v", err, producer.Len())
		} else {
			log.Info().Msgf("topic %s: %x", errorTopic, message)
		}
	}
}

func parseBinToHex(s string) (hexRes string, err error) {
	var res uint16
	err = biu.ReadBinaryString(s, &res)
	if err != nil {
		return
	}
	hexRes = fmt.Sprintf("%.4x", res)
	log.Info().Msgf("s is: %s, res is: %s", s, hexRes)
	return
}

func makeResponse(conn *network.Connection, message []byte) {
	payload := message[1 : len(message)-1]
	messageId := payload[0:2]
	messageInfo := payload[2] >> 9
	serialNumber := payload[4:10]
	messageNumber := payload[10:12]
	messageStringId := hex.EncodeToString(messageId)

	var respData []byte
	switch messageStringId {
	case "0100":
		bodyLength := 9
		type_ := fmt.Sprintf("%.6b%.10b", messageInfo, bodyLength) // 消息属性
		typeData, err := parseBinToHex(type_)
		if err != nil {
			log.Error().Msgf("warp message attribute error: %v", err)
			return
		}
		// TODO 暂定将serialNumber作为基础秘钥
		data := fmt.Sprintf("8100%s%x%x%x00%x", typeData, serialNumber, messageNumber, messageNumber, serialNumber)
		log.Info().Msgf("data is: %s", data)
		d, err := hex.DecodeString(data)
		if err != nil {
			log.Error().Msgf("make response error: %v", err)
			return
		}
		bcc := pkg.BCC(d)
		var responseData []byte
		responseData = append(responseData, d...)
		responseData = append(responseData, bcc)
		responseData = bytes.Replace(responseData, old7d, new7d, -1)
		responseData = bytes.Replace(responseData, old7e, new7e, -1)
		var content bytes.Buffer
		content.WriteByte(0x7e)
		content.Write(responseData)
		content.WriteByte(0x7e)
		respData = content.Bytes()
	case "0002", "0102", "0200", "0704": // 通用回复
		bodyLength := 5
		type_ := fmt.Sprintf("%.6b%.10b", messageInfo, bodyLength) // 消息属性
		typeData, err := parseBinToHex(type_)
		if err != nil {
			log.Error().Msgf("warp message attribute error: %v", err)
			return
		}
		data := fmt.Sprintf("8001%s%x%x%x%x00", typeData, serialNumber, messageNumber, messageNumber, messageId)
		d, err := hex.DecodeString(data)
		if err != nil {
			log.Error().Msgf("make response error: %v", err)
			return
		}

		bcc := pkg.BCC(d)
		var responseData []byte
		responseData = append(responseData, d...)
		responseData = append(responseData, bcc)
		responseData = bytes.Replace(responseData, old7d, new7d, -1)
		responseData = bytes.Replace(responseData, old7e, new7e, -1)
		var content bytes.Buffer
		content.WriteByte(0x7e)
		content.Write(responseData)
		content.WriteByte(0x7e)
		respData = content.Bytes()
	}
	if len(respData) > 0 {
		err := conn.Send(respData)
		if err != nil {
			log.Error().Msgf("failed to send data back, err: %v...", err)
		}
	}
}

// 新能源32960
func xny32960ConnectionMade(c *network.Connection, vin string) {
	c.SetID(vin)
	log.Info().Msgf("receive new connection vin: %s from client...", vin)
	dataSet := make(map[string]interface{})

	hostKey := fmt.Sprintf("%s_%s", common.ConnectionHostKey, host)
	redisClient.LPush(hostKey, vin)

	dataSet["vin"] = vin
	dataSet["host"] = host
	dataSet["address"] = hostAddress
	dataSet["last_updated"] = time.Now().Unix()
	vinKey := fmt.Sprintf("%s_%s", common.ConnectionKey, vin)
	redisClient.HMSet(vinKey, dataSet)
	redisClient.ExpireAt(vinKey, time.Now().Add(time.Duration(socketTimeout)*time.Second))
}

func xny32960ConnectionLost(c *network.Connection, err error) {
	log.Info().Msgf("connection was lost, vin: %s...", c.GetID())
	vin := c.GetID()
	if vin != "" {
		vinKey := fmt.Sprintf("%s_%s", common.ConnectionKey, vin)
		redisClient.Del(vinKey)

		hostKey := fmt.Sprintf("%s_%s", common.ConnectionHostKey, host)
		redisClient.LRem(hostKey, 0, vin) // 移除列表中所有与vin相等的值
	}
}

func xny32960MessageReceived(c *network.Connection, segment []byte) {

	log.Info().Msgf("Receive segment: %x from %v", segment, c)
	IdString := fmt.Sprintf("Receive segment: %x", segment)
	if len(c.ResidueBytes) > 0 {
		segment = append(c.ResidueBytes, segment...)
	}
	messages, residueBytes, invalidMessages := gateway.Split32960(segment)
	c.ResidueBytes = residueBytes
	for _, message := range messages {

		vin := string(message[4:21]) // vin码
		err := pkg.CheckBCCV1(message)
		if err != nil {
			err = gateway.ProduceWithKey(producer, errorTopic, message, []byte{0})
			if err != nil {
				log.Error().Msgf("error: %v, queue size: %v", err, producer.Len())
			} else {
				log.Info().Msgf("topic %s: data: %x", errorTopic, message)
			}
			continue
		}
		commandFlag := message[3] // 应答标志
		if commandFlag == 0xfe {  // 需要应答的标志
			if platForm == common.PlatFormXev {
				err = pkgXevQuickResponse(message, c)
			} else if platForm == common.PlatFormXevCar {
				err = pkgXevQuickResponse(message, c)
			} else {
				err = pkg32960QuickResponse(message, IdString, c)
			}
			if err != nil {
				log.Error().Msgf("xyn32960 quick response error: %v", err)
			}
		}
		err = gateway.Produce(producer, normalTopic, message)
		if err != nil {
			log.Error().Msgf("error: %v, queue size: %v", err, producer.Len())
		} else {
			log.Info().Msgf("topic %s: data: %x", normalTopic, message)
			enqueuedPackages.Observe(1)
		}

		if vin != pkg.PlaceholderVIN && vin != "" {
			if c.IsFirstMessage {
				c.IsFirstMessage = false
				c.Server.OnConnectionMade(c, vin)
			} else {
				nativeServer.ConnectionChan <- c
			}
		}
	}

	// producer invalid messages
	for _, message := range invalidMessages {
		if len(message) <= 0 {
			continue
		}
		log.Info().Msgf("checksum-invalid: %x", message)
		errorMsg := gateway.FormatErrorMsg("gateway", fmt.Sprintf("%x", message), "", "checksum-invalid")
		err := gateway.ProduceWithKey(producer, errorTopic, errorMsg, []byte{0})
		if err != nil {
			log.Info().Msgf("err: %v, queue size: %v", err, producer.Len())
		} else {
			log.Info().Msgf("topic %s: %x", errorTopic, message)
		}
	}
}

// 32960 回复
func pkg32960QuickResponse(message []byte, IdString string, conn *network.Connection) (err error) {
	// 只有时间 内容为空 长度固定为6字节
	if len(message) < 30 {
		err = errors.New("message length error")
	}
	messageFlag := message[2]      // 命令标识
	commonMessage := message[4:22] // 包含vin
	obdTimeBytes := message[24:30]

	var responseData bytes.Buffer
	flagBytes, _ := hex.DecodeString("2323")
	lengthBytes, _ := hex.DecodeString("0006")
	responseData.Write(flagBytes)
	responseData.WriteByte(messageFlag)
	responseData.WriteByte(0x01) // 回复成功
	responseData.Write(commonMessage)
	responseData.Write(lengthBytes)
	responseData.Write(obdTimeBytes)
	data := responseData.Bytes()
	bcc := pkg.BCC(data)
	responseData.WriteByte(bcc)
	log.Info().Msgf("responseData.Bytes are: %x, from source: %s", responseData.Bytes(), IdString)
	err = conn.Send(responseData.Bytes())
	return
}

func genXevCarResponse(message []byte) (bs []byte) {
	messageFlag := message[2]      // 命令标识
	commonMessage := message[4:22] // 包含vin
	serialNumber := message[30:32]

	var responseData bytes.Buffer
	flagBytes, _ := hex.DecodeString("2323")
	lengthBytes, _ := hex.DecodeString("0006")
	responseData.Write(flagBytes)
	responseData.WriteByte(0x41)
	responseData.WriteByte(0x01)           // 回复成功
	responseData.Write(commonMessage)      // 公共部分
	responseData.Write(lengthBytes)        // 长度
	responseData.Write(serialNumber)       // 应答流水号
	responseData.WriteByte(messageFlag)    // 应答Id
	responseData.Write([]byte{0x00, 0x01}) // 应答结果
	data := responseData.Bytes()
	bcc := pkg.BCC(data)
	responseData.WriteByte(bcc)
	bs = responseData.Bytes()
	return
}

func gen32960QuickResponse(message []byte) (bs []byte) {
	messageFlag := message[2]      // 命令标识
	commonMessage := message[4:22] // 包含vin
	obdTimeBytes := message[24:30]

	var responseData bytes.Buffer
	flagBytes, _ := hex.DecodeString("2323")
	lengthBytes, _ := hex.DecodeString("0006")
	responseData.Write(flagBytes)
	responseData.WriteByte(messageFlag)
	responseData.WriteByte(0x01) // 回复成功
	responseData.Write(commonMessage)
	responseData.Write(lengthBytes)
	responseData.Write(obdTimeBytes)
	data := responseData.Bytes()
	bcc := pkg.BCC(data)
	responseData.WriteByte(bcc)
	bs = responseData.Bytes()
	return
}

// xev 回复
func pkgXevQuickResponse(message []byte, conn *network.Connection) (err error) {
	// 只有时间 内容为空 长度固定为6字节
	if len(message) < 30 {
		err = errors.New("message length error")
	}
	messageFlag := message[2] // 命令标识
	var content []byte

	switch messageFlag {
	case 0x50, 0x51, 0x52, 0x53, 0x54, 0x56, 0x05B:
		content = genXevCarResponse(message)
	default:
		content = gen32960QuickResponse(message)
	}
	log.Info().Msgf("responseData.Bytes are: %x", content)
	err = conn.Send(content)
	return
}
