package main

import (
	"bufio"
	"context"
	"encoding/json"
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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
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

var (
	old7e = []byte{0x7e}
	new7e = []byte{0x7d, 0x02}
	old7d = []byte{0x7d}
	new7d = []byte{0x7d, 0x01}
)

var (
	logLevel         string
	platForm         string // 平台
	protocol         string // protocol
	consulAddr       string // consul
	enableMonitoring bool

	host        string
	hostAddress string

	commandPort int // grpc 命令下行服务端口
	serverType  string

	// log to file
	enableLogToFile bool // 输出到文件
	outputFile      *os.File
	writer          *bufio.Writer

	enableConfigFromFile bool
	enableConsul         bool

	// redis config
	redisHost        string
	redisPort        int
	redisPassword    string
	redisDB          int
	redisReadTimeout int

	kafkaBrokers []string
	normalTopic  string
	eventTopic   string
	errorTopic   string

	// network config
	port            int
	socketTimeout   int
	sendCommandPort int
	maxQps          int64
	maxConnection   int

	redisClient  *redis.Client
	consulClient *consulapi.Client
	nativeServer *network.NaiveServer
	producer     *confluentKafka.Producer

	signals = make(chan os.Signal)
)

var (
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

func readConfigFromFile() {
	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	err := viper.ReadInConfig()
	if err != nil {
		panic(err)
	}

	protocol = viper.GetString("protocol")

	kafkaBrokers = viper.GetStringSlice("kafka.brokerUrls")
	normalTopic = viper.GetString("kafka.normalTopic")
	eventTopic = viper.GetString("kafka.eventTopic") // 登录登出事件topic
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

	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	host = util.GetHost()
	if host == "" {
		log.Error().Msg("failed to get host name...")
	}

	var serverCmd = &cobra.Command{
		Use: "server",
		Run: func(cmd *cobra.Command, args []string) {
			startServer()
		},
	}

	serverCmd.Flags().BoolVarP(&enableConfigFromFile, "enable-config-from-file", "", true, "read config from file")
	serverCmd.Flags().BoolVarP(&enableConsul, "enable-config-from-consul", "", false, "read config from consul")
	serverCmd.Flags().StringVarP(&consulAddr, "consul", "", "", "consul address")
	serverCmd.Flags().IntVarP(&port, "port", "p", 11000, "port to listen")

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
	var err error
	hostAddress = util.GetLocalIP()
	if enableConfigFromFile {
		readConfigFromFile()
	}

	initConsul()

	global.CommandPort = commandPort
	global.TTL = socketTimeout
	log.Info().Msgf("enableConnReport: %v", enableConfigFromFile)
	log.Info().Msgf("enableConsul: %v", enableConsul)
	log.Info().Msgf("enableMonitoring: %v", enableMonitoring)
	log.Info().Msgf("protocol: %v", protocol)
	log.Info().Msgf("host address is: %s", hostAddress)
	log.Info().Msgf("serverType: %s", serverType)

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
			err = s.ListenAndServe()
			log.Info().Msgf("monitor serve error: %v", err)
		}()
	}
	producer, err = gateway.NewKafkaProducer(kafkaBrokers)
	if err != nil {
		log.Error().Msgf("fail to connect to kafka, error is: %v", err)
		panic(err)
	}
	redisClient = util.GetRedisClientWithTimeOut(redisHost, redisPort, redisPassword, redisDB, redisReadTimeout)

	if serverType == EpollServerType && runtime.GOOS == LinuxPlatform {
		// 配置启动epoll
		makeEpollServer()
	} else {
		makeServer()
	}
}

func makeEpollServer() {

	ctx, cancel := context.WithCancel(context.Background())
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
		StopContext:       ctx,
		NetworkMode:       gateway.ConfigNetworkFromEnv(),
	}

	//registerService() // 注册服务
	go func() {
		gateway.Serve(gatewayServerConfig)
	}()

	sig := <-signals
	cancel()
	log.Info().Msgf("exit signal: %v", sig)
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
		"bootstrap.servers":            kafkaBrokerString,
		"security.protocol":            "plaintext",
		"queue.buffering.max.messages": 200000,
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
	serverConfig := network.ServerConfig{
		Address:       fmt.Sprintf("0.0.0.0:%d", port),
		Timeout:       socketTimeout,
		MaxQps:        maxQps,
		MaxConnection: maxConnection,
	}
	nativeServer = network.NewNativeServer(&serverConfig)
	nativeServer.RegisterCallbacks(connectionMade, connectionLost, messageReceived)
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
	//registerService() // 服务注册
	//go startCheckServer() // 健康检查
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

// 登录登出事件
func CommitEvent(vin string, eventType int) {
	dataMap := make(map[string]interface{})
	dataMap["vin"] = vin
	dataMap["eventType"] = eventType
	dataMap["timestamp"] = time.Now().Unix()
	bs, err := json.Marshal(dataMap)
	if err != nil {
		log.Info().Msgf("marshall login event json error: %v", err)
		return
	}

	err = gateway.Produce(producer, eventTopic, bs)
	if err != nil {
		log.Info().Msgf("produce event json error: %v", err)
		return
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

	log.Info().Msgf("Receive segment: %x from %v", segment, c)
	if len(c.ResidueBytes) > 0 {
		segment = append(c.ResidueBytes, segment...)
	}
	messages, residueBytes, invalidMessages := gateway.SplitMessage(segment)
	c.ResidueBytes = residueBytes
	for _, message := range messages {
		p, err := pkg.DeconstractPackage(message, protocol)
		if err != nil {
			log.Error().Msgf("error: %v", err)
		}

		vin := string(p.UniqueCode())
		err = pkg.VerifyUid(p.UniqueCode())
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

		if vin != pkg.DefaultUid && vin != "" {
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
