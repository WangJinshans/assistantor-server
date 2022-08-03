package gateway

import (
	// "bytes"
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"runtime/debug"

	"github.com/rs/zerolog/log"
	confluentKafka "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

var (
	kafkaChan = make(chan []byte, kafkaBatchSize*100)

	bufferByVIN = make(map[string][][]byte, 20000)
	bufferLock  sync.Mutex

	splitFlag = []byte(",'")

	globalProducer *confluentKafka.Producer
	globalTopic    string
	errorTopic    string
)

const (
	NetworkModeNaive = iota
	NetworkModeEpoll

	kafkaBatchSize   = 100
	pakcageMaxLength = 100

	bufferSizePerVIN = 20
)

// GatewayServerConfig contain gateway config
type GatewayServerConfig struct {
	GatewayPort   int
	SocketTimeout int

	CommandSenderPort int

	RedisClient *redis.Client

	Protocol string

	Producer    *confluentKafka.Producer
	NormalTopic string
	ErrorTopic  string

	EnableMonitoring bool

	StopContext context.Context

	EnableSpider bool

	// handle command response directly
	EnableCommandResponseHandle bool

	NetworkMode int
}

// Serve create a server with given config
func Serve(config GatewayServerConfig) {
	addr := fmt.Sprintf("0.0.0.0:%d", config.GatewayPort)
	debug.SetMaxThreads(1000000)

	if config.EnableMonitoring {
		prometheusRegiste()
		mux := http.NewServeMux()
		// Monitering
		mux.Handle("/metrics", promhttp.Handler())
		// AttachProfiler(mux)

		s := &http.Server{
			Addr:           ":8080",
			Handler:        mux,
			ReadTimeout:    1000 * time.Second,
			WriteTimeout:   1000 * time.Second,
			MaxHeaderBytes: 1 << 20,
		}

		go func() {
			log.Fatal().Err(s.ListenAndServe())
		}()
	}

	serverConfig := ServerConfig{
		Address: addr,
		Timeout: config.SocketTimeout,
	}
	server := NewEpollServer(&serverConfig)
	globalProducer = config.Producer
	globalTopic = config.NormalTopic
	errorTopic = config.ErrorTopic

	// monitoring kafka messages
	// go monitorProducer(config.Producer)
	go StartEpollCommandServer(10010, server, config.RedisClient)
	log.Info().Msgf("gateway serve at %s", addr)
	server.Listen()
}


func (config *GatewayServerConfig) produce() {
	for {
		i := 0
		message := make([]byte, 0, kafkaBatchSize*pakcageMaxLength)
		for p := range kafkaChan {
			message = append(message, p...)
			i++

			if i == kafkaBatchSize {
				break
			}
		}

		err := Produce(config.Producer, config.NormalTopic, message)
		if err != nil {
			log.Error().Msgf("err: %v, queue size: %v", err, config.Producer.Len())
		} else {
			enqueuedPackages.Observe(1)
		}
	}
}

func (config *GatewayServerConfig) produceByVIN() {
	for {
		bufferLock.Lock()
		for vin, buf := range bufferByVIN {
			if len(buf) >= bufferSizePerVIN {
				var aggregateMessage []byte
				for _, message := range buf {
					aggregateMessage = append(aggregateMessage, message...)
				}

				err := Produce(config.Producer, config.NormalTopic, aggregateMessage)
				if err != nil {
					log.Error().Msgf("err: %v, queue size: %v", err, config.Producer.Len())
				}

				enqueuedPackages.Observe(float64(len(buf)))

				// clear buf
				bufferByVIN[vin] = nil
			}
		}
		bufferLock.Unlock()

		time.Sleep(2 * time.Second)
	}
}
