package gateway

import (
	"github.com/rs/zerolog/log"
	confluentKafka "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"strings"
)

func NewKafkaProducer(kafkaBrokers []string) (producer *confluentKafka.Producer, err error) {
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
		//"message.max.bytes":            3000000, // message.max.bytes 作用于全局,慎用
		"batch.size": 1000000,
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
				} else {
					producedPackages.Observe(1)
				}
			}
		}
	}()
	return
}
