package gateway

import (
	"github.com/rs/zerolog/log"
	confluentKafka "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"math/rand"
)

// MakeProducer make a kafak producer
func MakeProducer(kafkaBrokers []string, producerBufferSize int) *confluentKafka.Producer {
	log.Info().Msgf("broker list: %v", kafkaBrokers)
	randomIndex := rand.Intn(len(kafkaBrokers))
	pick := kafkaBrokers[randomIndex]
	log.Info().Msgf("connect to kafka at %v", pick)
	producer, err := confluentKafka.NewProducer(&confluentKafka.ConfigMap{
		"bootstrap.servers":            pick,
		"security.protocol":            "plaintext",
		//"ssl.ca.location":              "./ca-cert",
		"queue.buffering.max.messages": producerBufferSize,
		"go.events.channel.size":       producerBufferSize,
		"go.batch.producer":            true,
		"go.produce.channel.size":      producerBufferSize,
	})
	if err != nil {
		panic(err)
	}

	return producer
}

func monitorProducer(producer *confluentKafka.Producer) {
	for range producer.Events() {
	}
	// for e := range producer.Events() {
		// switch ev := e.(type) {
		// case *confluentKafka.Message:
		// 	if ev.TopicPartition.Error != nil {
		// 		log.Error().Msgf("ev: %v", ev.TopicPartition.Error)
		// 		errorPackages.Observe(bufferSizePerVIN)
		// 	} else {
		// 		producedPackages.Observe(bufferSizePerVIN)
		// 	}
		// }
	// }
}
