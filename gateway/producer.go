package gateway

import (
	"github.com/rs/zerolog/log"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

const (
	producerThreshold = 150000
	flushTimeout      = 3 * 1000
)

// Produce produce package to kafka
func Produce(producer *kafka.Producer, topic string, message []byte) error {
	if producer.Len() > producerThreshold {
		log.Info().Msgf("size of waiting queue is too big: %v", producer.Len())
		producer.Flush(flushTimeout)
		log.Info().Msgf("after flush: %v", producer.Len())
	}
	//log.Info().Msgf("size of waiting queue is: %v", producer.Len())
	err := producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: message,
	}, nil)
	return err
}

// Produce produce package to kafka
func ProduceWithKey(producer *kafka.Producer, topic string, message []byte, key []byte) error {
	if producer.Len() > producerThreshold {
		log.Info().Msgf("size of waiting queue is too big: %v", producer.Len())
		producer.Flush(flushTimeout)
		log.Info().Msgf("after flush: %v", producer.Len())
	}

	err := producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key:   key,
		Value: message,
	}, nil)
	return err
}
