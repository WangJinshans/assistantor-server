package gateway

import (
	"encoding/json"
	"github.com/rs/zerolog/log"
	"time"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

const (
	producerThreshold = 150000
	flushTimeout      = 3 * 1000
)

func FormatErrorMsg(source, content, ip, errorMessage string) []byte {
	t := time.Now().Unix()

	data := map[string]interface{}{
		"source":        source,
		"message":       content,
		"ip":            ip,
		"error_message": errorMessage,
		"create_time":   t,
	}
	msg, err := json.Marshal(data)
	if err != nil {
		log.Error().Msg(err.Error())
	}
	return msg
}

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
