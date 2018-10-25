package kafkaprod

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
)

// KafkaProducer holds topic and producer
type KafkaProducer struct {
	topic    string
	producer *kafka.Producer
}

// Create is the factory function to create KafkaProducer instance
func Create(broker string, topic string) KafkaProducer {
	var producer *kafka.Producer
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": broker})
	if err != nil {
		log.Panic("Cannot connect to Kafka")
	}

	// Delivery report handler for produced messages
	go func() {
		for e := range producer.Events() {

			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.WithFields(log.Fields{"topic": topic}).Error(fmt.Sprintf("Delivery failed: %v\n    %s\n", ev.TopicPartition, string(ev.Value)))
				} else {
					log.WithFields(log.Fields{"topic": topic}).Info(fmt.Sprintf("Delivered message to %v\n", ev.TopicPartition))
				}

			default:
				fmt.Println(ev)
				panic(ev)
			}
		}
	}()
	log.WithFields(log.Fields{"topic": topic}).Info("Producer created")
	return KafkaProducer{topic, producer}
}

// Send bytes to a kafka topic
func (p KafkaProducer) Send(bytes []byte) {
	log.WithFields(log.Fields{"topic": p.topic}).Debug(fmt.Sprintf("Sending message %s", string(bytes)))

	p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
		Value:          bytes,
	}, nil)

}
