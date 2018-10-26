package kafkawriter

import (
	"context"
	"fmt"
	"net"

	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

// KafkaWriter holds the write connection to Kafka
type KafkaWriter struct {
	*kafka.Writer
}

// Create is the factory function to create KafkaWriter instance
func Create(broker string, topic string) KafkaWriter {
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker},
		Topic:   topic,
	})
	return KafkaWriter{w}
}

// Send a message to Kafka via the writer
func (w KafkaWriter) Send(msg interface{}) error {
	var byteMsg []byte

	switch v := msg.(type) {
	case string:
		byteMsg = []byte(v)
	case []byte:
		byteMsg = v
	default:
		return fmt.Errorf("Message is neiter string nor []byte: %+v", msg)
	}

	if err := w.WriteMessages(context.Background(), kafka.Message{Value: byteMsg}); err != nil {
		switch err.(type) {
		case *net.OpError:
			log.Panic(err)
		default:
			return err
		}
	}
	return nil
}

// Close Kafka writer
func (w KafkaWriter) Close() {
	w.Close()
}
