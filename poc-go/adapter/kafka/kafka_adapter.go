package adapter

import (
	"context"
	"errors"
	"fmt"
	"log"
	"poc-go/model"
	"time"

	"github.com/segmentio/kafka-go"
)

const kafkaUrl string = "localhost:9092"
const topicName string = "command"

var writer = &kafka.Writer{
	Addr:     kafka.TCP(kafkaUrl),
	Topic:    topicName,
	Balancer: &kafka.LeastBytes{},
}

func Send(object string, correlationID string) error {
	return writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(correlationID),
		Value: []byte(object),
	})
}

func Receive(ctx context.Context, requestManager *model.RequestManager) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaUrl},
		GroupID:  "consumerGroup",
		Topic:    topicName,
		MinBytes: 10e3,
		MaxBytes: 10e6,
		MaxWait:  1 * time.Second,
	})

	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("Failed to close reader: %v", err)
		}
	}()

	log.Println("Kafka consumer started. Listening for messages...")

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				log.Println("Shutdown signal received. Exiting loop.")
				break
			}
			log.Printf("Error while reading message: %v", err)
			continue
		}

		correlationID := string(msg.Key)
		_, exists := requestManager.GetRequest(correlationID)
		if !exists {
			log.Printf("No request found for Correlation ID: %s", correlationID)
			continue
		}

		log.Printf("Processing message for Correlation ID: %s", correlationID)
		fmt.Printf("Message received: %s\n", string(msg.Value))

		if requestManager.ResolveRequest(correlationID, "SUCCESS") {
			log.Printf("Resolved request for Correlation ID: %s", correlationID)
		} else {
			log.Printf("Could not resolve pending request for Correlation ID: %s", correlationID)
		}

		requestManager.RemoveRequest(correlationID)
	}

	log.Println("Consumer shut down gracefully.")
}
