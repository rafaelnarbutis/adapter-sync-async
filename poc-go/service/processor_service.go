package service

import (
	"log"
	adapter "poc-go/adapter/kafka"
	"poc-go/model"
)

func ProcessRequest(request model.Request) {
	log.Printf("Processing request with Correlation ID: %s", request.CorrelationId)
	log.Printf("Request Body: %s", request.Body)

	if err := adapter.Send(request.Body, request.CorrelationId); err != nil {
		log.Printf("Failed to send message to Kafka: %v", err)
		return
	}

	log.Printf("Finished processing request with Correlation ID: %s", request.CorrelationId)
}
