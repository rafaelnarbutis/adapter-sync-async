package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"poc-go/adapter"
	adapterKafka "poc-go/adapter/kafka"
	"poc-go/model"
)

func main() {
	requestManager := &model.RequestManager{
		Requests: make(map[string]model.Request),
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go adapterKafka.Receive(ctx, requestManager)

	if err := adapter.StartServer(ctx, requestManager); err != nil {
		log.Fatalf("failed to start server: %v", err)
	}
}
