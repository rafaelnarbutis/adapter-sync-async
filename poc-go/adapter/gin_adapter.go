package adapter

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"poc-go/model"
	"poc-go/service"
	"time"

	"github.com/gin-gonic/gin"
)

func StartServer(ctx context.Context, requestManager *model.RequestManager) error {
	r := gin.Default()

	r.POST("/v1/simulate", func(c *gin.Context) {
		body, err := c.GetRawData()
		if err != nil {
			c.String(400, "Failed to read request body")
			return
		}

		correlationID := c.GetHeader("X-Correlation-ID")
		if correlationID == "" {
			correlationID = fmt.Sprintf("req-%d", time.Now().UnixNano())
		}

		request := model.Request{
			Body:          string(body),
			DateTime:      time.Now(),
			CorrelationId: correlationID,
		}

		responseCh := requestManager.AddRequest(request)
		log.Printf("Received request with Correlation ID: %s", request.CorrelationId)

		go service.ProcessRequest(request)

		select {
		case response := <-responseCh:
			log.Printf("Request completed for correlation ID: %s", request.CorrelationId)
			c.String(200, response)
		case <-time.After(10 * time.Second):
			log.Printf("Timed out waiting for Kafka response for correlation ID: %s", request.CorrelationId)
			requestManager.RemoveRequest(request.CorrelationId)
			c.String(504, "timeout waiting for Kafka response")
		case <-ctx.Done():
			c.String(503, "server shutting down")
		}
	})

	srv := &http.Server{
		Addr:    ":8081",
		Handler: r,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("server shutdown error: %v", err)
		}
	}()

	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	return nil
}
