package model

import (
	"sync"
	"time"
)

type RequestManager struct {
	mu       sync.RWMutex
	Requests map[string]Request
}

type Request struct {
	Body          string
	DateTime      time.Time
	CorrelationId string
	Response      chan string
}

func (rm *RequestManager) AddRequest(request Request) chan string {
	if request.Response == nil {
		request.Response = make(chan string, 1)
	}

	rm.mu.Lock()
	rm.Requests[request.CorrelationId] = request
	rm.mu.Unlock()
	return request.Response
}

func (rm *RequestManager) GetRequest(correlationId string) (Request, bool) {
	rm.mu.RLock()
	request, exists := rm.Requests[correlationId]
	rm.mu.RUnlock()
	return request, exists
}

func (rm *RequestManager) ResolveRequest(correlationId string, response string) bool {
	rm.mu.RLock()
	request, exists := rm.Requests[correlationId]
	rm.mu.RUnlock()

	if !exists || request.Response == nil {
		return false
	}

	select {
	case request.Response <- response:
		return true
	default:
		return true
	}
}

func (rm *RequestManager) RemoveRequest(correlationId string) {
	rm.mu.Lock()
	delete(rm.Requests, correlationId)
	rm.mu.Unlock()
}
