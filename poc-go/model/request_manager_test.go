package model

import "testing"

func TestAddRequestInitializesResponseChannel(t *testing.T) {
	manager := &RequestManager{Requests: make(map[string]Request)}

	req := Request{CorrelationId: "corr-123"}
	ch := manager.AddRequest(req)

	if ch == nil {
		t.Fatal("expected response channel to be initialized")
	}

	stored, exists := manager.Requests["corr-123"]
	if !exists {
		t.Fatal("expected request to be stored in the manager")
	}

	if stored.Response == nil {
		t.Fatal("expected stored request to carry a response channel")
	}
}

func TestResolveRequestPublishesResponse(t *testing.T) {
	manager := &RequestManager{Requests: make(map[string]Request)}

	ch := manager.AddRequest(Request{CorrelationId: "corr-456"})
	if !manager.ResolveRequest("corr-456", "SUCCESS") {
		t.Fatal("expected request to be resolved")
	}

	select {
	case response := <-ch:
		if response != "SUCCESS" {
			t.Fatalf("expected SUCCESS response, got %s", response)
		}
	default:
		t.Fatal("expected response to be published to the channel")
	}
}
