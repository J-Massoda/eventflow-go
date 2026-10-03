package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/J-Massoda/eventflow-go/internal/core"
	"github.com/J-Massoda/eventflow-go/internal/memory"
)

func TestHTTPDeliveryRetryAndIdempotency(t *testing.T) {
	var mu sync.Mutex
	count := 0
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if r.Header.Get("X-Event-Signature") != Signature("test-secret", body) {
			t.Error("bad signature")
		}
		if r.Header.Get("Idempotency-Key") != r.Header.Get("X-Event-ID") {
			t.Error("unstable delivery key")
		}
		mu.Lock()
		count++
		n := count
		mu.Unlock()
		if n == 1 {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(204)
	}))
	defer sink.Close()
	store := memory.New()
	s := New(store, Config{APIKey: "long-enough-api-key", SinkURL: sink.URL, SinkSecret: "test-secret", PollEvery: 10 * time.Millisecond, Workers: 3})
	server := httptest.NewServer(s.Handler())
	defer server.Close()
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	go s.Run(ctx)
	url := server.URL + "/v1/events"
	body := `{"source":"shop","type":"order.created","occurred_at":"2026-01-01T00:00:00Z","payload":{"order_id":"A1"}}`
	post := func(b, key string) (int, map[string]any) {
		req, _ := http.NewRequest("POST", url, strings.NewReader(b))
		req.Header.Set("Authorization", "Bearer long-enough-api-key")
		req.Header.Set("Idempotency-Key", key)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var data map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&data)
		return resp.StatusCode, data
	}
	code, first := post(body, "unique-order-A1")
	if code != 202 {
		t.Fatalf("first: %d %+v", code, first)
	}
	code, second := post(body, "unique-order-A1")
	if code != 200 || first["id"] != second["id"] {
		t.Fatalf("retry: %d %+v", code, second)
	}
	code, _ = post(strings.Replace(body, "A1", "B2", 1), "unique-order-A1")
	if code != 409 {
		t.Fatalf("conflict: %d", code)
	}
	if resp, _ := http.Post(url, "application/json", bytes.NewBufferString(body)); resp.StatusCode != 401 {
		t.Fatalf("missing auth: %d", resp.StatusCode)
	}
	id := first["id"].(string)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		req, _ := http.NewRequest("GET", fmt.Sprintf("%s/v1/events/%s", server.URL, id), nil)
		req.Header.Set("Authorization", "Bearer long-enough-api-key")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		var d core.Detail
		_ = json.NewDecoder(resp.Body).Decode(&d)
		resp.Body.Close()
		if d.Event.Status == "delivered" {
			if d.Event.Attempts != 2 || len(d.Attempts) != 2 || d.Attempts[0].HTTPStatus != 503 || d.Attempts[1].HTTPStatus != 204 {
				t.Fatalf("incorrect delivery history: %+v", d)
			}
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatal("event did not retry to delivery")
}

func TestPermanentFailureDies(t *testing.T) {
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(400) }))
	defer sink.Close()
	s := New(memory.New(), Config{APIKey: "long-enough-api-key", SinkSecret: "test-secret", SinkURL: sink.URL})
	e, _, _ := s.Store.Insert(context.Background(), core.NewEvent{Source: "shop", Type: "order.created", IdempotencyKey: "key-1234", OccurredAt: time.Now(), Payload: json.RawMessage(`{"a":1}`)})
	claimed, ok, _ := s.Store.Claim(context.Background(), time.Now().Add(time.Second), time.Minute)
	if !ok {
		t.Fatal("claim failed")
	}
	result := s.deliver(context.Background(), claimed)
	if result.Retryable {
		t.Fatal("HTTP 400 must not retry")
	}
	if err := s.Store.Complete(context.Background(), claimed, result, time.Now(), 5); err != nil {
		t.Fatal(err)
	}
	d, _ := s.Store.Get(context.Background(), e.ID)
	if d.Event.Status != "dead" || d.Event.Attempts != 1 {
		t.Fatalf("wrong state: %+v", d)
	}
}
