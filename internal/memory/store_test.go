package memory

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/J-Massoda/eventflow-go/internal/core"
)

func TestConcurrentIdempotencyAndLeases(t *testing.T) {
	s := New()
	ctx := context.Background()
	n := core.NewEvent{Source: "shop", Type: "order.created", IdempotencyKey: "order-123",
		OccurredAt: time.Now().UTC(), Payload: json.RawMessage(`{"order_id":"123"}`)}
	ids := make(chan string, 24)
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e, _, err := s.Insert(ctx, n)
			if err != nil {
				t.Error(err)
				return
			}
			ids <- e.ID
		}()
	}
	wg.Wait()
	close(ids)
	var id string
	for got := range ids {
		if id == "" {
			id = got
		}
		if got != id {
			t.Fatal("duplicate events created")
		}
	}
	changed := n
	changed.Payload = json.RawMessage(`{"order_id":"456"}`)
	if _, _, err := s.Insert(ctx, changed); !errors.Is(err, core.ErrIdempotencyConflict) {
		t.Fatalf("wanted conflict, got %v", err)
	}
	e, ok, err := s.Claim(ctx, time.Now().Add(time.Second), time.Second)
	if err != nil || !ok {
		t.Fatalf("claim: %v, %v", ok, err)
	}
	if _, ok, _ := s.Claim(ctx, time.Now().Add(time.Second), time.Second); ok {
		t.Fatal("same lease claimed twice")
	}
	stale := e
	fresh, ok, err := s.Claim(ctx, time.Now().Add(3*time.Second), time.Second)
	if err != nil || !ok || fresh.Attempts != 2 {
		t.Fatalf("reclaim: %+v, %v", fresh, err)
	}
	if err := s.Complete(ctx, stale, core.Result{HTTPStatus: 200}, time.Now(), 5); !errors.Is(err, core.ErrLeaseLost) {
		t.Fatalf("stale completion: %v", err)
	}
	if err := s.Complete(ctx, fresh, core.Result{HTTPStatus: 200}, time.Now(), 5); err != nil {
		t.Fatal(err)
	}
	detail, err := s.Get(ctx, id)
	if err != nil || detail.Event.Status != "delivered" || len(detail.Attempts) != 1 {
		t.Fatalf("detail: %+v, %v", detail, err)
	}
}
