package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/J-Massoda/eventflow-go/internal/core"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Set TEST_DATABASE_URL to a disposable database whose name ends in _test.
func TestPostgresAtomicityAndLeaseRecovery(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set TEST_DATABASE_URL to test PostgreSQL persistence")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var name string
	if err := pool.QueryRow(ctx, `SELECT current_database()`).Scan(&name); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(name, "_test") {
		t.Fatal("integration test requires a dedicated *_test database")
	}
	s := &Store{Pool: pool}
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE delivery_attempts, events`); err != nil {
		t.Fatal(err)
	}
	n := core.NewEvent{Source: "integration", Type: "order.created", IdempotencyKey: "same-1234",
		OccurredAt: time.Now().UTC(), Payload: json.RawMessage(`{"id":"A"}`)}
	var wg sync.WaitGroup
	ids := make(chan string, 12)
	for i := 0; i < 12; i++ {
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
			t.Fatal("idempotency created two events")
		}
	}
	if id == "" {
		t.Fatal("insert did not return an event")
	}
	changed := n
	changed.Payload = json.RawMessage(`{"id":"B"}`)
	if _, _, err := s.Insert(ctx, changed); !errors.Is(err, core.ErrIdempotencyConflict) {
		t.Fatalf("changed content: %v", err)
	}
	now := time.Now().Add(time.Second)
	e, ok, err := s.Claim(ctx, now, time.Second)
	if err != nil || !ok {
		t.Fatalf("first claim: %v", err)
	}
	if _, ok, err = s.Claim(ctx, now, time.Second); err != nil || ok {
		t.Fatalf("double claim: %v, %v", ok, err)
	}
	fresh, ok, err := s.Claim(ctx, now.Add(2*time.Second), time.Second)
	if err != nil || !ok || fresh.Attempts != 2 {
		t.Fatalf("lease recovery: %+v %v", fresh, err)
	}
	if err := s.Complete(ctx, e, core.Result{HTTPStatus: 200}, time.Now(), 5); !errors.Is(err, core.ErrLeaseLost) {
		t.Fatalf("stale lease completed: %v", err)
	}
	if err := s.Complete(ctx, fresh, core.Result{HTTPStatus: 503, Retryable: true}, time.Now(), 5); err != nil {
		t.Fatal(err)
	}
	d, err := s.Get(ctx, id)
	if err != nil || d.Event.Status != "pending" || d.Event.Attempts != 2 || len(d.Attempts) != 1 {
		t.Fatalf("wrong state: %+v %v", d, err)
	}
	third, ok, err := s.Claim(ctx, time.Now().Add(time.Minute), time.Second)
	if err != nil || !ok {
		t.Fatalf("retry claim: %v", err)
	}
	if err := s.Complete(ctx, third, core.Result{HTTPStatus: 200}, time.Now(), 5); err != nil {
		t.Fatal(err)
	}
	d, err = s.Get(ctx, id)
	if err != nil || d.Event.Status != "delivered" || len(d.Attempts) != 2 {
		t.Fatalf("wrong final state: %+v %v", d, err)
	}
}
