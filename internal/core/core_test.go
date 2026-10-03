package core

import (
	"encoding/json"
	"testing"
	"time"
)

func TestDigestNormalizesJSONAndRejectsChangedContent(t *testing.T) {
	a := NewEvent{Source: "shop", Type: "order.created", OccurredAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Payload: json.RawMessage(`{"a":1,"b":2}`)}
	b := a
	b.Payload = json.RawMessage(`{ "a": 1, "b": 2 }`)
	if Digest(a) != Digest(b) {
		t.Fatal("formatting changed digest")
	}
	b.Payload = json.RawMessage(`{"a":1,"b":3}`)
	if Digest(a) == Digest(b) {
		t.Fatal("changed data did not change digest")
	}
}

func TestRetryPolicy(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		result Result
		count  int
		want   string
	}{
		{Result{HTTPStatus: 200}, 1, "delivered"},
		{Result{HTTPStatus: 503, Retryable: true}, 1, "pending"},
		{Result{HTTPStatus: 429, Retryable: true}, 5, "dead"},
		{Result{HTTPStatus: 400}, 1, "dead"},
	} {
		got, next := Outcome(tc.result, tc.count, 5, now)
		if got != tc.want {
			t.Fatalf("got %s, want %s", got, tc.want)
		}
		if got == "pending" && !next.Equal(now.Add(2*time.Second)) {
			t.Fatalf("unexpected retry time %v", next)
		}
	}
	if NextDelay(100) != time.Minute {
		t.Fatal("backoff was not capped")
	}
}
