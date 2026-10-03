package core

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

var ErrNotFound = errors.New("event not found")
var ErrIdempotencyConflict = errors.New("idempotency key belongs to a different event")
var ErrLeaseLost = errors.New("delivery lease lost")

type NewEvent struct {
	Source         string          `json:"source"`
	Type           string          `json:"type"`
	OccurredAt     time.Time       `json:"occurred_at"`
	Payload        json.RawMessage `json:"payload"`
	IdempotencyKey string          `json:"-"`
}

type Event struct {
	ID             string          `json:"id"`
	Source         string          `json:"source"`
	Type           string          `json:"type"`
	OccurredAt     time.Time       `json:"occurred_at"`
	Payload        json.RawMessage `json:"payload"`
	Status         string          `json:"status"`
	Attempts       int             `json:"attempts"`
	NextAttemptAt  time.Time       `json:"next_attempt_at,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	DeliveredAt    *time.Time      `json:"delivered_at,omitempty"`
	LastError      string          `json:"last_error,omitempty"`
	IdempotencyKey string          `json:"-"`
	Digest         string          `json:"-"`
	LeaseToken     string          `json:"-"`
}

type Attempt struct {
	Number     int       `json:"number"`
	Time       time.Time `json:"time"`
	HTTPStatus int       `json:"http_status,omitempty"`
	Outcome    string    `json:"outcome"`
	Error      string    `json:"error,omitempty"`
}

type Detail struct {
	Event    Event     `json:"event"`
	Attempts []Attempt `json:"delivery_attempts"`
}

type Result struct {
	HTTPStatus int
	Error      string
	Retryable  bool
}

// Store must atomically deduplicate, lease, and complete events. A stale
// worker cannot complete an event after another worker has reclaimed its lease.
type Store interface {
	Insert(context.Context, NewEvent) (Event, bool, error)
	Get(context.Context, string) (Detail, error)
	Claim(context.Context, time.Time, time.Duration) (Event, bool, error)
	Complete(context.Context, Event, Result, time.Time, int) error
}

func NewID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func Digest(n NewEvent) string {
	// The API normalizes a JSON value before hashing. Whitespace in requests
	// will not make a legitimate idempotent retry conflict.
	b, _ := json.Marshal(struct {
		Source     string          `json:"source"`
		Type       string          `json:"type"`
		OccurredAt time.Time       `json:"occurred_at"`
		Payload    json.RawMessage `json:"payload"`
	}{n.Source, n.Type, n.OccurredAt, n.Payload})
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func NextDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 7 {
		attempt = 7
	}
	delay := time.Second * 2 * time.Duration(1<<uint(attempt-1))
	if delay > time.Minute {
		return time.Minute
	}
	return delay
}

func Outcome(r Result, attempts, maxAttempts int, now time.Time) (string, time.Time) {
	if r.Error == "" && r.HTTPStatus >= 200 && r.HTTPStatus < 300 {
		return "delivered", time.Time{}
	}
	if !r.Retryable || attempts >= maxAttempts {
		return "dead", time.Time{}
	}
	return "pending", now.Add(NextDelay(attempts))
}

func Validate(n NewEvent) error {
	if len(n.IdempotencyKey) < 4 || len(n.IdempotencyKey) > 128 {
		return errors.New("Idempotency-Key must contain 4 to 128 characters")
	}
	if !validName(n.Source) || !validName(n.Type) {
		return errors.New("source and type must be 1 to 80 letters, numbers, dots, hyphens or underscores")
	}
	if n.OccurredAt.IsZero() {
		return errors.New("occurred_at must be an RFC3339 timestamp")
	}
	payload := bytes.TrimSpace(n.Payload)
	if !json.Valid(payload) || len(payload) == 0 || payload[0] != '{' {
		return errors.New("payload must be a JSON object")
	}
	return nil
}

func validName(s string) bool {
	if len(s) < 1 || len(s) > 80 {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

func SafeError(r Result) string {
	if r.Error != "" {
		return r.Error
	}
	return fmt.Sprintf("receiver returned HTTP %d", r.HTTPStatus)
}
