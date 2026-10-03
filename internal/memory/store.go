package memory

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/J-Massoda/eventflow-go/internal/core"
)

// Store is an ephemeral implementation for demos and deterministic tests.
type Store struct {
	mu       sync.Mutex
	events   map[string]*record
	keys     map[string]string
	sequence []string
}

type record struct {
	event      core.Event
	attempts   []core.Attempt
	leaseUntil time.Time
}

func New() *Store {
	return &Store{events: map[string]*record{}, keys: map[string]string{}}
}

func (s *Store) Insert(_ context.Context, n core.NewEvent) (core.Event, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	digest := core.Digest(n)
	key := n.Source + "\x00" + n.IdempotencyKey
	if id, ok := s.keys[key]; ok {
		if s.events[id].event.Digest != digest {
			return core.Event{}, false, core.ErrIdempotencyConflict
		}
		return copyEvent(s.events[id].event), true, nil
	}
	id, err := core.NewID()
	if err != nil {
		return core.Event{}, false, err
	}
	now := time.Now().UTC()
	e := core.Event{ID: id, Source: n.Source, Type: n.Type, OccurredAt: n.OccurredAt,
		Payload: append(json.RawMessage(nil), n.Payload...), Status: "pending", NextAttemptAt: now,
		CreatedAt: now, IdempotencyKey: n.IdempotencyKey, Digest: digest}
	s.events[id] = &record{event: e}
	s.keys[key] = id
	s.sequence = append(s.sequence, id)
	return copyEvent(e), false, nil
}

func (s *Store) Get(_ context.Context, id string) (core.Detail, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.events[id]
	if !ok {
		return core.Detail{}, core.ErrNotFound
	}
	a := append([]core.Attempt(nil), r.attempts...)
	return core.Detail{Event: copyEvent(r.event), Attempts: a}, nil
}

func (s *Store) Claim(_ context.Context, now time.Time, lease time.Duration) (core.Event, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range s.sequence {
		r := s.events[id]
		if r.event.Status == "pending" && !r.event.NextAttemptAt.After(now) ||
			r.event.Status == "processing" && !r.leaseUntil.After(now) {
			token, err := core.NewID()
			if err != nil {
				return core.Event{}, false, err
			}
			r.event.Status = "processing"
			r.event.Attempts++
			r.event.NextAttemptAt = time.Time{}
			r.event.LeaseToken = token
			r.leaseUntil = now.Add(lease)
			return copyEvent(r.event), true, nil
		}
	}
	return core.Event{}, false, nil
}

func (s *Store) Complete(_ context.Context, claimed core.Event, result core.Result, now time.Time, maxAttempts int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.events[claimed.ID]
	if !ok || r.event.Status != "processing" || r.event.LeaseToken != claimed.LeaseToken {
		return core.ErrLeaseLost
	}
	status, next := core.Outcome(result, r.event.Attempts, maxAttempts, now)
	errText := ""
	if status != "delivered" {
		errText = core.SafeError(result)
	}
	r.attempts = append(r.attempts, core.Attempt{Number: r.event.Attempts, Time: now,
		HTTPStatus: result.HTTPStatus, Outcome: status, Error: errText})
	r.event.Status = status
	r.event.NextAttemptAt = next
	r.event.LastError = errText
	r.event.LeaseToken = ""
	r.leaseUntil = time.Time{}
	if status == "delivered" {
		r.event.DeliveredAt = &now
	}
	return nil
}

func copyEvent(e core.Event) core.Event {
	e.Payload = append(json.RawMessage(nil), e.Payload...)
	return e
}
