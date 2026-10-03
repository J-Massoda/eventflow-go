package postgres

import (
	"context"
	"embed"
	"errors"
	"time"

	"github.com/J-Massoda/eventflow-go/internal/core"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var migrations embed.FS

type Store struct{ Pool *pgxpool.Pool }

func (s *Store) Migrate(ctx context.Context) error {
	b, err := migrations.ReadFile("schema.sql")
	if err != nil {
		return err
	}
	_, err = s.Pool.Exec(ctx, string(b))
	return err
}

const eventColumns = `id, source, event_type, occurred_at, payload, status, attempts,
    next_attempt_at, created_at, delivered_at, last_error, idempotency_key, digest, lease_token`

func scanEvent(row pgx.Row) (core.Event, error) {
	var e core.Event
	var next, delivered *time.Time
	var lease *string
	err := row.Scan(&e.ID, &e.Source, &e.Type, &e.OccurredAt, &e.Payload, &e.Status,
		&e.Attempts, &next, &e.CreatedAt, &delivered, &e.LastError, &e.IdempotencyKey, &e.Digest, &lease)
	if next != nil {
		e.NextAttemptAt = *next
	}
	e.DeliveredAt = delivered
	if lease != nil {
		e.LeaseToken = *lease
	}
	return e, err
}

func (s *Store) Insert(ctx context.Context, n core.NewEvent) (core.Event, bool, error) {
	id, err := core.NewID()
	if err != nil {
		return core.Event{}, false, err
	}
	digest := core.Digest(n)
	// The conflict target is unique; the first insert wins even under concurrent
	// requests. A repeated key with different content is rejected below.
	e, err := scanEvent(s.Pool.QueryRow(ctx, `INSERT INTO events
        (id, source, event_type, occurred_at, payload, idempotency_key, digest, status, next_attempt_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,'pending',now())
        ON CONFLICT (source,idempotency_key) DO UPDATE SET idempotency_key=events.idempotency_key
        RETURNING `+eventColumns, id, n.Source, n.Type, n.OccurredAt, n.Payload, n.IdempotencyKey, digest))
	if err != nil {
		return core.Event{}, false, err
	}
	if e.Digest != digest {
		return core.Event{}, false, core.ErrIdempotencyConflict
	}
	return e, e.ID != id, nil
}

func (s *Store) Get(ctx context.Context, id string) (core.Detail, error) {
	e, err := scanEvent(s.Pool.QueryRow(ctx, `SELECT `+eventColumns+` FROM events WHERE id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return core.Detail{}, core.ErrNotFound
	}
	if err != nil {
		return core.Detail{}, err
	}
	rows, err := s.Pool.Query(ctx, `SELECT attempt_number,attempted_at,http_status,outcome,error_message
        FROM delivery_attempts WHERE event_id=$1 ORDER BY attempt_number`, id)
	if err != nil {
		return core.Detail{}, err
	}
	defer rows.Close()
	d := core.Detail{Event: e, Attempts: []core.Attempt{}}
	for rows.Next() {
		var a core.Attempt
		if err := rows.Scan(&a.Number, &a.Time, &a.HTTPStatus, &a.Outcome, &a.Error); err != nil {
			return core.Detail{}, err
		}
		d.Attempts = append(d.Attempts, a)
	}
	return d, rows.Err()
}

func (s *Store) Claim(ctx context.Context, now time.Time, lease time.Duration) (core.Event, bool, error) {
	token, err := core.NewID()
	if err != nil {
		return core.Event{}, false, err
	}
	// SKIP LOCKED lets multiple workers claim different events. A crash leaves
	// an expiring lease, so another worker can safely retry delivery.
	e, err := scanEvent(s.Pool.QueryRow(ctx, `WITH claim AS (
        SELECT id FROM events WHERE
          (status='pending' AND next_attempt_at <= $1)
          OR (status='processing' AND lease_until <= $1)
        ORDER BY created_at,id LIMIT 1 FOR UPDATE SKIP LOCKED
      ) UPDATE events SET status='processing', attempts=attempts+1,
        next_attempt_at=NULL, lease_until=$2, lease_token=$3
      WHERE id=(SELECT id FROM claim) RETURNING `+eventColumns, now, now.Add(lease), token))
	if errors.Is(err, pgx.ErrNoRows) {
		return core.Event{}, false, nil
	}
	return e, err == nil, err
}

func (s *Store) Complete(ctx context.Context, claimed core.Event, result core.Result, now time.Time, maxAttempts int) error {
	status, next := core.Outcome(result, claimed.Attempts, maxAttempts, now)
	errText := ""
	if status != "delivered" {
		errText = core.SafeError(result)
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var nextPtr *time.Time
	var delivered *time.Time
	if status == "pending" {
		nextPtr = &next
	}
	if status == "delivered" {
		delivered = &now
	}
	command, err := tx.Exec(ctx, `UPDATE events SET status=$1, next_attempt_at=$2,
        lease_until=NULL, lease_token=NULL, delivered_at=$3, last_error=$4
        WHERE id=$5 AND status='processing' AND lease_token=$6 AND attempts=$7`,
		status, nextPtr, delivered, errText, claimed.ID, claimed.LeaseToken, claimed.Attempts)
	if err != nil {
		return err
	}
	if command.RowsAffected() != 1 {
		return core.ErrLeaseLost
	}
	_, err = tx.Exec(ctx, `INSERT INTO delivery_attempts
        (event_id,attempt_number,attempted_at,http_status,outcome,error_message)
        VALUES ($1,$2,$3,$4,$5,$6)`, claimed.ID, claimed.Attempts, now, result.HTTPStatus, status, errText)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
