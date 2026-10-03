package service

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"embed"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/J-Massoda/eventflow-go/internal/core"
)

type Config struct {
	APIKey      string
	SinkURL     string
	SinkSecret  string
	Workers     int
	PollEvery   time.Duration
	Lease       time.Duration
	MaxAttempts int
	Demo        bool
	FailFirst   bool
}

type Service struct {
	Store  core.Store
	Config Config
	Client *http.Client
	Log    *slog.Logger
	demoMu sync.Mutex
	demo   map[string]int
	seen   map[string]int
}

//go:embed demo.html
var demoPage embed.FS

func New(store core.Store, cfg Config) *Service {
	if cfg.Workers < 1 {
		cfg.Workers = 4
	}
	if cfg.PollEvery <= 0 {
		cfg.PollEvery = 300 * time.Millisecond
	}
	if cfg.Lease <= 0 {
		cfg.Lease = 30 * time.Second
	}
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = 5
	}
	return &Service{Store: store, Config: cfg, Client: &http.Client{Timeout: 5 * time.Second},
		Log: slog.Default(), demo: map[string]int{}, seen: map[string]int{}}
}

func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	mux.Handle("POST /v1/events", s.authorize(http.HandlerFunc(s.create)))
	mux.Handle("GET /v1/events/{id}", s.authorize(http.HandlerFunc(s.get)))
	if s.Config.Demo {
		mux.HandleFunc("GET /", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.Header().Set("Cache-Control", "no-store")
			page, _ := demoPage.ReadFile("demo.html")
			_, _ = w.Write(page)
		})
		mux.HandleFunc("POST /demo/sink", s.demoSink)
		mux.Handle("GET /demo/received", s.authorize(http.HandlerFunc(s.demoReceived)))
	}
	return mux
}

func (s *Service) authorize(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		provided := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if len(provided) != len(s.Config.APIKey) || !hmac.Equal([]byte(provided), []byte(s.Config.APIKey)) {
			writeJSON(w, 401, map[string]string{"error": "unauthorized"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Service) create(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	defer r.Body.Close()
	var n core.NewEvent
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&n); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid JSON event"})
		return
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		writeJSON(w, 400, map[string]string{"error": "expected one JSON object"})
		return
	}
	n.IdempotencyKey = r.Header.Get("Idempotency-Key")
	if err := core.Validate(n); err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	e, repeat, err := s.Store.Insert(r.Context(), n)
	if errors.Is(err, core.ErrIdempotencyConflict) {
		writeJSON(w, 409, map[string]string{"error": err.Error()})
		return
	}
	if err != nil {
		s.Log.Error("insert failed", "error", err)
		writeJSON(w, 503, map[string]string{"error": "storage unavailable"})
		return
	}
	w.Header().Set("Location", "/v1/events/"+e.ID)
	code := 202
	if repeat {
		code = 200
	}
	writeJSON(w, code, map[string]any{"id": e.ID, "status": e.Status, "duplicate": repeat})
}

func (s *Service) get(w http.ResponseWriter, r *http.Request) {
	d, err := s.Store.Get(r.Context(), r.PathValue("id"))
	if errors.Is(err, core.ErrNotFound) {
		writeJSON(w, 404, map[string]string{"error": "event not found"})
		return
	}
	if err != nil {
		s.Log.Error("read failed", "error", err)
		writeJSON(w, 503, map[string]string{"error": "storage unavailable"})
		return
	}
	writeJSON(w, 200, d)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func Signature(secret string, body []byte) string {
	h := hmac.New(sha256.New, []byte(secret))
	_, _ = h.Write(body)
	return "sha256=" + hex.EncodeToString(h.Sum(nil))
}

func (s *Service) deliver(ctx context.Context, event core.Event) core.Result {
	// Only the operator configures SinkURL. Event payloads cannot choose a URL.
	body, err := json.Marshal(struct {
		ID         string          `json:"id"`
		Source     string          `json:"source"`
		Type       string          `json:"type"`
		OccurredAt time.Time       `json:"occurred_at"`
		Payload    json.RawMessage `json:"payload"`
	}{event.ID, event.Source, event.Type, event.OccurredAt, event.Payload})
	if err != nil {
		return core.Result{Error: "invalid delivery payload", Retryable: false}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.Config.SinkURL, bytes.NewReader(body))
	if err != nil {
		return core.Result{Error: "invalid sink configuration", Retryable: false}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Event-ID", event.ID)
	req.Header.Set("X-Delivery-Attempt", fmt.Sprint(event.Attempts))
	req.Header.Set("Idempotency-Key", event.ID)
	req.Header.Set("X-Event-Signature", Signature(s.Config.SinkSecret, body))
	resp, err := s.Client.Do(req)
	if err != nil {
		return core.Result{Error: "receiver unavailable", Retryable: true}
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1024))
	return core.Result{HTTPStatus: resp.StatusCode, Retryable: resp.StatusCode == 429 || resp.StatusCode >= 500}
}

func (s *Service) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < s.Config.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(s.Config.PollEvery)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
				e, ok, err := s.Store.Claim(ctx, time.Now().UTC(), s.Config.Lease)
				if err != nil {
					if ctx.Err() == nil {
						s.Log.Error("claim failed", "error", err)
					}
					continue
				}
				if !ok {
					continue
				}
				r := s.deliver(ctx, e)
				finishCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				err = s.Store.Complete(finishCtx, e, r, time.Now().UTC(), s.Config.MaxAttempts)
				cancel()
				if err != nil {
					s.Log.Error("complete failed", "event_id", e.ID, "error", err)
				}
			}
		}()
	}
	<-ctx.Done()
	wg.Wait()
}

func (s *Service) demoSink(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	defer r.Body.Close()
	data, err := io.ReadAll(r.Body)
	if err != nil || !hmac.Equal([]byte(r.Header.Get("X-Event-Signature")), []byte(Signature(s.Config.SinkSecret, data))) {
		writeJSON(w, 401, map[string]string{"error": "invalid signature"})
		return
	}
	id := r.Header.Get("X-Event-ID")
	if id == "" {
		writeJSON(w, 400, map[string]string{"error": "missing event ID"})
		return
	}
	s.demoMu.Lock()
	s.demo[id]++
	count := s.demo[id]
	if !s.Config.FailFirst || count > 1 {
		s.seen[id]++
	}
	s.demoMu.Unlock()
	if s.Config.FailFirst && count == 1 {
		writeJSON(w, 503, map[string]string{"error": "demo receiver is temporarily unavailable"})
		return
	}
	writeJSON(w, 200, map[string]string{"status": "received"})
}

func (s *Service) demoReceived(w http.ResponseWriter, _ *http.Request) {
	s.demoMu.Lock()
	defer s.demoMu.Unlock()
	writeJSON(w, 200, s.seen)
}
