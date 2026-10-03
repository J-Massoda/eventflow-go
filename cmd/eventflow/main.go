package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/J-Massoda/eventflow-go/internal/core"
	"github.com/J-Massoda/eventflow-go/internal/memory"
	"github.com/J-Massoda/eventflow-go/internal/postgres"
	"github.com/J-Massoda/eventflow-go/internal/service"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	mode := flag.String("mode", defaultValue("MODE", "demo"), "demo or postgres")
	addr := flag.String("listen", defaultValue("ADDR", ":8080"), "HTTP listen address")
	flag.Parse()
	key := os.Getenv("API_KEY")
	if len(key) < 16 {
		log.Fatal("API_KEY must contain at least 16 characters")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var store core.Store
	var closeDB func()
	cfg := service.Config{APIKey: key, Workers: 4, MaxAttempts: 5,
		SinkSecret: os.Getenv("SINK_SECRET")}
	switch *mode {
	case "demo":
		if !strings.HasPrefix(*addr, "127.0.0.1:") && !strings.HasPrefix(*addr, "localhost:") {
			log.Fatal("demo mode must bind to 127.0.0.1 or localhost")
		}
		cfg.Demo = true
		cfg.FailFirst = os.Getenv("DEMO_FAIL_FIRST") != "false"
		cfg.SinkURL = "http://127.0.0.1:" + strings.TrimPrefix(strings.Split(*addr, ":")[1], "/") + "/demo/sink"
		if cfg.SinkSecret == "" {
			cfg.SinkSecret = "local-demo-only-signature-secret"
		}
		store = memory.New()
	case "postgres":
		if os.Getenv("DATABASE_URL") == "" || os.Getenv("SINK_URL") == "" || len(cfg.SinkSecret) < 16 {
			log.Fatal("DATABASE_URL, SINK_URL, and SINK_SECRET (16+ characters) are required")
		}
		cfg.SinkURL = os.Getenv("SINK_URL")
		if !strings.HasPrefix(cfg.SinkURL, "https://") && !strings.HasPrefix(cfg.SinkURL, "http://127.0.0.1:") {
			log.Fatal("SINK_URL must use HTTPS (or localhost HTTP for testing)")
		}
		pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
		if err != nil {
			log.Fatal(err)
		}
		closeDB = pool.Close
		if err = pool.Ping(ctx); err != nil {
			log.Fatal(err)
		}
		pg := &postgres.Store{Pool: pool}
		if err = pg.Migrate(ctx); err != nil {
			log.Fatal(err)
		}
		store = pg
	default:
		log.Fatal("mode must be demo or postgres")
	}
	if closeDB != nil {
		defer closeDB()
	}
	s := service.New(store, cfg)
	server := &http.Server{Addr: *addr, Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second}
	go s.Run(ctx)
	go func() {
		log.Printf("EventFlow listening on %s (%s)", *addr, *mode)
		if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Print(err)
			stop()
		}
	}()
	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdown); err != nil {
		log.Print(err)
	}
}

func defaultValue(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	if name == "ADDR" {
		return "127.0.0.1:8080"
	}
	return fallback
}
