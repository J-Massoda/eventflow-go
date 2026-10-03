# EventFlow Go

EventFlow is a small Go service that accepts business events and delivers them to one configured webhook receiver. It handles a common problem: an order or other change happened, but the system that needed to hear about it was temporarily unavailable.

For example, an online store records an order. EventFlow accepts the `order.created` event, gives it a stable ID, and tries to send it to an operations service. If that service returns HTTP 503, EventFlow records the failure and retries. A dashboard or support team can look up the event and see each attempt.

**Portfolio evidence:** Go HTTP services, PostgreSQL transactions, `FOR UPDATE SKIP LOCKED`, concurrent workers, idempotency, expiring leases, signed webhooks, retry policy, testable interfaces, and failure-aware operations.

This is an independent demonstration, not a payment processor or customer integration. Demo events are fictional. It makes **at-least-once** delivery attempts; the receiver must deduplicate using the stable `Idempotency-Key` / event ID. It does not promise exactly-once effects or per-source strict ordering.

## Publish the portfolio walkthrough

[`docs/index.html`](docs/index.html) is a standalone, fictional browser walkthrough. Publish the repository's `/docs` folder through GitHub Pages using **Settings → Pages → Deploy from a branch**. It illustrates event intake, a temporary failure, a retry and duplicate handling. It does not run the Go server or call a real receiver. Use the local demo below to test the actual HTTP service.

## Two ways to run it

| Mode | Purpose | Storage | Prerequisites |
| --- | --- | --- | --- |
| `demo` | Explore the workflow locally in a browser | In memory; resets on restart | Go 1.24+ |
| `postgres` | Verify persistence and run your own receiver | PostgreSQL 14+ | Go 1.24+, PostgreSQL (Docker Compose provided) |

The server uses only Go's standard HTTP library and [pgx](https://github.com/jackc/pgx) for PostgreSQL. No frontend build step or paid service is required.

## Run the browser demo

Install [Go](https://go.dev/dl/) 1.24 or newer, open a terminal in this folder, and run:

**PowerShell (VS Code on Windows):**

```powershell
$env:API_KEY = "local-demo-key-change-me"
go mod download
go run ./cmd/eventflow -mode demo
```

**macOS / Linux:**

```bash
export API_KEY=local-demo-key-change-me
go mod download
go run ./cmd/eventflow -mode demo
```

Open **http://127.0.0.1:8080**. Paste `local-demo-key-change-me` into the page, then click **Send event**. You should see `pending` or `processing`, a first delivery returning `503`, and about two seconds later a second delivery returning `200`. The final state is `delivered` with two attempts. The fake receiver is inside the local service and intentionally fails once. This browser page is served only in demo mode, and demo mode refuses to bind to a public interface.

Stop with **Ctrl+C**. Demo records disappear after a restart.

### Try the API directly

Open a second PowerShell terminal while the demo is running:

```powershell
$headers = @{ Authorization = "Bearer local-demo-key-change-me"; "Idempotency-Key" = "order-101" }
$body = '{"source":"shop","type":"order.created","occurred_at":"2026-10-02T12:00:00Z","payload":{"order_id":"ORDER-101","amount_cents":2499}}'
$created = Invoke-RestMethod -Uri http://127.0.0.1:8080/v1/events -Method Post -Headers $headers -ContentType application/json -Body $body
$created
Start-Sleep -Seconds 3
Invoke-RestMethod -Uri "http://127.0.0.1:8080/v1/events/$($created.id)" -Headers @{ Authorization = "Bearer local-demo-key-change-me" } | ConvertTo-Json -Depth 10
```

Repeat the POST with the **same header and same body**: you get the same ID and `duplicate: true`. Change only `order_id` but keep the key: you get HTTP 409. Each new business event needs a new idempotency key. The key is scoped by `source` in this single-operator example.

### Check the receiver

In demo mode, `GET /demo/received` returns a count of successful receives per event ID. It requires the same bearer token. The receiver checks the HMAC signature and simulates an HTTP 503 on the first attempt unless `DEMO_FAIL_FIRST=false` is set before launch.

## Run with PostgreSQL

Start a local database with Docker Desktop or another Docker installation:

```bash
docker compose up -d postgres
```

Set `API_KEY` to a long, private value. Set `DATABASE_URL` to the URL below for local Docker Compose, `SINK_URL` to **your own** webhook receiver, and `SINK_SECRET` to another private value of at least 16 characters. Use HTTPS for a remote receiver. Example for PowerShell:

```powershell
$env:API_KEY = "replace-with-a-private-api-key"
$env:DATABASE_URL = "postgres://eventflow:eventflow@127.0.0.1:5432/eventflow?sslmode=disable"
$env:SINK_URL = "https://your-receiver.example/events"
$env:SINK_SECRET = "replace-with-a-private-signing-secret"
go run ./cmd/eventflow -mode postgres -listen 127.0.0.1:8080
```

The service creates its own tables on startup. PostgreSQL retains events and attempt history across server restarts. The sample database credentials are for local development only. There is **no browser UI in PostgreSQL mode**; use the API or create a separate receiver/dashboard. Do not use the public demo receiver with confidential data.

For a quick local receiver you can run an HTTP test server on `127.0.0.1` and set `SINK_URL=http://127.0.0.1:9000/events`. Your receiver should verify `X-Event-Signature`, a hex HMAC-SHA256 digest of the raw request body prefixed `sha256=`, using `SINK_SECRET`.

## API

| Endpoint | Purpose |
| --- | --- |
| `GET /healthz` | Process liveness |
| `POST /v1/events` | Accept one JSON event; requires bearer API key and `Idempotency-Key` header |
| `GET /v1/events/{id}` | Inspect status and delivery history; requires bearer API key |
| `GET /` | Guided browser demo, only in local demo mode |
| `GET /demo/received` | Successful local receiver counts, only in demo mode |

`POST` returns `202` for a new event, `200` for an identical repeat, `409` for changed content with a reused key, and `400` for invalid data. Request size is limited to 64 KiB. Event names contain only letters, digits, `.`, `_`, and `-`; `occurred_at` is RFC3339 and `payload` must be a JSON object. The receiver URL is configured by the operator, never by the event sender.

Each delivery includes `X-Event-ID`, `X-Delivery-Attempt`, `Idempotency-Key`, and `X-Event-Signature`. A 2xx response succeeds. HTTP 429, 5xx, and network errors are retried, up to five attempts; other 4xx responses become `dead`. Delays begin at 2 seconds and double up to a one-minute cap. Operators can inspect failed events; this MVP intentionally has no public replay/reset endpoint.

### Architecture

```text
Client POST → validate and deduplicate → durable event record
                                         ↓
                        concurrent workers lease due records
                                         ↓
                        signed POST to configured receiver
                                         ↓
                 record success, retry time, or dead state
```

Workers use PostgreSQL row locking with `SKIP LOCKED` and an expiring lease. A crash can cause the same event to be delivered again after lease expiry. The receiver therefore needs its own idempotency handling. An outdated worker cannot complete a newer worker's lease.

## Tests

```bash
go test -race ./...
go vet ./...
```

Tests cover concurrent duplicate intake, changed-content conflict, lease recovery, stale-worker protection, signed HTTP delivery, a real HTTP 503 followed by a successful retry, a permanent HTTP 400, and backoff rules.

For the **optional PostgreSQL integration test**, create a disposable database whose name ends in `_test`, set `TEST_DATABASE_URL`, and run `go test ./internal/postgres`. This test creates and truncates `events` and `delivery_attempts` in that test database only. Example:

```bash
TEST_DATABASE_URL='postgres://eventflow_test:password@127.0.0.1:5432/eventflow_test?sslmode=disable' go test ./internal/postgres -v
```

## Project boundaries and next steps

The demo is intentionally in memory. The PostgreSQL adapter stores event state but does not provide multi-tenancy, a hosted dashboard, per-customer API keys, metrics export, manual replay approvals, scheduled retention, or webhook destination management. Before production use, add TLS at the edge, a secret manager, monitoring, migrations under a deployment process, an operational replay policy, and receiver-specific end-to-end tests. The configured sink should be trusted; this project does not accept arbitrary customer-selected URLs.

## License

See `LICENSE`. The implementation and documentation are original to this demonstration.
