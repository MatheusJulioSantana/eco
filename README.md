# eco-runtime — portfolio

> Backend Engineer portfolio built as a small live system.  
> Go backend · real metrics · low-cost deploy path

**[matheusjuliosantana.fly.dev](https://matheusjuliosantana.fly.dev)** · [LinkedIn](https://linkedin.com/in/matheusjuliosantana) · [matheus.jsant@gmail.com](mailto:matheus.jsant@gmail.com)

## What It Is

This portfolio is a Go application, not only a static page.

The backend exposes live metrics from the running server: cache hit rate, p50 latency, uptime and estimated CO2 using `green-stack-monitor`. The frontend consumes those values directly. When the backend is unavailable, the UI shows missing data instead of fake numbers.

The implementation stays intentionally small: one Go process, one HTML file, in-memory cache, stdlib middleware, Docker and Fly.io configuration.

## Structure

```text
portfolio/
├── main.go          # Go server, cache, rate limiting and metrics
├── portfolio.html   # Vanilla JS frontend, EN/PT copy
├── Dockerfile       # multi-stage build: golang:alpine -> alpine:3.19
├── fly.toml         # Fly.io config, GRU region
└── README.md
```

## Local Run

Requires Go 1.22+.

```bash
git clone https://github.com/MatheusJulioSantana/eco
cd eco
go run main.go
```

```text
eco-portfolio on :8080
   /            -> portfolio  (5 min cache, gzip)
   /api/metrics -> metrics    (green-stack-monitor)
   /eco         -> eco ping   (30s cache)
   /api/ping    -> health
```

Open `http://localhost:8080`.

## Endpoints

### `GET /api/metrics`

```json
{
  "cache_hit_rate": 94.2,
  "p50_response_ms": 18.0,
  "cached_requests_today": 1247,
  "uptime_hours": 12.4,
  "co2_per_req_g": 0.000041,
  "co2_total_g": 0.0538,
  "co2_saved_g": 0.0472,
  "badge_color": "#44cc11",
  "carbon_intensity_gco2kwh": 87.3,
  "carbon_zone": "BR-CS",
  "total_requests": 1312,
  "cache_hits": 1247,
  "cache_misses": 65,
  "generated_at": "2026-07-04T14:32:00Z"
}
```

### `GET /eco`

```bash
curl https://matheusjuliosantana.fly.dev/eco
```

```json
{
  "co2_per_request_g": "0.000041",
  "co2_total_session_g": "0.0538",
  "co2_saved_session_g": "0.0472",
  "badge_color": "#44cc11",
  "cache_hit_rate": "94.2%",
  "cache_hits_total": 1247,
  "uptime_hours": "12.4",
  "formula": "CO2(g) = duration_ms × TDP × PUE × CI / 3_600_000",
  "methodology": "Green Algorithms — doi.org/10.1002/advs.202100707",
  "config": "TDP=4W PUE=1.2 CI=100gCO2/kWh (BR-CS grid)",
  "powered_by": "github.com/matheusjuliosantana/green-stack-monitor",
  "note": "wall time, not CPU time — conservative estimate"
}
```

### `GET /api/ping`

Simple health check.

## Carbon Awareness

The backend can call [Electricity Maps](https://electricitymap.org) to expose current carbon intensity for the BR-CS grid. Values are cached for 1 hour.

```bash
export ELECTRICITY_MAPS_KEY=your_key_here
flyctl secrets set ELECTRICITY_MAPS_KEY=your_key_here
```

Without the key, `carbon_intensity_gco2kwh` returns `0.0` and the app keeps running.

## Deployment

Fly.io is the intended low-cost path:

```bash
flyctl auth signup
flyctl launch
flyctl secrets set ELECTRICITY_MAPS_KEY=your_key_here
flyctl deploy
```

The included `fly.toml` uses:

- `auto_stop_machines = "stop"`
- `min_machines_running = 0`
- `memory = "256mb"`
- primary region `gru`

That keeps the app cheap for a personal portfolio while still being easy to wake on demand.

## Architecture

```text
request
  ↓
recovery
  ↓
security headers
  ↓
rate limiter
  ↓
logger
  ↓
gzip
  ↓
in-memory TTL cache
  ↓
handler
```

Most infrastructure code uses Go stdlib (`compress/gzip`, `sync`, `net/http`, `encoding/json`). Carbon estimation and badge color helpers come from `green-stack-monitor`.

## Stack

- **Backend:** Go 1.22 · stdlib · green-stack-monitor
- **Frontend:** HTML · CSS · Vanilla JS
- **Infra:** Docker · Fly.io · Cloudflare DNS
- **Carbon data:** Electricity Maps API, optional
