# Heavy

A standalone real-time financial streaming pipeline wired to [Traceback](https://github.com/) for error monitoring, bottleneck detection, and degradation signals.

## Architecture

```
Alpaca Test WS  →  producer.js  →  Kafka (market-ticks)  →  worker.js
                         │                                      │
                         └──────── RabbitMQ (logs.ingest) ──────┘
                                           │
                                      Traceback
                                   (Postgres + dashboard)
```

| Layer | Technology | Purpose |
|-------|------------|---------|
| Domain data | Kafka `market-ticks` | Market tick stream (unchanged) |
| Observability | RabbitMQ `logs.ingest` | Telemetry to Traceback only |
| Ingestion | Alpaca WebSocket | Mock `FAKEPACA` trade feed |
| Monitoring | Traceback dashboard | Incidents, failure sets, heartbeats |

## Prerequisites

- Node.js v18+
- Docker and Docker Compose
- Alpaca **paper trading** API keys
- Traceback stack running (RabbitMQ, log-processor, dashboard)

## Setup

### 1. Install dependencies

```bash
npm install
```

### 2. Configure environment

Copy the example and fill in your Alpaca keys:

```bash
cp .env.example .env
```

```env
ALPACA_KEY_ID=your_paper_api_key
ALPACA_SECRET_KEY=your_paper_api_secret

RABBITMQ_URL=amqp://guest:guest@localhost:5672
TRACEBACK_SERVICE_NAME=heavy
TRACE_QUEUE=logs.ingest
PROCESSING_WARN_MS=500
```

### 3. Start Kafka (market data)

```bash
docker compose up -d
node init-kafka.js
```

Kafka UI: [http://localhost:8080](http://localhost:8080)

### 4. Start Traceback (observability)

In the Traceback repo:

```bash
# Terminal A — infrastructure (RabbitMQ, Postgres, etc.)
docker compose up -d

# Terminal B — dashboard
cd apps && npm run dev        # http://localhost:3000

# Terminal C — log processor (consumes logs.ingest → Postgres)
cd log-processor && npm run start
```

Ensure Heavy is registered in Traceback **Monitored Applications** as:

| Field | Value |
|-------|-------|
| service_name | `heavy` |
| local path | `~/Desktop/heavy` |
| entry_files | `worker.js,producer.js` |

## Running the pipeline

Use separate terminals from the Heavy project root.

```bash
# Terminal 1 — consumer (with Traceback telemetry)
node worker.js

# Terminal 2 — streamer
node producer.js
```

Normal trade ticks stay in Kafka only — they are **not** sent to Traceback. Only errors, warnings, and periodic heartbeats are published.

## Traceback telemetry

### What gets sent

| Signal | Level | Source | When |
|--------|-------|--------|------|
| Processing error | `ERROR` | worker.js | `processMessage` catch (e.g. missing `trade.routing`) |
| Bottleneck | `WARN` | worker.js | Message processing exceeds `PROCESSING_WARN_MS` (default 500ms) |
| Heartbeat | `INFO` | worker.js | Every 60s with processed/error counts and latency stats |
| Kafka publish failure | `ERROR` | producer.js | `producer.send()` fails |
| Alpaca WS error | `ERROR` | producer.js | WebSocket or Alpaca stream error |
| WS disconnect | `WARN` | producer.js | WebSocket closes unexpectedly |

### Message envelope

```json
{
  "timestamp": "2026-07-10T10:44:05.935Z",
  "log_level": "ERROR",
  "service_name": "heavy",
  "trace_id": "uuid",
  "message": "[worker/processMessage] TypeError: Cannot read properties of undefined..."
}
```

### Degraded mode

If RabbitMQ is offline, Heavy continues running. Telemetry is logged locally with `[telemetry]` prefix and a console warning — the pipeline does not crash.

## Testing Traceback integration

### 1. Trigger a real-world error

With `worker.js` running:

```bash
node inject-error.js
```

This publishes valid JSON missing `routing` metadata. The worker throws on `trade.routing.primary` and sends an `ERROR` to Traceback.

Verify:

```bash
curl http://localhost:3000/api/failure-sets
```

Look for an incident with `root_cause_service: heavy`.

### 2. Trigger a bottleneck warning

```bash
node inject-slow.js
```

Injects a message with `__slowMs: 600`, exceeding the 500ms threshold. Emits a `WARN` to Traceback.

### 3. Heartbeat

Leave `worker.js` running for 60+ seconds. INFO heartbeats appear in the log-processor:

```
[worker/heartbeat] processed=1200 errors=2 p95_latency_ms=45 last_offset=...
```

### 4. RabbitMQ down (degraded mode)

Stop Traceback's RabbitMQ container. Restart `worker.js` — you should see:

```
[traceback] RabbitMQ offline — telemetry logs to console only
```

The worker continues consuming Kafka messages normally.

## Project structure

```
heavy/
├── .env.example          # Environment template
├── docker-compose.yml    # Local Kafka cluster
├── init-kafka.js         # Creates market-ticks topic
├── traceback.js          # RabbitMQ telemetry publisher
├── producer.js           # Alpaca WS → Kafka (+ error telemetry)
├── worker.js             # Kafka consumer (+ timing, heartbeats, errors)
├── inject-error.js       # Test: missing routing metadata
├── inject-slow.js        # Test: simulated bottleneck
└── README.md
```

## Ports

| Service | Port |
|---------|------|
| Kafka broker | `9092` |
| Kafka UI | `8080` |
| RabbitMQ (Traceback) | `5672` |
| Traceback dashboard | `3000` |

## Stopping

```bash
# Ctrl+C in worker and producer terminals
docker compose down   # Kafka
```
