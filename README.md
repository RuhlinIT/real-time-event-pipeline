# Real-Time Event Pipeline

A production-style, self-hosted event pipeline demonstrating reliable event delivery, real-time analytics, and operational recovery patterns.

```text
React frontend
    ↓
Node.js / Express API
    ↓
MongoDB transaction
    ├── raw_events
    └── event_outbox
    ↓
Outbox relay with retry + exponential backoff
    ↓
Kafka: user-events
    ↓
ClickHouse Kafka engine
    ↓
Materialized view
    ↓
ClickHouse MergeTree analytics table
    ↓
Grafana analytics dashboard
```

## What It Demonstrates

- React event tracking with typed event payloads
- Node.js and Express API development with TypeScript
- Zod validation at the API boundary
- MongoDB transactional outbox pattern
- Kafka production with a stable message key (`userId`)
- Outbox retry behavior with exponential backoff and lock recovery
- ClickHouse Kafka-engine ingestion and materialized-view transforms
- Grafana event-volume and event-type analytics
- Protected manual outbox requeue endpoint for reliability testing
- Docker Compose-based local infrastructure

## Stack

| Layer | Technology | Responsibility |
|---|---|---|
| Frontend | React, TypeScript, Vite | Emits product and user-behavior events |
| API | Node.js, Express, TypeScript | Validates, persists, and queues events |
| Operational database | MongoDB replica set | Stores raw events and outbox records transactionally |
| Event streaming | Apache Kafka | Durable event transport through `user-events` |
| Analytics database | ClickHouse | Fast event storage and time-series aggregation |
| Dashboard | Grafana | Real-time analytics and pipeline visibility |
| Local infrastructure | Docker Compose | Runs Kafka, MongoDB, ClickHouse, and Grafana |
| Package manager | pnpm workspace | Manages the frontend and backend packages |

## Repository Layout

```text
real-time-event-pipeline/
├── backend/                    # Express API, outbox relay, MongoDB/Kafka clients
│   ├── .env.example
│   ├── src/
│   │   ├── routes/             # Event, outbox-status, and admin routes
│   │   └── utils/              # Config, schema, MongoDB, Kafka, outbox modules
│   └── package.json
├── frontend/                   # React + Vite event-tracking client
├── clickhouse-init/            # ClickHouse schema and Kafka-engine definitions
├── docker/
│   └── mongodb/                # Local MongoDB replica-set image and keyfile location
├── docker-compose.yml          # Local services
├── package.json                # pnpm workspace scripts
└── pnpm-workspace.yaml
```

## Prerequisites

Install and verify:

- Node.js 22.12 or later
- pnpm 11
- Docker Desktop with Docker Compose
- Git

```bash
node --version
pnpm --version
docker --version
docker compose version
git --version
```

## Local Setup

### 1. Clone and install dependencies

```bash
git clone git@github.com:YOUR_GITHUB_USERNAME/real-time-event-pipeline.git
cd real-time-event-pipeline
pnpm install
```

If pnpm asks you to approve the `esbuild` install script:

```bash
pnpm approve-builds esbuild
pnpm install
```

### 2. Create local secrets

Create the MongoDB replica-set keyfile used by the local Docker image:

```bash
mkdir -p docker/mongodb
openssl rand -base64 756 > docker/mongodb/mongo-keyfile
chmod 400 docker/mongodb/mongo-keyfile
```

Create the backend environment file:

```bash
cp backend/.env.example backend/.env
```

Generate an admin API key:

```bash
openssl rand -hex 32
```

Paste the value into `backend/.env`:

```env
ADMIN_API_KEY=replace-with-your-generated-value
```

> Never commit `backend/.env` or `docker/mongodb/mongo-keyfile`.

### 3. Start infrastructure

```bash
pnpm infra:up
```

Check service status:

```bash
docker compose ps
```

Expected services:

```text
pipeline-zookeeper
pipeline-kafka
pipeline-mongodb
pipeline-clickhouse
pipeline-grafana
```

### 4. Initialize MongoDB replica set

Run once after MongoDB is healthy:

```bash
docker exec pipeline-mongodb mongosh \
  --username admin \
  --password password \
  --authenticationDatabase admin \
  --eval 'rs.initiate({_id: "rs0", members: [{_id: 0, host: "localhost:27017"}]})'
```

Verify it is primary:

```bash
docker exec pipeline-mongodb mongosh \
  --username admin \
  --password password \
  --authenticationDatabase admin \
  --eval 'rs.status().members.map(m => ({name: m.name, stateStr: m.stateStr}))'
```

Expected result:

```text
[{ name: 'localhost:27017', stateStr: 'PRIMARY' }]
```

### 5. Create the Kafka topic

```bash
docker exec pipeline-kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --create \
  --if-not-exists \
  --topic user-events \
  --partitions 3 \
  --replication-factor 1
```

Verify it:

```bash
docker exec pipeline-kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --describe \
  --topic user-events
```

### 6. Start application services

In one terminal:

```bash
pnpm dev:backend
```

In another terminal:

```bash
pnpm dev:frontend
```

## Local URLs

| Service | URL |
|---|---|
| React frontend | http://localhost:5173 |
| Node.js API health check | http://localhost:3001/api/health |
| Outbox status | http://localhost:3001/api/outbox/status |
| Grafana | http://localhost:3000 |
| ClickHouse HTTP | http://localhost:8123 |

Default Grafana credentials:

```text
Username: admin
Password: admin
```

## API

### Health check

```bash
curl http://localhost:3001/api/health
```

### Track an event

```bash
curl -X POST http://localhost:3001/api/track \
  -H "Content-Type: application/json" \
  -d '{
    "eventType": "page_view",
    "userId": "user-001",
    "sessionId": "session-001",
    "properties": {
      "page": "/dashboard",
      "source": "curl"
    }
  }'
```

Successful requests return `202 Accepted` after the event and its outbox record commit in MongoDB.

### Outbox status

```bash
curl http://localhost:3001/api/outbox/status
```

Example response:

```json
{
  "status": "ok",
  "counts": {
    "pending": 0,
    "processing": 0,
    "published": 23
  },
  "oldestPending": null,
  "recentFailures": []
}
```

### Manual outbox requeue

This endpoint is intended for local reliability and duplicate-delivery testing.

```bash
curl -X POST \
  http://localhost:3001/api/admin/outbox/EVENT_ID/requeue \
  -H "Content-Type: application/json" \
  -H "X-Admin-Key: YOUR_ADMIN_API_KEY" \
  -d '{
    "reason": "Manual duplicate-delivery test"
  }'
```

Requeueing an already-published record republishes the same `eventId`. This intentionally tests at-least-once delivery behavior.

## Outbox Design

The API does **not** write directly to MongoDB and Kafka in the request handler. Instead, it performs one MongoDB transaction:

```text
Transaction
    ├── Insert raw event into raw_events
    └── Insert pending Kafka payload into event_outbox
```

The background relay claims pending outbox records, publishes them to Kafka, and marks them `published`. If Kafka is unavailable, the relay stores the failure reason and retries with exponential backoff.

```text
pending → processing → published
            │
            └── failure → pending (retry later)
```

This gives the pipeline at-least-once publication to Kafka. Downstream consumers should use `eventId` as an idempotency key.

## ClickHouse Analytics

The Kafka table receives JSON timestamp values as strings. The materialized view converts them into `DateTime64(3, 'UTC')` values before inserting into the durable MergeTree analytics table.

Verify the analytics row count:

```bash
docker exec pipeline-clickhouse clickhouse-client \
  --user default \
  --password password \
  --query "SELECT count() FROM analytics.events_analytics"
```

Inspect recent events:

```bash
docker exec pipeline-clickhouse clickhouse-client \
  --user default \
  --password password \
  --query "
    SELECT
      eventId,
      eventType,
      userId,
      eventTimestamp,
      receivedAt
    FROM analytics.events_analytics
    ORDER BY receivedAt DESC
    LIMIT 20
  "
```

## Grafana Panels

Configure the ClickHouse data source with:

| Setting | Value |
|---|---|
| Server address | `clickhouse` |
| Port | `8123` |
| Protocol | `HTTP` |
| Database | `analytics` |
| Username | `default` |
| Password | `password` |

Recommended panels:

### Events per minute

```sql
SELECT
  toStartOfMinute(eventTimestamp) AS time,
  count() AS events
FROM analytics.events_analytics
WHERE $__timeFilter(eventTimestamp)
GROUP BY time
ORDER BY time ASC
```

### Events by type

```sql
SELECT
  toStartOfMinute(eventTimestamp) AS time,
  eventType AS metric,
  count() AS events
FROM analytics.events_analytics
WHERE $__timeFilter(eventTimestamp)
GROUP BY time, metric
ORDER BY time ASC, metric ASC
```

### Top event types

```sql
SELECT
  eventType,
  count() AS events
FROM analytics.events_analytics
WHERE $__timeFilter(eventTimestamp)
GROUP BY eventType
ORDER BY events DESC
LIMIT 10
```

## Analytics Dashboard

The Grafana dashboard visualizes real-time event throughput, event-type distribution, recent event activity, and ClickHouse-backed analytics.

![Grafana dashboard showing events per minute, event types, top event types, and recent pipeline events](screenshots/grafana-dashboard.png)

The dashboard reads from `analytics.events_analytics`, which is populated through the Kafka → ClickHouse materialized-view ingestion path.

### Pipeline reconciliation

The project includes separate operational and analytics checks:

```text
MongoDB outbox published count:  23
ClickHouse analytics row count: 23
```

This confirms published outbox events are reaching Kafka and persisting in ClickHouse.

```bash
curl http://localhost:3001/api/outbox/status

docker exec pipeline-clickhouse clickhouse-client \
  --user default \
  --password password \
  --query "SELECT count() FROM analytics.events_analytics"
```

## Testing Failure Recovery

Stop Kafka:

```bash
docker compose stop kafka
```

Send an event with `POST /api/track`. The API should still return `202 Accepted`, while `/api/outbox/status` shows a pending or processing record with retry details.

Restart Kafka:

```bash
docker compose start kafka
```

The relay should retry, publish the event, and eventually return the outbox record to `published`.

## Common Commands

```bash
# Install workspace dependencies
pnpm install

# Type-check all workspace packages
pnpm typecheck

# Build all workspace packages
pnpm build

# Start or stop local infrastructure
pnpm infra:up
pnpm infra:down

# Follow infrastructure logs
pnpm infra:logs

# Start development services
pnpm dev:backend
pnpm dev:frontend

# Inspect Compose services
docker compose ps

# Reset all local infrastructure data (destructive)
docker compose down -v
```

## Roadmap

- [ ] Add `schemaVersion` to every event contract
- [ ] Add idempotent analytics ingestion keyed by `eventId`
- [ ] Add a dead-letter topic for malformed events
- [ ] Add consumer lag and outbox operational metrics to Grafana
- [ ] Add automated integration tests with Docker Compose
- [ ] Add load generation and throughput benchmarks
- [ ] Add a feature-engineering and local ML scoring pipeline

## License

This project is licensed under the [MIT License](LICENSE).

The MIT License permits commercial use, modification, distribution, sublicensing, and private use, provided that the copyright notice and license text are included in copies or substantial portions of the software. The software is provided without warranty.