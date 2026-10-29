# Polymarket Real-Time Order Book Analytics

A Kafka-based system that collects live order book, trade, and comment data from [Polymarket](https://polymarket.com) prediction markets (tech/AI category). It archives raw events for replay, rebuilds full order books in memory, computes real-time metrics, and serves them on a Grafana dashboard.

The goal is a public order book analytics site built on this pipeline: flag suspicious depth, replay large price moves, push alerts for important events, and show pipeline health on a public status page. No trading, no fees, no redistribution of raw data, no user accounts. Not affiliated with Polymarket.

Everything runs on one machine with Docker Compose. Python 3.12 throughout.

> **Status:** the data pipeline is done (ingestion, archive and batch models, real-time metrics, dashboard and alerts). The website is next. See the [plan](polymarket-orderbook-platform.md) (in Chinese).

## Architecture

```
 Polymarket APIs                     Kafka (KRaft, single node)             Consumers
 ───────────────                     ──────────────────────────             ─────────
 Gamma REST (markets) ──fetch──────▶ pm.raw.markets   (compacted) ──┬──▶ archiver ──▶ Parquet (bronze) ──▶ dbt-duckdb (silver / gold) ◀── Airflow (daily)
 CLOB WebSocket ────────collect────▶ pm.raw.clob      (7 days)    ──┼──▶ metrics ───▶ pm.metrics.book / pm.metrics.trades
 Comments REST ─────────poll───────▶ pm.raw.comments  (compacted) ──┘                     │
                                                                                          ▼
                                                                    pg-sink (upsert) ──▶ Postgres ──▶ Grafana
```

| Layer | What it does | Code |
|---|---|---|
| Ingestion | Pulls the market catalog every hour; streams order book snapshots, price-level changes, and trades over WebSocket; polls comments. Handles reconnects, gap markers, and dynamic resubscription. | `src/pmdata/ingest/` |
| Archive (bronze) | Consumes all raw topics and writes Hive-partitioned Parquet files. Offsets are committed only after a file is fully written. | `src/pmdata/archive/` |
| Batch models | dbt-duckdb: staging models unpack JSON and deduplicate (silver); a star schema of `dim_markets`, `dim_tokens`, `fct_trades`, and `fct_top_of_book` (gold). 80 dbt tests. | `dbt/` |
| Order book | Pure-logic L2 order book rebuild from Kafka events. It does no I/O, so live processing and historical replay share the same code. | `src/pmdata/book/` |
| Real-time metrics | Mid price, spread, microprice, top-1/top-5 imbalance, depth within ±1¢ and ±5¢, log-odds price change over 1/5/15/60 minutes, and per-minute trade flow (aggressive buy vs. sell). | `src/pmdata/metrics/` |
| Serving and alerting | Idempotent upsert into Postgres; Grafana dashboard and alert rules provisioned from files. Alerts (pipeline stalled, market catalog stale) and failed Airflow tasks are sent to Discord. | `src/pmdata/sink/`, `grafana/` |
| Pipeline health | Every minute, records each consumer group's lag (committed offset vs. end offset, per partition) into Postgres; throughput is derived from the same offsets. Collector reconnects and their downtime are rebuilt from the gap markers in the raw stream. Per-minute p50/p99 latency is kept for each hop (exchange → received → computed → stored). Since the feed has no sequence numbers, missed messages are detected indirectly: the service keeps its own order book and counts how often its best bid/ask disagrees with the values the exchange attaches to each update; it also counts out-of-order exchange timestamps. | `src/pmdata/health/` |
| Orchestration | Airflow 3 (LocalExecutor) runs `dbt build` daily at 01:00 UTC, so models and tests are rebuilt on yesterday's complete data. dbt lives in its own virtualenv inside the Airflow image, and DuckDB is capped at 4 GB / 2 threads so the batch job cannot starve the live collectors. | `airflow/` |

## Design choices

- **Raw events stored unchanged.** Every message is wrapped in an envelope (`source`, `received_at`, connection ID and sequence number) with the original payload untouched. All transformation happens downstream, so anything can be recomputed from the archive.
- **Partitioned by market ID.** One `price_change` message usually updates both the Yes and No tokens of a market. Keying by market keeps all events for a market in one partition, in order.
- **Event time, not processing time.** Metrics use exchange timestamps. A second (or minute) is closed when a later message arrives for the same market. Live runs and replays produce identical output.
- **Delivery guarantees without a streaming framework.** State is in memory. On restart, the metrics service replays the last 24 hours to rebuild it, and only emits output from its last committed offset onward. Every stage commits offsets only after its output is durable (at-least-once). The Postgres sink upserts by primary key, so duplicates are harmless.
- **Gaps are explicit.** After each reconnect, the collector writes a `gap` marker per market. Downstream consumers invalidate that market's order book until the next snapshot arrives.

## Validation and results

| Check | Result |
|---|---|
| Ingestion soak test (27 h, unattended) | 1.97M messages, 0 duplicates, 99.998% coverage; one server-side disconnect, recovered in 1.6 s |
| Archived trades vs. official Data API | 253 / 253 trades matched by transaction hash |
| Gold tables vs. independent Python recomputation | 12 markets, identical row by row |
| Rebuilt order book vs. exchange-reported best bid/ask (5-day replay) | 10.2M comparisons, 99.998% match; the rest traced to one stale REST snapshot |
| Rebuilt order book vs. official REST snapshots (10 live rounds) | 582 / 584 books identical at every level (99.66%); 2 mismatched levels from a same-millisecond update |
| Storage | Raw JSON to Parquet (zstd) is about 8.9× smaller (~88 bytes per event) |
| End-to-end latency, exchange to Postgres | p50 2.1 s, p99 5.6 s (±0.3 s clock error) |

Most of the latency is a deliberate trade-off. Each token emits at most once per second, after the second has closed, with a 3-second grace period. This avoids publishing half-applied states when the exchange splits one update across several messages.

## Running it

Requirements: Docker, Python 3.12.

```bash
# 1. Start Kafka and create topics
docker compose up -d kafka kafka-ui
python -m venv .venv && .venv/Scripts/activate        # macOS/Linux: source .venv/bin/activate
pip install -e ".[dev,dbt]"
python scripts/create_topics.py

# 2. Create a .env file with POSTGRES_PASSWORD, GRAFANA_ADMIN_PASSWORD, and for Airflow
#    AIRFLOW_DB_PASSWORD, AIRFLOW_ADMIN_PASSWORD, AIRFLOW_FERNET_KEY, AIRFLOW_JWT_SECRET
#    and DISCORD_WEBHOOK_URL for alerts

# 3. Start all services. Airflow keeps its metadata in the same Postgres, so create its database first
docker compose up -d postgres
docker exec -i postgres psql -U pm -d pm -v pw="$AIRFLOW_DB_PASSWORD" <<'SQL'
CREATE ROLE airflow LOGIN PASSWORD :'pw';
CREATE DATABASE airflow OWNER airflow;
SQL
docker compose up -d

# 4. Batch models: Airflow runs them daily; to run by hand (from dbt/)
cd dbt && PYTHONUTF8=1 dbt build

# Tests and lint
pytest && ruff check .
```

Grafana runs at `localhost:3000`, Kafka UI at `localhost:8080`, and Airflow at `localhost:8081`.

## Tech stack

Kafka (Confluent 7.9, KRaft) · confluent-kafka-python · websockets · PyArrow / Parquet · DuckDB · dbt-duckdb · Airflow 3 · PostgreSQL 17 · Grafana 12 · Docker Compose · pytest · ruff

## Roadmap

- Deploy the website publicly. Market pages (depth chart, slippage calculator) and the status page (latency by stage, throughput, consumer lag, order book accuracy, hourly uptime, incidents) already run locally.
- Price-move replay: scrub the order book before and after a large move, with a one-line verdict (eaten by trades, pulled, or a thin book pushed by small orders)
- Suspicious depth: per price level, flag large orders that are repeatedly placed and pulled with little trading, and score each market
- Alerts for large moves, suspicious depth, and liquidity drops, pushed to a public Discord or Telegram channel
- More scheduled jobs (data quality checks, reconciliation against the official API, compaction of small Parquet files) and an external heartbeat
- Possibly porting metrics to Kafka Streams or Flink for comparison

Project notes and the decision log are in [`CLAUDE.md`](CLAUDE.md) (in Chinese).
