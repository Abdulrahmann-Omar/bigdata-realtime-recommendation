# Real-Time Movie Recommendation System

Big Data mini-project (Spring 2026) — end-to-end system combining batch ML (ALS on MovieLens 1M) with real-time Spark Structured Streaming over Kafka.

- **Domain:** Movies (MovieLens 1M — 1,000,209 ratings)
- **Focus:** Real-Time Intelligence (trending detection, rating spikes, alerts)
- **Stack:** Apache Spark 4.1.1 + Kafka 3.7.1 (KRaft) + Python 3.13 + Streamlit

## Architecture

```
MovieLens CSV ──► Spark ALS train ──► model/als (saved)
                                          │
Kafka Producer ──► ratings-stream  ──► Spark Structured Streaming
                   (2 partitions)         │
                                          ├─► 30s/10s window analytics
                                          ├─► trending_score custom metric
                                          ├─► Top-5 recs (recommendForUserSubset)
                                          ├─► TRENDING alerts (avg_rating > 4.5)
                                          └─► Parquet sink + Streamlit dashboard
```

## Project Layout

```
.
├── src/
│   ├── train_als.py              # batch ALS training (rank=10, regParam=0.1)
│   ├── producer.py               # Kafka producer (~20 events/sec, 10% spike injection)
│   ├── stream_app.py             # Structured Streaming + ML integration
│   ├── dashboard.py              # Streamlit dashboard (bonus)
│   ├── render_screenshot.py      # Playwright terminal-style screenshot renderer
│   └── run_pipeline_and_screenshot.py  # automated pipeline orchestrator
├── screenshots/                  # all captured visuals (see below)
├── logs/                         # raw stdout/stderr from each component
├── GUIDE.md                      # step-by-step build guide
└── requirements.txt
```

## Run Order

```bash
# 0. One-time setup
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium

# 1. Format KRaft storage (first time only)
~/kafka/bin/kafka-storage.sh format \
  -t $(~/kafka/bin/kafka-storage.sh random-uuid) \
  -c ~/kafka/config/kraft/server.properties

# 2. Start Kafka broker (terminal 1)
~/kafka/bin/kafka-server-start.sh ~/kafka/config/kraft/server.properties

# 3. ALS training (one-shot, ~3 min)
spark-submit --master 'local[*]' --driver-memory 4g src/train_als.py

# 4. Producer (terminal 2)
python src/producer.py

# 5. Streaming app (terminal 3)
spark-submit --master local[4] --driver-memory 4g \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \
  src/stream_app.py

# 6. Dashboard (terminal 4 — bonus)
streamlit run src/dashboard.py

# Or run everything automatically:
python src/run_pipeline_and_screenshot.py
```

## Key Metrics (Live Run — 2026-05-12)

| Metric | Value |
|--------|-------|
| Dataset | MovieLens 1M (1,000,209 ratings) |
| ALS RMSE | **0.8739** |
| Throughput | ~20 events/sec |
| Window | 30s window / 10s slide |
| Latency (batch 0) | 25.25s |
| Latency (batch 1) | 61.96s |
| Latency (batch 2) | **10.62s** |
| TRENDING alerts | Movie 2924 (avg_rating ~4.72, sustained) |

## Screenshots

All screenshots were captured from a live execution on 2026-05-12.

### Dataset & Training

| File | Description |
|------|-------------|
| [`00_project_structure.png`](screenshots/00_project_structure.png) | Project directory tree showing all source files and folders |
| [`01_als_training_rmse.png`](screenshots/01_als_training_rmse.png) | Full ALS training run — Spark progress bars + RMSE=0.8739 printed |
| [`01_als_training_log.png`](screenshots/01_als_training_log.png) | ALS training log: data load, split, fit, evaluate stages |
| [`01_als_training_results.png`](screenshots/01_als_training_results.png) | Training summary: RMSE result and model saved confirmation |
| [`03_dataset_analysis.png`](screenshots/03_dataset_analysis.png) | MovieLens 1M dataset statistics (ratings distribution, user/movie counts) |
| [`03_dataset_sample.png`](screenshots/03_dataset_sample.png) | Sample rows from ratings CSV |
| [`07_als_full_log.png`](screenshots/07_als_full_log.png) | Complete ALS training terminal output from logs/train_als.log |
| [`08_dataset_sample.png`](screenshots/08_dataset_sample.png) | Dataset sample with header (userId, movieId, rating, timestamp) |
| [`09_recommendations_output.png`](screenshots/09_recommendations_output.png) | Top-5 recommendations for sample users (users 1, 42, 100, 200, 500) |

### Architecture

| File | Description |
|------|-------------|
| [`02_architecture.png`](screenshots/02_architecture.png) | System architecture: batch ALS path + streaming pipeline diagram |

### Kafka & Streaming

| File | Description |
|------|-------------|
| [`11_kafka_topic_created.png`](screenshots/11_kafka_topic_created.png) | `kafka-topics.sh --describe` output — ratings-stream, 2 partitions, replication-factor 1 |
| [`12_streaming_batches.png`](screenshots/12_streaming_batches.png) | Spark Structured Streaming console output — batch 0/1/2 with window analytics results |
| [`13_latency.png`](screenshots/13_latency.png) | End-to-end Kafka→Spark latency probe across 3 batches (10.62s best) |
| [`14_alerts.png`](screenshots/14_alerts.png) | TRENDING alerts — movie 2924 triggering sustained alerts across windows (avg_rating ~4.72) |

### Spark UI & Dashboard

| File | Description |
|------|-------------|
| [`15_spark_ui_streaming.png`](screenshots/15_spark_ui_streaming.png) | Spark UI `/StreamingQuery/` — active streaming query stats |
| [`15_spark_ui_streaming2.png`](screenshots/15_spark_ui_streaming2.png) | Spark UI streaming page refreshed after 30s more data accumulation |
| [`16_spark_ui_jobs.png`](screenshots/16_spark_ui_jobs.png) | Spark UI `/jobs/` — all completed Spark jobs from the streaming run |
| [`17_dashboard.png`](screenshots/17_dashboard.png) | Streamlit dashboard — trending bar chart, Top-5 recs table, TRENDING alerts table |

## Bonus Features

- **Streamlit Dashboard** (+2 pts) — auto-refreshes every 5s, reads live parquet output from `output/trending` and `output/recs`
- **End-to-End Latency Probe** (+1 pt) — each event carries a producer timestamp; `foreachBatch` computes average Δt and logs `avg end-to-end lag`
- **Window Watermarking** — `withWatermark("event_time", "1 minute")` handles late-arriving data correctly
- **Spike Injection** — producer sends 10% of events towards one "hot" movie to reliably trigger TRENDING alerts
