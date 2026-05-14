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
├── data/ml-1m/                   # MovieLens 1M dataset
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
| Latency (batch 2) | **4.00** |
| TRENDING alerts | Movie 2924 (avg_rating ~4.72, sustained) |

---

## Screenshots

All screenshots were captured from a live execution on 2026-05-12.

### Project Structure

![Project Structure](screenshots/00_project_structure.png)

---

### Architecture Diagram

![Architecture](screenshots/02_architecture.png)

---

### Dataset

**Dataset sample (userId, movieId, rating, timestamp):**

![Dataset Sample](screenshots/03_dataset_sample.png)

**Dataset statistics:**

![Dataset Analysis](screenshots/03_dataset_analysis.png)

---

### ALS Batch Training

**Full training run — Spark progress + RMSE=0.8739:**

![ALS Training RMSE](screenshots/01_als_training_rmse.png)

**Training log (data load → split → fit → evaluate):**

![ALS Training Log](screenshots/01_als_training_log.png)

**Training results summary:**

![ALS Training Results](screenshots/01_als_training_results.png)

**Complete training terminal output:**

![ALS Full Log](screenshots/07_als_full_log.png)

---

### Recommendations

**Top-5 recommendations for sample users (users 1, 42, 100, 200, 500):**

![Recommendations Output](screenshots/09_recommendations_output.png)

---

### Kafka Setup

**Topic created — 2 partitions, replication-factor 1:**

![Kafka Topic Created](screenshots/11_kafka_topic_created.png)

---

### Spark Structured Streaming

**Streaming batches — window analytics results (batch 0/1/2):**

![Streaming Batches](screenshots/12_streaming_batches.png)

**End-to-end Kafka→Spark latency across 3 batches:**

![Latency](screenshots/13_latency.png)

**TRENDING alerts — movie 2924 triggering across windows (avg_rating ~4.72):**

![Alerts](screenshots/14_alerts.png)

---

### Spark UI

**Active streaming query stats:**

![Spark UI Streaming](screenshots/15_spark_ui_streaming.png)

**Streaming page after 30s more data:**

![Spark UI Streaming 2](screenshots/15_spark_ui_streaming2.png)

**All completed Spark jobs:**

![Spark UI Jobs](screenshots/16_spark_ui_jobs.png)

---

### Streamlit Dashboard (Bonus)

**Live dashboard — trending bar chart, Top-5 recs table, TRENDING alerts:**

![Dashboard](screenshots/17_dashboard.png)

---

## Bonus Features

- **Streamlit Dashboard** (+2 pts) — auto-refreshes every 5s, reads live parquet output from `output/trending` and `output/recs`
- **End-to-End Latency Probe** (+1 pt) — each event carries a producer timestamp; `foreachBatch` computes average Δt and logs `avg end-to-end lag`
- **Window Watermarking** — `withWatermark("event_time", "1 minute")` handles late-arriving data correctly
- **Spike Injection** — producer sends 10% of events towards one "hot" movie to reliably trigger TRENDING alerts
