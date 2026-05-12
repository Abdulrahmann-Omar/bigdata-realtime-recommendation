# Real-Time Movie Recommendation System

Big Data mini-project (Spring 2026) — end-to-end system combining batch ML (ALS on MovieLens) with real-time Spark Structured Streaming over Kafka.

- **Domain:** Movies (MovieLens 25M)
- **Focus:** Real-Time Intelligence (trending detection, rating spikes, alerts)
- **Stack:** Apache Spark 4.0 + Kafka 3.7 (KRaft) + Python 3.13 + Streamlit

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

## Layout

```
.
├── src/
│   ├── train_als.py        # batch ALS training
│   ├── producer.py         # Kafka producer (~20 events/sec)
│   ├── stream_app.py       # Structured Streaming + ML integration
│   ├── dashboard.py        # bonus Streamlit dashboard
│   └── screenshot.py       # Playwright capture script
├── screenshots/            # captured visuals for the report
├── logs/                   # raw stdout/stderr from each component
├── GUIDE.md                # step-by-step build guide
└── README.md
```

See [GUIDE.md](GUIDE.md) for full setup and run instructions.

## Run order

```bash
# 1. Kafka broker (terminal 1)
~/kafka/bin/kafka-server-start.sh ~/kafka/config/kraft/server.properties

# 2. ALS training (one-shot)
source .venv/bin/activate
spark-submit --master 'local[*]' --driver-memory 4g src/train_als.py

# 3. Producer (terminal 2)
python src/producer.py

# 4. Streaming app (terminal 3)
spark-submit --master 'local[*]' --driver-memory 4g \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
  src/stream_app.py

# 5. Dashboard (terminal 4)
streamlit run src/dashboard.py
```
