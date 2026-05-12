# Real-Time Recommendation System — Build Guide

End-to-end big-data project: **Apache Spark (ALS) + Kafka + Spark Structured Streaming**.

**Chosen defaults**
- **Domain:** Movies (MovieLens)
- **Focus:** Real-Time Intelligence (trending detection + rating spikes + alerts)

---

## 0. Architecture

```
MovieLens CSV ──► Spark (ALS train) ──► model/ (saved)
                                            │
Kafka Producer (Python) ──► Kafka topic ──► Spark Structured Streaming
                                            │
                                            ├─► Window analytics (30s / 10s slide)
                                            ├─► Trending detector + Alerts
                                            ├─► Top-5 recs (model.recommendForUserSubset)
                                            └─► Console + Parquet sink + (bonus) Streamlit dashboard
```

---

## 1. Install prerequisites (Ubuntu)

```bash
# Java (Spark needs it)
sudo apt update
sudo apt install -y openjdk-17-jdk python3-pip python3-venv curl wget

# Verify
java -version

# Python env
cd "/home/abdulrahman/Desktop/a Zewail City2/4Y/Y4S2/BigData/MiniPrj-3"
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install pyspark==4.1.1 kafka-python-ng==2.2.3 pandas numpy pyarrow streamlit playwright
playwright install chromium
```

---

## 2. Install Kafka (KRaft mode, no Zookeeper)

```bash
cd ~
wget https://downloads.apache.org/kafka/3.7.1/kafka_2.13-3.7.1.tgz
tar -xzf kafka_2.13-3.7.1.tgz
mv kafka_2.13-3.7.1 kafka
cd ~/kafka

# Format storage (one-time)
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties

# Start broker (leave running in its own terminal)
bin/kafka-server-start.sh config/kraft/server.properties
```

In another terminal — create the topic with **2 partitions**:

```bash
cd ~/kafka
bin/kafka-topics.sh --create \
  --topic ratings-stream \
  --bootstrap-server localhost:9092 \
  --partitions 2 --replication-factor 1

bin/kafka-topics.sh --describe --topic ratings-stream --bootstrap-server localhost:9092
```

**Partitioning justification (for the report):** 2 partitions keyed by `user_id` — same user's events stay ordered on the same partition (preserves per-user session ordering) while allowing parallel consumption across 2 Spark executors. Doubling partitions doubles parallel throughput without breaking per-user ordering.

---

## 3. Dataset (MovieLens 25M — satisfies the 500K+ requirement)

```bash
cd "/home/abdulrahman/Desktop/a Zewail City2/4Y/Y4S2/BigData/MiniPrj-3"
mkdir -p data model output
cd data
wget https://files.grouplens.org/datasets/movielens/ml-25m.zip
unzip ml-25m.zip
ls ml-25m/   # ratings.csv, movies.csv, ...
cd ..
```

**Using ml-1m (1M ratings, CSV)** — satisfies 500K requirement, trains fast on local hardware.
Convert from `::` delimiter once:
```bash
echo "userId,movieId,rating,timestamp" > data/ml-1m/ratings.csv
sed 's/::/,/g' data/ml-1m/ratings.dat >> data/ml-1m/ratings.csv
```

**Justification (for the report):**
- Fits movies domain — explicit user/item/rating/timestamp schema.
- 25M ratings across ~162K users and ~62K items needs distributed processing (single-node pandas won't hold the user-item matrix).
- Long-tail items, sparse matrix, timestamp skew, and cold-start users/items are real challenges.

---

## 4. Project layout

```
MiniPrj-3/
├── data/ml-25m/...
├── model/                  # saved ALS model
├── output/                 # streaming sinks, checkpoints
├── src/
│   ├── train_als.py        # batch ML
│   ├── producer.py         # Kafka producer
│   ├── stream_app.py       # Spark Structured Streaming
│   └── dashboard.py        # bonus
└── GUIDE.md
```

---

## 5. Batch ML — `src/train_als.py`

```python
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

spark = (SparkSession.builder.appName("ALSTrain")
         .config("spark.sql.shuffle.partitions", "8").getOrCreate())

ratings = (spark.read.csv("data/ml-25m/ratings.csv", header=True, inferSchema=True)
           .selectExpr("userId as user_id", "movieId as item_id",
                       "cast(rating as float) as rating", "timestamp"))

ratings = ratings.filter("rating between 0.5 and 5.0").dropna()

train, test = ratings.randomSplit([0.8, 0.2], seed=42)

als = ALS(userCol="user_id", itemCol="item_id", ratingCol="rating",
          coldStartStrategy="drop", nonnegative=True,
          rank=10, regParam=0.1, maxIter=10)
model = als.fit(train)

evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                predictionCol="prediction")
rmse = evaluator.evaluate(model.transform(test))
print(f"Baseline RMSE = {rmse}")

# Required tuning if RMSE > 1.5
if rmse > 1.5:
    als = ALS(userCol="user_id", itemCol="item_id", ratingCol="rating",
              coldStartStrategy="drop", rank=20, regParam=0.05, maxIter=15)
    model = als.fit(train)
    rmse = evaluator.evaluate(model.transform(test))
    print(f"Tuned RMSE = {rmse}")

model.write().overwrite().save("model/als")
```

Run:

```bash
source .venv/bin/activate
spark-submit --master local[*] --driver-memory 4g src/train_als.py
```

---

## 6. Kafka producer — `src/producer.py`

```python
import json, time, random
from datetime import datetime, timezone
from kafka import KafkaProducer
import pandas as pd

ratings = pd.read_csv("data/ml-25m/ratings.csv").sample(50000, random_state=1)
users = ratings.userId.unique().tolist()
items = ratings.movieId.unique().tolist()

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    key_serializer=lambda k: str(k).encode(),
    value_serializer=lambda v: json.dumps(v).encode())

print("Producing to topic 'ratings-stream'...")
while True:
    u = random.choice(users)
    # 10% chance to spike one trending item (drives the alert path)
    if random.random() < 0.10:
        i = items[0]
        r = round(random.uniform(4.5, 5.0), 1)
    else:
        i = random.choice(items)
        r = round(random.uniform(1.0, 5.0), 1)
    evt = {"user_id": int(u), "item_id": int(i), "rating": float(r),
           "timestamp": datetime.now(timezone.utc).isoformat()}
    producer.send("ratings-stream", key=u, value=evt)
    time.sleep(0.05)   # ~20 events/sec
```

Run:

```bash
python src/producer.py
```

---

## 7. Streaming + ML integration — `src/stream_app.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, from_json, window, avg, count,
                                   stddev, current_timestamp, unix_timestamp)
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType
from pyspark.ml.recommendation import ALSModel

spark = (SparkSession.builder.appName("RTRec")
         .config("spark.sql.shuffle.partitions","4").getOrCreate())

schema = (StructType()
          .add("user_id", IntegerType()).add("item_id", IntegerType())
          .add("rating", FloatType()).add("timestamp", StringType()))

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers","localhost:9092")
       .option("subscribe","ratings-stream")
       .option("startingOffsets","latest").load())

events = (raw.select(from_json(col("value").cast("string"), schema).alias("d"))
            .select("d.*")
            .withColumn("event_time", col("timestamp").cast("timestamp"))
            .dropna(subset=["user_id","item_id","rating","event_time"]))

# Late-data policy: drop events older than 1 minute from the max event-time seen
wm = events.withWatermark("event_time", "1 minute")

# Window analytics (30s window, 10s slide)
item_window = (wm.groupBy(window("event_time","30 seconds","10 seconds"),"item_id")
                 .agg(avg("rating").alias("avg_rating"),
                      count("*").alias("n_ratings"),
                      stddev("rating").alias("rating_variance")))

# Custom metric: trending_score = n_ratings * avg_rating
trending = item_window.withColumn("trending_score",
                                  col("n_ratings") * col("avg_rating"))

user_window = (wm.groupBy(window("event_time","30 seconds","10 seconds"),"user_id")
                 .agg(count("*").alias("interactions")))

# Alerts: trending items
alerts = (trending.filter("avg_rating > 4.5 AND n_ratings >= 5")
                  .selectExpr("'TRENDING' as type","item_id",
                              "avg_rating","n_ratings","window"))

# Sinks
q1 = (trending.writeStream.outputMode("update").format("console")
      .option("truncate","false").trigger(processingTime="10 seconds")
      .queryName("trending").start())

q2 = (user_window.writeStream.outputMode("update").format("console")
      .trigger(processingTime="10 seconds").queryName("users").start())

q3 = (alerts.writeStream.outputMode("update").format("console")
      .trigger(processingTime="10 seconds").queryName("alerts").start())

q4 = (trending.writeStream.outputMode("append").format("parquet")
      .option("path","output/trending")
      .option("checkpointLocation","output/_ckpt_trending")
      .trigger(processingTime="10 seconds").start())

# --- ML + Streaming integration: Top-5 recs per incoming user ---
model = ALSModel.load("model/als")

def recommend_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    # Latency probe
    batch_df = batch_df.withColumn("ingest_lag_s",
                                   unix_timestamp(current_timestamp()) -
                                   unix_timestamp(col("event_time")))
    avg_lag = batch_df.agg({"ingest_lag_s":"avg"}).collect()[0][0]
    print(f"[batch {batch_id}] avg end-to-end lag ≈ {avg_lag:.2f}s")

    users = batch_df.select("user_id").distinct()
    recs = model.recommendForUserSubset(users, 5)
    recs.show(20, truncate=False)
    recs.write.mode("append").parquet("output/recs")

q5 = (events.writeStream.foreachBatch(recommend_batch)
      .outputMode("append").trigger(processingTime="5 seconds")
      .option("checkpointLocation","output/_ckpt_recs").start())

spark.streams.awaitAnyTermination()
```

Run:

```bash
spark-submit --master 'local[*]' --driver-memory 4g \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \
  src/stream_app.py
```

---

## 8. Late-data handling (mandatory section in report)

- `withWatermark("event_time", "1 minute")` — Spark keeps aggregation state for 1 minute past the max observed event-time.
- Events that arrive late **but within** that watermark are still folded into the matching windows.
- Events later than the watermark are **dropped** automatically, so state size stays bounded.

Trade-off: tighter watermark → lower memory, more drops; looser watermark → more accurate windows, higher state.

---

## 9. Latency measurement (bonus +1)

The `recommend_batch` function above computes `current_timestamp() - event_time` per micro-batch and prints `avg end-to-end lag`. Capture screenshots showing `< 5s` for the bonus point.

---

## 10. Bonus dashboard — `src/dashboard.py`

```python
import time, glob, os
import streamlit as st
import pandas as pd

st.set_page_config(page_title="Real-Time Recs", layout="wide")
st.title("Real-Time Recommendation Dashboard")

placeholder = st.empty()

def load_parquet_dir(path):
    files = glob.glob(os.path.join(path, "*.parquet"))
    if not files: return pd.DataFrame()
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

while True:
    trending = load_parquet_dir("output/trending")
    recs = load_parquet_dir("output/recs")

    with placeholder.container():
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Top trending items")
            if not trending.empty:
                top = (trending.sort_values("trending_score", ascending=False)
                       .drop_duplicates("item_id").head(10))
                st.bar_chart(top.set_index("item_id")["trending_score"])
        with c2:
            st.subheader("Sample recommendations")
            if not recs.empty:
                st.dataframe(recs.head(20))
        st.subheader("Alerts (avg_rating > 4.5)")
        if not trending.empty:
            alerts = trending[(trending.avg_rating > 4.5) & (trending.n_ratings >= 5)]
            st.dataframe(alerts.tail(20))
    time.sleep(5)
```

Run:

```bash
streamlit run src/dashboard.py
```

---

## 11. End-to-end run order

```bash
# Terminal 1 — Kafka broker
~/kafka/bin/kafka-server-start.sh ~/kafka/config/kraft/server.properties

# Terminal 2 — one-time ALS training
spark-submit --master local[*] --driver-memory 4g src/train_als.py

# Terminal 3 — Kafka producer
python src/producer.py

# Terminal 4 — streaming app
spark-submit --master 'local[*]' --driver-memory 4g \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \
  src/stream_app.py

# Terminal 5 — dashboard (bonus)
streamlit run src/dashboard.py
```

---

## 12. Report-to-rubric mapping

| Rubric criterion | Where it lives |
|---|---|
| Data preprocessing & ML model (15%) | `train_als.py` — filter, dropna, 80/20 split, ALS |
| RMSE evaluation & tuning (10%) | Baseline + tuned RMSE prints; show both in report |
| Kafka setup & partitioning (10%) | Section 2 + partitioning justification |
| Streaming pipeline correctness (10%) | `stream_app.py` — Kafka source, JSON parsing, dropna |
| Window analytics (10%) | 30s/10s windows + `trending_score` custom metric |
| ML + streaming integration (10%) | `recommend_batch` using `ALSModel.recommendForUserSubset` |
| Alerts & watermarking (5%) | `alerts` stream + `withWatermark("1 minute")` |
| Documentation (20%) | This file + architecture diagram + screenshots |
| Discussion / innovation (10%) | Real-Time Intelligence focus + spike injection + lag probe |
| Dashboard bonus (+2) | `dashboard.py` |
| Latency bonus (+1) | `avg end-to-end lag` line in `recommend_batch` |

---

## 13. Screenshot checklist for the report

The rubric leans heavily on documentation (20%). Capture these — name them clearly so you can drop them straight into the report.

| # | What | Where | How |
|---|---|---|---|
| 1 | Kafka topic with 2 partitions | terminal | `kafka-topics.sh --describe --topic ratings-stream ...` — OS screenshot |
| 2 | Baseline RMSE | terminal | `train_als.py` stdout — OS screenshot |
| 3 | Tuned RMSE (only if baseline > 1.5) | terminal | `train_als.py` stdout — OS screenshot |
| 4 | Producer running | terminal | `producer.py` showing event emission |
| 5 | Window analytics output | terminal | streaming app console — `trending` query batch |
| 6 | Top-5 recs per user | terminal | streaming app — `recs.show()` block |
| 7 | Alerts firing | terminal | streaming app — `alerts` query batch |
| 8 | Latency line (bonus) | terminal | `[batch N] avg end-to-end lag ≈ X.XXs` |
| 9 | Spark UI streaming tab | browser http://localhost:4040 | browser screenshot or Playwright MCP |
| 10 | Streamlit dashboard | browser http://localhost:8501 | browser screenshot or Playwright MCP |

### Optional — script the browser screenshots

If you don't want to install Playwright MCP, this one-shot script captures #9 and #10:

```bash
pip install playwright
playwright install chromium

python - <<'EOF'
from playwright.sync_api import sync_playwright
targets = [
    ("http://localhost:4040/StreamingQuery/", "screenshots/spark_ui.png"),
    ("http://localhost:8501",                  "screenshots/dashboard.png"),
]
import os; os.makedirs("screenshots", exist_ok=True)
with sync_playwright() as p:
    b = p.chromium.launch()
    for url, out in targets:
        page = b.new_page(viewport={"width":1600,"height":1000})
        page.goto(url, wait_until="networkidle")
        page.screenshot(path=out, full_page=True)
        print("saved", out)
    b.close()
EOF
```

### Optional — Playwright MCP for repeated capture

```bash
claude mcp add playwright -- npx -y @playwright/mcp@latest
```

Then from Claude Code: *"Screenshot http://localhost:4040 full page and save to screenshots/spark_ui.png"*.

---

## 14. Common pitfalls

- **`ALSModel.load` errors** — Spark version mismatch between training and streaming. Use the same `pyspark==4.0.0`.
- **No data in console** — producer was started before topic existed, or `startingOffsets` is `latest`. Restart producer after streaming app is up.
- **Java errors** — Spark 4 needs Java 17+. `export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64`.
- **Kafka package download fails** — pre-download with: `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 ...` once with internet, then it's cached in `~/.ivy2`.
- **Python 3.13** — PySpark 3.5.x will not work; use PySpark 4.0.0.
- **Cold-start users** — ALS will produce empty recs for users unseen at training time; `coldStartStrategy="drop"` handles this in evaluation but at inference you'll need a fallback (e.g., popularity baseline) for new users.
