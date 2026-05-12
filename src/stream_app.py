"""Spark Structured Streaming app:

- Consumes ratings-stream from Kafka
- 30s window / 10s slide analytics: avg_rating, n_ratings, rating_variance
- Custom metric: trending_score = n_ratings * avg_rating
- Per-user interaction counts
- Late-data handling via withWatermark("1 minute")
- ML integration: Top-5 recs per incoming user via ALSModel.recommendForUserSubset
- Latency probe: avg end-to-end lag per micro-batch
- Alert sink: TRENDING items (avg_rating > 4.5, n_ratings >= 5)
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, avg, count, stddev,
    current_timestamp, unix_timestamp, lit,
)
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType
from pyspark.ml.recommendation import ALSModel

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("TOPIC", "ratings-stream")
MODEL_PATH = os.environ.get("MODEL_PATH", "model/als")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "output")

spark = (
    SparkSession.builder.appName("RTRec")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.streaming.checkpointLocation.deleteOnStop", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

schema = (
    StructType()
    .add("user_id", IntegerType())
    .add("item_id", IntegerType())
    .add("rating", FloatType())
    .add("timestamp", StringType())
)

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

events = (
    raw.select(from_json(col("value").cast("string"), schema).alias("d"))
    .select("d.*")
    .withColumn("event_time", col("timestamp").cast("timestamp"))
    .dropna(subset=["user_id", "item_id", "rating", "event_time"])
)

wm = events.withWatermark("event_time", "1 minute")

item_window = (
    wm.groupBy(window("event_time", "30 seconds", "10 seconds"), "item_id")
    .agg(
        avg("rating").alias("avg_rating"),
        count("*").alias("n_ratings"),
        stddev("rating").alias("rating_variance"),
    )
)

trending = item_window.withColumn(
    "trending_score", col("n_ratings") * col("avg_rating")
)

user_window = (
    wm.groupBy(window("event_time", "30 seconds", "10 seconds"), "user_id")
    .agg(count("*").alias("interactions"))
)

alerts = (
    trending.filter("avg_rating > 4.5 AND n_ratings >= 5")
    .selectExpr("'TRENDING' as type", "item_id", "avg_rating", "n_ratings", "window")
)

# Console sinks
q1 = (
    trending.writeStream.outputMode("update").format("console")
    .option("truncate", "false").option("numRows", 20)
    .trigger(processingTime="10 seconds").queryName("trending").start()
)
q2 = (
    user_window.writeStream.outputMode("update").format("console")
    .option("truncate", "false")
    .trigger(processingTime="10 seconds").queryName("user_window").start()
)
q3 = (
    alerts.writeStream.outputMode("update").format("console")
    .option("truncate", "false")
    .trigger(processingTime="10 seconds").queryName("alerts").start()
)

# Parquet sink for dashboard
q4 = (
    trending.writeStream.outputMode("append").format("parquet")
    .option("path", f"{OUTPUT_DIR}/trending")
    .option("checkpointLocation", f"{OUTPUT_DIR}/_ckpt_trending")
    .trigger(processingTime="10 seconds").start()
)

# ML + Streaming: Top-5 recs per incoming user
print(f"[stream] loading ALS model from {MODEL_PATH}")
model = ALSModel.load(MODEL_PATH)

def recommend_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    bd = batch_df.withColumn(
        "ingest_lag_s",
        unix_timestamp(current_timestamp()) - unix_timestamp(col("event_time")),
    )
    row = bd.agg({"ingest_lag_s": "avg"}).collect()[0]
    avg_lag = row[0] if row[0] is not None else float("nan")
    print(f"[batch {batch_id}] avg end-to-end lag ~ {avg_lag:.2f}s")

    users = bd.select("user_id").distinct()
    recs = model.recommendForUserSubset(users, 5)
    recs.show(10, truncate=False)
    recs.write.mode("append").parquet(f"{OUTPUT_DIR}/recs")

q5 = (
    events.writeStream.foreachBatch(recommend_batch)
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .option("checkpointLocation", f"{OUTPUT_DIR}/_ckpt_recs").start()
)

print("[stream] all queries started; waiting for termination")
spark.streams.awaitAnyTermination()
