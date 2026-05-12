"""Batch ALS training on MovieLens. Saves model/als and prints RMSE."""
import os, sys
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

RATINGS_PATH = os.environ.get("RATINGS_PATH", "data/ml-1m/ratings.csv")
MODEL_PATH = os.environ.get("MODEL_PATH", "model/als")

spark = (
    SparkSession.builder.appName("ALSTrain")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print(f"[train] reading {RATINGS_PATH}")
ratings = (
    spark.read.csv(RATINGS_PATH, header=True, inferSchema=True)
    .selectExpr(
        "userId as user_id",
        "movieId as item_id",
        "cast(rating as float) as rating",
        "timestamp",
    )
)

ratings = ratings.filter("rating between 0.5 and 5.0").dropna()
n = ratings.count()
print(f"[train] valid ratings: {n}")

train, test = ratings.randomSplit([0.8, 0.2], seed=42)

als = ALS(
    userCol="user_id", itemCol="item_id", ratingCol="rating",
    coldStartStrategy="drop", nonnegative=True,
    rank=10, regParam=0.1, maxIter=10,
)
print("[train] fitting baseline ALS (rank=10, reg=0.1, iter=10)")
model = als.fit(train)

evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
rmse = evaluator.evaluate(model.transform(test))
print(f"[train] BASELINE RMSE = {rmse:.4f}")

if rmse > 1.5:
    print("[train] RMSE > 1.5 -> tuning (rank=20, reg=0.05, iter=15)")
    als = ALS(
        userCol="user_id", itemCol="item_id", ratingCol="rating",
        coldStartStrategy="drop", nonnegative=True,
        rank=20, regParam=0.05, maxIter=15,
    )
    model = als.fit(train)
    rmse_tuned = evaluator.evaluate(model.transform(test))
    print(f"[train] TUNED RMSE = {rmse_tuned:.4f}")

model.write().overwrite().save(MODEL_PATH)
print(f"[train] model saved to {MODEL_PATH}")
spark.stop()
