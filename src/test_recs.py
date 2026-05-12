"""Quick demo: load saved ALS model and show Top-5 recs for a few users."""
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel

spark = (SparkSession.builder.appName("TestRecs")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

model = ALSModel.load("model/als")

sample_users = spark.createDataFrame([(1,),(42,),(100,),(200,),(500,)], ["user_id"])
recs = model.recommendForUserSubset(sample_users, 5)
recs.show(truncate=False)
spark.stop()
