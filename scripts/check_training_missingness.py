from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("TrustGate-Verify-Features")
    .getOrCreate()
)

path = (
    "/home/arshad/trustgate/processed/"
    "iot23_training_features_v2"
)

df = spark.read.parquet(path)

print("\n=== RECORD COUNT ===")
print(df.count())

print("\n=== MISSINGNESS FLAGS ===")
df.groupBy("traffic_measurements_missing").count().show()

print("\n=== INDIVIDUAL MISSINGNESS COUNTS ===")
df.agg(
    *[
        F.sum(F.col(f"{column}_missing")).alias(
            f"{column}_missing_count"
        )
        for column in ["duration", "orig_bytes", "resp_bytes"]
    ]
).show(truncate=False)

print("\n=== LABEL DISTRIBUTION ===")
df.groupBy("label").count().show()

spark.stop()
