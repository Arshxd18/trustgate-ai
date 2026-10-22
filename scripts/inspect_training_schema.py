from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("TrustGate-Inspect-Training-Schema")
    .getOrCreate()
)

path = (
    "/home/arshad/trustgate/processed/"
    "iot23_training_features_v2"
)

df = spark.read.parquet(path)

print("\n=== TRAINING DATA SCHEMA ===")
df.printSchema()

print("\n=== COLUMN NAMES ===")
for column in df.columns:
    print(column)

print(f"\nTotal columns: {len(df.columns)}")
print(f"Total records: {df.count()}")

spark.stop()
