from pyspark.sql import SparkSession, functions as F
from pyspark.ml.functions import vector_to_array

path = "/home/arshad/trustgate/models/iot23_random_forest_v1/test_predictions"

spark = (
    SparkSession.builder
    .appName("TrustGateThresholdEvaluation")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet(path)
print("=== SAVED PREDICTION SCHEMA ===")
df.printSchema()

df = df.withColumn(
    "p_malicious", vector_to_array("probability")[1]
).withColumn(
    "actual_malicious",
    F.lower(F.col("label")).contains("malicious")
)

print("\n=== TEST LABEL COUNTS ===")
df.groupBy("label").count().show(truncate=False)

print("\n=== THRESHOLD COMPARISON ===")
print(
    f"{'Threshold':>9} {'Precision':>10} {'Recall':>10} "
    f"{'F1':>9} {'FP':>8} {'FN':>8} {'Alerts':>9}"
)

for threshold in [x / 100 for x in range(10, 100, 5)]:
    predicted = F.col("p_malicious") >= threshold

    row = df.agg(
        F.sum(F.when(predicted & F.col("actual_malicious"), 1)
              .otherwise(0)).alias("tp"),
        F.sum(F.when(predicted & ~F.col("actual_malicious"), 1)
              .otherwise(0)).alias("fp"),
        F.sum(F.when(~predicted & F.col("actual_malicious"), 1)
              .otherwise(0)).alias("fn"),
        F.sum(F.when(predicted, 1).otherwise(0)).alias("alerts"),
    ).first()

    tp, fp, fn, alerts = [int(row[k] or 0) for k in
                          ["tp", "fp", "fn", "alerts"]]

    precision = tp / (tp + fp) if tp + fp else 0
    recall = tp / (tp + fn) if tp + fn else 0
    f1 = (2 * precision * recall / (precision + recall)
          if precision + recall else 0)

    print(
        f"{threshold:9.2f} {precision:10.4f} {recall:10.4f} "
        f"{f1:9.4f} {fp:8d} {fn:8d} {alerts:9d}"
    )

spark.stop()
