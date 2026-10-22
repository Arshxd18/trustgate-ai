from pyspark.sql import SparkSession

PREDICTIONS_PATH = (
    "/home/arshad/trustgate/models/"
    "iot23_random_forest_v1/test_predictions"
)

spark = (
    SparkSession.builder
    .appName("TrustGate-Prediction-Inspection")
    .getOrCreate()
)

try:
    df = spark.read.parquet(PREDICTIONS_PATH)

    print("\n=== SAVED PREDICTION SCHEMA ===")
    df.printSchema()

    print("\n=== SAMPLE PREDICTIONS ===")
    df.select(
        "label",
        "prediction",
        "probability"
    ).show(10, truncate=False)

    print("\n=== TOTAL PREDICTIONS ===")
    print(df.count())

finally:
    spark.stop()
