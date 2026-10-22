
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pathlib import Path

# --------------------------------------------------
# 1. Start Spark
# --------------------------------------------------
spark = (
    SparkSession.builder
    .appName("TrustGate-Structured-Parquet")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# --------------------------------------------------
# 2. Define input and output paths
# --------------------------------------------------
input_path = (
    "hdfs://localhost:50000/trustgate/raw/iot23/"
    "CTU-IoT-Malware-Capture-20-1_conn.log.labeled"
)

output_path = str(
    Path.home() / "trustgate/processed/iot23_structured_sample"
)

# --------------------------------------------------
# 3. Define the 23 column names
# --------------------------------------------------
columns = [
    "ts", "uid", "src_ip", "src_port",
    "dst_ip", "dst_port", "proto", "service",
    "duration", "orig_bytes", "resp_bytes",
    "conn_state", "local_orig", "local_resp",
    "missed_bytes", "history", "orig_pkts",
    "orig_ip_bytes", "resp_pkts", "resp_ip_bytes",
    "tunnel_parents", "label", "detailed_label"
]

numeric_types = {
    "ts": "DOUBLE",
    "src_port": "INT",
    "dst_port": "INT",
    "duration": "DOUBLE",
    "orig_bytes": "BIGINT",
    "resp_bytes": "BIGINT",
    "missed_bytes": "BIGINT",
    "orig_pkts": "BIGINT",
    "orig_ip_bytes": "BIGINT",
    "resp_pkts": "BIGINT",
    "resp_ip_bytes": "BIGINT"
}

try:
    # --------------------------------------------------
    # 4. Read raw records and remove metadata
    # --------------------------------------------------
    raw_df = spark.read.text(input_path)

    data_df = raw_df.filter(
        (~F.col("value").startswith("#"))
        & (F.trim(F.col("value")) != "")
    )

    # --------------------------------------------------
    # 5. Keep a small sample
    # --------------------------------------------------
    sample_df = data_df.limit(1000)

    # Split each line on tabs.
    split_df = sample_df.withColumn(
        "parts",
        F.split(F.col("value"), "\t", -1)
    )

    # All records must have 21 tab-separated parts.
    bad_rows = split_df.filter(
        F.size(F.col("parts")) != 21
    ).count()

    if bad_rows:
        raise ValueError(
            f"Found {bad_rows} records with unexpected field counts."
        )

    # The last tab-separated part contains three values.
    tail = F.split(
        F.trim(F.element_at(F.col("parts"), 21)),
        r"\s+"
    )

    # Extract the first 20 fields and the final three fields.
    parsed_df = split_df.select(
        *[
            F.element_at(F.col("parts"), i).alias(columns[i - 1])
            for i in range(1, 21)
        ],
        F.element_at(tail, 1).alias("tunnel_parents"),
        F.element_at(tail, 2).alias("label"),
        F.element_at(tail, 3).alias("detailed_label")
    )

    # --------------------------------------------------
    # 6. Convert numeric fields safely
    # --------------------------------------------------
    for column_name, data_type in numeric_types.items():
        parsed_df = parsed_df.withColumn(
            column_name,
            F.expr(
                f"try_cast(`{column_name}` AS {data_type})"
            )
        )

    # Normalize labels for consistent comparisons.
    parsed_df = parsed_df.withColumn(
        "label",
        F.lower(F.trim(F.col("label")))
    )

    # --------------------------------------------------
    # 7. Validate parsed output before writing
    # --------------------------------------------------
    record_count = parsed_df.count()

    print("\n=== TRUSTGATE STRUCTURED DATA PIPELINE ===")
    print(f"Input: {input_path}")
    print(f"Output: {output_path}")
    print(f"Records parsed: {record_count}")
    print(f"Columns parsed: {len(parsed_df.columns)}")

    if record_count == 0:
        raise ValueError("No records available to write.")

    if len(parsed_df.columns) != 23:
        raise ValueError("Expected exactly 23 output columns.")

    # Check numeric conversion failures.
    conversion_failures = parsed_df.filter(
        F.col("ts").isNull()
        | F.col("src_port").isNull()
        | F.col("dst_port").isNull()
    ).count()

    print(f"Required numeric fields missing: {conversion_failures}")

    if conversion_failures:
        raise ValueError(
            "Some required numeric fields could not be converted."
        )

    # Check labels.
    parsed_df.groupBy("label").count().show(truncate=False)

    # --------------------------------------------------
    # 8. Write structured Parquet output
    # --------------------------------------------------
    parsed_df.write.mode("errorifexists").parquet(output_path)

    # --------------------------------------------------
    # 9. Read it back and verify
    # --------------------------------------------------
    check_df = spark.read.parquet(output_path)

    verified_count = check_df.count()

    print(f"Records verified: {verified_count}")
    print("Output schema:")
    check_df.printSchema()

    print("Example records:")
    check_df.select(
        "ts", "src_ip", "dst_ip", "proto",
        "service", "duration", "label", "detailed_label"
    ).show(5, truncate=False)

    if verified_count != record_count:
        raise ValueError("Written and verified counts do not match.")

    print("\nSUCCESS: Structured Parquet output verified.")

finally:
    spark.stop()

