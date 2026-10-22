
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    input_path = str(
        Path.home() / "trustgate/processed/iot23_structured_sample"
    )

    spark = (
        SparkSession.builder
        .appName("TrustGate-IoT23-Sample-Inspection")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("\n=== TRUSTGATE: STRUCTURED SAMPLE INSPECTION ===")
        print(f"Input: {input_path}")

        df = spark.read.parquet(input_path)

        total = df.count()
        print(f"\nTotal records: {total}")
        print(f"Total columns: {len(df.columns)}")

        print("\n=== LABEL DISTRIBUTION ===")
        df.groupBy("label").count().orderBy("label").show(
            truncate=False
        )

        print("\n=== NUMERIC FEATURE STATISTICS ===")
        df.select(
            "duration",
            "orig_bytes",
            "resp_bytes",
            "orig_pkts",
            "resp_pkts",
        ).describe().show(truncate=False)

        print("\n=== NULL COUNTS IN KEY FEATURES ===")
        key_columns = [
            "duration",
            "orig_bytes",
            "resp_bytes",
            "orig_pkts",
            "resp_pkts",
        ]

        df.select(
            *[
                F.sum(
                    F.when(F.col(c).isNull(), 1).otherwise(0)
                ).alias(c)
                for c in key_columns
            ]
        ).show(truncate=False)

        print("\n=== SAMPLE RECORDS ===")  
        print("\n=== CONNECTION STATE VS MISSING VALUES ===")

        df.withColumn(
            "traffic_fields_missing",
            F.when(
                F.col("duration").isNull()
                & F.col("orig_bytes").isNull()
                & F.col("resp_bytes").isNull(),
                "missing",
            ).otherwise("present"),
        ).groupBy(
            "conn_state", "traffic_fields_missing", "label"
        ).count().orderBy(
            "conn_state", "traffic_fields_missing", "label"
        ).show(50, truncate=False)

        print("\n=== CONNECTION STATE SUMMARY ===")

        df.groupBy("conn_state").agg(
            F.count("*").alias("records"),
            F.sum(
                F.when(F.col("duration").isNull(), 1).otherwise(0)
            ).alias("missing_duration"),
            F.sum(
                F.when(F.col("duration").isNotNull(), 1).otherwise(0)
            ).alias("present_duration"),
        ).orderBy(
            F.desc("records")
        ).show(30, truncate=False)

        print("\n=== ORIGINAL RECORDS WITH MISSING VALUES ===")

        df.filter(
            F.col("duration").isNull()
            & F.col("orig_bytes").isNull()
            & F.col("resp_bytes").isNull()
        ).select(
            "ts",
            "uid",
            "src_ip",
            "src_port",
            "dst_ip",
            "dst_port",
            "proto",
            "service",
            "conn_state",
            "orig_pkts",
            "resp_pkts",
            "label",
            "detailed_label",
        ).show(10, truncate=False)

        print("\n=== ORIGINAL RECORDS WITH VALUES PRESENT ===")

        df.filter(
            F.col("duration").isNotNull()
            & F.col("orig_bytes").isNotNull()
            & F.col("resp_bytes").isNotNull()
        ).select(
            "ts",
            "uid",
            "src_ip",
            "src_port",
            "dst_ip",
            "dst_port",
            "proto",
            "service",
            "duration",
            "orig_bytes",
            "resp_bytes",
            "conn_state",
            "orig_pkts",
            "resp_pkts",
            "label",
        ).show(10, truncate=False)

        print("\n=== MISSING-VALUE PATTERNS ===")

        missing_df = df.withColumn(
            "numeric_fields_missing",
            F.when(
                F.col("duration").isNull()
                & F.col("orig_bytes").isNull()
                & F.col("resp_bytes").isNull(),
                "all_three_missing",
            ).when(
                F.col("duration").isNull()
                | F.col("orig_bytes").isNull()
                | F.col("resp_bytes").isNull(),
                "some_missing",
            ).otherwise("none_missing"),
        )

        missing_df.groupBy(
            "numeric_fields_missing", "label"
        ).count().orderBy(
            "numeric_fields_missing", "label"
        ).show(truncate=False)

        print("\n=== MISSING VALUES BY LABEL ===")

        missing_df.groupBy("label").agg(
            F.sum(
                F.when(F.col("duration").isNull(), 1).otherwise(0)
            ).alias("missing_duration"),
            F.sum(
                F.when(F.col("orig_bytes").isNull(), 1).otherwise(0)
            ).alias("missing_orig_bytes"),
            F.sum(
                F.when(F.col("resp_bytes").isNull(), 1).otherwise(0)
            ).alias("missing_resp_bytes"),
        ).show(truncate=False)

        df.select(
            "proto",
            "service",
            "duration",
            "orig_bytes",
            "resp_bytes",
            "orig_pkts",
            "resp_pkts",
            "label",
        ).show(10, truncate=False)

        print("\nSUCCESS: Inspection completed.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

