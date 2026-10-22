from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# TRUSTGATE - IOT-23 TRAINING FEATURE ENGINEERING
# ============================================================

INPUT_PATH = (
    "/home/arshad/trustgate/processed/iot23_training_sample"
)

# New output path to preserve the existing feature output.
OUTPUT_PATH = (
    "/home/arshad/trustgate/processed/iot23_training_features_v2"
)


def is_missing(column_name):
    """
    Detect missing values represented as:
    - Spark NULL
    - Empty strings
    - Whitespace-only strings
    - Hyphen (-)

    Valid numeric zero values are NOT considered missing.
    """
    value = F.trim(F.col(column_name).cast("string"))

    return (
        F.col(column_name).isNull()
        | (value == "")
        | (value == "-")
    )


def main():
    output_dir = Path(OUTPUT_PATH)

    # Never overwrite an existing output automatically.
    if output_dir.exists():
        raise FileExistsError(
            f"Output directory already exists: {OUTPUT_PATH}\n"
            "Choose a new output directory before running again."
        )

    spark = (
        SparkSession.builder
        .appName("TrustGate-Training-Feature-Engineering")
        .getOrCreate()
    )

    try:
        print("=== TRUSTGATE TRAINING FEATURE ENGINEERING ===")
        print(f"Input:  {INPUT_PATH}")
        print(f"Output: {OUTPUT_PATH}")

        # ----------------------------------------------------
        # 1. Load the original training sample
        # ----------------------------------------------------
        df = spark.read.parquet(INPUT_PATH)

        input_count = df.count()
        print(f"\nInput records: {input_count}")

        required_columns = [
            "ts",
            "src_port",
            "dst_port",
            "duration",
            "orig_bytes",
            "resp_bytes",
            "missed_bytes",
            "orig_pkts",
            "orig_ip_bytes",
            "resp_pkts",
            "resp_ip_bytes",
            "proto",
            "service",
            "conn_state",
            "history",
            "local_orig",
            "local_resp",
            "label",
            "source_capture",
        ]

        missing_columns = [
            column
            for column in required_columns
            if column not in df.columns
        ]

        if missing_columns:
            raise ValueError(
                f"Required columns are missing: {missing_columns}"
            )

        # ----------------------------------------------------
        # 2. Normalize labels and retain binary classes
        # ----------------------------------------------------
        df = df.withColumn(
            "label",
            F.lower(F.trim(F.col("label").cast("string")))
        )

        df = df.filter(
            F.col("label").isin("benign", "malicious")
        )

        # ----------------------------------------------------
        # 3. Create missingness indicators BEFORE conversion
        # ----------------------------------------------------
        traffic_columns = [
            "duration",
            "orig_bytes",
            "resp_bytes",
        ]

        missing_conditions = [
            is_missing(column)
            for column in traffic_columns
        ]

        df = df.withColumn(
            "traffic_measurements_missing",
            F.when(
                missing_conditions[0]
                | missing_conditions[1]
                | missing_conditions[2],
                1,
            ).otherwise(0)
        )

        for column in traffic_columns:
            df = df.withColumn(
                f"{column}_missing",
                F.when(is_missing(column), 1).otherwise(0)
            )

        # ----------------------------------------------------
        # 4. Convert numeric columns safely
        # ----------------------------------------------------
        numeric_columns = [
            "src_port",
            "dst_port",
            "duration",
            "orig_bytes",
            "resp_bytes",
            "missed_bytes",
            "orig_pkts",
            "orig_ip_bytes",
            "resp_pkts",
            "resp_ip_bytes",
        ]

        integer_columns = [
            "src_port",
            "dst_port",
        ]

        for column in numeric_columns:
            cleaned_value = F.when(
                is_missing(column),
                F.lit(None).cast("string")
            ).otherwise(
                F.trim(F.col(column).cast("string"))
            )

            target_type = (
                "int" if column in integer_columns else "double"
            )

            df = df.withColumn(
                column,
                cleaned_value.cast(target_type)
            )

        # ----------------------------------------------------
        # 5. Normalize categorical values
        # ----------------------------------------------------
        categorical_columns = [
            "proto",
            "service",
            "conn_state",
            "history",
            "local_orig",
            "local_resp",
        ]

        for column in categorical_columns:
            df = df.withColumn(
                column,
                F.when(
                    is_missing(column),
                    F.lit("unknown")
                ).otherwise(
                    F.lower(
                        F.trim(F.col(column).cast("string"))
                    )
                )
            )

        # ----------------------------------------------------
        # 6. Derive timestamp features
        # ----------------------------------------------------
        df = df.withColumn(
            "_event_time",
            F.timestamp_seconds(
                F.col("ts").cast("double")
            )
        )

        df = df.withColumn(
            "event_hour",
            F.hour(F.col("_event_time"))
        )

        df = df.withColumn(
            "event_day_of_week",
            F.dayofweek(F.col("_event_time"))
        )

        # ----------------------------------------------------
        # 7. Remove identifiers and leakage-prone columns
        # ----------------------------------------------------
        columns_to_drop = [
            "uid",
            "src_ip",
            "dst_ip",
            "ts",
            "tunnel_parents",
            "detailed_label",
            "_event_time",
        ]

        existing_drop_columns = [
            column
            for column in columns_to_drop
            if column in df.columns
        ]

        df = df.drop(*existing_drop_columns)

        # Keep source_capture for capture-based train/test splitting.
        # Do NOT use source_capture as a model feature.

        # ----------------------------------------------------
        # 8. Report label distribution
        # ----------------------------------------------------
        output_count = df.count()

        print(f"\nOutput records: {output_count}")
        print(f"Output columns: {len(df.columns)}")

        print("\n=== LABEL DISTRIBUTION ===")
        df.groupBy("label").count().orderBy("label").show()

        print("\n=== CAPTURE DISTRIBUTION ===")
        df.groupBy("source_capture").count().orderBy(
            F.desc("count")
        ).show(20, truncate=False)

        # ----------------------------------------------------
        # 9. Verify missingness indicators
        # ----------------------------------------------------
        print("\n=== MISSINGNESS SUMMARY ===")
        df.groupBy(
            "traffic_measurements_missing"
        ).count().orderBy(
            "traffic_measurements_missing"
        ).show()

        print("\n=== INDIVIDUAL MISSINGNESS COUNTS ===")
        df.agg(
            *[
                F.sum(F.col(f"{column}_missing")).alias(
                    f"{column}_missing_count"
                )
                for column in traffic_columns
            ]
        ).show(truncate=False)

        # ----------------------------------------------------
        # 10. Verify remaining numeric nulls
        # ----------------------------------------------------
        print("\n=== NUMERIC NULL COUNTS ===")

        feature_numeric_columns = [
            "src_port",
            "dst_port",
            "duration",
            "orig_bytes",
            "resp_bytes",
            "missed_bytes",
            "orig_pkts",
            "orig_ip_bytes",
            "resp_pkts",
            "resp_ip_bytes",
            "event_hour",
            "event_day_of_week",
        ]

        df.agg(
            *[
                F.sum(
                    F.when(F.col(column).isNull(), 1).otherwise(0)
                ).alias(column)
                for column in feature_numeric_columns
            ]
        ).show(truncate=False)

        # ----------------------------------------------------
        # 11. Save corrected features to a NEW directory
        # ----------------------------------------------------
        (
            df.write
            .mode("errorifexists")
            .parquet(OUTPUT_PATH)
        )

        # ----------------------------------------------------
        # 12. Read back and verify saved data
        # ----------------------------------------------------
        saved_df = spark.read.parquet(OUTPUT_PATH)
        saved_count = saved_df.count()

        print(f"\nVerified saved records: {saved_count}")
        print(f"Saved to: {OUTPUT_PATH}")

        if output_count != input_count:
            raise RuntimeError(
                "Record count changed. Investigate filtered labels."
            )

        if saved_count != output_count:
            raise RuntimeError(
                "Saved record count does not match output count."
            )

        print(
            "\nSUCCESS: Corrected training feature engineering "
            "completed."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
