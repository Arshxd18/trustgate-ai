
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    input_path = str(
        Path.home() / "trustgate/processed/iot23_structured_sample"
    )
    output_path = str(
        Path.home() / "trustgate/processed/iot23_features_sample"
    )

    spark = (
        SparkSession.builder
        .appName("TrustGate-Feature-Engineering")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("\n=== TRUSTGATE FEATURE ENGINEERING ===")
        print(f"Input:  {input_path}")
        print(f"Output: {output_path}")

        # Read the verified structured sample.
        df = spark.read.parquet(input_path)

        original_count = df.count()
        print(f"\nInput records: {original_count}")

        # Normalize labels and represent missing categorical values explicitly.
        df = df.withColumn(
            "label",
            F.lower(F.trim(F.col("label")))
        )

        categorical_columns = [
            "proto",
            "service",
            "conn_state",
            "history",
            "local_orig",
            "local_resp",
        ]

        for column_name in categorical_columns:
            df = df.withColumn(
                column_name,
                F.when(
                    F.col(column_name).isNull()
                    | (F.trim(F.col(column_name)) == "")
                    | (F.trim(F.col(column_name)) == "-"),
                    F.lit("unknown"),
                ).otherwise(F.trim(F.col(column_name)))
            )

        # Preserve the fact that duration and byte counts were missing.
        df = df.withColumn(
            "traffic_measurements_missing",
            F.when(
                F.col("duration").isNull()
                & F.col("orig_bytes").isNull()
                & F.col("resp_bytes").isNull(),
                1,
            ).otherwise(0)
        )

        # Convert timestamp into UTC hour-of-day and day-of-week features.
        # The epoch timestamp is in seconds.
        df = df.withColumn(
            "event_time",
            F.to_timestamp(
                F.from_unixtime(F.col("ts"))
            )
        )

        df = df.withColumn(
            "event_hour",
            F.hour(F.col("event_time"))
        ).withColumn(
            "event_day_of_week",
            F.dayofweek(F.col("event_time"))
        )

        # Drop raw identifiers and addresses from the initial model features.
        # Keep the ground-truth label for supervised training and evaluation.
        df = df.drop(
            "uid",
            "src_ip",
            "dst_ip",
            "ts",
            "tunnel_parents",
            "detailed_label",
            "event_time",
        )

        # Do not impute here. Fit imputation values on training data only
        # during the model-training stage to avoid data leakage.
        record_count = df.count()

        if record_count != original_count:
            raise ValueError(
                "Record count changed unexpectedly during feature engineering."
            )

        if "label" not in df.columns:
            raise ValueError("Ground-truth label is missing.")

        if "detailed_label" in df.columns:
            raise ValueError(
                "Target leakage risk: detailed_label remains in the features."
            )

        if "traffic_measurements_missing" not in df.columns:
            raise ValueError("Missing-value indicator was not created.")

        print(f"Output records: {record_count}")
        print(f"Output columns: {len(df.columns)}")
        print("\nFeature columns:")
        print(df.columns)

        print("\n=== LABEL DISTRIBUTION ===")
        df.groupBy("label").count().orderBy("label").show(
            truncate=False
        )

        print("\n=== MISSING-VALUE INDICATOR ===")
        df.groupBy("traffic_measurements_missing").count().show()

        print("\n=== CHECK REMAINING NULLS IN NUMERIC FEATURES ===")
        numeric_columns = [
            "duration",
            "orig_bytes",
            "resp_bytes",
            "orig_pkts",
            "resp_pkts",
            "src_port",
            "dst_port",
            "missed_bytes",
            "event_hour",
            "event_day_of_week",
        ]

        df.select(
            *[
                F.sum(
                    F.when(F.col(c).isNull(), 1).otherwise(0)
                ).alias(c)
                for c in numeric_columns
                if c in df.columns
            ]
        ).show(truncate=False)

        print("\n=== FEATURE EXAMPLES ===")
        df.show(10, truncate=False)

        # Fail safely if the output already exists.
        # Never overwrite a previous run automatically.
        output = Path(output_path)
        if output.exists():
            raise FileExistsError(
                f"Output already exists: {output_path}. "
                "Inspect it before choosing another output path."
            )

        df.write.mode("errorifexists").parquet(output_path)

        # Verify the saved Parquet output.
        saved_df = spark.read.parquet(output_path)
        saved_count = saved_df.count()

        if saved_count != original_count:
            raise ValueError(
                f"Verification failed: expected {original_count}, "
                f"found {saved_count}."
            )

        print(f"\nVerified saved records: {saved_count}")
        print("SUCCESS: Feature-engineering sample created.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
