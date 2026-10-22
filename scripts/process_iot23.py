
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ============================================================
# TrustGate: IoT-23 Batch Validation
# HDFS input only. No dataset output is written.
# ============================================================

HDFS_BASE = "hdfs://localhost:50000"
RAW_DIR = "/trustgate/raw/iot23"

COLUMNS = [
    "ts", "uid", "src_ip", "src_port",
    "dst_ip", "dst_port", "proto", "service",
    "duration", "orig_bytes", "resp_bytes",
    "conn_state", "local_orig", "local_resp",
    "missed_bytes", "history", "orig_pkts",
    "orig_ip_bytes", "resp_pkts", "resp_ip_bytes",
    "tunnel_parents", "label", "detailed_label"
]

NUMERIC_TYPES = {
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


def main():
    spark = (
        SparkSession.builder
        .appName("TrustGate-IoT23-Batch-Validation")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Explicitly connect to HDFS rather than the local filesystem.
        hadoop_conf = spark._jsc.hadoopConfiguration()
        hdfs_uri = spark._jvm.java.net.URI(HDFS_BASE)

        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            hdfs_uri, hadoop_conf
        )

        hdfs_dir = spark._jvm.org.apache.hadoop.fs.Path(RAW_DIR)

        if not fs.exists(hdfs_dir):
            raise FileNotFoundError(
                f"HDFS directory not found: {HDFS_BASE}{RAW_DIR}"
            )

        files = sorted(
            status.getPath().toString()
            for status in fs.listStatus(hdfs_dir)
            if status.isFile()
        )

        print("\n========== TRUSTGATE IoT-23 VALIDATION ==========")
        print(f"HDFS directory: {HDFS_BASE}{RAW_DIR}")
        print(f"Files discovered: {len(files)}")

        results = []

        for index, file_path in enumerate(files, start=1):
            filename = file_path.rsplit("/", 1)[-1]

            print(f"\n[{index}/{len(files)}] Validating: {filename}")

            try:
                raw_df = spark.read.text(file_path)

                # Remove metadata and empty lines.
                data_df = raw_df.filter(
                    (~F.col("value").startswith("#")) &
                    (F.trim(F.col("value")) != "")
                )

                # Split each record on tabs.
                split_df = data_df.withColumn(
                    "parts",
                    F.split(F.col("value"), "\t", -1)
                )

                # Validate the expected number of tab-separated fields.
                bad_rows = split_df.filter(
                    F.size(F.col("parts")) != 21
                ).count()

                if bad_rows > 0:
                    results.append((
                        filename, 0, 0, 0, 0, 0,
                        bad_rows, "FAILED"
                    ))
                    print(
                        f"FAILED: {bad_rows} records have unexpected "
                        "field counts."
                    )
                    continue

                # The final tab-separated field contains three values.
                tail = F.split(
                    F.trim(F.element_at(F.col("parts"), 21)),
                    r"\s+"
                )

                parsed_df = split_df.select(
                    *[
                        F.element_at(F.col("parts"), i).alias(
                            COLUMNS[i - 1]
                        )
                        for i in range(1, 21)
                    ],
                    F.element_at(tail, 1).alias("tunnel_parents"),
                    F.element_at(tail, 2).alias("label"),
                    F.element_at(tail, 3).alias("detailed_label")
                )

                # Convert numeric columns safely.
                for column_name, data_type in NUMERIC_TYPES.items():
                    parsed_df = parsed_df.withColumn(
                        column_name,
                        F.expr(
                            f"try_cast(`{column_name}` AS {data_type})"
                        )
                    )

                # Normalize label case and whitespace for comparisons.
                # This does not change the raw records in HDFS.
                normalized_label = F.lower(F.trim(F.col("label")))

                summary = parsed_df.agg(
                    F.count("*").alias("total"),

                    F.sum(
                        F.when(normalized_label == "benign", 1)
                        .otherwise(0)
                    ).alias("benign"),

                    F.sum(
                        F.when(normalized_label == "malicious", 1)
                        .otherwise(0)
                    ).alias("malicious"),

                    F.sum(
                        F.when(
                            F.col("label").isNull() |
                            (F.trim(F.col("label")) == ""),
                            1
                        ).otherwise(0)
                    ).alias("missing_labels"),

                    F.sum(
                        F.when(
                            ~normalized_label.isin("benign", "malicious") &
                            F.col("label").isNotNull() &
                            (F.trim(F.col("label")) != ""),
                            1
                        ).otherwise(0)
                    ).alias("other_labels")
                ).first()

                total = summary["total"] or 0
                benign = summary["benign"] or 0
                malicious = summary["malicious"] or 0
                missing = summary["missing_labels"] or 0
                other = summary["other_labels"] or 0

                status = (
                    "PASS"
                    if missing == 0 and other == 0
                    else "CHECK"
                )

                results.append((
                    filename, total, benign, malicious,
                    missing, other, 0, status
                ))

                print(
                    f"{status}: records={total}, benign={benign}, "
                    f"malicious={malicious}, missing={missing}, "
                    f"other_labels={other}"
                )

            except Exception as exc:
                results.append((
                    filename, 0, 0, 0, 0, 0, -1, "ERROR"
                ))
                print(f"ERROR: {filename}: {str(exc)[:400]}")

        # ----------------------------------------------------
        # Per-file report
        # ----------------------------------------------------
        print("\n========== PER-FILE VALIDATION REPORT ==========")

        schema = [
            "file", "records", "benign", "malicious",
            "missing_labels", "other_labels",
            "bad_field_rows", "status"
        ]

        if results:
            report = spark.createDataFrame(results, schema)

            report.orderBy("file").show(30, truncate=False)

            # ------------------------------------------------
            # Overall report
            # ------------------------------------------------
            print("\n========== OVERALL SUMMARY ==========")

            report.agg(
                F.count("*").alias("files_checked"),
                F.sum("records").alias("total_records"),
                F.sum("benign").alias("benign_records"),
                F.sum("malicious").alias("malicious_records"),
                F.sum("missing_labels").alias("missing_labels"),
                F.sum("other_labels").alias("other_labels"),
                F.sum("bad_field_rows").alias("bad_field_rows"),
                F.sum(
                    F.when(F.col("status") == "PASS", 1)
                    .otherwise(0)
                ).alias("passed_files"),
                F.sum(
                    F.when(F.col("status") != "PASS", 1)
                    .otherwise(0)
                ).alias("files_needing_review")
            ).show(truncate=False)

        print("\nValidation finished.")
        print("No dataset output was written or modified.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
