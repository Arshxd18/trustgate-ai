
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


HDFS_BASE = "hdfs://localhost:50000"
RAW_DIR = f"{HDFS_BASE}/trustgate/raw/iot23"

OUTPUT_DIR = str(
    Path.home() / "trustgate/processed/iot23_training_sample"
)

# Fixed seed makes the sampling reproducible.
SEED = 42

# Initial limits for a manageable prototype.
# These are per capture, not global class quotas.
MAX_ROWS_PER_CAPTURE = 100_000

# Selected captures include known benign and malicious scenarios.
CAPTURES = [
    "CTU-Honeypot-Capture-4-1_conn.log.labeled",
    "CTU-Honeypot-Capture-5-1_conn.log.labeled",
    "CTU-Honeypot-Capture-7-1_conn.log.labeled",
    "CTU-IoT-Malware-Capture-1-1_conn.log.labeled",
    "CTU-IoT-Malware-Capture-20-1_conn.log.labeled",
    "CTU-IoT-Malware-Capture-21-1_conn.log.labeled",
    "CTU-IoT-Malware-Capture-3-1_conn.log.labeled",
    "CTU-IoT-Malware-Capture-34-1_conn.log.labeled",
    "CTU-IoT-Malware-Capture-42-1_conn.log.labeled",
    "CTU-IoT-Malware-Capture-44-1_conn.log.labeled",
    "CTU-IoT-Malware-Capture-8-1_conn.log.labeled",
]

FIRST_20_COLUMNS = [
    "ts", "uid", "src_ip", "src_port",
    "dst_ip", "dst_port", "proto", "service",
    "duration", "orig_bytes", "resp_bytes",
    "conn_state", "local_orig", "local_resp",
    "missed_bytes", "history", "orig_pkts",
    "orig_ip_bytes", "resp_pkts", "resp_ip_bytes",
]


def parse_capture(spark, file_path):
    raw = spark.read.text(file_path)

    data = raw.filter(
        (~F.col("value").startswith("#"))
        & (F.trim(F.col("value")) != "")
    )

    split_df = data.withColumn(
        "parts",
        F.split(F.col("value"), "\t", -1)
    )

    # Fail safely if this capture has an unexpected format.
    bad_count = split_df.filter(
        F.size("parts") != 21
    ).limit(1).count()

    if bad_count:
        raise ValueError(
            f"Unexpected record format in {file_path}"
        )

    tail = F.split(
        F.trim(F.element_at(F.col("parts"), 21)),
        r"\s+"
    )

    parsed = split_df.select(
        *[
            F.element_at(F.col("parts"), i).alias(name)
            for i, name in enumerate(FIRST_20_COLUMNS, start=1)
        ],
        F.element_at(tail, 1).alias("tunnel_parents"),
        F.lower(
            F.trim(F.element_at(tail, 2))
        ).alias("label"),
        F.element_at(tail, 3).alias("detailed_label"),
    )

    # Only recognized ground-truth classes are retained.
    parsed = parsed.filter(
        F.col("label").isin("benign", "malicious")
    )

    return parsed


def main():
    spark = (
        SparkSession.builder
        .appName("TrustGate-Training-Sample")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        output = Path(OUTPUT_DIR)

        if output.exists():
            raise FileExistsError(
                f"Output already exists: {OUTPUT_DIR}. "
                "Inspect it before choosing a new destination."
            )

        # Use the Hadoop filesystem API to check HDFS inputs.
        conf = spark._jsc.hadoopConfiguration()
        uri = spark._jvm.java.net.URI(HDFS_BASE)
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            uri, conf
        )

        frames = []

        print("\n=== TRUSTGATE TRAINING SAMPLE ===")
        print(f"Selected captures: {len(CAPTURES)}")
        print(f"Per-capture maximum: {MAX_ROWS_PER_CAPTURE:,}")

        for filename in CAPTURES:
            path = f"{RAW_DIR}/{filename}"
            hdfs_path = spark._jvm.org.apache.hadoop.fs.Path(path)

            if not fs.exists(hdfs_path):
                print(f"SKIP: not found: {filename}")
                continue

            print(f"\nReading: {filename}")

            parsed = parse_capture(spark, path)

            # Use a fixed random seed and cap each capture.
            sampled = (
                parsed
                .orderBy(F.rand(SEED))
                .limit(MAX_ROWS_PER_CAPTURE)
                .withColumn("source_capture", F.lit(filename))
            )

            frames.append(sampled)

        if not frames:
            raise RuntimeError("No usable captures were found.")

        combined = frames[0]
        for frame in frames[1:]:
            combined = combined.unionByName(frame)

        print("\n=== SAMPLE LABEL DISTRIBUTION ===")
        combined.groupBy("label").count().show(truncate=False)

        print("\n=== CAPTURE CONTRIBUTIONS ===")
        combined.groupBy(
            "source_capture", "label"
        ).count().orderBy(
            "source_capture", "label"
        ).show(100, truncate=False)

        # Save one compact dataset. Avoid a second full-size HDFS copy.
        combined.write.mode("errorifexists").parquet(OUTPUT_DIR)

        saved = spark.read.parquet(OUTPUT_DIR)
        saved_count = saved.count()

        print(f"\nSaved dataset: {OUTPUT_DIR}")
        print(f"Verified saved records: {saved_count}")
        print("SUCCESS: Training sample created.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
