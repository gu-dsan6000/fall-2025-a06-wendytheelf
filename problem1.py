#!/usr/bin/env python3
import os, sys, time, argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, count, rand

def create_spark_session(master_url: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName("SparkLogLevelDistribution")
        .master(master_url)
        .config("spark.executor.memory", "2g")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.cores", "1")
        .config("spark.cores.max", "3")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

def run_log_analysis(spark: SparkSession, input_path: str, outdir: str):
    t0 = time.time()
    os.makedirs(outdir, exist_ok=True)

    logs_df = spark.read.text(input_path)
    total_lines = logs_df.count()

    pattern = r"\b(INFO|WARN|ERROR|DEBUG)\b"
    filtered = logs_df.withColumn("log_level", regexp_extract(col("value"), pattern, 1)) \
                      .filter(col("log_level") != "")
    matched_lines = filtered.count()

    # counts
    counts_df = filtered.groupBy("log_level").agg(count("*").alias("count"))
    counts_pdf = counts_df.orderBy(col("count").desc()).toPandas()  # 小表 collect 回 Driver

    # sample
    sample_df = filtered.orderBy(rand()).select(col("value").alias("log_entry"), col("log_level")).limit(10)
    sample_pdf = sample_df.toPandas()

    counts_path  = os.path.join(outdir, "problem1_counts.csv")
    sample_path  = os.path.join(outdir, "problem1_sample.csv")
    summary_path = os.path.join(outdir, "problem1_summary.txt")

    with open(counts_path, "w", encoding="utf-8") as f:
        f.write("log_level,count\n")
        for _, r in counts_pdf.iterrows():
            f.write(f"{r['log_level']},{int(r['count'])}\n")

    with open(sample_path, "w", encoding="utf-8") as f:
        f.write("log_entry,log_level\n")
        for _, r in sample_pdf.iterrows():
            log_entry = str(r["log_entry"]).replace('"', '""')
            f.write(f"\"{log_entry}\",{r['log_level']}\n")

    # summary
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(f"Total log lines processed: {total_lines:,}\n")
        f.write(f"Total lines with log levels: {matched_lines:,}\n")
        f.write(f"Unique log levels found: {len(counts_pdf)}\n\n")
        f.write("Log level distribution:\n")
        total = max(1, matched_lines)
        for _, r in counts_pdf.iterrows():
            pct = (int(r["count"]) / total) * 100
            f.write(f"  {r['log_level']:<5}: {int(r['count']):>10,} ({pct:5.2f}%)\n")
        f.write(f"\nProcessing time: {time.time() - t0:.2f} seconds\n")

    print("✅ Wrote files:")
    print("  -", counts_path)
    print("  -", sample_path)
    print("  -", summary_path)

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--master", required=True, help="spark://<MASTER_PRIVATE_IP>:7077")
    p.add_argument("--input",  required=True, help="e.g., s3a://athena-lh1078/data/a06/raw/")
    p.add_argument("--outdir", required=True, help="e.g., ~/spark-cluster/data/output/problem1-full")
    return p.parse_args()

def main():
    args = parse_args()
    spark = create_spark_session(args.master)
    try:
        run_log_analysis(spark, args.input, os.path.expanduser(args.outdir))
    finally:
        spark.stop()

if __name__ == "__main__":
    sys.exit(main())
