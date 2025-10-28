#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Problem 2: Cluster Usage Analysis

Usage:
  # 叢集完整版（約 10-20 分鐘）
  uv run python problem2.py spark://<MASTER_PRIVATE_IP>:7077 --net-id YOUR_NET_ID

  # 指定輸入路徑（覆蓋 --net-id）
  uv run python problem2.py spark://<MASTER_PRIVATE_IP>:7077 --input s3a://athena-YOUR_NET_ID/data/a06/raw/*

  # 跳過 Spark，僅用既有 CSV 重製圖（快）
  uv run python problem2.py --skip-spark
"""

import os
import sys
import math
import argparse
import shutil
from datetime import timedelta
from typing import Optional


# ---------- small utils ----------

def ensure_file_target(path: str):
    """若 path 已存在且是資料夾（過去用 .write.csv() 造成），先刪除；並保證上層資料夾存在。"""
    path = os.path.expanduser(path)
    if os.path.isdir(path):
        shutil.rmtree(path)
    os.makedirs(os.path.dirname(path), exist_ok=True)


# ---------- spark session ----------

def safe_import_spark(master_url: Optional[str]):
    from pyspark.sql import SparkSession

    spark_builder = (
        SparkSession.builder
        .appName("Problem2_ClusterUsage")
    )
    if master_url:
        spark_builder = spark_builder.master(master_url)

    # S3A 與穩健性設定
    spark = (
        spark_builder
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.ansi.enabled", "false")  # 避免嚴格模式在轉型時直接丟錯
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        # 新版 IAM Instance Profile provider
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider")
        # Requester Pays（課程教學桶常會需要）
        .config("spark.hadoop.fs.s3a.request.payer", "requester")
        .getOrCreate()
    )
    return spark


# ---------- args ----------

def build_arg_parser():
    p = argparse.ArgumentParser(description="Problem 2: Cluster Usage Analysis")
    p.add_argument("master_url", nargs="?", default=None,
                   help="Spark master URL, e.g., spark://<MASTER_PRIVATE_IP>:7077")
    p.add_argument("--net-id", default=None,
                   help="Your net id, used to form s3a://athena-<netid>/data/a06/raw/* if --input not provided")
    p.add_argument("--input", default=None,
                   help="Explicit input path (e.g., s3a://athena-<netid>/data/a06/raw/*). Overrides --net-id.")
    p.add_argument("--outdir", default="data/output",
                   help="Local output dir on the driver/master, default: data/output")
    p.add_argument("--skip-spark", action="store_true", default=False,
                   help="Skip Spark processing and regenerate figures from existing CSVs")
    return p


# ---------- core ----------

def run_spark(master_url: Optional[str], input_path: str, outdir: str):
    """主流程（Spark 解析 → 產出 timeline / cluster summary / stats 與圖）"""
    spark = safe_import_spark(master_url)
    from pyspark.sql import functions as F
    from pyspark.sql.functions import regexp_extract, col, min as fmin, max as fmax, lpad

    print(f"✅ Using input: {input_path}")
    print("⏳ Reading text files ...")
    df = spark.read.text(input_path).withColumnRenamed("value", "line")

    # 1) 解析時間戳（格式如 17/03/29 10:04:41）
    ts_pattern = r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})'
    df = df.withColumn("ts_str", regexp_extract(col("line"), ts_pattern, 1))

    # 用 SQL 表達式版本的 try_to_timestamp，避免 import/解析落差
    # 任何不符合的時間戳會得到 NULL，不會炸掉
    df = df.withColumn(
        "ts",
        F.expr("try_to_timestamp(ts_str, 'yy/MM/dd HH:mm:ss')")
    )

    # 2) 抓 application_id 與 cluster_id
    # application_13位數_clusterCounter
    app_pat = r'(application_(\d{13})_(\d+))'
    df = (
        df.withColumn("application_id", regexp_extract(col("line"), app_pat, 1))
          .withColumn("cluster_id",     regexp_extract(col("line"), app_pat, 2))
          .withColumn("app_num_raw",    regexp_extract(col("line"), app_pat, 3))
    )

    # 3) 只留有 ts 與 application_id 的行（濾掉無效時間戳與缺 application 的行）
    df_apps = df.filter((col("ts").isNotNull()) & (col("application_id") != ""))

    # 若沒有任何有效資料，提早結束並寫出空檔以利 --skip-spark 後續流程
    if df_apps.rdd.isEmpty():
        os.makedirs(outdir, exist_ok=True)
        import pandas as pd
        empty_timeline = pd.DataFrame(columns=["cluster_id", "application_id", "app_number", "start_time", "end_time"])
        empty_cluster  = pd.DataFrame(columns=["cluster_id", "num_applications", "cluster_first_app", "cluster_last_app"])

        timeline_path = os.path.join(outdir, "problem2_timeline.csv")
        cluster_summary_path = os.path.join(outdir, "problem2_cluster_summary.csv")
        stats_path = os.path.join(outdir, "problem2_stats.txt")

        ensure_file_target(timeline_path)
        ensure_file_target(cluster_summary_path)
        ensure_file_target(stats_path)

        empty_timeline.to_csv(timeline_path, index=False)
        empty_cluster.to_csv(cluster_summary_path, index=False)
        with open(stats_path, "w", encoding="utf-8") as f:
            f.write("Total unique clusters: 0\nTotal applications: 0\nAverage applications per cluster: 0.00\n\nMost heavily used clusters:\n")

        print("⚠️ 沒有偵測到有效的 application 記錄；已輸出空白結果。")
        spark.stop()
        return

    # 4) 以 application_id 為單位取 start/end
    app_agg = (
        df_apps.groupBy("application_id", "cluster_id")
               .agg(
                    fmin("ts").alias("start_time"),
                    fmax("ts").alias("end_time"),
                    F.count(F.lit(1)).alias("num_lines")
               )
    )

    # 整理 app_number（補零到四位）
    app_agg = app_agg.withColumn(
        "app_number",
        lpad(regexp_extract(col("application_id"), r'application_\d{13}_(\d+)', 1), 4, "0")
    )

    # 5) 產生 timeline（DataFrame → Pandas → CSV）
    timeline_cols = ["cluster_id", "application_id", "app_number", "start_time", "end_time"]
    timeline_pdf = app_agg.select(*timeline_cols).orderBy("cluster_id", "start_time").toPandas()

    # 6) 產生 cluster summary
    cluster_agg = (
        app_agg.groupBy("cluster_id")
               .agg(
                   F.count("application_id").alias("num_applications"),
                   fmin("start_time").alias("cluster_first_app"),
                   fmax("end_time").alias("cluster_last_app"),
               )
               .orderBy(F.col("num_applications").desc(), F.col("cluster_id"))
    )
    cluster_pdf = cluster_agg.toPandas()

    # 7) 輸出 CSV、TXT
    os.makedirs(outdir, exist_ok=True)
    timeline_path = os.path.join(outdir, "problem2_timeline.csv")
    cluster_summary_path = os.path.join(outdir, "problem2_cluster_summary.csv")
    stats_path = os.path.join(outdir, "problem2_stats.txt")

    ensure_file_target(timeline_path)
    ensure_file_target(cluster_summary_path)
    ensure_file_target(stats_path)

    timeline_pdf.to_csv(timeline_path, index=False)
    cluster_pdf.to_csv(cluster_summary_path, index=False)

    # 8) 文字統計
    total_clusters = cluster_pdf["cluster_id"].nunique() if len(cluster_pdf) else 0
    total_apps = timeline_pdf["application_id"].nunique() if len(timeline_pdf) else 0
    avg_apps = (total_apps / total_clusters) if total_clusters else 0.0

    cluster_sorted = cluster_pdf.sort_values("num_applications", ascending=False) if len(cluster_pdf) else cluster_pdf
    top_lines = []
    if len(cluster_sorted):
        for _, row in cluster_sorted.head(10).iterrows():
            top_lines.append(f"  Cluster {row['cluster_id']}: {int(row['num_applications'])} applications")

    with open(stats_path, "w", encoding="utf-8") as f:
        f.write(f"Total unique clusters: {total_clusters}\n")
        f.write(f"Total applications: {total_apps}\n")
        f.write(f"Average applications per cluster: {avg_apps:.2f}\n\n")
        f.write("Most heavily used clusters:\n")
        for line in top_lines:
            f.write(line + "\n")

    print(f"✅ Wrote: {timeline_path}")
    print(f"✅ Wrote: {cluster_summary_path}")
    print(f"✅ Wrote: {stats_path}")

    # 9) 視覺化
    bar_png = os.path.join(outdir, "problem2_bar_chart.png")
    dens_png = os.path.join(outdir, "problem2_density_plot.png")
    ensure_file_target(bar_png)
    ensure_file_target(dens_png)
    make_figures(timeline_pdf, cluster_pdf, bar_png, dens_png)

    # 額外印出最大的 cluster 用了幾個 app 與平均時長
    if len(cluster_sorted):
        largest = cluster_sorted.iloc[0]
        cid = largest["cluster_id"]
        tl = timeline_pdf[timeline_pdf["cluster_id"] == cid].copy()
        if len(tl):
            tl["duration_sec"] = (tl["end_time"] - tl["start_time"]).dt.total_seconds()
            mean_sec = tl["duration_sec"].mean() if len(tl) else 0.0
            print(f"ℹ️ Largest cluster: {cid} with {int(largest['num_applications'])} apps "
                  f"(avg duration ~ {timedelta(seconds=int(mean_sec))})")

    spark.stop()


def make_figures(timeline_pdf, cluster_pdf, bar_png, dens_png):
    """畫兩張圖：Apps per cluster (bar) 與最大 cluster 的 duration 分布（hist + KDE, log x）。"""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    # 盡量用 seaborn（作業需求），若沒裝就用純 matplotlib
    try:
        import seaborn as sns
        use_sns = True
    except Exception:
        use_sns = False

    # 圖1：每個 cluster 的 app 數量
    if len(cluster_pdf):
        fig = plt.figure(figsize=(10, 5))
        if use_sns:
            import seaborn as sns
            ax = sns.barplot(
                data=cluster_pdf.sort_values("num_applications", ascending=False),
                x="cluster_id", y="num_applications", hue="cluster_id", legend=False
            )
            # 在長柱上標上數字
            for p in ax.patches:
                v = int(p.get_height())
                if math.isfinite(v):
                    ax.annotate(str(v), (p.get_x() + p.get_width()/2, p.get_height()),
                                ha="center", va="bottom", xytext=(0, 3), textcoords="offset points")
        else:
            ordered = cluster_pdf.sort_values("num_applications", ascending=False)
            bars = plt.bar(ordered["cluster_id"].astype(str), ordered["num_applications"])
            for i, v in enumerate(ordered["num_applications"].tolist()):
                plt.text(i, v, str(int(v)), ha="center", va="bottom")

        plt.title("Applications per Cluster")
        plt.xlabel("Cluster ID")
        plt.ylabel("# Applications")
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        plt.savefig(bar_png, dpi=150)
        plt.close()
        print(f"✅ Wrote: {bar_png}")
    else:
        print("⚠️ No clusters to plot bar chart.")

    # 圖2：最大 cluster 的 duration 分布（hist + kde，log x）
    if len(cluster_pdf):
        largest_row = cluster_pdf.sort_values("num_applications", ascending=False).iloc[0]
        largest_cid = str(largest_row["cluster_id"])
        tl = timeline_pdf[timeline_pdf["cluster_id"].astype(str) == largest_cid].copy()

        if len(tl):
            tl["duration_sec"] = (tl["end_time"] - tl["start_time"]).dt.total_seconds()
            tl = tl[tl["duration_sec"].notna() & (tl["duration_sec"] > 0)]

            if len(tl):
                fig = plt.figure(figsize=(10, 5))
                if use_sns:
                    import seaborn as sns
                    sns.histplot(tl["duration_sec"], bins=30, kde=True)
                else:
                    plt.hist(tl["duration_sec"], bins=30, density=False)

                plt.xscale("log")
                plt.xlabel("Job Duration (seconds, log scale)")
                plt.ylabel("Count")
                plt.title(f"Duration Distribution (largest cluster {largest_cid}, n={len(tl)})")
                plt.tight_layout()
                plt.savefig(dens_png, dpi=150)
                plt.close()
                print(f"✅ Wrote: {dens_png}")
            else:
                print("⚠️ Largest cluster has no positive durations to plot.")
        else:
            print("⚠️ Largest cluster has no timeline rows to plot.")
    else:
        print("⚠️ No clusters to plot density.")


def main():
    args = build_arg_parser().parse_args()

    outdir = os.path.expanduser(args.outdir)
    os.makedirs(outdir, exist_ok=True)

    # skip-spark 模式：直接讀 CSV 然後出圖
    if args.skip_spark:
        timeline_path = os.path.join(outdir, "problem2_timeline.csv")
        cluster_summary_path = os.path.join(outdir, "problem2_cluster_summary.csv")
        if not (os.path.isfile(timeline_path) and os.path.isfile(cluster_summary_path)):
            print("❌ --skip-spark 模式需要既有的 CSV：")
            print(f"   {timeline_path}")
            print(f"   {cluster_summary_path}")
            return 1
        import pandas as pd
        timeline_pdf = pd.read_csv(timeline_path, parse_dates=["start_time", "end_time"])
        cluster_pdf = pd.read_csv(cluster_summary_path, parse_dates=["cluster_first_app", "cluster_last_app"])

        bar_png = os.path.join(outdir, "problem2_bar_chart.png")
        dens_png = os.path.join(outdir, "problem2_density_plot.png")
        ensure_file_target(bar_png)
        ensure_file_target(dens_png)
        make_figures(timeline_pdf, cluster_pdf, bar_png, dens_png)
        print("✅ Regenerated figures from existing CSVs.")
        return 0

    # 決定輸入路徑
    if args.input:
        input_path = args.input
    elif args.net_id:
        input_path = f"s3a://athena-{args.net_id}/data/a06/raw/*"
    else:
        print("❌ 請提供 --input 或 --net-id 其一來決定輸入資料位置")
        return 1

    run_spark(args.master_url, input_path, outdir)
    print("\n🎉 Problem 2 finished successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
