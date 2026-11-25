"""Benchmark trên bộ dữ liệu LỚN THẬT (chế độ Big Data): pandas vs Spark local vs Spark cluster + HDFS.

Khác bench_batch.py (nhân bản trạm để giả lập quy mô), script này chạy trên dữ liệu thật của nhiều trạm USGS
đã tải về landing zone, và so sánh nơi lưu data lake: đĩa local hay HDFS (replication 2).

    python -m benchmarks.bench_large --landing /app/data/raw/usgs_large \\
        --hdfs-raw hdfs://namenode:8020/wq/raw/usgs --hdfs-lake hdfs://namenode:8020/wq/lake

Kết quả nối vào benchmarks/results/batch_large.csv (pandas chạy trong tiến trình con để đo RAM đỉnh chính xác).
"""
from __future__ import annotations

import argparse
import csv
import glob
import json
import os
import resource
import shutil
import subprocess
import sys
import time
from pathlib import Path

FIELDS = ["engine", "mode", "storage", "cores", "stations", "input_values", "silver_rows", "total_s",
          "values_per_s", "peak_rss_mb", "host_cpus", "note"]


def count_stations(landing: str) -> int:
    return len({p.split("station_id=")[1].split("/")[0] for p in glob.glob(f"{landing}/station_id=*/parameter_code=*/*.csv*")})


def run_pandas_subprocess(landing: str, work: str) -> dict:
    code = ("import json; from benchmarks.bench_batch import run_pandas; "
            f"print('RESULT', json.dumps(run_pandas({landing!r}, {work!r}, 1)))")
    before = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    peak = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    if proc.returncode != 0:
        return {"engine": "pandas", "mode": "single-process", "storage": "local", "cores": 1,
                "note": "FAILED: " + (proc.stderr.strip().splitlines() or ["?"])[-1][:120],
                "peak_rss_mb": round(max(peak, before) / 1024)}
    res = json.loads(proc.stdout.split("RESULT", 1)[1])
    res.update({"storage": "local", "peak_rss_mb": round(peak / 1024), "note": ""})
    return res


def run_spark(raw: str, lake: str, master: str, cores: int, storage: str) -> dict:
    from batch.etl_usgs import run_etl
    from wq.spark_utils import build_spark

    extra = {"spark.sql.shuffle.partitions": str(max(8, 4 * cores))}
    if master.startswith("spark://"):
        extra["spark.cores.max"] = str(cores)
    spark = build_spark(f"bench-large-{storage}-{cores}c", master=master, extra_conf=extra)
    try:
        st = run_etl(spark, raw, lake, write=True)
    finally:
        spark.stop()
    return {"engine": "spark", "mode": "cluster" if master.startswith("spark://") else "local", "storage": storage,
            "cores": cores, "input_values": st["bronze_rows"], "silver_rows": st["silver_rows"],
            "total_s": st["total_s"], "values_per_s": round(st["bronze_rows"] / st["total_s"]), "note": ""}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--landing", default="/app/data/raw/usgs_large")
    ap.add_argument("--work", default="/app/data/bench_large")
    ap.add_argument("--hdfs-raw", default="hdfs://namenode:8020/wq/raw/usgs")
    ap.add_argument("--hdfs-lake", default="hdfs://namenode:8020/wq/lake")
    ap.add_argument("--cluster", default="spark://spark-master:7077")
    ap.add_argument("--cores", type=int, default=4)
    ap.add_argument("--skip-pandas", action="store_true")
    ap.add_argument("--out", default="benchmarks/results/batch_large.csv")
    args = ap.parse_args()

    stations = count_stations(args.landing)
    print(f"Landing zone: {stations} trạm", flush=True)
    Path(args.work).mkdir(parents=True, exist_ok=True)
    new_file = not Path(args.out).exists()
    with open(args.out, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        if new_file:
            w.writeheader()

        def record(row: dict) -> None:
            row = {**row, "stations": stations, "host_cpus": os.cpu_count()}
            w.writerow(row)
            f.flush()
            print(json.dumps(row, ensure_ascii=False), flush=True)

        if not args.skip_pandas:
            record(run_pandas_subprocess(args.landing, args.work))
        local_lake = f"{args.work}/lake_local"
        record(run_spark(args.landing, local_lake, f"local[{args.cores}]", args.cores, "local"))
        shutil.rmtree(local_lake, ignore_errors=True)
        record(run_spark(args.hdfs_raw, args.hdfs_lake, args.cluster, args.cores, "hdfs"))
    print(f"Đã ghi {args.out}")


if __name__ == "__main__":
    main()
