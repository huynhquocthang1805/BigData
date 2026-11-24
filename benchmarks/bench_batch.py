"""Benchmark BATCH: thời gian & throughput của ETL khi tăng (1) kích thước dữ liệu và (2) số CPU core.
So sánh thêm với pandas đơn luồng để chỉ ra khi nào Spark có lợi.

    python -m benchmarks.bench_batch --scales 1 2 4 --cores 1 2 4
    python -m benchmarks.bench_batch --cluster spark://spark-master:7077 --cluster-cores 2 4   # chạy trên cluster

Kết quả: benchmarks/results/batch_scaling.csv  (vẽ bằng: python -m benchmarks.plot_results)
Lưu ý: --scale k nhân bản số trạm k lần (k x 5.96 triệu giá trị đầu vào).
"""
from __future__ import annotations

import argparse
import csv
import glob
import os
import platform
import shutil
import time
from pathlib import Path

import numpy as np
import pandas as pd

from wq import contracts
from wq.config import get_settings

FIELDS = ["engine", "mode", "cores", "scale", "input_values", "silver_rows", "total_s", "values_per_s",
          "bronze_s", "silver_s", "host_cpus", "note"]


def run_spark(raw: str, work: str, master: str, cores: int, scale: int, note: str = "") -> dict:
    from batch.etl_usgs import run_etl
    from wq.spark_utils import build_spark

    extra = {"spark.sql.shuffle.partitions": str(max(4, 2 * cores))}
    if master.startswith("spark://"):
        import socket
        extra["spark.cores.max"] = str(cores)
        # executor trên worker phải kết nối ngược về driver (container đang chạy benchmark)
        extra["spark.driver.host"] = os.getenv("WQ_SPARK_DRIVER_HOST") or socket.gethostbyname(socket.gethostname())
        extra["spark.driver.bindAddress"] = "0.0.0.0"
    spark = build_spark(f"bench-batch-{cores}c-x{scale}", master=master, extra_conf=extra)
    lake = f"{work}/lake_{cores}c_x{scale}"
    try:
        st = run_etl(spark, raw, lake, scale=scale, write=True)
    finally:
        spark.stop()
        shutil.rmtree(lake, ignore_errors=True)
    return {"engine": "spark", "mode": "cluster" if master.startswith("spark://") else "local", "cores": cores,
            "scale": scale, "input_values": st["bronze_rows"], "silver_rows": st["silver_rows"],
            "total_s": st["total_s"], "values_per_s": round(st["bronze_rows"] / st["total_s"]),
            "bronze_s": st["bronze_s"], "silver_s": st["silver_s"], "note": note}


def run_pandas(raw: str, work: str, scale: int) -> dict:
    """Cùng logic ETL nhưng bằng pandas trên 1 tiến trình (baseline đơn máy)."""
    t0 = time.perf_counter()
    parts = []
    for f in glob.glob(f"{raw}/station_id=*/parameter_code=*/*.csv"):
        sid = f.split("station_id=")[1].split("/")[0]
        code = f.split("parameter_code=")[1].split("/")[0]
        d = pd.read_csv(f, usecols=["time", "value"], dtype={"value": "float64"})
        d["station_id"], d["variable"] = sid, contracts.USGS_CODE_TO_NAME[code]
        parts.append(d)
    df = pd.concat(parts, ignore_index=True)
    if scale > 1:
        df = pd.concat([df.assign(station_id=df.station_id + ("" if r == 0 else f"-r{r}")) for r in range(scale)],
                       ignore_index=True)
    n_in = len(df)
    df["ts"] = pd.to_datetime(df["time"], utc=True, format="ISO8601").dt.floor(contracts.FREQ)
    t_bronze = time.perf_counter() - t0
    wide = df.pivot_table(index=["station_id", "ts"], columns="variable", values="value", aggfunc="mean")
    grid = pd.date_range(contracts.DATA_START, contracts.DATA_END, freq=contracts.FREQ, tz="UTC", inclusive="left")
    idx = pd.MultiIndex.from_product([wide.index.get_level_values(0).unique(), grid], names=["station_id", "ts"])
    wide = wide.reindex(idx).reindex(columns=contracts.VARIABLE_NAMES)
    for name, (lo, hi) in contracts.VALID_RANGES.items():
        wide.loc[(wide[name] < lo) | (wide[name] > hi), name] = np.nan
    out = f"{work}/pandas_x{scale}.parquet"
    wide.reset_index().to_parquet(out, index=False)
    total = time.perf_counter() - t0
    os.remove(out)
    import resource
    peak_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # Linux: KB -> MB (đỉnh của cả tiến trình)
    return {"engine": "pandas", "mode": "single-process", "cores": 1, "scale": scale, "input_values": n_in,
            "silver_rows": len(wide), "total_s": round(total, 2), "values_per_s": round(n_in / total),
            "bronze_s": round(t_bronze, 2), "silver_s": round(total - t_bronze, 2),
            "note": f"peak_rss_mb={peak_mb:.0f}"}


def main() -> None:
    s = get_settings()
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--raw", default=s.raw_path)
    ap.add_argument("--scales", type=int, nargs="+", default=[1, 2, 4])
    ap.add_argument("--cores", type=int, nargs="*", default=[1, 2, 4], help="local[n] cho mỗi n (để trống = bỏ qua Spark)")
    ap.add_argument("--pandas-max-scale", type=int, default=2, help="pandas chỉ chạy tới scale này (giới hạn RAM)")
    ap.add_argument("--cluster", help="URL Spark master để chạy thêm ở chế độ cluster, vd spark://spark-master:7077")
    ap.add_argument("--cluster-cores", type=int, nargs="+", default=[2, 4])
    ap.add_argument("--work", default="data/bench")
    ap.add_argument("--out", default="benchmarks/results/batch_scaling.csv")
    args = ap.parse_args()

    Path(args.work).mkdir(parents=True, exist_ok=True)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    new_file = not Path(args.out).exists()
    host_cpus = os.cpu_count()
    with open(args.out, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        if new_file:
            w.writeheader()

        def record(row: dict) -> None:
            row = {**row, "host_cpus": host_cpus, "note": row.get("note") or platform.node()}
            w.writerow(row)
            f.flush()
            print(f"{row['engine']:6s} {row['mode']:14s} cores={row['cores']} scale=x{row['scale']} "
                  f"values={row['input_values']:,} time={row['total_s']}s -> {row['values_per_s']:,} values/s")

        for scale in args.scales:
            if scale <= args.pandas_max_scale:
                record(run_pandas(args.raw, args.work, scale))
            for c in args.cores:
                record(run_spark(args.raw, args.work, f"local[{c}]", c, scale))
            if args.cluster:
                for c in args.cluster_cores:
                    record(run_spark(args.raw, args.work, args.cluster, c, scale))
    print(f"Đã ghi {args.out}")


if __name__ == "__main__":
    main()
