"""Batch ETL bằng PySpark: raw CSV (long) -> bronze Parquet (long) -> silver Parquet (wide, lưới 15 phút).

Đây là bản BASELINE do TV1 dựng để cả pipeline chạy được từ tuần 1. TV2 (Data Engineering) sở hữu
và mở rộng: kiểm tra chất lượng (spike/flatline), xử lý qualifier của USGS, thống kê khoảng trống
(longest gap), EDA, WQI...

Medallion layout (data lake):
    data/lake/bronze/usgs_long/station_id=.../part-*.parquet   (giữ nguyên giá trị gốc + approval/qualifier)
    data/lake/silver/measurements/station_id=.../part-*.parquet (station_id, ts, 6 biến, qc_flags)
    data/lake/gold/missingness/                                 (tỉ lệ thiếu theo trạm x biến)

Chạy:
    python -m batch.etl_usgs                       # local[*]
    python -m batch.etl_usgs --scale 4             # nhân bản x4 số trạm (dùng cho benchmark)
"""
from __future__ import annotations

import argparse
import json
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from wq import contracts
from wq.config import get_settings
from wq.spark_transforms import apply_range_qc
from wq.spark_utils import build_spark

RAW_SCHEMA = "time STRING, value STRING, approval_status STRING, qualifier STRING"


def read_raw(spark: SparkSession, raw_path: str) -> DataFrame:
    # station_id / parameter_code lấy từ tên thư mục (Hive partition). Tắt suy luận kiểu để
    # "00300" không bị đổi thành số 300.
    spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
    return spark.read.option("header", True).schema(RAW_SCHEMA).csv(raw_path)


def to_bronze(raw: DataFrame, scale: int = 1) -> DataFrame:
    code_to_name = F.create_map(*[F.lit(x) for kv in contracts.USGS_CODE_TO_NAME.items() for x in kv])
    df = (
        raw.withColumn("ts", F.to_timestamp("time"))
        .withColumn("value", F.col("value").cast("double"))
        .withColumn("variable", code_to_name[F.col("parameter_code")])
        .where(F.col("ts").isNotNull() & F.col("variable").isNotNull())
        .select("station_id", "parameter_code", "variable", "ts", "value", "approval_status", "qualifier")
    )
    if scale > 1:  # nhân bản trạm để giả lập dữ liệu lớn hơn (chỉ dùng cho benchmark)
        reps = df.sparkSession.range(scale).withColumnRenamed("id", "rep")
        df = (df.crossJoin(reps)
              .withColumn("station_id", F.when(F.col("rep") == 0, F.col("station_id"))
                          .otherwise(F.concat("station_id", F.lit("-r"), F.col("rep").cast("string"))))
              .drop("rep"))
    return df


def to_silver(bronze: DataFrame, start: str, end: str) -> DataFrame:
    step = contracts.FREQ_MINUTES * 60
    # 1) Căn về ô 15 phút (dữ liệu 5 phút -> lấy trung bình trong ô)
    aligned = bronze.withColumn(
        "ts", F.timestamp_seconds((F.unix_timestamp("ts") / step).cast("long") * step))
    # 2) Pivot long -> wide: mỗi dòng = 1 trạm x 1 mốc thời gian
    wide = (aligned.groupBy("station_id", "ts")
            .pivot("variable", contracts.VARIABLE_NAMES)
            .agg(F.avg("value")))
    # 3) QC phạm vi vật lý: ngoài khoảng => NULL + bật bit trong qc_flags (dùng CHUNG với streaming)
    wide = apply_range_qc(wide)
    # 4) Lưới thời gian đầy đủ [start, end) cho MỌI trạm => mốc không có dữ liệu thành dòng NULL
    stations = wide.select("station_id").distinct()
    grid = stations.select(
        "station_id",
        F.explode(F.sequence(F.to_timestamp(F.lit(start)),
                             F.to_timestamp(F.lit(end)) - F.expr(f"INTERVAL {contracts.FREQ_MINUTES} MINUTES"),
                             F.expr(f"INTERVAL {contracts.FREQ_MINUTES} MINUTES"))).alias("ts"))
    silver = (grid.join(wide, ["station_id", "ts"], "left")
              .withColumn("qc_flags", F.coalesce("qc_flags", F.lit(0)))
              .select("station_id", "ts", *contracts.VARIABLE_NAMES, "qc_flags"))
    return silver


def missingness(silver: DataFrame) -> DataFrame:
    aggs = [F.count("*").alias("n_slots")]
    for c in contracts.VARIABLE_NAMES:
        aggs.append(F.round(F.avg(F.col(c).isNull().cast("double")), 4).alias(f"{c}_missing_rate"))
    return silver.groupBy("station_id").agg(*aggs).orderBy("station_id")


def run_etl(spark: SparkSession, raw_path: str, lake_path: str, start: str = contracts.DATA_START,
            end: str = contracts.DATA_END, scale: int = 1, write: bool = True) -> dict:
    """Chạy toàn bộ ETL, trả về thống kê thời gian từng bước (dùng cho benchmark)."""
    stats: dict = {"scale": scale}
    t0 = time.perf_counter()
    bronze = to_bronze(read_raw(spark, raw_path), scale=scale)
    if write:
        bronze.write.mode("overwrite").partitionBy("station_id").parquet(f"{lake_path}/bronze/usgs_long")
        bronze = spark.read.parquet(f"{lake_path}/bronze/usgs_long")
    stats["bronze_rows"] = bronze.count()
    stats["bronze_s"] = round(time.perf_counter() - t0, 2)

    t1 = time.perf_counter()
    silver = to_silver(bronze, start, end)
    if write:
        (silver.repartition("station_id").sortWithinPartitions("ts")
         .write.mode("overwrite").partitionBy("station_id").parquet(f"{lake_path}/silver/measurements"))
        silver = spark.read.parquet(f"{lake_path}/silver/measurements")
    stats["silver_rows"] = silver.count()
    stats["silver_s"] = round(time.perf_counter() - t1, 2)

    t2 = time.perf_counter()
    miss = missingness(silver)
    if write:
        miss.coalesce(1).write.mode("overwrite").parquet(f"{lake_path}/gold/missingness")
    stats["missingness"] = [r.asDict() for r in miss.collect()] if scale == 1 else None
    stats["gold_s"] = round(time.perf_counter() - t2, 2)
    stats["total_s"] = round(time.perf_counter() - t0, 2)
    return stats


def main() -> None:
    s = get_settings()
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--raw", default=s.raw_path)
    ap.add_argument("--lake", default=s.lake_path)
    ap.add_argument("--start", default=contracts.DATA_START)
    ap.add_argument("--end", default=contracts.DATA_END)
    ap.add_argument("--scale", type=int, default=1)
    ap.add_argument("--master", default=None)
    args = ap.parse_args()

    spark = build_spark("wq-batch-etl", master=args.master)
    stats = run_etl(spark, args.raw, args.lake, args.start, args.end, args.scale)
    miss = stats.pop("missingness") or []
    print(json.dumps(stats, indent=2))
    if miss:
        print("\nTỉ lệ thiếu dữ liệu (silver, lưới 15 phút):")
        cols = ["station_id"] + [f"{c}_missing_rate" for c in contracts.VARIABLE_NAMES]
        print(" | ".join(c.replace("_missing_rate", "") for c in cols))
        for r in miss:
            print(" | ".join(str(r[c]) for c in cols))
    spark.stop()


if __name__ == "__main__":
    main()
