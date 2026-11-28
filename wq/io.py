"""Đọc dữ liệu silver thành pandas — dùng chung cho producer, notebook TV3/TV4 và script kiểm chứng.

Data lake có thể nằm trên ổ đĩa local (``data/lake``) hoặc trên HDFS (``hdfs://namenode:8020/wq/lake``):
- local: đọc trực tiếp bằng pyarrow (nhanh, không cần JVM);
- HDFS : đọc bằng Spark (lọc trạm/thời gian được đẩy xuống Parquet) rồi chuyển sang pandas qua Arrow.

    from wq.io import read_silver
    df = read_silver(stations="core", start="2023-01-01", end="2025-01-01")   # 10 trạm của contract
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from wq import contracts
from wq.config import get_settings


def core_station_ids(stations_file: str = "config/stations.json") -> list[str]:
    """10 trạm chuẩn của contract (TV3/TV4 làm thí nghiệm trên bộ này)."""
    return [s["station_id"] for s in json.loads(Path(stations_file).read_text())["stations"]]


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["station_id"] = df["station_id"].astype(str)
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    cols = ["station_id", "ts", *contracts.VARIABLE_NAMES] + (["qc_flags"] if "qc_flags" in df.columns else [])
    # Silver exports may carry only DO. Keep absent sensor channels null for replay and validation.
    if not set(contracts.VARIABLE_NAMES).intersection(df.columns):
        raise ValueError("Silver contains no measurement columns.")
    return df.reindex(columns=cols).sort_values(["station_id", "ts"]).reset_index(drop=True)


def read_silver(stations: list[str] | str | None = None, start: str | None = None, end: str | None = None,
                lake: str | None = None) -> pd.DataFrame:
    """Đọc ``{lake}/silver/measurements`` trong khoảng [start, end) cho danh sách trạm.

    stations: None = mọi trạm; "core" = 10 trạm chuẩn; hoặc list station_id.
    """
    lake = lake or get_settings().lake_path
    path = f"{lake}/silver/measurements"
    if stations == "core":
        stations = core_station_ids()
    if lake.startswith("hdfs://"):
        return _read_spark(path, stations, start, end)
    return _read_pyarrow(path, stations, start, end)


def _read_pyarrow(path, stations, start, end) -> pd.DataFrame:
    import pyarrow as pa
    import pyarrow.dataset as ds

    dataset = ds.dataset(path, format="parquet", partitioning="hive")
    ts_type = dataset.schema.field("ts").type

    def bound(x: str) -> pa.Scalar:
        t = pd.Timestamp(x, tz="UTC")
        return pa.scalar(t if getattr(ts_type, "tz", None) else t.tz_localize(None), type=ts_type)

    flt = None
    if start:
        flt = ds.field("ts") >= bound(start)
    if end:
        f2 = ds.field("ts") < bound(end)
        flt = f2 if flt is None else flt & f2
    if stations:
        f3 = ds.field("station_id").isin(list(stations))
        flt = f3 if flt is None else flt & f3
    return _normalize(dataset.to_table(filter=flt).to_pandas())


def _read_spark(path, stations, start, end) -> pd.DataFrame:
    from pyspark.sql import functions as F

    from wq.spark_utils import build_spark

    spark = build_spark("wq-read-silver", master="local[2]")
    df = spark.read.parquet(path)
    if stations:
        df = df.where(F.col("station_id").isin(list(stations)))   # partition pruning theo thư mục station_id=
    if start:
        df = df.where(F.col("ts") >= F.to_timestamp(F.lit(start)))
    if end:
        df = df.where(F.col("ts") < F.to_timestamp(F.lit(end)))
    return _normalize(df.toPandas())
