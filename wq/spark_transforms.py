"""Phép biến đổi Spark dùng chung cho BATCH và STREAMING (cùng một logic QC ở cả hai luồng)."""
from __future__ import annotations

from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from wq import contracts

# Schema của message JSON trong Kafka topic wq.sensor.raw (xem docs/data_contract.md)
SENSOR_MESSAGE_SCHEMA = StructType(
    [StructField("station_id", StringType()), StructField("event_time", StringType())]
    + [StructField(v, DoubleType()) for v in contracts.VARIABLE_NAMES]
    + [StructField("producer_ts", LongType()), StructField("seq", LongType())]
)


def _bit(name: str) -> Column:
    return F.lit(contracts.VARIABLE_BIT[name])


def apply_range_qc(df: DataFrame, flags_col: str = "qc_flags") -> DataFrame:
    """Giá trị ngoài phạm vi vật lý (contracts.VALID_RANGES) => NULL, đồng thời bật bit tương ứng trong qc_flags."""
    bad = {
        name: F.col(name).isNotNull() & ((F.col(name) < lo) | (F.col(name) > hi))
        for name, (lo, hi) in contracts.VALID_RANGES.items()
    }
    flags = reduce(lambda a, b: a.bitwiseOR(b),
                   [F.when(cond, _bit(name)).otherwise(F.lit(0)) for name, cond in bad.items()])
    if flags_col in df.columns:
        flags = F.col(flags_col).bitwiseOR(flags)
    out = df.withColumn(flags_col, flags)
    for name, cond in bad.items():
        out = out.withColumn(name, F.when(cond, F.lit(None).cast("double")).otherwise(F.col(name)))
    return out


def alert_events(df: DataFrame, ts_col: str = "ts") -> DataFrame:
    """Sinh các sự kiện cảnh báo theo contracts.ALERT_RULES (1 dòng / vi phạm)."""
    parts = []
    for r in contracts.ALERT_RULES:
        cond = F.col(r.parameter) < r.threshold if r.op == "<" else F.col(r.parameter) > r.threshold
        parts.append(df.where(cond).select(
            "station_id", F.col(ts_col).alias("ts"), F.lit(r.parameter).alias("parameter"),
            F.col(r.parameter).alias("value"), F.lit(r.threshold).alias("threshold"),
            F.lit(r.rule).alias("rule"), F.lit(r.severity).alias("severity"), F.lit(r.message_vi).alias("message")))
    return reduce(DataFrame.unionByName, parts)
