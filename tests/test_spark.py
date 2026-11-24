"""Kiểm thử logic Spark dùng chung cho batch và streaming (chạy Spark local, không cần Kafka/DB)."""
import json
from datetime import datetime, timezone

from pyspark.sql import functions as F

from batch.etl_usgs import to_bronze, to_silver
from streaming.stream_processor import build_hourly, parse_messages
from wq import contracts
from wq.spark_transforms import alert_events, apply_range_qc


def test_apply_range_qc_nulls_and_flags(spark):
    df = spark.createDataFrame([(1, "S1", 7.5, 30.0), (2, "S1", 15.0, 99.0)],
                               "id int, station_id string, ph double, water_temp double")
    for v in contracts.VARIABLE_NAMES:
        if v not in df.columns:
            df = df.withColumn(v, F.lit(None).cast("double"))
    rows = apply_range_qc(df).orderBy("id").collect()
    assert rows[0]["qc_flags"] == 0
    assert rows[1]["ph"] is None and rows[1]["water_temp"] is None
    assert rows[1]["qc_flags"] == contracts.qc_bitmask(["water_temp", "ph"])


def test_alert_events(spark):
    df = spark.createDataFrame([("S1", datetime(2025, 7, 1, tzinfo=timezone.utc), 1.5, 9.5)],
                               "station_id string, ts timestamp, dissolved_oxygen double, ph double")
    for v in contracts.VARIABLE_NAMES:
        if v not in df.columns:
            df = df.withColumn(v, F.lit(None).cast("double"))
    rules = {r["rule"] for r in alert_events(df).collect()}
    assert rules == {"DO_LOW", "DO_HYPOXIA", "PH_HIGH"}


def _kafka_like(spark, messages):
    rows = [(json.dumps(m).encode(), datetime(2026, 1, 1, tzinfo=timezone.utc)) for m in messages]
    return spark.createDataFrame(rows, "value binary, timestamp timestamp")


def test_parse_messages_handles_nulls_and_garbage(spark):
    msgs = [{"station_id": "S1", "event_time": "2025-07-01T00:15:00Z", "ph": 7.1, "dissolved_oxygen": None,
             "producer_ts": 1_700_000_000_000, "seq": 1},
            {"event_time": "2025-07-01T00:15:00Z"},  # thiếu station_id -> bị loại
            {"station_id": "S2", "event_time": "not-a-time"}]  # thời gian hỏng -> bị loại
    out = parse_messages(_kafka_like(spark, msgs)).collect()
    assert len(out) == 1
    r = out[0]
    assert r["station_id"] == "S1" and r["ph"] == 7.1 and r["dissolved_oxygen"] is None
    assert r["event_time"].replace(tzinfo=None) == datetime(2025, 7, 1, 0, 15)


def test_hourly_aggregation_counts(spark):
    msgs = [{"station_id": "S1", "event_time": f"2025-07-01T00:{m:02d}:00Z", "ph": 7.0 + i, "producer_ts": 0, "seq": i}
            for i, m in enumerate([0, 15, 30])]
    df = parse_messages(_kafka_like(spark, msgs))
    r = build_hourly(df).collect()[0]
    assert r["n_records"] == 3 and r["ph__n"] == 3 and r["ph__avg"] == 8.0 and r["dissolved_oxygen__n"] == 0


def test_etl_bronze_to_silver_grid(spark):
    raw = spark.createDataFrame(
        [("2025-01-01 00:00:00+00:00", "7.0", "Approved", None, "USGS-1", "00400"),
         ("garbage-time", "7.0", "Approved", None, "USGS-1", "00400"),                 # bị loại, không ném lỗi
         ("2025-01-01 00:15:00+00:00", "n/a", "Approved", None, "USGS-1", "00400"),    # value hỏng -> NULL
         ("2025-01-01 00:05:00+00:00", "8.0", "Approved", None, "USGS-1", "00400"),
         ("2025-01-01 00:30:00+00:00", "99", "Approved", None, "USGS-1", "00400"),   # pH ngoài phạm vi
         ("2025-01-01 00:00:00+00:00", "20.5", "Approved", None, "USGS-1", "00010")],
        "time string, value string, approval_status string, qualifier string, station_id string, parameter_code string")
    silver = to_silver(to_bronze(raw), "2025-01-01", "2025-01-01T01:00:00").orderBy("ts").collect()
    assert len(silver) == 4  # 00:00, 00:15, 00:30, 00:45
    assert silver[0]["ph"] == 7.5 and silver[0]["water_temp"] == 20.5
    assert silver[1]["ph"] is None
    assert silver[2]["ph"] is None and silver[2]["qc_flags"] == contracts.VARIABLE_BIT["ph"]


def test_bronze_scale_replicates_stations(spark):
    raw = spark.createDataFrame([("2025-01-01 00:00:00+00:00", "7.0", "A", None, "USGS-1", "00400")],
                                "time string, value string, approval_status string, qualifier string, "
                                "station_id string, parameter_code string")
    ids = {r[0] for r in to_bronze(raw, scale=3).select("station_id").collect()}
    assert ids == {"USGS-1", "USGS-1-r1", "USGS-1-r2"}
