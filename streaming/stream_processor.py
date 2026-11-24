"""Spark Structured Streaming - trái tim của hệ thống (TV1).

Kafka (wq.sensor.raw)
  └─> parse JSON + QC phạm vi vật lý + watermark + khử trùng lặp   (build_clean_stream)
        ├─ Query 1 "lake_sink"    : ghi Parquet vào data lake (append, exactly-once nhờ file sink log)
        ├─ Query 2 "serving_sink" : foreachBatch -> PostgreSQL (upsert) + cảnh báo + ML online
        │                            (điền khuyết TV3 -> dự báo TV4, chạy phân tán theo trạm bằng applyInPandas)
        └─ Query 3 "hourly_agg"   : cửa sổ 1 giờ theo event-time (watermark) -> PostgreSQL

Mọi lần ghi PostgreSQL đều là UPSERT theo khóa chính => nếu Spark chạy lại 1 micro-batch sau sự cố
(at-least-once) thì kết quả vẫn đúng (idempotent => effectively exactly-once).

Chạy local:   python -m streaming.stream_processor
Trong Docker: xem service `stream-processor` trong docker-compose.yml
"""
from __future__ import annotations

import argparse
import itertools
import json
import signal
import sys
import threading
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery, StreamingQueryListener

from wq import contracts
from wq.config import Settings, get_settings
from wq.db import seed_stations, upsert_sql, wait_for_db
from wq.spark_transforms import SENSOR_MESSAGE_SCHEMA, apply_range_qc
from wq.spark_utils import build_spark

V = contracts.VARIABLE_NAMES
MEAS_COLUMNS = ["station_id", "ts", *V, "qc_flags", "producer_ts"]
IMPUTED_COLUMNS = ["station_id", "ts", *V, "imputed_mask", "method"]
PRED_COLUMNS = ["station_id", "target", "issued_at", "horizon_min", "target_ts", "y_pred", "model"]
HOURLY_COLUMNS = ["station_id", "parameter", "window_start", "window_end", "avg_value", "min_value",
                  "max_value", "n_obs", "n_expected", "missing_rate"]
ALERT_COLUMNS = ["station_id", "ts", "parameter", "value", "threshold", "rule", "severity", "message"]

IMPUTED_SCHEMA = ("station_id string, ts timestamp, " + ", ".join(f"{v} double" for v in V)
                  + ", imputed_mask int, method string, is_new boolean")
PRED_SCHEMA = ("station_id string, target string, issued_at timestamp, horizon_min int, "
               "target_ts timestamp, y_pred double, model string")


# ---------------------------------------------------------------------------
# 1. Nguồn streaming đã làm sạch
# ---------------------------------------------------------------------------
def read_kafka(spark: SparkSession, s: Settings) -> DataFrame:
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", s.kafka_bootstrap)
            .option("subscribe", s.topic_raw)
            .option("startingOffsets", s.starting_offsets)
            # Giới hạn số record mỗi micro-batch => backpressure, batch không phình to khi dữ liệu dồn về
            .option("maxOffsetsPerTrigger", s.max_offsets_per_trigger)
            .option("failOnDataLoss", "false")
            .load())


def parse_messages(kafka_df: DataFrame) -> DataFrame:
    parsed = kafka_df.select(F.from_json(F.col("value").cast("string"), SENSOR_MESSAGE_SCHEMA).alias("m"),
                             F.col("timestamp").alias("kafka_ts"))
    return (parsed.select("m.*", "kafka_ts")
            # try_to_timestamp: chuỗi thời gian hỏng => NULL (Spark 4 bật ANSI mode: to_timestamp sẽ NÉM LỖI
            # và làm sập cả streaming query chỉ vì 1 message xấu)
            .withColumn("event_time", F.try_to_timestamp("event_time"))
            .withColumn("producer_ts", F.timestamp_millis("producer_ts"))
            # message hỏng (JSON lỗi, thiếu khóa) bị loại; có thể ghi ra dead-letter topic nếu cần
            .where(F.col("station_id").isNotNull() & F.col("event_time").isNotNull()))


def build_clean_stream(spark: SparkSession, s: Settings) -> DataFrame:
    df = apply_range_qc(parse_messages(read_kafka(spark, s)).withColumn("qc_flags", F.lit(0)))
    # Watermark: chấp nhận dữ liệu đến trễ tối đa `s.watermark` theo event-time; trễ hơn => bỏ,
    # đồng thời cho phép Spark dọn state cũ (không phình bộ nhớ).
    # dropDuplicatesWithinWatermark: khử bản ghi trùng (cùng trạm, cùng mốc) do gửi lặp.
    return (df.withWatermark("event_time", s.watermark)
            .dropDuplicatesWithinWatermark(["station_id", "event_time"]))


# ---------------------------------------------------------------------------
# 2. Ghi PostgreSQL từ EXECUTOR (song song theo partition, không gom về driver)
# ---------------------------------------------------------------------------
def _ts_to_micros(df: DataFrame, cols: list[str]) -> DataFrame:
    # Chuyển timestamp -> epoch micro giây trước khi rời JVM: tránh lệch múi giờ ở Python worker
    for c in cols:
        df = df.withColumn(c, F.unix_micros(c))
    return df


def make_partition_writer(dsn: str, table: str, columns: list[str], keys: list[str],
                          ts_cols: list[str], update: list[str] | None = None):
    sql = upsert_sql(table, columns, keys, update)

    def write_partition(rows) -> None:
        rows = iter(rows)
        first = next(rows, None)
        if first is None:
            return
        import psycopg  # import trong executor

        def conv(r):
            out = []
            for c in columns:
                v = r[c]
                if c in ts_cols and v is not None:
                    v = datetime.fromtimestamp(v / 1e6, tz=timezone.utc)
                out.append(v)
            return out

        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            buf = []
            for r in itertools.chain([first], rows):
                buf.append(conv(r))
                if len(buf) >= 5000:
                    cur.executemany(sql, buf)
                    buf.clear()
            if buf:
                cur.executemany(sql, buf)
            conn.commit()

    return write_partition


def write_pg(df: DataFrame, s: Settings, table: str, columns: list[str], keys: list[str],
             ts_cols: list[str], update: list[str] | None = None) -> None:
    out = _ts_to_micros(df.select(*columns), ts_cols)
    out.foreachPartition(make_partition_writer(s.pg_dsn, table, columns, keys, ts_cols, update))


# ---------------------------------------------------------------------------
# 3. ML online: điền khuyết -> dự báo, phân tán theo trạm (groupBy(station).applyInPandas)
# ---------------------------------------------------------------------------
_MODEL_CACHE: dict = {}


def _utc(ts: pd.Series) -> pd.Series:
    """Arrow trả timestamp naive theo session time zone (UTC); chuẩn hóa về tz-aware UTC theo contract."""
    ts = pd.to_datetime(ts)
    return ts.dt.tz_localize("UTC") if ts.dt.tz is None else ts.dt.tz_convert("UTC")


def _get_model(kind: str, spec: str):
    """Nạp mô hình 1 lần cho mỗi Python worker (worker được tái sử dụng giữa các micro-batch)."""
    key = (kind, spec)
    if key not in _MODEL_CACHE:
        from wq.models.registry import load_forecaster, load_imputer
        _MODEL_CACHE[key] = load_imputer(spec) if kind == "imputer" else load_forecaster(spec)
    return _MODEL_CACHE[key]


def make_impute_fn(spec: str, write_from: pd.Timestamp):
    def impute(pdf: pd.DataFrame) -> pd.DataFrame:
        from wq.timeseries import missing_mask, regularize
        pdf = pdf.assign(ts=_utc(pdf["ts"]))
        reg = regularize(pdf)          # thêm các mốc 15 phút bị mất hẳn (trạm không gửi)
        before = missing_mask(reg)
        imputer = _get_model("imputer", spec)
        out = imputer.transform(reg)
        after = missing_mask(out)
        out["imputed_mask"] = (before & ~after).astype("int32")
        out["method"] = imputer.name
        out["is_new"] = out["ts"] >= write_from
        return out[["station_id", "ts", *V, "imputed_mask", "method", "is_new"]]
    return impute


def make_forecast_fn(spec: str):
    def forecast(pdf: pd.DataFrame) -> pd.DataFrame:
        pdf = pdf.assign(ts=_utc(pdf["ts"]))
        model = _get_model("forecaster", spec)
        preds = model.predict(pdf[contracts.MEASUREMENT_COLUMNS])
        if preds.empty:
            return pd.DataFrame({c: [] for c in PRED_COLUMNS})
        preds["horizon_min"] = preds["horizon_min"].astype("int32")
        return preds[PRED_COLUMNS]
    return forecast


def make_serving_writer(dsn: str):
    """Ghi measurements + sinh cảnh báo trong CÙNG một lần duyệt partition (1 Spark job thay vì 2)."""
    meas_sql = upsert_sql("measurements", MEAS_COLUMNS, ["station_id", "ts"])
    alert_sql = upsert_sql("alerts", ALERT_COLUMNS, ["station_id", "ts", "parameter", "rule"], update_columns=[])
    rules = contracts.ALERT_RULES

    def write_partition(rows) -> None:
        rows = iter(rows)
        first = next(rows, None)
        if first is None:
            return
        import psycopg

        def to_dt(us):
            return None if us is None else datetime.fromtimestamp(us / 1e6, tz=timezone.utc)

        meas, alerts = [], []
        for r in itertools.chain([first], rows):
            ts = to_dt(r["ts"])
            meas.append([r["station_id"], ts, *(r[v] for v in V), r["qc_flags"], to_dt(r["producer_ts"])])
            for rule in rules:
                val = r[rule.parameter]
                if val is not None and (val < rule.threshold if rule.op == "<" else val > rule.threshold):
                    alerts.append([r["station_id"], ts, rule.parameter, val, rule.threshold,
                                   rule.rule, rule.severity, rule.message_vi])
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            for i in range(0, len(meas), 5000):
                cur.executemany(meas_sql, meas[i:i + 5000])
            if alerts:
                cur.executemany(alert_sql, alerts)
            conn.commit()

    return write_partition


class ServingSink:
    """Hàm foreachBatch: mỗi micro-batch là 1 DataFrame tĩnh => dùng được mọi API batch của Spark."""

    def __init__(self, s: Settings, enable_ml: bool):
        self.s = s
        self.enable_ml = enable_ml

    def __call__(self, batch_df: DataFrame, batch_id: int) -> None:
        t0 = time.perf_counter()
        batch = batch_df.withColumnRenamed("event_time", "ts").persist()
        # (a) 1 job thống kê: số dòng, danh sách trạm, khoảng thời gian, phân vị thời điểm gửi của producer
        sent_ms = F.unix_millis("producer_ts")
        st = batch.agg(
            F.count("*").alias("n"), F.collect_set("station_id").alias("stations"),
            F.unix_micros(F.min("ts")).alias("min_us"), F.unix_micros(F.max("ts")).alias("max_us"),
            F.percentile_approx(sent_ms, [0.05, 0.5]).alias("sent_q"), F.min(sent_ms).alias("sent_min")).first()
        if not st["n"]:
            batch.unpersist()
            return
        # (b) 1 job ghi: measurements (upsert) + alerts (DO NOTHING nếu đã có), chạy song song trên executor
        _ts_to_micros(batch.select(*MEAS_COLUMNS), ["ts", "producer_ts"]).foreachPartition(
            make_serving_writer(self.s.pg_dsn))
        t_db = time.perf_counter()
        # (c) Độ trễ end-to-end = (thời điểm ghi xong vào PostgreSQL) - (thời điểm producer gửi)
        now_ms = time.time() * 1000.0
        q05, q50 = st["sent_q"] or (None, None)
        lat = {"p50": now_ms - q50 if q50 else None, "p95": now_ms - q05 if q05 else None,
               "pmax": now_ms - st["sent_min"] if st["sent_min"] else None}

        ml_ms = 0.0
        if self.enable_ml:
            t_ml = time.perf_counter()
            self.run_ml(batch.sparkSession, sorted(st["stations"]), st["min_us"], st["max_us"])
            ml_ms = (time.perf_counter() - t_ml) * 1000
        batch.unpersist()

        total_ms = (time.perf_counter() - t0) * 1000
        self.record_latency(batch_id, st, lat, (t_db - t0) * 1000, ml_ms, total_ms)
        print(f"[serving] batch={batch_id} rows={st['n']} stations={len(st['stations'])} "
              f"latency_p50={lat['p50'] or 0:.0f}ms p95={lat['p95'] or 0:.0f}ms "
              f"db={(t_db - t0) * 1000:.0f}ms ml={ml_ms:.0f}ms total={total_ms:.0f}ms")

    def run_ml(self, spark: SparkSession, stations: list[str], min_us: int, max_us: int) -> None:
        s = self.s
        min_ts = datetime.fromtimestamp(min_us / 1e6, tz=timezone.utc)
        max_ts = datetime.fromtimestamp(max_us / 1e6, tz=timezone.utc)
        hist_from = min_ts - timedelta(hours=s.history_hours)
        # Ghi lại cả 6 giờ trước batch: lấp các mốc bị mất hẳn giữa 2 batch
        write_from = pd.Timestamp(min_ts - timedelta(hours=6))
        cols = ", ".join(["station_id", "ts", *V])
        # Đọc lịch sử song song: mỗi predicate (nhóm trạm) là 1 partition JDBC
        n_groups = max(1, min(len(stations), s.shuffle_partitions))
        groups = [stations[i::n_groups] for i in range(n_groups)]
        predicates = ["station_id IN (" + ", ".join(f"'{x}'" for x in g) + ")" for g in groups if g]
        table = (f"(SELECT {cols} FROM measurements WHERE ts >= '{hist_from.isoformat()}' "
                 f"AND ts <= '{max_ts.isoformat()}') AS h")
        hist = spark.read.jdbc(s.jdbc_url, table, predicates=predicates, properties=s.jdbc_properties)

        imputed = (hist.groupBy("station_id")
                   .applyInPandas(make_impute_fn(s.imputer, write_from), schema=IMPUTED_SCHEMA)
                   .persist())
        write_pg(imputed.where("is_new"), s, "measurements_imputed", IMPUTED_COLUMNS,
                 ["station_id", "ts"], ["ts"])
        preds = (imputed.drop("imputed_mask", "method", "is_new").groupBy("station_id")
                 .applyInPandas(make_forecast_fn(s.forecaster), schema=PRED_SCHEMA))
        write_pg(preds, s, "predictions", PRED_COLUMNS,
                 ["station_id", "target", "issued_at", "horizon_min", "model"], ["issued_at", "target_ts"])
        imputed.unpersist()

    def record_latency(self, batch_id: int, st, lat: dict, db_ms: float, ml_ms: float, total_ms: float) -> None:
        import psycopg
        max_ev = datetime.fromtimestamp(st["max_us"] / 1e6, tz=timezone.utc) if st["max_us"] else None
        with psycopg.connect(self.s.pg_dsn) as conn:
            conn.execute(
                "INSERT INTO batch_latency (batch_id, n_rows, n_stations, latency_p50_ms, latency_p95_ms, "
                "latency_max_ms, db_write_ms, ml_ms, total_ms, max_event_time) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (batch_id, st["n"], len(st["stations"]), lat["p50"], lat["p95"], lat["pmax"],
                 db_ms, ml_ms, total_ms, max_ev))


# ---------------------------------------------------------------------------
# 4. Tổng hợp theo giờ (windowed aggregation theo event-time)
# ---------------------------------------------------------------------------
def build_hourly(clean: DataFrame) -> DataFrame:
    aggs = [F.count("*").alias("n_records")]
    for v in V:
        aggs += [F.avg(v).alias(f"{v}__avg"), F.min(v).alias(f"{v}__min"),
                 F.max(v).alias(f"{v}__max"), F.count(v).alias(f"{v}__n")]
    return clean.groupBy(F.window("event_time", "1 hour").alias("w"), "station_id").agg(*aggs)


def hourly_sink(s: Settings):
    n_expected = contracts.STEPS_PER_HOUR

    def sink(batch_df: DataFrame, batch_id: int) -> None:
        parts = [batch_df.select(
            "station_id", F.lit(v).alias("parameter"), F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"), F.col(f"{v}__avg").alias("avg_value"),
            F.col(f"{v}__min").alias("min_value"), F.col(f"{v}__max").alias("max_value"),
            F.col(f"{v}__n").cast("int").alias("n_obs"), F.lit(n_expected).alias("n_expected"),
            F.greatest(F.lit(0.0), 1 - F.col(f"{v}__n") / n_expected).alias("missing_rate"))
            for v in V]
        long_df = parts[0]
        for p in parts[1:]:
            long_df = long_df.unionByName(p)
        write_pg(long_df, s, "measurements_hourly", HOURLY_COLUMNS,
                 ["station_id", "parameter", "window_start"], ["window_start", "window_end"])

    return sink


# ---------------------------------------------------------------------------
# 5. Giám sát: ghi tiến độ mỗi micro-batch vào PostgreSQL (phục vụ dashboard + benchmark)
# ---------------------------------------------------------------------------
class MetricsListener(StreamingQueryListener):
    def __init__(self, dsn: str):
        self.dsn = dsn
        self._conn = None

    def _db(self):
        import psycopg
        if self._conn is None or self._conn.closed:
            self._conn = psycopg.connect(self.dsn, autocommit=True)
        return self._conn

    def onQueryStarted(self, event) -> None:
        print(f"[listener] query started: {event.name} ({event.id})")

    def onQueryProgress(self, event) -> None:
        try:
            p = json.loads(event.progress.json)
            if not p.get("numInputRows"):
                return
            dur = p.get("durationMs", {})
            wm = (p.get("eventTime") or {}).get("watermark")
            state_rows = sum(op.get("numRowsTotal", 0) for op in p.get("stateOperators", []))
            self._db().execute(
                "INSERT INTO stream_progress (query_name, batch_id, num_input_rows, input_rows_per_sec, "
                "processed_rows_per_sec, trigger_ms, add_batch_ms, watermark, state_rows, raw) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (p.get("name"), p.get("batchId"), p.get("numInputRows"), p.get("inputRowsPerSecond"),
                 p.get("processedRowsPerSecond"), dur.get("triggerExecution"), dur.get("addBatch"),
                 wm, state_rows, json.dumps(p)))
        except Exception as e:  # không để lỗi giám sát làm dừng pipeline
            print(f"[listener] lỗi ghi metrics: {e}", file=sys.stderr)
            self._conn = None

    def onQueryIdle(self, event) -> None:
        pass

    def onQueryTerminated(self, event) -> None:
        print(f"[listener] query terminated: {event.id} exception={event.exception}")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def start_queries(spark: SparkSession, s: Settings, queries: list[str], enable_ml: bool) -> list[StreamingQuery]:
    clean = build_clean_stream(spark, s)
    started = []
    if "lake" in queries:
        started.append(
            clean.withColumn("event_date", F.to_date("event_time"))
            .writeStream.queryName("lake_sink").format("parquet").outputMode("append")
            .option("path", f"{s.lake_path}/stream/measurements")
            .option("checkpointLocation", f"{s.checkpoint_path}/lake_sink")
            .partitionBy("station_id", "event_date")
            .trigger(processingTime=s.lake_trigger_interval)
            .start())
    if "serving" in queries:
        started.append(
            clean.writeStream.queryName("serving_sink").outputMode("append")
            .foreachBatch(ServingSink(s, enable_ml))
            .option("checkpointLocation", f"{s.checkpoint_path}/serving_sink")
            .trigger(processingTime=s.trigger_interval)
            .start())
    if "hourly" in queries:
        started.append(
            build_hourly(clean).writeStream.queryName("hourly_agg").outputMode("append")
            .foreachBatch(hourly_sink(s))
            .option("checkpointLocation", f"{s.checkpoint_path}/hourly_agg")
            .trigger(processingTime=s.trigger_interval)
            .start())
    return started


def main() -> None:
    s = get_settings()
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--queries", default="lake,serving,hourly", help="danh sách query cần chạy, phân tách dấu phẩy")
    ap.add_argument("--no-ml", action="store_true", help="tắt điền khuyết/dự báo online")
    args = ap.parse_args()
    queries = [q.strip() for q in args.queries.split(",") if q.strip()]

    print(f"Chờ PostgreSQL {s.pg_host}:{s.pg_port} ...")
    wait_for_db(s)
    print(f"Seed {seed_stations(s)} trạm từ {s.stations_file}")

    spark = build_spark("wq-stream-processor", s)
    spark.streams.addListener(MetricsListener(s.pg_dsn))
    started = start_queries(spark, s, queries, enable_ml=s.enable_ml and not args.no_ml)
    print(f"Đang chạy {[q.name for q in started]} | Kafka={s.kafka_bootstrap} topic={s.topic_raw} "
          f"| ML={'on' if s.enable_ml and not args.no_ml else 'off'} (imputer={s.imputer}, forecaster={s.forecaster})")

    # Tắt an toàn: signal handler chỉ bật cờ; vòng lặp chính mới gọi q.stop() (không gọi py4j trong handler)
    stop_requested = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop_requested.set())
    signal.signal(signal.SIGINT, lambda *_: stop_requested.set())
    failed = False
    try:
        while not stop_requested.is_set():
            if spark.streams.awaitAnyTermination(5):  # có query kết thúc (thường là do lỗi)
                failed = True
                break
    except Exception as e:  # query lỗi => awaitAnyTermination ném exception
        print(f"Streaming query lỗi: {e}", file=sys.stderr)
        failed = True
    print("Dừng các query (checkpoint giữ nguyên offset => lần sau chạy tiếp, không mất dữ liệu) ...")
    for q in started:
        q.stop()
    spark.stop()
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
