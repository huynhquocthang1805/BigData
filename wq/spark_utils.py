"""Tạo SparkSession thống nhất cho mọi job (batch, streaming, benchmark)."""
from __future__ import annotations

import os

from pyspark.sql import SparkSession

from wq.config import Settings, get_settings

SPARK_VERSION = "4.2.1"
SCALA_BINARY = "2.13"
# Chỉ dùng khi chạy LOCAL bằng pip pyspark (Docker image đã có sẵn các jar này)
SPARK_PACKAGES = ",".join([
    f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_BINARY}:{SPARK_VERSION}",
    "org.postgresql:postgresql:42.7.15",
])


def build_spark(app_name: str, settings: Settings | None = None, master: str | None = None,
                extra_conf: dict[str, str] | None = None) -> SparkSession:
    s = settings or get_settings()
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master or s.spark_master)
        # Luôn xử lý thời gian theo UTC để không lệch giờ giữa máy các thành viên
        .config("spark.sql.session.timeZone", "UTC")
        # Parquet timestamp chuẩn (UTC, micro giây) thay vì INT96 cũ => pandas/pyarrow đọc ra tz=UTC
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.sql.shuffle.partitions", str(s.shuffle_partitions))
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.memory", os.getenv("WQ_DRIVER_MEMORY", "1g"))
        .config("spark.executor.memory", os.getenv("WQ_EXECUTOR_MEMORY", "1g"))
    )
    if os.getenv("WQ_STATE_STORE", "hdfs").lower() == "rocksdb":
        # RocksDB: state (dedup, window) nằm ngoài JVM heap -> nên dùng khi state rất lớn (hàng triệu khóa).
        # Mặc định (HDFS-backed, in-memory) nhanh hơn với state nhỏ như đề tài này.
        builder = (builder
                   .config("spark.sql.streaming.stateStore.providerClass",
                           "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
                   .config("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true"))
    if os.getenv("WQ_SPARK_DRIVER_HOST"):  # chế độ cluster trong Docker: executor phải kết nối ngược về driver
        builder = (builder.config("spark.driver.host", os.environ["WQ_SPARK_DRIVER_HOST"])
                   .config("spark.driver.bindAddress", "0.0.0.0"))
    if os.getenv("WQ_EXECUTOR_CORES"):
        builder = builder.config("spark.executor.cores", os.environ["WQ_EXECUTOR_CORES"])
    if os.getenv("WQ_CORES_MAX"):
        builder = builder.config("spark.cores.max", os.environ["WQ_CORES_MAX"])
    if not os.getenv("WQ_SPARK_JARS_BUNDLED"):
        builder = builder.config("spark.jars.packages", SPARK_PACKAGES)
    for k, v in (extra_conf or {}).items():
        builder = builder.config(k, v)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("WQ_SPARK_LOG_LEVEL", "WARN"))
    return spark
