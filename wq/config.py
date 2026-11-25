"""Cấu hình đọc từ biến môi trường (12-factor). Mặc định phù hợp khi chạy LOCAL (ngoài Docker);
trong Docker Compose các biến được đặt trong docker-compose.yml / .env.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field

from wq import contracts


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


def _env_bool(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    # Kafka
    kafka_bootstrap: str = field(default_factory=lambda: _env("WQ_KAFKA_BOOTSTRAP", "localhost:9094"))
    topic_raw: str = field(default_factory=lambda: _env("WQ_TOPIC_RAW", contracts.TOPIC_SENSOR_RAW))

    # PostgreSQL (serving DB)
    pg_host: str = field(default_factory=lambda: _env("WQ_PG_HOST", "localhost"))
    pg_port: int = field(default_factory=lambda: int(_env("WQ_PG_PORT", "5432")))
    pg_db: str = field(default_factory=lambda: _env("WQ_PG_DB", "wq"))
    pg_user: str = field(default_factory=lambda: _env("WQ_PG_USER", "wq"))
    pg_password: str = field(default_factory=lambda: _env("WQ_PG_PASSWORD", "wq"))

    # Data lake (Parquet) & checkpoint: phải là thư mục CHIA SẺ giữa driver và mọi executor
    raw_path: str = field(default_factory=lambda: _env("WQ_RAW_PATH", "data/raw/usgs"))
    # Landing zone: nơi downloader ghi file thô trên máy. Chế độ HDFS: landing (local) -> ingest -> raw_path (hdfs://)
    landing_path: str = field(default_factory=lambda: _env("WQ_LANDING_PATH", _env("WQ_RAW_PATH", "data/raw/usgs")))
    raw_gzip: bool = field(default_factory=lambda: _env_bool("WQ_RAW_GZIP", False))
    lake_path: str = field(default_factory=lambda: _env("WQ_LAKE_PATH", "data/lake"))
    checkpoint_path: str = field(default_factory=lambda: _env("WQ_CHECKPOINT_PATH", "data/checkpoints"))
    stations_file: str = field(default_factory=lambda: _env("WQ_STATIONS_FILE", "config/stations.json"))

    # Spark
    spark_master: str = field(default_factory=lambda: _env("WQ_SPARK_MASTER", "local[*]"))
    shuffle_partitions: int = field(default_factory=lambda: int(_env("WQ_SHUFFLE_PARTITIONS", "8")))

    # Streaming
    starting_offsets: str = field(default_factory=lambda: _env("WQ_STARTING_OFFSETS", "earliest"))
    trigger_interval: str = field(default_factory=lambda: _env("WQ_TRIGGER", "5 seconds"))
    lake_trigger_interval: str = field(default_factory=lambda: _env("WQ_LAKE_TRIGGER", "30 seconds"))
    watermark: str = field(default_factory=lambda: _env("WQ_WATERMARK", "2 hours"))
    max_offsets_per_trigger: int = field(default_factory=lambda: int(_env("WQ_MAX_OFFSETS_PER_TRIGGER", "200000")))
    enable_ml: bool = field(default_factory=lambda: _env_bool("WQ_ENABLE_ML", True))
    history_hours: int = field(default_factory=lambda: int(_env("WQ_HISTORY_HOURS", "48")))

    # Mô hình (tên built-in hoặc đường dẫn .joblib do TV3/TV4 bàn giao)
    imputer: str = field(default_factory=lambda: _env("WQ_IMPUTER", "locf"))
    forecaster: str = field(default_factory=lambda: _env("WQ_FORECASTER", "seasonal_naive"))

    @property
    def pg_dsn(self) -> str:
        return (f"host={self.pg_host} port={self.pg_port} dbname={self.pg_db} "
                f"user={self.pg_user} password={self.pg_password}")

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.pg_host}:{self.pg_port}/{self.pg_db}"

    @property
    def jdbc_properties(self) -> dict[str, str]:
        return {"user": self.pg_user, "password": self.pg_password, "driver": "org.postgresql.Driver"}


def get_settings() -> Settings:
    return Settings()
