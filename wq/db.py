"""Tiện ích PostgreSQL (psycopg 3) dùng chung cho stream processor (chạy trên executor), API và script."""
from __future__ import annotations

import json
import time
from collections.abc import Iterable, Sequence
from pathlib import Path

import psycopg

from wq.config import Settings, get_settings


def connect(settings: Settings | None = None, autocommit: bool = False) -> psycopg.Connection:
    s = settings or get_settings()
    return psycopg.connect(s.pg_dsn, autocommit=autocommit, connect_timeout=10)


def wait_for_db(settings: Settings | None = None, timeout_s: int = 120) -> None:
    """Chờ PostgreSQL sẵn sàng (dùng khi các container khởi động cùng lúc)."""
    deadline = time.time() + timeout_s
    while True:
        try:
            with connect(settings) as conn:
                conn.execute("SELECT 1")
            return
        except psycopg.OperationalError:
            if time.time() > deadline:
                raise
            time.sleep(2)


def upsert_sql(table: str, columns: Sequence[str], key_columns: Sequence[str],
               update_columns: Sequence[str] | None = None) -> str:
    """INSERT ... ON CONFLICT (khóa) DO UPDATE => ghi lặp lại vẫn cho cùng kết quả (idempotent).

    Tính idempotent quan trọng vì Spark có thể chạy lại 1 micro-batch khi có lỗi (at-least-once):
    nhờ upsert theo khóa chính, kết quả cuối cùng trong DB vẫn đúng (effectively exactly-once).
    """
    cols = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    keys = ", ".join(key_columns)
    upd = [c for c in (update_columns if update_columns is not None else columns) if c not in key_columns]
    if upd:
        action = "DO UPDATE SET " + ", ".join(f"{c} = EXCLUDED.{c}" for c in upd)
    else:
        action = "DO NOTHING"
    return f"INSERT INTO {table} ({cols}) VALUES ({placeholders}) ON CONFLICT ({keys}) {action}"


def upsert_rows(conn: psycopg.Connection, table: str, columns: Sequence[str], key_columns: Sequence[str],
                rows: Iterable[Sequence], update_columns: Sequence[str] | None = None,
                page_size: int = 5000) -> int:
    sql = upsert_sql(table, columns, key_columns, update_columns)
    n = 0
    batch: list[Sequence] = []
    with conn.cursor() as cur:
        for r in rows:
            batch.append(r)
            if len(batch) >= page_size:
                cur.executemany(sql, batch)
                n += len(batch)
                batch.clear()
        if batch:
            cur.executemany(sql, batch)
            n += len(batch)
    return n


def seed_stations(settings: Settings | None = None) -> int:
    """Nạp danh sách trạm từ config/stations.json vào bảng stations."""
    s = settings or get_settings()
    cfg = json.loads(Path(s.stations_file).read_text())
    cols = ["station_id", "name", "state", "site_type", "latitude", "longitude", "drainage_area_sqmi", "huc"]
    rows = [[st.get(c) for c in cols] for st in cfg["stations"]]
    with connect(s) as conn:
        n = upsert_rows(conn, "stations", cols, ["station_id"], rows)
        conn.commit()
    return n
