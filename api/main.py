"""REST API (FastAPI) phục vụ dashboard và hệ thống bên ngoài. Tài liệu tương tác: http://localhost:8000/docs

Thiết kế: API chỉ ĐỌC từ PostgreSQL (do Spark Streaming ghi) + một endpoint dự báo theo yêu cầu
(on-demand) gọi đúng interface mô hình mà streaming dùng => một mô hình, hai đường phục vụ.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from wq import contracts
from wq.config import get_settings

settings = get_settings()
pool = ConnectionPool(settings.pg_dsn, min_size=1, max_size=8, open=False, kwargs={"row_factory": dict_row})
VARS = contracts.VARIABLE_NAMES


@asynccontextmanager
async def lifespan(_app: FastAPI):
    pool.open(wait=False)
    yield
    pool.close()


app = FastAPI(
    title="Water Quality Monitoring API",
    version=contracts.CONTRACT_VERSION,
    description="Giám sát, điền khuyết và dự báo chất lượng nước từ dữ liệu cảm biến USGS (Kafka + Spark).",
    lifespan=lifespan,
)


def q(sql: str, params: tuple | dict = ()) -> list[dict]:
    with pool.connection() as conn:
        return conn.execute(sql, params).fetchall()


def _check_param(name: str) -> str:
    if name not in VARS:
        raise HTTPException(400, f"parameter phải thuộc {VARS}")
    return name


def _window(station_id: str | None, start: datetime | None, end: datetime | None, days: float) -> tuple[datetime, datetime]:
    """Mặc định: `days` ngày gần nhất TÍNH THEO DỮ LIỆU (dữ liệu phát lại là quá khứ, không theo giờ hệ thống)."""
    if end is None:
        row = q("SELECT max(ts) AS m FROM measurements WHERE (%(s)s::text IS NULL OR station_id = %(s)s)",
                {"s": station_id})
        end = row[0]["m"] or datetime.now(timezone.utc)
    if start is None:
        start = end - timedelta(days=days)
    return start, end


@app.get("/health", tags=["system"])
def health():
    try:
        q("SELECT 1 AS ok")
        return {"status": "ok", "db": True, "contract_version": contracts.CONTRACT_VERSION}
    except Exception as e:  # noqa: BLE001
        raise HTTPException(503, f"database unavailable: {e}") from e


@app.get("/meta", tags=["system"])
def meta():
    """Thông tin contract: biến, đơn vị, ngưỡng cảnh báo, horizon dự báo."""
    return {
        "variables": [{"name": v.name, "unit": v.unit, "label": v.label_vi, "usgs_code": v.usgs_code}
                      for v in contracts.VARIABLES],
        "alert_rules": [r.__dict__ for r in contracts.ALERT_RULES],
        "forecast_targets": contracts.FORECAST_TARGETS,
        "forecast_horizons_min": [h * contracts.FREQ_MINUTES for h in contracts.FORECAST_HORIZONS_STEPS],
        "freq_minutes": contracts.FREQ_MINUTES,
    }


@app.get("/stations", tags=["monitoring"])
def stations():
    """Danh sách trạm + bản ghi mới nhất của từng trạm."""
    return q(f"""
        SELECT s.*, m.ts AS last_ts, {", ".join(f"m.{v}" for v in VARS)},
               (SELECT count(*) FROM alerts a WHERE a.station_id = s.station_id
                  AND a.ts > m.ts - interval '24 hours') AS alerts_24h
        FROM stations s
        LEFT JOIN LATERAL (SELECT * FROM measurements x WHERE x.station_id = s.station_id
                           ORDER BY ts DESC LIMIT 1) m ON true
        ORDER BY s.station_id""")


@app.get("/stations/{station_id}/latest", tags=["monitoring"])
def station_latest(station_id: str):
    raw = q("SELECT * FROM measurements WHERE station_id = %s ORDER BY ts DESC LIMIT 1", (station_id,))
    if not raw:
        raise HTTPException(404, "Chưa có dữ liệu cho trạm này")
    imp = q("SELECT * FROM measurements_imputed WHERE station_id = %s ORDER BY ts DESC LIMIT 1", (station_id,))
    return {"raw": raw[0], "imputed": imp[0] if imp else None}


@app.get("/measurements", tags=["monitoring"])
def measurements(station_id: str, start: datetime | None = None, end: datetime | None = None,
                 days: float = Query(7, gt=0, le=366), source: str = Query("raw", pattern="^(raw|imputed)$"),
                 limit: int = Query(20000, gt=0, le=200000)):
    """Chuỗi thời gian 15 phút. source=raw (đã QC) hoặc imputed (đã điền khuyết, kèm imputed_mask)."""
    start, end = _window(station_id, start, end, days)
    table = "measurements" if source == "raw" else "measurements_imputed"
    extra = "qc_flags" if source == "raw" else "imputed_mask, method"
    return q(f"""SELECT station_id, ts, {", ".join(VARS)}, {extra} FROM {table}
                 WHERE station_id = %s AND ts BETWEEN %s AND %s ORDER BY ts LIMIT %s""",
             (station_id, start, end, limit))


@app.get("/measurements/hourly", tags=["monitoring"])
def hourly(station_id: str, parameter: str, start: datetime | None = None, end: datetime | None = None,
           days: float = Query(30, gt=0, le=366)):
    """Tổng hợp theo giờ do Spark tính bằng window + watermark."""
    _check_param(parameter)
    start, end = _window(station_id, start, end, days)
    return q("""SELECT * FROM measurements_hourly WHERE station_id = %s AND parameter = %s
                AND window_start BETWEEN %s AND %s ORDER BY window_start""", (station_id, parameter, start, end))


@app.get("/predictions/latest", tags=["prediction"])
def predictions_latest(station_id: str, target: str = "dissolved_oxygen"):
    """Bản dự báo mới nhất (mọi horizon) của mỗi mô hình."""
    _check_param(target)
    return q("""SELECT p.* FROM predictions p
                JOIN (SELECT model, max(issued_at) AS mi FROM predictions
                      WHERE station_id = %(s)s AND target = %(t)s GROUP BY model) l
                  ON p.model = l.model AND p.issued_at = l.mi
                WHERE p.station_id = %(s)s AND p.target = %(t)s ORDER BY p.model, p.horizon_min""",
             {"s": station_id, "t": target})


@app.get("/predictions", tags=["prediction"])
def predictions(station_id: str, target: str = "dissolved_oxygen", horizon_min: int = 60,
                start: datetime | None = None, end: datetime | None = None, days: float = Query(7, gt=0, le=366)):
    """Chuỗi dự báo theo target_ts cho 1 horizon, kèm giá trị thực (y_true) nếu đã quan sát được."""
    _check_param(target)
    start, end = _window(station_id, start, end, days)
    return q(f"""SELECT p.target_ts, p.issued_at, p.y_pred, p.model, m.{target} AS y_true
                 FROM predictions p LEFT JOIN measurements m ON m.station_id = p.station_id AND m.ts = p.target_ts
                 WHERE p.station_id = %s AND p.target = %s AND p.horizon_min = %s
                   AND p.target_ts BETWEEN %s AND %s ORDER BY p.target_ts""",
             (station_id, target, horizon_min, start, end + timedelta(days=1)))


@app.get("/predictions/accuracy", tags=["prediction"])
def predictions_accuracy(target: str = "dissolved_oxygen", station_id: str | None = None):
    """Đánh giá ONLINE: so dự báo với giá trị thực đến sau đó (MAE, RMSE theo mô hình x horizon)."""
    _check_param(target)
    return q(f"""SELECT p.model, p.horizon_min, count(*) AS n,
                        avg(abs(p.y_pred - m.{target})) AS mae,
                        sqrt(avg(power(p.y_pred - m.{target}, 2))) AS rmse
                 FROM predictions p JOIN measurements m ON m.station_id = p.station_id AND m.ts = p.target_ts
                 WHERE p.target = %(t)s AND m.{target} IS NOT NULL
                   AND (%(s)s::text IS NULL OR p.station_id = %(s)s)
                 GROUP BY p.model, p.horizon_min ORDER BY p.model, p.horizon_min""",
             {"t": target, "s": station_id})


@app.get("/stations/{station_id}/forecast", tags=["prediction"])
def forecast_on_demand(station_id: str, model: str | None = None, history_hours: int = Query(48, ge=6, le=720)):
    """Dự báo theo yêu cầu: lấy lịch sử từ DB -> imputer -> forecaster (cùng interface với streaming)."""
    from wq.models.registry import load_forecaster, load_imputer
    from wq.timeseries import regularize

    rows = q(f"""SELECT station_id, ts, {", ".join(VARS)} FROM measurements
                 WHERE station_id = %(s)s AND ts > (SELECT max(ts) FROM measurements WHERE station_id = %(s)s)
                       - make_interval(hours => %(h)s) ORDER BY ts""", {"s": station_id, "h": history_hours})
    if not rows:
        raise HTTPException(404, "Chưa có dữ liệu cho trạm này")
    try:
        imputer = load_imputer(settings.imputer)
        forecaster = load_forecaster(model or settings.forecaster)
    except (ValueError, TypeError) as e:
        raise HTTPException(400, str(e)) from e
    hist = imputer.transform(regularize(pd.DataFrame(rows)))
    preds = forecaster.predict(hist)
    return {"imputer": imputer.name, "forecaster": forecaster.name, "history_points": len(hist),
            "predictions": preds.to_dict(orient="records")}


@app.get("/alerts", tags=["monitoring"])
def alerts(station_id: str | None = None, severity: str | None = None, limit: int = Query(200, gt=0, le=5000)):
    return q("""SELECT * FROM alerts WHERE (%(s)s::text IS NULL OR station_id = %(s)s)
                AND (%(v)s::text IS NULL OR severity = %(v)s) ORDER BY ts DESC LIMIT %(l)s""",
             {"s": station_id, "v": severity, "l": limit})


@app.get("/pipeline/metrics", tags=["system"])
def pipeline_metrics(limit: int = Query(200, gt=0, le=5000)):
    """Số liệu vận hành streaming: độ trễ end-to-end (batch_latency) + throughput (stream_progress)."""
    latency = q("SELECT * FROM batch_latency ORDER BY id DESC LIMIT %s", (limit,))
    progress = q("""SELECT id, query_name, batch_id, recorded_at, num_input_rows, input_rows_per_sec,
                           processed_rows_per_sec, trigger_ms, add_batch_ms, watermark, state_rows
                    FROM stream_progress ORDER BY id DESC LIMIT %s""", (limit,))
    totals = q("""SELECT (SELECT count(*) FROM measurements) AS measurements,
                         (SELECT count(*) FROM measurements_imputed) AS imputed_rows,
                         (SELECT count(*) FROM predictions) AS predictions,
                         (SELECT count(*) FROM alerts) AS alerts,
                         (SELECT max(ts) FROM measurements) AS max_event_time""")[0]
    return {"totals": totals, "latency": latency[::-1], "progress": progress[::-1]}
