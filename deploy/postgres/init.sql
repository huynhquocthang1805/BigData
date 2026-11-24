-- Serving database cho dashboard/API. Được nạp tự động khi container postgres khởi tạo lần đầu.
-- Khóa chính tự nhiên (station_id, ts, ...) => mọi lần ghi từ Spark là UPSERT idempotent.

CREATE TABLE IF NOT EXISTS stations (
    station_id          TEXT PRIMARY KEY,
    name                TEXT,
    state               TEXT,
    site_type           TEXT,
    latitude            DOUBLE PRECISION,
    longitude           DOUBLE PRECISION,
    drainage_area_sqmi  DOUBLE PRECISION,
    huc                 TEXT
);

-- Dữ liệu đo đã qua QC từ luồng streaming (giá trị gốc, NULL = thiếu/lỗi cảm biến)
CREATE TABLE IF NOT EXISTS measurements (
    station_id            TEXT        NOT NULL,
    ts                    TIMESTAMPTZ NOT NULL,
    water_temp            DOUBLE PRECISION,
    specific_conductance  DOUBLE PRECISION,
    ph                    DOUBLE PRECISION,
    dissolved_oxygen      DOUBLE PRECISION,
    turbidity             DOUBLE PRECISION,
    discharge             DOUBLE PRECISION,
    qc_flags              INTEGER     NOT NULL DEFAULT 0,   -- bit i = biến i bị loại do ngoài phạm vi vật lý
    producer_ts           TIMESTAMPTZ,                      -- thời điểm producer gửi (đo độ trễ end-to-end)
    ingested_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (station_id, ts)
);

-- Kết quả điền khuyết online (mô hình của TV3, mặc định baseline LOCF)
CREATE TABLE IF NOT EXISTS measurements_imputed (
    station_id            TEXT        NOT NULL,
    ts                    TIMESTAMPTZ NOT NULL,
    water_temp            DOUBLE PRECISION,
    specific_conductance  DOUBLE PRECISION,
    ph                    DOUBLE PRECISION,
    dissolved_oxygen      DOUBLE PRECISION,
    turbidity             DOUBLE PRECISION,
    discharge             DOUBLE PRECISION,
    imputed_mask          INTEGER     NOT NULL DEFAULT 0,   -- bit i = biến i được điền bởi mô hình
    method                TEXT        NOT NULL,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (station_id, ts)
);

-- Tổng hợp theo cửa sổ 1 giờ (Spark windowed aggregation + watermark), dạng long
CREATE TABLE IF NOT EXISTS measurements_hourly (
    station_id    TEXT        NOT NULL,
    parameter     TEXT        NOT NULL,
    window_start  TIMESTAMPTZ NOT NULL,
    window_end    TIMESTAMPTZ NOT NULL,
    avg_value     DOUBLE PRECISION,
    min_value     DOUBLE PRECISION,
    max_value     DOUBLE PRECISION,
    n_obs         INTEGER     NOT NULL,   -- số giá trị hợp lệ trong giờ
    n_expected    INTEGER     NOT NULL,   -- 4 (lưới 15 phút)
    missing_rate  DOUBLE PRECISION,
    PRIMARY KEY (station_id, parameter, window_start)
);

-- Dự báo online (mô hình của TV4, mặc định baseline seasonal-naive)
CREATE TABLE IF NOT EXISTS predictions (
    station_id   TEXT        NOT NULL,
    target       TEXT        NOT NULL,
    issued_at    TIMESTAMPTZ NOT NULL,   -- mốc dữ liệu cuối cùng dùng để dự báo
    horizon_min  INTEGER     NOT NULL,
    target_ts    TIMESTAMPTZ NOT NULL,   -- = issued_at + horizon
    y_pred       DOUBLE PRECISION,
    model        TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (station_id, target, issued_at, horizon_min, model)
);
CREATE INDEX IF NOT EXISTS predictions_target_ts_idx ON predictions (station_id, target, target_ts);

CREATE TABLE IF NOT EXISTS alerts (
    alert_id    BIGSERIAL PRIMARY KEY,
    station_id  TEXT        NOT NULL,
    ts          TIMESTAMPTZ NOT NULL,
    parameter   TEXT        NOT NULL,
    value       DOUBLE PRECISION,
    threshold   DOUBLE PRECISION,
    rule        TEXT        NOT NULL,
    severity    TEXT        NOT NULL,
    message     TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (station_id, ts, parameter, rule)
);
CREATE INDEX IF NOT EXISTS alerts_ts_idx ON alerts (ts DESC);

-- Giám sát pipeline: tiến độ từng micro-batch (StreamingQueryListener)
CREATE TABLE IF NOT EXISTS stream_progress (
    id                      BIGSERIAL PRIMARY KEY,
    query_name              TEXT        NOT NULL,
    batch_id                BIGINT      NOT NULL,
    recorded_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    num_input_rows          BIGINT,
    input_rows_per_sec      DOUBLE PRECISION,
    processed_rows_per_sec  DOUBLE PRECISION,
    trigger_ms              BIGINT,
    add_batch_ms            BIGINT,
    watermark               TIMESTAMPTZ,
    state_rows              BIGINT,
    raw                     JSONB
);

-- Giám sát pipeline: độ trễ end-to-end (producer -> ghi xong vào PostgreSQL) của serving sink
CREATE TABLE IF NOT EXISTS batch_latency (
    id               BIGSERIAL PRIMARY KEY,
    batch_id         BIGINT      NOT NULL,
    recorded_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    n_rows           BIGINT,
    n_stations       INTEGER,
    latency_p50_ms   DOUBLE PRECISION,
    latency_p95_ms   DOUBLE PRECISION,
    latency_max_ms   DOUBLE PRECISION,
    db_write_ms      DOUBLE PRECISION,
    ml_ms            DOUBLE PRECISION,
    total_ms         DOUBLE PRECISION,
    max_event_time   TIMESTAMPTZ
);
