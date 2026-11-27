"""Chạy mô hình theo từng trạm trên Spark, dùng cho bộ lớn (235 trạm) trên HDFS và cụm Spark.

Mỗi trạm là một nhóm ``groupBy("station_id")``: Spark gửi dữ liệu của trạm tới một Python worker trên executor, nên
các trạm được xử lý song song trên cả cụm thay vì nạp hết vào pandas trên một máy.

- ``read_silver_sdf``: đọc silver thành Spark DataFrame (local hoặc HDFS), lọc theo trạm và thời gian.
- ``fit_by_station``: fit một mô hình riêng cho từng trạm trên executor rồi gộp thành MỘT mô hình (cùng kết quả như
  fit cả bảng bằng pandas, vì các imputer của nhóm vốn học tham số riêng theo trạm).
- ``imputation_benchmark``: giao thức TV3.1 (che, điền, chấm) chạy theo trạm bằng ``cogroup``; mỗi trạm trả về tổng
  sai số, driver cộng lại để ra MAE / RMSE / R² gộp trên mọi ô bị che (cùng định nghĩa với ``score_imputation``).
"""
from __future__ import annotations

import pickle
import zlib
from collections.abc import Callable, Sequence
from time import perf_counter

import numpy as np
import pandas as pd

from wq import contracts

KEY = "station_id"
PART_SCHEMA = ("method string, causal boolean, missing_type string, missing_rate double, seed long, "
               "variable string, station_id string, n_masked long, n_scored long, sae double, sse double, "
               "sy double, syy double, fit_s double, transform_s double")
RESULT_COLUMNS = ["method", "causal", "missing_type", "missing_rate", "variable", "seed", "n_masked", "n_scored",
                  "mae", "rmse", "r2", "fit_s", "transform_s", "n_stations"]


def read_silver_sdf(spark, lake: str, stations: Sequence[str] | None = None, start: str | None = None,
                    end: str | None = None):
    from pyspark.sql import functions as F

    df = spark.read.parquet(f"{lake}/silver/measurements").select(*contracts.MEASUREMENT_COLUMNS)
    if stations:
        df = df.where(F.col(KEY).isin(list(stations)))
    if start:
        df = df.where(F.col("ts") >= F.to_timestamp(F.lit(start)))
    if end:
        df = df.where(F.col("ts") < F.to_timestamp(F.lit(end)))
    return df


def _frame(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.copy()
    pdf[KEY] = pdf[KEY].astype(str)
    pdf["ts"] = pd.to_datetime(pdf["ts"], utc=True)
    return pdf[contracts.MEASUREMENT_COLUMNS].sort_values("ts").reset_index(drop=True)


def station_seed(seed: int, station: str) -> int:
    """Seed riêng, cố định cho mỗi (seed, trạm): chạy lại ra cùng mask dù Spark xếp trạm vào worker nào."""
    return seed * 1_000_003 + zlib.crc32(station.encode()) % 1_000_003


def fit_by_station(sdf, make_model: Callable[[], object]):
    """Fit ``make_model()`` riêng trên dữ liệu từng trạm (trên executor), gộp các tham số theo trạm về driver."""

    def fit_one(pdf: pd.DataFrame) -> pd.DataFrame:
        frame = _frame(pdf)
        model = make_model().fit(frame)
        return pd.DataFrame({KEY: [frame[KEY].iloc[0]], "state": [pickle.dumps(model)]})

    rows = sdf.groupBy(KEY).applyInPandas(fit_one, f"{KEY} string, state binary").collect()
    merged = make_model()
    for row in rows:
        piece = pickle.loads(row["state"])
        # Tham số đã fit của các mô hình theo trạm là dict {station_id: ...} trong thuộc tính kết thúc bằng "_"
        for name, value in vars(piece).items():
            if name.endswith("_") and isinstance(value, dict):
                merged.__dict__.setdefault(name, {}).update(value)
    return merged


def _score_parts(truth: pd.DataFrame, filled: pd.DataFrame, mask: pd.DataFrame, variables: Sequence[str]):
    for var in variables:
        sel = mask[var].to_numpy(dtype=bool)
        actual = truth.loc[sel, var].to_numpy(dtype=float, na_value=np.nan)
        pred = filled.loc[sel, var].to_numpy(dtype=float, na_value=np.nan)
        known = np.isfinite(actual)
        ok = known & np.isfinite(pred)
        a, err = actual[ok], pred[ok] - actual[ok]
        yield var, int(known.sum()), int(ok.sum()), float(np.abs(err).sum()), float(np.square(err).sum()), \
            float(a.sum()), float(np.square(a).sum())


def station_benchmark(station: str, train: pd.DataFrame, truth: pd.DataFrame, factories: dict[str, Callable],
                      seeds: Sequence[int], kinds: Sequence[str], rates: Sequence[float],
                      variables: Sequence[str] = contracts.CORE_VARIABLES) -> pd.DataFrame:
    """Giao thức TV3.1 cho MỘT trạm: fit trên train, che truth, điền, trả tổng sai số theo biến."""
    from wq.evaluation import apply_mask, make_mask

    columns = [c.split()[0] for c in PART_SCHEMA.split(", ")]
    if train.empty or truth.empty:
        return pd.DataFrame(columns=columns)
    models, fit_s = {}, {}
    for name, factory in factories.items():
        t0 = perf_counter()
        models[name] = factory().fit(train)
        fit_s[name] = perf_counter() - t0
    rows = []
    for seed in seeds:
        for kind in kinds:
            for rate in rates:
                mask = make_mask(truth, rate, kind, variables=variables, seed=station_seed(seed, station))
                masked = apply_mask(truth, mask)
                for name, model in models.items():
                    t0 = perf_counter()
                    filled = model.transform(masked)
                    dt = perf_counter() - t0
                    for var, n_masked, n_scored, sae, sse, sy, syy in _score_parts(truth, filled, mask, variables):
                        rows.append((name, bool(model.causal), kind, float(rate), int(seed), var, station, n_masked,
                                     n_scored, sae, sse, sy, syy, fit_s[name], dt))
    return pd.DataFrame(rows, columns=columns)


def aggregate(parts: pd.DataFrame) -> pd.DataFrame:
    """Cộng tổng sai số của các trạm => MAE, RMSE, R² gộp trên mọi ô bị che (như ``score_imputation``)."""
    keys = ["method", "causal", "missing_type", "missing_rate", "variable", "seed"]
    g = parts.groupby(keys, as_index=False).agg(
        n_masked=("n_masked", "sum"), n_scored=("n_scored", "sum"), sae=("sae", "sum"), sse=("sse", "sum"),
        sy=("sy", "sum"), syy=("syy", "sum"), fit_s=("fit_s", "sum"), transform_s=("transform_s", "sum"),
        n_stations=("station_id", "nunique"))
    n = g["n_scored"].where(g["n_scored"] > 0)
    g["mae"] = g["sae"] / n
    g["rmse"] = np.sqrt(g["sse"] / n)
    ss_tot = g["syy"] - g["sy"] ** 2 / n
    constant = ss_tot <= 1e-12 * g["syy"].abs()   # đáp án không đổi: theo quy ước của score_imputation
    g["r2"] = np.where(g["n_scored"] < 2, np.nan,
                       np.where(constant, (g["sse"] == 0).astype(float), 1 - g["sse"] / ss_tot.where(~constant)))
    return g[RESULT_COLUMNS].sort_values(keys).reset_index(drop=True)


def imputation_benchmark(train_sdf, eval_sdf, factories: dict[str, Callable], seeds: Sequence[int],
                         kinds: Sequence[str] = ("random", "block"), rates: Sequence[float] = (0.05, 0.10, 0.20, 0.30),
                         variables: Sequence[str] = contracts.CORE_VARIABLES) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Chạy ``station_benchmark`` cho mọi trạm trên cụm Spark. Trả (kết quả gộp, tổng sai số theo trạm)."""

    def run(key, train_pdf: pd.DataFrame, eval_pdf: pd.DataFrame) -> pd.DataFrame:
        station = str(key[0])
        if train_pdf.empty or eval_pdf.empty:
            return pd.DataFrame(columns=[c.split()[0] for c in PART_SCHEMA.split(", ")])
        return station_benchmark(station, _frame(train_pdf), _frame(eval_pdf), factories, seeds, kinds, rates,
                                 variables)

    parts = (train_sdf.groupBy(KEY).cogroup(eval_sdf.groupBy(KEY))
             .applyInPandas(run, PART_SCHEMA).toPandas())
    return aggregate(parts), parts
