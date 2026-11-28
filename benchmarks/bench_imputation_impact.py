"""Measure how switching from LOCF to Kalman changes a fixed Ridge DO forecast."""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from wq import contracts
from wq.config import get_settings
from wq.evaluation import apply_mask, make_mask
from wq.models.baselines import LOCFImputer
from wq.models.forecast_experiment import FEATURES, _forecast_rows
from wq.models.imputers_tv3 import KalmanImputer
from wq.spark_ml import read_silver_sdf, station_seed
from wq.spark_utils import build_spark

TARGET = "dissolved_oxygen"
SCHEMA = ("station_id string, seed long, horizon_min int, n_scored long, "
          "locf_sae double, locf_sse double, kalman_sae double, kalman_sse double")
COLUMNS = [field.split()[0] for field in SCHEMA.split(", ")]


def history_features(history: pd.Series, horizon: int) -> pd.DataFrame:
    # Each lookup is at or before issue time. Silver uses a regular 15-minute grid.
    day = contracts.STEPS_PER_DAY
    return pd.DataFrame({
        "persistence": history, "lag_1h": history.shift(4), "lag_3h": history.shift(12),
        "lag_24h": history.shift(day), "seasonal_naive": history.shift(day - horizon),
    })[FEATURES]


def station_scores(frame: pd.DataFrame, split: str, rate: float, seeds: list[int]) -> pd.DataFrame:
    station = frame["station_id"].iloc[0]
    series = frame.set_index(pd.to_datetime(frame["ts"], utc=True))[TARGET].sort_index()
    train_start, train_end = contracts.SPLITS["train"]
    start, end = contracts.SPLITS[split]

    def split_frame(lo, hi):
        data = series[(series.index >= pd.Timestamp(lo, tz="UTC")) & (series.index < pd.Timestamp(hi, tz="UTC"))]
        return data.asfreq(contracts.FREQ).rename_axis("ts").reset_index().assign(station_id=station)

    train, evaluation = split_frame(train_start, train_end), split_frame(start, end)
    if train.empty or evaluation[TARGET].count() < 2:
        return pd.DataFrame(columns=COLUMNS)

    # Match the existing forecast benchmark's training features, label filter and Ridge settings.
    models = {}
    for horizon in contracts.FORECAST_HORIZONS_STEPS:
        fit = _forecast_rows(train, horizon)
        if len(fit) >= 48:
            models[horizon] = make_pipeline(StandardScaler(), Ridge()).fit(fit[FEATURES], fit["observed"])
    if not models:
        return pd.DataFrame(columns=COLUMNS)

    locf = LOCFImputer()
    kalman = KalmanImputer()
    kalman.variables = [TARGET]
    kalman.fit(train)
    truth = evaluation.set_index("ts")[TARGET]
    results = []
    for seed in seeds:
        mask = make_mask(evaluation, rate, "block", variables=[TARGET], seed=station_seed(seed, station))
        masked = apply_mask(evaluation, mask)
        histories = [imputer.transform(masked).set_index("ts")[TARGET] for imputer in (locf, kalman)]
        for horizon, ridge in models.items():
            target = truth.shift(-horizon)
            left, right = (history_features(history, horizon) for history in histories)
            eligible = target.notna() & left.notna().all(axis=1) & right.notna().all(axis=1)
            if eligible.any():
                observed = target.loc[eligible].to_numpy()
                errors = [ridge.predict(features.loc[eligible]) - observed for features in (left, right)]
                locf_err, kalman_err = errors
                results.append((station, seed, horizon * contracts.FREQ_MINUTES, len(observed),
                                np.abs(locf_err).sum(), np.square(locf_err).sum(),
                                np.abs(kalman_err).sum(), np.square(kalman_err).sum()))
    return pd.DataFrame(results, columns=COLUMNS)


def summarize(parts: pd.DataFrame) -> pd.DataFrame:
    groups = parts.groupby(["seed", "horizon_min"])
    totals = groups[["n_scored", "locf_sae", "locf_sse", "kalman_sae", "kalman_sse"]].sum()
    report = totals[["n_scored"]].assign(n_stations=groups.station_id.nunique())
    for name in ("locf", "kalman"):
        report[f"mae_{name}"] = totals[f"{name}_sae"] / totals.n_scored
        report[f"rmse_{name}"] = np.sqrt(totals[f"{name}_sse"] / totals.n_scored)
    report["delta_rmse_kalman_minus_locf"] = report.rmse_kalman - report.rmse_locf
    return report.reset_index()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lake", default=get_settings().lake_path)
    parser.add_argument("--master")
    parser.add_argument("--split", choices=["valid", "test"], required=True)
    parser.add_argument("--missing-rate", type=float, default=0.10)
    parser.add_argument("--seeds", type=int, nargs="+", default=[42, 43, 44])
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    if not 0 <= args.missing_rate < 1 or len(set(args.seeds)) != len(args.seeds):
        parser.error("Choose a missing rate in [0, 1) and distinct seeds.")
    spark = build_spark("wq-imputation-impact", master=args.master)
    try:
        train_start, train_end = contracts.SPLITS["train"]
        start, end = contracts.SPLITS[args.split]
        frame = read_silver_sdf(spark, args.lake, start=train_start, end=end).select("station_id", "ts", TARGET)
        frame = frame.where((frame.ts < train_end) | (frame.ts >= start))

        def score(group: pd.DataFrame) -> pd.DataFrame:
            return station_scores(group, args.split, args.missing_rate, args.seeds)

        parts = frame.groupBy("station_id").applyInPandas(score, schema=SCHEMA).toPandas()
    finally:
        spark.stop()
    if parts.empty:
        parser.exit(1, "No paired forecasts with observed targets. Check the Silver lake and split.\n")
    report = summarize(parts).assign(split=args.split, model="ridge", missing_type="block", missing_rate=args.missing_rate)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    report.to_csv(args.out, index=False)
    print(report.to_string(index=False))
    print(f"Saved {args.out}")


if __name__ == "__main__":
    main()
