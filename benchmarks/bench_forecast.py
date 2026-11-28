"""Compare Ridge and two baselines on all eligible stations using Spark."""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from wq import contracts
from wq.config import get_settings
from wq.models.forecast_experiment import SCORE_COLUMNS, evaluate_forecast
from wq.spark_ml import read_silver_sdf
from wq.spark_utils import build_spark


def run_benchmark(spark, lake: str, split: str, target: str = "dissolved_oxygen") -> pd.DataFrame:
    from pyspark.sql import functions as F

    train_start, train_end = contracts.SPLITS["train"]
    eval_start, eval_end = contracts.SPLITS[split]
    sdf = read_silver_sdf(spark, lake, start=train_start, end=eval_end, columns=["station_id", "ts", target])
    sdf = sdf.where((F.col("ts") < train_end) | (F.col("ts") >= eval_start))
    schema = "model string, horizon_min int, n_scored long, mae double, rmse double, fit_s double, station_id string"

    def score_station(frame: pd.DataFrame) -> pd.DataFrame:
        station = frame["station_id"].iloc[0]
        frame["ts"] = pd.to_datetime(frame["ts"], utc=True)
        train = frame[frame["ts"] < pd.Timestamp(train_end, tz="UTC")]
        evaluation = frame[frame["ts"] >= pd.Timestamp(eval_start, tz="UTC")]
        if train.empty or evaluation.empty:
            return pd.DataFrame(columns=[*SCORE_COLUMNS, "station_id"])
        scores, _, _ = evaluate_forecast(train, evaluation, target=target)
        return scores.assign(station_id=station)

    return sdf.groupBy("station_id").applyInPandas(score_station, schema=schema).toPandas()


def summarize(parts: pd.DataFrame) -> pd.DataFrame:
    weighted = parts.assign(sae=parts.mae * parts.n_scored, sse=parts.rmse.pow(2) * parts.n_scored)
    totals = weighted.groupby(["horizon_min", "model"], as_index=False).agg(
        n_stations=("station_id", "nunique"), n_scored=("n_scored", "sum"),
        sae=("sae", "sum"), sse=("sse", "sum"), fit_s_sum=("fit_s", "sum"),
    )
    totals["mae"] = totals.sae / totals.n_scored
    totals["rmse"] = np.sqrt(totals.sse / totals.n_scored)
    return totals.drop(columns=["sae", "sse"])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lake", default=get_settings().lake_path)
    parser.add_argument("--master")
    parser.add_argument("--target", choices=contracts.VARIABLE_NAMES, default="dissolved_oxygen")
    parser.add_argument("--split", choices=["valid", "test"], default="valid")
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--per-station-out", type=Path)
    args = parser.parse_args()

    spark = build_spark("wq-bench-forecast", master=args.master)
    try:
        parts = run_benchmark(spark, args.lake, args.split, args.target)
    finally:
        spark.stop()
    if parts.empty:
        parser.exit(1, "Không có trạm đủ dữ liệu để tính điểm dự báo.\n")
    summary = summarize(parts)
    if args.target != "dissolved_oxygen":
        parts = parts.assign(target=args.target, unit=contracts.UNITS[args.target])
        summary = summary.assign(target=args.target, unit=contracts.UNITS[args.target])
    args.out.parent.mkdir(parents=True, exist_ok=True)
    summary.assign(split=args.split).to_csv(args.out, index=False)
    if args.per_station_out:
        args.per_station_out.parent.mkdir(parents=True, exist_ok=True)
        parts.sort_values(["station_id", "horizon_min", "model"]).assign(split=args.split).to_csv(
            args.per_station_out, index=False
        )
    print(f"{parts.station_id.nunique()} trạm có kết quả ({args.split}) → {args.out}")


if __name__ == "__main__":
    main()
