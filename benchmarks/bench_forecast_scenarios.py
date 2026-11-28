"""Compare original, block-masked and LOCF-filled DO histories across the USGS catalog."""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from wq import contracts
from wq.config import get_settings
from wq.evaluation import apply_mask, make_mask
from wq.io import core_station_ids
from wq.models.baselines import LOCFImputer
from wq.models.forecast_experiment import fit_station_ridges, history_features
from wq.spark_ml import read_silver_sdf, station_seed
from wq.spark_utils import build_spark

TARGET = contracts.FORECAST_TARGETS[0]
SCHEMA = ("station_id string, seed long, horizon_min int, scenario string, n_targets long, "
          "n_scored long, sae double, sse double, n_common long, common_sae double, common_sse double, "
          "n_recovered_pair long, recovered_sae double, recovered_sse double")
COLUMNS = [field.split()[0] for field in SCHEMA.split(", ")]
GROUP = ["seed", "horizon_min", "scenario"]


def error_totals(errors: pd.Series) -> tuple[int, float, float]:
    return int(errors.count()), errors.abs().sum(), errors.pow(2).sum()


def station_scores(frame: pd.DataFrame, rate: float, seeds: list[int]) -> pd.DataFrame:
    station = frame["station_id"].iloc[0]
    frame = frame.assign(ts=pd.to_datetime(frame.ts, utc=True)).sort_values("ts")
    train_start, train_end = contracts.SPLITS["train"]
    train = frame[(frame.ts >= train_start) & (frame.ts < train_end)]
    models = fit_station_ridges(train) if not train.empty else {}

    start, end = contracts.SPLITS["test"]
    grid = pd.date_range(start, end, freq=contracts.FREQ, inclusive="left", tz="UTC", name="ts")
    original = frame.set_index("ts")[TARGET].reindex(grid)
    evaluation = original.reset_index().assign(station_id=station)
    # Issue only on the hour. Every possible target lies in the test split.
    issues = grid[grid.minute == 0]
    rows = []
    for seed in seeds:
        mask = make_mask(evaluation, rate, "block", variables=[TARGET], seed=station_seed(seed, station))
        masked_frame = apply_mask(evaluation, mask)
        masked = masked_frame.set_index("ts")[TARGET]
        filled = LOCFImputer().transform(masked_frame).set_index("ts")[TARGET]
        for horizon in contracts.FORECAST_HORIZONS_STEPS:
            # A single target pool for all three scenarios. Exclude artificially hidden targets too.
            targets = masked.shift(-horizon).loc[issues].dropna()
            errors = {}
            for scenario, history in (("original", original), ("masked", masked), ("locf", filled)):
                errors[scenario] = pd.Series(index=targets.index, dtype=float)
                if horizon in models and not targets.empty:
                    features = history_features(history, horizon).loc[targets.index].dropna()
                    if not features.empty:
                        errors[scenario].loc[features.index] = (
                            models[horizon].predict(features) - targets.loc[features.index]
                        )

            common = pd.DataFrame(errors).dropna()
            recovered = errors["masked"].isna() & errors["locf"].notna() & errors["original"].notna()
            for scenario, values in errors.items():
                rows.append((station, seed, horizon * contracts.FREQ_MINUTES, scenario, len(targets),
                             *error_totals(values), *error_totals(common[scenario]), *error_totals(values[recovered])))
    return pd.DataFrame(rows, columns=COLUMNS)


def with_metrics(totals: pd.DataFrame) -> pd.DataFrame:
    counts = totals.n_scored.replace(0, np.nan)
    paired_counts = totals.n_common.replace(0, np.nan)
    recovered_counts = totals.n_recovered_pair.replace(0, np.nan)
    return totals.assign(
        coverage=totals.n_scored / totals.n_targets.replace(0, np.nan),
        mae=totals.sae / counts, rmse=np.sqrt(totals.sse / counts),
        mae_common=totals.common_sae / paired_counts, rmse_common=np.sqrt(totals.common_sse / paired_counts),
        mae_recovered=totals.recovered_sae / recovered_counts, rmse_recovered=np.sqrt(totals.recovered_sse / recovered_counts),
    ).drop(columns=["sae", "sse", "common_sae", "common_sse", "recovered_sae", "recovered_sse"])


def summarize(parts: pd.DataFrame) -> pd.DataFrame:
    totals = parts.assign(scored=parts.n_scored.gt(0), paired=parts.n_common.gt(0)).groupby(GROUP).agg(
        n_stations_considered=("station_id", "nunique"), n_stations_scored=("scored", "sum"),
        n_stations_common=("paired", "sum"), n_targets=("n_targets", "sum"), n_scored=("n_scored", "sum"),
        sae=("sae", "sum"), sse=("sse", "sum"), n_common=("n_common", "sum"),
        common_sae=("common_sae", "sum"), common_sse=("common_sse", "sum"),
        n_recovered_pair=("n_recovered_pair", "sum"), recovered_sae=("recovered_sae", "sum"),
        recovered_sse=("recovered_sse", "sum"),
    )
    return with_metrics(totals).reset_index()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lake", default=get_settings().lake_path)
    parser.add_argument("--master")
    parser.add_argument("--stations-file", default="config/stations_large.json")
    parser.add_argument("--seeds", type=int, nargs="+", default=[42, 43, 44])
    parser.add_argument("--missing-rate", type=float, default=0.2)
    parser.add_argument("--out", type=Path, default=Path("docs/report/tables/ridge-scenarios-test.csv"))
    parser.add_argument("--per-station-out", type=Path, default=Path("data/report/ridge-scenarios-test-stations.csv"))
    args = parser.parse_args()
    if not 0 < args.missing_rate < 1 or len(set(args.seeds)) != len(args.seeds) or min(args.seeds) < 0:
        parser.error("Choose a rate in (0, 1) and distinct non-negative seeds.")
    if args.out.resolve() == args.per_station_out.resolve():
        parser.error("The summary and per-station CSV need different paths.")
    stations = core_station_ids(args.stations_file)
    if not stations or len(set(stations)) != len(stations):
        parser.error("The station catalog must contain distinct station IDs.")

    spark = build_spark("wq-forecast-scenarios", master=args.master)
    try:
        train_start, train_end = contracts.SPLITS["train"]
        start, end = contracts.SPLITS["test"]
        frame = read_silver_sdf(spark, args.lake, stations, train_start, end, columns=["station_id", "ts", TARGET])
        frame = frame.where((frame.ts < train_end) | (frame.ts >= start))
        present = {row.station_id for row in frame.select("station_id").distinct().collect()}
        missing = sorted(set(stations) - present)
        if missing:
            parser.exit(1, f"Silver contains {len(present)}/{len(stations)} requested stations. Missing: "
                           + ", ".join(missing) + "\n")
        print(f"Scoring {len(stations)} stations, {len(args.seeds)} seeds, 3 scenarios...", flush=True)

        def score(group: pd.DataFrame) -> pd.DataFrame:
            return station_scores(group, args.missing_rate, args.seeds)

        parts = frame.groupBy("station_id").applyInPandas(score, schema=SCHEMA).toPandas()
    finally:
        spark.stop()
    if not parts.n_scored.any():
        parser.exit(1, "No test forecasts could be scored. Check the date ranges and DO observations.\n")

    summary = summarize(parts)
    metadata = dict(split="test", model="ridge", mask_type="block", experiment_mask_rate=args.missing_rate,
                    issue_interval_min=60)
    for table, path in ((with_metrics(parts).sort_values(["station_id", *GROUP]), args.per_station_out),
                        (summary, args.out)):
        path.parent.mkdir(parents=True, exist_ok=True)
        table.assign(**metadata).to_csv(path, index=False)
        print(f"Saved {len(table)} rows to {path}")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
