"""Station-specific Ridge forecasts for dissolved oxygen. Input history is already imputed."""
from __future__ import annotations

import argparse
from pathlib import Path
from tempfile import NamedTemporaryFile

import numpy as np
import pandas as pd

from wq import contracts
from wq.models.base import BaseForecaster, make_prediction_frame
from wq.models.forecast_experiment import FEATURES, fit_station_ridges, history_features

TARGET = contracts.FORECAST_TARGETS[0]
BUNDLED = Path(__file__).resolve().parents[2] / "models" / "forecaster_ridge_235.csv"


class RidgeForecaster(BaseForecaster):
    name = "ridge"
    min_history_steps = contracts.STEPS_PER_DAY + 1

    def __init__(self):
        self.coefficients_: dict[str, dict[int, np.ndarray]] = {}

    def fit(self, df: pd.DataFrame) -> RidgeForecaster:
        self.coefficients_ = {}
        for station, train in df.groupby("station_id", sort=False):
            weights = {}
            for horizon, pipeline in fit_station_ridges(train).items():
                scaler = pipeline.named_steps["standardscaler"]
                ridge = pipeline.named_steps["ridge"]
                slopes = ridge.coef_ / scaler.scale_
                intercept = ridge.intercept_ - scaler.mean_ @ slopes
                weights[horizon] = np.r_[intercept, slopes]
            if weights:
                self.coefficients_[station] = weights
        return self

    def predict(self, history: pd.DataFrame) -> pd.DataFrame:
        rows = []
        for station, frame in history.groupby("station_id", sort=False):
            horizons = self.coefficients_.get(station, {})
            if not horizons or TARGET not in frame or len(frame) < self.min_history_steps:
                continue
            series = frame.set_index(pd.to_datetime(frame.ts, utc=True))[TARGET]
            issued = series.index.max()
            grid = pd.date_range(end=issued, periods=self.min_history_steps, freq=contracts.FREQ)
            series = series.reindex(grid)
            for horizon, weights in horizons.items():
                features = history_features(series, horizon).iloc[-1].to_numpy(dtype=float)
                if np.isfinite(features).all():
                    minutes = horizon * contracts.FREQ_MINUTES
                    rows.append(dict(station_id=station, target=TARGET, issued_at=issued, horizon_min=minutes,
                                     target_ts=issued + pd.Timedelta(minutes=minutes),
                                     y_pred=weights[0] + features @ weights[1:], model=self.name))
        return make_prediction_frame(rows)

    def to_csv(self, path: Path) -> None:
        rows = [(station, horizon, *weights) for station, models in sorted(self.coefficients_.items())
                for horizon, weights in sorted(models.items())]
        pd.DataFrame(rows, columns=["station_id", "horizon_steps", "intercept", *FEATURES]).to_csv(path, index=False)

    @classmethod
    def from_csv(cls, path: Path = BUNDLED) -> RidgeForecaster:
        frame = pd.read_csv(path, float_precision="round_trip")
        model = cls()
        columns = ["station_id", "horizon_steps", "intercept", *FEATURES]
        for station, horizon, *weights in frame[columns].itertuples(index=False, name=None):
            model.coefficients_.setdefault(station, {})[horizon] = np.asarray(weights)
        return model


def main() -> None:
    from wq.config import get_settings
    from wq.io import core_station_ids
    # Use the importable class name when running this module with python -m.
    from wq.models.forecasters_tv4 import RidgeForecaster as Model
    from wq.spark_ml import fit_by_station, read_silver_sdf
    from wq.spark_utils import build_spark

    parser = argparse.ArgumentParser(description="Fit Ridge on the chronological train split and export it.")
    parser.add_argument("--model", choices=["ridge"], default="ridge")
    parser.add_argument("--stations", choices=["core", "all"], default="all")
    parser.add_argument("--master")
    parser.add_argument("--lake", default=get_settings().lake_path)
    parser.add_argument("--out", type=Path, default=Path("models/forecaster_ridge_235.joblib"))
    args = parser.parse_args()
    if args.out.suffix not in {".joblib", ".csv"}:
        parser.error("--out must end in .joblib or .csv")
    catalog = "config/stations_large.json" if args.stations == "all" else "config/stations.json"
    stations = core_station_ids(catalog)
    cols = ["station_id", "ts", TARGET]
    spark = build_spark("wq-fit-ridge", master=args.master)
    try:
        train = read_silver_sdf(spark, args.lake, stations, *contracts.SPLITS["train"], columns=cols)
        seen = {row.station_id for row in train.select("station_id").distinct().collect()}
        if missing := sorted(set(stations) - seen):
            parser.exit(1, f"Train Silver missing {len(missing)} catalog stations: {', '.join(missing)}\n")
        model = fit_by_station(train, Model, columns=cols)
    finally:
        spark.stop()
    if not model.coefficients_:
        parser.exit(1, "No station had enough training examples for Ridge.\n")
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=args.out.parent, delete=False) as tmp:
        temp = Path(tmp.name)
    try:
        if args.out.suffix == ".csv":
            model.to_csv(temp)
        else:
            model.save(temp)
        temp.replace(args.out)
    finally:
        temp.unlink(missing_ok=True)
    print(f"{len(model.coefficients_)}/{len(stations)} stations, "
          f"{sum(map(len, model.coefficients_.values()))} station/horizon models -> {args.out}")


if __name__ == "__main__":
    main()
