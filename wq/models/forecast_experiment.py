"""Chronological dissolved-oxygen experiments with common evaluation rows."""
from __future__ import annotations

from time import perf_counter

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from wq import contracts

SCORE_COLUMNS = ["model", "horizon_min", "n_scored", "mae", "rmse", "fit_s"]
FEATURES = ["persistence", "lag_1h", "lag_3h", "lag_24h", "seasonal_naive"]


def _forecast_rows(frame: pd.DataFrame, horizon: int) -> pd.DataFrame:
    target = contracts.FORECAST_TARGETS[0]
    series = frame.set_index("ts")[target].sort_index().asfreq(f"{contracts.FREQ_MINUTES}min")
    # Each feature is available no later than the forecast issue time.
    filled = series.ffill(limit=8)
    rows = pd.DataFrame({
        "persistence": filled,
        "lag_1h": filled.shift(4),
        "lag_3h": filled.shift(12),
        "lag_24h": filled.shift(contracts.STEPS_PER_DAY),
        "seasonal_naive": series.shift(contracts.STEPS_PER_DAY - horizon),
        "observed": series.shift(-horizon),
    })
    rows["seasonal_naive"] = rows["seasonal_naive"].fillna(rows["persistence"])
    return rows.dropna()


def evaluate_forecast(train: pd.DataFrame, evaluation: pd.DataFrame, *, include_preview: bool = False
                      ) -> tuple[pd.DataFrame, pd.DataFrame | None, dict]:
    scores, previews, estimators = [], [], {}
    for horizon in contracts.FORECAST_HORIZONS_STEPS:
        fit_rows, eval_rows = (_forecast_rows(frame, horizon) for frame in (train, evaluation))
        if len(fit_rows) < 48 or eval_rows.empty:
            continue
        minutes = horizon * contracts.FREQ_MINUTES
        model = make_pipeline(StandardScaler(), Ridge())
        started = perf_counter()
        model.fit(fit_rows[FEATURES], fit_rows["observed"])
        fit_s = perf_counter() - started
        predictions = {
            "persistence": eval_rows["persistence"],
            "seasonal_naive": eval_rows["seasonal_naive"],
            "ridge": model.predict(eval_rows[FEATURES]),
        }
        for name, prediction in predictions.items():
            diff = np.asarray(prediction) - eval_rows["observed"].to_numpy()
            scores.append({
                "model": name, "horizon_min": minutes, "n_scored": len(diff),
                "mae": np.abs(diff).mean(), "rmse": np.sqrt(np.square(diff).mean()),
                "fit_s": fit_s if name == "ridge" else 0.0,
            })
        if include_preview:
            estimators[minutes] = model
            preview = eval_rows[["observed", "persistence", "seasonal_naive"]].tail(192).copy()
            preview["ridge"] = predictions["ridge"][-len(preview):]
            preview["target_ts"] = preview.index + pd.Timedelta(minutes=minutes)
            previews.append(preview.assign(horizon_min=minutes).reset_index(drop=True))
    preview = pd.concat(previews, ignore_index=True) if previews else None
    return pd.DataFrame(scores, columns=SCORE_COLUMNS), preview, estimators
