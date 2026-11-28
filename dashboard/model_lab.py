"""Bounded, chronological experiments on one station's silver Parquet data."""
from __future__ import annotations

from io import BytesIO
from time import perf_counter

import joblib
import pandas as pd

from wq import contracts
from wq.evaluation import apply_mask, make_mask, score_imputation
from wq.io import read_silver
from wq.models.baselines import LOCFImputer
from wq.models.forecast_experiment import FEATURES, evaluate_forecast
from wq.models.imputers_tv3 import KalmanImputer


def train_station(station: str, split: str, task: str, missing_type: str, missing_rate: float):
    train = read_silver([station], *contracts.SPLITS["train"])
    evaluation = read_silver([station], *contracts.SPLITS[split])
    if train.empty or evaluation.empty:
        raise ValueError("Trạm cần có dữ liệu Silver trong cả giai đoạn huấn luyện và đánh giá.")

    if task == "imputation":
        mask = make_mask(evaluation, missing_rate, missing_type, seed=42)
        masked = apply_mask(evaluation, mask)
        scores = []
        kalman = KalmanImputer()
        for model in (LOCFImputer(), kalman):
            started = perf_counter()
            model.fit(train)
            fit_s = perf_counter() - started
            predictions = model.transform(masked)
            score = score_imputation(evaluation, predictions, mask, contracts.CORE_VARIABLES)
            scores.append(score.assign(
                model=model.name, fit_s=fit_s, missing_type=missing_type, missing_rate=missing_rate, seed=42,
            ))
        result = pd.concat(scores, ignore_index=True)
        if not result["n_scored"].any():
            raise ValueError("Không đủ điểm quan trắc để chấm điểm điền khuyết.")
        result["coverage_pct"] = 100 * result.n_scored.div(result.n_masked.where(result.n_masked.gt(0)))
        return result.assign(station_id=station, split=split), None, _serialize(kalman)

    scores, preview, estimators = evaluate_forecast(train, evaluation, include_preview=True)
    if scores.empty:
        raise ValueError("Không đủ số đo oxy hòa tan liên tục để huấn luyện và đánh giá dự báo.")
    artifact = {"station_id": station, "target": contracts.FORECAST_TARGETS[0],
                "features": FEATURES, "estimators_by_horizon_min": estimators}
    return scores.assign(station_id=station, split=split), preview, _serialize(artifact)


def _serialize(model) -> bytes:
    buffer = BytesIO()
    joblib.dump(model, buffer)
    return buffer.getvalue()
