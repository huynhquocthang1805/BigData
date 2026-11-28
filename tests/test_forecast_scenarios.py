import numpy as np
import pandas as pd
import pytest

from benchmarks.bench_forecast_scenarios import TARGET, station_scores, summarize
from wq import contracts
from wq.evaluation import make_mask
from wq.spark_ml import station_seed


def test_hourly_targets_coverage_and_paired_errors(monkeypatch):
    train_ts = pd.date_range("2024-12-01", periods=576, freq="15min", tz="UTC")
    test_ts = pd.date_range("2025-07-01", periods=576, freq="15min", tz="UTC")
    monkeypatch.setitem(contracts.SPLITS, "test", ("2025-07-01", "2025-07-07"))
    train = pd.DataFrame({"station_id": "S1", "ts": train_ts, TARGET: 8 + np.sin(np.arange(576) / 16)})
    evaluation = train.assign(ts=test_ts)
    evaluation.loc[180:202, TARGET] = np.nan
    mask = make_mask(evaluation, 0.2, "block", variables=[TARGET], seed=station_seed(42, "S1"))
    truth = evaluation[TARGET].mask(mask[TARGET])
    frame = pd.concat([train, evaluation], ignore_index=True)
    parts = station_scores(frame, 0.2, [42])
    # A station with observed test targets but insufficient training remains in the denominator.
    empty_train = station_scores(evaluation.assign(station_id="S2"), 0.2, [42])
    assert (empty_train.n_targets > 0).all()
    assert not empty_train.n_scored.any()
    report = summarize(pd.concat([parts, empty_train], ignore_index=True))
    assert (report.n_stations_considered == 2).all()
    assert (report.n_stations_scored == 1).all()
    for horizon, group in parts.groupby("horizon_min"):
        steps = horizon // contracts.FREQ_MINUTES
        expected = truth.shift(-steps).iloc[::contracts.STEPS_PER_HOUR].notna().sum()
        assert (group.n_targets == expected).all()
        rows = group.set_index("scenario")
        assert rows.loc["locf", "n_scored"] > rows.loc["masked", "n_scored"]
        assert (rows.n_common == rows.loc["masked", "n_scored"]).all()
        assert rows.common_sse.to_numpy() == pytest.approx(np.repeat(rows.loc["original", "common_sse"], 3))
        assert rows.loc["locf", "n_recovered_pair"] == rows.loc["original", "n_recovered_pair"] > 0
        assert rows.loc["masked", "n_recovered_pair"] == 0
