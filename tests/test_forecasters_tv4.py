import numpy as np
import pandas as pd
import pytest

from wq import contracts
from wq.models.base import PREDICTION_COLUMNS
from wq.models.baselines import LOCFImputer
from wq.models.forecast_experiment import fit_station_ridges, history_features
from wq.models.forecasters_tv4 import RidgeForecaster, TARGET
from wq.models.registry import load_forecaster


@pytest.fixture
def training(sample_df):
    return pd.concat([sample_df.assign(ts=sample_df.ts - pd.Timedelta(days=d)) for d in (367, 365)],
                     ignore_index=True)


def test_export_matches_fixed_ridge_at_each_issue(sample_df, training, tmp_path):
    model = RidgeForecaster().fit(training)
    path = tmp_path / "ridge.csv"
    model.to_csv(path)
    exported = RidgeForecaster.from_csv(path)
    restored = load_forecaster(str(exported.save(tmp_path / "ridge.joblib")))
    history = LOCFImputer().transform(sample_df)
    for sid, station in history.groupby("station_id"):
        expected = fit_station_ridges(training[training.station_id == sid])
        for length in [97, 150, len(station)]:
            past = station.iloc[:length]
            actual = restored.predict(past).set_index("horizon_min")
            assert set(actual.index) == {60, 180, 360, 1440}
            series = past.set_index("ts")[TARGET]
            for h, pipeline in expected.items():
                row = actual.loc[h * contracts.FREQ_MINUTES]
                assert row.model == "ridge" and row.target == TARGET and row.station_id == sid
                assert row.issued_at == past.ts.iloc[-1]
                assert row.target_ts == row.issued_at + pd.Timedelta(minutes=h * contracts.FREQ_MINUTES)
                assert row.y_pred == pytest.approx(pipeline.predict(history_features(series, h).tail(1))[0], abs=1e-10)


def test_missing_history_unknown_station_and_sparse_grid(sample_df, training):
    model = RidgeForecaster().fit(training)
    station = LOCFImputer().transform(sample_df.query("station_id == 'S2'"))
    valid = model.predict(station)
    assert list(valid.columns) == PREDICTION_COLUMNS
    assert len(valid) == 4
    assert model.predict(station.iloc[:96]).empty
    assert model.predict(station.assign(station_id="unseen")).empty
    assert model.predict(station.assign(dissolved_oxygen=np.nan)).empty
    assert model.predict(station.drop(columns=TARGET)).empty
    # Dropping y(t-24h) must not move an older reading into this lag.
    assert model.predict(station.drop(station.index[-97])).empty
    pd.testing.assert_frame_equal(model.predict(station.sample(frac=1, random_state=1)), valid)
