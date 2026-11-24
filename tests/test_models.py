import numpy as np
import pandas as pd
import pytest

from wq import contracts
from wq.models.base import PREDICTION_COLUMNS, BaseForecaster, BaseImputer
from wq.models.baselines import (LinearInterpolationImputer, LOCFImputer, PersistenceForecaster,
                                 SeasonalNaiveForecaster)
from wq.models.registry import load_forecaster, load_imputer
from wq.timeseries import missing_mask, regularize, time_split


def test_regularize_inserts_missing_timestamps_and_averages():
    df = pd.DataFrame({"station_id": ["A", "A", "A"],
                       "ts": ["2025-01-01T00:00Z", "2025-01-01T00:05Z", "2025-01-01T01:00Z"],
                       "ph": [7.0, 8.0, 7.2]})
    out = regularize(df)
    assert len(out) == 5  # 00:00, 00:15, 00:30, 00:45, 01:00
    assert out.loc[0, "ph"] == pytest.approx(7.5)  # 00:00 và 00:05 cùng ô 15 phút -> trung bình
    assert out["ph"].isna().sum() == 3
    assert set(contracts.VARIABLE_NAMES) <= set(out.columns)


def test_locf_is_causal(sample_df):
    imp = LOCFImputer()
    full = imp.transform(sample_df)
    # đổi dữ liệu TƯƠNG LAI không được làm thay đổi giá trị đã điền ở QUÁ KHỨ
    cut = 60
    changed = sample_df.copy()
    changed.loc[cut:, "dissolved_oxygen"] = 99.0
    again = imp.transform(changed)
    pd.testing.assert_series_equal(full.loc[:cut - 1, "dissolved_oxygen"], again.loc[:cut - 1, "dissolved_oxygen"])
    assert imp.causal


def test_locf_fills_gaps_and_keeps_observed(sample_df):
    out = LOCFImputer(max_gap_steps=8).transform(sample_df)
    s1 = out[out.station_id == "S1"]
    assert s1["dissolved_oxygen"].isna().sum() == 0
    observed = sample_df["dissolved_oxygen"].notna()
    np.testing.assert_allclose(out.loc[observed, "dissolved_oxygen"], sample_df.loc[observed, "dissolved_oxygen"])
    # bitmask: những ô thiếu trước khi điền phải bật bit DO
    before = missing_mask(sample_df)
    assert ((before & contracts.VARIABLE_BIT["dissolved_oxygen"]) > 0).sum() == sample_df["dissolved_oxygen"].isna().sum()


def test_linear_interpolation_non_causal(sample_df):
    imp = LinearInterpolationImputer()
    out = imp.transform(sample_df)
    assert not imp.causal
    assert out.loc[11, "dissolved_oxygen"] == pytest.approx(
        np.interp(11, [9, 14], sample_df.loc[[9, 14], "dissolved_oxygen"]), rel=1e-6)


@pytest.mark.parametrize("cls", [PersistenceForecaster, SeasonalNaiveForecaster])
def test_forecaster_output_contract(cls, sample_df):
    hist = LOCFImputer().transform(sample_df)
    pred = cls().predict(hist)
    assert list(pred.columns) == PREDICTION_COLUMNS
    assert len(pred) == 2 * len(contracts.FORECAST_TARGETS) * len(contracts.FORECAST_HORIZONS_STEPS)
    assert (pred["target_ts"] - pred["issued_at"] == pd.to_timedelta(pred["horizon_min"], unit="min")).all()
    assert pred["y_pred"].notna().all()


def test_seasonal_naive_uses_previous_day(sample_df):
    hist = LOCFImputer().transform(sample_df)
    pred = SeasonalNaiveForecaster().predict(hist)
    row = pred[(pred.station_id == "S2") & (pred.horizon_min == 60)].iloc[0]
    expected = hist[(hist.station_id == "S2") & (hist.ts == row.target_ts - pd.Timedelta(days=1))]["dissolved_oxygen"]
    assert row.y_pred == pytest.approx(expected.iloc[0])


def test_registry_builtin_and_joblib_roundtrip(tmp_path):
    assert isinstance(load_imputer("locf"), BaseImputer)
    assert isinstance(load_forecaster("seasonal_naive"), BaseForecaster)
    path = LOCFImputer(max_gap_steps=3).save(tmp_path / "imp.joblib")
    loaded = load_imputer(str(path))
    assert isinstance(loaded, LOCFImputer) and loaded.max_gap_steps == 3
    PersistenceForecaster().save(tmp_path / "f.joblib")
    with pytest.raises(TypeError):
        load_imputer(str(tmp_path / "f.joblib"))
    with pytest.raises(ValueError):
        load_forecaster("khong_ton_tai")


def test_time_split_half_open():
    ts = pd.to_datetime(["2024-12-31T23:45Z", "2025-01-01T00:00Z", "2025-07-01T00:00Z"])
    df = pd.DataFrame({"station_id": "A", "ts": ts})
    assert len(time_split(df, "train")) == 1
    assert len(time_split(df, "valid")) == 1
    assert len(time_split(df, "test")) == 1
