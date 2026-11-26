import os

import numpy as np
import pandas as pd
import pytest
from sklearn.exceptions import NotFittedError

from wq.contracts import CORE_VARIABLES, KEY_COLUMNS
from wq.evaluation import apply_mask, make_mask, score_imputation
from wq.models.imputers_tv3 import IterativeImputer, KNNImputer, LinearInterpolationImputer


def test_linear_interpolates_by_time_within_station():
    ts = pd.date_range("2025-07-01", periods=5, freq="15min", tz="UTC")
    data = pd.DataFrame({
        "station_id": ["A"] * 5 + ["B"] * 5,
        "ts": ts.append(ts),
        "dissolved_oxygen": [np.nan, 4.0, np.nan, 8.0, np.nan, np.nan, 8.0, np.nan, 10.0, np.nan],
    })
    filled = LinearInterpolationImputer().transform(data)
    np.testing.assert_array_equal(
        filled["dissolved_oxygen"], [np.nan, 4.0, 6.0, 8.0, np.nan, np.nan, 8.0, 9.0, 10.0, np.nan]
    )


@pytest.fixture
def paired_measurements():
    columns = ["water_temp", "dissolved_oxygen"]
    train = pd.DataFrame({
        "station_id": ["A"] * 20 + ["B"] * 20,
        "water_temp": np.tile(np.arange(20, dtype=float), 2),
        "dissolved_oxygen": np.r_[2.0 + np.arange(20) / 5, 10.0 + np.arange(20) / 5],
    })
    query = pd.DataFrame({
        "station_id": ["B", "A", "C", "A"],
        "water_temp": [10.0, 10.0, 10.0, np.nan],
        "dissolved_oxygen": np.nan,
    }, index=[8, 6, 8, 10])
    return columns, train, query


@pytest.mark.parametrize("model_cls", [KNNImputer, IterativeImputer], ids=["knn", "iterative"])
def test_learned_imputer_keeps_rows_local_and_preserves_observations(model_cls, paired_measurements):
    columns, train, query = paired_measurements
    model = model_cls(variables=columns)
    with pytest.raises(NotFittedError):
        model.transform(query)

    original = query.copy()
    filled = model.fit(train).transform(query)
    pd.testing.assert_frame_equal(query, original)
    pd.testing.assert_frame_equal(filled.drop(columns="dissolved_oxygen"), query.drop(columns="dissolved_oxygen"))
    np.testing.assert_allclose(filled["dissolved_oxygen"], [12.0, 4.0, np.nan, np.nan], atol=1e-3)
    pd.testing.assert_frame_equal(model.transform(query.iloc[:1]), filled.iloc[:1])


def test_knn_reference_sampling_keeps_rare_measurement_and_is_reproducible():
    train = pd.DataFrame({
        "station_id": "A",
        "water_temp": np.arange(50, dtype=float),
        "ph": [np.nan] * 49 + [7.2],
    })
    query = train.iloc[[48]].copy()
    config = {"variables": ["water_temp", "ph"], "n_neighbors": 1, "max_train_rows": 5, "seed": 42}
    first, second = (KNNImputer(**config).fit(train).transform(query) for _ in range(2))
    pd.testing.assert_frame_equal(first, second)
    assert first["ph"].iloc[0] == pytest.approx(7.2)


@pytest.fixture(scope="module")
def usgs_silver():
    train_lake = os.environ.get("WQ_TV3_TEST_TRAIN_LAKE")
    test_lake = os.environ.get("WQ_TV3_TEST_HELDOUT_LAKE")
    if not train_lake or not test_lake:
        pytest.skip("Cần đường dẫn silver USGS train và test để chạy kiểm tra tích hợp.")
    from wq.io import read_silver

    train = read_silver(stations="core", start="2024-06-01", end="2024-07-01", lake=train_lake)
    truth = read_silver(stations="core", start="2025-07-01", end="2025-07-08", lake=test_lake)
    assert train["station_id"].nunique() == truth["station_id"].nunique() == 10
    assert len(train) == 28_800 and len(truth) == 6_720
    assert train["ts"].max() < truth["ts"].min()
    mask = make_mask(truth, 0.20, "block", seed=42)
    return train, truth, mask


@pytest.mark.parametrize(
    "model_cls", [LinearInterpolationImputer, KNNImputer, IterativeImputer], ids=["linear", "knn", "iterative"]
)
def test_imputer_on_real_usgs_silver(model_cls, usgs_silver):
    train, truth, mask = usgs_silver
    masked = apply_mask(truth, mask)
    original = masked.copy()
    result = model_cls().fit(train).transform(masked)

    pd.testing.assert_frame_equal(masked, original)
    assert result.index.equals(masked.index) and result.columns.equals(masked.columns)
    metadata = KEY_COLUMNS + ["qc_flags", "discharge"]
    pd.testing.assert_frame_equal(result[metadata], masked[metadata])
    observed = masked[CORE_VARIABLES].notna().to_numpy()
    np.testing.assert_array_equal(
        result[CORE_VARIABLES].to_numpy()[observed], masked[CORE_VARIABLES].to_numpy()[observed]
    )
    scores = score_imputation(truth, result, mask, CORE_VARIABLES)
    assert (scores["n_scored"] > 0).all()
    assert np.isfinite(scores[["mae", "rmse", "r2"]].to_numpy()).all()
