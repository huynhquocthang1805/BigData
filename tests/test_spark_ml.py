"""wq.spark_ml: fit và benchmark theo trạm trên Spark phải cho cùng kết quả như tính bằng pandas."""
import numpy as np
import pandas as pd
import pytest

from wq import contracts
from wq.evaluation import apply_mask, make_mask, score_imputation
from wq.models.baselines import LOCFImputer
from wq.models.imputers_tv3 import KalmanImputer, KNNImputer
from wq.spark_ml import aggregate, fit_by_station, imputation_benchmark, station_benchmark, station_seed

FACTORIES = {"locf": LOCFImputer, "kalman": KalmanImputer}


def split_days(sample_df):
    df = sample_df[contracts.MEASUREMENT_COLUMNS]
    cut = df["ts"].min() + pd.Timedelta(days=1)
    return df[df["ts"] < cut].reset_index(drop=True), df[df["ts"] >= cut].reset_index(drop=True)


def to_spark(spark, df):
    return spark.createDataFrame(df)


@pytest.mark.parametrize("cls,attr", [(KalmanImputer, "parameters_"), (KNNImputer, "models_")])
def test_fit_by_station_matches_pandas_fit(spark, sample_df, cls, attr):
    train, _ = split_days(sample_df)
    on_spark = fit_by_station(to_spark(spark, train), cls)
    local = cls().fit(train)
    assert set(getattr(on_spark, attr)) == set(getattr(local, attr)) == {"S1", "S2"}
    if cls is KalmanImputer:
        for sid in ("S1", "S2"):
            for a, b in zip(on_spark.parameters_[sid], local.parameters_[sid]):
                np.testing.assert_allclose(a, b, equal_nan=True)
    _, truth = split_days(sample_df)
    pd.testing.assert_frame_equal(on_spark.transform(truth), local.transform(truth))


def test_spark_benchmark_equals_pandas_per_station(spark, sample_df):
    train, truth = split_days(sample_df)
    kw = dict(seeds=[7], kinds=("random", "block"), rates=(0.1, 0.2))
    scores, parts = imputation_benchmark(to_spark(spark, train), to_spark(spark, truth), FACTORIES, **kw)
    expected = aggregate(pd.concat([
        station_benchmark(s, train[train.station_id == s].reset_index(drop=True),
                          truth[truth.station_id == s].reset_index(drop=True), FACTORIES, **kw)
        for s in ("S1", "S2")]))
    assert set(parts["station_id"]) == {"S1", "S2"}
    cols = ["method", "missing_type", "missing_rate", "variable", "n_masked", "n_scored", "mae", "rmse", "r2"]
    pd.testing.assert_frame_equal(scores[cols].reset_index(drop=True), expected[cols].reset_index(drop=True))


def test_aggregate_equals_score_imputation_on_all_stations(sample_df):
    """Tổng sai số theo trạm cộng lại phải ra đúng MAE/RMSE/R² gộp của score_imputation."""
    train, truth = split_days(sample_df)
    parts = pd.concat([station_benchmark(s, train[train.station_id == s].reset_index(drop=True),
                                         truth[truth.station_id == s].reset_index(drop=True),
                                         {"locf": LOCFImputer}, [3], ("block",), (0.2,)) for s in ("S1", "S2")])
    got = aggregate(parts).set_index("variable")
    masks, filled = [], []
    for s in ("S1", "S2"):
        t = truth[truth.station_id == s].reset_index(drop=True)
        m = make_mask(t, 0.2, "block", seed=station_seed(3, s))
        masks.append(m)
        filled.append(LOCFImputer().fit(train).transform(apply_mask(t, m)))
    want = score_imputation(truth, pd.concat(filled, ignore_index=True), pd.concat(masks, ignore_index=True),
                            contracts.CORE_VARIABLES).set_index("variable")
    for var in want.index[want.n_scored > 0]:
        for c in ("n_masked", "n_scored"):
            assert got.loc[var, c] == want.loc[var, c]
        for c in ("mae", "rmse", "r2"):
            assert got.loc[var, c] == pytest.approx(want.loc[var, c], rel=1e-9, abs=1e-12, nan_ok=True)
