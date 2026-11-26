import math

import pandas as pd
import pytest

from wq import contracts
from wq.evaluation import apply_mask, make_mask, score_imputation


@pytest.mark.parametrize("kind", ["random", "block"])
@pytest.mark.parametrize("rate", [0.0, 0.05, 0.10, 0.20, 0.30, 1.0])
def test_mask_rate_and_seed(sample_df, kind, rate):
    columns = contracts.CORE_VARIABLES
    mask = make_mask(sample_df, rate, kind, seed=42)
    observed = sample_df[columns].notna()

    assert mask.columns.tolist() == columns
    assert not (mask & ~observed).any().any()
    assert (mask.sum() == (observed.sum() * rate).round()).all()
    pd.testing.assert_frame_equal(mask, make_mask(sample_df, rate, kind, seed=42))


def test_block_stays_within_station_and_observed_time_runs(sample_df):
    column = "dissolved_oxygen"
    # Station boundaries, a timestamp absent from the grid, and natural NaNs
    # all split a run. Shuffle to ensure the mask uses time order.
    frame = sample_df.drop(index=240).sample(frac=1, random_state=7)
    mask = make_mask(frame, 0.30, "block", variables=[column], block_steps=(4, 12), seed=7)
    assert mask.index.equals(frame.index)

    ordered = frame.assign(masked=mask[column]).sort_values(["station_id", "ts"])
    new_run = (
        ordered["station_id"].ne(ordered["station_id"].shift())
        | ordered["ts"].diff().ne(pd.Timedelta(minutes=15))
        | ~ordered["masked"].shift(fill_value=False)
    )
    lengths = ordered[ordered["masked"]].groupby(new_run.cumsum()).size()
    assert len(lengths) > 1
    assert lengths.between(4, 12).all()
    assert not (mask[column] & frame[column].isna()).any()


def test_apply_mask_changes_only_selected_cells(sample_df):
    columns = ["dissolved_oxygen"]
    original = sample_df.copy()
    mask = make_mask(sample_df, 0.20, "random", variables=columns, seed=42)
    result = apply_mask(sample_df, mask)

    assert mask.columns.tolist() == columns
    assert result[columns].isna().equals(sample_df[columns].isna() | mask)
    pd.testing.assert_frame_equal(result[columns].mask(mask), original[columns].mask(mask))
    pd.testing.assert_frame_equal(result.drop(columns=columns), original.drop(columns=columns))
    pd.testing.assert_frame_equal(sample_df, original)


def test_empty_frame_preserves_mask_schema(sample_df):
    empty = sample_df.iloc[:0]
    mask = make_mask(empty, 0.20, "block", seed=42)
    assert mask.empty
    assert mask.columns.tolist() == contracts.CORE_VARIABLES
    pd.testing.assert_frame_equal(apply_mask(empty, mask), empty)


def test_perfect_imputation_scores_zero_error(sample_df):
    columns = contracts.CORE_VARIABLES
    mask = make_mask(sample_df, 0.20, "block", seed=42)
    result = score_imputation(sample_df, sample_df, mask, columns)

    assert result["variable"].tolist() == columns
    assert result["n_scored"].tolist() == mask.sum().tolist()
    assert (result[["mae", "rmse"]] == 0).all().all()
    assert (result["r2"] == 1).all()


def test_score_matches_manual_calculation_and_ignores_unmasked_cells():
    truth = pd.DataFrame({"do": [2.0, 4.0, 6.0, float("nan")]}, index=[10, 20, 30, 40])
    mask = pd.DataFrame({"do": [True, True, False, True]}, index=truth.index)
    imputed = pd.DataFrame({"do": [1.0, 6.0, 6.0, 900.0]}, index=truth.index)

    result = score_imputation(truth, imputed, mask, ["do"])
    row = result.iloc[0]
    assert row["n_masked"] == row["n_scored"] == 2
    assert row["mae"] == pytest.approx(1.5)
    assert row["rmse"] == pytest.approx(math.sqrt(2.5))
    assert row["r2"] == pytest.approx(-1.5)

    imputed.loc[30, "do"] = -1_000.0
    imputed.loc[40, "do"] = 1_000.0
    pd.testing.assert_frame_equal(score_imputation(truth, imputed, mask, ["do"]), result)


@pytest.mark.parametrize(
    ("predictions", "scored", "expected_r2"),
    [
        ([float("nan"), float("inf"), float("nan")], 0, None),
        ([0.1, float("nan"), float("nan")], 1, None),
        ([0.1, 0.1, 0.1], 3, 1.0),
        ([0.2, 0.2, 0.2], 3, 0.0),
    ],
)
def test_score_reports_unfilled_cells_and_constant_target(predictions, scored, expected_r2):
    truth = pd.DataFrame({"do": [0.1, 0.1, 0.1]})
    mask = pd.DataFrame({"do": [True, True, True]})
    result = score_imputation(truth, pd.DataFrame({"do": predictions}), mask, ["do"]).iloc[0]

    assert result["n_masked"] == 3
    assert result["n_scored"] == scored
    if expected_r2 is None:
        assert math.isnan(result["r2"])
    else:
        assert result["r2"] == expected_r2
    assert math.isnan(result["mae"]) == (scored == 0)


def test_apply_mask_rejects_misaligned_index_and_non_boolean_values(sample_df):
    mask = make_mask(sample_df, 0.20, "random", seed=42)
    with pytest.raises(ValueError, match="chỉ mục"):
        apply_mask(sample_df, mask.iloc[::-1])
    with pytest.raises(ValueError, match="boolean"):
        apply_mask(sample_df, mask.astype(int))


def test_score_does_not_silently_pair_different_timestamps(sample_df):
    mask = make_mask(sample_df, 0.20, "random", seed=42)
    with pytest.raises(ValueError, match="khóa trạm và thời gian"):
        score_imputation(sample_df, sample_df.iloc[::-1].reset_index(drop=True), mask, contracts.CORE_VARIABLES)
