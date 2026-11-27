"""Tạo kịch bản thiếu dữ liệu và chấm kết quả điền khuyết."""
from __future__ import annotations

from collections.abc import Sequence
from numbers import Integral

import numpy as np
import pandas as pd

from wq import contracts


def make_mask(
    df: pd.DataFrame,
    rate: float,
    kind: str,
    *,
    variables: Sequence[str] = contracts.CORE_VARIABLES,
    block_steps: int | tuple[int, int] = (4, 96),
    seed: int | None = None,
) -> pd.DataFrame:
    """Chọn ô đã quan sát để che, giữ nguyên chỉ mục và thứ tự dòng.

    Tỉ lệ tính riêng trên số ô đã quan sát của mỗi biến, làm tròn đến ô
    gần nhất. Khối đi theo lưới 15 phút của từng trạm và dừng ở khoảng
    thiếu sẵn. Tạo khối theo khoảng độ dài yêu cầu khi đủ dữ liệu.
    Khối cuối hoặc đoạn quan sát ngắn có thể ngắn hơn để đạt đúng tỉ lệ.
    Ở tỉ lệ cao, các khối có thể tiếp giáp khi không còn chỗ tách chúng.
    """
    if not 0 <= rate <= 1:
        raise ValueError("rate phải nằm trong [0, 1]")
    if kind not in ("random", "block"):
        raise ValueError("kind phải là 'random' hoặc 'block'")
    columns = list(variables)
    if len(set(columns)) != len(columns):
        raise ValueError("variables không được có cột trùng")
    if set(columns).intersection((*contracts.KEY_COLUMNS, "qc_flags")):
        raise ValueError("variables chỉ được chọn các cột đo")

    observed = df[columns].notna().to_numpy()
    mask = np.zeros_like(observed)
    rng = np.random.default_rng(seed)
    if kind == "block":
        lo, hi = (block_steps, block_steps) if isinstance(block_steps, Integral) else block_steps
        if not isinstance(lo, Integral) or not isinstance(hi, Integral) or not 1 <= lo <= hi:
            raise ValueError("block_steps phải là số bước dương hoặc cặp (min, max) hợp lệ")
        order, same_run = _time_order(df)

    for col, available in enumerate(observed.T):
        count = round(rate * np.count_nonzero(available))
        if count == 0:
            continue
        if kind == "random":
            positions = rng.choice(np.flatnonzero(available), size=count, replace=False)
        else:
            selected = _sample_blocks(available[order], same_run, count, lo, hi, rng)
            positions = order[selected]
        mask[positions, col] = True
    return pd.DataFrame(mask, index=df.index, columns=columns)


def _time_order(df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    keys = df[contracts.KEY_COLUMNS].reset_index(drop=True)
    keys["ts"] = pd.to_datetime(keys["ts"], utc=True)
    if keys.isna().any().any() or keys.duplicated().any():
        raise ValueError("station_id và ts phải đầy đủ và xác định duy nhất mỗi dòng")
    sorted_keys = keys.sort_values(contracts.KEY_COLUMNS)
    same_station = sorted_keys["station_id"].eq(sorted_keys["station_id"].shift())
    next_step = sorted_keys["ts"].diff().eq(pd.Timedelta(contracts.FREQ))
    return sorted_keys.index.to_numpy(), (same_station & next_step).to_numpy(dtype=bool)


def _sample_blocks(
    observed: np.ndarray,
    same_run: np.ndarray,
    count: int,
    lo: int,
    hi: int,
    rng: np.random.Generator,
) -> np.ndarray:
    previous = np.r_[False, observed[:-1]]
    connected = observed & previous & same_run
    starts = np.flatnonzero(observed & ~connected)
    ends = np.flatnonzero(observed & ~np.r_[connected[1:], False]) + 1

    long_runs: list[tuple[int, int]] = []
    short_runs: list[tuple[int, int]] = []

    def add_run(start: int, end: int) -> None:
        if end - start >= lo:
            long_runs.append((start, end))
        elif end > start:
            short_runs.append((start, end))

    for start, end in zip(starts, ends):
        add_run(start, end)

    selected = np.zeros(len(observed), dtype=bool)
    separators: list[int] = []
    while count and (long_runs or short_runs):
        runs = long_runs or short_runs
        run_index = int(rng.integers(len(runs)))
        start, end = runs[run_index]
        runs[run_index] = runs[-1]
        runs.pop()

        max_size = min(hi, end - start, count)
        size = int(rng.integers(min(lo, max_size), max_size + 1))
        if 0 < count - size < lo:
            # Tránh chừa một khối cuối quá ngắn nếu có thể điều chỉnh.
            if count <= max_size:
                size = count
            elif count - lo >= lo:
                size = count - lo
        left = int(rng.integers(start, end - size + 1))
        right = left + size
        selected[left:right] = True
        count -= size

        if left > start:
            separators.append(left - 1)
            add_run(start, left - 1)
        if right < end:
            separators.append(right)
            add_run(right + 1, end)

    if count:
        selected[rng.choice(separators, size=count, replace=False)] = True
    return selected


def _validate_mask(df: pd.DataFrame, mask: pd.DataFrame) -> None:
    if not mask.index.equals(df.index):
        raise ValueError("mask phải có cùng chỉ mục và thứ tự dòng với dữ liệu")
    if not mask.columns.is_unique or not mask.columns.isin(df.columns).all():
        raise ValueError("các cột mask phải duy nhất và có trong dữ liệu")
    if not all(pd.api.types.is_bool_dtype(dtype) for dtype in mask.dtypes) or mask.isna().any().any():
        raise ValueError("mask chỉ được chứa giá trị boolean")


def apply_mask(df: pd.DataFrame, mask: pd.DataFrame) -> pd.DataFrame:
    """Đặt NaN ở các ô được chọn trên bản sao của dữ liệu."""
    _validate_mask(df, mask)
    out = df.copy()
    out[mask.columns] = out[mask.columns].mask(mask)
    return out


def score_imputation(
    truth: pd.DataFrame,
    imputed: pd.DataFrame,
    mask: pd.DataFrame,
    variables: Sequence[str],
) -> pd.DataFrame:
    """Tính MAE, RMSE và R² theo biến trên các ô bị che có giá trị thật.

    n_masked đếm các ô có đáp án hữu hạn. n_scored đếm số ô trong đó
    imputer trả về kết quả hữu hạn. Chỉ so sánh sai số cùng với độ bao phủ.
    R² là NaN nếu có dưới hai cặp. Với đáp án không đổi, R² là 1 cho
    dự đoán hoàn hảo và 0 trong các trường hợp còn lại.
    """
    _validate_mask(truth, mask)
    if not imputed.index.equals(truth.index):
        raise ValueError("imputed phải có cùng chỉ mục và thứ tự dòng với truth")
    keys = [key for key in contracts.KEY_COLUMNS if key in truth]
    if keys and (not set(keys).issubset(imputed.columns) or not truth[keys].equals(imputed[keys])):
        raise ValueError("imputed phải giữ nguyên khóa trạm và thời gian của truth")

    rows = []
    for column in variables:
        selected = mask[column].to_numpy(dtype=bool)
        actual = truth.loc[selected, column].to_numpy(dtype=float, na_value=np.nan)
        predicted = imputed.loc[selected, column].to_numpy(dtype=float, na_value=np.nan)
        known = np.isfinite(actual)
        scored = known & np.isfinite(predicted)
        n_scored = int(scored.sum())
        mae = rmse = r2 = np.nan
        if n_scored:
            actual = actual[scored]
            errors = predicted[scored] - actual
            mae = float(np.abs(errors).mean())
            mse = float(np.square(errors).mean())
            rmse = float(np.sqrt(mse))
            if n_scored >= 2:
                if np.all(actual == actual[0]):
                    r2 = float(mse == 0.0)
                else:
                    variance = float(np.square(actual - actual.mean()).mean())
                    r2 = 1.0 - mse / variance
        rows.append((column, int(known.sum()), n_scored, mae, rmse, r2))

    return pd.DataFrame(rows, columns=["variable", "n_masked", "n_scored", "mae", "rmse", "r2"])
