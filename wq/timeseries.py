"""Tiện ích chuỗi thời gian (pandas) dùng chung cho imputation/prediction/streaming.

Định dạng chuẩn của 1 DataFrame đo đạc (xem docs/data_contract.md):
    station_id: str | ts: datetime64[ns, UTC] (lưới 15 phút) | water_temp ... discharge: float64 (NaN = thiếu)
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from wq import contracts


def ensure_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Chuẩn hóa kiểu dữ liệu + thêm cột biến còn thiếu (NaN) + sắp xếp."""
    out = df.copy()
    out["ts"] = pd.to_datetime(out["ts"], utc=True)
    for c in contracts.VARIABLE_NAMES:
        if c not in out.columns:
            out[c] = np.nan
        out[c] = pd.to_numeric(out[c], errors="coerce").astype("float64")
    out["station_id"] = out["station_id"].astype(str)
    return out.sort_values(contracts.KEY_COLUMNS).reset_index(drop=True)


def regularize(df: pd.DataFrame, start: pd.Timestamp | None = None, end: pd.Timestamp | None = None,
               freq: str = contracts.FREQ) -> pd.DataFrame:
    """Đưa mỗi trạm về lưới thời gian đều (mặc định 15 phút).

    - Mốc thời gian bị thiếu hoàn toàn (cảm biến không gửi) => thêm dòng với giá trị NaN.
    - Nhiều bản ghi trong cùng 1 ô 15 phút => lấy trung bình.
    """
    if df.empty:
        return ensure_schema(df)
    df = ensure_schema(df)
    parts = []
    for sid, g in df.groupby("station_id", sort=True):
        g = g.assign(ts=g["ts"].dt.floor(freq)).groupby("ts")[contracts.VARIABLE_NAMES].mean()
        lo = start if start is not None else g.index.min()
        hi = end if end is not None else g.index.max()
        grid = pd.date_range(pd.Timestamp(lo).floor(freq), pd.Timestamp(hi).floor(freq), freq=freq, tz="UTC")
        g = g.reindex(grid)
        g.index.name = "ts"
        g = g.reset_index()
        g.insert(0, "station_id", sid)
        parts.append(g)
    return pd.concat(parts, ignore_index=True)


def missing_mask(df: pd.DataFrame, columns: list[str] | None = None) -> pd.Series:
    """Bitmask (int) theo contracts.VARIABLE_BIT cho các ô đang NaN."""
    cols = columns or contracts.VARIABLE_NAMES
    mask = pd.Series(0, index=df.index, dtype="int64")
    for c in cols:
        mask = mask | (df[c].isna().astype("int64") * contracts.VARIABLE_BIT[c])
    return mask


def time_split(df: pd.DataFrame, split: str) -> pd.DataFrame:
    """Lấy phần dữ liệu thuộc train/valid/test theo contracts.SPLITS (nửa mở [start, end))."""
    lo, hi = contracts.SPLITS[split]
    ts = pd.to_datetime(df["ts"], utc=True)
    return df[(ts >= pd.Timestamp(lo, tz="UTC")) & (ts < pd.Timestamp(hi, tz="UTC"))]
