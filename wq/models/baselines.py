"""Mô hình BASELINE để pipeline chạy end-to-end ngay từ tuần 1.

TV3/TV4 PHẢI so sánh mô hình của mình với các baseline này trong chương 5
(một mô hình phức tạp mà không thắng được Persistence/Seasonal-naive thì không có giá trị).
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from wq import contracts
from wq.models.base import BaseForecaster, BaseImputer, make_prediction_frame


class LOCFImputer(BaseImputer):
    """Last Observation Carried Forward (causal - dùng được trong streaming).

    1) ffill tối đa `max_gap_steps` bước;  2) khoảng trống dài hơn: lấy giá trị cùng giờ hôm trước
    (chu kỳ ngày của DO/nhiệt độ);  3) còn lại: ffill không giới hạn.
    """

    name = "locf"
    causal = True

    def __init__(self, max_gap_steps: int = 8, seasonal_fallback: bool = True):
        self.max_gap_steps = max_gap_steps
        self.seasonal_fallback = seasonal_fallback

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        cols = [c for c in self.variables if c in out.columns]
        for _, idx in out.groupby("station_id", sort=False).groups.items():
            g = out.loc[idx, cols]
            filled = g.ffill(limit=self.max_gap_steps)
            if self.seasonal_fallback:
                filled = filled.fillna(filled.shift(contracts.STEPS_PER_DAY))
            filled = filled.ffill()
            out.loc[idx, cols] = filled.to_numpy()
        return out


class LinearInterpolationImputer(BaseImputer):
    """Nội suy tuyến tính theo thời gian (NON-causal: dùng cả điểm sau => chỉ dùng offline)."""

    name = "linear"
    causal = False

    def __init__(self, max_gap_steps: int | None = None):
        self.max_gap_steps = max_gap_steps

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        cols = [c for c in self.variables if c in out.columns]
        for _, idx in out.groupby("station_id", sort=False).groups.items():
            g = out.loc[idx, ["ts", *cols]].set_index("ts")
            g = g.interpolate(method="time", limit=self.max_gap_steps, limit_area="inside")
            out.loc[idx, cols] = g[cols].to_numpy()
        return out


class _LastValueBase(BaseForecaster):
    def _iter_targets(self, history: pd.DataFrame):
        for sid, g in history.sort_values("ts").groupby("station_id", sort=False):
            for target in self.targets:
                s = g[["ts", target]].dropna()
                if len(s) == 0:
                    continue
                yield sid, target, g, s


class PersistenceForecaster(_LastValueBase):
    """y(t+h) = y(t). Baseline tối thiểu."""

    name = "persistence"
    min_history_steps = 1

    def predict(self, history: pd.DataFrame) -> pd.DataFrame:
        rows = []
        for sid, target, g, s in self._iter_targets(history):
            issued_at = g["ts"].iloc[-1]
            last = float(s[target].iloc[-1])
            for h in self.horizons_steps:
                rows.append(dict(station_id=sid, target=target, issued_at=issued_at,
                                 horizon_min=h * contracts.FREQ_MINUTES,
                                 target_ts=issued_at + pd.Timedelta(minutes=h * contracts.FREQ_MINUTES),
                                 y_pred=last, model=self.name))
        return make_prediction_frame(rows)


class SeasonalNaiveForecaster(_LastValueBase):
    """y(t+h) = y(t+h-96k): giá trị cùng thời điểm trong ngày gần nhất (chu kỳ ngày-đêm rất rõ ở DO)."""

    name = "seasonal_naive"
    min_history_steps = contracts.STEPS_PER_DAY

    def predict(self, history: pd.DataFrame) -> pd.DataFrame:
        rows = []
        for sid, target, g, s in self._iter_targets(history):
            issued_at = g["ts"].iloc[-1]
            series = s.set_index("ts")[target]
            last = float(series.iloc[-1])
            for h in self.horizons_steps:
                step = pd.Timedelta(minutes=h * contracts.FREQ_MINUTES)
                k = int(np.ceil(h / contracts.STEPS_PER_DAY))
                ref_ts = issued_at + step - pd.Timedelta(days=k)
                y = series.get(ref_ts, np.nan)
                rows.append(dict(station_id=sid, target=target, issued_at=issued_at,
                                 horizon_min=h * contracts.FREQ_MINUTES, target_ts=issued_at + step,
                                 y_pred=float(y) if pd.notna(y) else last, model=self.name))
        return make_prediction_frame(rows)
