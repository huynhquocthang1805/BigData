"""Nạp mô hình theo tên built-in hoặc đường dẫn file .joblib (điểm tích hợp với TV3/TV4)."""
from __future__ import annotations

from pathlib import Path

import joblib

from wq.models.base import BaseForecaster, BaseImputer
from wq.models.baselines import (LinearInterpolationImputer, LOCFImputer, PersistenceForecaster,
                                 SeasonalNaiveForecaster)
from wq.models.forecasters_tv4 import RidgeForecaster

BUILTIN_IMPUTERS = {"locf": LOCFImputer, "linear": LinearInterpolationImputer}
BUILTIN_FORECASTERS = {"persistence": PersistenceForecaster, "seasonal_naive": SeasonalNaiveForecaster,
                       "ridge": RidgeForecaster.from_csv}


def _load(spec: str, builtins: dict, base_cls: type):
    if spec in builtins:
        return builtins[spec]()
    path = Path(spec)
    if path.suffix == ".joblib" and path.exists():
        obj = joblib.load(path)
        if not isinstance(obj, base_cls):
            raise TypeError(f"{path} không phải {base_cls.__name__} (nhận được {type(obj).__name__})")
        return obj
    raise ValueError(f"Không tìm thấy mô hình '{spec}'. Dùng một trong {sorted(builtins)} hoặc đường dẫn .joblib")


def load_imputer(spec: str) -> BaseImputer:
    return _load(spec, BUILTIN_IMPUTERS, BaseImputer)


def load_forecaster(spec: str) -> BaseForecaster:
    return _load(spec, BUILTIN_FORECASTERS, BaseForecaster)
