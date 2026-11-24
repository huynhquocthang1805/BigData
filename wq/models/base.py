"""Interface (hợp đồng) giữa pipeline và mô hình của TV3 (imputation) / TV4 (prediction).

Pipeline streaming và API chỉ gọi các phương thức dưới đây, nên TV3/TV4 có thể thay mô hình
bất kỳ (KNN, MICE, Kalman, DBN, XGBoost, LSTM...) mà KHÔNG phải sửa code tích hợp:

    class MyImputer(BaseImputer): ...      -> lưu bằng .save("models/imputer_xxx.joblib")
    WQ_IMPUTER=models/imputer_xxx.joblib   -> stream processor tự nạp

Lưu ý: class phải nằm trong package `wq.models` (ví dụ wq/models/imputers_tv3.py) để file
joblib nạp lại được trong container.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path

import joblib
import pandas as pd

from wq import contracts

PREDICTION_COLUMNS = ["station_id", "target", "issued_at", "horizon_min", "target_ts", "y_pred", "model"]


class BaseImputer(ABC):
    """Điền giá trị thiếu.

    Input  : DataFrame chuẩn (station_id, ts, các biến), đã ở lưới đều 15 phút (wq.timeseries.regularize),
             có thể chứa nhiều trạm.
    Output : DataFrame cùng shape/cột/thứ tự dòng, NaN đã được điền nếu có thể.
    """

    name: str = "base"
    #: True nếu chỉ dùng dữ liệu QUÁ KHỨ (filtering) -> được phép chạy online trong streaming.
    #: False nếu dùng cả tương lai (smoothing, nội suy 2 phía) -> chỉ dùng offline/batch.
    causal: bool = True
    variables: list[str] = contracts.VARIABLE_NAMES

    def fit(self, df: pd.DataFrame) -> "BaseImputer":
        return self

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame: ...

    def fit_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.fit(df).transform(df)

    def save(self, path: str | Path) -> Path:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(self, path)
        return path


class BaseForecaster(ABC):
    """Dự báo các bước tương lai.

    Input  : lịch sử ĐÃ IMPUTE (DataFrame chuẩn, lưới 15 phút), 1 hoặc nhiều trạm.
    Output : DataFrame với cột PREDICTION_COLUMNS, mỗi dòng = (trạm, target, horizon).
             issued_at = mốc thời gian cuối cùng của lịch sử; target_ts = issued_at + horizon.
    """

    name: str = "base"
    targets: list[str] = contracts.FORECAST_TARGETS
    horizons_steps: list[int] = contracts.FORECAST_HORIZONS_STEPS
    min_history_steps: int = contracts.STEPS_PER_DAY  # cần tối thiểu bao nhiêu bước lịch sử

    def fit(self, df: pd.DataFrame) -> "BaseForecaster":
        return self

    @abstractmethod
    def predict(self, history: pd.DataFrame) -> pd.DataFrame: ...

    def save(self, path: str | Path) -> Path:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(self, path)
        return path


def make_prediction_frame(rows: list[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame({c: pd.Series(dtype="object") for c in PREDICTION_COLUMNS})
    out = pd.DataFrame(rows, columns=PREDICTION_COLUMNS)
    out["issued_at"] = pd.to_datetime(out["issued_at"], utc=True)
    out["target_ts"] = pd.to_datetime(out["target_ts"], utc=True)
    out["horizon_min"] = out["horizon_min"].astype("int64")
    out["y_pred"] = out["y_pred"].astype("float64")
    return out
