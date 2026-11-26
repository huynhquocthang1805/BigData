"""Các mô hình điền khuyết và lệnh xuất imputer causal của TV3."""
from __future__ import annotations

import argparse
from abc import abstractmethod
from collections.abc import Sequence

import numpy as np
import pandas as pd
from sklearn.exceptions import NotFittedError
from sklearn.experimental import enable_iterative_imputer  # noqa: F401
from sklearn.impute import IterativeImputer as SklearnIterativeImputer
from sklearn.impute import KNNImputer as SklearnKNNImputer
from sklearn.preprocessing import StandardScaler

from wq.contracts import CORE_VARIABLES, FREQ, SPLITS
from wq.models.base import BaseImputer
from wq.models.baselines import LOCFImputer, LinearInterpolationImputer as BaselineLinearInterpolation


class LinearInterpolationImputer(BaselineLinearInterpolation):
    """Dùng baseline nội suy theo thời gian của TV1 trên năm biến chính."""

    variables = CORE_VARIABLES


class _StationwiseImputer(BaseImputer):
    """Fit theo trạm trên train, điền từ biến quan sát cùng mốc và model đã fit."""

    causal = True

    def __init__(self, variables: Sequence[str]):
        self.variables = list(variables)

    def _reference_rows(self, train: pd.DataFrame) -> pd.DataFrame:
        return train

    @abstractmethod
    def _estimator(self) -> SklearnKNNImputer | SklearnIterativeImputer: ...

    def fit(self, df: pd.DataFrame) -> _StationwiseImputer:
        models = {}
        for station, frame in df.groupby("station_id", sort=False):
            train = frame.reindex(columns=self.variables).dropna(how="all")
            if train.empty:
                continue
            reference = self._reference_rows(train)
            columns = reference.columns[reference.notna().any()].tolist()
            scaler = StandardScaler().fit(train[columns])
            estimator = self._estimator().fit(scaler.transform(reference[columns]))
            models[station] = columns, scaler, estimator
        self.models_ = models
        return self

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if not hasattr(self, "models_"):
            raise NotFittedError(f"Hãy fit {self.name} trên dữ liệu train trước khi transform.")
        out = df.copy()
        for station, positions in df.groupby("station_id", sort=False).indices.items():
            if station not in self.models_:
                continue
            columns, scaler, estimator = self.models_[station]
            values = df.iloc[positions].reindex(columns=columns)
            present = values.notna().to_numpy()
            # Không có biến đo nào để suy ra giá trị tại mốc này.
            rows = ~present.all(axis=1) & present.any(axis=1)
            if not rows.any():
                continue
            selected = values.iloc[rows]
            filled = scaler.inverse_transform(estimator.transform(scaler.transform(selected)))
            unchanged = selected.to_numpy(dtype=float, na_value=np.nan)
            result = np.where(present[rows], unchanged, filled)
            trained_columns = pd.Index(columns)
            existing = trained_columns.isin(df.columns)
            destination = df.columns.get_indexer(trained_columns[existing])
            out.iloc[positions[rows], destination] = result[:, existing]
        return out


class KNNImputer(_StationwiseImputer):
    """Điền từ K hàng train gần nhất theo khoảng cách trên các biến cùng quan sát."""

    name = "knn"

    def __init__(
        self,
        n_neighbors: int = 5,
        max_train_rows: int | None = 1024,
        seed: int = 42,
        variables: Sequence[str] = CORE_VARIABLES,
    ):
        super().__init__(variables)
        if max_train_rows is not None and max_train_rows < len(self.variables):
            raise ValueError("max_train_rows phải đủ chỗ cho ít nhất một mẫu mỗi biến")
        self.n_neighbors = n_neighbors
        self.max_train_rows = max_train_rows
        self.seed = seed

    def _reference_rows(self, train: pd.DataFrame) -> pd.DataFrame:
        if self.max_train_rows is None or len(train) <= self.max_train_rows:
            return train
        random = np.random.default_rng(self.seed)
        # Giữ ít nhất một hàng có quan sát cho từng biến hiếm trước khi lấy mẫu phần còn lại.
        observed = train.notna().to_numpy()
        anchors = np.unique([random.choice(np.flatnonzero(col)) for col in observed.T if col.any()])
        pool = np.setdiff1d(np.arange(len(train)), anchors, assume_unique=True)
        extra = random.choice(pool, size=self.max_train_rows - len(anchors), replace=False)
        return train.iloc[np.sort(np.r_[anchors, extra])]

    def _estimator(self) -> SklearnKNNImputer:
        return SklearnKNNImputer(n_neighbors=self.n_neighbors, weights="distance")


class IterativeImputer(_StationwiseImputer):
    """Luân phiên dự đoán từng biến từ các biến còn lại bằng Bayesian Ridge."""

    name = "iterative"

    def __init__(self, max_iter: int = 10, variables: Sequence[str] = CORE_VARIABLES):
        super().__init__(variables)
        self.max_iter = max_iter

    def _estimator(self) -> SklearnIterativeImputer:
        return SklearnIterativeImputer(max_iter=self.max_iter)


class KalmanImputer(BaseImputer):
    """Bộ lọc Kalman AR(1) độc lập theo biến và theo trạm trên lưới 15 phút."""

    name = "kalman"
    variables = CORE_VARIABLES

    def fit(self, df: pd.DataFrame) -> KalmanImputer:
        self.parameters_ = {}
        for station, frame in df.sort_values("ts").groupby("station_id", sort=False):
            values = frame.reindex(columns=self.variables)
            centers = values.mean().to_numpy()
            centered = values.to_numpy(dtype=float) - centers
            left, right = centered[:-1], centered[1:]
            adjacent = frame["ts"].diff().eq(pd.Timedelta(FREQ)).to_numpy()[1:]
            valid = adjacent[:, None] & np.isfinite(left) & np.isfinite(right)
            left, right = np.where(valid, left, 0), np.where(valid, right, 0)
            energy = np.square(left).sum(axis=0)
            persistence = np.divide(
                (left * right).sum(axis=0), energy, out=np.ones_like(energy), where=energy > 0
            ).clip(0, 1)
            variance = np.square(right - persistence * left).sum(axis=0) / valid.sum(axis=0).clip(1)
            # Xấp xỉ phương sai nhiễu bằng sai số một bước trên train.
            variance = np.maximum(variance, 1e-9)
            self.parameters_[station] = centers, persistence, 0.9 * variance, 0.1 * variance
        return self

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if not hasattr(self, "parameters_"):
            raise NotFittedError("Hãy fit KalmanImputer trên dữ liệu train trước.")
        out = df.copy()
        columns = df.columns.get_indexer(self.variables)
        existing = columns >= 0
        for station, positions in df.groupby("station_id", sort=False).indices.items():
            if station not in self.parameters_:
                continue
            order = positions[np.argsort(df["ts"].iloc[positions].to_numpy(), kind="stable")]
            values = df.iloc[order].reindex(columns=self.variables).to_numpy(dtype=float, copy=True)
            center, persistence, process_noise, observation_noise = self.parameters_[station]
            state = np.full(len(self.variables), np.nan)
            uncertainty = observation_noise.copy()
            persistence_squared = persistence**2
            for row in values:
                prior = center + persistence * (state - center)
                variance = persistence_squared * uncertainty + process_noise
                observed = np.isfinite(row)
                initialized = np.isfinite(prior)
                gain = variance / (variance + observation_noise)
                state = np.where(observed, np.where(initialized, prior + gain * (row - prior), row), prior)
                updated_variance = np.where(initialized, (1 - gain) * variance, observation_noise)
                uncertainty = np.where(observed, updated_variance, variance)
                row[~observed] = prior[~observed]
            out.iloc[order, columns[existing]] = values[:, existing]
        return out


def main() -> None:
    from wq.io import read_silver
    # Dùng tên module chuẩn để nạp được joblib trong tiến trình khác.
    from wq.models import imputers_tv3

    models = {
        "locf": LOCFImputer,
        "knn": imputers_tv3.KNNImputer,
        "iterative": imputers_tv3.IterativeImputer,
        "kalman": imputers_tv3.KalmanImputer,
    }
    parser = argparse.ArgumentParser(description="Fit và xuất imputer causal trên split train.")
    parser.add_argument("--method", choices=models, required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--lake", help="Data lake train. Mặc định đọc WQ_LAKE_PATH.")
    args = parser.parse_args()
    train = read_silver("core", *SPLITS["train"], lake=args.lake)
    if train.empty:
        parser.error("Không đọc được dòng train từ silver.")
    print(models[args.method]().fit(train).save(args.out))


if __name__ == "__main__":
    main()
