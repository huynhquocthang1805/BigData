"""Nội suy tuyến tính, KNN và Iterative Imputer cho thực nghiệm TV3."""
from __future__ import annotations

from abc import abstractmethod
from collections.abc import Sequence

import numpy as np
import pandas as pd
from sklearn.exceptions import NotFittedError
from sklearn.experimental import enable_iterative_imputer  # noqa: F401
from sklearn.impute import IterativeImputer as SklearnIterativeImputer
from sklearn.impute import KNNImputer as SklearnKNNImputer
from sklearn.preprocessing import StandardScaler

from wq.contracts import CORE_VARIABLES
from wq.models.base import BaseImputer
from wq.models.baselines import LinearInterpolationImputer as BaselineLinearInterpolation


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
