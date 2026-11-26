"""So sánh ba imputer TV3 trên các mask dùng chung từ giao thức TV3.1."""
from __future__ import annotations

import argparse
from pathlib import Path
from time import perf_counter

import pandas as pd

from wq.contracts import CORE_VARIABLES, SPLITS
from wq.evaluation import apply_mask, make_mask, score_imputation
from wq.io import read_silver
from wq.models.imputers_tv3 import IterativeImputer, KNNImputer, LinearInterpolationImputer


def run_benchmark(train: pd.DataFrame, truth: pd.DataFrame, seeds: list[int]) -> pd.DataFrame:
    if train.empty or truth.empty or train["ts"].max() >= truth["ts"].min():
        raise ValueError("Cần dữ liệu train và đánh giá ở hai khoảng thời gian tách biệt.")

    models = (LinearInterpolationImputer(), KNNImputer(), IterativeImputer())
    fit_times = {}
    for model in models:
        started = perf_counter()
        model.fit(train)
        fit_times[model.name] = perf_counter() - started

    results = []
    for seed in seeds:
        for kind in ("random", "block"):
            for rate in (0.05, 0.10, 0.20, 0.30):
                mask = make_mask(truth, rate, kind, seed=seed)
                masked = apply_mask(truth, mask)
                for model in models:
                    started = perf_counter()
                    filled = model.transform(masked)
                    elapsed = perf_counter() - started
                    scores = score_imputation(truth, filled, mask, CORE_VARIABLES)
                    results.append(scores.assign(
                        method=model.name, causal=model.causal, missing_type=kind, missing_rate=rate,
                        seed=seed, fit_s=fit_times[model.name], transform_s=elapsed,
                    ))
    columns = [
        "method", "causal", "missing_type", "missing_rate", "variable", "seed", "n_masked", "n_scored",
        "mae", "rmse", "r2", "fit_s", "transform_s",
    ]
    return pd.concat(results, ignore_index=True)[columns]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--train-lake", help="Data lake chứa split train. Mặc định đọc WQ_LAKE_PATH.")
    parser.add_argument("--eval-lake", help="Data lake chứa split đánh giá. Mặc định đọc WQ_LAKE_PATH.")
    parser.add_argument("--split", choices=("valid", "test"), default="test")
    parser.add_argument("--seeds", type=int, nargs="+", default=[42, 43, 44])
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()

    train = read_silver("core", *SPLITS["train"], lake=args.train_lake)
    truth = read_silver("core", *SPLITS[args.split], lake=args.eval_lake)
    scores = run_benchmark(train, truth, args.seeds)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    scores.to_csv(args.out, index=False)
    print(f"{len(train):,} dòng train, {len(truth):,} dòng {args.split}, {len(scores):,} kết quả → {args.out}")


if __name__ == "__main__":
    main()
