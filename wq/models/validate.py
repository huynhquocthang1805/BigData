"""Kiểm tra mô hình TV3/TV4 bàn giao TRƯỚC khi đưa vào streaming (TV1.5).

    python -m wq.models.validate models/imputer_kalman.joblib
    python -m wq.models.validate models/forecaster_xgb.joblib
    python -m wq.models.validate locf            # tên built-in cũng được

Kiểm tra trên dữ liệu thật (10 trạm chuẩn, split test):
  imputer    : nạp được, đúng lớp, causal=True (bắt buộc cho streaming), giữ nguyên giá trị quan sát,
               cùng shape/cột, không "nhìn trước" tương lai, thời gian chạy / trạm
  forecaster : nạp được, đúng lớp, đúng cột PREDICTION_COLUMNS, đủ horizon của contract,
               target_ts = issued_at + horizon, không NaN, thời gian chạy / trạm
Thoát với mã 1 nếu có lỗi (dùng được trong CI).
"""
from __future__ import annotations

import argparse
import sys
import time

import numpy as np
import pandas as pd

from wq import contracts
from wq.models.base import PREDICTION_COLUMNS, BaseForecaster, BaseImputer
from wq.models.registry import BUILTIN_FORECASTERS, BUILTIN_IMPUTERS, load_forecaster, load_imputer
from wq.timeseries import regularize


class Report:
    def __init__(self):
        self.rows: list[tuple[str, str, str]] = []

    def add(self, ok: bool | None, name: str, detail: str = "") -> None:
        self.rows.append(("PASS" if ok else ("WARN" if ok is None else "FAIL"), name, detail))

    @property
    def failed(self) -> bool:
        return any(r[0] == "FAIL" for r in self.rows)

    def print(self) -> None:
        for status, name, detail in self.rows:
            print(f"  [{status}] {name}" + (f" — {detail}" if detail else ""))


def sample_history(days: int) -> pd.DataFrame:
    from wq.io import read_silver
    lo, hi = contracts.SPLITS["test"]
    end = pd.Timestamp(hi, tz="UTC")
    start = end - pd.Timedelta(days=days)
    df = read_silver(stations="core", start=str(start.date()), end=str(end.date()))
    return regularize(df[contracts.MEASUREMENT_COLUMNS])


def check_imputer(model: BaseImputer, hist: pd.DataFrame, rep: Report) -> None:
    rep.add(isinstance(model, BaseImputer), "là BaseImputer", type(model).__name__)
    causal = getattr(model, "causal", None)
    rep.add(True if causal else False, "causal=True (bắt buộc để chạy online)",
            "" if causal else "mô hình nhìn cả tương lai => chỉ dùng offline")
    t0 = time.perf_counter()
    out = model.transform(hist)
    dt = time.perf_counter() - t0
    n_st = hist["station_id"].nunique()
    rep.add(list(out.columns) == list(hist.columns) and len(out) == len(hist), "giữ nguyên cột và số dòng")
    obs = hist[contracts.CORE_VARIABLES].notna().to_numpy()
    same = np.allclose(out[contracts.CORE_VARIABLES].to_numpy()[obs], hist[contracts.CORE_VARIABLES].to_numpy()[obs],
                       equal_nan=True)
    rep.add(same, "không sửa giá trị đã quan sát")
    before = int(hist[contracts.CORE_VARIABLES].isna().sum().sum())
    after = int(out[contracts.CORE_VARIABLES].isna().sum().sum())
    rep.add(after <= before, "số ô thiếu giảm", f"{before:,} -> {after:,} ô NaN")
    if causal:  # đổi dữ liệu tương lai, phần quá khứ phải giữ nguyên
        cut = hist["ts"].quantile(0.6)
        changed = hist.copy()
        changed.loc[changed["ts"] >= cut, contracts.CORE_VARIABLES] = 999.0
        again = model.transform(changed)
        past = hist["ts"] < cut
        rep.add(np.allclose(again.loc[past, contracts.CORE_VARIABLES].to_numpy(),
                            out.loc[past, contracts.CORE_VARIABLES].to_numpy(), equal_nan=True),
                "thật sự causal (đổi tương lai không ảnh hưởng quá khứ)")
    rep.add(True, "thời gian chạy", f"{dt * 1000 / n_st:.1f} ms/trạm cho {len(hist) // n_st} mốc/trạm")


def check_forecaster(model: BaseForecaster, hist: pd.DataFrame, rep: Report) -> None:
    rep.add(isinstance(model, BaseForecaster), "là BaseForecaster", type(model).__name__)
    from wq.models.baselines import LOCFImputer
    filled = LOCFImputer().transform(hist)
    t0 = time.perf_counter()
    pred = model.predict(filled)
    dt = time.perf_counter() - t0
    n_st = hist["station_id"].nunique()
    rep.add(list(pred.columns) == PREDICTION_COLUMNS, "đúng cột PREDICTION_COLUMNS", str(list(pred.columns)))
    want_h = {h * contracts.FREQ_MINUTES for h in getattr(model, "horizons_steps", contracts.FORECAST_HORIZONS_STEPS)}
    got_h = set(pred["horizon_min"].astype(int)) if len(pred) else set()
    rep.add(want_h <= got_h, "đủ các horizon", f"{sorted(got_h)} phút")
    if len(pred):
        lag = (pd.to_datetime(pred["target_ts"], utc=True) - pd.to_datetime(pred["issued_at"], utc=True))
        rep.add(bool((lag == pd.to_timedelta(pred["horizon_min"], unit="min")).all()), "target_ts = issued_at + horizon")
        rep.add(bool(pred["y_pred"].notna().all()), "không có dự báo NaN")
        last = filled.groupby("station_id")["ts"].max()
        rep.add(bool((pd.to_datetime(pred["issued_at"], utc=True).values ==
                      pred["station_id"].map(last).values).all()), "issued_at = mốc dữ liệu cuối của trạm")
    rep.add(len(pred) > 0, "có kết quả cho các trạm", f"{pred['station_id'].nunique() if len(pred) else 0}/{n_st} trạm")
    rep.add(True, "thời gian chạy", f"{dt * 1000 / n_st:.1f} ms/trạm")


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("spec", help="đường dẫn .joblib hoặc tên built-in")
    ap.add_argument("--kind", choices=["imputer", "forecaster"], help="mặc định: tự đoán")
    ap.add_argument("--days", type=int, default=3, help="số ngày lịch sử dùng để thử (mặc định 3)")
    args = ap.parse_args(argv)

    kind = args.kind
    if kind is None:
        if args.spec in BUILTIN_IMPUTERS or "imput" in args.spec:
            kind = "imputer"
        elif args.spec in BUILTIN_FORECASTERS or "forecast" in args.spec:
            kind = "forecaster"
        else:
            sys.exit("Không đoán được loại mô hình, thêm --kind imputer|forecaster")
    rep = Report()
    try:
        model = load_imputer(args.spec) if kind == "imputer" else load_forecaster(args.spec)
        rep.add(True, "nạp mô hình", f"{type(model).__module__}.{type(model).__name__} (name={getattr(model, 'name', '?')})")
    except Exception as e:  # noqa: BLE001
        rep.add(False, "nạp mô hình", f"{type(e).__name__}: {e}")
        rep.print()
        sys.exit(1)
    hist = sample_history(args.days)
    print(f"Kiểm tra {kind} '{args.spec}' trên {hist['station_id'].nunique()} trạm x {args.days} ngày (split test):")
    (check_imputer if kind == "imputer" else check_forecaster)(model, hist, rep)
    rep.print()
    print("KẾT LUẬN:", "KHÔNG ĐẠT — sửa các mục FAIL trước khi bàn giao" if rep.failed else "ĐẠT — có thể bật trong streaming")
    sys.exit(1 if rep.failed else 0)


if __name__ == "__main__":
    main()
