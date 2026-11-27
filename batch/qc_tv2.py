"""QC nâng cao cho silver (TV2.2a): qualifier của USGS, spike, flatline.

Ba luật chạy sau QC phạm vi vật lý có sẵn (``wq.spark_transforms.apply_range_qc``), theo đúng contract: giá trị bị loại
=> đặt NULL và bật bit của biến trong ``qc_flags``; không thêm cột, không đổi số dòng silver.

1. **Qualifier** (trên bronze, trước khi gộp ô 15 phút): bỏ các lần đo USGS gắn mã trong ``QUALIFIER_REJECT``.
   Danh sách lấy từ mã thực tế trong bronze của 10 trạm chuẩn (xem ``qualifier_stats``). Ô 15 phút chỉ bị đặt NULL
   và gắn cờ khi MỌI lần đo trong ô đều bị loại; còn lần đo tốt thì ô lấy trung bình các lần đo tốt.
2. **Spike** (Hampel): ``|x - median| > max(k * 1.4826 * MAD, sàn)`` với median, MAD tính trên cửa sổ trượt căn giữa
   ``SPIKE_WINDOW`` mốc. Sàn vật lý cần thiết vì giá trị USGS được làm tròn (pH, nhiệt độ tới 0,1) nên MAD thường
   bằng 0: không có sàn, Hampel gắn cờ ~10% dữ liệu. Độ dẫn điện và độ đục lệch phải, biến đổi theo bội số => xét
   trên log10.
3. **Flatline**: một giá trị lặp lại liên tiếp >= ``min_steps`` mốc NGAY SAU một giai đoạn chuỗi dao động rõ
   (độ lệch chuẩn ``prior_steps`` mốc trước đó >= ``min_prior_std``): cảm biến đang đo bình thường rồi đột nhiên
   "đứng". Điều kiện dao động trước đó loại các trường hợp sông lớn đổi chậm hơn độ phân giải của số liệu (DO sông
   Ohio đứng 4 ngày ở 11,8 mg/L khi trước đó chỉ dao động 0,1). Không áp dụng cho pH (độ phân giải 0,1, đứng yên
   nhiều ngày là bình thường), độ đục (hay nằm ở ngưỡng dưới của cảm biến) và lưu lượng (dòng điều tiết).
   Nhiệt độ quanh 0 °C (nước đóng băng) không bị coi là flatline.

Spike và flatline dùng cửa sổ hai phía (biết tương lai) nên chỉ dùng cho batch/huấn luyện; luồng streaming vẫn chỉ
có QC phạm vi (xem ghi chú trong PR).

    python -m batch.qc_tv2             # bảng docs/report/tables/tv2_qc_summary.csv và tv2_qualifier_codes.csv
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from wq import contracts

# ---------------------------------------------------------------------------------------------- tham số
# Mã qualifier (cột ``qualifier`` của collection continuous; một lần đo có thể có nhiều mã, ngăn cách bằng dấu phẩy).
# Ý nghĩa theo bảng "Instantaneous value qualification codes" của USGS NWIS.
QUALIFIER_REJECT: dict[str, str] = {
    "EQUIP": "Thiết bị hỏng (equipment malfunction): giá trị không đáng tin",
    "MAINT": "Trạm đang bảo trì: giá trị đo trong lúc thao tác cảm biến",
    "ICE": "Bị băng ảnh hưởng: số đo sai lệch do băng",
    "ESTIMATED": "Giá trị ước tính, không phải số đo",
    "FORCEINTERPOLATION": "Giá trị do USGS nội suy, không phải số đo",
}
QUALIFIER_KEEP: dict[str, str] = {
    "REVISED": "Đã được USGS sửa sau khi duyệt: vẫn là số đo",
}

SPIKE_WINDOW = 25   # mốc 15 phút, căn giữa => ±3 giờ
SPIKE_K = 6.0


@dataclass(frozen=True)
class SpikeRule:
    floor: float        # độ lệch tối thiểu so với median để bị coi là spike (đơn vị biến, hoặc log10 nếu log=True)
    log: bool = False
    note: str = ""


SPIKE_RULES: dict[str, SpikeRule] = {
    "water_temp": SpikeRule(2.0, note="2 °C lệch khỏi median ±3h; bước 15' lớn nhất thực tế p99,99 = 1,1 °C"),
    "specific_conductance": SpikeRule(0.15, log=True, note="lệch > 41% (0,15 log10) so với median ±3h"),
    "ph": SpikeRule(0.5, note="0,5 đơn vị pH; bước 15' p99,99 = 0,4"),
    "dissolved_oxygen": SpikeRule(2.0, note="2 mg/L; bước 15' p99,9 = 1,8"),
    "turbidity": SpikeRule(0.5, log=True, note="gấp > 3,2 lần (0,5 log10) median ±3h: rác/bọt bám cảm biến"),
    # discharge: không xét spike (lũ lên nhanh là thật, biến phụ trợ)
}


@dataclass(frozen=True)
class FlatlineRule:
    min_steps: int          # số mốc lặp liên tiếp tối thiểu
    prior_steps: int        # cửa sổ trước chuỗi lặp để đo mức dao động
    min_prior_std: float    # độ lệch chuẩn tối thiểu của cửa sổ đó
    ignore_abs_below: float | None = None   # |x| <= ngưỡng này thì không xét (nước đóng băng)


FLATLINE_RULES: dict[str, FlatlineRule] = {
    "water_temp": FlatlineRule(96, 192, 0.5, ignore_abs_below=1.0),
    "specific_conductance": FlatlineRule(96, 192, 20.0),
    "dissolved_oxygen": FlatlineRule(96, 192, 0.5),
}

QC_COLUMNS = ["station_id", "ts", *contracts.VARIABLE_NAMES, "qc_flags"]
SILVER_SCHEMA = ("station_id string, ts timestamp, "
                 + ", ".join(f"{v} double" for v in contracts.VARIABLE_NAMES) + ", qc_flags int")


# ---------------------------------------------------------------------------------------------- qualifier (Spark)
def qualifier_rejected(col: str = "qualifier"):
    """Biểu thức Spark: True nếu lần đo có ít nhất một mã trong QUALIFIER_REJECT."""
    from pyspark.sql import functions as F

    codes = F.array(*[F.lit(c) for c in QUALIFIER_REJECT])
    parts = F.transform(F.split(F.coalesce(F.col(col), F.lit("")), ","), lambda s: F.upper(F.trim(s)))
    return F.arrays_overlap(parts, codes)


def qualifier_stats(bronze):
    """Số lần đo theo mã qualifier x biến trên bronze (để chọn QUALIFIER_REJECT dựa trên dữ liệu thật)."""
    from pyspark.sql import functions as F

    return (bronze.withColumn("code", F.explode(F.split(F.coalesce("qualifier", F.lit("(none)")), ",")))
            .groupBy("code", "variable")
            .agg(F.count("*").alias("n_values"), F.countDistinct("station_id").alias("n_stations"))
            .orderBy("code", "variable"))


# ---------------------------------------------------------------------------------------------- spike / flatline (pandas)
def spike_mask(x: pd.Series, rule: SpikeRule, window: int = SPIKE_WINDOW, k: float = SPIKE_K) -> pd.Series:
    """Hampel có sàn vật lý. ``x`` là một chuỗi đều 15 phút (NaN = thiếu) của MỘT trạm, đã sắp theo thời gian."""
    y = np.log10(x.where(x > 0)) if rule.log else x
    minp = window // 2 + 1
    med = y.rolling(window, center=True, min_periods=minp).median()
    mad = (y - med).abs().rolling(window, center=True, min_periods=minp).median() * 1.4826
    dev = (y - med).abs()
    return (dev > np.maximum(k * mad.fillna(0), rule.floor)) & y.notna() & med.notna()


def flatline_mask(x: pd.Series, rule: FlatlineRule) -> pd.Series:
    """Đánh dấu mọi mốc thuộc chuỗi lặp đủ dài xuất hiện sau giai đoạn có dao động."""
    run_id = (x != x.shift()).cumsum()
    run_len = x.groupby(run_id).transform("size")
    run_start = ~run_id.duplicated()
    prior_std = x.shift(1).rolling(rule.prior_steps, min_periods=rule.prior_steps // 2).std()
    start_ok = run_start & (run_len >= rule.min_steps) & x.notna() & (prior_std >= rule.min_prior_std)
    if rule.ignore_abs_below is not None:
        start_ok &= x.abs() > rule.ignore_abs_below
    flagged_runs = set(run_id[start_ok])
    return run_id.isin(flagged_runs) & x.notna()


def station_masks(pdf: pd.DataFrame) -> dict[tuple[str, str], pd.Series]:
    """{(luật, biến): mask} cho một trạm. Flatline xét trên dữ liệu đã bỏ spike."""
    out: dict[tuple[str, str], pd.Series] = {}
    for v, rule in SPIKE_RULES.items():
        out[("spike", v)] = spike_mask(pdf[v], rule)
    for v, rule in FLATLINE_RULES.items():
        x = pdf[v].mask(out.get(("spike", v), False))
        out[("flatline", v)] = flatline_mask(x, rule)
    return out


def qc_station(pdf: pd.DataFrame) -> pd.DataFrame:
    """Áp spike + flatline lên silver của MỘT trạm (dùng trong applyInPandas)."""
    pdf = pdf.sort_values("ts").reset_index(drop=True)
    flags = pdf["qc_flags"].fillna(0).astype("int64")
    for (_, v), m in station_masks(pdf).items():
        if m.any():
            pdf.loc[m, v] = np.nan
            flags = flags.where(~m, flags | contracts.VARIABLE_BIT[v])
    pdf["qc_flags"] = flags.astype("int32")
    return pdf[QC_COLUMNS]


def station_counts(pdf: pd.DataFrame) -> pd.DataFrame:
    """Số giá trị bị từng luật loại (cho bảng tóm tắt); cùng thứ tự áp luật như qc_station."""
    pdf = pdf.sort_values("ts").reset_index(drop=True)
    rows = [{"station_id": pdf["station_id"].iloc[0], "rule": r, "variable": v, "n_rejected": int(m.sum())}
            for (r, v), m in station_masks(pdf).items()]
    return pd.DataFrame(rows, columns=["station_id", "rule", "variable", "n_rejected"])


# ---------------------------------------------------------------------------------------------- Spark
def apply_advanced_qc(silver):
    """Spike + flatline trên silver (Spark): mỗi trạm một nhóm, chia cho các executor."""
    return silver.select(*QC_COLUMNS).groupBy("station_id").applyInPandas(qc_station, schema=SILVER_SCHEMA)


def qc_summary(bronze, silver_range_only, silver_before_advanced, start: str, end: str) -> pd.DataFrame:
    """Bảng số giá trị bị loại theo luật x biến: qualifier, range (phạm vi vật lý), spike, flatline.

    - qualifier: đếm trên từng lần đo thô của bronze;
    - range: đếm trên silver chỉ có QC phạm vi (``silver_range_only``) để không lẫn cờ qualifier;
    - spike, flatline: đếm trên silver đã qua qualifier + range (``silver_before_advanced``).
    rate = n_rejected / n_checked (số giá trị có trước bước đó).
    """
    from pyspark.sql import functions as F

    lo, hi = F.to_timestamp(F.lit(start)), F.to_timestamp(F.lit(end))
    b = bronze.where((F.col("ts") >= lo) & (F.col("ts") < hi) & F.col("value").isNotNull())
    qual = (b.groupBy("variable").agg(F.count("*").alias("n_checked"),
                                      F.sum(qualifier_rejected().cast("long")).alias("n_rejected"))
            .withColumn("rule", F.lit("qualifier")).toPandas())

    rng = []
    for v in contracts.VARIABLE_NAMES:
        bit = contracts.VARIABLE_BIT[v]
        r = silver_range_only.agg(F.sum((F.col("qc_flags").bitwiseAND(bit) != 0).cast("long")).alias("n_rejected"),
                  F.count(v).alias("n_after")).first()
        rng.append({"variable": v, "rule": "range", "n_rejected": int(r["n_rejected"] or 0),
                    "n_checked": int(r["n_after"]) + int(r["n_rejected"] or 0)})
    s = silver_before_advanced
    counts = (s.select(*QC_COLUMNS).groupBy("station_id")
              .applyInPandas(station_counts, "station_id string, rule string, variable string, n_rejected long")
              .groupBy("rule", "variable").agg(F.sum("n_rejected").alias("n_rejected")).toPandas())
    n_obs = {v: int(s.agg(F.count(v)).first()[0]) for v in contracts.VARIABLE_NAMES}
    counts["n_checked"] = counts["variable"].map(n_obs)
    out = pd.concat([qual, pd.DataFrame(rng), counts], ignore_index=True)
    out["rate"] = (out["n_rejected"] / out["n_checked"]).round(6)
    out["level"] = out["rule"].map(lambda r: "lần đo thô (bronze)" if r == "qualifier" else "ô 15 phút (silver)")
    order = {r: i for i, r in enumerate(["qualifier", "range", "spike", "flatline"])}
    out = out.assign(_r=out["rule"].map(order), _v=out["variable"].map(contracts.VARIABLE_NAMES.index))
    return out.sort_values(["_r", "_v"])[["rule", "variable", "level", "n_checked", "n_rejected", "rate"]].reset_index(drop=True)


def main(argv: list[str] | None = None) -> None:
    from batch.etl_usgs import to_silver
    from wq.config import get_settings
    from wq.spark_utils import build_spark

    s = get_settings()
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--lake", default=s.lake_path)
    ap.add_argument("--tables-dir", type=Path, default=Path("docs/report/tables"))
    ap.add_argument("--master", default=None)
    args = ap.parse_args(argv)

    spark = build_spark("wq-qc-tv2", master=args.master)
    bronze = spark.read.parquet(f"{args.lake}/bronze/usgs_long").cache()
    args.tables_dir.mkdir(parents=True, exist_ok=True)

    codes = qualifier_stats(bronze).toPandas()
    codes["decision"] = codes["code"].map(lambda c: "loại" if c in QUALIFIER_REJECT else "giữ")
    codes["reason"] = codes["code"].map(lambda c: QUALIFIER_REJECT.get(c) or QUALIFIER_KEEP.get(c)
                                        or ("Không có mã" if c == "(none)" else "Không thuộc danh sách loại"))
    codes.to_csv(args.tables_dir / "tv2_qualifier_codes.csv", index=False)

    # silver sau qualifier + range nhưng TRƯỚC spike/flatline, để đếm riêng từng luật
    range_only = to_silver(bronze, contracts.DATA_START, contracts.DATA_END, qualifier_qc=False, advanced_qc=False)
    base = to_silver(bronze, contracts.DATA_START, contracts.DATA_END, advanced_qc=False).cache()
    summary = qc_summary(bronze, range_only, base, contracts.DATA_START, contracts.DATA_END)
    summary.to_csv(args.tables_dir / "tv2_qc_summary.csv", index=False)
    print(codes.to_string(index=False))
    print()
    print(summary.to_string(index=False))
    spark.stop()


if __name__ == "__main__":
    main()
