"""Luật cảnh báo tinh chỉnh (TV2.2c) và bảng so sánh trước / sau trên split test, tính bằng Spark.

Vấn đề của bộ luật cũ (trong lượt demo chế độ Big Data: 11.417 cảnh báo cho ~26.666 bản ghi):
1. Ba ngưỡng tuyệt đối không có tiêu chí số chung toàn quốc: độ dẫn điện > 2000 µS/cm luôn đúng ở trạm nước lợ
   (Tidal stream, Estuary), nhiệt độ > 30 °C là bình thường của mùa hè miền Nam, độ đục > 300 FNU tùy thủy vực.
2. Mỗi bản ghi 15 phút vi phạm sinh một cảnh báo: DO thấp suốt một đêm = 30–40 cảnh báo cho cùng một sự kiện.

Bộ luật mới:
- ``contracts.ALERT_RULES`` (dùng trong streaming, theo từng bản ghi): chỉ giữ các ngưỡng có tiêu chí số được trích
  dẫn: DO_LOW, DO_HYPOXIA, PH_LOW, PH_HIGH (nguồn trong ``wq.wqi.SOURCES``).
- ``BASELINE_RULES``: nhiệt độ, độ dẫn điện, độ đục vượt **p99 của chính trạm trên split train** (``wq.wqi``,
  bảng gold ``site_baselines``). Theo cách CCME (2002) định nghĩa hướng dẫn độ đục: so với mức nền của thủy vực.
  Trạm nước lợ có mức nền độ dẫn điện cao nên không còn cảnh báo thường trực.
- **Theo sự kiện**: các mốc vi phạm liên tiếp (cùng trạm, cùng luật) gộp thành một sự kiện; chỉ cảnh báo khi kéo dài
  >= ``MIN_STEPS`` mốc (1 giờ), trừ luật mức ``critical`` (cảnh báo ngay từ mốc đầu tiên).

Streaming hiện chỉ làm được phần ngưỡng tuyệt đối theo từng bản ghi; phần mức nền + sự kiện cần state, đặc tả cho TV1
ghi trong PR.

    python -m batch.alerts_tv2                       # 10 trạm chuẩn
    python -m batch.alerts_tv2 --lake data/lake_nonstream --dataset nonstream34   # bộ trạm phụ (2024 + test)
"""
from __future__ import annotations

import argparse
from functools import reduce
from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from wq import contracts
from wq.config import get_settings
from wq.contracts import AlertRule
from wq.spark_utils import build_spark
from wq.spark_transforms import alert_events

MIN_STEPS = 4   # 1 giờ
STEP_S = contracts.FREQ_MINUTES * 60

# Bộ luật trước TV2.2c (giữ để tính cột "trước")
LEGACY_ALERT_RULES: tuple[AlertRule, ...] = (
    AlertRule("dissolved_oxygen", "<", 5.0, "warning", "DO_LOW", "DO < 5 mg/L"),
    AlertRule("dissolved_oxygen", "<", 2.0, "critical", "DO_HYPOXIA", "DO < 2 mg/L"),
    AlertRule("ph", "<", 6.5, "warning", "PH_LOW", "pH < 6.5"),
    AlertRule("ph", ">", 9.0, "warning", "PH_HIGH", "pH > 9.0"),
    AlertRule("water_temp", ">", 30.0, "warning", "TEMP_HIGH", "Nhiệt độ nước > 30 °C"),
    AlertRule("turbidity", ">", 300.0, "warning", "TURB_HIGH", "Độ đục > 300 FNU"),
    AlertRule("specific_conductance", ">", 2_000.0, "warning", "SC_HIGH", "Độ dẫn điện > 2000 uS/cm"),
)

# Luật theo mức nền của trạm: (biến, mã luật, mức, thông điệp)
BASELINE_RULES: tuple[tuple[str, str, str, str], ...] = (
    ("water_temp", "TEMP_ANOM", "warning", "Nhiệt độ nước vượt p99 lịch sử của trạm"),
    ("specific_conductance", "SC_ANOM", "warning", "Độ dẫn điện vượt p99 lịch sử của trạm"),
    ("turbidity", "TURB_ANOM", "warning", "Độ đục vượt p99 lịch sử của trạm"),
)


def violations(df: DataFrame, rules: tuple[AlertRule, ...]) -> DataFrame:
    """Mỗi bản ghi vi phạm một luật tuyệt đối => 1 dòng (cùng logic ``alert_events`` nhưng nhận bộ luật bất kỳ)."""
    parts = []
    for r in rules:
        cond = F.col(r.parameter) < r.threshold if r.op == "<" else F.col(r.parameter) > r.threshold
        parts.append(df.where(cond).select(
            "station_id", "ts", F.lit(r.parameter).alias("parameter"), F.col(r.parameter).alias("value"),
            F.lit(float(r.threshold)).alias("threshold"), F.lit(r.rule).alias("rule"),
            F.lit(r.severity).alias("severity")))
    return reduce(DataFrame.unionByName, parts)


def baseline_violations(df: DataFrame, baselines: DataFrame) -> DataFrame:
    """Bản ghi vượt p99 train của chính trạm (``baselines``: station_id, variable, p99)."""
    parts = []
    for var, rule, sev, _ in BASELINE_RULES:
        b = baselines.where(F.col("variable") == var).select("station_id", F.col("p99").alias("threshold"))
        parts.append(df.join(b, "station_id").where(F.col(var) > F.col("threshold")).select(
            "station_id", "ts", F.lit(var).alias("parameter"), F.col(var).alias("value"), "threshold",
            F.lit(rule).alias("rule"), F.lit(sev).alias("severity")))
    return reduce(DataFrame.unionByName, parts)


def episodes(v: DataFrame, min_steps: int = MIN_STEPS) -> DataFrame:
    """Gộp các mốc vi phạm liên tiếp (lưới 15 phút) của cùng trạm + luật thành sự kiện.

    Giữ sự kiện dài >= min_steps mốc, riêng mức critical giữ mọi sự kiện. Mỗi sự kiện: thời điểm bắt đầu, kết thúc,
    số mốc, giá trị xa ngưỡng nhất.
    """
    idx = (F.unix_timestamp("ts") / STEP_S).cast("long")
    w = Window.partitionBy("station_id", "rule").orderBy("idx")
    ep = (v.withColumn("idx", idx).withColumn("grp", F.col("idx") - F.row_number().over(w))
          .groupBy("station_id", "rule", "severity", "parameter", "grp")
          .agg(F.min("ts").alias("start_ts"), F.max("ts").alias("end_ts"), F.count("*").alias("n_steps"),
               F.min("value").alias("vmin"), F.max("value").alias("vmax"), F.first("threshold").alias("threshold")))
    ep = ep.withColumn("peak_value", F.when(F.col("vmin") < F.col("threshold"), F.col("vmin")).otherwise(F.col("vmax")))
    return (ep.where((F.col("n_steps") >= min_steps) | (F.col("severity") == "critical"))
            .select("station_id", "rule", "severity", "parameter", "start_ts", "end_ts", "n_steps", "peak_value",
                    "threshold"))


def test_split(silver: DataFrame) -> DataFrame:
    lo, hi = contracts.SPLITS["test"]
    return silver.where((F.col("ts") >= F.to_timestamp(F.lit(lo))) & (F.col("ts") < F.to_timestamp(F.lit(hi))))


def _count(df: DataFrame, prefix: str) -> pd.DataFrame:
    return (df.groupBy("rule").agg(F.count("*").alias(f"{prefix}_alerts"),
                                   F.countDistinct("station_id").alias(f"{prefix}_stations")).toPandas())


def before_after(silver: DataFrame, baselines: DataFrame, dataset: str) -> pd.DataFrame:
    """Số cảnh báo theo luật trên split test:
    - before: bộ luật cũ, mỗi bản ghi vi phạm 1 cảnh báo (như streaming trước TV2.2c);
    - rules_now: ``contracts.ALERT_RULES`` mới qua ``wq.spark_transforms.alert_events`` (streaming sau PR này);
    - after: đề xuất đầy đủ = luật tuyệt đối + luật mức nền, theo sự kiện >= 1 giờ.
    """
    test = test_split(silver).cache()
    n_records = test.where(F.greatest(*[F.col(v).isNotNull() for v in contracts.VARIABLE_NAMES])).count()
    before = _count(violations(test, LEGACY_ALERT_RULES), "before")
    now = _count(alert_events(test).select("station_id", "rule"), "rules_now")
    viol = violations(test, contracts.ALERT_RULES).unionByName(baseline_violations(test, baselines))
    after = _count(episodes(viol), "after")
    out = before.merge(now, on="rule", how="outer").merge(after, on="rule", how="outer")
    rules = [r.rule for r in LEGACY_ALERT_RULES] + [r for _, r, _, _ in BASELINE_RULES]
    out = out.set_index("rule").reindex(rules).fillna(0).astype(int).reset_index()
    out.insert(0, "dataset", dataset)
    out.insert(1, "n_records_test", n_records)
    total = out.drop(columns=["dataset", "rule", "n_records_test"]).sum()
    total[[c for c in total.index if c.endswith("_stations")]] = test.select("station_id").distinct().count()
    out.loc[len(out)] = {"dataset": dataset, "rule": "TOTAL", "n_records_test": n_records, **total.to_dict()}
    test.unpersist()
    return out


def main(argv: list[str] | None = None) -> None:
    s = get_settings()
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--lake", default=s.lake_path)
    ap.add_argument("--dataset", default="core10")
    ap.add_argument("--out", type=Path, default=Path("docs/report/tables/tv2_alerts_before_after.csv"))
    ap.add_argument("--master", default=None)
    args = ap.parse_args(argv)

    from wq.wqi import site_baselines

    spark = build_spark("wq-alerts-tv2", master=args.master)
    silver = spark.read.parquet(f"{args.lake}/silver/measurements")
    baselines = site_baselines(silver)   # split train 2023–2024 (bộ trạm phụ chỉ tải 2024, vẫn nằm trong train)
    table = before_after(silver, baselines, args.dataset)
    if args.out.exists():   # nối các bộ dữ liệu vào cùng một file, thay dòng cũ của cùng dataset
        old = pd.read_csv(args.out)
        table = pd.concat([old[old["dataset"] != args.dataset], table], ignore_index=True)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    table.to_csv(args.out, index=False)
    print(table.to_string(index=False))
    spark.stop()


if __name__ == "__main__":
    main()
