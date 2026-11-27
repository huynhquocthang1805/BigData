"""Chỉ số chất lượng nước CCME WQI (TV2.2c) và ngưỡng nền theo trạm.

Vì sao CCME WQI: các chỉ số phổ biến khác (NSF WQI, Brown et al. 1970) cần BOD, coliform, nitrat, phosphat... mà
cảm biến liên tục của USGS không đo. CCME WQI (CCME, 2001) được thiết kế để dùng với **bất kỳ tập biến nào** (tối thiểu
4 biến, 4 lần đo) và **ngưỡng mục tiêu (objective) do người dùng chọn**, kể cả ngưỡng riêng theo trạm. Công thức:

    F1 (scope)     = 100 * số biến có vi phạm / số biến
    F2 (frequency) = 100 * số lần đo vi phạm / số lần đo
    excursion      = giá trị / ngưỡng - 1          (ngưỡng trên, "không được vượt")
                   = ngưỡng / giá trị - 1          (ngưỡng dưới, "không được thấp hơn")
    nse            = tổng excursion / số lần đo
    F3 (amplitude) = nse / (0.01 * nse + 0.01)
    WQI            = 100 - sqrt(F1^2 + F2^2 + F3^2) / 1.732

Phân loại (CCME, 2001): Excellent 95–100, Good 80–94, Fair 65–79, Marginal 45–64, Poor 0–44.

Ngưỡng mục tiêu:
- **Ngưỡng quốc gia** (có tiêu chí số của U.S. EPA): DO >= 5,0 mg/L; pH trong [6,5; 9,0].
- **Ngưỡng nền theo trạm** cho nhiệt độ, độ dẫn điện, độ đục: phân vị 99 của chính trạm đó trên split **train**.
  Ba biến này không có tiêu chí số toàn quốc: độ dẫn điện tự nhiên khác nhau hàng trăm lần giữa sông nội địa và
  cửa sông nước lợ; hướng dẫn độ đục của CCME (2002) được định nghĩa theo mức tăng so với nền (background) của
  chính thủy vực. Chỉ dùng train nên không rò rỉ dữ liệu valid/test.

File này không import pyspark ở mức module để notebook dùng được hàm pandas.
"""
from __future__ import annotations

import argparse
import math
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from wq import contracts

SOURCES: dict[str, str] = {
    "CCME2001": "CCME (2001). Canadian Water Quality Guidelines for the Protection of Aquatic Life: CCME Water "
                "Quality Index 1.0, Technical Report. Canadian Council of Ministers of the Environment, Winnipeg.",
    "CCME2002": "CCME (2002). Canadian Water Quality Guidelines for the Protection of Aquatic Life: Total "
                "Particulate Matter. Canadian Council of Ministers of the Environment, Winnipeg.",
    "EPA1986": "U.S. EPA (1986). Quality Criteria for Water 1986 (\"Gold Book\"). EPA 440/5-86-001, Washington, DC.",
    "EPA1986DO": "U.S. EPA (1986). Ambient Water Quality Criteria for Dissolved Oxygen. EPA 440/5-86-003, "
                 "Washington, DC.",
    "Rabalais2002": "Rabalais, N.N., Turner, R.E., Wiseman, W.J. Jr. (2002). Gulf of Mexico hypoxia, a.k.a. "
                    "\"The dead zone\". Annual Review of Ecology and Systematics 33: 235–263.",
}

WQI_CLASSES: list[tuple[float, str]] = [(95, "Excellent"), (80, "Good"), (65, "Fair"), (45, "Marginal"), (0, "Poor")]
MIN_VARIABLES = 4          # CCME (2001): tối thiểu 4 biến
MIN_TESTS_PER_VARIABLE = 4  # và 4 lần đo mỗi biến trong cửa sổ tính
SITE_QUANTILE = 0.99
SITE_VARIABLES = ["water_temp", "specific_conductance", "turbidity"]
WQI_VARIABLES = contracts.CORE_VARIABLES
_EPS = 0.01                 # tránh chia cho 0 khi DO = 0


@dataclass(frozen=True)
class Objective:
    variable: str
    lo: float | None = None      # không được thấp hơn
    hi: float | None = None      # không được vượt
    source: str = ""


NATIONAL_OBJECTIVES: tuple[Objective, ...] = (
    Objective("dissolved_oxygen", lo=5.0, source="EPA1986DO: tối thiểu 1 ngày, nước ấm, giai đoạn sớm của sinh vật"),
    Objective("ph", lo=6.5, hi=9.0, source="EPA1986: tiêu chí pH cho đời sống thủy sinh nước ngọt"),
)


def wqi_class(wqi: float | None) -> str | None:
    if wqi is None or (isinstance(wqi, float) and math.isnan(wqi)):
        return None
    for lo, name in WQI_CLASSES:
        if wqi >= lo:
            return name
    return "Poor"


def objectives_for_station(baselines: pd.DataFrame | None, station_id: str) -> list[Objective]:
    """Ngưỡng quốc gia + ngưỡng nền (p99 train) của trạm; ``baselines`` có cột station_id, variable, p99."""
    objs = list(NATIONAL_OBJECTIVES)
    if baselines is not None:
        b = baselines[baselines["station_id"] == station_id]
        for _, r in b.iterrows():
            if r["variable"] in SITE_VARIABLES and pd.notna(r["p99"]):
                objs.append(Objective(r["variable"], hi=float(r["p99"]), source="nền trạm: p99 split train"))
    return objs


def ccme_wqi(values: pd.DataFrame, objectives: list[Objective]) -> dict:
    """CCME WQI cho MỘT cửa sổ (ví dụ một trạm trong một ngày). ``values``: mỗi cột một biến, NaN = không đo."""
    n_vars = n_failed_vars = n_tests = n_failed = 0
    exc_sum = 0.0
    for o in objectives:
        if o.variable not in values:
            continue
        x = values[o.variable].dropna().to_numpy(dtype=float)
        if len(x) < MIN_TESTS_PER_VARIABLE:
            continue
        exc = np.zeros(len(x))
        if o.lo is not None:
            low = x < o.lo
            exc[low] = o.lo / np.maximum(x[low], _EPS) - 1
        if o.hi is not None:
            high = x > o.hi
            exc[high] = x[high] / o.hi - 1
        fail = exc > 0
        n_vars += 1
        n_failed_vars += int(fail.any())
        n_tests += len(x)
        n_failed += int(fail.sum())
        exc_sum += float(exc.sum())
    if n_vars < MIN_VARIABLES:
        return {"wqi": None, "wqi_class": None, "f1": None, "f2": None, "f3": None, "n_variables": n_vars}
    f1 = 100.0 * n_failed_vars / n_vars
    f2 = 100.0 * n_failed / n_tests
    nse = exc_sum / n_tests
    f3 = nse / (0.01 * nse + 0.01)
    wqi = 100.0 - math.sqrt(f1 ** 2 + f2 ** 2 + f3 ** 2) / 1.732
    wqi = max(0.0, wqi)
    return {"wqi": round(wqi, 2), "wqi_class": wqi_class(wqi), "f1": round(f1, 2), "f2": round(f2, 2),
            "f3": round(f3, 2), "n_variables": n_vars}


def wqi_daily_pandas(df: pd.DataFrame, baselines: pd.DataFrame | None = None) -> pd.DataFrame:
    """WQI mỗi trạm mỗi ngày (UTC) bằng pandas: dùng cho dashboard (dữ liệu lấy từ API) và để kiểm chứng bản Spark."""
    rows = []
    d = df.assign(date=pd.to_datetime(df["ts"], utc=True).dt.date)
    for (sid, date), g in d.groupby(["station_id", "date"], sort=True):
        r = ccme_wqi(g, objectives_for_station(baselines, sid))
        rows.append({"station_id": sid, "date": date, **r})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------------------------- Spark
def site_baselines(silver):
    """Ngưỡng nền theo trạm từ split train: p01, p50, p99 và số giá trị của mỗi biến core."""
    from pyspark.sql import functions as F

    lo, hi = contracts.SPLITS["train"]
    train = silver.where((F.col("ts") >= F.to_timestamp(F.lit(lo))) & (F.col("ts") < F.to_timestamp(F.lit(hi))))
    pairs = ", ".join(f"'{v}', `{v}`" for v in contracts.CORE_VARIABLES)
    long = train.select("station_id", F.expr(f"stack({len(contracts.CORE_VARIABLES)}, {pairs}) as (variable, value)"))
    q = F.percentile_approx("value", [0.01, 0.5, SITE_QUANTILE], 10_000)
    return (long.where(F.col("value").isNotNull()).groupBy("station_id", "variable")
            .agg(q.alias("q"), F.count("*").alias("n"))
            .select("station_id", "variable", F.col("q")[0].alias("p01"), F.col("q")[1].alias("p50"),
                    F.col("q")[2].alias("p99"), "n")
            .orderBy("station_id", "variable"))


def objectives_frame(spark, baselines):
    """DataFrame (station_id, variable, lo, hi): ngưỡng quốc gia cho mọi trạm + ngưỡng nền p99 theo trạm."""
    from pyspark.sql import functions as F

    stations = baselines.select("station_id").distinct()
    nat = spark.createDataFrame([(o.variable, o.lo, o.hi) for o in NATIONAL_OBJECTIVES],
                                "variable string, lo double, hi double")
    site = (baselines.where(F.col("variable").isin(SITE_VARIABLES))
            .select("station_id", "variable", F.lit(None).cast("double").alias("lo"), F.col("p99").alias("hi")))
    return stations.crossJoin(nat).select("station_id", "variable", "lo", "hi").unionByName(site)


def wqi_daily(silver, objectives):
    """Bảng gold ``wqi_daily``: station_id, date, wqi, wqi_class, f1, f2, f3, n_variables (tính bằng Spark)."""
    from pyspark.sql import functions as F

    pairs = ", ".join(f"'{v}', `{v}`" for v in WQI_VARIABLES)
    long = (silver.select("station_id", F.to_date("ts").alias("date"),
                          F.expr(f"stack({len(WQI_VARIABLES)}, {pairs}) as (variable, value)"))
            .where(F.col("value").isNotNull())
            .join(objectives, ["station_id", "variable"]))
    low = F.col("lo").isNotNull() & (F.col("value") < F.col("lo"))
    high = F.col("hi").isNotNull() & (F.col("value") > F.col("hi"))
    exc = (F.when(low, F.col("lo") / F.greatest(F.col("value"), F.lit(_EPS)) - 1)
           .when(high, F.col("value") / F.col("hi") - 1).otherwise(F.lit(0.0)))
    per_var = (long.withColumn("exc", exc)
               .groupBy("station_id", "date", "variable")
               .agg(F.count("*").alias("n"), F.sum((F.col("exc") > 0).cast("long")).alias("n_fail"),
                    F.sum("exc").alias("exc_sum"))
               .where(F.col("n") >= MIN_TESTS_PER_VARIABLE))
    day = per_var.groupBy("station_id", "date").agg(
        F.count("*").alias("n_variables"),
        F.sum((F.col("n_fail") > 0).cast("long")).alias("n_failed_variables"),
        F.sum("n").alias("n_tests"), F.sum("n_fail").alias("n_failed"), F.sum("exc_sum").alias("exc_sum"))
    nse = F.col("exc_sum") / F.col("n_tests")
    out = (day.withColumn("f1", 100.0 * F.col("n_failed_variables") / F.col("n_variables"))
           .withColumn("f2", 100.0 * F.col("n_failed") / F.col("n_tests"))
           .withColumn("f3", nse / (0.01 * nse + 0.01))
           .withColumn("wqi", F.greatest(F.lit(0.0), 100.0 - F.sqrt(F.col("f1") ** 2 + F.col("f2") ** 2
                                                                   + F.col("f3") ** 2) / 1.732))
           .withColumn("wqi", F.when(F.col("n_variables") >= MIN_VARIABLES, F.round("wqi", 2))))
    cls = F.lit(None).cast("string")
    for lo, name in reversed(WQI_CLASSES):
        cls = F.when(F.col("wqi") >= lo, F.lit(name)).otherwise(cls)
    return (out.withColumn("wqi_class", cls)
            .select("station_id", "date", "wqi", "wqi_class", F.round("f1", 2).alias("f1"),
                    F.round("f2", 2).alias("f2"), F.round("f3", 2).alias("f3"), "n_variables")
            .orderBy("station_id", "date"))


def main(argv: list[str] | None = None) -> None:
    from pyspark.sql import functions as F

    from wq.config import get_settings
    from wq.spark_utils import build_spark

    s = get_settings()
    ap = argparse.ArgumentParser(description="Ghi gold site_baselines + wqi_daily và bảng tóm tắt WQI")
    ap.add_argument("--lake", default=s.lake_path)
    ap.add_argument("--tables-dir", type=Path, default=Path("docs/report/tables"))
    ap.add_argument("--master", default=None)
    args = ap.parse_args(argv)

    spark = build_spark("wq-wqi-tv2", master=args.master)
    silver = spark.read.parquet(f"{args.lake}/silver/measurements")
    base = site_baselines(silver)
    base.coalesce(1).write.mode("overwrite").parquet(f"{args.lake}/gold/site_baselines")
    base = spark.read.parquet(f"{args.lake}/gold/site_baselines")
    wqi = wqi_daily(silver, objectives_frame(spark, base))
    wqi.write.mode("overwrite").partitionBy("station_id").parquet(f"{args.lake}/gold/wqi_daily")
    wqi = spark.read.parquet(f"{args.lake}/gold/wqi_daily")

    args.tables_dir.mkdir(parents=True, exist_ok=True)
    base.orderBy("station_id", "variable").toPandas().round(4).to_csv(
        args.tables_dir / "tv2_site_baselines.csv", index=False)
    split = F.when(F.col("date") < F.lit(contracts.SPLITS["valid"][0]).cast("date"), "train") \
        .when(F.col("date") < F.lit(contracts.SPLITS["test"][0]).cast("date"), "valid").otherwise("test")
    counts = (wqi.where(F.col("wqi").isNotNull()).withColumn("split", split)
              .groupBy("station_id", "split", "wqi_class").count().toPandas()
              .pivot_table(index=["station_id", "split"], columns="wqi_class", values="count", fill_value=0))
    counts = counts.reindex(columns=[c for _, c in WQI_CLASSES], fill_value=0).astype(int).reset_index()
    counts.columns.name = None
    counts.to_csv(args.tables_dir / "tv2_wqi_class_days.csv", index=False)
    print(counts.to_string(index=False))
    spark.stop()


if __name__ == "__main__":
    main()
