"""EDA dữ liệu silver (TV2.2b): phân bố từng biến, chu kỳ ngày của DO và nhiệt độ, theo mùa, tương quan.

Mọi phép tổng hợp chạy bằng Spark trên data lake (cục bộ hoặc HDFS); chỉ kết quả nhỏ (vài trăm dòng) mới
về pandas để vẽ. Notebook ``notebooks/tv2_eda.ipynb`` gọi các hàm ở đây.

    python -m batch.eda_tv2                 # ghi docs/report/tables/tv2_*.csv và docs/images/tv2_*.png

Giờ trong ngày tính theo **giờ mặt trời địa phương** = giờ UTC + kinh độ / 15: dữ liệu lưu ở UTC nên nếu vẽ theo
giờ UTC, đỉnh DO của trạm Oregon và trạm New Jersey lệch nhau 3 giờ chỉ vì múi giờ.
"""
from __future__ import annotations

import argparse
import time
from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from batch.data_report_tv2 import _short_name, load_stations, read_silver_spark, to_long
from wq import contracts
from wq.config import get_settings
from wq.spark_utils import build_spark

QUANTILES = [0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99]
SEASON_VI = {12: "Đông", 1: "Đông", 2: "Đông", 3: "Xuân", 4: "Xuân", 5: "Xuân",
             6: "Hè", 7: "Hè", 8: "Hè", 9: "Thu", 10: "Thu", 11: "Thu"}


def variable_stats(silver: DataFrame) -> DataFrame:
    """Thống kê mô tả theo trạm x biến (chỉ giá trị có đo): n, mean, std, min, các phân vị, max."""
    long = to_long(silver).where(F.col("value").isNotNull())
    q = F.percentile_approx("value", QUANTILES, 10_000)
    out = (long.groupBy("station_id", "variable").agg(
        F.count("*").alias("n"), F.mean("value").alias("mean"), F.stddev("value").alias("std"),
        F.min("value").alias("min"), q.alias("q"), F.max("value").alias("max")))
    for i, p in enumerate(QUANTILES):
        out = out.withColumn(f"p{int(p * 100):02d}", F.col("q")[i])
    return out.drop("q").orderBy("station_id", "variable")


def with_solar_hour(df: DataFrame, meta: pd.DataFrame) -> DataFrame:
    """Thêm ``solar_hour`` (0..23) = floor(giờ UTC + kinh độ/15) mod 24, kinh độ lấy từ bảng trạm."""
    lon = F.create_map(*[F.lit(x) for _, r in meta.iterrows() for x in (r["station_id"], float(r["longitude"]))])
    hour = F.hour("ts") + F.minute("ts") / 60.0 + lon[F.col("station_id")] / 15.0
    return df.withColumn("solar_hour", F.pmod(F.floor(hour), F.lit(24)).cast("int"))


def diurnal_profile(silver: DataFrame, meta: pd.DataFrame, variables=("dissolved_oxygen", "water_temp")) -> DataFrame:
    """Trung bình theo trạm x biến x mùa x giờ mặt trời, và độ lệch so với trung bình ngày của chính trạm-mùa đó
    (``anomaly``) để so được biên độ dao động ngày giữa các trạm có mức nền khác nhau."""
    season = F.create_map(*[F.lit(x) for kv in SEASON_VI.items() for x in kv])
    long = (with_solar_hour(to_long(silver, list(variables)).where(F.col("value").isNotNull()), meta)
            .withColumn("season", season[F.month("ts")]))
    prof = long.groupBy("station_id", "variable", "season", "solar_hour").agg(
        F.mean("value").alias("mean"), F.count("*").alias("n"))
    base = prof.groupBy("station_id", "variable", "season").agg(F.mean("mean").alias("daily_mean"))
    return (prof.join(base, ["station_id", "variable", "season"])
            .withColumn("anomaly", F.col("mean") - F.col("daily_mean"))
            .orderBy("station_id", "variable", "season", "solar_hour"))


def monthly_profile(silver: DataFrame) -> DataFrame:
    """Trung bình theo trạm x biến x tháng (gộp 3 năm) và theo năm-tháng (để thấy xu hướng)."""
    long = to_long(silver).where(F.col("value").isNotNull())
    return (long.groupBy("station_id", "variable", F.year("ts").alias("year"), F.month("ts").alias("month"))
            .agg(F.mean("value").alias("mean"), F.count("*").alias("n"))
            .orderBy("station_id", "variable", "year", "month"))


def within_station_corr(silver: DataFrame, variables: list[str] | None = None) -> pd.DataFrame:
    """Hệ số tương quan Pearson từng cặp biến, tính RIÊNG trong từng trạm (gộp chung các trạm sẽ trộn khác biệt
    về mức nền giữa trạm vào tương quan). Trả về long: station_id, var_a, var_b, r, n."""
    variables = variables or contracts.VARIABLE_NAMES
    pairs = [(a, b) for i, a in enumerate(variables) for b in variables[i + 1:]]
    aggs = []
    for a, b in pairs:
        both = F.col(a).isNotNull() & F.col(b).isNotNull()
        aggs += [F.corr(a, b).alias(f"r__{a}__{b}"), F.sum(both.cast("long")).alias(f"n__{a}__{b}")]
    wide = silver.groupBy("station_id").agg(*aggs).orderBy("station_id").toPandas()
    rows = []
    for _, r in wide.iterrows():
        for a, b in pairs:
            rows.append({"station_id": r["station_id"], "var_a": a, "var_b": b,
                         "r": r[f"r__{a}__{b}"], "n": int(r[f"n__{a}__{b}"])})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------------------------- hình
def _plt():
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    return plt


def _clean(ax) -> None:
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    ax.grid(alpha=0.3)
    ax.set_axisbelow(True)


def plot_distributions(stats: pd.DataFrame, meta: pd.DataFrame, path: Path) -> None:
    """Hộp phân vị (p05–p95 râu, p25–p75 hộp, trung vị) theo trạm cho từng biến; trục log cho biến lệch phải."""
    plt = _plt()
    labels = _short_name(meta)
    variables = [v for v in contracts.VARIABLE_NAMES if v in set(stats["variable"])]
    fig, axes = plt.subplots(2, 3, figsize=(15, 8.5), constrained_layout=True)
    for ax, v in zip(axes.ravel(), variables):
        g = stats[stats["variable"] == v].sort_values("station_id")
        y = range(len(g))
        ax.hlines(y, g["p05"], g["p95"], color="#94a3b8", lw=1.2)
        ax.barh(y, g["p75"] - g["p25"], left=g["p25"], height=0.55, color="#93c5fd", edgecolor="#2563eb")
        ax.scatter(g["p50"], y, color="#1e3a8a", s=14, zorder=3)
        ax.set_yticks(list(y), [labels.get(s, s) for s in g["station_id"]], fontsize=7)
        ax.invert_yaxis()
        if v in ("turbidity", "specific_conductance", "discharge") and (g["p05"] > 0).all():
            ax.set_xscale("log")
        ax.set_title(f"{contracts.LABELS_VI[v]} ({contracts.UNITS[v]})", fontsize=10)
        _clean(ax)
    for ax in axes.ravel()[len(variables):]:
        ax.set_visible(False)
    fig.suptitle("Phân bố giá trị theo trạm (hộp: p25–p75, chấm: trung vị, râu: p05–p95)", fontsize=11)
    fig.savefig(path, dpi=120)
    plt.close(fig)


def plot_diurnal(prof: pd.DataFrame, meta: pd.DataFrame, path: Path) -> None:
    """Chu kỳ ngày (độ lệch so với trung bình ngày) của DO và nhiệt độ, mùa hè và mùa đông, mỗi trạm 1 đường."""
    plt = _plt()
    labels = _short_name(meta)
    fig, axes = plt.subplots(2, 2, figsize=(13, 8), sharex=True, constrained_layout=True)
    cmap = plt.get_cmap("tab10")
    for i, v in enumerate(["dissolved_oxygen", "water_temp"]):
        for j, season in enumerate(["Hè", "Đông"]):
            ax = axes[i, j]
            g = prof[(prof["variable"] == v) & (prof["season"] == season)]
            for k, (sid, s) in enumerate(g.groupby("station_id")):
                ax.plot(s["solar_hour"], s["anomaly"], color=cmap(k % 10), lw=1.3, label=labels.get(sid, sid))
            ax.axhline(0, color="#475569", lw=0.8)
            ax.set_title(f"{contracts.LABELS_VI[v]}: mùa {season.lower()}", fontsize=10)
            ax.set_ylabel(f"lệch so với TB ngày ({contracts.UNITS[v]})", fontsize=9)
            _clean(ax)
    for ax in axes[1]:
        ax.set_xlabel("Giờ mặt trời địa phương")
        ax.set_xticks(range(0, 24, 3))
    axes[0, 1].legend(fontsize=7, frameon=False, loc="upper left", bbox_to_anchor=(1.0, 1.0))
    fig.suptitle("Chu kỳ ngày của DO và nhiệt độ nước (2023–2025)", fontsize=11)
    fig.savefig(path, dpi=120)
    plt.close(fig)


def plot_seasonal(monthly: pd.DataFrame, meta: pd.DataFrame, path: Path) -> None:
    """Trung bình tháng theo thời gian (2023–2025) của DO và nhiệt độ: thấy chu kỳ năm và quan hệ ngược chiều."""
    plt = _plt()
    labels = _short_name(meta)
    cmap = plt.get_cmap("tab10")
    fig, axes = plt.subplots(2, 1, figsize=(13, 7.5), sharex=True, constrained_layout=True)
    for ax, v in zip(axes, ["water_temp", "dissolved_oxygen"]):
        g = monthly[monthly["variable"] == v].copy()
        g["date"] = pd.to_datetime(dict(year=g["year"], month=g["month"], day=15))
        for k, (sid, s) in enumerate(g.groupby("station_id")):
            ax.plot(s["date"], s["mean"], color=cmap(k % 10), lw=1.3, marker="o", ms=2.5, label=labels.get(sid, sid))
        ax.set_ylabel(f"{contracts.LABELS_VI[v]} ({contracts.UNITS[v]})", fontsize=9)
        _clean(ax)
    axes[0].legend(fontsize=7, frameon=False, ncol=2, loc="upper left", bbox_to_anchor=(1.0, 1.0))
    fig.suptitle("Trung bình tháng: nhiệt độ tăng thì DO giảm (độ hòa tan oxy giảm theo nhiệt độ)", fontsize=11)
    fig.savefig(path, dpi=120)
    plt.close(fig)


def plot_correlation(corr: pd.DataFrame, path: Path) -> pd.DataFrame:
    """Ma trận trung vị (qua các trạm) của hệ số tương quan trong-trạm. Trả về ma trận để in ra."""
    import numpy as np
    plt = _plt()
    variables = contracts.VARIABLE_NAMES
    med = corr.dropna(subset=["r"]).groupby(["var_a", "var_b"])["r"].median()
    nst = corr.dropna(subset=["r"]).groupby(["var_a", "var_b"])["r"].size()
    mat = pd.DataFrame(np.eye(len(variables)), index=variables, columns=variables)
    for (a, b), r in med.items():
        mat.loc[a, b] = mat.loc[b, a] = r
    fig, ax = plt.subplots(figsize=(7.2, 6), constrained_layout=True)
    im = ax.imshow(mat.to_numpy(), cmap="RdBu_r", vmin=-1, vmax=1)
    for i, a in enumerate(variables):
        for j, b in enumerate(variables):
            r = mat.iloc[i, j]
            k = nst.get((a, b), nst.get((b, a)))
            txt = "1" if i == j else f"{r:+.2f}\n({k} trạm)" if k else "—"
            ax.text(j, i, txt, ha="center", va="center", fontsize=8, color="white" if abs(r) > 0.6 else "#111827")
    names = [contracts.LABELS_VI[v] for v in variables]
    ax.set_xticks(range(len(variables)), names, rotation=35, ha="right", fontsize=8)
    ax.set_yticks(range(len(variables)), names, fontsize=8)
    fig.colorbar(im, ax=ax, shrink=0.8, label="Pearson r (trung vị qua các trạm)")
    ax.set_title("Tương quan giữa các biến, tính trong từng trạm", fontsize=10)
    fig.savefig(path, dpi=120)
    plt.close(fig)
    return mat


# ---------------------------------------------------------------------------------------------- main
def run_all(spark, lake: str, stations_file: str, tables_dir: Path, images_dir: Path) -> dict[str, pd.DataFrame]:
    meta = load_stations(stations_file)
    silver = read_silver_spark(spark, lake, list(meta["station_id"])).cache()
    tables_dir.mkdir(parents=True, exist_ok=True)
    images_dir.mkdir(parents=True, exist_ok=True)

    stats = variable_stats(silver).toPandas()
    stats.round(4).to_csv(tables_dir / "tv2_variable_stats.csv", index=False)
    plot_distributions(stats, meta, images_dir / "tv2_distributions.png")

    diurnal = diurnal_profile(silver, meta).toPandas()
    diurnal.round(4).to_csv(tables_dir / "tv2_diurnal.csv", index=False)
    plot_diurnal(diurnal, meta, images_dir / "tv2_diurnal.png")

    monthly = monthly_profile(silver).toPandas()
    monthly.round(4).to_csv(tables_dir / "tv2_monthly.csv", index=False)
    plot_seasonal(monthly, meta, images_dir / "tv2_seasonal.png")

    corr = within_station_corr(silver)
    corr.round(4).to_csv(tables_dir / "tv2_correlations.csv", index=False)
    mat = plot_correlation(corr, images_dir / "tv2_correlation.png")
    silver.unpersist()
    return {"stats": stats, "diurnal": diurnal, "monthly": monthly, "corr": corr, "corr_matrix": mat}


def main(argv: list[str] | None = None) -> None:
    s = get_settings()
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--lake", default=s.lake_path)
    ap.add_argument("--stations-file", default="config/stations.json")
    ap.add_argument("--tables-dir", type=Path, default=Path("docs/report/tables"))
    ap.add_argument("--images-dir", type=Path, default=Path("docs/images"))
    ap.add_argument("--master", default=None)
    args = ap.parse_args(argv)
    t0 = time.perf_counter()
    spark = build_spark("wq-eda-tv2", master=args.master)
    out = run_all(spark, args.lake, args.stations_file, args.tables_dir, args.images_dir)
    print(out["corr_matrix"].round(2).to_string())
    print(f"\nEDA xong trong {time.perf_counter() - t0:.1f} s")
    spark.stop()


if __name__ == "__main__":
    main()
