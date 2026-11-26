"""Báo cáo dữ liệu chuẩn (TV2.1) và phân bố khoảng thiếu (TV2.2b), tính bằng Spark trên data lake.

Đọc ``{lake}/silver/measurements`` (đĩa cục bộ hoặc HDFS, theo ``WQ_LAKE_PATH``), kiểm chứng lưới thời gian
rồi ghi các bảng nhỏ cho báo cáo:

    docs/report/tables/tv2_silver_checks.csv   số dòng, số mốc mỗi trạm, khóa trùng, mốc ngoài lưới
    docs/report/tables/tv2_stations.csv        mỗi trạm một dòng: mã, tên, bang, loại thủy vực, biến có đo, lý do chọn
    docs/report/tables/tv2_missingness.csv     tỉ lệ thiếu theo trạm x biến x split (train / valid / test / all)
    docs/report/tables/tv2_gap_lengths.csv     station_id, variable, gap_steps, count (đoạn thiếu liên tiếp)
    docs/report/tables/tv2_gap_summary.csv     số đoạn thiếu, trung vị, p95, độ dài lớn nhất theo trạm x biến
    docs/images/tv2_missingness.png, docs/images/tv2_gap_lengths.png

Chạy (trong container, sau ``make data`` và ``make etl``):
    python -m batch.data_report_tv2
    python -m batch.data_report_tv2 --stations core     # chỉ 10 trạm chuẩn khi lake có cả bộ lớn

Mọi bảng sắp xếp cố định nên chạy lại ra đúng cùng nội dung.
"""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from wq import contracts
from wq.config import get_settings
from wq.spark_utils import build_spark

STEP_S = contracts.FREQ_MINUTES * 60
SPLIT_ORDER = [*contracts.SPLITS, "all"]

# Loại thủy vực theo trường site_type của USGS
WATER_BODY_VI = {
    "Stream": "Sông/suối",
    "Tidal stream": "Sông chịu ảnh hưởng triều",
    "Estuary": "Cửa sông",
    "Lake, Reservoir, Impoundment": "Hồ/hồ chứa",
    "Lake": "Hồ",
    "Canal": "Kênh",
    "Spring": "Mạch nước",
}

# Vùng địa lý (Census Bureau regions/divisions) để thể hiện độ phủ khí hậu của bộ trạm
STATE_REGION_VI = {
    "Illinois": "Trung Tây", "Missouri": "Trung Tây", "North Dakota": "Trung Tây", "Minnesota": "Trung Tây",
    "Iowa": "Trung Tây", "Ohio": "Trung Tây", "Indiana": "Trung Tây", "Wisconsin": "Trung Tây",
    "New Jersey": "Đông Bắc", "New York": "Đông Bắc", "Pennsylvania": "Đông Bắc",
    "Texas": "Nam (vùng duyên hải vịnh Mexico)", "Georgia": "Đông Nam", "Florida": "Đông Nam",
    "Oregon": "Tây Bắc (Thái Bình Dương)", "Washington": "Tây Bắc (Thái Bình Dương)", "California": "Tây",
}


# ---------------------------------------------------------------------------------------------- Spark logic
def with_split(df: DataFrame, ts_col: str = "ts") -> DataFrame:
    """Thêm cột ``split`` theo contracts.SPLITS (nửa mở [start, end)); mốc ngoài mọi split => NULL."""
    expr = None
    for name, (lo, hi) in contracts.SPLITS.items():
        cond = (F.col(ts_col) >= F.to_timestamp(F.lit(lo))) & (F.col(ts_col) < F.to_timestamp(F.lit(hi)))
        expr = F.when(cond, F.lit(name)) if expr is None else expr.when(cond, F.lit(name))
    return df.withColumn("split", expr)


def to_long(silver: DataFrame, variables: list[str] | None = None) -> DataFrame:
    """Wide -> long: (station_id, ts, qc_flags, variable, value). Dùng stack để không phải union 6 lần."""
    variables = variables or contracts.VARIABLE_NAMES
    pairs = ", ".join(f"'{v}', `{v}`" for v in variables)
    return silver.select("station_id", "ts", "qc_flags",
                         F.expr(f"stack({len(variables)}, {pairs}) as (variable, value)"))


def silver_checks(silver: DataFrame, start: str, end: str) -> pd.DataFrame:
    """Kiểm chứng silver: mỗi trạm đủ mốc của lưới [start, end), không trùng khóa, không mốc lệch lưới."""
    expected = int((pd.Timestamp(end) - pd.Timestamp(start)).total_seconds() // STEP_S)
    lo, hi = F.to_timestamp(F.lit(start)), F.to_timestamp(F.lit(end))
    per = (silver.groupBy("station_id").agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("ts").alias("n_distinct_ts"),
        F.min("ts").alias("ts_min"),
        F.max("ts").alias("ts_max"),
        F.sum(((F.col("ts") < lo) | (F.col("ts") >= hi)).cast("int")).alias("n_outside_range"),
        F.sum((F.unix_timestamp("ts") % STEP_S != 0).cast("int")).alias("n_off_grid"),
        F.sum((F.col("qc_flags") != 0).cast("int")).alias("n_rows_qc_flagged"),
    ).orderBy("station_id").toPandas())
    per["expected_rows"] = expected
    per["n_duplicate_keys"] = per["n_rows"] - per["n_distinct_ts"]
    per["ok"] = ((per["n_rows"] == expected) & (per["n_duplicate_keys"] == 0)
                 & (per["n_outside_range"] == 0) & (per["n_off_grid"] == 0))
    for c in ("ts_min", "ts_max"):
        per[c] = pd.to_datetime(per[c]).dt.strftime("%Y-%m-%d %H:%M")
    cols = ["station_id", "n_rows", "expected_rows", "n_duplicate_keys", "n_outside_range", "n_off_grid",
            "ts_min", "ts_max", "n_rows_qc_flagged", "ok"]
    return per[cols]


def missingness_by_split(silver: DataFrame) -> DataFrame:
    """Tỉ lệ thiếu theo trạm x biến x split; split ``all`` là toàn bộ [DATA_START, DATA_END).

    n_qc_flagged: số ô bị QC phạm vi vật lý đặt NULL (đã nằm trong n_missing) - tách thiếu do cảm biến không gửi
    với thiếu do giá trị bị loại.
    """
    long = with_split(to_long(silver)).where(F.col("split").isNotNull())
    bit = F.create_map(*[F.lit(x) for kv in contracts.VARIABLE_BIT.items() for x in kv])
    long = long.withColumn("flagged", (F.col("qc_flags").bitwiseAND(bit[F.col("variable")]) != 0).cast("long"))
    aggs = [F.count("*").alias("n_slots"),
            F.sum(F.col("value").isNotNull().cast("long")).alias("n_observed"),
            F.sum("flagged").alias("n_qc_flagged")]
    by_split = long.groupBy("station_id", "variable", "split").agg(*aggs)
    overall = long.groupBy("station_id", "variable").agg(*aggs).withColumn("split", F.lit("all"))
    out = by_split.unionByName(overall)
    return (out.withColumn("n_missing", F.col("n_slots") - F.col("n_observed"))
            .withColumn("missing_rate", F.round(F.col("n_missing") / F.col("n_slots"), 4))
            .select("station_id", "variable", "split", "n_slots", "n_observed", "n_missing", "missing_rate",
                    "n_qc_flagged"))


def gap_runs(silver: DataFrame, start: str = contracts.DATA_START) -> DataFrame:
    """Mỗi đoạn thiếu liên tiếp (gaps-and-islands) của mỗi trạm x biến: station_id, variable, gap_start,
    gap_steps, at_edge.

    Silver là lưới đều nên chỉ số mốc idx = (ts - start) / 15 phút. Trong các ô NULL sắp theo idx,
    idx - row_number() không đổi trên một đoạn liên tiếp => groupBy theo hiệu đó ra từng đoạn.
    Biến mà trạm không bao giờ đo (100% thiếu) bị loại: đó là cảm biến không lắp, không phải khoảng thiếu.
    at_edge = đoạn chạm đầu hoặc cuối chuỗi (độ dài thật có thể dài hơn, bị cắt bởi khoảng dữ liệu).
    """
    t0 = F.unix_timestamp(F.to_timestamp(F.lit(start)))
    long = to_long(silver).withColumn("idx", ((F.unix_timestamp("ts") - t0) / STEP_S).cast("long"))
    bounds = long.groupBy("station_id", "variable").agg(
        F.min("idx").alias("first_idx"), F.max("idx").alias("last_idx"),
        F.count("value").alias("n_observed"))
    w = Window.partitionBy("station_id", "variable").orderBy("idx")
    runs = (long.where(F.col("value").isNull())
            .withColumn("grp", F.col("idx") - F.row_number().over(w))
            .groupBy("station_id", "variable", "grp")
            .agg(F.min("idx").alias("start_idx"), F.max("idx").alias("end_idx"), F.min("ts").alias("gap_start"))
            .join(bounds, ["station_id", "variable"])
            .where(F.col("n_observed") > 0)
            .withColumn("gap_steps", F.col("end_idx") - F.col("start_idx") + 1)
            .withColumn("at_edge", (F.col("start_idx") == F.col("first_idx")) | (F.col("end_idx") == F.col("last_idx"))))
    return runs.select("station_id", "variable", "gap_start", "gap_steps", "at_edge")


def gap_length_counts(runs: DataFrame) -> DataFrame:
    """Phân bố độ dài đoạn thiếu: station_id, variable, gap_steps, count (đúng cột TV3.1 cần)."""
    return (runs.groupBy("station_id", "variable", "gap_steps").agg(F.count("*").alias("count"))
            .orderBy("station_id", "variable", "gap_steps"))


def gap_summary(runs: DataFrame) -> DataFrame:
    """Theo trạm x biến: số đoạn, tổng mốc thiếu, trung vị/p95/lớn nhất của độ dài đoạn (đơn vị: mốc 15 phút)."""
    return (runs.groupBy("station_id", "variable").agg(
        F.count("*").alias("n_gaps"),
        F.sum("gap_steps").alias("missing_steps"),
        F.percentile("gap_steps", 0.5).alias("median_gap_steps"),
        F.percentile("gap_steps", 0.95).alias("p95_gap_steps"),
        F.max("gap_steps").alias("max_gap_steps"),
        F.sum(F.col("at_edge").cast("int")).alias("n_edge_gaps"))
        .withColumn("max_gap_hours", F.round(F.col("max_gap_steps") / contracts.STEPS_PER_HOUR, 2))
        .orderBy("station_id", "variable"))


# ---------------------------------------------------------------------------------------------- bảng trạm
def load_stations(stations_file: str) -> pd.DataFrame:
    return pd.DataFrame(json.loads(Path(stations_file).read_text(encoding="utf-8"))["stations"])


def stations_table(meta: pd.DataFrame, miss: pd.DataFrame) -> pd.DataFrame:
    """Bảng trạm cho Ch.3. ``miss``: kết quả missingness_by_split (pandas).

    Lý do chọn sinh từ số liệu và metadata (không viết tay) để luôn khớp dữ liệu: vùng địa lý, quy mô lưu vực,
    số biến core có đo, tỉ lệ thiếu trung bình của các biến core.
    """
    allm = miss[miss["split"] == "all"]
    rows = []
    for _, st in meta.iterrows():
        m = allm[allm["station_id"] == st["station_id"]].set_index("variable")
        measured = [v for v in contracts.VARIABLE_NAMES if v in m.index and m.loc[v, "n_observed"] > 0]
        core = [v for v in contracts.CORE_VARIABLES if v in measured]
        core_miss = float(m.loc[core, "missing_rate"].mean()) if core else float("nan")
        area = st.get("drainage_area_sqmi")
        if pd.isna(area):
            size = "lưu vực không công bố (sông lớn, trạm trên dòng chính)"
        elif area >= 10_000:
            size = f"sông lớn, lưu vực {area:,.0f} mi²"
        elif area >= 1_000:
            size = f"sông vừa, lưu vực {area:,.0f} mi²"
        else:
            size = f"sông nhỏ/đô thị, lưu vực {area:,.0f} mi²"
        region = STATE_REGION_VI.get(st.get("state"), "khác")
        reason = (f"{region}; {size}; đo {len(core)}/{len(contracts.CORE_VARIABLES)} biến core"
                  f"{'' if 'discharge' in measured else ', không có lưu lượng'}; "
                  f"thiếu trung bình {core_miss:.1%} ở biến core")
        rows.append({
            "station_id": st["station_id"],
            "name": st.get("name"),
            "state": st.get("state"),
            "region": region,
            "site_type": st.get("site_type"),
            "water_body": WATER_BODY_VI.get(st.get("site_type"), st.get("site_type")),
            "latitude": round(float(st["latitude"]), 5) if pd.notna(st.get("latitude")) else None,
            "longitude": round(float(st["longitude"]), 5) if pd.notna(st.get("longitude")) else None,
            "drainage_area_sqmi": area,
            "variables_measured": ";".join(measured),
            "n_core_measured": len(core),
            "core_missing_rate": round(core_miss, 4),
            "selection_reason": reason,
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------------------------- hình
def _short_name(meta: pd.DataFrame) -> dict[str, str]:
    """Nhãn ngắn cho trục: mã số trạm + 2 từ đầu tên."""
    out = {}
    for _, r in meta.iterrows():
        words = str(r.get("name") or "").replace(",", "").split()
        out[r["station_id"]] = f"{r['station_id'].replace('USGS-', '')} {' '.join(words[:2]).title()}"
    return out


def plot_missingness(miss: pd.DataFrame, meta: pd.DataFrame, path: Path) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np

    labels = _short_name(meta)
    stations = sorted(miss["station_id"].unique())
    splits = list(contracts.SPLITS)
    fig, axes = plt.subplots(1, len(splits), figsize=(15, 5.2), sharey=True, constrained_layout=True)
    cmap = plt.get_cmap("Blues").copy()
    for ax, sp in zip(axes, splits):
        piv = (miss[miss["split"] == sp].pivot(index="station_id", columns="variable", values="missing_rate")
               .reindex(index=stations, columns=contracts.VARIABLE_NAMES))
        data = piv.to_numpy(dtype=float)
        im = ax.imshow(data * 100, cmap=cmap, vmin=0, vmax=100, aspect="auto")
        for i in range(data.shape[0]):
            for j in range(data.shape[1]):
                v = data[i, j] * 100
                txt = ("—" if np.isnan(v) else "0" if v == 0 else "<0.1" if v < 0.05
                       else f"{v:.0f}" if v >= 10 else f"{v:.1f}")
                ax.text(j, i, txt,
                        ha="center", va="center", fontsize=8, color="white" if v > 55 else "#1f2937")
        lo, hi = contracts.SPLITS[sp]
        ax.set_title(f"{sp}  [{lo} → {hi})", fontsize=10)
        ax.set_xticks(range(len(contracts.VARIABLE_NAMES)),
                      [contracts.LABELS_VI[v] for v in contracts.VARIABLE_NAMES], rotation=35, ha="right", fontsize=8)
        ax.set_yticks(range(len(stations)), [labels.get(s, s) for s in stations], fontsize=8)
        ax.tick_params(length=0)
        for s in ax.spines.values():
            s.set_visible(False)
    fig.colorbar(im, ax=axes, shrink=0.8, label="Tỉ lệ thiếu (%)")
    fig.suptitle("Tỉ lệ thiếu dữ liệu theo trạm × biến × split (silver, lưới 15 phút)", fontsize=11)
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=130)
    plt.close(fig)


GAP_BINS = [(1, 1, "15 phút"), (2, 4, "30 phút–1 giờ"), (5, 24, "1–6 giờ"), (25, 96, "6–24 giờ"),
            (97, 672, "1–7 ngày"), (673, 10**9, "> 7 ngày")]


def bin_gaps(counts: pd.DataFrame) -> pd.DataFrame:
    """Gom phân bố độ dài đoạn thiếu theo nhóm thời lượng: số đoạn và số mốc thiếu trong mỗi nhóm, theo biến."""
    rows = []
    for v, g in counts.groupby("variable"):
        for lo, hi, label in GAP_BINS:
            sel = g[(g["gap_steps"] >= lo) & (g["gap_steps"] <= hi)]
            rows.append({"variable": v, "bin": label, "n_gaps": int(sel["count"].sum()),
                         "missing_steps": int((sel["gap_steps"] * sel["count"]).sum())})
    out = pd.DataFrame(rows)
    tot = out.groupby("variable")[["n_gaps", "missing_steps"]].transform("sum")
    out["share_gaps"] = out["n_gaps"] / tot["n_gaps"].where(tot["n_gaps"] > 0)
    out["share_missing_steps"] = out["missing_steps"] / tot["missing_steps"].where(tot["missing_steps"] > 0)
    return out


def plot_gap_lengths(counts: pd.DataFrame, path: Path) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np

    binned = bin_gaps(counts)
    variables = [v for v in contracts.VARIABLE_NAMES if v in set(binned["variable"])]
    labels = [b[2] for b in GAP_BINS]
    colors = ["#2563eb", "#0891b2", "#16a34a", "#ca8a04", "#ea580c", "#9333ea"]
    fig, axes = plt.subplots(1, 2, figsize=(14, 4.8), constrained_layout=True)
    x = np.arange(len(labels))
    width = 0.8 / max(len(variables), 1)
    for k, v in enumerate(variables):
        g = binned[binned["variable"] == v].set_index("bin").reindex(labels)
        n_gaps = int(g["n_gaps"].sum())
        axes[0].bar(x + k * width, g["share_gaps"] * 100, width, color=colors[k % len(colors)],
                    label=f"{contracts.LABELS_VI[v]} ({n_gaps:,} đoạn)")
        axes[1].bar(x + k * width, g["share_missing_steps"] * 100, width, color=colors[k % len(colors)])
    for ax, title in zip(axes, ["Tỉ lệ số đoạn thiếu theo độ dài", "Tỉ lệ số mốc thiếu nằm trong đoạn có độ dài đó"]):
        ax.set_xticks(x + width * (len(variables) - 1) / 2, labels, fontsize=9)
        ax.set_ylabel("%")
        ax.set_title(title, fontsize=10)
        ax.grid(axis="y", alpha=0.3)
        ax.set_axisbelow(True)
        for s in ("top", "right"):
            ax.spines[s].set_visible(False)
    axes[0].legend(fontsize=8, frameon=False)
    fig.suptitle("Phân bố độ dài đoạn thiếu liên tiếp (10 trạm chuẩn, 2023–2025)", fontsize=11)
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=130)
    plt.close(fig)


# ---------------------------------------------------------------------------------------------- main
def read_silver_spark(spark: SparkSession, lake: str, station_ids: list[str] | None) -> DataFrame:
    df = spark.read.parquet(f"{lake}/silver/measurements")
    if station_ids:
        df = df.where(F.col("station_id").isin(station_ids))
    return df


def main(argv: list[str] | None = None) -> None:
    s = get_settings()
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--lake", default=s.lake_path)
    ap.add_argument("--stations-file", default="config/stations.json")
    ap.add_argument("--stations", default="core", help="core = các trạm trong --stations-file; all = mọi trạm trong lake")
    ap.add_argument("--tables-dir", type=Path, default=Path("docs/report/tables"))
    ap.add_argument("--images-dir", type=Path, default=Path("docs/images"))
    ap.add_argument("--master", default=None)
    args = ap.parse_args(argv)

    t0 = time.perf_counter()
    meta = load_stations(args.stations_file)
    spark = build_spark("wq-data-report-tv2", master=args.master)
    ids = list(meta["station_id"]) if args.stations == "core" else None
    silver = read_silver_spark(spark, args.lake, ids).cache()
    args.tables_dir.mkdir(parents=True, exist_ok=True)

    checks = silver_checks(silver, contracts.DATA_START, contracts.DATA_END)
    checks.to_csv(args.tables_dir / "tv2_silver_checks.csv", index=False)
    print(checks.to_string(index=False))
    print(f"\nSilver: {len(checks)} trạm, {int(checks['n_rows'].sum()):,} dòng "
          f"(kỳ vọng {len(checks)} x {int(checks['expected_rows'].iloc[0]):,} = "
          f"{len(checks) * int(checks['expected_rows'].iloc[0]):,}); mọi trạm đạt: {bool(checks['ok'].all())}")

    miss = (missingness_by_split(silver).toPandas())
    miss["split"] = pd.Categorical(miss["split"], SPLIT_ORDER, ordered=True)
    miss["variable"] = pd.Categorical(miss["variable"], contracts.VARIABLE_NAMES, ordered=True)
    miss = miss.sort_values(["station_id", "variable", "split"]).reset_index(drop=True)
    miss["split"] = miss["split"].astype(str)
    miss["variable"] = miss["variable"].astype(str)
    miss.to_csv(args.tables_dir / "tv2_missingness.csv", index=False)

    meta = meta[meta["station_id"].isin(set(checks["station_id"]))]
    stations_table(meta, miss).to_csv(args.tables_dir / "tv2_stations.csv", index=False)
    plot_missingness(miss, meta, args.images_dir / "tv2_missingness.png")

    runs = gap_runs(silver).cache()
    counts = gap_length_counts(runs).toPandas()
    counts.to_csv(args.tables_dir / "tv2_gap_lengths.csv", index=False)
    summary = gap_summary(runs).toPandas()
    summary.to_csv(args.tables_dir / "tv2_gap_summary.csv", index=False)
    bin_gaps(counts).to_csv(args.tables_dir / "tv2_gap_bins.csv", index=False)
    plot_gap_lengths(counts, args.images_dir / "tv2_gap_lengths.png")

    print(f"\nĐã ghi bảng vào {args.tables_dir}, hình vào {args.images_dir} "
          f"trong {time.perf_counter() - t0:.1f} s")
    spark.stop()


if __name__ == "__main__":
    main()
