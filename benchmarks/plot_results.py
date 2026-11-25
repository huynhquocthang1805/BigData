"""Vẽ biểu đồ kết quả benchmark (PNG cho báo cáo) từ benchmarks/results/*.csv.

    python -m benchmarks.plot_results

Quy ước trình bày: một trục y cho mỗi biểu đồ, màu theo thứ tự cố định, đường 2px, marker có viền nền,
lưới mảnh, nhãn trực tiếp ở cuối đường + chú thích (legend) khi có >= 2 chuỗi.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300"]  # thứ tự cố định
SURFACE, INK, INK2, MUTED, GRID, AXIS = "#fcfcfb", "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7"


def style(ax, title: str, xlabel: str, ylabel: str, note: str | None = None) -> None:
    ax.set_facecolor(SURFACE)
    ax.figure.set_facecolor(SURFACE)
    ax.set_title(title, loc="left", fontsize=12, color=INK, pad=12)
    ax.set_xlabel(xlabel, color=INK2)
    ax.set_ylabel(ylabel, color=INK2)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(AXIS)
    ax.tick_params(colors=MUTED, labelcolor=INK2)
    if note:
        ax.figure.text(0.01, 0.01, note, fontsize=8, color=MUTED, ha="left", va="bottom")


def line(ax, x, y, i: int, label: str) -> None:
    ax.plot(x, y, color=SERIES[i], linewidth=2, solid_capstyle="round", marker="o", markersize=7,
            markeredgecolor=SURFACE, markeredgewidth=2, label=label)
    ax.annotate(label, (x.iloc[-1], y.iloc[-1]), xytext=(8, 0), textcoords="offset points",
                va="center", fontsize=9, color=INK2)  # nhãn trực tiếp bằng màu chữ, không dùng màu chuỗi


def save(fig, path: Path) -> None:
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    fig.savefig(path, dpi=200, facecolor=SURFACE)
    plt.close(fig)
    print(f"  -> {path}")


def plot_batch(csv: Path, out: Path) -> None:
    df = pd.read_csv(csv)
    df = df.groupby(["engine", "mode", "cores", "scale"], as_index=False).agg(
        total_s=("total_s", "median"), input_values=("input_values", "first"), host_cpus=("host_cpus", "first"))
    df["mvals_per_s"] = df["input_values"] / df["total_s"] / 1e6
    df["mvalues"] = df["input_values"] / 1e6
    note = f"Máy đo: {int(df.host_cpus.iloc[0])} vCPU, Docker; trung vị nếu đo nhiều lần. Nguồn: {csv.name}"

    # 1) Throughput theo quy mô dữ liệu
    fig, ax = plt.subplots(figsize=(8, 4.6))
    groups = [("pandas", "single-process", 1, "pandas (1 tiến trình)")] + \
             [("spark", "local", c, f"Spark local[{c}]") for c in sorted(df[df["mode"] == "local"].cores.unique())]
    for i, (eng, mode, cores, label) in enumerate(groups):
        g = df[(df.engine == eng) & (df["mode"] == mode) & (df.cores == cores)].sort_values("mvalues")
        if not g.empty:
            line(ax, g["mvalues"], g["mvals_per_s"], i, label)
    style(ax, "Throughput ETL theo quy mô dữ liệu", "Số giá trị đầu vào (triệu)", "Triệu giá trị / giây", note)
    ax.set_ylim(bottom=0)
    ax.legend(frameon=False, labelcolor=INK2, loc="upper center", bbox_to_anchor=(0.5, -0.16), ncol=4)
    ax.margins(x=0.15)
    raw = pd.read_csv(csv)  # chú thích RAM đỉnh của pandas (nếu có đo)
    mem = raw[(raw.engine == "pandas") & raw.note.astype(str).str.startswith("peak_rss_mb=")]
    if not mem.empty:
        r = mem.sort_values("input_values").iloc[-1]
        gb = float(str(r.note).split("=")[1]) / 1024
        ax.annotate(f"pandas: RAM đỉnh {gb:.1f} GB", (r.input_values / 1e6, r.input_values / r.total_s / 1e6),
                    xytext=(8, -16), textcoords="offset points", ha="left", va="center", fontsize=8, color=MUTED)
    save(fig, out / "batch_throughput_vs_size.png")

    # 2) Speedup theo số core (quy mô lớn nhất có đủ 1 core)
    loc = df[df["mode"] == "local"]
    if loc.empty:
        scale = None
    else:
        n_cores = loc.groupby("scale").cores.nunique()
        scale = n_cores[n_cores == n_cores.max()].index.max()
    if scale is not None:
        g = loc[loc.scale == scale].sort_values("cores")
        base = g[g.cores == g.cores.min()].total_s.iloc[0]
        g = g.assign(speedup=base / g.total_s)
        fig, ax = plt.subplots(figsize=(6.5, 4.2))
        ax.bar(g.cores.astype(str), g.speedup, width=0.3, color=SERIES[0])
        ax.plot(g.cores.astype(str), g.cores / g.cores.min(), color=MUTED, linewidth=1, label="tuyến tính lý tưởng")
        for xc, yv, ts in zip(g.cores.astype(str), g.speedup, g.total_s):
            ax.annotate(f"{yv:.2f}×\n({ts:.0f}s)", (xc, yv), xytext=(0, 4), textcoords="offset points",
                        ha="center", va="bottom", fontsize=9, color=INK)
        style(ax, f"Speedup ETL theo số core (quy mô ×{scale})", "Số core Spark (local)", "Speedup so với 1 core", note)
        ax.legend(frameon=False, labelcolor=INK2, loc="upper left")
        ax.set_ylim(0, max(g.cores / g.cores.min()) * 1.25)
        save(fig, out / "batch_speedup_vs_cores.png")

    # 3) Local vs cluster (cùng số core lớn nhất) theo từng quy mô
    sp = df[df.engine == "spark"]
    if (sp["mode"] == "cluster").any():
        cores = sp[sp["mode"] == "cluster"].cores.max()
        piv = sp[sp.cores == cores].pivot_table(index="scale", columns="mode", values="total_s").dropna()
        fig, ax = plt.subplots(figsize=(7, 4.2))
        xs = list(range(len(piv)))
        w = 0.28
        for j, mode in enumerate(["local", "cluster"]):
            pos = [x + (j - 0.5) * (w + 0.04) for x in xs]
            ax.bar(pos, piv[mode], width=w, color=SERIES[j], label=f"{mode} ({cores} core)")
            for x, v in zip(pos, piv[mode]):
                ax.annotate(f"{v:.0f}s", (x, v), xytext=(0, 3), textcoords="offset points", ha="center",
                            fontsize=9, color=INK)
        ax.set_xticks(xs, [f"×{sc} ({df[df.scale == sc].input_values.iloc[0] / 1e6:.0f} triệu)" for sc in piv.index])
        style(ax, f"Local vs cluster 2 worker trên cùng máy ({cores} core)", "Quy mô dữ liệu",
              "Thời gian ETL (giây)", note)
        ax.legend(frameon=False, labelcolor=INK2, loc="upper left")
        save(fig, out / "batch_local_vs_cluster.png")


def plot_stream(csv: Path, out: Path) -> None:
    df = pd.read_csv(csv)
    note = f"Máy đo: {int(df.host_cpus.iloc[0])} vCPU, Docker; Kafka 6 partition, trigger 2s. Nguồn: {csv.name}"
    for (mode, cores, ml), g in df.groupby(["mode", "cores", "ml"]):
        g = g.groupby("target_rate", as_index=False).median(numeric_only=True).sort_values("target_rate")
        if len(g) < 2:  # 1 điểm đo (vd. chạy kèm ML) -> báo cáo bằng bảng, không vẽ đường
            continue
        tag = f"{mode}, {cores} core{', ML bật' if ml else ''}"
        suffix = f"{mode}_{cores}c{'_ml' if ml else ''}"
        fig, ax = plt.subplots(figsize=(8, 4.4))
        line(ax, g.target_rate, g.latency_p50_ms / 1000, 0, "p50")
        line(ax, g.target_rate, g.latency_p95_ms / 1000, 1, "p95")
        style(ax, f"Độ trễ end-to-end theo tốc độ phát ({tag})", "Tốc độ phát (message/giây)",
              "Độ trễ producer → PostgreSQL (giây)", note)
        ax.set_ylim(bottom=0)
        ax.legend(frameon=False, labelcolor=INK2, loc="upper left")
        ax.margins(x=0.12)
        save(fig, out / f"stream_latency_{suffix}.png")

        fig, ax = plt.subplots(figsize=(8, 4.4))
        lim = max(g.target_rate.max(), g.throughput_rows_s.max()) * 1.1
        ax.plot([0, lim], [0, lim], color=MUTED, linewidth=1)
        ax.annotate("xử lý kịp 100%", (lim * 0.62, lim * 0.62), xytext=(-4, 8), textcoords="offset points",
                    fontsize=8, color=MUTED, rotation=0)
        line(ax, g.target_rate, g.throughput_rows_s, 0, "rows/s đã ghi vào DB")
        style(ax, f"Throughput thực tế theo tốc độ phát ({tag})", "Tốc độ phát (message/giây)",
              "Bản ghi xử lý / giây", note)
        ax.set_xlim(0, lim)
        ax.set_ylim(0, lim)
        save(fig, out / f"stream_throughput_{suffix}.png")


def vn(x: float, nd: int = 1) -> str:
    """Số kiểu Việt Nam: dấu phẩy thập phân, dấu chấm phân nghìn (khớp cách viết trong báo cáo)."""
    return f"{x:,.{nd}f}".replace(",", " ").replace(".", ",").replace(" ", ".")


def plot_large(res: Path, out: Path) -> None:
    """Dữ liệu thật quy mô lớn: thời gian ETL theo cấu hình và quy mô (batch_large.csv), độ trễ streaming có ML
    theo số trạm (stream_stations.csv)."""
    bl, st = res / "batch_large.csv", res / "stream_stations.csv"

    if bl.exists():
        raw = pd.read_csv(bl)
        raw["failed"] = raw.note.astype(str).str.startswith("FAILED")
        labels = {("pandas", "single-process", "local"): "pandas (1 tiến trình)",
                  ("spark", "local", "local"): "Spark local[4], đĩa cục bộ",
                  ("spark", "cluster", "hdfs"): "Cụm Spark 4 core, đọc/ghi HDFS"}
        keys = [k for k in labels if k in set(zip(raw.engine, raw["mode"], raw.storage))]
        sizes = sorted(raw.stations.unique())                       # mỗi quy mô dữ liệu là một chuỗi màu
        mvals = {n: raw[(raw.stations == n) & raw.input_values.notna()].input_values.max() / 1e6 for n in sizes}
        fig, ax = plt.subplots(figsize=(8, 1.6 + 1.0 * len(keys) * max(1, len(sizes)) ** 0.5))
        h = 0.7 / len(sizes)
        tmax = raw.total_s.max()
        labeled: set = set()
        for j, n in enumerate(sizes):
            for y, (e, m, s) in enumerate(keys):
                g = raw[(raw.stations == n) & (raw.engine == e) & (raw["mode"] == m) & (raw.storage == s)]
                if g.empty:
                    continue
                pos = y + (j - (len(sizes) - 1) / 2) * h
                ok = g[~g.failed]
                if ok.empty:  # không chạy xong (vd. hết bộ nhớ): không vẽ cột, ghi rõ lý do
                    r = g.iloc[-1]
                    x0 = tmax * 0.012  # ô màu nhận diện quy mô dữ liệu (chữ giữ màu mực)
                    ax.plot([x0], [pos], marker="s", ms=7, color=SERIES[j], linestyle="none", clip_on=False, zorder=3)
                    ax.annotate(f"{n} trạm: không chạy được, hết bộ nhớ (RAM đỉnh {vn(r.peak_rss_mb / 1024)} GB)",
                                (x0, pos), xytext=(9, 0), textcoords="offset points", va="center", fontsize=9,
                                color=INK)
                    continue
                t = ok.total_s.median()
                first = n not in labeled  # mỗi quy mô đúng một mục chú thích (kể cả khi cột pandas không có)
                labeled.add(n)
                ax.barh(pos, t, height=h * 0.9, color=SERIES[j],
                        label=f"{n} trạm · {vn(mvals[n])} triệu giá trị" if first else None)
                txt = f"{vn(t)} s"
                if e == "pandas" and ok.peak_rss_mb.notna().any():
                    txt += f"   RAM đỉnh {vn(ok.peak_rss_mb.max() / 1024)} GB"
                ax.annotate(txt, (t, pos), xytext=(6, 0), textcoords="offset points", va="center", fontsize=9,
                            color=INK)
        ax.set_yticks(range(len(keys)), [labels[k] for k in keys])
        ax.invert_yaxis()
        note = f"USGS, dữ liệu thật; máy {int(raw.host_cpus.iloc[0])} vCPU, 15,7 GB RAM, Docker. Nguồn: {bl.name}"
        title = ("ETL trên dữ liệu thật: thời gian theo cấu hình và quy mô" if len(sizes) > 1 else
                 f"ETL {vn(mvals[sizes[0]])} triệu giá trị thật: thời gian theo cấu hình")
        style(ax, title, "Thời gian ETL (giây)", "", note)
        ax.grid(False)
        ax.grid(axis="x", color=GRID, linewidth=0.8)
        ax.set_xlim(0, tmax * 1.5)  # chừa chỗ cho nhãn giá trị
        if len(sizes) > 1:  # dưới trục: không đè lên nhãn giá trị của các cột
            ax.legend(frameon=False, labelcolor=INK2, loc="upper center", bbox_to_anchor=(0.5, -0.2), ncol=2)
        save(fig, out / "batch_large.png")

    if st.exists():  # so sánh có kiểm soát: cùng phiên, cùng cấu hình, chỉ đổi số trạm (bench_streaming --max-stations)
        d = pd.read_csv(st)
        d = d[d.ml.astype(str) == "True"].groupby(["target_rate", "stations"], as_index=False).median(numeric_only=True)
        rate = int(d.target_rate.mode().iloc[0])
        d = d[d.target_rate == rate].sort_values("stations")
        fig, ax = plt.subplots(figsize=(8, 4.2))
        xs = list(range(len(d)))
        w = 0.3
        for j, (name, col) in enumerate([("p50", "latency_p50_ms"), ("p95", "latency_p95_ms")]):
            pos = [x + (j - 0.5) * (w + 0.04) for x in xs]
            vals = (d[col] / 1000).tolist()
            ax.bar(pos, vals, width=w, color=SERIES[j], label=name)
            for x, v in zip(pos, vals):
                ax.annotate(f"{vn(v)} s", (x, v), xytext=(0, 3), textcoords="offset points", ha="center",
                            fontsize=9, color=INK)
        ax.set_xticks(xs, [f"{int(n)} trạm" for n in d.stations])
        style(ax, f"Độ trễ streaming theo số trạm ({vn(rate, 0)} msg/s, bật điền khuyết và dự báo)",
              "Số trạm trong luồng", "Độ trễ producer → PostgreSQL (giây)",
              f"Cùng phiên đo, cùng cấu hình: cụm Spark 4 core, trigger 2 s, Kafka 6 partition. Nguồn: {st.name}")
        ax.legend(frameon=False, labelcolor=INK2, loc="upper left")
        ax.set_ylim(0, d.latency_p95_ms.max() / 1000 * 1.18)
        save(fig, out / "stream_latency_by_stations.png")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", default="benchmarks/results")
    args = ap.parse_args()
    res = Path(args.results)

    def has_rows(p: Path) -> bool:
        return p.exists() and sum(1 for _ in p.open()) > 1  # có header + ít nhất 1 dòng

    if has_rows(res / "batch_scaling.csv"):
        print("Batch:")
        plot_batch(res / "batch_scaling.csv", res)
    if has_rows(res / "stream_scaling.csv"):
        print("Streaming:")
        plot_stream(res / "stream_scaling.csv", res)
    if has_rows(res / "batch_large.csv") or has_rows(res / "stream_stations.csv"):
        print("Dữ liệu thật quy mô lớn:")
        plot_large(res, res)


if __name__ == "__main__":
    main()
