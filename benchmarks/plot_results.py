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
    ax.legend(frameon=False, labelcolor=INK2, loc="upper left")
    ax.margins(x=0.15)
    save(fig, out / "batch_throughput_vs_size.png")

    # 2) Speedup theo số core (quy mô lớn nhất có đủ 1 core)
    loc = df[df["mode"] == "local"]
    scale = loc.groupby("scale").cores.nunique().idxmax() if not loc.empty else None
    if scale is not None:
        g = loc[loc.scale == scale].sort_values("cores")
        base = g[g.cores == g.cores.min()].total_s.iloc[0]
        g = g.assign(speedup=base / g.total_s)
        fig, ax = plt.subplots(figsize=(6.5, 4.2))
        ax.bar(g.cores.astype(str), g.speedup, width=0.45, color=SERIES[0])
        ax.plot(g.cores.astype(str), g.cores / g.cores.min(), color=MUTED, linewidth=1, label="tuyến tính lý tưởng")
        for xc, yv, ts in zip(g.cores.astype(str), g.speedup, g.total_s):
            ax.annotate(f"{yv:.2f}×\n({ts:.0f}s)", (xc, yv), xytext=(0, 4), textcoords="offset points",
                        ha="center", va="bottom", fontsize=9, color=INK)
        style(ax, f"Speedup ETL theo số core (quy mô ×{scale})", "Số core Spark (local)", "Speedup so với 1 core", note)
        ax.legend(frameon=False, labelcolor=INK2, loc="upper left")
        ax.set_ylim(0, max(g.cores / g.cores.min()) * 1.25)
        save(fig, out / "batch_speedup_vs_cores.png")

    # 3) Local vs cluster cùng số core (quy mô x1)
    lc = df[(df.engine == "spark") & (df.scale == df.scale.min())]
    if (lc["mode"] == "cluster").any():
        piv = lc.pivot_table(index="cores", columns="mode", values="total_s")
        piv = piv.dropna()
        fig, ax = plt.subplots(figsize=(6.5, 4.2))
        xs = range(len(piv))
        w = 0.36
        for j, mode in enumerate(["local", "cluster"]):
            ax.bar([x + (j - 0.5) * (w + 0.04) for x in xs], piv[mode], width=w, color=SERIES[j], label=mode)
            for x, v in zip(xs, piv[mode]):
                ax.annotate(f"{v:.0f}s", (x + (j - 0.5) * (w + 0.04), v), xytext=(0, 3), textcoords="offset points",
                            ha="center", fontsize=9, color=INK)
        ax.set_xticks(list(xs), [f"{c} core" for c in piv.index])
        style(ax, "Local vs cluster (2 worker trên cùng máy), quy mô ×1", "", "Thời gian ETL (giây)", note)
        ax.legend(frameon=False, labelcolor=INK2)
        save(fig, out / "batch_local_vs_cluster.png")


def plot_stream(csv: Path, out: Path) -> None:
    df = pd.read_csv(csv)
    note = f"Máy đo: {int(df.host_cpus.iloc[0])} vCPU, Docker; Kafka 6 partition, trigger 2s. Nguồn: {csv.name}"
    for (mode, cores, ml), g in df.groupby(["mode", "cores", "ml"]):
        g = g.sort_values("target_rate")
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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", default="benchmarks/results")
    args = ap.parse_args()
    res = Path(args.results)
    if (res / "batch_scaling.csv").exists():
        print("Batch:")
        plot_batch(res / "batch_scaling.csv", res)
    if (res / "stream_scaling.csv").exists():
        print("Streaming:")
        plot_stream(res / "stream_scaling.csv", res)


if __name__ == "__main__":
    main()
