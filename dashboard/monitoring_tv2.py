"""Tab "Chất lượng nước" của dashboard (TV2.2d): WQI theo trạm và theo thời gian, bản đồ trạm theo mức WQI,
cảnh báo gần nhất, heatmap tỉ lệ thiếu.

Chỉ dùng REST API có sẵn (``/stations``, ``/measurements``, ``/alerts``) như phần còn lại của dashboard. WQI tính bằng
``wq.wqi`` (CCME WQI theo ngày) trên dữ liệu lấy từ API. Ngưỡng nền theo trạm (p99 split train) lấy từ endpoint
``/wqi/baselines`` nếu API có; chưa có thì đọc ``docs/report/tables/tv2_site_baselines.csv`` (sinh bởi
``python -m wq.wqi``). Các hàm tính toán không phụ thuộc Streamlit để test được.
"""
from __future__ import annotations

from pathlib import Path
from typing import Callable

import pandas as pd

from wq import contracts
from wq.wqi import WQI_CLASSES, wqi_daily_pandas

BASELINES_CSV = Path(__file__).resolve().parents[1] / "docs" / "report" / "tables" / "tv2_site_baselines.csv"

WQI_COLORS: dict[str, str] = {"Excellent": "#1a9850", "Good": "#91cf60", "Fair": "#fee08b",
                              "Marginal": "#fc8d59", "Poor": "#d73027"}
NO_WQI_COLOR = "#9AA9AE"
WQI_LABELS_VI: dict[str, str] = {"Excellent": "Rất tốt", "Good": "Tốt", "Fair": "Trung bình",
                                 "Marginal": "Kém", "Poor": "Rất kém"}


# ---------------------------------------------------------------------------------------------- tính toán
def load_baselines(api: Callable | None = None, csv_path: Path = BASELINES_CSV) -> tuple[pd.DataFrame | None, str]:
    """Ngưỡng nền (station_id, variable, p99). Ưu tiên API, sau đó CSV trong repo. Trả về (bảng, nguồn)."""
    if api is not None:
        try:
            rows = api("/wqi/baselines")
            if rows:
                return pd.DataFrame(rows), "API /wqi/baselines"
        except Exception:  # noqa: BLE001 - endpoint chưa có => dùng CSV
            pass
    if csv_path.exists():
        return pd.read_csv(csv_path), csv_path.name
    return None, "không có"


def to_grid(df: pd.DataFrame, freq_minutes: int = contracts.FREQ_MINUTES,
            start: pd.Timestamp | None = None, end: pd.Timestamp | None = None) -> pd.DataFrame:
    """Đưa chuỗi của MỘT trạm về lưới đều: mốc không có bản ghi thành dòng NaN (để đếm thiếu đúng)."""
    if df.empty:
        return df
    d = df.copy()
    d["ts"] = pd.to_datetime(d["ts"], utc=True)
    d = d.drop_duplicates("ts").set_index("ts").sort_index()
    grid = pd.date_range(start or d.index.min(), end or d.index.max(), freq=f"{freq_minutes}min")
    out = d.reindex(grid)
    out.index.name = "ts"
    out["station_id"] = df["station_id"].iloc[0]
    return out.reset_index()


def missing_matrix(frames: dict[str, pd.DataFrame], variables: list[str] = contracts.VARIABLE_NAMES,
                   freq_minutes: int = contracts.FREQ_MINUTES) -> pd.DataFrame:
    """% thiếu theo trạm (hàng) x biến (cột) trên lưới 15 phút trong cửa sổ đã lấy."""
    rows = {}
    for sid, df in frames.items():
        g = to_grid(df, freq_minutes)
        if g.empty:
            continue
        rows[sid] = {v: 100.0 * g[v].isna().mean() if v in g else 100.0 for v in variables}
    return pd.DataFrame.from_dict(rows, orient="index", columns=variables).sort_index()


MIN_DAY_RECORDS = 48   # ngày có >= 12 giờ dữ liệu mới coi là đủ để báo "WQI ngày gần nhất"


def daily_wqi(frames: dict[str, pd.DataFrame], baselines: pd.DataFrame | None) -> pd.DataFrame:
    """CCME WQI mỗi trạm mỗi ngày (UTC) bằng ``wq.wqi.wqi_daily_pandas``, kèm số bản ghi của ngày (``n_records``)."""
    parts = []
    for sid, df in frames.items():
        if df.empty:
            continue
        d = df.copy()
        d["ts"] = pd.to_datetime(d["ts"], utc=True)
        for v in contracts.VARIABLE_NAMES:
            d[v] = pd.to_numeric(d.get(v), errors="coerce")
        w = wqi_daily_pandas(d, baselines)
        n = d.groupby(d["ts"].dt.date).size().rename("n_records")
        parts.append(w.merge(n, left_on="date", right_index=True, how="left"))
    if not parts:
        return pd.DataFrame(columns=["station_id", "date", "wqi", "wqi_class", "f1", "f2", "f3", "n_variables",
                                     "n_records"])
    return pd.concat(parts, ignore_index=True)


def latest_wqi(daily: pd.DataFrame, min_records: int = MIN_DAY_RECORDS) -> pd.DataFrame:
    """Ngày gần nhất có WQI và đủ dữ liệu (>= ``min_records`` bản ghi) của mỗi trạm; ngày đang phát dở bị bỏ qua."""
    ok = daily.dropna(subset=["wqi"])
    if "n_records" in ok:
        ok = ok[ok["n_records"] >= min_records]
    if ok.empty:
        return ok
    return ok.sort_values("date").groupby("station_id").tail(1).sort_values("station_id").reset_index(drop=True)


def station_wqi_table(stations: pd.DataFrame, latest: pd.DataFrame) -> pd.DataFrame:
    """Bảng trạm + WQI ngày gần nhất + màu theo mức (trạm chưa tính được WQI: màu xám)."""
    cols = ["station_id", "name", "latitude", "longitude", "alerts_24h"]
    t = stations[[c for c in cols if c in stations]].merge(
        latest[["station_id", "date", "wqi", "wqi_class"]], on="station_id", how="left")
    t["color"] = t["wqi_class"].map(WQI_COLORS).fillna(NO_WQI_COLOR)
    t["mức"] = t["wqi_class"].map(WQI_LABELS_VI)
    return t


def station_map(table: pd.DataFrame):
    """Bản đồ trạm (Plotly Scattergeo, nền ranh giới bang của Mỹ: không cần tải ảnh bản đồ từ máy chủ ngoài)."""
    import plotly.graph_objects as go

    fig = go.Figure()
    groups = [(c, WQI_COLORS[c], WQI_LABELS_VI[c]) for _, c in WQI_CLASSES] + [(None, NO_WQI_COLOR, "Chưa đủ dữ liệu")]
    for cls, color, label in groups:
        g = table[table["wqi_class"].isna()] if cls is None else table[table["wqi_class"] == cls]
        if g.empty:
            continue
        text = [f"{r.station_id}<br>{r.name}<br>WQI {r.wqi:.1f}" if pd.notna(r.wqi) else f"{r.station_id}<br>{r.name}"
                for r in g.itertuples()]
        fig.add_trace(go.Scattergeo(lon=g["longitude"], lat=g["latitude"], text=text, hoverinfo="text",
                                    mode="markers", name=label,
                                    marker=dict(size=13, color=color, line=dict(width=1, color="#334155"))))
    fig.update_geos(scope="usa", showland=True, landcolor="#f1f5f9", subunitcolor="#cbd5e1")
    fig.update_layout(height=380, margin=dict(l=0, r=0, t=0, b=0),
                      legend=dict(orientation="h", y=-0.02, font=dict(size=11)))
    return fig


# ---------------------------------------------------------------------------------------------- Streamlit
def render(api: Callable, stations: pd.DataFrame, var_info: dict, days: int, station_ids: list[str],
           freq_minutes: int = contracts.FREQ_MINUTES) -> None:
    import plotly.graph_objects as go
    import streamlit as st

    st.subheader("Chỉ số chất lượng nước (CCME WQI) và tình trạng dữ liệu")
    if not station_ids:
        st.info("Chưa có trạm nào có dữ liệu.")
        return
    frames = {sid: pd.DataFrame(api("/measurements", station_id=sid, days=days, source="raw", limit=200000))
              for sid in station_ids}
    baselines, source = load_baselines(api)
    daily = daily_wqi(frames, baselines)
    latest = latest_wqi(daily)
    table = station_wqi_table(stations[stations.station_id.isin(station_ids)], latest)

    # --- hàng chỉ số tóm tắt
    c = st.columns(len(WQI_CLASSES) + 1)
    for i, (_, cls) in enumerate(WQI_CLASSES):
        c[i].metric(f"{WQI_LABELS_VI[cls]} ({cls})", int((table["wqi_class"] == cls).sum()))
    c[-1].metric("Chưa đủ dữ liệu", int(table["wqi"].isna().sum()))
    st.caption(f"WQI của ngày gần nhất có ≥ 12 giờ dữ liệu và ≥ 4 biến. Ngưỡng: DO ≥ 5 mg/L, pH 6,5–9,0 (U.S. EPA 1986); nhiệt độ, "
               f"độ dẫn điện, độ đục ≤ p99 lịch sử của trạm (nguồn ngưỡng nền: {source}).")

    left, right = st.columns([3, 2])
    with left:
        st.markdown("**WQI theo ngày**")
        fig = go.Figure()
        bands = [(lo, hi, cls) for (lo, cls), hi in zip(WQI_CLASSES, [100] + [x for x, _ in WQI_CLASSES[:-1]])]
        for lo, hi, cls in bands:
            fig.add_hrect(y0=lo, y1=hi, fillcolor=WQI_COLORS[cls], opacity=0.12, line_width=0)
        names = dict(zip(table.station_id, table.get("name", table.station_id)))
        for sid, g in daily.dropna(subset=["wqi"]).groupby("station_id"):
            fig.add_trace(go.Scatter(x=g["date"], y=g["wqi"], mode="lines+markers", name=f"{sid} {names.get(sid, '')}"[:40],
                                     hovertemplate="%{x}<br>WQI %{y:.1f}<extra>" + sid + "</extra>"))
        fig.update_layout(height=360, margin=dict(l=10, r=10, t=10, b=10), yaxis=dict(range=[0, 101], title="WQI"),
                          legend=dict(font=dict(size=10)))
        st.plotly_chart(fig, width="stretch")
    with right:
        st.markdown("**Trạm theo mức WQI**")
        pts = table.dropna(subset=["latitude", "longitude"])
        if not pts.empty:
            st.plotly_chart(station_map(pts), width="stretch")

    show = table[["station_id", "name", "date", "wqi", "mức", "alerts_24h"]].rename(
        columns={"station_id": "Trạm", "name": "Tên", "date": "Ngày", "wqi": "WQI", "mức": "Mức",
                 "alerts_24h": "Cảnh báo 24h"})
    st.dataframe(show.sort_values("WQI", na_position="last"), hide_index=True, width="stretch")

    left, right = st.columns([3, 2])
    with left:
        st.markdown(f"**Tỉ lệ thiếu dữ liệu (%) trong {days} ngày gần nhất**")
        miss = missing_matrix(frames, list(var_info), freq_minutes)
        if not miss.empty:
            fig = go.Figure(go.Heatmap(
                z=miss.values, x=[var_info[v]["label"] for v in miss.columns], y=list(miss.index),
                zmin=0, zmax=100, colorscale="Blues", text=miss.round(1).values, texttemplate="%{text}",
                hovertemplate="%{y}<br>%{x}: %{z:.1f}%<extra></extra>", colorbar=dict(title="%")))
            fig.update_layout(height=max(260, 28 * len(miss) + 80), margin=dict(l=10, r=10, t=10, b=10),
                              yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig, width="stretch")
    with right:
        st.markdown("**Cảnh báo gần nhất**")
        al = pd.DataFrame(api("/alerts", limit=20))
        if al.empty:
            st.success("Không có cảnh báo.")
        else:
            al["ts"] = pd.to_datetime(al["ts"], utc=True).dt.strftime("%Y-%m-%d %H:%M")
            al["severity"] = al["severity"].map({"critical": "🔴", "warning": "🟠"}).fillna(al["severity"])
            st.dataframe(al[["severity", "ts", "station_id", "rule", "value"]].rename(
                columns={"severity": "", "ts": "Thời điểm (UTC)", "station_id": "Trạm", "rule": "Luật",
                         "value": "Giá trị"}), hide_index=True, width="stretch", height=360)
