"""Dashboard giám sát chất lượng nước (Streamlit). Chỉ gọi REST API, không truy cập DB trực tiếp.

Chạy: streamlit run dashboard/app.py   (biến WQ_API_URL, mặc định http://localhost:8000)
TV2 mở rộng phần "Giám sát" (WQI, thống kê, bản đồ); TV3/TV4 bổ sung biểu đồ so sánh mô hình.
"""
from __future__ import annotations

import os

import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st

API = os.getenv("WQ_API_URL", "http://localhost:8000")
COLORS = {"raw": "#1f77b4", "imputed": "#ff7f0e", "forecast": "#2ca02c", "threshold": "#d62728"}

st.set_page_config(page_title="Water Quality Monitoring", page_icon="💧", layout="wide")


@st.cache_data(ttl=5, show_spinner=False)
def api(path: str, **params):
    r = requests.get(f"{API}{path}", params={k: v for k, v in params.items() if v is not None}, timeout=60)
    r.raise_for_status()
    return r.json()


def frame(rows, time_cols=("ts",)) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    for c in time_cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], utc=True)
    return df


try:
    meta = api("/meta")
    stations = frame(api("/stations"), ("last_ts",))
except requests.RequestException as e:
    st.error(f"Không kết nối được API tại {API}: {e}")
    st.stop()

var_info = {v["name"]: v for v in meta["variables"]}

# ------------------------------------------------------------------ Sidebar
st.sidebar.header("Bộ lọc")
station_labels = {r.station_id: f"{r.station_id} – {r['name']}" for _, r in stations.iterrows()}
station_id = st.sidebar.selectbox("Trạm", list(station_labels), format_func=station_labels.get)
parameter = st.sidebar.selectbox("Thông số", list(var_info), index=list(var_info).index("dissolved_oxygen"),
                                 format_func=lambda v: f"{var_info[v]['label']} ({var_info[v]['unit']})")
days = st.sidebar.slider("Cửa sổ thời gian (ngày dữ liệu)", 1, 30, 3)
auto = st.sidebar.toggle("Tự làm mới mỗi 10 giây", value=True)
st.sidebar.caption(f"API: {API}")


@st.fragment(run_every="10s" if auto else None)
def main_view():
    st.title("💧 Water Quality Monitoring, Imputation & Prediction")
    pm = api("/pipeline/metrics", limit=300)
    tot, lat = pm["totals"], pm["latency"]
    c = st.columns(5)
    c[0].metric("Bản ghi đã xử lý", f"{tot['measurements']:,}")
    c[1].metric("Giá trị được điền khuyết", f"{tot['imputed_rows']:,}")
    c[2].metric("Dự báo đã phát hành", f"{tot['predictions']:,}")
    c[3].metric("Cảnh báo", f"{tot['alerts']:,}")
    c[4].metric("Độ trễ p95 (batch gần nhất)", f"{lat[-1]['latency_p95_ms'] / 1000:.1f} s" if lat else "–")
    st.caption(f"Thời điểm dữ liệu mới nhất (event time): {tot['max_event_time'] or '–'}")

    tab_mon, tab_pred, tab_alert, tab_pipe = st.tabs(["Giám sát", "Dự báo", "Cảnh báo", "Pipeline"])

    with tab_mon:
        left, right = st.columns([2, 1])
        with right:
            st.subheader("Trạm quan trắc")
            st.map(stations.dropna(subset=["latitude", "longitude"]), latitude="latitude", longitude="longitude", size=20000)
            row = stations[stations.station_id == station_id].iloc[0]
            st.dataframe(pd.DataFrame({"Thông số": [var_info[v]["label"] for v in var_info],
                                       "Giá trị mới nhất": [row.get(v) for v in var_info],
                                       "Đơn vị": [var_info[v]["unit"] for v in var_info]}),
                         hide_index=True, width="stretch")
        with left:
            raw = frame(api("/measurements", station_id=station_id, days=days, source="raw"))
            imp = frame(api("/measurements", station_id=station_id, days=days, source="imputed"))
            st.subheader(f"{var_info[parameter]['label']} – {station_labels[station_id]}")
            fig = go.Figure()
            if not imp.empty:
                fig.add_trace(go.Scatter(x=imp.ts, y=imp[parameter], name="Sau điền khuyết", mode="lines",
                                         line=dict(color=COLORS["imputed"], dash="dot")))
                bit = 1 << list(var_info).index(parameter)
                filled = imp[(imp.imputed_mask & bit) > 0]
                fig.add_trace(go.Scatter(x=filled.ts, y=filled[parameter], name="Điểm được điền", mode="markers",
                                         marker=dict(color=COLORS["imputed"], size=6)))
            if not raw.empty:
                fig.add_trace(go.Scatter(x=raw.ts, y=raw[parameter], name="Quan trắc (đã QC)", mode="lines",
                                         line=dict(color=COLORS["raw"])))
            if parameter in meta["forecast_targets"]:
                pred = frame(api("/predictions/latest", station_id=station_id, target=parameter),
                             ("issued_at", "target_ts"))
                for model, g in (pred.groupby("model") if not pred.empty else []):
                    fig.add_trace(go.Scatter(x=g.target_ts, y=g.y_pred, name=f"Dự báo ({model})",
                                             mode="lines+markers", line=dict(color=COLORS["forecast"])))
            for rule in meta["alert_rules"]:
                if rule["parameter"] == parameter:
                    fig.add_hline(y=rule["threshold"], line=dict(color=COLORS["threshold"], dash="dash", width=1),
                                  annotation_text=rule["rule"], annotation_position="top left")
            fig.update_layout(height=420, margin=dict(l=10, r=10, t=30, b=10), hovermode="x unified",
                              yaxis_title=var_info[parameter]["unit"], legend=dict(orientation="h", y=-0.15))
            st.plotly_chart(fig, width="stretch")
            hourly = frame(api("/measurements/hourly", station_id=station_id, parameter=parameter, days=days),
                           ("window_start", "window_end"))
            if not hourly.empty:
                st.caption("Tổng hợp theo giờ (Spark window 1h + watermark): trung bình và tỉ lệ thiếu")
                fig2 = go.Figure([
                    go.Scatter(x=hourly.window_start, y=hourly.avg_value, name="Trung bình giờ", mode="lines"),
                    go.Bar(x=hourly.window_start, y=hourly.missing_rate, name="Tỉ lệ thiếu", yaxis="y2", opacity=0.4)])
                fig2.update_layout(height=260, margin=dict(l=10, r=10, t=10, b=10),
                                   yaxis2=dict(overlaying="y", side="right", range=[0, 1], title="missing"),
                                   legend=dict(orientation="h", y=-0.25))
                st.plotly_chart(fig2, width="stretch")

    with tab_pred:
        target = meta["forecast_targets"][0]
        horizons = meta["forecast_horizons_min"]
        h = st.select_slider("Horizon (phút)", horizons, value=horizons[0])
        pr = frame(api("/predictions", station_id=station_id, target=target, horizon_min=h, days=days),
                   ("target_ts", "issued_at"))
        if pr.empty:
            st.info("Chưa có dự báo cho trạm này.")
        else:
            fig = go.Figure()
            obs = pr.dropna(subset=["y_true"]).drop_duplicates("target_ts")
            fig.add_trace(go.Scatter(x=obs.target_ts, y=obs.y_true, name="Thực tế", mode="lines",
                                     line=dict(color=COLORS["raw"])))
            for model, g in pr.groupby("model"):
                fig.add_trace(go.Scatter(x=g.target_ts, y=g.y_pred, name=f"Dự báo {model} (+{h} phút)",
                                         mode="markers", marker=dict(color=COLORS["forecast"], size=6)))
            fig.update_layout(height=380, margin=dict(l=10, r=10, t=30, b=10), hovermode="x unified")
            st.plotly_chart(fig, width="stretch")
        acc = pd.DataFrame(api("/predictions/accuracy", target=target, station_id=station_id))
        st.subheader("Độ chính xác online (so với giá trị thực đến sau)")
        st.dataframe(acc, hide_index=True, width="stretch")
        if st.button("Dự báo ngay (on-demand qua API)"):
            res = api(f"/stations/{station_id}/forecast")
            st.json(res, expanded=False)

    with tab_alert:
        al = frame(api("/alerts", station_id=None, limit=500), ("ts", "created_at"))
        if al.empty:
            st.success("Không có cảnh báo.")
        else:
            st.dataframe(al[["ts", "station_id", "parameter", "value", "threshold", "rule", "severity", "message"]],
                         hide_index=True, width="stretch")

    with tab_pipe:
        latency = frame(lat, ("recorded_at", "max_event_time"))
        prog = frame(pm["progress"], ("recorded_at", "watermark"))
        if latency.empty:
            st.info("Chưa có số liệu pipeline.")
        else:
            st.subheader("Độ trễ end-to-end (producer → PostgreSQL)")
            fig = go.Figure([go.Scatter(x=latency.recorded_at, y=latency[c] / 1000, name=c, mode="lines")
                             for c in ["latency_p50_ms", "latency_p95_ms"]])
            fig.update_layout(height=280, yaxis_title="giây", margin=dict(l=10, r=10, t=10, b=10))
            st.plotly_chart(fig, width="stretch")
        if not prog.empty:
            st.subheader("Throughput theo micro-batch (StreamingQueryListener)")
            fig = go.Figure([go.Scatter(x=g.recorded_at, y=g.processed_rows_per_sec, name=q, mode="lines+markers")
                             for q, g in prog.groupby("query_name")])
            fig.update_layout(height=280, yaxis_title="rows/s", margin=dict(l=10, r=10, t=10, b=10))
            st.plotly_chart(fig, width="stretch")


main_view()
