"""WaterScope — monitoring, on-demand sensor forecasts and station experiments."""
from __future__ import annotations

from html import escape
import json
import os
from pathlib import Path
from urllib.parse import urlsplit

import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st

from dashboard.design import COLORS, PAGES, brand, chart, hero, icon, metrics, panel_title, station_map, stylesheet, timestamp
from dashboard.lab_view import show_lab
from wq.config import get_settings

API = os.getenv("WQ_API_URL", "http://localhost:8000")
st.set_page_config(page_title="WaterScope · Quan trắc USGS", page_icon="💧", layout="wide")
st.markdown(stylesheet(), unsafe_allow_html=True)
brand()
page = st.sidebar.radio("Điều hướng", list(PAGES), label_visibility="collapsed", key="navigation", width="stretch")


@st.cache_data(ttl=5, show_spinner=False)
def api(path, **params):
    # Compose resolves its own service name inside the network, including on hosts with an HTTP proxy.
    proxies = {"no_proxy": "api"} if urlsplit(API).hostname == "api" else None
    response = requests.get(API + path, params=params, proxies=proxies, timeout=(4, 60))
    response.raise_for_status()
    return response.json()


def frame(rows, *time_cols):
    df = pd.DataFrame(rows)
    for col in time_cols:
        if col in df:
            df[col] = pd.to_datetime(df[col], utc=True)
    return df


def number(value):
    return f"{value:,}".replace(",", ".")


def draw(fig, height, unit=None):
    st.plotly_chart(chart(fig, height, unit), theme=None, width="stretch",
                    config={"displaylogo": False, "scrollZoom": False})


if page == "Mô hình":
    catalog = Path(get_settings().stations_file)
    if not catalog.is_file():
        catalog = Path("config/stations_large.json")
    catalog_rows = json.loads(catalog.read_text(encoding="utf-8"))["stations"]
    hero(page, "So sánh sai số, huấn luyện theo trạm và xuất kết quả.", "Silver · USGS")
    show_lab({row["station_id"]: f"{row['station_id']} · {row['name']}" for row in catalog_rows},
             st.session_state.get("selected_station", st.query_params.get("station")))
    st.stop()

try:
    meta = api("/meta")
    stations = frame(api("/stations"), "last_ts")
except requests.RequestException:
    hero(page, "Dịch vụ quan trắc", "API chưa kết nối")
    st.warning("Chưa kết nối được API quan trắc. Bạn vẫn có thể mở Mô hình để xem benchmark và huấn luyện.")
    if st.button("Thử kết nối lại"):
        st.rerun()
    st.stop()

info = {v["name"]: v for v in meta["variables"]}
has_data = stations["last_ts"].notna() if not stations.empty else pd.Series(dtype=bool)
station_id = None
auto = st.sidebar.toggle("Tự làm mới mỗi 10 giây", value=True, key="auto_refresh")

if page in ("Giám sát", "Dự báo"):
    if stations.empty:
        hero(page, "Mạng lưới quan trắc USGS", "API kết nối", True)
        st.info("Danh mục trạm đang trống. Hãy kiểm tra bước nạp danh mục trong stream-processor.")
        st.stop()
    st.sidebar.markdown(f'<div class="wq-filter-title">{icon("adjustments-horizontal")} Bộ lọc trạm</div>',
                        unsafe_allow_html=True)
    preferred = st.session_state.get("selected_station", st.query_params.get("station"))
    only_active = st.sidebar.toggle("Chỉ trạm có dữ liệu", value=False, key="only_active")
    listed = stations[has_data] if only_active else pd.concat([stations[has_data], stations[~has_data]])
    labels = {r.station_id: f"{r.station_id} · {r['name']}" for _, r in listed.iterrows()}
    ids = list(labels)
    station_id = st.sidebar.selectbox("Trạm quan trắc", ids, format_func=labels.get,
                                      index=ids.index(preferred) if preferred in ids else 0)
    if not station_id:
        st.info("Bộ lọc hiện tại chưa có trạm. Tắt “Chỉ trạm có dữ liệu” để xem danh mục.")
        st.stop()
    st.session_state["selected_station"] = station_id
    station = stations.loc[stations.station_id == station_id].iloc[0]
    param_keys = list(info)
    default_param = st.session_state.get("selected_param", st.query_params.get("param", "dissolved_oxygen"))
    parameter = default_param if default_param in info else param_keys[0]
    if page == "Giám sát":
        parameter = st.sidebar.selectbox("Thông số", param_keys, index=param_keys.index(parameter),
                                         format_func=lambda v: info[v]["label"])
        st.session_state["selected_param"] = parameter
    try:
        default_days = min(30, max(1, int(st.session_state.get("selected_days", st.query_params.get("days", 3)))))
    except ValueError:
        default_days = 3
    days = st.sidebar.slider("Số ngày dữ liệu", 1, 30, default_days)
    st.session_state["selected_days"] = days
    st.sidebar.caption(f"{int(has_data.sum())} / {len(stations)} trạm đã có dữ liệu")
    page_detail = str(station["name"])
else:
    page_detail = {"Cảnh báo": "Các lần vượt ngưỡng ghi nhận trên toàn mạng lưới.",
                   "Pipeline": "Theo dõi độ trễ và thông lượng các micro-batch của Spark."}[page]

hero(page, page_detail, "API kết nối", True, station_id)


def show_monitor(stations, station):
    raw = frame(api("/measurements", station_id=station_id, days=days, source="raw"), "ts")
    imp = frame(api("/measurements", station_id=station_id, days=days, source="imputed"), "ts")
    pred = pd.DataFrame()
    if parameter in meta["forecast_targets"]:
        pred = frame(api("/predictions/latest", station_id=station_id, target=parameter), "issued_at", "target_ts")
    left, right = st.columns([1.8, 1], gap="medium")
    with left.container(border=True, key="card_trend"):
        label, unit = info[parameter]["label"], info[parameter]["unit"]
        panel_title("Diễn biến quan trắc", f"{label} · {days} ngày dữ liệu · UTC")
        if raw.empty and imp.empty and pred.empty:
            st.info("Chưa có số đo trong cửa sổ đã chọn.")
        else:
            fig = go.Figure()
            if not raw.empty:
                grid = pd.date_range(raw.ts.min(), raw.ts.max(), freq=f"{meta['freq_minutes']}min")
                series = raw.set_index("ts")[parameter].reindex(grid)
                fig.add_trace(go.Scatter(x=grid, y=series, name="Quan trắc", mode="lines",
                                         line=dict(color=COLORS["raw"], width=2.8), connectgaps=False))
            if not imp.empty:
                bit = 1 << list(info).index(parameter)
                filled = imp[(imp.imputed_mask & bit) > 0]
                fig.add_trace(go.Scatter(x=imp.ts, y=imp[parameter], name="Sau điền khuyết", mode="lines",
                                         line=dict(color=COLORS["imputed"], dash="dot", width=1.3)))
                if not filled.empty:
                    fig.add_trace(go.Scatter(x=filled.ts, y=filled[parameter], name="Điểm được điền", mode="markers",
                                             marker=dict(color=COLORS["imputed"], size=6)))
            if not pred.empty:
                for model, group in pred.groupby("model"):
                    fig.add_trace(go.Scatter(x=group.target_ts, y=group.y_pred, name=f"Dự báo {model}",
                                             mode="lines+markers", line=dict(color=COLORS["forecast"])))
            for rule in meta["alert_rules"]:
                if rule["parameter"] == parameter:
                    fig.add_hline(y=rule["threshold"], line=dict(color=COLORS["threshold"], dash="dot", width=1),
                                  annotation_text=rule["rule"], annotation_position="top left")
            draw(fig, 325, unit)
    with right.container(border=True, key="card_map"):
        panel_title("Mạng lưới trạm", f"{number(len(stations))} trạm trong danh mục")
        station_map(stations, station_id)
        st.markdown('<div class="wq-map-key"><span><i style="background:#f57f34"></i>Đang chọn</span>'
                    '<span><i style="background:#116ee2"></i>Có dữ liệu</span>'
                    '<span><i style="background:#93a8b8"></i>Chưa có dữ liệu</span></div>', unsafe_allow_html=True)
        active = int(stations.last_ts.notna().sum())
        st.progress(active / len(stations), text=f"{active} / {len(stations)} trạm có quan trắc")
    with st.container(border=True, key="card_latest"):
        panel_title("Số đo gần nhất", f"{station_id} · {timestamp(station.last_ts)}")
        readings = []
        for var, item in info.items():
            value = station.get(var)
            reading = f"{value:.2f} {escape(item['unit'])}" if pd.notna(value) else "—"
            readings.append(f'<div><span>{escape(item["label"])}</span><b>{reading}</b></div>')
        st.markdown('<div class="wq-sensors">' + "".join(readings) + "</div>", unsafe_allow_html=True)
    hourly = frame(api("/measurements/hourly", station_id=station_id, parameter=parameter, days=days), "window_start")
    if not hourly.empty:
        avg, missing = st.columns(2)
        with avg.container(border=True, key="card_avg"):
            panel_title("Trung bình theo giờ", label)
            draw(go.Figure(go.Scatter(x=hourly.window_start, y=hourly.avg_value, mode="lines",
                                      line=dict(color=COLORS["raw"]))), 205, unit)
        with missing.container(border=True, key="card_gaps"):
            panel_title("Tỷ lệ thiếu theo giờ", label)
            fig = go.Figure(go.Bar(x=hourly.window_start, y=hourly.missing_rate*100, marker_color=COLORS["imputed"]))
            fig.update_yaxes(range=[0, 100])
            draw(fig, 205, "%")


def show_forecast(station):
    scope = api(f"/stations/{station_id}/forecast-options")
    observed = scope["last_observed_at"]
    available = [v for v in meta["on_demand_targets"] if v in observed]
    with st.container(border=True, key="card_demand"):
        panel_title("Dự báo tại trạm", f"Quan trắc mới nhất của trạm · {timestamp(station.last_ts)}")
        if not available:
            st.info(f"Trạm chưa có thông số quan trắc trong {scope['history_hours']} giờ dữ liệu gần nhất.")
            return
        left, right = st.columns(2)
        preferred = st.session_state.get("selected_param", st.query_params.get("param", "dissolved_oxygen"))
        target = left.selectbox("Thông số dự báo", available, format_func=lambda v: info[v]["label"],
                                index=available.index(preferred) if preferred in available else 0)
        streaming = target in meta["forecast_targets"]
        options = ([None] if streaming else []) + ["seasonal_naive", "persistence"]
        labels = {None: "Mô hình đang phục vụ", "seasonal_naive": "Cùng thời điểm hôm trước",
                  "persistence": "Giữ giá trị gần nhất"}
        model = right.selectbox("Phương pháp dự báo", options, format_func=labels.get)
        unit = info[target]["unit"]
        st.caption(f"{info[target]['label']} · Quan trắc cuối lúc {timestamp(observed[target])}")
        selection = (station_id, target, model)
        if st.button("Dự báo ngay", type="primary"):
            with st.spinner("Đang tính dự báo…"):
                result = api(f"/stations/{station_id}/forecast", target=target, model=model)
            st.session_state["station_forecast"] = (selection, result)
        saved, result = st.session_state.get("station_forecast", (None, None))
        live = pd.DataFrame()
        if saved == selection:
            live = frame(result["predictions"], "issued_at", "target_ts")
            st.caption(f"{result['forecaster']} · {result['imputer']} · {result['history_points']} điểm lịch sử")
            if live.empty:
                st.info("Mô hình đã chọn chưa đủ lịch sử hoặc hệ số tại trạm. Bạn có thể thử phương pháp giữ giá trị gần nhất.")
            else:
                tiles = [("clock", f"Sau {int(row.horizon_min)//60} giờ", f"{row.y_pred:.3f} {unit}",
                          timestamp(row.target_ts), "teal") for row in live.sort_values("horizon_min").itertuples()]
                metrics(tiles)
    if not streaming:
        with st.container(border=True, key="card_sensor_history"):
            panel_title("Quan trắc và dự báo theo yêu cầu", f"{info[target]['label']} · {days} ngày dữ liệu")
            raw = frame(api("/measurements", station_id=station_id, days=days, source="raw"), "ts")
            fig = go.Figure()
            if not raw.empty:
                series = raw.set_index("ts")[target].asfreq(f"{meta['freq_minutes']}min")
                fig.add_trace(go.Scatter(x=series.index, y=series, name="Quan trắc", mode="lines+markers"))
            if not live.empty:
                fig.add_trace(go.Scatter(x=live.target_ts, y=live.y_pred, name=result["forecaster"], mode="markers"))
            draw(fig, 285, unit)
        return
    with st.container(border=True, key="card_forecast_history"):
        panel_title("Dự báo và quan trắc", f"{station_id} · {days} ngày dữ liệu")
        horizons = meta["forecast_horizons_min"]
        h = st.radio("Khoảng dự báo", horizons, horizontal=True, format_func=lambda m: f"{m//60} giờ")
        history = frame(api("/predictions", station_id=station_id, target=target, horizon_min=h, days=days),
                        "issued_at", "target_ts")
        if history.empty:
            st.info("Chưa có dự báo do luồng streaming ghi trong khoảng đã chọn.")
        else:
            fig = go.Figure()
            obs = history.dropna(subset=["y_true"]).drop_duplicates("target_ts")
            fig.add_trace(go.Scatter(x=obs.target_ts, y=obs.y_true, name="Quan trắc", mode="lines+markers"))
            for model, g in history.groupby("model"):
                fig.add_trace(go.Scatter(x=g.target_ts, y=g.y_pred, name=model, mode="markers"))
            draw(fig, 285, "mg/L")
    with st.container(border=True, key="card_accuracy"):
        panel_title("Sai số dự báo trực tuyến", "So với quan trắc nhận được sau khi phát dự báo")
        scores = pd.DataFrame(api("/predictions/accuracy", station_id=station_id, target=target))
        if scores.empty:
            st.info("Chưa có cặp dự báo và quan trắc để chấm.")
        else:
            st.dataframe(scores, hide_index=True, width="stretch")


def show_pipeline(pm):
    latency = frame(pm["latency"], "recorded_at")
    progress = frame(pm["progress"], "recorded_at")
    with st.container(border=True, key="card_latency"):
        panel_title("Độ trễ từ producer đến PostgreSQL", "Theo thời điểm ghi nhận micro-batch · UTC")
        if latency.empty:
            st.info("Chưa có số đo độ trễ.")
        else:
            fig = go.Figure([go.Scatter(x=latency.recorded_at, y=latency[col]/1000, name=name, mode="lines+markers")
                             for col, name in [("latency_p50_ms", "p50"), ("latency_p95_ms", "p95")]])
            draw(fig, 240, "giây")
    with st.container(border=True, key="card_throughput"):
        panel_title("Thông lượng theo truy vấn Spark")
        if progress.empty:
            st.info("Chưa có micro-batch được ghi nhận.")
        else:
            fig = go.Figure([go.Scatter(x=g.recorded_at, y=g.processed_rows_per_sec, name=name, mode="lines+markers")
                             for name, g in progress.groupby("query_name")])
            draw(fig, 240, "dòng/s")


@st.fragment(run_every="10s" if auto else None)
def main_view():
    try:
        pm = api("/pipeline/metrics", limit=300)
        totals, latency = pm["totals"], pm["latency"]
        p95 = latency[-1].get("latency_p95_ms") if latency else None
        metrics([
            ("database", "Bản ghi quan trắc", number(totals["measurements"]), "Toàn mạng lưới", "blue"),
            ("stack-2", "Sau điền khuyết", number(totals["imputed_rows"]), "Dòng đã ghi", "teal"),
            ("chart-line", "Dự báo đã phát", number(totals["predictions"]), "Từ luồng streaming", "violet"),
            ("bell", "Cảnh báo", number(totals["alerts"]), "Lượt vượt ngưỡng", "amber"),
        ])
        if page in ("Giám sát", "Dự báo"):
            current_stations = frame(api("/stations"), "last_ts")
            selected = current_stations.loc[current_stations.get("station_id", pd.Series(dtype=str)) == station_id]
            if selected.empty:
                st.info("Trạm vừa thay đổi trong danh mục. Hãy chọn lại trạm.")
            elif page == "Giám sát":
                show_monitor(current_stations, selected.iloc[0])
            else:
                show_forecast(selected.iloc[0])
        elif page == "Pipeline":
            st.caption(f"Quan trắc mới nhất · {timestamp(totals['max_event_time'])}   |   "
                       + (f"Độ trễ p95 batch gần nhất · {p95/1000:.2f} s" if p95 is not None else "Chưa có độ trễ p95"))
            show_pipeline(pm)
        else:
            with st.container(border=True, key="card_alerts"):
                panel_title("Nhật ký cảnh báo", "Tối đa 500 cảnh báo mới nhất trên toàn mạng lưới")
                alerts = frame(api("/alerts", limit=500), "ts", "created_at")
                if alerts.empty:
                    st.info("Chưa ghi nhận cảnh báo.")
                else:
                    st.dataframe(alerts[["ts", "station_id", "parameter", "value", "threshold", "rule", "severity", "message"]],
                                 width="stretch", hide_index=True)
    except requests.RequestException as error:
        st.error(f"Chưa lấy được dữ liệu từ API. {error}")


main_view()
