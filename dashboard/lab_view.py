"""Streamlit views for recorded and interactive model experiments."""
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard.design import MODEL_COLORS, chart
from dashboard.model_lab import train_station
from wq import contracts
from wq.config import get_settings


def show_lab(station_labels, selected_station=None):
    recorded, forecast, training = st.tabs(["Điền khuyết 235 trạm", "Benchmark dự báo", "Huấn luyện theo trạm"])
    with recorded, st.container(border=True, key="card_imputation_lab"):
        st.subheader("Benchmark điền khuyết trên 235 trạm")
        paths = [p for p in (Path("data/report/imputation-test.csv"), Path("data/report/imputation-valid.csv"))
                 if p.is_file()]
        if paths:
            path = st.selectbox("Tập kết quả điền khuyết", paths, format_func=lambda p: p.stem)
            scores = pd.read_csv(path)
            a, b, c = st.columns(3, vertical_alignment="bottom")
            variables = list(scores["variable"].unique())
            variable = a.selectbox("Thông số", variables, index=variables.index("dissolved_oxygen")
                                   if "dissolved_oxygen" in variables else 0, format_func=contracts.LABELS_VI.get)
            missing_type = b.selectbox("Kiểu khuyết", ["block", "random"],
                                       format_func={"block": "Theo đoạn", "random": "Ngẫu nhiên"}.get)
            causal = c.toggle("Chỉ mô hình dùng được online", value=True)
            selected = scores[(scores.variable == variable) & (scores.missing_type == missing_type)]
            if causal:
                selected = selected[selected.causal]
            summary = selected.groupby(["method", "missing_rate"], as_index=False).agg(
                rmse=("rmse", "mean"), mae=("mae", "mean"), seeds=("seed", "nunique"),
                n_masked=("n_masked", "sum"), n_scored=("n_scored", "sum"),
            )
            summary["missing_pct"] = summary.missing_rate * 100
            summary["coverage_pct"] = 100 * summary.n_scored.div(summary.n_masked.where(summary.n_masked.gt(0)))
            figure = px.line(summary, x="missing_pct", y="rmse", color="method", markers=True,
                             color_discrete_sequence=MODEL_COLORS,
                             labels={"missing_pct": "Tỷ lệ che (%)", "method": "Mô hình"})
            st.plotly_chart(chart(figure, yaxis_title=f"RMSE ({contracts.UNITS[variable]})"), width="stretch", theme=None)
            st.caption("RMSE và MAE trung bình theo seed. Độ phủ là tỷ lệ điểm bị che có dự đoán để chấm.")
            st.dataframe(summary[["method", "missing_pct", "rmse", "mae", "coverage_pct", "seeds"]],
                         hide_index=True, width="stretch",
                         column_config={
                             "method": "Mô hình", "missing_pct": "Tỷ lệ che (%)",
                             "rmse": st.column_config.NumberColumn("RMSE", format="%.4f"),
                             "mae": st.column_config.NumberColumn("MAE", format="%.4f"),
                             "coverage_pct": st.column_config.NumberColumn("Độ phủ (%)", format="%.2f"),
                             "seeds": "Số seed",
                         })
            st.download_button("Tải CSV benchmark 235 trạm", path.read_bytes(), path.name, "text/csv")
        else:
            st.info("Chưa có CSV benchmark 235 trạm.")

    with forecast, st.container(border=True, key="card_forecast_lab"):
        st.subheader("Benchmark dự báo theo thông số")
        paths = {}
        for variable in contracts.VARIABLE_NAMES:
            prefix = "forecast" if variable == "dissolved_oxygen" else f"forecast-{variable}"
            results = [p for split in ("test", "valid") if (p := Path(f"data/report/{prefix}-{split}.csv")).is_file()]
            if results:
                paths[variable] = results
        if paths:
            variables = list(paths)
            target = st.selectbox("Thông số benchmark", variables, format_func=contracts.LABELS_VI.get,
                                  index=variables.index("dissolved_oxygen") if "dissolved_oxygen" in paths else 0)
            path = st.selectbox("Tập kết quả dự báo", paths[target], format_func=lambda p: p.stem)
            scores = pd.read_csv(path)
            figure = px.line(scores, x="horizon_min", y="rmse", color="model", markers=True,
                             color_discrete_sequence=MODEL_COLORS,
                             labels={"horizon_min": "Khoảng dự báo (phút)", "model": "Mô hình"})
            st.plotly_chart(chart(figure, yaxis_title=f"RMSE ({contracts.UNITS[target]})"), width="stretch", theme=None)
            st.dataframe(scores, hide_index=True, width="stretch")
            st.download_button("Tải CSV benchmark dự báo", path.read_bytes(), path.name, "text/csv")
        else:
            st.info("Chạy benchmark dự báo Spark để tạo bảng kết quả validation và test trong data/report.")

    with training, st.container(border=True, key="card_training"):
        st.subheader("Thí nghiệm trên dữ liệu Silver")
        st.caption(f"Nguồn: {get_settings().lake_path}/silver/measurements · Giới hạn một trạm mỗi lần chạy.")
        a, b, c = st.columns([2, 1, 1])
        station_ids = list(station_labels)
        station = a.selectbox("Trạm huấn luyện", station_ids, format_func=station_labels.get,
                              index=station_ids.index(selected_station) if selected_station in station_ids else 0)
        task = b.selectbox("Bài toán", ["imputation", "forecast"],
                           format_func={"imputation": "Điền khuyết", "forecast": "Dự báo theo thông số"}.get)
        split = c.selectbox("Tập đánh giá", ["valid", "test"])
        missing_type, missing_rate = "block", 10
        target = "dissolved_oxygen"
        if task == "forecast":
            target = st.selectbox("Thông số huấn luyện", contracts.VARIABLE_NAMES,
                                  index=contracts.VARIABLE_NAMES.index(target), format_func=contracts.LABELS_VI.get)
        if task == "imputation":
            d, e = st.columns(2)
            missing_type = d.selectbox("Kiểu che", ["block", "random"],
                                       format_func={"block": "Theo đoạn", "random": "Ngẫu nhiên"}.get)
            missing_rate = e.select_slider("Tỷ lệ che (%)", [5, 10, 20, 30], value=10)
        st.caption("Huấn luyện trên 2023–2024. Chọn mô hình trên validation và báo cáo sai số trên test.")
        submit = st.button("Huấn luyện và đánh giá", type="primary", disabled=station is None)
        if submit:
            st.session_state.pop("station_experiment", None)
            try:
                with st.spinner("Đang đọc Silver và đánh giá trên khoảng thời gian đã chọn…"):
                    scores, preview, artifact = train_station(station, split, task, missing_type, missing_rate / 100, target)
                st.session_state["station_experiment"] = (station, split, task, scores, preview, artifact)
            except Exception as error:
                st.error(f"Không chạy được thí nghiệm. Kiểm tra đường dẫn Silver và dữ liệu của trạm. {error}")
        if "station_experiment" in st.session_state:
            station, split, task, scores, preview, artifact = st.session_state["station_experiment"]
            if task == "forecast":
                target = scores["target"].iloc[0] if "target" in scores else "dissolved_oxygen"
            title = "Điền khuyết" if task == "imputation" else contracts.LABELS_VI[target]
            st.markdown(f"**{station} · {split} · {title}**")
            st.dataframe(scores, hide_index=True, width="stretch")
            if preview is not None:
                horizon = st.selectbox("Khoảng dự báo (phút)", sorted(preview.horizon_min.unique()))
                series = preview[preview.horizon_min == horizon]
                figure = px.line(series, x="target_ts", y=["observed", "ridge", "persistence", "seasonal_naive"],
                                 color_discrete_sequence=MODEL_COLORS,
                                 labels={"target_ts": "Thời điểm quan trắc", "variable": "Chuỗi", "value": contracts.LABELS_VI[target]})
                st.plotly_chart(chart(figure, yaxis_title=f"{contracts.LABELS_VI[target]} ({contracts.UNITS[target]})"), width="stretch", theme=None)
            metrics, models = st.columns(2)
            metrics.download_button("Tải CSV thí nghiệm", scores.to_csv(index=False), f"{station}-{task}-{split}.csv",
                                    "text/csv")
            model_name = "kalman" if task == "imputation" else "ridge"
            suffix = f"-{target}" if task == "forecast" and target != "dissolved_oxygen" else ""
            models.download_button(f"Tải mô hình {model_name}", artifact, f"{station}-{model_name}{suffix}.joblib",
                                   "application/octet-stream")
            st.caption("Tệp mô hình được fit riêng cho trạm đã chọn. Cấu hình streaming được quản lý qua Docker Compose.")
