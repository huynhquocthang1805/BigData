"""Locally bundled visuals and shared presentation for the water-quality console."""
from __future__ import annotations

import base64
from functools import lru_cache
from html import escape
import json
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

ASSETS = Path(__file__).with_name("assets")
BLUE = "#116ee2"
MODEL_COLORS = [BLUE, "#18988f", "#785aca", "#cc7126", "#906549"]
COLORS = {"raw": BLUE, "imputed": "#9860ce", "forecast": "#00877a", "threshold": "#c76332"}
PAGES = {
    "Giám sát": ("activity", "Giám sát chất lượng nước"),
    "Dự báo": ("chart-line", "Dự báo chất lượng nước"),
    "Cảnh báo": ("bell", "Cảnh báo chất lượng nước"),
    "Pipeline": ("server", "Hiệu năng pipeline"),
    "Mô hình": ("flask", "Phòng thí nghiệm mô hình"),
}


@lru_cache
def asset_json(name):
    return json.loads((ASSETS / name).read_text(encoding="utf-8"))


def icon(name):
    paths = asset_json("tabler-icons.json")[name]
    return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="20" height="20" '
            f'fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" '
            f'stroke-linejoin="round" aria-hidden="true">{paths}</svg>')


@lru_cache
def stylesheet():
    css = (ASSETS.parent / "style.css").read_text(encoding="utf-8")
    image = base64.b64encode((ASSETS / "water.webp").read_bytes()).decode()
    css = css.replace("WATER_IMAGE", "data:image/webp;base64," + image)
    for token, file in (("LATIN_FONT", "inter-latin.woff2"), ("VIETNAMESE_FONT", "inter-vietnamese.woff2")):
        encoded_font = base64.b64encode((ASSETS / file).read_bytes()).decode()
        css = css.replace(token, "data:font/woff2;base64," + encoded_font)
    for index, (name, _) in enumerate(PAGES.values(), start=1):
        encoded = base64.b64encode(icon(name).encode()).decode()
        css += (f'.st-key-navigation [role="radiogroup"] >div:nth-child({index}) label::before'
                f'{{mask-image:url(data:image/svg+xml;base64,{encoded})}}')
    return f"<style>{css}</style>"


def brand():
    st.sidebar.markdown(f'<div class="wq-brand"><span>{icon("ripple")}</span>'
                        '<div><b>WaterScope</b><small>Quan trắc USGS</small></div></div>',
                        unsafe_allow_html=True)


def timestamp(value):
    return pd.to_datetime(value, utc=True).strftime("%d/%m/%Y · %H:%M UTC") if pd.notna(value) else "Chưa có dữ liệu"


def hero(page, detail, status, connected=False, station_id=None):
    glyph, title = PAGES[page]
    context = f'<p class="wq-context">{icon(glyph)} {escape(station_id)}</p>' if station_id else ""
    st.markdown(
        f'<section class="wq-hero"><div class="wq-hero-content">{context}<h1>{escape(title)}</h1>'
        f'<p class="wq-detail">{escape(detail)}</p></div>'
        f'<span class="wq-status"><i class="{"connected" if connected else ""}"></i>{escape(status)}</span></section>',
        unsafe_allow_html=True,
    )


def metrics(entries):
    cards = []
    for name, title, value, note, color in entries:
        cards.append(f'<div class="wq-kpi {color}"><div class="wq-kpi-head"><span>{escape(title)}</span>'
                     f'<i>{icon(name)}</i></div><strong>{escape(str(value))}</strong>'
                     f'<small>{escape(str(note))}</small></div>')
    st.markdown('<div class="wq-kpis">' + "".join(cards) + "</div>", unsafe_allow_html=True)


def panel_title(title, detail=""):
    small = f'<p>{escape(detail)}</p>' if detail else ""
    st.markdown(f'<div class="wq-section"><h2>{escape(title)}</h2>{small}</div>', unsafe_allow_html=True)


def chart(fig, height=315, yaxis_title=None):
    fig.update_layout(
        height=height, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="#fff",
        font=dict(family="Inter, sans-serif", color="#425e7a", size=12),
        margin=dict(l=10, r=12, t=12, b=8), hovermode="x unified", colorway=MODEL_COLORS,
        legend=dict(orientation="h", yanchor="top", y=-0.17, x=0),
        yaxis_title=yaxis_title, hoverlabel=dict(bgcolor="white", font_color="#142e4a"),
    )
    fig.update_xaxes(showgrid=False, automargin=True, linecolor="#dce5ef")
    fig.update_yaxes(gridcolor="#e6edf5", zeroline=False, automargin=True)
    return fig


def station_map(stations, selected):
    points = stations.dropna(subset=["longitude", "latitude"])
    if points.empty:
        st.caption("Các trạm chưa có tọa độ.")
        return
    fig = go.Figure()
    boundaries = asset_json("north-america.json")
    for kind in ("land", "states"):
        coords = [point for line in boundaries[kind] for point in [*line, (None, None)]]
        xs, ys = zip(*coords)
        fig.add_trace(go.Scatter(x=xs, y=ys, mode="lines", line=dict(color="#b9cddc", width=0.7),
                                 fill="toself" if kind == "land" else None, fillcolor="#f7fafc",
                                 hoverinfo="skip", showlegend=False))
    chosen = points[points.station_id == selected]
    for subset, color, size, label in (
        (points[points.last_ts.isna()], "#93a8b8", 4, "Chưa có dữ liệu"),
        (points[points.last_ts.notna()], BLUE, 6, "Có dữ liệu"),
        (chosen, "#f57f34", 10, "Trạm đang chọn"),
    ):
        if not subset.empty:
            fig.add_trace(go.Scatter(
                x=subset.longitude, y=subset.latitude, mode="markers", text=subset.station_id,
                marker=dict(color=color, size=size, line=dict(color="white", width=0.6)),
                hovertemplate="%{text}<extra>" + label + "</extra>", showlegend=False,
            ))
    fig.update_layout(height=228, margin=dict(l=0, r=0, t=0, b=0), hovermode="closest",
                      plot_bgcolor="#e0eff8", paper_bgcolor="#e0eff8", dragmode="pan",
                      xaxis=dict(visible=False, range=[min(points.longitude.min()-3,-128),
                                                      max(points.longitude.max()+3,-65)]),
                      yaxis=dict(visible=False, range=[min(points.latitude.min()-3,23),
                                                      max(points.latitude.max()+3,52)]))
    st.plotly_chart(fig, theme=None, width="stretch", config={"displayModeBar": False, "scrollZoom": False})
