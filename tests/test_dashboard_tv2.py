"""Kiểm thử phần tính toán của tab "Chất lượng nước" (dashboard/monitoring_tv2.py), không cần Streamlit/API."""
import numpy as np
import pandas as pd
import pytest

from dashboard.monitoring_tv2 import (NO_WQI_COLOR, WQI_COLORS, daily_wqi, latest_wqi, load_baselines,
                                      missing_matrix, station_wqi_table, to_grid)
from wq import contracts


def _api_rows(sid: str, days: int = 2, drop: slice | None = None, **const) -> pd.DataFrame:
    """Giống JSON của /measurements: ts dạng chuỗi ISO, NaN = None."""
    ts = pd.date_range("2025-07-01", periods=days * 96, freq="15min", tz="UTC")
    df = pd.DataFrame({"station_id": sid, "ts": ts.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
                       "water_temp": 20.0, "specific_conductance": 300.0, "ph": 7.5, "dissolved_oxygen": 8.0,
                       "turbidity": 5.0, "discharge": None, "qc_flags": 0})
    for k, v in const.items():
        df[k] = v
    if drop is not None:
        df = df.drop(df.index[drop])
    return df


BASE = pd.DataFrame([{"station_id": s, "variable": v, "p99": p} for s in ("A", "B")
                     for v, p in (("water_temp", 28.0), ("specific_conductance", 600.0), ("turbidity", 50.0))])


def test_to_grid_restores_missing_rows():
    g = to_grid(_api_rows("A", drop=slice(10, 20)))
    assert len(g) == 2 * 96 and g["ph"].isna().sum() == 10 and (g["station_id"] == "A").all()


def test_missing_matrix_counts_absent_rows_and_unmeasured_variables():
    m = missing_matrix({"A": _api_rows("A", drop=slice(0, 48)), "B": _api_rows("B")})
    assert m.loc["A", "ph"] == pytest.approx(0.0)       # hàng bị thiếu ở đầu nằm ngoài lưới [min, max]
    a = _api_rows("A")
    a.loc[a.index[50:98], "dissolved_oxygen"] = None
    m = missing_matrix({"A": a})
    assert m.loc["A", "dissolved_oxygen"] == pytest.approx(25.0)
    assert m.loc["A", "discharge"] == 100.0           # trạm không đo lưu lượng


def test_daily_wqi_and_latest():
    a = _api_rows("A")                                 # mọi giá trị trong ngưỡng => 100 (Excellent)
    b = _api_rows("B", dissolved_oxygen=3.0)           # DO luôn < 5 => 1/5 biến vi phạm, mọi lần đo DO vi phạm
    d = daily_wqi({"A": a, "B": b}, BASE)
    assert set(d["station_id"]) == {"A", "B"} and len(d) == 4
    assert (d[d.station_id == "A"]["wqi"] == 100.0).all()
    lb = latest_wqi(d).set_index("station_id")
    assert lb.loc["A", "wqi_class"] == "Excellent"
    # Tính tay cho B (mỗi ngày 96 lần đo x 5 biến): F1 = 1/5*100 = 20; F2 = 96/480*100 = 20;
    # nse = (5/3 - 1) * 96/480 = 0,1333; F3 = 0,1333/(0,001333 + 0,01) = 11,76;
    # WQI = 100 - sqrt(20^2 + 20^2 + 11,76^2)/1,732 = 82,31 (Good)
    assert lb.loc["B", "wqi"] == pytest.approx(82.31, abs=0.01) and lb.loc["B", "wqi_class"] == "Good"


def test_wqi_needs_baselines_for_four_variables():
    """Không có ngưỡng nền thì chỉ còn DO và pH (< 4 biến) => không tính WQI, trạm hiện màu xám."""
    d = daily_wqi({"A": _api_rows("A")}, None)
    assert d["wqi"].isna().all()
    st = pd.DataFrame({"station_id": ["A"], "name": ["X"], "latitude": [40.0], "longitude": [-75.0],
                       "alerts_24h": [0]})
    t = station_wqi_table(st, latest_wqi(d))
    assert t.loc[0, "color"] == NO_WQI_COLOR and pd.isna(t.loc[0, "wqi"])


def test_station_table_colors_by_class():
    st = pd.DataFrame({"station_id": ["A", "B"], "name": ["X", "Y"], "latitude": [40.0, 41.0],
                       "longitude": [-75.0, -76.0], "alerts_24h": [0, 3]})
    latest = pd.DataFrame({"station_id": ["A", "B"], "date": ["2025-07-02"] * 2, "wqi": [97.0, 50.0],
                           "wqi_class": ["Excellent", "Marginal"]})
    t = station_wqi_table(st, latest).set_index("station_id")
    assert t.loc["A", "color"] == WQI_COLORS["Excellent"] and t.loc["B", "mức"] == "Kém"


def test_load_baselines_prefers_api_then_csv(tmp_path):
    api_rows = [{"station_id": "A", "variable": "turbidity", "p99": 1.0}]
    df, src = load_baselines(lambda path: api_rows, tmp_path / "none.csv")
    assert src.startswith("API") and len(df) == 1

    def broken(path):
        raise RuntimeError("404")
    csv = tmp_path / "b.csv"
    BASE.to_csv(csv, index=False)
    df, src = load_baselines(broken, csv)
    assert src == "b.csv" and len(df) == len(BASE)
    assert load_baselines(broken, tmp_path / "none.csv") == (None, "không có")


def test_repo_baselines_csv_covers_core_stations():
    """Bảng nền trong repo (python -m wq.wqi) phải có p99 của 3 biến cho cả 10 trạm chuẩn."""
    df, _ = load_baselines(None)
    from wq.io import core_station_ids
    need = {(s, v) for s in core_station_ids() for v in ("water_temp", "specific_conductance", "turbidity")}
    have = {(r.station_id, r.variable) for r in df.itertuples() if not np.isnan(r.p99)}
    assert need <= have
    assert set(df.columns) >= {"station_id", "variable", "p99"}
    assert all(v in contracts.CORE_VARIABLES for v in df["variable"])


def test_latest_wqi_skips_partial_day():
    """Ngày đang phát dở (ít hơn 12 giờ dữ liệu) không được báo là WQI ngày gần nhất."""
    a = _api_rows("A", days=3, drop=slice(2 * 96 + 8, None))      # ngày 3 chỉ có 8 mốc = 2 giờ
    d = daily_wqi({"A": a}, BASE)
    assert d["n_records"].tolist() == [96, 96, 8]
    assert str(latest_wqi(d).loc[0, "date"]) == "2025-07-02"


def test_station_map_groups_by_class():
    from dashboard.monitoring_tv2 import station_map
    t = pd.DataFrame({"station_id": ["A", "B"], "name": ["X", "Y"], "latitude": [40.0, 41.0],
                      "longitude": [-75.0, -76.0], "wqi": [97.0, np.nan], "wqi_class": ["Excellent", None]})
    names = [tr.name for tr in station_map(t).data]
    assert names == ["Rất tốt", "Chưa đủ dữ liệu"]
