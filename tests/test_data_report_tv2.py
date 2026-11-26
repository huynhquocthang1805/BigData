"""Kiểm thử báo cáo dữ liệu TV2.1 / TV2.2b (batch/data_report_tv2.py) trên silver tổng hợp nhỏ."""
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from batch.data_report_tv2 import (bin_gaps, gap_length_counts, gap_runs, gap_summary, missingness_by_split,
                                   silver_checks, stations_table)
from wq import contracts

START = datetime(2025, 6, 30, 23, 0, tzinfo=timezone.utc)   # 4 mốc cuối valid + 8 mốc đầu test
N = 12


def _silver(spark, values: dict[str, dict[str, list]], qc: dict[str, list[int]] | None = None):
    """values[station][variable] = list N giá trị (None = thiếu); biến không khai báo = toàn NULL."""
    # list tuple (không qua pandas) để None giữ là NULL, không thành NaN
    rows = []
    for sid, cols in values.items():
        for i in range(N):
            vals = [cols.get(v, [None] * N)[i] for v in contracts.VARIABLE_NAMES]
            rows.append((sid, START + timedelta(minutes=15 * i), *[None if x is None else float(x) for x in vals],
                         (qc or {}).get(sid, [0] * N)[i]))
    schema = ("station_id string, ts timestamp, "
              + ", ".join(f"{v} double" for v in contracts.VARIABLE_NAMES) + ", qc_flags int")
    return spark.createDataFrame(rows, schema)


DO_A = [7, None, None, 7, 7, None, 7, 7, None, None, None, 7]   # đoạn thiếu 2 | 1 | 3
DO_B = [None, None, 6, 6, 6, 6, 6, 6, 6, 6, 6, None]            # đoạn chạm đầu (2) và cuối (1)


def test_gap_runs_lengths_and_edges(spark):
    sdf = _silver(spark, {"A": {"dissolved_oxygen": DO_A, "ph": [7.0] * N},
                          "B": {"dissolved_oxygen": DO_B}})
    runs = gap_runs(sdf, start="2025-06-30T23:00:00").toPandas()
    got = {(r.station_id, r.variable): [] for r in runs.itertuples()}
    for r in runs.sort_values("gap_start").itertuples():
        got[(r.station_id, r.variable)].append((r.gap_steps, bool(r.at_edge)))
    assert got[("A", "dissolved_oxygen")] == [(2, False), (1, False), (3, False)]
    assert got[("B", "dissolved_oxygen")] == [(2, True), (1, True)]
    # ph của A đủ => không có đoạn thiếu; biến không bao giờ đo (B.ph, turbidity...) bị loại
    assert ("A", "ph") not in got and ("B", "ph") not in got and ("A", "turbidity") not in got


def test_gap_length_counts_and_summary(spark):
    sdf = _silver(spark, {"A": {"dissolved_oxygen": [7, None, 7, None, 7, None, None, 7, 7, 7, 7, 7]}})
    runs = gap_runs(sdf, start="2025-06-30T23:00:00")
    counts = gap_length_counts(runs).toPandas()
    assert list(counts.columns) == ["station_id", "variable", "gap_steps", "count"]
    assert counts[["gap_steps", "count"]].values.tolist() == [[1, 2], [2, 1]]
    s = gap_summary(runs).toPandas().iloc[0]
    assert (s.n_gaps, s.missing_steps, s.max_gap_steps, s.n_edge_gaps) == (3, 4, 2, 0)


def test_gap_does_not_cross_stations(spark):
    """Cuối trạm A thiếu và đầu trạm B thiếu không được nối thành một đoạn."""
    a = [7] * (N - 2) + [None, None]
    b = [None, None] + [6] * (N - 2)
    runs = gap_runs(_silver(spark, {"A": {"dissolved_oxygen": a}, "B": {"dissolved_oxygen": b}}),
                    start="2025-06-30T23:00:00").toPandas()
    assert sorted(runs["gap_steps"]) == [2, 2]


def test_missingness_by_split(spark):
    flags = [0] * N
    flags[4] = contracts.VARIABLE_BIT["dissolved_oxygen"]   # ô bị QC loại (mốc đầu test)
    do = list(DO_A)
    do[4] = None
    sdf = _silver(spark, {"A": {"dissolved_oxygen": do}}, qc={"A": flags})
    m = missingness_by_split(sdf).toPandas().set_index(["variable", "split"])
    # valid = 4 mốc đầu (7, NaN, NaN, 7); test = 8 mốc còn lại
    assert m.loc[("dissolved_oxygen", "valid"), ["n_slots", "n_missing"]].tolist() == [4, 2]
    assert m.loc[("dissolved_oxygen", "test"), ["n_slots", "n_missing", "n_qc_flagged"]].tolist() == [8, 5, 1]
    assert m.loc[("dissolved_oxygen", "all"), "missing_rate"] == pytest.approx(7 / 12, abs=1e-4)
    assert m.loc[("turbidity", "all"), "missing_rate"] == 1.0
    assert ("dissolved_oxygen", "train") not in m.index


def test_silver_checks_detects_problems(spark):
    sdf = _silver(spark, {"A": {"ph": [7.0] * N}, "B": {"ph": [7.0] * N}})
    ok = silver_checks(sdf, "2025-06-30T23:00:00", "2025-07-01T02:00:00")
    assert ok["ok"].all() and ok["n_rows"].tolist() == [N, N]
    dup = sdf.unionByName(sdf.where("station_id = 'B'").limit(1))
    bad = silver_checks(dup, "2025-06-30T23:00:00", "2025-07-01T02:00:00").set_index("station_id")
    assert bool(bad.loc["A", "ok"]) and not bool(bad.loc["B", "ok"])
    assert bad.loc["B", "n_duplicate_keys"] == 1


def test_stations_table_reason_from_data():
    meta = pd.DataFrame([{"station_id": "A", "name": "X RIVER", "state": "Oregon", "site_type": "Stream",
                          "latitude": 45.0, "longitude": -122.0, "drainage_area_sqmi": 940.0}])
    rows = []
    for v in contracts.VARIABLE_NAMES:
        observed = 0 if v == "discharge" else 90
        rows.append({"station_id": "A", "variable": v, "split": "all", "n_slots": 100,
                     "n_observed": observed, "missing_rate": 1 - observed / 100})
    t = stations_table(meta, pd.DataFrame(rows)).iloc[0]
    assert t.water_body == "Sông/suối" and t.n_core_measured == 5
    assert "discharge" not in t.variables_measured
    assert "không có lưu lượng" in t.selection_reason and "10.0%" in t.selection_reason


def test_bin_gaps_shares_sum_to_one():
    counts = pd.DataFrame({"station_id": "A", "variable": "ph", "gap_steps": [1, 3, 30, 800], "count": [10, 5, 2, 1]})
    b = bin_gaps(counts)
    assert b["share_gaps"].sum() == pytest.approx(1.0)
    assert b["share_missing_steps"].sum() == pytest.approx(1.0)
    assert b.set_index("bin").loc["> 7 ngày", "missing_steps"] == 800


def test_solar_hour_uses_longitude(spark):
    from batch.eda_tv2 import with_solar_hour

    meta = pd.DataFrame([{"station_id": "W", "longitude": -120.0}, {"station_id": "E", "longitude": -75.0}])
    df = spark.createDataFrame([("W", datetime(2025, 7, 1, 20, 0, tzinfo=timezone.utc)),
                                ("E", datetime(2025, 7, 1, 2, 0, tzinfo=timezone.utc))],
                               "station_id string, ts timestamp")
    got = {r.station_id: r.solar_hour for r in with_solar_hour(df, meta).collect()}
    assert got == {"W": 12, "E": 21}   # 20h UTC - 8h = 12h; 2h UTC - 5h = 21h hôm trước


def test_within_station_corr_not_pooled(spark):
    """Hai trạm đều có DO giảm khi nhiệt độ tăng (r = -1 trong trạm) nhưng trạm ấm hơn có DO nền cao hơn:
    gộp chung sẽ ra tương quan dương; tính trong trạm phải ra -1 ở cả hai."""
    from batch.eda_tv2 import within_station_corr

    vals = {"A": {"water_temp": list(range(N)), "dissolved_oxygen": [10 - 0.1 * i for i in range(N)]},
            "B": {"water_temp": [20 + i for i in range(N)], "dissolved_oxygen": [20 - 0.1 * i for i in range(N)]}}
    c = within_station_corr(_silver(spark, vals), ["water_temp", "dissolved_oxygen"])
    assert c["r"].round(6).tolist() == [-1.0, -1.0] and c["n"].tolist() == [N, N]
