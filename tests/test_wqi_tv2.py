"""Kiểm thử CCME WQI (wq/wqi.py) và luật cảnh báo TV2.2c (batch/alerts_tv2.py)."""
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from batch.alerts_tv2 import LEGACY_ALERT_RULES, baseline_violations, episodes, violations
from wq import contracts
from wq.spark_transforms import alert_events
from wq.wqi import (NATIONAL_OBJECTIVES, Objective, ccme_wqi, objectives_frame, wqi_class, wqi_daily,
                    wqi_daily_pandas)

OBJ = [Objective("dissolved_oxygen", lo=5.0), Objective("ph", lo=6.5, hi=9.0),
       Objective("water_temp", hi=25.0), Objective("specific_conductance", hi=500.0)]


def test_ccme_wqi_matches_hand_calculation():
    """Tính tay: F1 = 2/4*100 = 50; F2 = 2/16*100 = 12,5;
    excursion DO = 5/4 - 1 = 0,25, pH = 9,9/9 - 1 = 0,1 => nse = 0,35/16 = 0,021875;
    F3 = 0,021875 / (0,01*0,021875 + 0,01) = 2,1407;
    WQI = 100 - sqrt(50^2 + 12,5^2 + 2,1407^2) / 1,732 = 70,22 (Fair)."""
    w = pd.DataFrame({"dissolved_oxygen": [6, 6, 4, 6], "ph": [7, 7, 7, 9.9],
                      "water_temp": [20.0] * 4, "specific_conductance": [400.0] * 4})
    r = ccme_wqi(w, OBJ)
    assert (r["f1"], r["f2"], r["f3"]) == (50.0, 12.5, pytest.approx(2.14, abs=0.01))
    assert r["wqi"] == pytest.approx(70.22, abs=0.01) and r["wqi_class"] == "Fair"


def test_ccme_wqi_all_pass_is_100_and_needs_four_variables():
    ok = pd.DataFrame({"dissolved_oxygen": [8.0] * 4, "ph": [7.5] * 4, "water_temp": [20.0] * 4,
                       "specific_conductance": [300.0] * 4})
    assert ccme_wqi(ok, OBJ)["wqi"] == 100.0
    assert ccme_wqi(ok.drop(columns="ph"), OBJ)["wqi"] is None            # 3 biến < 4
    short = ok.copy()
    short.loc[1:, "ph"] = np.nan                                           # pH chỉ 1 lần đo < 4
    assert ccme_wqi(short, OBJ)["wqi"] is None


def test_wqi_classes_boundaries():
    assert [wqi_class(x) for x in (100, 95, 94.99, 80, 65, 45, 44.9, 0)] == \
        ["Excellent", "Excellent", "Good", "Good", "Fair", "Marginal", "Poor", "Poor"]
    assert wqi_class(None) is None


def test_national_objectives_cite_sources():
    from wq.wqi import SOURCES
    for o in NATIONAL_OBJECTIVES:
        assert o.source.split(":")[0] in SOURCES


def test_spark_wqi_matches_pandas(spark):
    rng = np.random.default_rng(0)
    ts = pd.date_range("2024-06-01", periods=2 * 96, freq="15min", tz="UTC")
    rows = []
    for sid in ("A", "B"):
        df = pd.DataFrame({"station_id": sid, "ts": ts,
                           "water_temp": rng.normal(24, 2, len(ts)), "specific_conductance": rng.normal(450, 60, len(ts)),
                           "ph": rng.normal(8.6, 0.3, len(ts)), "dissolved_oxygen": rng.normal(6, 1.2, len(ts)),
                           "turbidity": rng.gamma(2, 5, len(ts)), "discharge": np.nan})
        rows.append(df)
    pdf = pd.concat(rows, ignore_index=True)
    base = pd.DataFrame([{"station_id": s, "variable": v, "p99": p} for s in ("A", "B")
                         for v, p in (("water_temp", 26.0), ("specific_conductance", 520.0), ("turbidity", 25.0))])
    expected = wqi_daily_pandas(pdf, base).set_index(["station_id", "date"])["wqi"]

    sp = pdf.copy()
    sp["ts"] = sp["ts"].dt.tz_localize(None)
    silver = spark.createDataFrame(sp[["station_id", "ts", *contracts.VARIABLE_NAMES]])
    got = (wqi_daily(silver, objectives_frame(spark, spark.createDataFrame(base))).toPandas()
           .set_index(["station_id", "date"])["wqi"])
    assert len(got) == len(expected) == 4
    for k, v in expected.items():
        assert got[k] == pytest.approx(v, abs=0.01)


def _silver(spark, do_values, sid="S1", extra=None):
    t0 = datetime(2025, 7, 1, tzinfo=timezone.utc)
    rows = []
    for i, do in enumerate(do_values):
        row = {v: None for v in contracts.VARIABLE_NAMES}
        row.update({"dissolved_oxygen": do, **(extra or {})})
        rows.append((sid, t0 + timedelta(minutes=15 * i), *[row[v] for v in contracts.VARIABLE_NAMES]))
    schema = "station_id string, ts timestamp, " + ", ".join(f"{v} double" for v in contracts.VARIABLE_NAMES)
    return spark.createDataFrame(rows, schema)


def test_violations_equal_alert_events_for_current_rules(spark):
    df = _silver(spark, [8.0, 4.0, 1.5, None, 6.0], extra={"ph": 9.5})
    mine = sorted((r.rule, r.ts) for r in violations(df, contracts.ALERT_RULES).collect())
    ref = sorted((r.rule, r.ts) for r in alert_events(df).collect())
    assert mine == ref and len(ref) == 3 + 5   # DO_LOW x2, DO_HYPOXIA x1, PH_HIGH x5


def test_episodes_merge_and_min_duration(spark):
    # DO thấp 2 mốc (ngắn, bỏ), bình thường, thấp 5 mốc (1 sự kiện), một mốc hypoxia đơn lẻ (critical => giữ)
    do = [4.5, 4.5, 7.0, 4.9, 4.8, 4.0, 4.6, 4.7, 7.0, 7.0, 1.0, 7.0]
    ep = episodes(violations(_silver(spark, do), contracts.ALERT_RULES)).orderBy("start_ts").collect()
    got = [(e.rule, e.n_steps, e.peak_value) for e in ep]
    assert ("DO_LOW", 5, 4.0) in got and ("DO_HYPOXIA", 1, 1.0) in got
    assert all(not (r == "DO_LOW" and n == 2) for r, n, _ in got)


def test_baseline_rules_use_station_threshold(spark):
    """Độ dẫn điện 3000 là bình thường ở trạm nước lợ (p99 = 20000) nhưng bất thường ở trạm sông (p99 = 800)."""
    a = _silver(spark, [8.0] * 4, "SALT", extra={"specific_conductance": 3000.0})
    b = _silver(spark, [8.0] * 4, "RIVER", extra={"specific_conductance": 3000.0})
    base = spark.createDataFrame([("SALT", "specific_conductance", 20000.0), ("RIVER", "specific_conductance", 800.0)],
                                 "station_id string, variable string, p99 double")
    v = baseline_violations(a.unionByName(b), base).collect()
    assert {r.station_id for r in v} == {"RIVER"} and {r.rule for r in v} == {"SC_ANOM"}
    old = violations(a.unionByName(b), LEGACY_ALERT_RULES).collect()
    assert {r.station_id for r in old if r.rule == "SC_HIGH"} == {"SALT", "RIVER"}
