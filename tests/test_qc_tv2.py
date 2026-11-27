"""Kiểm thử QC nâng cao TV2.2a (batch/qc_tv2.py): qualifier, spike, flatline."""
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

from batch.etl_usgs import to_bronze, to_silver
from batch.qc_tv2 import (FLATLINE_RULES, SPIKE_RULES, apply_advanced_qc, flatline_mask, qc_station, spike_mask)
from wq import contracts

N = 400
T0 = datetime(2025, 7, 1, tzinfo=timezone.utc)


def _station(sid: str, seed: int = 0) -> pd.DataFrame:
    """Chuỗi bình thường: DO và nhiệt độ có chu kỳ ngày + nhiễu nhỏ, pH làm tròn 0,1 như USGS."""
    rng = np.random.default_rng(seed)
    t = np.arange(N)
    day = np.sin(2 * np.pi * t / contracts.STEPS_PER_DAY)
    df = pd.DataFrame({
        "station_id": sid,
        "ts": pd.date_range(T0, periods=N, freq="15min"),
        "water_temp": np.round(22 + 1.5 * day + rng.normal(0, 0.05, N), 1),
        "specific_conductance": np.round(400 + 10 * day + rng.normal(0, 2, N)),
        "ph": np.round(7.8 + 0.1 * day, 1),
        "dissolved_oxygen": np.round(8 + 1.2 * day + rng.normal(0, 0.05, N), 2),
        "turbidity": np.round(5 + rng.gamma(2, 0.5, N), 1),
        "discharge": 1000 + 50 * day,
        "qc_flags": 0,
    })
    return df


def test_normal_series_not_flagged():
    df = _station("A")
    out = qc_station(df.copy())
    assert (out["qc_flags"] == 0).all()
    pd.testing.assert_frame_equal(out[contracts.VARIABLE_NAMES], df[contracts.VARIABLE_NAMES], check_dtype=False)


def test_spike_flagged_and_set_null():
    df = _station("A")
    df.loc[200, "dissolved_oxygen"] = 15.0        # nhảy 7 mg/L trong 1 mốc
    df.loc[250, "turbidity"] = 80.0               # gấp ~13 lần
    df.loc[300, "ph"] = 9.4
    out = qc_station(df)
    for i, v in [(200, "dissolved_oxygen"), (250, "turbidity"), (300, "ph")]:
        assert np.isnan(out.loc[i, v])
        assert out.loc[i, "qc_flags"] & contracts.VARIABLE_BIT[v]
    assert int((out["qc_flags"] != 0).sum()) == 3   # chỉ đúng 3 điểm


def test_spike_floor_prevents_flags_when_mad_is_zero():
    """pH đứng yên ở 7.8 (MAD = 0) rồi đổi 0,1: không được coi là spike."""
    x = pd.Series([7.8] * 60 + [7.9] + [7.8] * 60)
    assert not spike_mask(x, SPIKE_RULES["ph"]).any()


def test_flatline_after_variation_flagged():
    df = _station("A")
    df.loc[250:369, "dissolved_oxygen"] = 7.3       # cảm biến "đứng" 120 mốc = 30 giờ
    m = flatline_mask(df["dissolved_oxygen"], FLATLINE_RULES["dissolved_oxygen"])
    assert m[250:369].all() and not m[:250].any() and not m[370:].any()
    out = qc_station(df)
    assert out.loc[250:369, "dissolved_oxygen"].isna().all()


def test_flatline_ignores_short_runs_quiet_series_and_ice():
    rule = FLATLINE_RULES["water_temp"]
    short = pd.Series(np.r_[np.sin(np.arange(200)), [5.0] * 50])        # đứng 50 mốc < 96
    assert not flatline_mask(short, rule).any()
    quiet = pd.Series(np.r_[np.full(200, 11.0) + np.tile([0, 0.1], 100), [11.0] * 120])  # trước đó gần như không đổi
    assert not flatline_mask(quiet, rule).any()
    ice = pd.Series(np.r_[np.sin(np.arange(200)) * 2 + 3, [0.0] * 300])  # đóng băng ở 0 °C
    assert not flatline_mask(ice, rule).any()


def test_station_does_not_affect_other_station(spark):
    a, b = _station("A", 1), _station("B", 2)
    a.loc[200, "dissolved_oxygen"] = 15.0
    pdf = pd.concat([a, b], ignore_index=True)
    pdf["ts"] = pdf["ts"].dt.tz_convert("UTC").dt.tz_localize(None)
    sdf = spark.createDataFrame(pdf[["station_id", "ts", *contracts.VARIABLE_NAMES, "qc_flags"]])
    out = apply_advanced_qc(sdf).toPandas()
    assert len(out) == 2 * N and list(out.columns) == ["station_id", "ts", *contracts.VARIABLE_NAMES, "qc_flags"]
    fa = out[out.station_id == "A"]
    fb = out[out.station_id == "B"]
    assert int((fa.qc_flags != 0).sum()) == 1 and int((fb.qc_flags != 0).sum()) == 0
    expected_b = b.sort_values("ts")["dissolved_oxygen"].to_numpy()
    assert np.allclose(fb.sort_values("ts")["dissolved_oxygen"].to_numpy(), expected_b)


def _raw(spark, rows):
    return spark.createDataFrame(
        rows, "time string, value string, approval_status string, qualifier string, "
              "station_id string, parameter_code string")


def test_qualifier_qc_in_silver(spark):
    ts = lambda m: f"2025-01-01 00:{m:02d}:00+00:00"
    raw = _raw(spark, [
        (ts(0), "7.0", "Approved", "EQUIP", "S1", "00400"),              # ô 00:00 chỉ có lần đo hỏng => NULL + cờ
        (ts(15), "7.2", "Approved", "EQUIP,ESTIMATED", "S1", "00400"),   # ô 00:15: 1 hỏng, 1 tốt => giữ lần tốt
        (ts(20), "7.4", "Approved", None, "S1", "00400"),
        (ts(30), "7.6", "Approved", "REVISED", "S1", "00400"),          # REVISED vẫn là số đo => giữ
        (ts(45), "20.0", "Provisional", "MAINT", "S1", "00010"),         # nhiệt độ lúc bảo trì => NULL + cờ
        (ts(45), "7.8", "Approved", None, "S1", "00400"),
    ])
    rows = to_silver(to_bronze(raw), "2025-01-01", "2025-01-01T01:00:00").orderBy("ts").collect()
    assert len(rows) == 4
    assert rows[0]["ph"] is None and rows[0]["qc_flags"] == contracts.VARIABLE_BIT["ph"]
    assert rows[1]["ph"] == 7.4 and rows[1]["qc_flags"] == 0
    assert rows[2]["ph"] == 7.6 and rows[2]["qc_flags"] == 0
    assert rows[3]["water_temp"] is None and rows[3]["ph"] == 7.8
    assert rows[3]["qc_flags"] == contracts.VARIABLE_BIT["water_temp"]


def test_silver_row_count_and_columns_unchanged_by_qc(spark):
    rows = []
    for i in range(N):
        t = T0 + timedelta(minutes=15 * i)
        v = 8 + np.sin(2 * np.pi * i / 96)
        rows.append((t.strftime("%Y-%m-%d %H:%M:%S+00:00"), f"{15.0 if i == 200 else v:.2f}", "Approved", None,
                     "S1", "00300"))
    bronze = to_bronze(_raw(spark, rows))
    end = (T0 + timedelta(minutes=15 * N)).strftime("%Y-%m-%dT%H:%M:%S")
    plain = to_silver(bronze, "2025-07-01", end, advanced_qc=False)
    qc = to_silver(bronze, "2025-07-01", end)
    assert plain.count() == qc.count() == N
    assert plain.columns == qc.columns == ["station_id", "ts", *contracts.VARIABLE_NAMES, "qc_flags"]
    flagged = qc.where("qc_flags != 0").collect()
    assert len(flagged) == 1 and flagged[0]["dissolved_oxygen"] is None
