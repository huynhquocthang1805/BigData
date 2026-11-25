"""Test cho các công cụ TV1: đọc data lake (wq.io), kiểm tra mô hình bàn giao, bộ tải dữ liệu IV cũ."""
import json
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from wq import contracts
from wq.io import read_silver


def _make_lake(tmp_path, stations=("S1", "S2"), days=2):
    rows = []
    ts = pd.date_range("2025-06-30", periods=days * contracts.STEPS_PER_DAY, freq="15min", tz="UTC")
    for sid in stations:
        df = pd.DataFrame({"ts": ts})
        for i, v in enumerate(contracts.VARIABLE_NAMES):
            df[v] = np.arange(len(ts), dtype="float64") + i
        df["qc_flags"] = 0
        df["station_id"] = sid
        rows.append(df)
    table = pa.Table.from_pandas(pd.concat(rows, ignore_index=True), preserve_index=False)
    pq.write_to_dataset(table, root_path=str(tmp_path / "silver" / "measurements"), partition_cols=["station_id"])
    return str(tmp_path)


def test_read_silver_local_filters(tmp_path):
    lake = _make_lake(tmp_path)
    df = read_silver(stations=["S2"], start="2025-07-01", end="2025-07-02", lake=lake)
    assert set(df.station_id) == {"S2"}
    assert len(df) == contracts.STEPS_PER_DAY
    assert str(df.ts.dt.tz) == "UTC" and df.ts.min() == pd.Timestamp("2025-07-01", tz="UTC")
    assert list(df.columns[:2]) == ["station_id", "ts"]


def test_validator_accepts_causal_baselines_and_rejects_noncausal(sample_df):
    from wq.models.baselines import LinearInterpolationImputer, LOCFImputer, SeasonalNaiveForecaster
    from wq.models.validate import Report, check_forecaster, check_imputer

    rep = Report()
    check_imputer(LOCFImputer(), sample_df, rep)
    assert not rep.failed, rep.rows
    rep = Report()
    check_imputer(LinearInterpolationImputer(), sample_df, rep)
    assert rep.failed and any("causal" in r[1] and r[0] == "FAIL" for r in rep.rows)
    rep = Report()
    check_forecaster(SeasonalNaiveForecaster(), sample_df, rep)
    assert not rep.failed, rep.rows


def test_legacy_parser_converts_to_utc_and_handles_nodata(monkeypatch):
    import ingestion.download_usgs as dl

    def fake_request(url, retries=6):
        series = []
        for code, vals in {"00300": ["8.5", "-999999"], "00010": ["20.1", "20.3"]}.items():
            series.append({
                "variable": {"variableCode": [{"value": code}], "noDataValue": -999999.0},
                "values": [{"value": [
                    {"value": vals[0], "qualifiers": ["A"], "dateTime": "2025-01-01T00:00:00.000-06:00"},
                    {"value": vals[1], "qualifiers": ["P", "e"], "dateTime": "2025-01-01T00:15:00.000-06:00"},
                ]}],
            })
        return json.dumps({"value": {"timeSeries": series}}).encode()

    monkeypatch.setattr(dl, "_request", fake_request)
    a = datetime(2025, 1, 1, tzinfo=timezone.utc)
    b = datetime(2025, 4, 1, tzinfo=timezone.utc)   # 1 quý => 1 request
    out = dl.fetch_legacy_site_year("USGS-123", ["00300", "00010"], a, b)
    assert [r["time"] for r in out["00300"]] == ["2025-01-01 06:00:00+00:00", "2025-01-01 06:15:00+00:00"]
    assert out["00300"][1]["value"] == ""              # noDataValue -> rỗng
    assert out["00300"][0]["approval_status"] == "Approved"
    assert out["00010"][1]["approval_status"] == "Provisional" and out["00010"][1]["qualifier"] == "e"
