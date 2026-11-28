import pandas as pd
import pytest
from fastapi import HTTPException

from api import main
from wq.models.forecast_experiment import evaluate_forecast


def test_sensor_forecast_uses_last_measurement_of_selected_channel(monkeypatch):
    ts = pd.date_range("2025-07-01", periods=110, freq="15min", tz="UTC")
    rows = [{"station_id": "S1", "ts": t, "ph": 6 + i / 100 if i < 105 else None, "dissolved_oxygen": 15.0}
            for i, t in enumerate(ts)]
    monkeypatch.setattr(main, "q", lambda *_: rows)
    forecast = main.forecast_on_demand("S1", history_hours=48, target="ph")
    preds = forecast["predictions"]
    assert forecast["last_observed_at"] == ts[104]
    assert forecast["forecaster"] == "seasonal_naive"
    assert [row["horizon_min"] for row in preds] == [60, 180, 360, 1440]
    for row, steps in zip(preds, (4, 12, 24, 96), strict=True):
        assert row["target"] == "ph"
        assert row["issued_at"] == ts[104]
        assert row["target_ts"] == ts[104] + pd.Timedelta(minutes=15 * steps)
        assert row["y_pred"] == pytest.approx(rows[104 + steps - 96]["ph"])
    assert main.forecast_on_demand("S1", history_hours=48, target="turbidity")["predictions"] == []
    with pytest.raises(HTTPException) as exc:
        main.forecast_on_demand("S1", model="ridge", history_hours=48, target="ph")
    assert exc.value.status_code == 400


def test_lab_evaluates_selected_channel_independently_from_do():
    ts = pd.date_range("2024-01-01", periods=768, freq="15min", tz="UTC")
    df = pd.DataFrame({"ts": ts, "ph": 7.25, "dissolved_oxygen": float("nan")})
    scores, preview, models = evaluate_forecast(df[:384], df[384:], target="ph", include_preview=True)
    assert set(models) == {60, 180, 360, 1440}
    assert len(scores) == 12 and (scores.rmse == 0).all()
    assert (preview.observed == 7.25).all()
    assert evaluate_forecast(df[:384], df[384:])[0].empty
