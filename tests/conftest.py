import os

import numpy as np
import pandas as pd
import pytest

from wq import contracts


@pytest.fixture(scope="session")
def spark():
    os.environ.setdefault("WQ_SPARK_JARS_BUNDLED", "1")  # test không cần connector Kafka/JDBC
    from wq.spark_utils import build_spark

    s = build_spark("wq-tests", master="local[2]", extra_conf={"spark.sql.shuffle.partitions": "2"})
    yield s
    s.stop()


@pytest.fixture
def sample_df() -> pd.DataFrame:
    """2 trạm x 2 ngày dữ liệu 15 phút, DO có chu kỳ ngày, có lỗ hổng."""
    ts = pd.date_range("2025-07-01", periods=2 * contracts.STEPS_PER_DAY, freq="15min", tz="UTC")
    rows = []
    for sid, base in [("S1", 8.0), ("S2", 6.0)]:
        do = base + np.sin(np.arange(len(ts)) * 2 * np.pi / contracts.STEPS_PER_DAY)
        df = pd.DataFrame({"station_id": sid, "ts": ts, "dissolved_oxygen": do, "water_temp": 25.0,
                           "specific_conductance": 300.0, "ph": 7.5, "turbidity": 5.0, "discharge": np.nan})
        rows.append(df)
    out = pd.concat(rows, ignore_index=True)
    out.loc[10:13, "dissolved_oxygen"] = np.nan      # lỗ hổng ngắn (1 giờ) ở S1
    out.loc[100:140, "dissolved_oxygen"] = np.nan    # lỗ hổng dài (~10 giờ) ở S1
    return out
