import json

import numpy as np
import pandas as pd

from ingestion.replay_producer import serialize_bodies
from wq import contracts


def test_serialized_messages_are_valid_json_with_nulls():
    df = pd.DataFrame({"station_id": ["S1"], "ts": pd.to_datetime(["2025-07-01T00:15Z"], utc=True)})
    for i, v in enumerate(contracts.VARIABLE_NAMES):
        df[v] = [np.nan if i % 2 else 1.5 + i]
    body = serialize_bodies(df)[0]
    msg = json.loads('{"station_id":"S1"' + body + ',"producer_ts":1,"seq":0}')
    assert msg["event_time"] == "2025-07-01T00:15:00Z"
    assert set(contracts.SENSOR_MESSAGE_FIELDS) == set(msg)
    assert msg[contracts.VARIABLE_NAMES[1]] is None and msg[contracts.VARIABLE_NAMES[0]] == 1.5
