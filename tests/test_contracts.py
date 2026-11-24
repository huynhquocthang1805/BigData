import pandas as pd

from wq import contracts


def test_variable_names_unique_and_bits():
    names = contracts.VARIABLE_NAMES
    assert len(names) == len(set(names))
    assert sorted(contracts.VARIABLE_BIT.values()) == [1 << i for i in range(len(names))]
    mask = contracts.qc_bitmask(["ph", "turbidity"])
    assert contracts.decode_bitmask(mask) == ["ph", "turbidity"]


def test_splits_are_contiguous_and_ordered():
    bounds = [contracts.SPLITS[k] for k in ("train", "valid", "test")]
    assert bounds[0][0] == contracts.DATA_START and bounds[-1][1] == contracts.DATA_END
    for (a0, a1), (b0, b1) in zip(bounds, bounds[1:]):
        assert a1 == b0 and pd.Timestamp(a0) < pd.Timestamp(a1) < pd.Timestamp(b1)


def test_alert_rules_reference_known_variables():
    for r in contracts.ALERT_RULES:
        assert r.parameter in contracts.VARIABLE_NAMES
        assert r.op in {"<", ">"} and r.severity in {"warning", "critical"}
        lo, hi = contracts.VALID_RANGES[r.parameter]
        assert lo <= r.threshold <= hi


def test_forecast_targets_are_core_variables():
    assert set(contracts.FORECAST_TARGETS) <= set(contracts.CORE_VARIABLES)
    assert all(h > 0 for h in contracts.FORECAST_HORIZONS_STEPS)
