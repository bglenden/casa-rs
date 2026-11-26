"""Compare quantity-record sequence edge cases against CASA 6.7.6.14."""

import json
from pathlib import Path

import numpy as np
import pytest

from casars.tools import measures, quanta


FIXTURE = json.loads(
    (Path(__file__).with_name("data") / "casa_quanta_record_sequence_edges.json").read_text()
)


def decode(value):
    if isinstance(value, list):
        return [decode(item) for item in value]
    if isinstance(value, dict):
        if "tuple" in value:
            return tuple(decode(item) for item in value["tuple"])
        if "array" in value:
            return np.array(value["array"], dtype=value["dtype"])
        if "complex" in value:
            return complex(*value["complex"])
        if "timedelta" in value:
            return np.timedelta64(*value["timedelta"])
        if "int_power" in value:
            return pow(*value["int_power"])
        return np.dtype(value["dtype"]).type(value["scalar"])
    return value


@pytest.mark.parametrize("case", FIXTURE["cases"], ids=lambda case: f"{case['name']}-{case['method']}")
def test_quantity_record_sequence_edges(case):
    qa = quanta()
    args = ({"value": decode(case["value"]), "unit": case["unit"]},)
    if case["method"] == "add":
        args += ("1m",)
    method = getattr(qa, case["method"])
    error = case.get("native_error", case.get("error"))
    if error is not None:
        error_type = {"RuntimeError": RuntimeError, "OverflowError": OverflowError}[error["type"]]
        with pytest.raises(error_type) as exc:
            method(*args)
        assert str(exc.value) == error["message"]
        return
    expected = case["result"]
    actual = method(*args)
    if isinstance(expected, dict):
        assert actual["unit"] == expected["unit"]
        assert np.ndim(actual["value"]) == np.ndim(expected["value"])
        np.testing.assert_equal(actual["value"], expected["value"])
    else:
        assert actual == expected


def test_shared_measure_record_retains_existing_values():
    record = {
        "type": "frequency",
        "refer": "LSRK",
        "m0": {"unit": "Hz", "value": [1.0, np.uint64(2)]},
    }
    result = measures().measure(record, "LSRK")
    assert result["m0"]["unit"] == "Hz"
    np.testing.assert_equal(result["m0"]["value"], [1.0, 2.0])
