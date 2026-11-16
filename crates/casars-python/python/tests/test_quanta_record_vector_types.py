"""CASA numeric vector kinds inside quantity records."""
import json
from pathlib import Path

import numpy as np
import pytest

from casars.tools import quanta


def test_record_vector_types_match_casa() -> None:
    fixture = json.loads((Path(__file__).parent / "data" / "casa_quanta_record_vector_types.json").read_text())
    qa = quanta()
    for case in fixture["cases"]:
        value = case["value"]
        if isinstance(value, dict):
            if "numpy_sequence" in value:
                sequence = [np.dtype(value["dtype"]).type(v) for v in value["numpy_sequence"]]
                value = tuple(sequence) if value["container"] == "tuple" else sequence
            else:
                value = tuple(value["tuple"]) if "tuple" in value else np.array(value["array"], dtype=value["dtype"])
        args = ({"value": value, "unit": case["unit"]},)
        if case["method"] == "add":
            args += ("1m",)
        method = getattr(qa, case["method"])
        if "error" in case:
            with pytest.raises(RuntimeError) as error:
                method(*args)
            assert str(error.value) == case["error"]["message"], case
        else:
            actual = method(*args)
            expected = case["result"]
            if isinstance(expected, dict):
                assert actual["unit"] == expected["unit"], case
                np.testing.assert_equal(actual["value"], expected["value"], err_msg=str(case))
                assert np.ndim(actual["value"]) == np.ndim(expected["value"]), case
            else:
                assert actual == expected, case


def test_integer_arrays_keep_their_values_across_byte_orders() -> None:
    # CASA 6.7.6.14 on little-endian systems corrupts big-endian values (#1183).
    fixture = json.loads((Path(__file__).parent / "data" / "casa_quanta_record_endian.json").read_text())
    assert fixture["byteorder"] == "little"
    qa = quanta()
    for case in fixture["cases"]:
        value = np.array([1], dtype=case["dtype"])
        assert qa.quantity({"value": value, "unit": "m"}) == {"unit": "m", "value": 1.0}
        assert qa.add({"value": value, "unit": "m"}, "1m") == {"unit": "m", "value": 2.0}
