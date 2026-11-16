"""Empty NumPy vectors retain their quantity type, as in CASA."""
import json
from pathlib import Path

import numpy as np
import pytest

from casars.tools import quanta


def test_empty_record_arrays_match_casa() -> None:
    fixture = json.loads((Path(__file__).parent / "data" / "casa_quantity_record_empty_array.json").read_text())
    qa = quanta()
    for case in fixture["cases"]:
        record = {"value": np.array([], dtype=case["dtype"]), "unit": case["unit"]}
        method = case["method"]
        args = (record,) + (("1m",) if method == "add" else (("cm",) if method == "convert" else ()))
        if "error" in case:
            with pytest.raises(RuntimeError) as error:
                getattr(qa, method)(*args)
            assert str(error.value) == case["error"]["message"], case
            continue
        actual = getattr(qa, method)(*args)
        expected = case["result"]
        if isinstance(expected, dict) and "value" in expected:
            assert actual["unit"] == expected["unit"], case
            assert actual["value"].shape == tuple(expected["shape"]), case
            np.testing.assert_array_equal(actual["value"], expected["value"])
        elif isinstance(expected, dict):
            assert actual.shape == tuple(expected["shape"]), case
            np.testing.assert_array_equal(actual, expected["array"])
        else:
            assert actual == expected, case
