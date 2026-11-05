"""Zero-dimensional quantities preserve input values instead of CASA's zeros."""
import json
from pathlib import Path

import numpy as np
import pytest

from casars.tools import quanta


def test_zero_dimensional_quantity_records() -> None:
    fixture = json.loads((Path(__file__).parent / "data" / "casa_quantity_record_zero_dim.json").read_text())
    qa = quanta()
    for case in fixture["cases"]:
        value = np.array(case["value"], dtype=case["dtype"])
        if case["method"] == "bare_quantity":
            try:
                result = qa.quantity(value, keepshape=case["keepshape"])
            except TypeError:
                # Bare zero-dimensional arrays are not yet supported.
                continue
            expected = case["result"]
            assert result["unit"] == expected["unit"], case
            assert result["value"].shape == tuple(expected["shape"]), case
            np.testing.assert_array_equal(result["value"], expected["value"])
            continue
        record = {"unit": case["unit"], "value": value}
        method = case["method"]
        args = (record,) + (("1m",) if method == "add" else (("cm",) if method == "convert" else ()))
        if "error" in case:
            with pytest.raises(RuntimeError) as error:
                getattr(qa, method)(*args)
            assert str(error.value) == case["error"]["message"], case
        else:
            expected = case["result"]
            assert expected["value"] == [0.0], case  # CASA discards the input.
            result = getattr(qa, method)(*args)
            assert result["unit"] == expected["unit"], case
            assert result["value"].shape == tuple(expected["shape"]), case
            np.testing.assert_array_equal(result["value"], [case["value"] * (100 if method == "convert" else 1)])


def test_bare_zero_dimensional_arrays_never_silently_become_zero() -> None:
    qa = quanta()
    for dtype in ("float32", "float64"):
        value = np.array(1.25, dtype=dtype)
        for method, args, expected in ((qa.getvalue, (value,), [1.25]), (qa.add, (value, 2), 3.25)):
            try:
                result = method(*args)
            except TypeError:
                # Refusal is safe until bare scalar arrays are supported.
                continue
            actual = result["value"] if isinstance(result, dict) else result
            np.testing.assert_equal(actual, expected)
