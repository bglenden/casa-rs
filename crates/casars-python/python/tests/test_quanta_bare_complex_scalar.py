"""CASA comparisons and corruption refusal for bare complex scalars."""
import json
import warnings
from pathlib import Path

import numpy as np
import pytest

from casars.tools import quanta


def test_bare_complex_values_do_not_lose_their_imaginary_part() -> None:
    fixture = json.loads((Path(__file__).parent / "data" / "casa_quanta_bare_complex_scalar.json").read_text())
    qa = quanta()
    types = {"python": complex, "complex64": np.complex64, "complex128": np.complex128}
    for case in fixture["cases"]:
        value = types[case["kind"]](1 + 2j)
        args = (value, "1m") if case["method"] == "add" else (value,)
        if case["kind"] == "complex64":
            # CASA returns dimensionless zero (or uses it for arithmetic),
            # discarding both parts; casa-rs refuses the value instead.
            if case["method"] == "quantity":
                assert case["result"] == {"value": 0.0, "unit": ""}
            elif case["method"] == "getunit":
                assert case["result"] == ""
        else:
            assert case["error"]["message"] == "Wrong QuantumHolder to convert asQuantumDouble"
        with warnings.catch_warnings(record=True) as emitted:
            warnings.simplefilter("always")
            with pytest.raises(RuntimeError, match="Wrong QuantumHolder to convert asQuantumDouble"):
                getattr(qa, case["method"])(*args)
        assert not emitted, case
