"""CASA's malformed quantity records and scalar-versus-array behavior."""

import json
from pathlib import Path

import numpy as np
import pytest

from casars.tools import quanta


def test_malformed_records_match_casa() -> None:
    fixture = json.loads((Path(__file__).parent / "data" / "casa_quanta_malformed_records.json").read_text())
    qa = quanta()
    for case in fixture["cases"]:
        method = getattr(qa, case["method"])
        if "error" in case:
            with pytest.raises(RuntimeError) as error:
                method(*case["args"])
            assert str(error.value) == case["error"]["message"], case
        else:
            result = method(*case["args"])
            expected = case["result"]
            if isinstance(expected, dict):
                assert result["unit"] == expected["unit"]
                np.testing.assert_array_equal(result["value"], expected["value"])
            else:
                assert result == expected
    # These complex cases were also recorded against CASA; complex values are
    # valid holders but cannot be converted to one real double.
    for value in (1 + 0j, [1 + 0j], [1 + 1j, 2 + 2j]):
        for method, rest in ((qa.quantity, ()), (qa.getunit, ()), (qa.add, (1.0,))):
            with pytest.raises(RuntimeError, match="^Wrong QuantumHolder to convert asQuantumDouble$"):
                method({"value": value, "unit": "m"}, *rest)
