"""CASA's angle normalization, including non-finite values and errors."""
import json
import math
from pathlib import Path

import numpy as np
import pytest

from casars.tools import quanta


def test_normalize_matches_casa() -> None:
    fixture = json.loads((Path(__file__).parent / "data" / "casa_quanta_normalize.json").read_text())
    special = {"NaN": math.nan, "Infinity": math.inf, "-Infinity": -math.inf}
    qa = quanta()
    for case in fixture["cases"]:
        value, origin = case["input"], case["origin"]
        if isinstance(value, dict) and "special" in value:
            value = {"value": special[value["special"]], "unit": value["unit"]}
        if isinstance(origin, dict) and "special" in origin:
            origin = special[origin["special"]]
        if "error" in case:
            expected = case["error"]
            error_type = {"RuntimeError": RuntimeError, "SystemError": SystemError}[expected["type"]]
            with pytest.raises(error_type) as error:
                qa.norm(value, origin)
            assert str(error.value) == expected["message"], case
        else:
            result = qa.norm(value, origin)
            expected = case["result"]
            assert result["unit"] == expected["unit"], case
            number = expected["value"]
            if number == "NaN":
                assert math.isnan(result["value"]), case
            elif number in ("Infinity", "-Infinity"):
                assert result["value"] == float(number), case
            else:
                assert result["value"] == pytest.approx(number, rel=1e-14, abs=1e-13), case
                if number == 0:
                    assert math.copysign(1, result["value"]) == math.copysign(1, number), case
    assert qa.norm("10deg") == {"unit": "deg", "value": 10.0}
    assert qa.norm("10deg", np.float64(0.5))["value"] == pytest.approx(370.0)
