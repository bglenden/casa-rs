"""CASA's scalar integer power, including non-finite values and errors."""
import json
import math
from pathlib import Path

import numpy as np
import pytest

from casars.tools import quanta


def test_power_matches_casa() -> None:
    fixture = json.loads((Path(__file__).parent / "data" / "casa_quanta_power.json").read_text())
    special = {"NaN": math.nan, "Infinity": math.inf, "-Infinity": -math.inf}
    qa = quanta()
    for case in fixture["cases"]:
        value = case["input"]
        if isinstance(value, str):
            value = special.get(value, value)
        if "error" in case:
            expected = case["error"]
            error_type = {"RuntimeError": RuntimeError, "TypeError": TypeError, "SystemError": SystemError}[expected["type"]]
            with pytest.raises(error_type) as error:
                qa.pow(value, case["power"])
            assert str(error.value) == expected["message"], case
        else:
            result = qa.pow(value, case["power"])
            expected = case["result"]
            assert result["unit"] == expected["unit"], case
            number = expected["value"]
            if number == "NaN":
                assert math.isnan(result["value"]), case
            elif number in ("Infinity", "-Infinity"):
                assert result["value"] == float(number), case
            else:
                assert result["value"] == pytest.approx(number, rel=1e-14, abs=0.0), case
                if number == 0:
                    assert math.copysign(1, result["value"]) == math.copysign(1, number), case
    assert qa.pow() == {"unit": "", "value": 0.0}
    assert qa.pow("2m", np.int64(2)) == {"unit": "(m)2", "value": 4.0}
