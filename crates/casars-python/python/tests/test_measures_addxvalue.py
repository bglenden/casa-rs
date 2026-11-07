"""CASA extra measure values and explicit unsupported cases."""
import json
from pathlib import Path

import numpy as np
import pytest
from casars.tools import measures

ORACLE = json.loads((Path(__file__).parent / "data" / "casa_measures_addxvalue.json").read_text())


def test_scalar_extra_values_match_casa():
    me = measures()
    for case in ORACLE["cases"]:
        if "error" in case:
            with pytest.raises(TypeError) as error:
                me.addxvalue(case["input"])
            assert str(error.value) == case["error"]["message"]
        else:
            result = me.addxvalue(case["input"])
            expected = case["result"]
            assert result.keys() == expected.keys(), case["case"]
            if expected:
                assert result["unit"] == expected["unit"]
                assert result["value"].dtype == np.float64
                assert result["value"].shape == (3,)
                np.testing.assert_allclose(result["value"], expected["value"], rtol=1e-14, atol=1e-16)


def test_unrepresented_measures_are_refused():
    me = measures()
    direction = me.direction("J2000", "10deg", "20deg")
    offset = me.direction("J2000", "1deg", "2deg")
    with pytest.raises(NotImplementedError, match="#1053"):
        me.addxvalue(dict(direction, offset=offset))
    with pytest.raises(NotImplementedError, match="#1053"):
        me.addxvalue(dict(direction, m0={"value": [0.1, 0.2], "unit": "rad"}))
    with pytest.raises(NotImplementedError, match="#1053"):
        me.addxvalue({"type": "baseline", "refer": "ITRF"})


def test_cyclic_and_deep_records_are_refused_safely():
    assert ORACLE["cycle_returncode"] == -11
    cyclic = {}
    cyclic["self"] = cyclic
    with pytest.raises(ValueError, match="cyclic"):
        measures().addxvalue(cyclic)
    deep = {}
    current = deep
    for _ in range(130):
        current["next"] = {}
        current = current["next"]
    with pytest.raises(ValueError, match="deeply nested"):
        measures().addxvalue(deep)
