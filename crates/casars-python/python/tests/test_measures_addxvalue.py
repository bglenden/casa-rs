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


def test_extra_values_read_direction_records_with_offsets():
    me = measures()
    from test_measures_spectral import CASES
    case = next(c for c in CASES if c['name']=='support_direction_offset_addxvalue')
    result = me.addxvalue(case['args'][0])
    assert result['unit'] == case['result']['unit']
    np.testing.assert_allclose(result['value'], case['result']['value'], rtol=1e-14, atol=1e-16)


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
