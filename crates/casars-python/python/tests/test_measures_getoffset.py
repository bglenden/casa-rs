"""CASA measure offset inspection and safe value handling."""
import json
from pathlib import Path

import numpy as np
import pytest
from casars.tools import measures

DATA = Path(__file__).parent / "data"
ORACLE = json.loads((DATA / "casa_measures_getoffset.json").read_text())


def test_scalar_measure_offsets_match_casa():
    me = measures()
    for case in ORACLE["cases"]:
        if "error" in case:
            with pytest.raises(TypeError) as error:
                me.getoffset(case["input"])
            assert case["error"]["message"] in str(error.value)
        else:
            assert me.getoffset(case["input"]) == case["result"], case["case"]


def test_offset_values_are_copied_without_losing_integer_types():
    assert ORACLE["large_result"] == 1 and ORACLE["large_input"] == 2**53 + 1
    me = measures()
    measure = me.direction("J2000", "10deg", "20deg")
    offset = me.direction("J2000", "1deg", "2deg")
    offset["extra"] = {"large": 2**53 + 1, "flag": True, "name": "safe"}
    measure["offset"] = offset
    result = me.getoffset(measure)
    assert result == offset
    assert type(result["extra"]["large"]) is int
    assert result["extra"]["large"] == 2**53 + 1
    result["m0"]["value"] = 123.0
    assert offset["m0"]["value"] != 123.0


def test_array_offsets_are_copied_and_cycles_are_refused_safely():
    from test_measures_spectral import CASES, compare
    case = next(c for c in CASES if c['name']=='support_offset_extra_array')
    from copy import deepcopy
    measure = deepcopy(case['args'][0])
    measure['offset']['extra'] = np.array(measure['offset']['extra'])
    me = measures()
    result = me.getoffset(measure)
    compare(result, case['result'])
    result['extra'][0] = -1
    assert measure['offset']['extra'][0] == 1
    assert ORACLE["cycle_returncode"] == -11
    cyclic = {}
    cyclic["self"] = cyclic
    with pytest.raises(ValueError, match="cyclic"):
        me.getoffset(cyclic)
    deep = {}
    current = deep
    for _ in range(130):
        current["next"] = {}
        current = current["next"]
    with pytest.raises(ValueError, match="deeply nested"):
        me.getoffset(deep)


def test_nested_offsets_match_casa():
    from test_measures_spectral import CASES, compare
    case = next(c for c in CASES if c['name']=='support_nested_offset')
    compare(measures().getoffset(case['args'][0]), case['result'])
