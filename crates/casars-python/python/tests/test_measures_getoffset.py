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


def test_unsupported_arrays_and_cycles_are_refused_safely():
    me = measures()
    measure = me.direction("J2000", "10deg", "20deg")
    offset = me.direction("J2000", "1deg", "2deg")
    offset["extra"] = np.array([1, 2])
    measure["offset"] = offset
    with pytest.raises(NotImplementedError, match="#1053"):
        me.getoffset(measure)
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


def test_nested_offsets_are_explicitly_unimplemented():
    me = measures()
    measure = me.direction("J2000", "10deg", "20deg")
    offset = me.direction("J2000", "1deg", "2deg")
    offset["offset"] = me.direction("J2000", "3deg", "4deg")
    measure["offset"] = offset
    with pytest.raises(NotImplementedError, match="#1053"):
        me.getoffset(measure)
