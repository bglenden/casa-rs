"""CASA's component-list type-check placeholder and diagnostics."""
import json
import logging
from pathlib import Path

import numpy as np
import pytest

from casars.tools import componentlist


def values():
    return {
        "default": (), "none": (None,), "empty": ([],), "integer": (1,),
        "text": ("x",), "boolean": (True,), "record": ({"a": 1},),
        "array": (np.array([1, 2]),), "array0": (np.array(1.2),),
        "nested": ([[1, 2]],), "bytes": (b"x",), "complex": (1 + 2j,),
        "object": (object(),), "set": ({1, 2},), "u64": (np.uint64(2**63),),
        "huge": (2**63,), "negative_huge": (-2**63 - 1,),
        "huge_list": ([2**80],), "huge_tuple": ((2**80,),),
        "huge_record": ({"a": [2**80]},), "huge_key": ({2**80: 1},),
        "object_array": (np.array([2**80], dtype=object),),
        "huge_after_float": ([1.0, 2**80],), "huge_before_float": ([2**80, 1.0],),
        "huge_after_text": (["x", 2**80],), "huge_after_bool": ([True, 2**80],),
        "huge_after_none": ([None, 2**80],), "huge_nested_float": ([[1.0, 2**80]],),
        "huge_record_float": ({"v": [1.0, 2**80]},),
    }


def test_iscomponentlist_matches_casa_and_preserves_state(caplog):
    fixture = json.loads((Path(__file__).parent / "data" / "casa_componentlist_iscomponentlist.json").read_text())
    cl = componentlist()
    state = None
    with caplog.at_level(logging.WARNING, logger="casars.tools.componentlist"):
        for case in fixture["cases"]:
            if case["state"] != state:
                state = case["state"]
                if state == "populated":
                    assert cl.addcomponent()
                elif state == "done":
                    cl.done()
            args = values()[case["input"]]
            length = cl.length()
            caplog.clear()
            if "error" in case:
                assert case["error"]["type"] == "SystemError"
                with pytest.raises(SystemError) as error:
                    cl.iscomponentlist(*args)
                assert str(error.value) == case["error"]["message"]
            else:
                assert cl.iscomponentlist(*args) is case["result"]
            assert [record.message for record in caplog.records] == ["is_componentlist not implemented yet"]
            assert cl.length() == length


def test_cyclic_python_argument_is_safe(caplog):
    fixture = json.loads((Path(__file__).parent / "data" / "casa_componentlist_iscomponentlist.json").read_text())
    assert fixture["cyclic_exit"] == -11
    value = []
    value.append(value)
    with caplog.at_level(logging.WARNING, logger="casars.tools.componentlist"):
        assert componentlist().iscomponentlist(value) is False
