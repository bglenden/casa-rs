"""CASA's sample placeholder and its argument conversion behavior."""
import json
import logging
from pathlib import Path

import numpy as np
import pytest
from casars.tools import componentlist


def inputs():
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
        "realistic": ("J2000 0deg 0deg", "1arcsec", "1arcsec", "1GHz"),
        "huge_second": (None, 2**63), "huge_third": (None, None, 2**63),
        "huge_fourth": (None, None, None, 2**63),
    }


def test_sample_matches_casa_and_preserves_list(caplog):
    fixture = json.loads((Path(__file__).parent / "data" / "casa_componentlist_sample.json").read_text())
    cl = componentlist()
    state = None
    with caplog.at_level(logging.WARNING, logger="casars.tools.componentlist"):
        for case in fixture["cases"]:
            if case["state"] != state:
                state = case["state"]
                if state == "populated":
                    assert cl.addcomponent(label="unchanged")
                elif state == "done":
                    cl.done()
            args = inputs()[case["input"]]
            caplog.clear()
            if "error" in case:
                assert case["error"]["type"] == "SystemError"
                with pytest.raises(SystemError) as error:
                    cl.sample(*args)
                assert str(error.value) == case["error"]["message"]
            else:
                result = cl.sample(*args)
                expected = case["result"]
                assert isinstance(result, np.ndarray)
                assert result.tolist() == expected["value"]
                assert list(result.shape) == expected["shape"]
                assert str(result.dtype) == expected["dtype"]
            assert [record.message for record in caplog.records] == ["sample not implemented yet"]
            assert cl.length() == case["length"]
            if cl.length():
                assert cl.getlabel(0) == case["label"]


def test_cyclic_input_does_not_crash(caplog):
    fixture = json.loads((Path(__file__).parent / "data" / "casa_componentlist_sample.json").read_text())
    assert fixture["cyclic_exit"] == -11
    value = []
    value.append(value)
    with caplog.at_level(logging.WARNING, logger="casars.tools.componentlist"):
        result = componentlist().sample(value)
    assert result.dtype == np.float64 and result.shape == (0,)
    assert [record.message for record in caplog.records] == ["sample not implemented yet"]
