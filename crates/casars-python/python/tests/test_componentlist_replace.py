"""CASA's component-list replace placeholder and its argument handling."""

import json
import logging
from pathlib import Path

import numpy as np
import pytest

from casars.tools import componentlist

ORACLE = json.loads((Path(__file__).parent / "data" / "casa_componentlist_replace.json").read_text())


def value(item):
    if isinstance(item, dict) and "__numpy__" in item:
        if item["__numpy__"] == "array":
            return np.array(item["value"], dtype=item.get("dtype"))
        return getattr(np, item["__numpy__"])(value(item["value"]))
    if isinstance(item, dict) and "__bytes__" in item:
        return bytes(item["__bytes__"])
    if isinstance(item, dict) and "__complex__" in item:
        return complex(*item["__complex__"])
    if isinstance(item, dict) and "__object__" in item:
        return object()
    if isinstance(item, dict):
        return {k: value(v) for k, v in item.items()}
    if isinstance(item, list):
        return [value(v) for v in item]
    return item


def test_replace_matches_casa_and_leaves_components_untouched(caplog):
    with caplog.at_level(logging.WARNING, logger="casars.tools.componentlist"):
        for state in ("empty", "populated", "closed"):
            cl = componentlist()
            if state != "empty":
                assert cl.addcomponent(flux=1, dir="J2000 00h00m00s +00d00m00s", shape="Gaussian",
                                       majoraxis="2arcmin", minoraxis="1arcmin", positionangle="20deg")
            if state == "closed":
                assert cl.done()
            before = cl.getshape(0) if state == "populated" else None
            for case in ORACLE["cases"]:
                if case["state"] != state:
                    continue
                caplog.clear()
                args = [value(arg) for arg in case["args"]]
                kwargs = {key: value(arg) for key, arg in case["kwargs"].items()}
                if "error" in case:
                    expected = case["error"]
                    error_type = {"TypeError": TypeError, "SystemError": SystemError}[expected["type"]]
                    with pytest.raises(error_type) as error:
                        cl.replace(*args, **kwargs)
                    assert str(error.value) == expected["message"], case
                else:
                    assert cl.replace(*args, **kwargs) is case["result"], case
                warns = "error" not in case or case["error"]["type"] == "SystemError"
                assert caplog.messages == (["replace not implemented yet"] if warns else []), case
            if before is not None:
                assert cl.getshape(0) == before
            assert cl.done()


def test_replace_converts_an_index_once():
    class Index:
        calls = 0

        def __index__(self):
            self.calls += 1
            return 0

    cl = componentlist()
    index = Index()
    assert cl.replace(index, {}) is ORACLE["index_once"]["result"]
    assert index.calls == ORACLE["index_once"]["calls"]
    other = Index()
    with pytest.raises(SystemError) as error:
        cl.replace(0, {}, other)
    assert str(error.value) == ORACLE["index_whichones"]["error"]["message"]
    assert other.calls == ORACLE["index_whichones"]["calls"]
    assert cl.done()


def test_replace_avoids_casa_crashes():
    cl = componentlist()
    record = {}
    record["self"] = record
    assert cl.replace(0, record) is False
    indices = []
    indices.append(indices)
    with pytest.raises(ValueError, match="cyclic component selection"):
        cl.replace(0, {}, indices)
    for number in (float("nan"), float("inf"), -float("inf")):
        with pytest.raises(ValueError, match="non-finite component selection"):
            cl.replace(0, {}, number)
    assert cl.done()
