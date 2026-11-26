"""CASA's component-list edit placeholder, warnings and argument errors."""

import json
import logging
from pathlib import Path

import numpy as np
import pytest

from casars.tools import componentlist

ORACLE = json.loads((Path(__file__).parent / "data" / "casa_componentlist_edit.json").read_text())


def value(item):
    if isinstance(item, dict) and "numpy" in item:
        return getattr(np, item["numpy"])(item["value"])
    return item


def test_edit_matches_casa_and_leaves_components_untouched(caplog):
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
                        cl.edit(*args, **kwargs)
                    assert str(error.value) == expected["message"], case
                else:
                    assert cl.edit(*args, **kwargs) is case["result"], case
                warns = "error" not in case or case["error"]["type"] == "SystemError"
                assert caplog.messages == (["edit  not implemented yet"] if warns else []), case
            if before is not None:
                assert cl.getshape(0) == before
            assert cl.done()


def test_edit_converts_an_index_once():
    class Index:
        calls = 0

        def __index__(self):
            self.calls += 1
            return 0

    cl = componentlist()
    index = Index()
    assert cl.edit(index) is ORACLE["index_once"]["result"]
    assert index.calls == ORACLE["index_once"]["calls"]
    assert cl.done()
