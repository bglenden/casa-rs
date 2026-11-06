"""CASA's convertshape placeholder, including its argument errors."""

import json
import logging
from pathlib import Path

import pytest

from casars.tools import componentlist

ORACLE = json.loads((Path(__file__).parent / "data" / "casa_componentlist_convertshape.json").read_text())
CASES = ORACLE["cases"]


def value(item):
    if isinstance(item, dict) and "bytes" in item:
        return bytes(item["bytes"])
    return item


def test_convertshape_matches_casa_and_leaves_components_untouched(caplog):
    with caplog.at_level(logging.WARNING, logger="casars.tools.componentlist"):
        for state in ("empty", "populated", "closed"):
            cl = componentlist()
            if state != "empty":
                assert cl.addcomponent(flux=1, dir="J2000 00h00m00s +00d00m00s", shape="Gaussian",
                                       majoraxis="2arcmin", minoraxis="1arcmin", positionangle="20deg")
            if state == "closed":
                assert cl.done()
            before = cl.getshape(0) if state == "populated" else None
            for case in CASES:
                if case["state"] != state:
                    continue
                caplog.clear()
                args = [value(arg) for arg in case["args"]]
                kwargs = {key: value(arg) for key, arg in case["kwargs"].items()}
                if "error" in case:
                    expected = case["error"]
                    error_type = {"TypeError": TypeError, "SystemError": SystemError}[expected["type"]]
                    with pytest.raises(error_type) as error:
                        cl.convertshape(*args, **kwargs)
                    assert str(error.value) == expected["message"], case
                else:
                    assert cl.convertshape(*args, **kwargs) is case["result"], case
                warns = "error" not in case or case["error"]["type"] == "SystemError"
                assert caplog.messages == (["convertshape not implemented yet"] if warns else []), case
            if before is not None:
                assert cl.getshape(0) == before
            assert cl.done()


def test_convertshape_converts_an_index_once():
    # CASA calls __index__ once; calling it twice can change the argument.
    class Index:
        calls = 0

        def __index__(self):
            self.calls += 1
            return 0

    cl = componentlist()
    index = Index()
    assert cl.convertshape(index) is ORACLE["index_once"]["result"]
    assert index.calls == ORACLE["index_once"]["calls"]
    assert cl.done()
