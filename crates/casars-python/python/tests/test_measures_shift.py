"""CASA 6.7.6.14 offsets, direction records, errors, and spherical controls."""
import copy
import json
from pathlib import Path

import pytest

from casars.tools import measures, quanta
from test_measures_geometry import arguments, snapshot

ORACLE = json.loads((Path(__file__).with_name("data") / "casa_measures_shift.json").read_text())


def compare(actual, expected):
    if isinstance(expected, dict):
        assert actual.keys() == expected.keys()
        for key in expected:
            compare(actual[key], expected[key])
    elif isinstance(expected, float):
        assert actual == pytest.approx(expected, rel=2e-12, abs=2e-13)
    else:
        assert actual == expected


@pytest.mark.parametrize("case", ORACLE["cases"], ids=lambda case: case["name"])
def test_shift_matches_casa(case):
    tool = measures()
    assert tool.done()
    try:
        args = arguments(case["args"])
        if "error" in case:
            expected = case["error"]
            error_type = {"RuntimeError": RuntimeError, "TypeError": TypeError,
                          "ValueError": ValueError}[expected["type"]]
            with pytest.raises(error_type) as actual:
                tool.shift(*args)
            # CASA appends the .cc path from its wheel builder.
            assert str(actual.value) == expected["message"].split("\n... thrown by ", 1)[0]
        else:
            actual = tool.shift(*args)
            compare(actual, case.get("native_result", case["result"]))
    finally:
        tool.done()


def test_upstream_ganymede_shift(snapshot, monkeypatch):
    """Port me_test_shift in CASA's test_tool_measures.py."""
    monkeypatch.setenv("CASA_RS_MEASURESPATH", str(snapshot))
    tool = measures()
    qa = quanta()
    try:
        assert tool.framecomet("JPL-Horizons/Ganymede_55437-56293dUTC.tab")
        assert tool.doframe(tool.epoch("UTC", "2011/01/03/17:00:00"))
        assert tool.doframe(tool.observatory("ALMA"))
        original = tool.direction("J2000", "4h20m30s", "+30.20.30")

        north = tool.shift(original, "20arcmin", "0deg")
        expected = copy.deepcopy(original)
        expected["m1"] = qa.add(expected["m1"], "20arcmin")
        assert north == expected

        east = tool.shift(original, "20arcmin", "90deg")
        assert east["m0"]["value"] == pytest.approx(1.1433867531223854, rel=1e-7)
        assert east["m1"]["value"] == pytest.approx(0.5295520783025025, rel=1e-7)

        south = tool.shift(original, "20arcmin", "180deg")
        assert south["m0"]["value"] == original["m0"]["value"]
        expected_latitude = qa.sub(original["m1"], "20arcmin")["value"]
        assert south["m1"]["value"] == pytest.approx(expected_latitude, rel=1e-7)
    finally:
        tool.done()
        qa.done()
