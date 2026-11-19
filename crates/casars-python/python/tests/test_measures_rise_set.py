"""Compare rise and riseset with recorded CASA 6.7.6.14 results."""

import json
import logging
from pathlib import Path

import pytest

from casars.tools import measures
from test_measures_geometry import snapshot

ORACLE = json.loads(
    (Path(__file__).with_name("data") / "casa_measures_rise_set.json").read_text()
)
LOGGER = "casars.tools.measures"


def assert_record(actual, expected):
    assert actual.keys() == expected.keys()
    for key, value in expected.items():
        if isinstance(value, dict):
            assert_record(actual[key], value)
        elif isinstance(value, float):
            tolerance = 1e-7 if expected.get("unit") == "rad" else 2e-7
            assert actual[key] == pytest.approx(value, abs=tolerance, rel=0)
        else:
            assert actual[key] == value


@pytest.mark.parametrize("method", ["rise", "riseset"])
@pytest.mark.parametrize("case", ORACLE["cases"], ids=lambda case: case["name"])
def test_rise_set_matches_casa(method, case, snapshot, monkeypatch, caplog):
    monkeypatch.setenv("CASA_RS_MEASURESPATH", str(snapshot))
    tool = measures()
    try:
        assert tool.done()
        assert all(tool.doframe(element) for element in case["frame"])
        before = tool.showframe()
        with caplog.at_level(logging.INFO, logger=LOGGER):
            actual = getattr(tool, method)(*case["args"])
        assert_record(actual, case["results"][method])
        assert tool.showframe() == before
        if method == "rise":
            for reference, expected in case.get("direction_controls", {}).items():
                assert_record(tool.measure(case["args"][0], reference), expected)
        if not actual:
            assert any(
                record.levelno == logging.ERROR and record.name == LOGGER
                for record in caplog.records
            )
        elif method == "riseset" and isinstance(actual["rise"], dict):
            assert [m.split("=", 1)[0].strip() for m in caplog.messages] == [
                "LAST of rise", "LAST of  set", "UTC of rise",
            ]
    finally:
        tool.done()


def test_rise_set_without_frame_fails_cleanly(snapshot, monkeypatch):
    monkeypatch.setenv("CASA_RS_MEASURESPATH", str(snapshot))
    tool = measures()
    assert tool.done()
    source = tool.direction("J2000", "10deg", "20deg")
    assert tool.rise(source) == tool.riseset(source) == {}
    assert tool.showframe() == "Frame: "
