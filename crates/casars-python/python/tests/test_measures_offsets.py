"""CASA 6.7.6.14 oracle for output reference offsets across measure kinds."""

import json
from pathlib import Path

import pytest

from casars.tools import measures
from test_measures_geometry import snapshot
from test_measures_spectral import compare

ORACLE = json.loads(
    (Path(__file__).with_name("data") / "casa_measures_offsets.json").read_text()
)


@pytest.mark.parametrize("case", ORACLE["cases"], ids=lambda case: case["name"])
def test_reference_offsets_match_casa(case, snapshot, monkeypatch):
    monkeypatch.setenv("CASA_RS_MEASURESPATH", str(snapshot))
    tool = measures()
    try:
        assert tool.done()
        assert all(tool.doframe(item) for item in case.get("frame", ORACLE["frame"]))
        actual = getattr(tool, case["method"])(*case["args"])
        compare(actual, case.get("native_result", case["result"]))
    finally:
        tool.done()
