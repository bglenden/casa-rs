"""Exact CASA 6.7.6.14 frame-display output and direct conversion controls."""
import json
from pathlib import Path

import numpy as np
import pytest

from casars.tools import measures
from test_measures_geometry import snapshot

ORACLE = json.loads(
    (Path(__file__).with_name("data") / "casa_measures_showframe.json").read_text()
)


@pytest.mark.parametrize("case", ORACLE["cases"], ids=lambda case: case["name"])
def test_showframe_matches_casa(case, snapshot, monkeypatch):
    monkeypatch.setenv("CASA_RS_MEASURESPATH", str(snapshot))
    tool = measures()
    try:
        for element, expected in zip(case["setup"], case["setup_results"], strict=True):
            if element == "done":
                result = tool.done()
            elif isinstance(element, str):
                result = tool.framecomet(element)
            else:
                result = tool.doframe(element)
            assert result is expected

        if "native_result" in case:
            assert tool.showframe() == case["native_result"]
        elif "error" in case:
            with pytest.raises(RuntimeError) as error:
                tool.showframe()
            assert str(error.value) == case["error"]["message"]
        else:
            assert tool.showframe() == case["result"]
        for source, target, expected in case.get("conversion_controls", ()):
            actual = tool.measure(source, target)
            assert actual["refer"] == expected["refer"]
            for coordinate in expected.keys() - {"type", "refer"}:
                assert actual[coordinate]["unit"] == expected[coordinate]["unit"]
                tolerance = 1e-8 if expected["type"] == "direction" else 1e-9
                np.testing.assert_allclose(
                    actual[coordinate]["value"], expected[coordinate]["value"],
                    rtol=0, atol=tolerance,
                )
    finally:
        tool.done()


def test_cached_display_tracks_frame_updates(snapshot, monkeypatch):
    monkeypatch.setenv("CASA_RS_MEASURESPATH", str(snapshot))
    tool = measures()
    try:
        for step in ORACLE["transitions"]:
            element = step["element"]
            if element == "done":
                result = tool.done()
            elif isinstance(element, str):
                result = tool.framecomet(element)
            else:
                result = tool.doframe(element)
            assert result is step["setup_result"]
            assert tool.showframe() == step["result"]
            assert tool.showframe() == step["result"]

        assert tool.framenow()
        assert tool.showframe().startswith("Frame: Epoch: ")
    finally:
        tool.done()
