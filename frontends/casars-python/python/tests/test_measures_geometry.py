"""Independent CASA 6.7.6.14 geometry-tool acceptance."""
import json
import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pytest

from casars.tools import measures

ORACLE = json.loads(
    (Path(__file__).with_name("data") / "casa_measures_geometry.json").read_text()
)
CASES = ORACLE["cases"]
COMPONENTS = ("m0", "m1", "m2")


def arguments(value):
    if isinstance(value, dict):
        if set(value) == {"__python_bytes__"}:
            return bytes(value["__python_bytes__"])
        return {key: arguments(item) for key, item in value.items()}
    if isinstance(value, list):
        return [arguments(item) for item in value]
    return value


def assert_close(actual, expected):
    np.testing.assert_allclose(actual, expected, rtol=2e-11, atol=2e-7, equal_nan=True)


def cartesian(record):
    lon, lat, radius = [np.asarray(record[key]["value"]) for key in COMPONENTS]
    return np.array([
        radius * np.cos(lat) * np.cos(lon),
        radius * np.cos(lat) * np.sin(lon),
        radius * np.sin(lat),
    ])


def compare(actual, expected):
    if isinstance(expected, dict):
        assert actual.keys() == expected.keys()
        kind = expected.get("type")
        if kind in ("baseline", "uvw", "earthmagnetic") and all(
            key in expected for key in COMPONENTS
        ):
            for key in COMPONENTS:
                assert actual[key].keys() == expected[key].keys()
                assert actual[key]["unit"] == expected[key]["unit"]
                assert np.shape(actual[key]["value"]) == np.shape(expected[key]["value"])
            if kind == "earthmagnetic":
                values = np.array([expected[key]["value"] for key in COMPONENTS])
                result = np.array([actual[key]["value"] for key in COMPONENTS])
                tolerance = 2e-11 * np.max(np.abs(values), initial=0.0)
                np.testing.assert_allclose(result, values, rtol=2e-11, atol=tolerance)
            else:
                # A zero-length vector has undefined spherical angles.
                assert_close(cartesian(actual), cartesian(expected))
            for name, value in expected.items():
                if name not in COMPONENTS:
                    compare(actual[name], value)
        else:
            for name, value in expected.items():
                compare(actual[name], value)
    elif isinstance(expected, (list, tuple)):
        assert len(actual) == len(expected)
        for left, right in zip(actual, expected):
            compare(left, right)
    elif isinstance(expected, bool):
        assert actual is expected
    elif isinstance(expected, (int, float)):
        assert_close(actual, expected)
    else:
        assert actual == expected


@pytest.fixture(scope="module")
def snapshot(tmp_path_factory):
    # Earth orientation affects the strict position-to-baseline comparison.
    # Match the reference epoch, ephemerides and EOP tables on every host.
    provenance_name = ".casa-rs-measures-provenance.json"
    if configured := os.environ.get("CASA_RS_MEASURESPATH"):
        configured = Path(configured)
        provenance = configured / provenance_name
        if provenance.is_file() and json.loads(provenance.read_text()) == ORACLE["measures_snapshot"]:
            return configured
    repo = Path(__file__).resolve().parents[4]
    root = tmp_path_factory.mktemp("geometry-measures") / "snapshot"
    subprocess.run(
        [sys.executable, str(repo / "scripts/install-measures-snapshot.py"), str(root)],
        check=True,
    )
    assert json.loads((root / provenance_name).read_text()) == ORACLE["measures_snapshot"], (
        "regenerate the fixture for the bundled measures snapshot"
    )
    return root


@pytest.mark.parametrize("case", CASES, ids=lambda case: case["name"])
def test_geometry_matches_independent_casa(case, snapshot, monkeypatch):
    monkeypatch.setenv("CASA_RS_MEASURESPATH", str(snapshot))
    tool = measures()
    try:
        tool.done()
        for frame in case.get("frame", []):
            assert tool.doframe(arguments(frame))
        operation = getattr(tool, case["method"])
        args = arguments(case["args"])
        kwargs = arguments(case["kwargs"])
        if "error" in case:
            error_types = {
                "TypeError": TypeError,
                "RuntimeError": RuntimeError,
                "ValueError": ValueError,
            }
            with pytest.raises(error_types[case["error"]["type"]]) as error:
                operation(*args, **kwargs)
            assert str(error.value) == case["error"]["message"]
        else:
            actual = operation(*args, **kwargs)
            assert type(actual).__name__ == case["python_type"]
            compare(actual, case.get("native_result", case["result"]))
    finally:
        tool.done()
