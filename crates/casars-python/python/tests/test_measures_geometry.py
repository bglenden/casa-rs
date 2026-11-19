"""Independent CASA 6.7.6.14 geometry-tool acceptance."""
import json
from pathlib import Path

import numpy as np
import pytest

from casars.tools import measures

CASES = json.loads(
    (Path(__file__).with_name("data") / "casa_measures_geometry.json").read_text()
)["cases"]
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
        if expected.get("type") in ("baseline", "uvw") and all(
            key in expected for key in COMPONENTS
        ):
            # Compare physical vectors without depending on undefined angles
            # when the output reference offset leaves a zero-length vector.
            for key in COMPONENTS:
                assert actual[key].keys() == expected[key].keys()
                assert actual[key]["unit"] == expected[key]["unit"]
                assert np.shape(actual[key]["value"]) == np.shape(expected[key]["value"])
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


@pytest.mark.parametrize("case", CASES, ids=lambda case: case["name"])
def test_geometry_matches_independent_casa(case):
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
