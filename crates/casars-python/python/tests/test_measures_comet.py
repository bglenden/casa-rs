"""CASA 6.7.6.14 table and lifecycle comparisons for the comet tool."""
import json
from pathlib import Path

import numpy as np
import pytest

from casars.tools import measures, table
from test_measures_geometry import snapshot


ORACLE = json.loads(
    (Path(__file__).with_name("data") / "casa_measures_comet.json").read_text()
)
SYNTHETIC = ORACLE["synthetic"]


@pytest.fixture(scope="module")
def synthetic(tmp_path_factory):
    root = tmp_path_factory.mktemp("measures-comet")
    columns = SYNTHETIC["columns"]
    description = {key: {"valueType": "double"} for key in columns}
    for name, changes in SYNTHETIC["variants"].items():
        keywords = SYNTHETIC["keywords"] | changes
        count = SYNTHETIC["row_counts"].get(name, len(columns["MJD"]))
        tool = table()
        try:
            assert tool.create(str(root / f"{name}.tab"), description, nrow=count)
            for column, values in columns.items():
                assert tool.putcol(column, np.asarray(values[:count]))
            assert tool.putkeywords({key: value for key, value in keywords.items() if value is not None})
            assert tool.putinfo({"type": "IERS"})
        finally:
            tool.close()
    return root


def decode(value, snapshot, synthetic):
    if not isinstance(value, dict) or not {"ephemeris", "synthetic"} & value.keys():
        return value
    if "synthetic" in value:
        return str(synthetic / (value["synthetic"] + ".tab"))
    path = str(snapshot / "ephemerides" / value["ephemeris"])
    if value["form"] == "bytes":
        return path.encode()
    if value["form"] == "nul":
        return path + "\0ignored"
    return path


def compare(actual, expected):
    if isinstance(expected, dict):
        assert actual.keys() == expected.keys()
        assert actual["unit"] == expected["unit"]
        assert np.shape(actual["value"]) == np.shape(expected["value"])
        np.testing.assert_allclose(actual["value"], expected["value"], rtol=2e-11, atol=0)
    else:
        assert actual == expected


@pytest.mark.parametrize("case", ORACLE["cases"], ids=lambda case: case["name"])
def test_comet_states_match_casa(case, snapshot, synthetic, monkeypatch):
    monkeypatch.setenv("CASA_RS_MEASURESPATH", str(snapshot))
    tool = measures()
    try:
        assert tool.done()
        for step in case["steps"]:
            name = step["method"]
            args = [decode(value, snapshot, synthetic) for value in step["args"]]
            if name == "set_epoch":
                result = tool.doframe(tool.epoch(*args))
            elif name == "set_alma":
                result = tool.doframe(tool.observatory("ALMA"))
            elif "error" in step:
                error_type = {"TypeError": TypeError, "RuntimeError": RuntimeError,
                              "ValueError": ValueError}[step["error"]]
                with pytest.raises(error_type) as error:
                    getattr(tool, name)(*args)
                assert str(error.value) == step["message"]
                continue
            else:
                result = getattr(tool, name)(*args)
            assert type(result).__name__ == step["python_type"]
            compare(result, step["native_result"] if "native_result" in step else step["result"])
    finally:
        tool.done()


def test_upstream_ganymede_distance_and_diameter(snapshot, monkeypatch):
    """Port me_test_cometdist and me_test_cometangdiam in test_tool_measures.py."""
    monkeypatch.setenv("CASA_RS_MEASURESPATH", str(snapshot))
    tool = measures()
    try:
        assert tool.framecomet("JPL-Horizons/Ganymede_55437-56293dUTC.tab")
        assert tool.doframe(tool.epoch("UTC", "2011/01/03/17:00:00"))
        assert tool.doframe(tool.observatory("ALMA"))
        distance = tool.cometdist()
        diameter = tool.cometangdiam()
        assert distance["unit"] == "AU"
        assert distance["value"] == pytest.approx(5.1241, abs=0.001)
        assert diameter["unit"] == "rad"
        assert diameter["value"] == pytest.approx(6.868e-6, abs=1e-9)
    finally:
        tool.done()
