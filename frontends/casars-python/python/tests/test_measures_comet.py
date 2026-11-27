"""CASA 6.7.6.14 table and lifecycle comparisons for the comet tool."""
import json
import os
from pathlib import Path

import numpy as np
import pytest

from casars.tools import measures, table
from test_measures_geometry import snapshot
from test_measures_rise_set import assert_record
from test_measures_spectral import compare as compare_spectral


ORACLE = json.loads(
    (Path(__file__).with_name("data") / "casa_measures_comet.json").read_text()
)
SYNTHETIC = ORACLE["synthetic"]


@pytest.fixture(scope="module")
def synthetic(tmp_path_factory):
    root = tmp_path_factory.mktemp("measures-comet")
    for name, changes in SYNTHETIC["variants"].items():
        columns = SYNTHETIC["columns"] | SYNTHETIC["column_overrides"].get(name, {})
        description = {key: {"valueType": "string" if isinstance(values[0], str) else "double"}
                       for key, values in columns.items()}
        keywords = SYNTHETIC["keywords"] | changes
        count = SYNTHETIC["row_counts"].get(name, len(columns["MJD"]))
        tool = table()
        try:
            assert tool.create(str(root / f"{name}.tab"), description, nrow=count)
            for column, values in columns.items():
                if isinstance(values[0], str):
                    for row, value in enumerate(values[:count]):
                        assert tool.putcell(column, row, value)
                else:
                    assert tool.putcol(column, np.asarray(values[:count], dtype=float))
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
    if value["form"] == "home":
        return "~/" + os.path.relpath(path, Path.home())
    if value["form"] == "plain_environment":
        return "$CASA_RS_TEST_COMET_ROOT/" + value["ephemeris"]
    if value["form"] == "environment":
        return "${CASA_RS_TEST_COMET_ROOT}/" + value["ephemeris"]
    return path


def compare(actual, expected):
    if isinstance(expected, dict):
        assert actual.keys() == expected.keys()
        assert actual["unit"] == expected["unit"]
        assert np.shape(actual["value"]) == np.shape(expected["value"])
        np.testing.assert_allclose(actual["value"], np.asarray(expected["value"], dtype=float),
                                   rtol=2e-11, atol=0, equal_nan=True)
    else:
        assert actual == expected


@pytest.mark.parametrize("case", ORACLE["cases"], ids=lambda case: case["name"])
def test_comet_states_match_casa(case, snapshot, synthetic, monkeypatch):
    monkeypatch.setenv("CASA_RS_MEASURESPATH", str(snapshot))
    monkeypatch.setenv("CASA_RS_TEST_COMET_ROOT", str(snapshot / "ephemerides"))
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
            expected = step["native_result"] if "native_result" in step else step["result"]
            if name == "measure" and expected.get("type") in ("frequency", "radialvelocity"):
                compare_spectral(result, expected)
            elif name in ("measure", "rise", "riseset"):
                assert_record(result, expected)
            else:
                compare(result, expected)
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


def test_relative_lookup_uses_named_tables_in_both_data_directories(
    snapshot, tmp_path, monkeypatch
):
    bundled = snapshot / "ephemerides/JPL-Horizons/Ganymede_55437-56293dUTC.tab"
    for directory, name in (("ephemerides", "local-eph.tab"), ("geodetic", "local-geo.tab")):
        (tmp_path / directory).mkdir()
        (tmp_path / directory / name).symlink_to(bundled, target_is_directory=True)
    # An ordinary directory or a non-table control file must not hide the
    # readable candidate in the next search location.
    (tmp_path / "ephemerides/local-geo.tab").mkdir()
    (tmp_path / "ephemerides/local-geo.tab/table.dat").write_bytes(b"not a CASA table")
    iers = snapshot / "geodetic/IERSeop2000"
    (tmp_path / "geodetic/IERSeop2000").symlink_to(iers, target_is_directory=True)
    monkeypatch.setenv("CASA_RS_MEASURESPATH", str(tmp_path))
    tool = measures()
    try:
        for name in ("local-eph.tab", "local-geo.tab"):
            assert tool.framecomet(name)
            assert tool.cometname() == "Ganymede"
    finally:
        tool.done()
