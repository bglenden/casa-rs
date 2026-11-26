# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA 6.7.6.14 record imports, including diagnostics and partial mutation."""
import json
from pathlib import Path

import numpy as np
import pytest

from casars.tools import componentlist

ORACLE = json.loads((Path(__file__).parent / "data/casa_componentlist_records.json").read_text())
SPECTRA = json.loads((Path(__file__).parent / "data/casa_componentlist_record_spectra.json").read_text())
ORACLE["cases"].extend(SPECTRA["cases"])


def assert_record(actual, expected):
    if isinstance(expected, dict):
        assert set(actual) == set(expected)
        for name, value in expected.items():
            assert_record(actual[name], value)
    elif isinstance(expected, list) or isinstance(expected, (int, float)) and not isinstance(expected, bool):
        np.testing.assert_allclose(actual, expected, rtol=1e-12, atol=1e-15)
    else:
        assert actual == expected


@pytest.mark.parametrize("case", ORACLE["cases"], ids=lambda case: case["name"])
def test_record_import_matches_casa(case):
    cl = componentlist()
    fromrecord = case["name"].startswith("fromrecord_")
    if fromrecord:
        assert cl.addcomponent(label="existing")
    record = case["record"]
    if case["name"] == "complex":
        record = {**record, "flux": {**record["flux"], "value": np.array(record["flux"]["value"], complex)}}
    method = cl.fromrecord if fromrecord else cl.add
    try:
        if "error" in case:
            with pytest.raises(RuntimeError) as error:
                method(record)
            assert str(error.value) == case["error"]["message"]
        else:
            assert method(record) is case["result"]
        if "length" in case:
            assert cl.length() == case["length"]
        if "after" in case:
            expected = case["after"]
            if case["name"] == "missing_pol_no_error":
                # CASA logs that it defaults to Stokes, then stores Unknown.
                # Preserve the stated default rather than an unusable basis.
                assert expected["flux"]["polarisation"] == "Unknown"
                expected = {**expected, "flux": {**expected["flux"], "polarisation": "Stokes"}}
            assert_record(cl.torecord() if fromrecord else cl.getcomponent(0), expected)
    finally:
        cl.done()


def test_empty_record_defaults_warn_in_order(caplog):
    cl = componentlist()
    assert cl.add({})
    assert caplog.messages == [
        "The component does not have a 'flux' field.\nThe default is 1.0 Jy in I and 0.0 in Q, U & V",
        "The component does not have a 'shape' field.\nThe default is a point component at the J2000 north pole",
        "The component does not have a 'spectrum' field.\nThe default is a constant spectrum",
    ]
    cl.done()


def test_record_concatenate_selection_shape_units_and_persistence(tmp_path):
    source = componentlist()
    assert source.addcomponent(flux=1, label="first", shape="Gaussian", majoraxis="4arcsec", minoraxis="2arcsec", positionangle="30deg")
    assert source.addcomponent(flux=3, label="second")
    record = source.torecord()
    target = componentlist()
    assert target.addcomponent(label="existing")
    assert target.select([0])
    assert target.rename(str(tmp_path / "joined.cl"))
    assert target.concatenate(record, [1, 0, 1])
    assert [target.getlabel(i) for i in range(4)] == ["existing", "second", "first", "second"]
    assert list(target.selected()) == [0]
    assert target.getshape(2)["majoraxis"]["unit"] == "arcsec"
    assert target.getshape(2)["majoraxis"]["value"] == pytest.approx(4)
    assert target.done()
    assert target.open(str(tmp_path / "joined.cl"))
    assert [target.getlabel(i) for i in range(4)] == ["existing", "second", "first", "second"]
    frequency = target.getcomponent(2)["spectrum"]["frequency"]["m0"]
    assert frequency["value"] == pytest.approx(1.42)
    assert frequency["unit"] == "GHz"
    assert target.done()
    source.done()


def test_fromrecord_releases_attached_table_before_overwrite(tmp_path):
    path = str(tmp_path / "old.cl")
    cl = componentlist()
    assert cl.addcomponent(label="saved")
    assert cl.rename(path)
    assert cl.setlabel(0, "latest")
    assert cl.fromrecord({})
    assert cl.length() == 0
    assert cl.open(path)
    assert cl.getlabel(0) == "latest"
    cl.done()


VALIDATION = json.loads((Path(__file__).parent / "data/casa_componentlist_record_validation.json").read_text())


@pytest.mark.parametrize("case", VALIDATION["cases"], ids=lambda c: c["name"])
def test_record_validation_matches_casa_or_refuses_its_abort(case):
    cl = componentlist()
    try:
        if "crash" in case:
            # CASA aborts in GaussianShape's constructor. Keep the process
            # usable and refuse the same invalid geometry atomically.
            assert case["crash"]["returncode"] == -6
            with pytest.raises(RuntimeError):
                cl.add(case["record"])
            assert cl.length() == 0
        elif "error" in case:
            with pytest.raises(RuntimeError) as error:
                cl.add(case["record"])
            # Exclude CASA's log timestamp and C++ build path/line number.
            expected = case["error"]["message"].split("\n... thrown by")[0]
            expected = expected.split("\t", 3)[-1]
            assert str(error.value) == expected
            assert cl.length() == 0
        else:
            assert cl.add(case["record"]) is case["result"]
            assert_record(cl.getcomponent(0), case["after"])
    finally:
        cl.done()


@pytest.mark.parametrize("attached", [False, True])
def test_rename_empty_name_means_current_directory_in_casa(attached, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    cl = componentlist()
    try:
        assert cl.addcomponent(label="kept")
        if attached:
            assert cl.rename("source.cl")
            assert cl.putkeyword("example", 42) is None
        with pytest.raises(RuntimeError) as error:
            cl.rename("")
        assert str(error.value) == f"Table {tmp_path} already exists"
        assert cl.getlabel(0) == "kept"
        if attached:
            assert (tmp_path / "source.cl").is_dir()
            assert cl.getkeyword("example") == 42
    finally:
        cl.done()
