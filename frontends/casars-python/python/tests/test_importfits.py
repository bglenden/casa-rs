# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA task coverage and readback for the shared FITS importer."""

import json
import os
from pathlib import Path
import subprocess

import numpy as np
import pytest

from casars import tasks, tools


ROOT = Path(__file__).resolve().parents[4]
DATA = ROOT / "libs/casacore/casa-images/tests/data/importfits"
CASA_PYTHON = Path(os.environ.get("CASA_RS_CASA_PYTHON", "/Applications/CASA.app/Contents/MacOS/python3"))
ORACLE = ROOT / "tools/casa-compat"
REFERENCE = json.loads((DATA / "expected.json").read_text())
assert REFERENCE["casa_version"] == "6.7.6.14"
CASES = REFERENCE["cases"]




def run(name, product):
    case = CASES[name]
    return tasks.importfits(DATA / case["fitsimage"], product, **case["params"])


def test_public_importfits_task(tmp_path):
    for name in ("selected_hdu", "beam_override", "default_stokes_and_spectral", "numeric_direction"):
        case = CASES[name]
        product = tmp_path / f"{name}.im"
        assert run(name, product) is None
        tool = tools.image()
        assert tool.open(str(product))
        try:
            assert tool.shape().tolist() == case["shape"]
            np.testing.assert_array_equal(tool.getchunk().ravel(order="F"), case["pixels"])
            np.testing.assert_array_equal(tool.getchunk(getmask=True).ravel(order="F"), case["mask"])
        finally:
            tool.done()


def test_importfits_preserves_existing_output_on_failure(tmp_path):
    product = tmp_path / "existing.im"
    fits = DATA / "plane.fits"
    tasks.importfits(fits, product)
    data = (product / "table.dat").read_bytes()
    with pytest.raises(RuntimeError, match="already exists"):
        tasks.importfits(fits, product)
    with pytest.raises(TypeError, match="valid|angle"):
        tasks.importfits(fits, product, beam=["0.5mas", "0.3mas", "invalid"], overwrite=True)
    with pytest.raises(RuntimeError, match="Position angle must have angular units"):
        tasks.importfits(fits, product, beam=["5arcsec", "2arcsec", "1h"], overwrite=True)
    assert (product / "table.dat").read_bytes() == data
    tasks.importfits(fits, product, beam=["11arcsec", "7arcsec", "35deg"], overwrite=True)
    tool = tools.image()
    assert tool.open(str(product))
    assert tool.restoringbeam()["major"]["value"] == 11
    tool.done()

    input_copy = tmp_path / "input.fits"
    input_copy.write_bytes(fits.read_bytes())
    with pytest.raises(RuntimeError, match="contains the input"):
        tasks.importfits(input_copy, tmp_path, overwrite=True)
    assert input_copy.read_bytes() == fits.read_bytes()


def test_importfits_rejects_reference_errors(tmp_path):
    for name, case in CASES.items():
        if "error" not in case:
            continue
        error_type = {"TypeError": TypeError, "RuntimeError": RuntimeError}[case["error"]]
        product = tmp_path / f"{name}.im"
        with pytest.raises(error_type):
            run(name, product)
        assert not product.exists()


def assert_beam_metadata(actual, expected):
    if isinstance(expected, dict):
        assert actual.keys() == expected.keys()
        for name, item in actual.items():
            assert_beam_metadata(item, expected[name])
    elif isinstance(expected, float):
        assert actual == pytest.approx(expected, rel=1e-12)
    else:
        assert actual == expected


def test_casa_reopens_importfits_products(tmp_path):
    if not CASA_PYTHON.is_file():
        pytest.skip("CASA Python is unavailable")
    good_cases = {name: case for name, case in CASES.items() if "error" not in case}
    for name in good_cases:
        assert run(name, tmp_path / f"{name}.im") is None
    probe = """
import json
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from importfits_oracle import snapshot, version_string
assert version_string() == "6.7.6.14"
results = {path.stem: snapshot(path) for path in Path.cwd().glob("*.im")}
Path("readback.json").write_text(json.dumps(results, allow_nan=False))
"""
    proc = subprocess.run(
        [str(CASA_PYTHON), "-I", "-c", probe, str(ORACLE)],
        cwd=tmp_path,
        env={**os.environ, "MPLCONFIGDIR": str(tmp_path / "mpl")},
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    readback = json.loads((tmp_path / "readback.json").read_text())
    assert readback.keys() == good_cases.keys()
    for name, actual in readback.items():
        expected = good_cases[name]
        for key, value in actual.items():
            if key in ("reference", "increment", "last_world") or (
                key == "rest_frequency" and value is not None
            ):
                np.testing.assert_allclose(value, expected[key], atol=1e-10, rtol=1e-10, err_msg=name)
            elif key == "beam":
                assert_beam_metadata(value, expected[key])
            else:
                assert value == expected[key], (name, key, value, expected[key])
