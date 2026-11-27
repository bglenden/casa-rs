# SPDX-License-Identifier: LGPL-3.0-or-later
"""Position-velocity task inputs and live CASA interchange."""

import json
import os
from pathlib import Path
import shutil
import subprocess

import numpy as np
import pytest

from casars import tasks, tools


ROOT = Path(__file__).resolve().parents[4]
DATA = ROOT / "libs/casacore/casa-images/tests/data/impv"
ORACLE = ROOT / "tools/casa-compat"
CASA_PYTHON = Path(os.environ.get("CASA_RS_CASA_PYTHON", "/Applications/CASA.app/Contents/MacOS/python3"))
CASES = json.loads((DATA / "expected.json").read_text())["cases"]


@pytest.fixture
def workspace(tmp_path, monkeypatch):
    shutil.copytree(DATA, tmp_path, dirs_exist_ok=True, ignore=shutil.ignore_patterns("table.lock"))
    monkeypatch.chdir(tmp_path)
    return tmp_path


def invoke(name):
    values = dict(CASES[name]["params"])
    if name == "transposed":
        values.update(start=np.asarray(values["start"], dtype=np.int32), end=np.asarray(values["end"], dtype=np.int32))
    assert tasks.impv(**values, outfile=f"{name}.out") is None


def test_impv_python_inputs_and_output_axis(workspace):
    for name in ("transposed", "length_pixels", "width_angle", "region_record_channels", "galactic_length"):
        invoke(name)
        tool = tools.image()
        assert tool.open(f"{name}.out")
        try:
            expected = CASES[name]["product"]
            assert tool.shape().tolist() == expected["shape"]
            assert tool.coordsys().names() == expected["names"]
            np.testing.assert_allclose(tool.getchunk().ravel(order="F"), expected["pixels"], rtol=2e-5, atol=1e-6)
        finally:
            tool.done()


def test_all_masked_overwrite_keeps_image(workspace):
    invoke("horizontal")
    before = (workspace / "horizontal.out" / "table.dat").read_bytes()
    with pytest.raises(tasks.TaskExecutionError, match="All selected pixels are masked"):
        tasks.impv(**CASES["all_masked"]["params"], outfile="horizontal.out", overwrite=True)
    assert (workspace / "horizontal.out" / "table.dat").read_bytes() == before


def compare_beams(actual, expected):
    assert actual.keys() == expected.keys()
    for key, reference in expected.items():
        value = actual[key]
        if isinstance(reference, dict):
            compare_beams(value, reference)
        elif isinstance(reference, list):
            assert value[1] == reference[1]
            assert value[0] == pytest.approx(reference[0], rel=1e-10)
        else:
            assert value == reference


def test_casa_reopens_pv_products(workspace):
    if not CASA_PYTHON.is_file():
        pytest.skip("CASA Python is unavailable")
    accepted = {name: case.get("product", case.get("padded_reference")) for name, case in CASES.items()}
    accepted = {name: case for name, case in accepted.items() if case is not None}
    for name in accepted:
        invoke(name)
    probe = """
import json
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from impv_oracle import snapshot, version_string
assert version_string() == "6.7.6.14"
p = {name: snapshot(f"{name}.out") for name in json.loads(sys.argv[2])}
Path("readback.json").write_text(json.dumps(p, allow_nan=False))
"""
    result = subprocess.run(
        [str(CASA_PYTHON), "-I", "-c", probe, str(ORACLE), json.dumps(list(accepted))],
        cwd=workspace, env={**os.environ, "MPLCONFIGDIR": str(workspace / "mpl")},
        capture_output=True, text=True, timeout=120,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    actual = json.loads((workspace / "readback.json").read_text())
    assert actual.keys() == accepted.keys()
    for name, result in actual.items():
        reference = accepted[name]
        for field in ("shape", "names", "units", "brightness", "coordinate_record_maps", "mask", "history"):
            assert result[field] == reference[field], (name, field)
        compare_beams(result["beams"], reference["beams"])
        for field, values in reference["secondary_direction"].items():
            np.testing.assert_allclose(result["secondary_direction"][field], values,
                                       rtol=1e-7, atol=1e-6, err_msg=f"{name} secondary {field}")
        for field in ("reference_pixel", "reference_value", "increment", "pixel0_world"):
            np.testing.assert_allclose(result[field], reference[field], rtol=1e-8, atol=1e-8, err_msg=f"{name} {field}")
        valid = np.asarray(reference["mask"])
        np.testing.assert_allclose(np.asarray(result["pixels"], dtype=float)[valid],
                                   np.asarray(reference["pixels"], dtype=float)[valid],
                                   rtol=2e-5, atol=1e-6, err_msg=name)
