# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA's primary-beam arrays and selected image products through Python."""

import json
import os
from pathlib import Path
import shutil
import subprocess

import numpy as np
import pytest

from casars import tasks, tools
from casars.tasks import _runner


ROOT = Path(__file__).resolve().parents[4]
DATA = ROOT / "crates/casa-images/tests/data/impbcor"
ORACLE = ROOT / "tools/casa-compat"
CASA_PYTHON = Path(os.environ.get("CASA_RS_CASA_PYTHON", "/Applications/CASA.app/Contents/MacOS/python3"))
CASES = json.loads((DATA / "expected.json").read_text())["cases"]


@pytest.fixture
def workspace(tmp_path, monkeypatch):
    try:
        _runner._task_executable("impbcor")
    except tasks.TaskBinaryNotFoundError:
        if os.environ.get("CASARS_REQUIRE_TASK_BINARIES"):
            raise
        pytest.skip("impbcor executable is unavailable")
    shutil.copytree(DATA, tmp_path, dirs_exist_ok=True, ignore=shutil.ignore_patterns("table.lock"))
    monkeypatch.chdir(tmp_path)
    return tmp_path


def invoke(name):
    params = dict(CASES[name]["params"])
    if name in {"full_array", "plane_array", "selected_plane_array"}:
        params["pbimage"] = np.array(params["pbimage"], dtype=np.float32, order="F")
    if name == "nan_cutoff":
        params["cutoff"] = float(params["cutoff"])
    assert tasks.impbcor(**params, outfile=f"{name}.out") is None


def test_numpy_beams_and_region_records(workspace):
    for name in ("full_array", "plane_array", "region_record", "circle_record", "selection", "nan_cutoff"):
        invoke(name)
        tool = tools.image()
        assert tool.open(f"{name}.out")
        try:
            expected = CASES[name]["product"]
            assert tool.shape().tolist() == expected["shape"]
            assert tool.getchunk(getmask=True).ravel(order="F").tolist() == expected["mask"]
            pixels = tool.getchunk().ravel(order="F")
            np.testing.assert_array_equal(np.isfinite(pixels), [p is not None for p in expected["pixels"]])
            finite = np.isfinite(pixels)
            np.testing.assert_allclose(pixels[finite], np.asarray(expected["pixels"], dtype=float)[finite], rtol=2e-6)
        finally:
            tool.done()


def test_casa_reopens_all_primary_beam_products(workspace):
    if not CASA_PYTHON.is_file():
        pytest.skip("CASA Python is unavailable")
    expected = {}
    for name, case in CASES.items():
        if "product" in case:
            invoke(name)
            expected[name] = case["product"]
    probe = """
import json
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from impbcor_oracle import snapshot, version_string
assert version_string() == "6.7.6.14"
out = {name: snapshot(f"{name}.out") for name in json.loads(sys.argv[2])}
Path("readback.json").write_text(json.dumps(out, allow_nan=False))
"""
    out = subprocess.run(
        [str(CASA_PYTHON), "-I", "-c", probe, str(ORACLE), json.dumps(list(expected))],
        cwd=workspace,
        env={**os.environ, "MPLCONFIGDIR": str(workspace / "mpl")},
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert out.returncode == 0, out.stdout + out.stderr
    actual = json.loads((workspace / "readback.json").read_text())
    assert actual.keys() == expected.keys()
    for name, product in actual.items():
        ref = expected[name]
        for key in ("shape", "names", "stokes", "units", "mask", "default_mask", "history", "task_history"):
            assert product[key] == ref[key], (name, key)
        for key in ("reference_pixel", "reference_value", "pixels"):
            np.testing.assert_allclose(np.asarray(product[key], dtype=float), np.asarray(ref[key], dtype=float),
                                       rtol=2e-6, atol=1e-8, err_msg=f"{name} {key}")
        assert product["beam"].keys() == ref["beam"].keys()
        angular_units = {"rad": 1, "deg": np.pi / 180, "arcmin": np.pi / 10800, "arcsec": np.pi / 648000}
        for key, quantity in product["beam"].items():
            a = quantity["value"] * angular_units[quantity["unit"]]
            b = ref["beam"][key]["value"] * angular_units[ref["beam"][key]["unit"]]
            assert a == pytest.approx(b, abs=1e-10, rel=2e-6), (name, key)
