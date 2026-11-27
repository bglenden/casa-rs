# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA task parameters and live readback for immoments products."""

import json
import os
from pathlib import Path
import shutil
import subprocess

import numpy as np
import pytest

from casars import tasks, tools


ROOT = Path(__file__).resolve().parents[4]
DATA = ROOT / "libs/casacore/casa-images/tests/data/immoments"
CASA_PYTHON = Path(os.environ.get("CASA_RS_CASA_PYTHON", "/Applications/CASA.app/Contents/MacOS/python3"))
ORACLE = ROOT / "tools/casa-compat"
REFERENCE = json.loads((DATA / "expected.json").read_text())
assert REFERENCE["casa_version"] == "6.7.6.14"
CASES = REFERENCE["cases"]


@pytest.fixture
def workspace(tmp_path, monkeypatch):
    shutil.copytree(DATA, tmp_path, dirs_exist_ok=True, ignore=shutil.ignore_patterns("table.lock"))
    monkeypatch.chdir(tmp_path)
    return tmp_path


def run(name, product):
    case = CASES[name]
    return tasks.immoments(case["imagename"], outfile=product, **case["params"])


def test_immoments_products_and_defaults(workspace):
    assert run("subset", "selected.out") is None
    selected = tools.image()
    assert selected.open("selected.out")
    assert selected.shape().tolist() == [2, 2, 1, 1]
    assert selected.coordsys().stokes() == ["Q"]
    selected.done()

    assert run("all_positive", "all.out") is None
    products = CASES["all_positive"]["products"]
    assert {path.name for path in workspace.glob("all.out*")} == {
        "all.out" + suffix for suffix in products
    }

    with pytest.raises(RuntimeError, match="mask should not be extended"):
        run("stretch_off", "refused.out")
    with pytest.raises(RuntimeError, match="median coordinate"):
        run("median_needs_range", "refused.out")
    assert not (workspace / "refused.out").exists()


def assert_beams(actual, reference):
    if isinstance(reference, dict) and "unit" in reference:
        radians = {"rad": 1.0, "deg": np.pi / 180, "arcmin": np.pi / 10800, "arcsec": np.pi / 648000}
        assert actual["value"] * radians[actual["unit"]] == pytest.approx(
            reference["value"] * radians[reference["unit"]], rel=1e-6, abs=1e-12)
    elif isinstance(reference, dict):
        assert actual.keys() == reference.keys()
        for key, value in reference.items():
            assert_beams(actual[key], value)
    else:
        assert actual == reference


def test_casa_reopens_immoments_products(workspace):
    if not CASA_PYTHON.is_file():
        pytest.skip("CASA Python is unavailable")
    expected = {}
    for name, case in CASES.items():
        if "error" in case:
            continue
        stem = name + ".out"
        assert run(name, stem) is None
        expected.update({stem + suffix: product for suffix, product in case["products"].items()})
    probe = """
import json
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from immoments_oracle import snapshot, version_string
assert version_string() == "6.7.6.14"
output = {name: snapshot(Path(name)) for name in json.loads(sys.argv[2])}
Path("readback.json").write_text(json.dumps(output, allow_nan=False))
"""
    proc = subprocess.run(
        [str(CASA_PYTHON), "-I", "-c", probe, str(ORACLE), json.dumps(list(expected))],
        cwd=workspace,
        env={**os.environ, "MPLCONFIGDIR": str(workspace / "mpl")},
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    output = json.loads((workspace / "readback.json").read_text())
    assert output.keys() == expected.keys()
    for name, actual in output.items():
        reference = expected[name]
        for key in ("shape", "names", "units", "mask", "default_mask", "input_history", "task_history"):
            assert actual[key] == reference[key], (name, key)
        assert_beams(actual["beam"], reference["beam"])
        for key in ("reference_pixel", "reference_value", "first_world"):
            np.testing.assert_allclose(actual[key], reference[key], atol=1e-8, rtol=1e-10, err_msg=name)
        mask = np.asarray(reference["mask"])
        np.testing.assert_allclose(
            np.asarray(actual["pixels"])[mask].astype(float),
            np.asarray(reference["pixels"])[mask].astype(float),
            atol=1e-6, rtol=1e-5, err_msg=name,
        )
