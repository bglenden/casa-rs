# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA effective-dish settings through Python and native image interchange."""

import json
import os
from pathlib import Path
import shutil
import subprocess

import numpy as np
import pytest

from casars import tasks

ROOT = Path(__file__).resolve().parents[4]
DATA = ROOT / "libs/casacore/casa-images/tests/data/feather"
ORACLE = ROOT / "tools/casa-compat"
CASA_PYTHON = Path(os.environ.get("CASA_RS_CASA_PYTHON", "/Applications/CASA.app/Contents/MacOS/python3"))
CASES = json.loads((DATA / "expected.json").read_text())["cases"]


@pytest.fixture
def workspace(tmp_path, monkeypatch):
    shutil.copytree(DATA, tmp_path, dirs_exist_ok=True, ignore=shutil.ignore_patterns("table.lock"))
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_positive_diameter_and_switch_are_accepted_and_failed_overwrite_is_safe(workspace):
    assert tasks.feather("feathered.im", "high.im", "low.im", effdishdiam=1600.0,
                         lowpassfiltersd=True) is None
    table = workspace / "feathered.im" / "table.dat"
    before = table.read_bytes()
    with pytest.raises(tasks.TaskExecutionError, match="smaller than the original single-dish beam"):
        tasks.feather("feathered.im", "high.im", "low.im", effdishdiam=4200.0, overwrite=True)
    assert table.read_bytes() == before


def test_casa_reopens_filtered_feather_products(workspace):
    if not CASA_PYTHON.is_file():
        pytest.skip("CASA Python is unavailable")
    cases = {name: case for name, case in CASES.items() if "product" in case}
    for name, case in cases.items():
        assert tasks.feather(**case["params"], imagename=f"{name}.out") is None
    probe = """
import json
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from feather_oracle import snapshot, version_string
assert version_string() == '6.7.6.14'
outputs = {name: snapshot(f'{name}.out') for name in json.loads(sys.argv[2])}
Path('readback.json').write_text(json.dumps(outputs, allow_nan=False))
"""
    result = subprocess.run(
        [str(CASA_PYTHON), "-I", "-c", probe, str(ORACLE), json.dumps(list(cases))],
        cwd=workspace, env={**os.environ, "MPLCONFIGDIR": str(workspace / "mpl")},
        capture_output=True, text=True, timeout=120,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    actual = json.loads((workspace / "readback.json").read_text())
    assert actual.keys() == cases.keys()
    for name, product in actual.items():
        reference = cases[name]["product"]
        for field in ("shape", "units", "mask", "default_mask"):
            assert product[field] == reference[field], (name, field)
        for field, (value, unit) in reference["beam"].items():
            assert product["beam"][field][1] == unit
            assert product["beam"][field][0] == pytest.approx(value, rel=1e-10)
        pixels = np.asarray(reference["pixels"], dtype=float)
        residual = np.asarray(product["pixels"], dtype=float) - pixels
        assert np.linalg.norm(residual)/np.linalg.norm(pixels) < 2e-6, name
