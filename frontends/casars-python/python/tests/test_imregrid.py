# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA template reuse, masks and readback of native regrids."""

import json
import os
from pathlib import Path
import shutil
import subprocess

import numpy as np
import pytest

from casars import tasks


ROOT = Path(__file__).resolve().parents[4]
DATA = ROOT / "crates/casa-images/tests/data/imregrid"
ORACLE = ROOT / "tools/casa-compat"
CASA_PYTHON = Path(os.environ.get("CASA_RS_CASA_PYTHON", "/Applications/CASA.app/Contents/MacOS/python3"))
EVIDENCE = json.loads((DATA / "expected.json").read_text())


@pytest.fixture
def workspace(tmp_path, monkeypatch):
    shutil.copytree(DATA, tmp_path, dirs_exist_ok=True, ignore=shutil.ignore_patterns("table.lock"))
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_get_returns_casa_coordinate_dictionary_and_reuses_it(workspace):
    template = tasks.imregrid("template.im", template="get", output="unused.im")
    assert set(template) == {"csys", "shap"}
    assert template["shap"].dtype == np.int64
    np.testing.assert_array_equal(template["shap"], EVIDENCE["template"]["shap"])
    np.testing.assert_allclose(template["csys"]["direction0"]["pc"],
                               EVIDENCE["template"]["csys"]["direction0"]["pc"])
    assert template["csys"]["spectral2"]["wcs"]["ctype"] == "FREQ"
    assert not (workspace / "unused.im").exists()
    assert tasks.imregrid("cube.im", template, "reused.im", asvelocity=False, decimate=0) is None
    assert (workspace / "reused.im").is_dir()
    assert tasks.imregrid("cube.im", "template.im", decimate=0) is None
    assert (workspace / "cube.im.regridded").is_dir()


def test_invalid_overwrite_preserves_existing_product(workspace):
    assert tasks.imregrid("cube.im", "template.im", "out.im", decimate=0) is None
    before = (workspace / "out.im" / "table.dat").read_bytes()
    with pytest.raises(tasks.TaskExecutionError, match="input length"):
        tasks.imregrid("cube.im", "template.im", "out.im", axes=[0, 1],
                       shape=[10, 9, 1, 4], decimate=0, overwrite=True)
    assert (workspace / "out.im" / "table.dat").read_bytes() == before


def test_casa_reopens_regrid_products(workspace):
    if not CASA_PYTHON.is_file():
        pytest.skip("CASA Python is unavailable")
    cases = {name: case for name, case in EVIDENCE["cases"].items() if "product" in case}
    for name, case in cases.items():
        assert tasks.imregrid(**case["params"], output=f"{name}.out") is None
    probe = """
import json
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from imregrid_oracle import snapshot, version_string
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
        case = cases[name]
        # Use the analytic result for the documented same-shape linear shift.
        reference = case.get("corrected_product", case["product"])
        metadata = case.get("preserved_input_metadata", reference)
        for field in ("shape", "names", "units", "direction_frame", "mask"):
            assert product[field] == reference[field], (name, field)
        assert product["brightness"] == metadata["brightness"], name
        assert product["beam"].keys() == metadata["beam"].keys(), name
        for key, (value, unit) in metadata["beam"].items():
            assert product["beam"][key][1] == unit, (name, key)
            assert product["beam"][key][0] == pytest.approx(value, rel=1e-8), (name, key)
        for field in ("reference_pixel", "reference_value", "increment", "pixel0_world"):
            np.testing.assert_allclose(product[field], reference[field], rtol=1e-8,
                                       atol=1e-8, err_msg=f"{name} {field}")
        mask = np.asarray(reference["mask"])
        np.testing.assert_allclose(np.asarray(product["pixels"])[mask],
                                   np.asarray(reference["pixels"])[mask],
                                   rtol=3e-6, atol=1e-6, err_msg=name)
