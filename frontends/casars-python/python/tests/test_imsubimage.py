# SPDX-License-Identifier: LGPL-3.0-or-later
"""Python task coverage and CASA 6.7.6.14 readback for imsubimage."""

import json
import os
from pathlib import Path
import shutil
import subprocess

import numpy as np
import pytest

from casars import tasks, tools

ROOT = Path(__file__).resolve().parents[4]
DATA = ROOT / "crates/casa-images/tests/data/imsubimage"
ORACLE = ROOT / "tools/casa-compat/imsubimage_oracle.py"
CASA_PYTHON = Path(
    os.environ.get("CASA_RS_CASA_PYTHON", "/Applications/CASA.app/Contents/MacOS/python3")
)


@pytest.fixture
def workspace(tmp_path, monkeypatch):
    shutil.copytree(DATA, tmp_path, dirs_exist_ok=True, ignore=shutil.ignore_patterns("table.lock"))
    shutil.copytree(DATA.parent / "casa-6.7.6/axis_maps", tmp_path / "axis_maps")
    monkeypatch.chdir(tmp_path)
    return tmp_path


def cases():
    doc = json.loads((DATA / "expected.json").read_text())
    assert doc["casa_version"] == "6.7.6.14"
    return doc["cases"]


def assert_pixels(actual, good, expected, name):
    valid = np.array([v is not None for v in expected])
    np.testing.assert_array_equal(np.asarray(good).ravel(), valid, err_msg=name)
    expected_data = np.array(
        [complex(*value) if isinstance(value, list) else value for value in expected if value is not None]
    )
    np.testing.assert_array_equal(np.asarray(actual).ravel()[valid], expected_data, err_msg=name)


def test_list_scalar_and_multiline_through_python(workspace):
    matrix = cases()
    for name in ("keep_list", "keep_scalar", "union", "all_true", "named_mask", "bare_lel", "mask_image", "fits"):
        case = matrix[name]
        outfile = workspace / f"{name}.out"
        tasks.imsubimage(case["imagename"], outfile, **case["params"])
        image = tools.image()
        assert image.open(str(outfile))
        assert image.shape().tolist() == case["shape"]
        data = image.getchunk()
        mask = image.getchunk(getmask=True)
        assert_pixels(data, mask, case["pixels"], name)
        assert image.maskhandler("default") == (["mask0"] if "mask0" in case["masks"] else [""])
        assert image.done()


def test_imsubimage_errors_and_overwrite(workspace, capsys, monkeypatch):
    monkeypatch.setenv("CASA_RS_LOG_STDERR_PRIORITY", "INFO")
    with pytest.raises(ValueError, match="outfile"):
        tasks.imsubimage("cube.im")
    with pytest.raises(tasks.TaskExecutionError, match="drop all axes"):
        tasks.imsubimage("point.im", "point.out", dropdeg=True)
    tasks.imsubimage("plane.im", "existing.out", verbose=True)
    assert "Created subimage" in "".join(capsys.readouterr())
    image = tools.image()
    assert image.open("existing.out")
    original = image.getchunk()
    assert image.done()
    with pytest.raises(tasks.TaskExecutionError, match="already exists"):
        tasks.imsubimage("plane.im", "existing.out", box="1,1,3,4")
    with pytest.raises(tasks.TaskExecutionError, match="mask should not be extended"):
        tasks.imsubimage("cube.im", "existing.out", mask='"stretch.im" > 200', overwrite=True)
    assert image.open("existing.out")
    np.testing.assert_array_equal(image.getchunk(), original)
    assert image.done()
    tasks.imsubimage("plane.im", "existing.out", box="1,1,3,4", overwrite=True, verbose=False)
    assert "Created subimage" not in "".join(capsys.readouterr())
    assert image.open("existing.out")
    assert image.shape().tolist() == [3, 4]
    assert image.done()


def assert_metadata(actual, expected):
    if isinstance(expected, dict):
        assert actual.keys() == expected.keys()
        for key, value in expected.items():
            assert_metadata(actual[key], value)
    elif isinstance(expected, float):
        assert actual == pytest.approx(expected, rel=1e-10, abs=1e-10)
    else:
        assert actual == expected


def test_casatools_reads_imsubimage_products(workspace):
    if not CASA_PYTHON.is_file():
        pytest.skip("CASA Python is unavailable")
    successful = {name: case for name, case in cases().items() if "error" not in case}
    for name, case in successful.items():
        tasks.imsubimage(case["imagename"], f"{name}.out", **case["params"])
    probe = """
import json
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from imsubimage_oracle import snapshot, casatasks
assert casatasks.version_string() == "6.7.6.14"
products = {path.stem: snapshot(path) for path in Path.cwd().glob("*.out")}
Path("readback.json").write_text(json.dumps(products, allow_nan=False))
"""
    proc = subprocess.run(
        [str(CASA_PYTHON), "-I", "-c", probe, str(ORACLE.parent)],
        cwd=workspace,
        env={**os.environ, "MPLCONFIGDIR": str(workspace / "mpl")},
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    outputs = json.loads((workspace / "readback.json").read_text())
    assert outputs.keys() == successful.keys()
    for name, actual in outputs.items():
        expected = successful[name]
        try:
            for key in ("shape", "pixel_type", "unit", "names", "units", "maps", "masks",
                        "pixels", "copied_history", "task_history"):
                assert actual[key] == expected[key], key
            np.testing.assert_allclose(actual["world"], expected["world"], rtol=1e-10, atol=1e-10)
            assert_metadata(actual["beams"], expected["beams"])
        except AssertionError as error:
            raise AssertionError(f"{name}: {error}") from error
