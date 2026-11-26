# SPDX-License-Identifier: LGPL-3.0-or-later
"""End-to-end Python tests and CASA 6.7.6.14 readback interop for `casars.tasks.exportfits`."""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess

import numpy as np
import pytest

from casars import tasks
from casars.tasks import _runner

REPO_ROOT = Path(__file__).resolve().parents[4]
EXPORTFITS_DATA_DIR = REPO_ROOT / "crates" / "casa-images" / "tests" / "data" / "exportfits"
CASA_PYTHON = Path("/Applications/CASA.app/Contents/MacOS/python3")


@pytest.fixture
def exportfits_bin() -> str:
    try:
        return _runner._task_executable("exportfits")
    except tasks.TaskBinaryNotFoundError as missing:
        if os.environ.get("CASARS_REQUIRE_TASK_BINARIES"):
            raise
        pytest.skip(f"no exportfits executable: {missing}")


def _stage_fixtures(dst: Path) -> Path:
    if not EXPORTFITS_DATA_DIR.exists():
        pytest.skip(f"exportfits fixture directory missing: {EXPORTFITS_DATA_DIR}")
    for entry in EXPORTFITS_DATA_DIR.iterdir():
        if entry.name == "expected.json":
            continue
        target = dst / entry.name
        if entry.is_dir():
            shutil.copytree(entry, target, ignore=shutil.ignore_patterns("table.lock"))
        else:
            shutil.copy2(entry, target)
    return dst


def _parse_fits_primary(path: Path) -> tuple[dict[str, object], list[str], np.ndarray | None]:
    raw = path.read_bytes()
    cards = []
    pos = 0
    end_idx = 0
    while pos + 80 <= len(raw):
        card = raw[pos : pos + 80].decode("ascii", errors="replace")
        pos += 80
        cards.append(card.rstrip())
        if card.startswith("END     "):
            end_idx = pos
            break
    kv: dict[str, object] = {}
    hist: list[str] = []
    for card in cards:
        if card.startswith("HISTORY "):
            hist.append(card)
            continue
        if "=" not in card[:10]:
            continue
        key = card[:8].strip()
        rest = card[10:].strip()
        if rest.startswith("'"):
            end_q = 1
            while end_q < len(rest):
                if rest[end_q] == "'":
                    if end_q + 1 < len(rest) and rest[end_q + 1] == "'":
                        end_q += 2
                    else:
                        break
                else:
                    end_q += 1
            kv[key] = rest[1:end_q].replace("''", "'").rstrip()
        else:
            val_part = rest.split("/", 1)[0].strip()
            if val_part == "T":
                kv[key] = True
            elif val_part == "F":
                kv[key] = False
            else:
                try:
                    kv[key] = (
                        float(val_part.replace("D", "E"))
                        if ("." in val_part or "E" in val_part or "D" in val_part)
                        else int(val_part)
                    )
                except ValueError:
                    kv[key] = val_part

    raw_i16 = None
    if kv.get("BITPIX") == 16:
        data_off = ((end_idx + 2879) // 2880) * 2880
        naxis = int(kv["NAXIS"])
        count = 1
        for ax in range(1, naxis + 1):
            count *= int(kv[f"NAXIS{ax}"])
        raw_i16 = np.frombuffer(raw[data_off : data_off + count * 2], dtype=">i2")
    return kv, hist, raw_i16


def test_exportfits_python_all_parameters_and_task_surfaces(
    tmp_path: Path, exportfits_bin: str
) -> None:
    _ = exportfits_bin
    ws = _stage_fixtures(tmp_path)

    # 1. Velocity + optical + history + run("exportfits", ...)
    out_vopt = ws / "vopt.fits"
    res1 = tasks.run(
        "exportfits",
        imagename=str(ws / "cube_i.im"),
        fitsimage=str(out_vopt),
        velocity=True,
        optical=True,
        history=True,
        overwrite=True,
    )
    assert out_vopt.exists()
    assert res1.products[0].path == str(out_vopt)
    kv, hist, _ = _parse_fits_primary(out_vopt)
    assert kv["CTYPE3"] == "VOPT"
    assert kv["CUNIT3"] == "m/s"
    assert kv["VELREF"] == 1
    assert "HISTORY CASA START LOGTABLE" in hist
    assert "HISTORY CASA END LOGTABLE" in hist

    # 2. bitpix=16 with explicit minpix/maxpix + no-history
    out_16 = ws / "bp16.fits"
    tasks.exportfits(
        imagename=str(ws / "cube_i.im"),
        fitsimage=str(out_16),
        bitpix=16,
        minpix=-1.5,
        maxpix=4.0,
        history=False,
    )
    kv16, hist16, raw_i16 = _parse_fits_primary(out_16)
    assert kv16["BITPIX"] == 16
    assert kv16["BLANK"] == -32768
    assert kv16["DATAMIN"] == pytest.approx(-1.5)
    assert kv16["DATAMAX"] == pytest.approx(4.0)
    assert hist16 == []
    assert raw_i16 is not None and raw_i16[0] == -32768

    # 3. dropstokes=True drops len-1 Stokes on cube_i but preserves IQUV on iquv.im
    out_ds = ws / "ds1.fits"
    tasks.exportfits(
        imagename=str(ws / "cube_i.im"),
        fitsimage=str(out_ds),
        dropstokes=True,
        history=False,
    )
    kv_ds, _, _ = _parse_fits_primary(out_ds)
    assert kv_ds["NAXIS"] == 3
    assert kv_ds["CTYPE3"] == "FREQ"

    out_iquv_nofirst = ws / "iquv_nostokeslast.fits"
    tasks.exportfits(
        imagename=str(ws / "iquv.im"),
        fitsimage=str(out_iquv_nofirst),
        dropstokes=True,
        stokeslast=False,
        history=False,
    )
    kv_iq, _, _ = _parse_fits_primary(out_iquv_nofirst)
    assert kv_iq["NAXIS"] == 4
    assert kv_iq["CTYPE3"] == "STOKES"
    assert kv_iq["CTYPE4"] == "FREQ"
    assert kv_iq["CASAMBM"] is True

    # 4. dropdeg=True collapses degenerate RA+Stokes in dir1_deg.im
    out_dd = ws / "dir1_dd.fits"
    tasks.exportfits(
        imagename=str(ws / "dir1_deg.im"),
        fitsimage=str(out_dd),
        dropdeg=True,
        history=False,
    )
    kv_dd, _, _ = _parse_fits_primary(out_dd)
    assert kv_dd["NAXIS"] == 2
    assert kv_dd["CTYPE1"] == "DEC--SIN"
    assert kv_dd["CTYPE2"] == "FREQ"

    # 5. Parameter refusal attribution on invalid bitpix and all-degenerate dropdeg
    with pytest.raises(tasks.TaskParameterError):
        tasks.exportfits(
            imagename=str(ws / "no_mask.im"),
            fitsimage=str(ws / "bad.fits"),
            bitpix=8,
        )
    with pytest.raises(tasks.TaskExecutionError):
        tasks.exportfits(
            imagename=str(ws / "all_deg.im"),
            fitsimage=str(ws / "empty.fits"),
            dropdeg=True,
        )


def test_exportfits_casa_readback_interoperability(
    tmp_path: Path, exportfits_bin: str
) -> None:
    _ = exportfits_bin
    if not CASA_PYTHON.exists():
        pytest.skip(f"CASA Python binary not available at {CASA_PYTHON}")

    ws = _stage_fixtures(tmp_path)
    oracle = json.loads((EXPORTFITS_DATA_DIR / "expected.json").read_text())
    expected_by_name = {c["name"]: c for c in oracle["cases"]}

    check_cases = [
        "vel0_opt1",
        "vel1_opt0",
        "vel1_opt1",
        "bitpix16_auto_masked",
        "bitpix16_clipped",
        "bitpix16_nomask_auto",
        "bitpix16_const",
        "stokeslast_false_iquv",
        "stokeslast_true_iquv",
        "mb_1stokes_dropstokes",
        "dropdeg_deg2d",
        "history_true",
        "fits_input",
    ]
    outputs: dict[str, str] = {}
    for name in check_cases:
        spec = expected_by_name[name]
        out_fits = ws / f"{name}.fits"
        params = dict(spec["params"])
        tasks.exportfits(
            imagename=str(ws / spec["imagename"]),
            fitsimage=str(out_fits),
            **params,
        )
        outputs[name] = str(out_fits)

    outputs_json = json.dumps(outputs)
    expected_json = json.dumps({k: expected_by_name[k] for k in check_cases})
    probe = f"""
import os, json, numpy as np
from casatools import image

ws_dir = {str(ws)!r}
outputs = json.loads({outputs_json!r})
expected = json.loads({expected_json!r})
ia = image()
for name, fits_path in outputs.items():
    exp = expected[name]
    ia.open(fits_path)
    try:
        shape = [int(x) for x in ia.shape()]
        assert shape == exp["shape"], f"{{name}} shape {{shape}} != {{exp['shape']}}"
        pix = ia.getchunk()
        mask = ia.getchunk(getmask=True)
        assert mask.ravel(order="F").tolist() == exp["mask_f_order"], f"{{name}} mask mismatch"
        got_flat = pix.ravel(order="F")
        exp_pix = np.array([float("nan") if v == "NaN" else float(v) for v in exp["pixels_f_order"]])
        valid = np.array(exp["mask_f_order"], dtype=bool)
        if valid.any():
            tol = 5e-4 if exp["params"].get("bitpix") == 16 else 1e-5
            assert np.allclose(got_flat[valid], exp_pix[valid], rtol=1e-4, atol=tol), f"{{name}} pixel diff"
        if exp["has_perplanebeams"]:
            src_ia = image()
            src_ia.open(os.path.join(ws_dir, exp["imagename"]))
            try:
                src_rb = src_ia.restoringbeam()
            finally:
                src_ia.close()
            rb = ia.restoringbeam()
            assert "beams" in rb, f"{{name}} missing CASA beams on readback"
            assert int(rb["nChannels"]) == int(src_rb["nChannels"])
            assert int(rb["nStokes"]) == int(src_rb["nStokes"])
            for p in range(int(rb["nStokes"])):
                for c in range(int(rb["nChannels"])):
                    got_b = rb["beams"][f"*{{c}}"][f"*{{p}}"]
                    exp_b = src_rb["beams"][f"*{{c}}"][f"*{{p}}"]
                    assert np.isclose(got_b["major"]["value"], exp_b["major"]["value"], rtol=1e-4)
                    assert np.isclose(got_b["minor"]["value"], exp_b["minor"]["value"], rtol=1e-4)
                    assert np.isclose(got_b["positionangle"]["value"], exp_b["positionangle"]["value"], atol=1e-3)
        if exp["has_casa_logtable_markers"]:
            hist = ia.history(list=False)
            assert len(hist) >= 2, f"{{name}} missing history on CASA readback: {{hist}}"
    finally:
        ia.close()
print("ALL_CASA_READBACK_OK")
"""
    env = dict(os.environ, MPLCONFIGDIR="/tmp/mpl")
    proc = subprocess.run(
        [str(CASA_PYTHON), "-c", probe],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, f"CASA readback failed:\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
    assert "ALL_CASA_READBACK_OK" in proc.stdout
