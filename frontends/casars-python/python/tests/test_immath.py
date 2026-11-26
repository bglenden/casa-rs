# SPDX-License-Identifier: LGPL-3.0-or-later
"""End-to-end Python tests and CASA 6.7.6.14 readback interop for `casars.tasks.immath`."""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import numpy as np
import pytest

from casars import tasks
from casars._native_image import NativeImageView
from casars.tasks import _runner

REPO_ROOT = Path(__file__).resolve().parents[4]
IMMATH_DATA_DIR = REPO_ROOT / "crates" / "casa-images" / "tests" / "data" / "immath"
CASA_PYTHON = Path("/Applications/CASA.app/Contents/MacOS/python3")


@pytest.fixture
def immath_bin() -> str:
    try:
        return _runner._task_executable("immath")
    except tasks.TaskBinaryNotFoundError as missing:
        if os.environ.get("CASARS_REQUIRE_TASK_BINARIES"):
            raise
        pytest.skip(f"no immath executable: {missing}")


def _stage_immath_fixtures(dst: Path) -> Path:
    if not IMMATH_DATA_DIR.exists():
        pytest.skip(f"immath fixture directory missing: {IMMATH_DATA_DIR}")
    for entry in IMMATH_DATA_DIR.iterdir():
        if entry.name == "expected.json":
            continue
        target = dst / entry.name
        if entry.is_dir():
            shutil.copytree(
                entry,
                target,
                ignore=shutil.ignore_patterns("table.lock"),
            )
        else:
            shutil.copy2(entry, target)
    return dst


def _write_minimal_fits(path: Path, data_zyx: np.ndarray) -> None:
    """Write a self-contained primary-HDU float32 FITS image for wheel/offline tests."""
    # data_zyx is already in FITS axis order (NAXISn ... NAXIS1), big-endian float32.
    shape = list(reversed(data_zyx.shape))
    cards = [
        "SIMPLE  =                    T",
        "BITPIX  =                  -32",
        f"NAXIS   = {len(shape):20d}",
    ]
    for idx, dim in enumerate(shape, start=1):
        cards.append(f"NAXIS{idx:<3d}= {dim:20d}")
    cards.extend(
        [
            "BUNIT   = 'Jy/beam '",
            "CTYPE1  = 'RA---SIN'",
            "CRVAL1  =                180.0",
            "CRPIX1  =                  1.0",
            "CDELT1  =       -0.00027777778",
            "CTYPE2  = 'DEC--SIN'",
            "CRVAL2  =                  0.0",
            "CRPIX2  =                  1.0",
            "CDELT2  =        0.00027777778",
            "CTYPE3  = 'FREQ    '",
            "CRVAL3  =          1000000000.",
            "CRPIX3  =                  1.0",
            "CDELT3  =           500000000.",
            "END",
        ]
    )
    header = "".join(card.ljust(80) for card in cards).encode("ascii")
    rem = len(header) % 2880
    if rem:
        header += b" " * (2880 - rem)
    payload = np.asarray(data_zyx, dtype=">f4").tobytes(order="C")
    prem = len(payload) % 2880
    if prem:
        payload += b"\x00" * (2880 - prem)
    path.write_bytes(header + payload)


def test_immath_self_contained_fits_and_default_outfile(
    immath_bin: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _ = immath_bin
    monkeypatch.chdir(tmp_path)
    arr = np.arange(1.0, 19.0, dtype=np.float32).reshape((2, 3, 3))
    arr[1, 0, 2] = np.nan
    fits_path = tmp_path / "tiny.fits"
    _write_minimal_fits(fits_path, arr)

    assert (
        tasks.immath(
            imagename="",
            expr="'tiny.fits' * 2.0 + 1.0",
            outfile="",
        )
        is True
    )
    default_out = tmp_path / "immath_results.im"
    assert default_out.is_dir()

    out = NativeImageView.open(default_out)
    assert out.shape == (3, 3, 2)
    assert out.pixel_type == "float32"
    assert out.units == "Jy/beam"
    assert out.default_mask_name == "mask0"
    pix = np.asarray(out.get_slice((0, 0, 0), out.shape))
    mask = np.asarray(out.get_mask_slice((0, 0, 0), out.shape))
    assert bool(mask[2, 0, 1]) is False
    assert bool(mask[0, 0, 0]) is True
    assert float(pix[0, 0, 0]) == pytest.approx(3.0, rel=1e-5)

    # Generic tasks.run('immath', outfile='') also resolves default product path via Rust completion.
    completion = tasks.run(
        "immath",
        imagename="",
        expr="'tiny.fits' * 2.0 + 1.0",
        outfile="",
        overwrite=True,
    )
    assert Path(completion.products[0].path) == default_out
    assert completion.products[0].exists is True
    out2 = NativeImageView.open(default_out)
    pix2 = np.asarray(out2.get_slice((0, 0, 0), out2.shape))
    assert np.allclose(pix2[mask], pix[mask], rtol=1e-6, atol=1e-6)


def _assert_matches_oracle(image_path: Path, oracle_case: dict[str, object], *, rtol: float = 1e-4, atol: float = 1e-5) -> None:
    im = NativeImageView.open(image_path)
    shape = im.shape
    pix = np.asarray(im.get_slice((0,) * len(shape), shape))
    m_raw = im.get_mask_slice((0,) * len(shape), shape)
    got_mask = np.ones(shape, dtype=bool) if m_raw is None else np.asarray(m_raw, dtype=bool)
    # expected.json stores C-order flattened arrays of im.get_slice() / ia.getchunk().
    exp_mask = np.asarray(oracle_case["mask"], dtype=bool).reshape(shape)
    assert np.array_equal(got_mask, exp_mask), f"{image_path.name}: mask mismatch"
    exp_pix = np.array(
        [np.nan if v is None else float(v) for v in oracle_case["pixels"]],
        dtype=np.float64,
    ).reshape(shape)
    valid = got_mask & ~np.isnan(exp_pix)
    assert np.allclose(pix[valid], exp_pix[valid], rtol=rtol, atol=atol), f"{image_path.name}: pixel mismatch"


def test_immath_python_all_modes_and_parameters(
    immath_bin: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _ = immath_bin
    ws = _stage_immath_fixtures(tmp_path)
    monkeypatch.chdir(ws)
    oracle_cases = json.loads((IMMATH_DATA_DIR / "expected.json").read_text())["cases"]

    def _read_pix_and_mask(p: Path) -> tuple[np.ndarray, np.ndarray]:
        im = NativeImageView.open(p)
        shp = im.shape
        px = np.asarray(im.get_slice((0,) * len(shp), shp), dtype=np.float64)
        mr = im.get_mask_slice((0,) * len(shp), shp)
        mk = np.ones(shp, dtype=bool) if mr is None else np.asarray(mr, dtype=bool)
        return px, mk

    cube_p, cube_m = _read_pix_and_mask(ws / "cube_iquv.im")
    spix_lo_p, _ = _read_pix_and_mask(ws / "spix_lo.im")
    deg_p, _ = _read_pix_and_mask(ws / "deg4d.im")
    map_p, map_m = _read_pix_and_mask(ws / "map2d.im")

    # 1. evalexpr with varnames, imagemd, double precision, mask+stretch, chans, stokes
    assert (
        tasks.immath(
            imagename=["cube_iquv.im", "spix_lo.im"],
            mode="evalexpr",
            outfile="eval_custom.im",
            expr="A * 2.0 + value(B)",
            varnames=["A", "B"],
            chans="0~2",
            stokes="I",
            mask="'map2d.im' > 4.0",
            stretch=True,
            imagemd="IM1",
            prec="d",
        )
        is True
    )
    ev = NativeImageView.open(ws / "eval_custom.im")
    assert ev.shape == (4, 4, 1, 3)
    assert ev.pixel_type == "float64"
    assert ev.units == "Jy/beam"
    assert ev.default_mask_name == "mask0"
    ev_p, ev_m = _read_pix_and_mask(ws / "eval_custom.im")
    exp_ev_p = cube_p[:, :, 0:1, 0:3] * 2.0 + spix_lo_p
    # CASA mask='map2d.im > 4.0' evaluates the comparison directly over pixel values.
    exp_ev_m = (cube_m[:, :, 0:1, 0:3]) & (map_p > 4.0)[:, :, None, None]
    assert np.array_equal(ev_m, exp_ev_m)
    assert np.allclose(ev_p[ev_m], exp_ev_p[ev_m], rtol=1e-10, atol=1e-10)

    # 2. imagename="" with explicit outfile="" creating default immath_results.im
    assert tasks.immath(imagename="", expr="'spix_lo.im' + 'deg4d.im'", outfile="") is True
    assert (ws / "immath_results.im").is_dir()
    res_p, res_m = _read_pix_and_mask(ws / "immath_results.im")
    assert bool(res_m.all()) is True
    assert np.allclose(res_p, spix_lo_p + float(deg_p[0, 0, 0, 0]), rtol=1e-6, atol=1e-6)

    # 3. rebin + box + region
    assert (
        tasks.immath(
            imagename=["cube_iquv.im"],
            chans="0",
            expr="rebin(IM0, [2, 2, 1, 1])",
            outfile="rebin_out.im",
        )
        is True
    )
    assert NativeImageView.open(ws / "rebin_out.im").shape == (2, 2, 4, 1)
    _assert_matches_oracle(ws / "rebin_out.im", oracle_cases["eval_rebin_2x2"]["expected"])

    assert (
        tasks.immath(
            imagename=["map2d.im"],
            expr="IM0",
            region="poly [[0pix,0pix],[2pix,0pix],[2pix,2pix],[0pix,2pix]]",
            outfile="poly_out.im",
        )
        is True
    )
    poly = NativeImageView.open(ws / "poly_out.im")
    assert poly.shape == (3, 3)
    poly_p, poly_mask = _read_pix_and_mask(ws / "poly_out.im")
    assert bool(poly_mask[0, 2]) is True
    assert bool(poly_mask[2, 2]) is True
    assert bool(poly_mask[1, 1]) is False
    assert np.allclose(poly_p[poly_mask], map_p[:3, :3][poly_mask], rtol=1e-6, atol=1e-6)

    assert (
        tasks.immath(
            imagename=["map2d.im"],
            expr="IM0",
            box="0,0,1,1,2,2,3,3",
            outfile="box_out.im",
        )
        is True
    )
    assert NativeImageView.open(ws / "box_out.im").shape == (4, 4)
    _assert_matches_oracle(ws / "box_out.im", oracle_cases["eval_box_multi"]["expected"])

    # 4. spix, pola (+polithresh), poli (+sigma), lpoli, tpoli
    assert tasks.immath(imagename=["spix_lo.im", "spix_hi.im"], mode="spix", outfile="spix_out.im") is True
    spix = NativeImageView.open(ws / "spix_out.im")
    assert spix.shape == (4, 4, 1, 3)
    assert spix.units == ""
    _assert_matches_oracle(ws / "spix_out.im", oracle_cases["spix_two_images"]["expected"])

    assert (
        tasks.immath(
            imagename=["q_plane.im", "u_plane.im"],
            mode="pola",
            polithresh="100mJy/beam",
            mask="IM0 > 999",
            stokes="Q",
            outfile="pola_out.im",
        )
        is True
    )
    pola = NativeImageView.open(ws / "pola_out.im")
    assert pola.shape == (4, 4, 1, 2)
    assert pola.units == "deg"
    assert pola.default_mask_name == "mask0"
    _assert_matches_oracle(ws / "pola_out.im", oracle_cases["pola_multifile_polithresh"]["expected"], rtol=1e-3, atol=1e-2)

    assert (
        tasks.immath(
            imagename=["q_plane.im", "u_plane.im", "v_plane.im"],
            mode="poli",
            sigma="100mJy/beam",
            outfile="poli_out.im",
        )
        is True
    )
    assert NativeImageView.open(ws / "poli_out.im").units == "Jy/beam"
    _assert_matches_oracle(ws / "poli_out.im", oracle_cases["poli_multifile_sigma_debias"]["expected"])

    assert (
        tasks.immath(
            imagename=["cube_iquv.im"],
            mode="lpoli",
            sigma="0.06Jy/beam",
            outfile="lpoli_out.im",
        )
        is True
    )
    assert NativeImageView.open(ws / "lpoli_out.im").shape == (4, 4, 1, 3)
    _assert_matches_oracle(ws / "lpoli_out.im", oracle_cases["lpoli_debias"]["expected"])

    assert (
        tasks.immath(
            imagename=["q_plane.im", "u_plane.im", "v_plane.im"],
            mode="tpoli",
            outfile="tpoli_out.im",
        )
        is True
    )
    assert NativeImageView.open(ws / "tpoli_out.im").shape == (4, 4, 1, 2)
    _assert_matches_oracle(ws / "tpoli_out.im", oracle_cases["tpoli_multifile"]["expected"])


def test_immath_casa_readback_interoperability(
    immath_bin: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    if not CASA_PYTHON.exists():
        pytest.skip(f"CASA 6.7.6.14 python3 not present at {CASA_PYTHON}")
    _ = immath_bin
    ws = _stage_immath_fixtures(tmp_path)
    monkeypatch.chdir(ws)

    outputs = [
        ("out_f32.im", dict(imagename=["cube_iquv.im", "map2d.im"], expr="IM0 + IM1")),
        ("out_f64.im", dict(imagename=["spix_lo.im"], expr="IM0 * 1.25", prec="double")),
        ("out_spix.im", dict(imagename=["spix_lo.im", "spix_hi.im"], mode="spix")),
        ("out_pola.im", dict(imagename=["q_plane.im", "u_plane.im"], mode="pola", polithresh="100mJy/beam")),
        ("out_poli.im", dict(imagename=["cube_iquv.im"], mode="poli")),
        ("out_lpoli.im", dict(imagename=["cube_iquv.im"], mode="lpoli", sigma="0.06Jy/beam")),
        ("out_tpoli.im", dict(imagename=["q_plane.im", "u_plane.im", "v_plane.im"], mode="tpoli")),
        (
            "out_submask.im",
            dict(
                imagename=["cube_iquv.im"],
                expr="IM0",
                chans="0,2",
                stokes="IU",
                mask="'map2d.im' > 4.0",
                stretch=True,
            ),
        ),
        ("out_rebin.im", dict(imagename=["cube_iquv.im"], chans="0", expr="rebin(IM0, [2, 2, 1, 1])")),
    ]
    rs_summary = {}
    for name, kwargs in outputs:
        assert tasks.immath(outfile=name, **kwargs) is True
        im = NativeImageView.open(ws / name)
        shape = im.shape
        pix = np.asarray(im.get_slice((0,) * len(shape), shape))
        m_arr = im.get_mask_slice((0,) * len(shape), shape)
        mask = np.ones(shape, dtype=bool) if m_arr is None else np.asarray(m_arr, dtype=bool)
        rs_summary[name] = {
            "shape": list(shape),
            "pixel_type": "float" if im.pixel_type == "float32" else "double",
            "bunit": im.units,
            "pix_list": np.where(np.isnan(pix), None, pix.astype(float)).ravel(order="C").tolist(),
            "mask_list": mask.ravel(order="C").tolist(),
        }

    probe = f"""
import json, casatools, numpy as np
from pathlib import Path
ws = Path({str(ws)!r})
expected = json.loads({json.dumps(rs_summary)!r})
expected_stokes = {{
    "out_f32.im": ["I", "Q", "U", "V"],
    "out_f64.im": ["I"],
    "out_spix.im": ["I"],
    "out_pola.im": ["Pangle"],
    "out_poli.im": ["Ptotal"],
    "out_lpoli.im": ["Plinear"],
    "out_tpoli.im": ["Ptotal"],
    "out_submask.im": ["I", "Q", "U"],
    "out_rebin.im": ["I", "Q", "U", "V"],
}}
ia = casatools.image()
for name, exp in expected.items():
    ia.open(str(ws / name))
    try:
        assert list(ia.shape()) == exp["shape"], (name, list(ia.shape()), exp["shape"])
        assert ia.pixeltype() == exp["pixel_type"], (name, ia.pixeltype(), exp["pixel_type"])
        assert ia.brightnessunit() == exp["bunit"], (name, ia.brightnessunit(), exp["bunit"])
        cs = ia.coordsys()
        st = [str(s) for s in cs.stokes()]
        cs.done()
        assert st == expected_stokes[name], (name, st, expected_stokes[name])
        pix = ia.getchunk().ravel(order="C")
        mask = ia.getchunk(getmask=True).ravel(order="C")
        assert [bool(b) for b in mask] == exp["mask_list"], (name, "mask mismatch")
        for idx, (got, ref, valid) in enumerate(zip(pix, exp["pix_list"], exp["mask_list"])):
            if not valid:
                continue
            if ref is None:
                assert np.isnan(got), (name, idx, got, ref)
            else:
                assert np.isclose(float(got), float(ref), rtol=1e-6, atol=1e-6), (name, idx, got, ref)
        hist = [str(h) for h in ia.history(list=False)]
        assert any("immath" in h for h in hist), (name, hist)
    finally:
        ia.close()
print("CASA_READBACK_OK")
"""
    env = dict(os.environ)
    env["MPLCONFIGDIR"] = "/tmp/mpl"
    proc = subprocess.run(
        [str(CASA_PYTHON), "-I", "-c", probe],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert proc.returncode == 0, f"CASA readback failed:\n{proc.stdout}\n{proc.stderr}"
    assert "CASA_READBACK_OK" in proc.stdout
