#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA 6.7.6.14 oracle fixtures and expected outputs for `immath`."""

import json
import math
import os
from pathlib import Path
import shutil
import tempfile

import casatasks
import casatools
import numpy as np


ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = ROOT / "libs" / "casacore" / "casa-images" / "tests" / "data" / "immath"


def encode_float(val):
    v = float(val)
    if math.isnan(v):
        return "NaN"
    if math.isinf(v):
        return "Inf" if v > 0 else "-Inf"
    return v


def read_image_snapshot(ia, path):
    ia.open(str(path))
    try:
        shape = [int(x) for x in ia.shape()]
        pix = ia.getchunk()
        mask = ia.getchunk(getmask=True)
        bunit = ia.brightnessunit()
        pix_type = ia.pixeltype()
        csys = ia.coordsys()
        try:
            stokes = [str(s) for s in csys.stokes()]
        except Exception:
            stokes = []
        crpix = [float(x) for x in csys.referencepixel()["numeric"]]
        cdelt = [float(x) for x in csys.increment()["numeric"]]
        csys.done()
        masks = [str(m) for m in ia.maskhandler("get")]
        hist = [str(h) for h in ia.history(list=False)]
        return {
            "shape": shape,
            "pixel_type": pix_type,
            "bunit": bunit,
            "stokes": stokes,
            "crpix": crpix,
            "cdelt": cdelt,
            "masks": masks,
            "has_history": len(hist) > 0,
            "pixels": [encode_float(x) for x in pix.ravel(order="C")],
            "mask": [bool(x) for x in mask.ravel(order="C")],
        }
    finally:
        ia.close()


def make_csys(stokes_list, freq0_hz=1.0e9, dfreq_hz=5.0e8):
    cs = casatools.coordsys()
    cs.addcoordinate(direction=True, spectral=False, stokes=stokes_list)
    cs.addcoordinate(spectral=True)
    # Direction axis refval/inc/refpix defaults are already valid J2000 SIN;
    # set clear spectral values on pixel axis 3 (or 2 when 3D):
    sp_axis = 3 if stokes_list else 2
    cs.setreferencepixel([0.0], type="spectral")
    cs.setreferencevalue([freq0_hz], type="spectral")
    cs.setincrement([dfreq_hz], type="spectral")
    cs.setrestfrequency(1.420405751786e9)
    _ = sp_axis
    return cs


def build_inputs(ia):
    if DATA_DIR.exists():
        shutil.rmtree(DATA_DIR)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # 1. cube_iquv.im: [4, 4, 4, 3] (RA, Dec, [I,Q,U,V], 3 channels)
    arr = np.zeros((4, 4, 4, 3), dtype=np.float32)
    for x in range(4):
        for y in range(4):
            for c in range(3):
                arr[x, y, 0, c] = 2.0 + 0.5 * x + 0.25 * y + c
                arr[x, y, 1, c] = 0.04 * (x + 1) - 0.01 * y + 0.005 * c
                arr[x, y, 2, c] = 0.03 * (y + 1) + 0.01 * x - 0.005 * c
                arr[x, y, 3, c] = 0.01 * (x - y + c + 1)
    # Small Q/U corner for polithresh / sigma masking
    arr[0, 1, 1, :] = 0.001
    arr[0, 1, 2, :] = 0.001
    arr[0, 1, 3, :] = 0.0
    cs = make_csys(["I", "Q", "U", "V"], 1.0e9, 5.0e8)
    p = DATA_DIR / "cube_iquv.im"
    ia.fromarray(str(p), arr, csys=cs.torecord(), overwrite=True)
    ia.setbrightnessunit("Jy/beam")
    m = np.ones_like(arr, dtype=bool)
    m[0, 0, :, :] = False
    m[3, 3, 1, 1] = False
    ia.putregion(pixelmask=m)
    # Give cube_iquv.im a restoring beam too so imagemd / metadata copy is tested
    ia.setrestoringbeam(major="2arcsec", minor="1arcsec", pa="30deg")
    ia.close()
    cs.done()

    # 2 & 3. spix_lo.im and spix_hi.im: [4, 4, 1, 3]
    lo = np.full((4, 4, 1, 3), 4.0, dtype=np.float32)
    hi = np.full((4, 4, 1, 3), 16.0, dtype=np.float32)
    for x in range(4):
        for y in range(4):
            lo[x, y, 0, :] = 1.0 + x + 0.5 * y
            hi[x, y, 0, :] = 2.0 * (1.0 + x + 0.5 * y)
    # Edge cases: (0,0)->(0,0), (1,0)->(>0,0), (2,0)->(<0,>0)
    lo[0, 0, 0, :] = 0.0
    hi[0, 0, 0, :] = 0.0
    lo[1, 0, 0, :] = 2.0
    hi[1, 0, 0, :] = 0.0
    lo[2, 0, 0, :] = -1.0
    hi[2, 0, 0, :] = 2.0
    cs_lo = make_csys(["I"], 1.0e9, 5.0e8)
    # Channel 1 in both is 1.5e9 (f1 == f2!); ch 0 is 1e9 vs 2e9; ch 2 is 2e9 vs 2.5e9 -> make linear matching f1==f2 at ch1:
    # Actually make cs_hi linear starting at 2.0e9 step -5.0e8? No, +5e8 at ref=2e9, refpix=0 gives [2.0e9, 2.5e9, 3.0e9]
    # To make ch1 equal (1.5e9): cs_hi refval=2.0e9? Set ch0=2e9, dfreq=-0.5e9 gives [2.0e9, 1.5e9, 1.0e9]!
    cs_hi = make_csys(["I"], 2.0e9, -5.0e8)
    ia.fromarray(str(DATA_DIR / "spix_lo.im"), lo, csys=cs_lo.torecord(), overwrite=True)
    ia.setbrightnessunit("Jy/beam")
    ia.close()
    ia.fromarray(str(DATA_DIR / "spix_hi.im"), hi, csys=cs_hi.torecord(), overwrite=True)
    ia.setbrightnessunit("mJy/beam")
    ia.close()
    cs_lo.done()
    cs_hi.done()

    # 4. map2d.im: [4, 4]
    cs2 = casatools.coordsys()
    cs2.addcoordinate(direction=True)
    m2 = np.arange(16, dtype=np.float32).reshape((4, 4)) + 1.0
    ia.fromarray(str(DATA_DIR / "map2d.im"), m2, csys=cs2.torecord(), overwrite=True)
    ia.setbrightnessunit("Jy/beam")
    mask2 = np.ones((4, 4), dtype=bool)
    mask2[1, 1] = False
    ia.putregion(pixelmask=mask2)
    ia.close()
    cs2.done()

    # 5. deg4d.im: [4, 4, 1, 1]
    d4 = np.ones((4, 4, 1, 1), dtype=np.float32) * 3.0
    cs4 = make_csys(["I"], 1.0e9, 5.0e8)
    ia.fromarray(str(DATA_DIR / "deg4d.im"), d4, csys=cs4.torecord(), overwrite=True)
    ia.setbrightnessunit("K")
    ia.close()
    cs4.done()

    # 6. Single-Stokes q_plane.im, u_plane.im, v_plane.im, only_i.im, only_q.im
    for fname, st, val_fn in [
        ("q_plane.im", "Q", lambda x, y, c: 0.05 * (x + 1) + 0.01 * c),
        ("u_plane.im", "U", lambda x, y, c: 0.04 * (y + 1) - 0.01 * c),
        ("v_plane.im", "V", lambda x, y, c: 0.02 * (x + y + 1)),
        ("only_i.im", "I", lambda x, y, c: 1.0 + x),
        ("only_q.im", "Q", lambda x, y, c: 0.5 + y),
    ]:
        nchan = 2 if fname in ("q_plane.im", "u_plane.im", "v_plane.im") else 1
        pa = np.zeros((4, 4, 1, nchan), dtype=np.float32)
        for x in range(4):
            for y in range(4):
                for c in range(nchan):
                    pa[x, y, 0, c] = val_fn(x, y, c)
        csp = make_csys([st], 1.0e9, 5.0e8)
        ia.fromarray(str(DATA_DIR / fname), pa, csys=csp.torecord(), overwrite=True)
        ia.setbrightnessunit("Jy/beam")
        ia.close()
        csp.done()

    # 7. input.fits with NaN blank
    fits_arr = np.arange(1, 33, dtype=np.float32).reshape((4, 4, 1, 2))
    fits_arr[0, 2, 0, 1] = np.nan
    csf = make_csys(["I"], 1.0e9, 5.0e8)
    src_for_fits = DATA_DIR / "_tmp_fits_src.im"
    ia.fromarray(str(src_for_fits), fits_arr, csys=csf.torecord(), overwrite=True)
    ia.setbrightnessunit("Jy/beam")
    ia.tofits(str(DATA_DIR / "input.fits"), overwrite=True)
    ia.close()
    csf.done()
    shutil.rmtree(src_for_fits)


def main():
    ia = casatools.image()
    build_inputs(ia)

    results = {"cases": {}, "errors": {}}
    cwd = os.getcwd()
    try:
        os.chdir(DATA_DIR)
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)

            cases = [
                ("eval_arith_mod_pow", dict(imagename=["spix_lo.im", "spix_hi.im"], expr="(IM0 + IM1) * 2 - (IM0 ^ 2) + (IM1 % 3)")),
                ("eval_trig_log_amp_real", dict(imagename=["cube_iquv.im"], expr="amplitude(real(double(float(sin(IM0 * 0.1) + cos(IM0 * 0.1) + log(abs(IM0) + 1)))))")),
                ("eval_stats_mean_var_std_avdev", dict(imagename=["cube_iquv.im"], expr="IM0 * 0 + (mean(IM0) + variance(IM0) + stddev(IM0) + avdev(IM0) + sum(IM0) + min(IM0) + max(IM0) + median(IM0))")),
                ("eval_fractile_range", dict(imagename=["map2d.im"], expr="IM0 + fractile(IM0, 0.25) + fractilerange(IM0, 0.25, 0.75)")),
                ("eval_value_strip_mask", dict(imagename=["map2d.im"], expr="value(IM0) + 1.0")),
                ("eval_replace_preserve_mask", dict(imagename=["map2d.im"], expr="replace(IM0, 99.0)")),
                ("eval_iif_indexin_mask", dict(imagename=["cube_iquv.im", "spix_lo.im"], chans="0~2", stokes="I", expr="iif(indexin(3, [0, 2]), IM0, IM1)")),
                ("eval_rebin_2x2", dict(imagename=["cube_iquv.im"], chans="0", expr="rebin(IM0, [2, 2, 1, 1])")),
                ("eval_pa_func", dict(imagename=["u_plane.im", "q_plane.im"], expr="pa(IM0, IM1)")),
                ("eval_spectralindex_func", dict(imagename=["spix_lo.im", "spix_hi.im"], expr="spectralindex(IM0, IM1)")),
                ("eval_broadcast_4d_2d", dict(imagename=["cube_iquv.im", "map2d.im"], expr="IM0 + IM1")),
                ("eval_broadcast_4d_4d_deg", dict(imagename=["cube_iquv.im", "deg4d.im"], expr="IM0 - IM1")),
                ("eval_varnames_custom", dict(imagename=["spix_lo.im", "spix_hi.im"], varnames=["LO", "HI"], expr="LO * 2.5 + HI")),
                ("eval_imagename_empty_quoted", dict(imagename="", expr="'spix_lo.im' + 2.0 * \"deg4d.im\"")),
                ("eval_prec_double_minmatch", dict(imagename=["spix_lo.im"], expr="IM0 * 0.1234567890123", prec="d")),
                ("eval_imagemd_im1", dict(imagename=["spix_lo.im", "cube_iquv.im"], stokes="I", chans="0~2", expr="IM0 + IM1", imagemd="IM1")),
                ("eval_imagemd_path", dict(imagename=["spix_lo.im"], expr="IM0 * 2", imagemd="spix_hi.im")),
                ("eval_box_multi", dict(imagename=["map2d.im"], box="0,0,1,1,2,2,3,3", expr="IM0")),
                ("eval_chans_noncontig", dict(imagename=["cube_iquv.im"], chans="0,2", stokes="I", expr="IM0")),
                ("eval_stokes_noncontig", dict(imagename=["cube_iquv.im"], stokes="IU", chans="0", expr="IM0")),
                ("eval_region_crtf_poly", dict(imagename=["map2d.im"], region="poly [[0pix,0pix],[2pix,0pix],[2pix,2pix],[0pix,2pix]]", expr="IM0")),
                ("eval_mask_lel_stretch", dict(imagename=["cube_iquv.im"], mask="'map2d.im' > 4.0", stretch=True, expr="IM0")),
                ("eval_fits_input_nan", dict(imagename=["input.fits"], expr="IM0 * 2.0")),
                ("eval_median_even_n", dict(imagename=["map2d.im"], box="2,2,3,3", expr="IM0 * 0 + median(IM0) + fractile(IM0, 0.5)")),
                ("eval_indexin_negated_nonzero_start", dict(imagename=["cube_iquv.im", "spix_lo.im"], chans="1~2", stokes="I", expr="iif(!indexin(3, [0]), IM0, IM1)")),
                ("eval_mask_indexin_nonzero_start", dict(imagename=["cube_iquv.im"], chans="1~2", stokes="I", mask="indexin(3, [2])", expr="IM0")),
                ("spix_two_images", dict(imagename=["spix_lo.im", "spix_hi.im"], mode="spix")),
                ("pola_single", dict(imagename=["cube_iquv.im"], mode="pola", chans="0")),
                ("pola_multifile_polithresh", dict(imagename=["q_plane.im", "u_plane.im"], mode="pola", polithresh="100mJy/beam", mask="IM0 > 999", stokes="Q")),
                ("poli_total_4stokes", dict(imagename=["cube_iquv.im"], mode="poli")),
                ("poli_multifile_sigma_debias", dict(imagename=["q_plane.im", "u_plane.im", "v_plane.im"], mode="poli", sigma="100mJy/beam")),
                ("lpoli_debias", dict(imagename=["cube_iquv.im"], mode="lpoli", sigma="0.06Jy/beam")),
                ("tpoli_multifile", dict(imagename=["q_plane.im", "u_plane.im", "v_plane.im"], mode="tpoli")),
                ("poli_v_only", dict(imagename=["v_plane.im"], mode="poli")),
            ]

            for idx, (name, kwargs) in enumerate(cases):
                out_path = tmp_dir / f"out_{idx:02d}_{name}.im"
                call_kwargs = dict(kwargs)
                call_kwargs["outfile"] = str(out_path)
                casatasks.immath(**call_kwargs)
                results["cases"][name] = {
                    "params": kwargs,
                    "expected": read_image_snapshot(ia, out_path),
                }

            error_cases = [
                ("err_bad_prec", dict(imagename=["map2d.im"], expr="IM0", prec="int")),
                ("err_bad_imagemd", dict(imagename=["map2d.im"], expr="IM0", imagemd="no_such.im")),
                ("err_bad_polithresh_unit", dict(imagename=["cube_iquv.im"], mode="pola", polithresh="5K")),
                ("err_spix_one_image", dict(imagename=["spix_lo.im"], mode="spix")),
                ("err_tpoli_missing_v", dict(imagename=["q_plane.im", "u_plane.im"], mode="tpoli")),
                ("err_lpoli_only_q", dict(imagename=["only_q.im"], mode="lpoli")),
                ("err_pola_no_quv", dict(imagename=["only_i.im"], mode="pola")),
                ("err_mask_no_stretch", dict(imagename=["cube_iquv.im"], mask="'map2d.im' > 4.0", stretch=False, expr="IM0")),
            ]
            for idx, (name, kwargs) in enumerate(error_cases):
                out_path = tmp_dir / f"err_{idx:02d}.im"
                call_kwargs = dict(kwargs)
                call_kwargs["outfile"] = str(out_path)
                try:
                    casatasks.immath(**call_kwargs)
                except Exception as exc:
                    results["errors"][name] = {
                        "params": kwargs,
                        "error": str(exc),
                    }
                else:
                    raise RuntimeError(f"expected error case {name} to fail in CASA!")

        (DATA_DIR / "expected.json").write_text(json.dumps(results, indent=2) + "\n")
    finally:
        os.chdir(cwd)
        for lock in DATA_DIR.rglob("table.lock"):
            lock.unlink()


if __name__ == "__main__":
    main()
