#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA 6.7.6.14 oracle fixtures and expected outputs for `exportfits`."""

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
DATA_DIR = ROOT / "libs" / "casacore" / "casa-images" / "tests" / "data" / "exportfits"


def encode_float(val):
    v = float(val)
    if math.isnan(v):
        return "NaN"
    if math.isinf(v):
        return "Inf" if v > 0 else "-Inf"
    return v


def parse_primary_header(fits_path):
    raw = Path(fits_path).read_bytes()
    cards = []
    pos = 0
    while pos + 80 <= len(raw):
        card = raw[pos : pos + 80].decode("ascii", errors="replace")
        pos += 80
        cards.append(card.rstrip())
        if card.startswith("END     "):
            break

    kv = {}
    history = []
    for card in cards:
        if card.startswith("HISTORY "):
            history.append(card)
            continue
        if "=" not in card[:10]:
            continue
        key = card[:8].strip()
        rest = card[10:].strip()
        if rest.startswith("'"):
            # Unescape FITS string literal
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
                    if "." in val_part or "E" in val_part or "D" in val_part:
                        kv[key] = float(val_part.replace("D", "E"))
                    else:
                        kv[key] = int(val_part)
                except ValueError:
                    kv[key] = val_part

    raw_i16 = None
    if kv.get("BITPIX") == 16:
        end_idx = raw.index(b"END     ")
        data_off = ((end_idx // 2880) + 1) * 2880
        count = 1
        for ax in range(1, int(kv["NAXIS"]) + 1):
            count *= int(kv[f"NAXIS{ax}"])
        raw_i16 = [int(x) for x in np.frombuffer(raw[data_off : data_off + count * 2], dtype=">i2")]

    return kv, history, raw_i16


def strip_table_locks(root_dir):
    for lock in Path(root_dir).rglob("table.lock"):
        lock.unlink()


def create_inputs(ia):
    if DATA_DIR.exists():
        shutil.rmtree(DATA_DIR)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    def make_im(
        name,
        shape,
        stokes="I",
        freq_frame="LSRK",
        restfreq="1.420405751786GHz",
        bunit="Jy/beam",
        object_name="TestObj",
        vals=None,
        mask=None,
        single_beam=True,
        multi_beam=False,
        hist=None,
    ):
        p = DATA_DIR / name
        ia.fromshape(str(p), list(shape), overwrite=True)
        cs = ia.coordsys()
        cs.setstokes(stokes)
        cs.setreferencecode(freq_frame, type="spectral", adjust=False)
        cs.setrestfrequency(restfreq if restfreq else "0Hz")
        cs.setreferencevalue([0.0, 0.0], type="direction")
        cs.setreferencevalue([1.42e9], type="spectral")
        cs.setincrement([1.0e6], type="spectral")
        cs.setreferencepixel([1.0], type="spectral")
        ia.setcoordsys(cs.torecord())
        cs.done()
        if bunit:
            ia.setbrightnessunit(bunit)
        if object_name:
            ia.setmiscinfo({"object": object_name})
        if multi_beam:
            npol = shape[2]
            nchan = shape[3]
            for ch in range(nchan):
                for pol in range(npol):
                    ia.setrestoringbeam(
                        major=f"{3.0 + ch + 0.5 * pol}arcsec",
                        minor="2arcsec",
                        pa=f"{10 * ch - 5 * pol}deg",
                        channel=ch,
                        polarization=pol,
                    )
        elif single_beam:
            ia.setrestoringbeam(major="4arcsec", minor="2arcsec", pa="30deg")
        if vals is not None:
            ia.putchunk(vals)
        if mask is not None:
            ia.putregion(pixelmask=mask)
        if hist:
            ia.sethistory(origin="test_origin", history=hist)
        ia.close()
        return p

    # 1. cube_i.im [4, 4, 1, 3]
    vals4 = np.zeros((4, 4, 1, 3), dtype=np.float32)
    for x in range(4):
        for y in range(4):
            for c in range(3):
                vals4[x, y, 0, c] = (x + 2 * y + 3 * c) * 0.5 - 3.0
    mask4 = np.ones((4, 4, 1, 3), dtype=bool)
    mask4[0, 0, 0, 0] = False
    cube_i = make_im(
        "cube_i.im",
        [4, 4, 1, 3],
        vals=vals4,
        mask=mask4,
        hist=["short history entry", "a" * 95 + " end_of_long_line"],
    )

    # 2. iquv.im [3, 2, 4, 3] with per-plane beams
    vals_iquv = np.zeros((3, 2, 4, 3), dtype=np.float32)
    for x in range(3):
        for y in range(2):
            for s in range(4):
                for c in range(3):
                    vals_iquv[x, y, s, c] = x * 100.0 + y * 10.0 + s + 0.1 * c
    make_im(
        "iquv.im",
        [3, 2, 4, 3],
        stokes="I Q U V",
        bunit="K",
        vals=vals_iquv,
        single_beam=False,
        multi_beam=True,
    )

    # 3. deg_2d.im [4, 4, 1, 1]
    vals_deg = np.arange(16, dtype=np.float32).reshape((4, 4, 1, 1))
    deg_2d = make_im("deg_2d.im", [4, 4, 1, 1], bunit="Jy/pixel", vals=vals_deg)

    # 4. dir1_deg.im [1, 4, 1, 3]
    vals_dir1 = np.arange(12, dtype=np.float32).reshape((1, 4, 1, 3))
    make_im("dir1_deg.im", [1, 4, 1, 3], vals=vals_dir1)

    # 5. const.im [2, 2, 1, 1]
    make_im(
        "const.im",
        [2, 2, 1, 1],
        vals=np.full((2, 2, 1, 1), 3.5, dtype=np.float32),
    )

    # 6. all_masked.im [2, 2, 1, 1]
    make_im(
        "all_masked.im",
        [2, 2, 1, 1],
        vals=np.full((2, 2, 1, 1), 3.5, dtype=np.float32),
        mask=np.zeros((2, 2, 1, 1), dtype=bool),
    )

    # 7. no_mask.im [2, 2, 1, 1]
    make_im(
        "no_mask.im",
        [2, 2, 1, 1],
        vals=np.array([-2.0, 0.0, 1.0, 6.0], dtype=np.float32).reshape((2, 2, 1, 1)),
    )

    # 8. no_restfreq.im [2, 2, 1, 3]
    make_im(
        "no_restfreq.im",
        [2, 2, 1, 3],
        restfreq=None,
        vals=np.arange(12, dtype=np.float32).reshape((2, 2, 1, 3)),
    )

    # 9. bary_spec.im [2, 2, 1, 4]
    make_im(
        "bary_spec.im",
        [2, 2, 1, 4],
        freq_frame="BARY",
        vals=np.arange(16, dtype=np.float32).reshape((2, 2, 1, 4)),
    )

    # 10. all_deg.im [1, 1, 1, 1]
    make_im("all_deg.im", [1, 1, 1, 1], vals=np.ones((1, 1, 1, 1), dtype=np.float32))

    # 11. mb_1stokes.im [4, 4, 1, 3] multi-beam across channels with len-1 Stokes
    make_im(
        "mb_1stokes.im",
        [4, 4, 1, 3],
        vals=vals4,
        single_beam=False,
        multi_beam=True,
    )

    # 12. input_2d.fits (2D FITS input for exportfits)
    casatasks.exportfits(
        imagename=str(deg_2d),
        fitsimage=str(DATA_DIR / "input_2d.fits"),
        dropdeg=True,
        history=False,
        overwrite=True,
    )


def record_cases(ia):
    cases_spec = [
        ("default_vel0_opt0", "cube_i.im", dict(velocity=False, optical=False, history=False)),
        ("vel0_opt1", "cube_i.im", dict(velocity=False, optical=True, history=False)),
        ("vel1_opt0", "cube_i.im", dict(velocity=True, optical=False, history=False)),
        ("vel1_opt1", "cube_i.im", dict(velocity=True, optical=True, history=False)),
        ("bary_vel1_opt0", "bary_spec.im", dict(velocity=True, optical=False, history=False)),
        ("bary_vel1_opt1", "bary_spec.im", dict(velocity=True, optical=True, history=False)),
        ("norest_vel1_opt1", "no_restfreq.im", dict(velocity=True, optical=True, history=False)),
        ("bitpix16_auto_masked", "cube_i.im", dict(bitpix=16, history=False)),
        ("bitpix16_clipped", "cube_i.im", dict(bitpix=16, minpix=-1.5, maxpix=4.0, history=False)),
        ("bitpix16_nomask_auto", "no_mask.im", dict(bitpix=16, history=False)),
        ("bitpix16_nomask_explicit", "no_mask.im", dict(bitpix=16, minpix=-1.0, maxpix=4.0, history=False)),
        ("bitpix16_const", "const.im", dict(bitpix=16, history=False)),
        ("bitpix16_all_masked", "all_masked.im", dict(bitpix=16, history=False)),
        ("bitpix32_ignores_min_max", "no_mask.im", dict(bitpix=-32, minpix=-1.0, maxpix=1.0, history=False)),
        ("dropstokes_1stokes", "cube_i.im", dict(dropstokes=True, history=False)),
        ("dropstokes_iquv_kept", "iquv.im", dict(dropstokes=True, history=False)),
        ("stokeslast_false_cube", "cube_i.im", dict(stokeslast=False, history=False)),
        ("stokeslast_false_iquv", "iquv.im", dict(stokeslast=False, history=False)),
        ("stokeslast_true_iquv", "iquv.im", dict(stokeslast=True, history=False)),
        ("dropdeg_deg2d", "deg_2d.im", dict(dropdeg=True, history=False)),
        ("dropdeg_dir1", "dir1_deg.im", dict(dropdeg=True, history=False)),
        ("mb_1stokes_dropstokes", "mb_1stokes.im", dict(dropstokes=True, history=False)),
        ("history_true", "cube_i.im", dict(history=True)),
        ("history_false", "cube_i.im", dict(history=False)),
        ("fits_input", "input_2d.fits", dict(history=False)),
    ]

    recorded = []
    with tempfile.TemporaryDirectory(prefix="exportfits_oracle_") as tmpdir:
        tmp = Path(tmpdir)
        for name, imagename, kwargs in cases_spec:
            out_fits = tmp / f"{name}.fits"
            casatasks.exportfits(
                imagename=str(DATA_DIR / imagename),
                fitsimage=str(out_fits),
                **kwargs,
            )
            hdr, history_cards, raw_i16 = parse_primary_header(out_fits)
            ia.open(str(out_fits))
            try:
                shape = [int(x) for x in ia.shape()]
                pix = ia.getchunk()
                mask = ia.getchunk(getmask=True)
                ia_history = [str(h) for h in ia.history(list=False)]
                rb = ia.restoringbeam()
            finally:
                ia.close()

            recorded.append(
                {
                    "name": name,
                    "imagename": imagename,
                    "params": kwargs,
                    "shape": shape,
                    "header": hdr,
                    "history_cards_count": len(history_cards),
                    "has_casa_logtable_markers": (
                        "HISTORY CASA START LOGTABLE" in history_cards
                        and "HISTORY CASA END LOGTABLE" in history_cards
                    ),
                    "ia_history_count": len(ia_history),
                    "has_perplanebeams": "beams" in rb,
                    "pixels_f_order": [encode_float(x) for x in pix.ravel(order="F")],
                    "mask_f_order": [bool(x) for x in mask.ravel(order="F")],
                    "raw_i16": raw_i16,
                }
            )

        error_cases = []
        existing = tmp / "existing.fits"
        casatasks.exportfits(
            imagename=str(DATA_DIR / "no_mask.im"),
            fitsimage=str(existing),
            history=False,
        )
        err_specs = [
            ("bad_bitpix_8", "no_mask.im", str(tmp / "e1.fits"), dict(bitpix=8)),
            ("empty_fitsimage", "no_mask.im", "", dict()),
            ("existing_no_overwrite", "no_mask.im", str(existing), dict(overwrite=False)),
            ("all_deg_dropdeg_true", "all_deg.im", str(tmp / "e2.fits"), dict(dropdeg=True)),
            ("missing_imagename", "missing.im", str(tmp / "e3.fits"), dict()),
        ]
        for name, imagename, fitsimage, kwargs in err_specs:
            try:
                src = str(DATA_DIR / imagename) if imagename != "missing.im" else str(tmp / "missing.im")
                casatasks.exportfits(imagename=src, fitsimage=fitsimage, **kwargs)
                raise AssertionError(f"Expected failure for error case {name}")
            except Exception as exc:
                error_cases.append(
                    {
                        "name": name,
                        "imagename": imagename,
                        "params": kwargs,
                        "error_type": type(exc).__name__,
                        "message": str(exc),
                    }
                )

    strip_table_locks(DATA_DIR)
    (DATA_DIR / "expected.json").write_text(
        json.dumps(
            {
                "casa_version": "6.7.6.14",
                "cases": recorded,
                "error_cases": error_cases,
            },
            indent=2,
        )
        + "\n"
    )
    print(f"Wrote {len(recorded)} oracle cases and {len(error_cases)} error cases to {DATA_DIR / 'expected.json'}")


def main():
    ia = casatools.image()
    create_inputs(ia)
    record_cases(ia)
    strip_table_locks(DATA_DIR)


if __name__ == "__main__":
    main()
