#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Capture CASA 6.7.6.14 imsubimage inputs and results.

Run with CASA's Python. Output images are temporary; expected.json holds their
pixel values, masks and sampled WCS for offline checks and live CASA readback.
"""

import json
import os
import shutil
from pathlib import Path
import tempfile

from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False

import casatasks
import casatools
import numpy as np

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "crates/casa-images/tests/data/imsubimage"
AXIS_MAPS = ROOT / "crates/casa-images/tests/data/casa-6.7.6/axis_maps"
HISTORY = "imsubimage oracle input history"
BOX = "box[[1pix, 0pix], [3pix, 2pix]]"
CIRCLE = "circle[[4pix, 4pix], 1.5pix]"
POLY = "poly[[1pix, 1pix], [5pix, 2pix], [2pix, 4pix]]"


def make_inputs():
    ia = casatools.image()
    cs = casatools.coordsys()
    cs.addcoordinate(direction=True, stokes="I Q U V")
    cs.addcoordinate(spectral=True)
    coords = cs.torecord()
    cs.done()

    data = np.arange(6 * 6 * 4 * 4, dtype=np.float32).reshape(6, 6, 4, 4)
    assert ia.fromarray("cube.im", data, csys=coords, overwrite=True)
    ia.setbrightnessunit("Jy/beam")
    ia.sethistory(origin="imsubimage_oracle", history=[HISTORY])
    for channel in range(4):
        for pol in range(4):
            ia.setrestoringbeam(
                major=f"{3 + channel + pol / 4}arcsec", minor="2arcsec",
                pa=f"{10 * channel - pol}deg", channel=channel, polarization=pol,
            )
    # Named mask, with a different default mask, to distinguish the two.
    ia.calcmask('"cube.im" > 150', name="selected", asdefault=False)
    ia.calcmask('"cube.im" != 0', name="valid", asdefault=True)
    ia.done()
    tb = casatools.table()
    tb.open("cube.im/logtable", nomodify=False)
    row = list(tb.getcol("MESSAGE")).index(HISTORY)
    tb.putcell("PRIORITY", row, "WARN")
    tb.done()

    plane = data[:, :, 0, 0]
    for name, pixels, precision, linear in [
        ("plane.im", plane, "f", False),
        ("double.im", plane.astype(np.float64) + 2**-30, "d", False),
        ("complex.im", plane + 1j * (plane + 1), "f", False),
        ("dcomplex.im", plane.astype(np.float64) + 1j * (plane + 2**-20), "d", False),
        ("point.im", np.ones((1, 1, 1, 1)), "f", False),
        ("stretch.im", plane[:, :, None, None], "f", False),
    ]:
        assert ia.fromarray(name, pixels, type=precision, overwrite=True, linear=linear)
        if name == "double.im":
            ia.calcmask('"double.im" > 150', name="selected", asdefault=False)
        ia.done()

    casatasks.exportfits(imagename="plane.im", fitsimage="plane.fits", overwrite=True)
    fits = Path("plane.fits").read_bytes()
    data_offset = next(i for i in range(0, len(fits), 80) if fits[i:i+8] == b"END     ")
    data_offset = (data_offset // 2880 + 1) * 2880
    header = bytearray(fits[:data_offset])
    bitpix = header.find(b"BITPIX  ")
    header[bitpix:bitpix+80] = b"BITPIX  =                  -64".ljust(80)
    pixels = np.asarray(plane.astype(np.float64) + 2**-30, dtype=">f8").tobytes(order="F")
    Path("double.fits").write_bytes(header + pixels + bytes((-len(pixels)) % 2880))
    casatasks.imtrans(imagename="cube.im", outfile="transposed.im", order="3012")
    Path("circle.crtf").write_text(f"#CRTFv0\n{BOX}\n{CIRCLE}\n")
    ia.open("plane.im")
    cs = ia.coordsys()
    rg = casatools.regionmanager()
    assert rg.tofile("saved.rgn", rg.fromtext(POLY, ia.shape(), cs.torecord()))
    rg.done()
    cs.done()
    ia.done()
    ia.open("cube.im")
    cs = ia.coordsys()
    rg = casatools.regionmanager()
    region = rg.wbox(blc=["1pix", "1pix"], trc=["3pix", "4pix"],
                     pixelaxes=[0, 1], csys=cs.torecord())
    assert rg.tofile("box.rgn", region)
    rg.done()
    cs.done()
    ia.done()


def history_marker(image):
    tb = casatools.table()
    tb.open(str(Path(image) / "logtable"))
    try:
        messages = list(tb.getcol("MESSAGE"))
        if HISTORY not in messages:
            return None
        row = messages.index(HISTORY)
        return [tb.getcell(column, row) for column in ("TIME", "PRIORITY", "LOCATION", "MESSAGE")]
    finally:
        tb.done()


def snapshot(path):
    ia = casatools.image()
    assert ia.open(str(path))
    try:
        shape = list(map(int, ia.shape()))
        valid = ia.getchunk(getmask=True).ravel()
        data = ia.getchunk().ravel()
        pixels = [
            ([float(p.real), float(p.imag)] if np.iscomplexobj(data) else float(p))
            if good else None for p, good in zip(data, valid)
        ]
        cs = ia.coordsys()
        try:
            coords = cs.torecord()
            names = list(cs.names())
            units = list(cs.units())
            world = [
                ia.toworld(pixel)["numeric"].tolist()
                for pixel in ([0] * len(shape), [n - 1 for n in shape])
            ]
            maps = {
                k: v.tolist() for k, v in coords.items()
                if k.startswith(("pixelmap", "worldmap"))
            }
        finally:
            cs.done()
        history = ia.history(list=False)
        return dict(
            shape=shape, pixel_type=ia.pixeltype(), unit=ia.brightnessunit(),
            pixels=pixels, masks=list(ia.maskhandler("get")), names=names, units=units, world=world,
            maps=maps, beams=ia.restoringbeam(),
            copied_history=history_marker(path),
            task_history=any("imsubimage(" in line for line in history),
        )
    finally:
        ia.done()


def capture_cases(output):
    cases = {}

    def capture(name, imagename="cube.im", *, error=False, **args):
        outfile = args.pop("outfile", str(output / (name + ".im")))
        try:
            casatasks.imsubimage(imagename=imagename, outfile=outfile, **args)
        except Exception as exc:
            if not error:
                raise
            result = {"error": str(exc).split(" at File:", 1)[0]}
        else:
            if error:
                raise AssertionError(f"{name}: CASA unexpectedly succeeded")
            result = snapshot(outfile)
        cases[name] = dict(imagename=imagename, params=args, **result)

    capture("box", box="1,1,4,4", chans="1~2", stokes="QU", verbose=True)
    capture("gaps", box="0,0,2,2", chans="0;2", stokes="IV")
    capture("dropdeg", chans="2", stokes="Q", dropdeg=True)
    capture("keep_list", chans="2", stokes="Q", dropdeg=True, keepaxes=[-1, 3])
    capture("keep_scalar", chans="2", stokes="Q", dropdeg=True, keepaxes=2)
    capture("drop_all", "point.im", dropdeg=True, error=True)
    capture("transposed", "transposed.im", box="1,2,4,5", chans="2", stokes="Q", dropdeg=True, keepaxes=[3])
    capture("one_direction", box="2,0,2,5", chans="1", stokes="Q", dropdeg=True)
    capture("other_direction", box="0,2,5,2", chans="1", stokes="Q", dropdeg=True)
    for image in ("spectral_first.image", "transposed.image"):
        capture("axis_maps_" + image.removesuffix(".image"), f"axis_maps/{image}",
                dropdeg=True, chans="1", stokes="I")

    capture("lel", mask='"cube.im" > 200', box="1,1,4,4", chans="1", stokes="Q")
    capture("bare_lel", mask="abs(cube.im) > 200", box="1,1,4,4", chans="1", stokes="Q")
    capture("mask_image", "plane.im", mask="plane.im")
    capture("named_mask", mask='mask("cube.im:selected")', box="1,1,4,4", stokes="Q", chans="1")
    capture("named_double_mask", "double.im", mask='mask("double.im:selected")', error=True)
    capture("all_true", "plane.im", mask='"plane.im" >= 0')
    capture("stretch", mask='"stretch.im" > 200', stretch=True, box="1,1,4,4")
    capture("no_stretch", mask='"stretch.im" > 200', error=True)
    capture("transposed_stretch", "transposed.im", mask='"plane.im" > 200', stretch=True)
    capture("complex_abs", "complex.im", mask='abs("complex.im" - 100) > 100')
    capture("complex_imag", "complex.im", mask='imag("complex.im") > 100')
    capture("complex_signed_imag", "complex.im", mask='imag(-"complex.im") < -100')
    capture("qualified_union", region=(
        "box[[0pix, 0pix], [1pix, 1pix]], corr=[I], range=[0pix, 1pix]\n"
        "box[[3pix, 3pix], [4pix, 4pix]], corr=[Q], range=[2pix, 3pix]"
    ))
    capture("saved_box", region="box.rgn")
    capture("saved_box_conflict", "plane.im", region="saved.rgn", box="0,0,4,4", error=True)
    capture("saved_chans_conflict", region="box.rgn", chans="1", error=True)
    capture("saved_stokes_conflict", region="box.rgn", stokes="Q", error=True)
    for name, text in [
        ("crtf_box", BOX),
        ("centerbox", "centerbox[[3pix, 2pix], [3pix, 2pix]]"),
        ("rotbox", "rotbox[[3pix, 3pix], [3pix, 2pix], 30deg]"),
        ("circle", CIRCLE),
        ("ellipse", "ellipse[[3pix, 2pix], [2.5pix, 1.2pix], 35deg]"),
        ("polygon", POLY),
        ("union", BOX + "\n" + CIRCLE),
        ("file", "circle.crtf"),
        ("saved", "saved.rgn"),
    ]:
        capture(name, "plane.im", region=text)
    capture("box_union", "plane.im", box="0,0,1,1", region=CIRCLE)
    for name in ("double", "complex", "dcomplex"):
        capture(name, name + ".im", box="1,1,3,4")
    capture("fits", "plane.fits", box="1,1,3,4")
    capture("fits64", "double.fits", box="1,1,3,4")
    capture("empty", "plane.im", outfile="", error=True)
    return cases


def main():
    version = casatasks.version_string()
    if version != "6.7.6.14":
        raise RuntimeError(f"Expected CASA 6.7.6.14, found {version}")
    original = Path.cwd()
    DATA.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as scratch:
        work = Path(scratch)
        inputs = work / "inputs"
        outputs = work / "outputs"
        inputs.mkdir()
        outputs.mkdir()
        # CASA may write table.lock beside inputs. Stage the existing images
        # alongside the generated inputs without modifying the shared fixture tree.
        shutil.copytree(AXIS_MAPS, work / "axis_maps")
        (inputs / "axis_maps").symlink_to(work / "axis_maps")
        os.chdir(inputs)
        try:
            casatasks.casalog.setlogfile(str(work / "casa.log"))
            make_inputs()
            cases = capture_cases(outputs)
        finally:
            os.chdir(original)
        for lock in inputs.rglob("table.lock"):
            lock.unlink()
        for path in inputs.iterdir():
            if path.is_symlink():
                continue
            target = DATA / path.name
            if path.is_dir():
                if target.is_dir():
                    shutil.rmtree(target)
                shutil.copytree(path, target)
            else:
                shutil.copyfile(path, target)
        # Compact each case without losing the per-case structure of the file.
        rows = [f"    {json.dumps(key)}: {json.dumps(case, allow_nan=False)}"
                for key, case in cases.items()]
        (DATA / "expected.json").write_text(
            f'{{\n  "casa_version": "{version}",\n  "cases": {{\n'
            + ",\n".join(rows) + "\n  }\n}\n"
        )


if __name__ == "__main__":
    main()
