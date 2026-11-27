#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Capture CASA 6.7.6.14 imsubimage inputs and results.

Run with CASA's Python. Output images are temporary; expected.json holds their
pixel values, masks and sampled WCS for offline checks and live CASA readback.
"""

import copy
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
DATA = ROOT / "libs/casacore/casa-images/tests/data/imsubimage"
AXIS_MAPS = ROOT / "libs/casacore/casa-images/tests/data/casa-6.7.6/axis_maps"
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

    cs = casatools.coordsys()
    cs.addcoordinate(direction=True)
    assert cs.setunits(["deg", "deg"])
    assert cs.setreferencepixel([2.2, 3.4])
    assert cs.setreferencevalue([60, 70])
    assert cs.setincrement([-0.75, 0.67])
    assert ia.fromarray("relative_sky.im", plane, csys=cs.torecord(), overwrite=True)
    ia.done()
    assert cs.setreferencepixel([0, 0])
    assert cs.setreferencevalue([0, 0])
    assert cs.setincrement([-30, 30])
    assert ia.fromarray("relative_wide.im", plane, csys=cs.torecord(), overwrite=True)
    ia.done()
    cs.done()

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
    for name, image, axes, mode, blc, trc in [
        ("relative_ref", "cube.im", [0, 1], "relref",
         ["1.7arcmin", "-1.6arcmin"], ["-1.2arcmin", "1.5arcmin"]),
        ("relative_center", "cube.im", [0, 1], "relcen",
         ["1.2arcmin", "-1.2arcmin"], ["-0.9arcmin", "0.9arcmin"]),
        ("relative_reordered", "transposed.im", [1, 2], "relcen",
         ["1.2arcmin", "-1.2arcmin"], ["-0.9arcmin", "0.9arcmin"]),
        ("relative_sky", "relative_sky.im", [0, 1], "relref",
         ["1.5deg", "-1.1deg"], ["-1.05deg", "0.75deg"]),
        ("relative_disjoint", "cube.im", [0, 1], "relref",
         ["-10arcmin", "0arcmin"], ["-12arcmin", "1arcmin"]),
        ("relative_pixels_world", "relative_sky.im", [1, 0], "relref",
         ["0pix", "0.5deg"], ["1pix", "-0.5deg"]),
        ("relative_wide", "relative_wide.im", [0, 1], "relref",
         ["0deg", "0deg"], ["-30deg", "30deg"]),
    ]:
        assert ia.open(image)
        cs = ia.coordsys()
        rg = casatools.regionmanager()
        region = rg.wbox(blc=blc, trc=trc, pixelaxes=axes, csys=cs.torecord(), absrel=mode)
        assert rg.tofile(name + ".rgn", region)
        rg.done()
        cs.done()
        ia.done()
    ia.open("cube.im")
    cs = ia.coordsys()
    rg = casatools.regionmanager()
    region = rg.wbox(blc=["1pix", "1pix"], trc=["3pix", "4pix"],
                     pixelaxes=[0, 1], csys=cs.torecord())
    assert rg.tofile("box.rgn", region)
    make_compound_regions(ia, cs.torecord(), rg)
    make_extended_regions(ia, cs.torecord(), rg)
    make_relative_shapes(cs.torecord(), rg)
    for name, lower, upper, stride in [
        ("stride", [0, 1, 0, 0], [5, 5, 3, 3], [2, 2, 1, 2]),
        ("stride_degenerate", [0, 1, 1, 0], [5, 5, 2, 1], [2, 2, 3, 4]),
        ("stride_tabular", [0, 0, 0, 0], [3, 2, 1, 0], [2, 1, 1, 1]),
    ]:
        assert rg.tofile(name + ".rgn", rg.box(blc=lower, trc=upper, inc=stride))
    rg.done()
    cs.done()
    ia.done()


def make_compound_regions(ia, coords, rg):
    def box(low, high, axes=(0, 1, 3)):
        return rg.wbox(blc=[f"{p}pix" for p in low], trc=[f"{p}pix" for p in high],
                       pixelaxes=list(axes), csys=coords)

    a = box([0, 1, 0], [3, 3, 2])
    b = box([2, 2, 1], [5, 5, 3])
    inputs = {"*1": a, "*2": b}
    union = rg.makeunion(inputs)
    ellipse = rg.fromtext("ellipse[[4pix, 3pix], [1pix, 0.7pix], 25deg]", ia.shape(), coords)
    circle = rg.fromtext("circle[[2pix, 2.5pix], 1pix]", ia.shape(), coords)
    polygon = rg.fromtext(POLY, ia.shape(), coords)
    pair = {"*1": polygon, "*2": box([1, 1, 1], [4, 3, 2])}
    for name, region in [
        ("union", union),
        ("intersection", rg.intersection(inputs)),
        ("difference", rg.difference(a, b)),
        ("complement", rg.complement(a)),
        ("nested", rg.difference(union, box([0, 1, 0], [1, 3, 3]))),
        ("nonoverlap", rg.intersection({"*1": a, "*2": box([4, 0, 0], [5, 1, 0])})),
        ("mixed", rg.makeunion({"*1": a, "*2": ellipse})),
        ("masked_edge", rg.difference(circle, box([2, 2], [2, 2], axes=(0, 1)))),
        ("polygon_transposed", rg.intersection(pair)),
    ]:
        assert rg.tofile(f"saved_compound_{name}.rgn", region)


def make_extended_regions(ia, coords, rg):
    def box(axes, low, high):
        return rg.wbox(blc=[f"{p}pix" for p in low], trc=[f"{p}pix" for p in high],
                       pixelaxes=axes, csys=coords)

    qualified = rg.fromtext(POLY + ", range=[1pix,2pix], corr=[Q]", ia.shape(), coords)
    extension = box([3, 2], [0, 1], [2, 2])

    def stretched(last):
        original = box([0, 1, 3], [1, 1, 1], [4, 3, last])
        return {"name": "WCExtension", "isRegion": 2, "comment": "",
                "regions": {"*1": original, "*2": extension, "nr": 2}}

    first = box([0, 1], [0, 1], [2, 4])
    second = box([0, 1], [3, 2], [4, 4])
    members = {"*1": first, "*2": second}

    def concatenated(end):
        region = rg.concatenation(box=box([3], [1], [end]), regions=members)
        # RegionManager repeats its first argument when building the record.
        # Store distinct members to exercise the actual casacore reader.
        region["regions"]["*2"] = second
        return region

    polygons = {"*1": rg.fromtext(POLY, ia.shape(), coords),
                "*2": rg.fromtext("poly[[0pix,2pix],[2pix,2pix],[2pix,4pix],[0pix,4pix]]",
                                  ia.shape(), coords)}
    polygon_stack = rg.concatenation(box=box([3], [1], [2]), regions=polygons)
    polygon_stack["regions"]["*2"] = polygons["*2"]
    wrong_frame = copy.deepcopy(concatenated(2))
    spectral = wrong_frame["box"]["coordinates"]["spectral2"]
    spectral["system"] = "BARY"
    spectral["conversion"]["system"] = "BARY"

    sky = casatools.image()
    assert sky.open("relative_sky.im")
    sky_cs = sky.coordsys()
    sky_coords = sky_cs.torecord()
    original = rg.wbox(blc=["2pix"],trc=["2pix"],pixelaxes=[1],csys=sky_coords)
    target = rg.wbox(blc=["-1pix","0pix"],trc=["1pix","1pix"],pixelaxes=[0,1],
                    csys=sky_coords,absrel="relref")
    stretch = {"name":"WCExtension","isRegion":2,"comment":"",
               "regions":{"*1":original,"*2":target,"nr":2}}
    sibling = rg.wbox(blc=["4pix","3pix"],trc=["5pix","4pix"],pixelaxes=[0,1],csys=sky_coords)
    nested = rg.makeunion({"*1":stretch,"*2":sibling})
    sky_cs.done()
    sky.done()

    for name, region in [
        ("extension", qualified),
        ("stretch", stretched(1)),
        ("stretch_invalid", stretched(2)),
        ("concat", concatenated(2)),
        ("concat_invalid", concatenated(3)),
        ("concat_polygons", polygon_stack),
        ("concat_frame_invalid", wrong_frame),
        ("nested_relative", nested),
    ]:
        assert rg.tofile(f"saved_axis_{name}.rgn", region)


def make_relative_shapes(coords, rg):
    polygon = rg.wpolygon(
        x=["0arcmin", "-2.1arcmin", "-1.2arcmin"],
        y=["0arcmin", "0arcmin", "2arcmin"],
        pixelaxes=[0, 1], csys=coords, absrel="relref",
    )
    assert rg.tofile("saved_relative_polygon_ref.rgn", polygon)
    extent = rg.wbox(blc=["1pix", "2pix"], trc=["2pix", "2pix"], pixelaxes=[3, 2], csys=coords)
    extended = {"name": "WCExtension", "isRegion": 2, "comment": "",
                "regions": {"*1": polygon, "*2": extent, "nr": 2}}
    assert rg.tofile("saved_relative_polygon_extended.rgn", extended)
    # The tool refuses to serialize a polygon with a default axis unit.
    # Replace an equal-length unit spelling to test reading that record.
    invalid = Path("saved_relative_polygon_default_invalid.rgn")
    polygon["x"]["unit"] = "arcmin1"
    assert rg.tofile(str(invalid), polygon)
    record = invalid.read_bytes()
    assert record.count(b"arcmin1") == 1
    invalid.write_bytes(record.replace(b"arcmin1", b"default"))

    sky = casatools.image()
    assert sky.open("relative_sky.im")
    cs = sky.coordsys()
    coords = cs.torecord()
    relative = rg.wpolygon(
        x=["0.9deg", "-1.4deg", "0.3deg"], y=["-0.8deg", "-0.6deg", "1.1deg"],
        pixelaxes=[0, 1], csys=coords, absrel="relcen",
    )
    assert rg.tofile("saved_relative_polygon_center.rgn", relative)
    ellipse = rg.fromtext("circle[[2pix,2pix],1.7pix]", sky.shape(), coords)
    ellipse["absrel"] = 3
    for field, value in zip(ellipse["center"], (0.4, -0.2)):
        ellipse["center"][field].update(unit="deg", value=value)
    assert rg.tofile("saved_relative_ellipse_center.rgn", ellipse)
    ellipse["absrel"] = 2
    ellipse["pixelAxes"] = ellipse["pixelAxes"][::-1].copy()
    assert rg.tofile("saved_relative_ellipse_reversed.rgn", ellipse)
    ellipse["pixelAxes"] = ellipse["pixelAxes"][::-1].copy()
    for field in ellipse["center"].values():
        field["unit"] = "pix"
    assert rg.tofile("saved_relative_ellipse_far_side_invalid.rgn", ellipse)
    cs.done()
    sky.done()


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
    capture("saved_relative_ref", region="relative_ref.rgn")
    capture("saved_relative_center", region="relative_center.rgn")
    capture("saved_relative_reordered", "transposed.im", region="relative_reordered.rgn")
    capture("saved_relative_sky", "relative_sky.im", region="relative_sky.rgn")
    capture("saved_relative_disjoint", region="relative_disjoint.rgn", error=True)
    capture("saved_relative_pixels_world", "relative_sky.im", region="relative_pixels_world.rgn")
    capture("saved_relative_wide", "relative_wide.im", region="relative_wide.rgn", error=True)
    for label in ("union", "intersection", "difference", "complement", "nested", "nonoverlap", "mixed"):
        name = "saved_compound_" + label
        capture(name, region=name + ".rgn", error=label == "nonoverlap")
    capture("saved_compound_masked_edge", region="saved_compound_masked_edge.rgn")
    capture("saved_compound_polygon_transposed", "transposed.im", region="saved_compound_polygon_transposed.rgn")
    capture("saved_stride", region="stride.rgn")
    capture("saved_stride_mask", region="stride.rgn", mask='"cube.im" > 195')
    capture("saved_stride_stretch", region="stride.rgn", mask='"stretch.im" > 200', stretch=True)
    capture("saved_stride_drop", region="stride_degenerate.rgn", dropdeg=True, keepaxes=[3])
    capture("saved_stride_tabular", "axis_maps/tabular_spectral_first.image", region="stride_tabular.rgn")
    for label in ("extension", "stretch", "stretch_invalid", "concat", "concat_invalid",
                  "concat_polygons", "concat_frame_invalid"):
        name = "saved_axis_" + label
        capture(name, region=name + ".rgn", error=label.endswith("_invalid"))
    capture("saved_axis_nested_relative", "relative_sky.im", region="saved_axis_nested_relative.rgn")
    for label in ("polygon_ref", "polygon_extended", "polygon_center", "ellipse_center"):
        name = "saved_relative_" + label
        image = "relative_sky.im" if label.endswith("_center") else "cube.im"
        capture(name, image, region=name + ".rgn")
    capture("saved_relative_polygon_default_invalid", region="saved_relative_polygon_default_invalid.rgn", error=True)
    for label in ("ellipse_reversed", "ellipse_far_side_invalid"):
        name = "saved_relative_" + label
        capture(name, "relative_sky.im", region=name + ".rgn", error=label.endswith("_invalid"))
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
