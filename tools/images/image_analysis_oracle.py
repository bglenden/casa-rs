#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA's image analysis on small images, for casa-images' port (#841 Phase 6a).

Run with CASA's Python (for example `/Applications/CASA.app/Contents/MacOS/python3 -I`):

    python3 -I tools/images/image_analysis_oracle.py \\
        --cases crates/casa-images/tests/data/image_analysis_cases.json \\
        --out crates/casa-images/tests/data/image_analysis_casa.json

For each image of the case file it builds a CASA image:
- `flat` images as `simutil.flatimage` writes them: a default image dropped
  to [RA, Dec] and given a Stokes I axis;
- `cube` images as `tclean` and `tsdimaging` write them: [RA, Dec, Stokes I,
  Frequency], from 330.076 GHz in 50 MHz channels, channel c scaled by
  1 + c/2;
- a `pc` matrix, when given, as the direction coordinate's linear transform.

It records each image's pixels and mask, as built in float32, and its world
coordinates at a few pixels. The Rust test builds the same images from them,
and checks the coordinates first.

It then runs each case and records the output's pixels, mask, unit, beam,
shape and direction coordinate:
- `imregrid`: the task with a template record that `imregrid(template='get')`
  returns, changed as `simanalyze` changes it (the reference pixel, the
  direction increments scaled and the shape);
- `ia_regrid`: `ia.regrid` onto another image's coordinate system and shape,
  as `simutil.convimage` calls it.

Both regrids take the case's `decimate`, CASA's default 10 when absent. Most
cases use 0, the exact position of every output pixel, which is what casa-rs
computes; casacore otherwise interpolates the positions between a coarser
grid of converted ones. A template may also move a cube's one channel
(`spectral_shift_hz`) or relabel its one Stokes plane (`stokes`), to record
what CASA keeps for axes it does not regrid.
- `convolve2d`: `ia.convolve2d` with a Gaussian;
- `statistics`: `ia.statistics(robust=True)`;
- `impbcor`, `immath` (`evalexpr`) and `feather`, the tasks at the
  arguments given.

Pixels and masks are flattened in Fortran order, the first axis fastest.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import shutil
import tempfile

import numpy as np
from casatasks import feather, immath, impbcor, imregrid
from casatools import ctsys, image, quanta

qa = quanta()
ARCSEC = math.pi / (180.0 * 3600.0)


def pixels_of(spec):
    """The image's pixels: its sources summed on its shape, in float64; a
    cube's channel c is scaled by 1 + c/2."""
    shape = spec["shape"]
    nx, ny = shape[0], shape[1]
    x = np.arange(nx, dtype=np.float64)[:, None]
    y = np.arange(ny, dtype=np.float64)[None, :]
    plane = np.zeros((nx, ny), dtype=np.float64)
    for source in spec["sources"]:
        if source["sigma"] == 0.0:
            plane[int(source["x"]), int(source["y"])] += source["peak"]
        else:
            r2 = (x - source["x"]) ** 2 + (y - source["y"]) ** 2
            plane += source["peak"] * np.exp(-r2 / (2.0 * source["sigma"] ** 2))
    pixels = np.zeros(shape, dtype=np.float64)
    for channel in range(shape[3] if len(shape) == 4 else 1):
        if len(shape) == 4:
            pixels[:, :, 0, channel] = plane * (1.0 + 0.5 * channel)
        else:
            pixels[:, :, 0] = plane
    return pixels


def mask_of(spec):
    """The image's mask (True where good), or None."""
    boxes = spec.get("mask_boxes")
    if not boxes:
        return None
    mask = np.ones(spec["shape"], dtype=bool)
    for x0, y0, x1, y1 in boxes:
        mask[x0:x1 + 1, y0:y1 + 1, ...] = False
    return mask


def build(spec, path):
    """Build the image `spec` describes at `path` and return its record."""
    ia = image()
    nx, ny = spec["shape"][0], spec["shape"][1]
    nchan = spec["shape"][3] if spec["axes"] == "cube" else 1
    ia.fromshape(path + ".tmp4", shape=[nx, ny, 1, nchan], overwrite=True)
    if spec["axes"] == "flat":
        flat = ia.subimage(path + ".tmp2", dropdeg=True, overwrite=True)
        ia.done()
        flat.done()
        ia.open(path + ".tmp2")
        stokes = ia.adddegaxes(path, stokes="I", overwrite=True)
        stokes.done()
        ia.done()
        shutil.rmtree(path + ".tmp2")
    else:
        ia.done()
        shutil.copytree(path + ".tmp4", path)
    shutil.rmtree(path + ".tmp4")
    ia.open(path)
    cs = ia.coordsys()
    cs.setreferencecode(spec["frame"], "direction", True)
    cs.setprojection("SIN")
    cs.setunits(["rad", "rad"], type="direction")
    cs.setreferencevalue(value=list(spec["refval_rad"]), type="direction")
    cs.setreferencepixel(value=list(spec["refpix"]), type="direction")
    cs.setincrement(value=[v * ARCSEC for v in spec["inc_arcsec"]], type="direction")
    if "pc" in spec:
        cs.setlineartransform(type="direction", value=np.array(spec["pc"], dtype=np.float64))
    if spec["axes"] == "cube":
        cs.setreferencecode("LSRK", "spectral", True)
        cs.setreferencevalue(value="330.076GHz", type="spectral")
        cs.setreferencepixel(value=[0.0], type="spectral")
        cs.setincrement(value="50MHz", type="spectral")
        cs.setrestfrequency(value="330.076GHz")
    ia.setcoordsys(cs.torecord())
    pixels = pixels_of(spec).astype(np.float32)
    mask = mask_of(spec)
    if mask is None:
        ia.putchunk(pixels)
    else:
        ia.putregion(pixels=pixels, pixelmask=mask)
    ia.setbrightnessunit(spec["unit"])
    beam = spec.get("beam")
    if beam:
        ia.setrestoringbeam(major=f"{beam['major_arcsec']}arcsec",
                            minor=f"{beam['minor_arcsec']}arcsec",
                            pa=f"{beam['pa_deg']}deg")
    record = {
        "pixels": flat_list(ia.getchunk()),
        "mask": mask_list(ia),
        "world": [],
    }
    for pixel in ([0.0, 0.0], [nx - 1.0, ny - 1.0], [2.5, ny - 3.25]):
        full = pixel + [0.0] * (len(spec["shape"]) - 2)
        world = cs.toworld(full, "n")["numeric"]
        record["world"].append({"pixel": full, "world": [float(v) for v in world]})
    cs.done()
    ia.done()
    return record


def flat_list(array):
    # Nine significant digits round-trip a float32 exactly.
    return [float(f"{np.float32(v):.9g}") for v in np.asarray(array).flatten(order="F")]


def mask_list(ia):
    mask = np.asarray(ia.getchunk(getmask=True))
    if mask.all():
        return None
    return [bool(v) for v in mask.flatten(order="F")]


def describe(path):
    """An output image's pixels, mask, unit, beam, shape and direction axes."""
    ia = image()
    ia.open(path)
    cs = ia.coordsys()
    beam = ia.restoringbeam()
    out = {
        "shape": [int(v) for v in ia.shape()],
        "pixels": flat_list(ia.getchunk()),
        "mask": mask_list(ia),
        "unit": ia.brightnessunit(),
        "beam": None,
        "refpix": [float(v) for v in cs.referencepixel(type="direction")["numeric"]],
        "refval_rad": [float(v) for v in cs.referencevalue(type="direction", format="n")["numeric"]],
        "inc_rad": [float(v) for v in cs.increment(type="direction", format="n")["numeric"]],
        "stokes": list(cs.stokes()),
        "spectral_refval_hz": None,
    }
    if cs.findcoordinate("spectral")["return"]:
        out["spectral_refval_hz"] = float(
            qa.convert(cs.referencevalue(type="spectral", format="q")["quantity"]["*1"], "Hz")["value"])
    if beam and "major" in beam:
        out["beam"] = {
            "major_arcsec": qa.convert(beam["major"], "arcsec")["value"],
            "minor_arcsec": qa.convert(beam["minor"], "arcsec")["value"],
            "pa_deg": qa.convert(beam["positionangle"], "deg")["value"],
        }
    cs.done()
    ia.done()
    return out


def template_record(case, paths):
    """The template record `simanalyze` builds from `imregrid(template='get')`:
    another image's grid with its reference pixel, its direction increments
    scaled and its shape changed, and, to probe the other axes, its one
    channel moved or its one Stokes plane relabelled."""
    spec = case["template"]
    template = imregrid(imagename=paths[spec["from"]], template="get")
    if "refpix" in spec:
        ia = image()
        ia.open(paths[spec["from"]])
        cs = ia.coordsys()
        ia.done()
        cs.fromrecord(template["csys"])
        cs.setreferencepixel(value=list(spec["refpix"]), type="direction")
        inc = cs.increment(type="direction", format="n")["numeric"]
        cs.setincrement(value=[v * spec["inc_scale"] for v in inc], type="direction")
        template["csys"] = cs.torecord()
        cs.done()
        template["shap"] = list(spec["shape"])
    if "spectral_shift_hz" in spec:
        wcs = template["csys"]["spectral2"]["wcs"]
        wcs["crval"] = wcs["crval"] + spec["spectral_shift_hz"]
    if "stokes" in spec:
        template["csys"]["stokes1"]["stokes"] = np.array(spec["stokes"])
    return template


def run(case, paths, work):
    out = os.path.join(work, case["name"])
    op = case["op"]
    if op == "imregrid":
        imregrid(imagename=paths[case["input"]], template=template_record(case, paths),
                 output=out, interpolation=case["interpolation"],
                 decimate=case.get("decimate", 10), overwrite=True)
        return describe(out)
    if op == "ia_regrid":
        template = template_record(case, paths)
        ia = image()
        ia.open(paths[case["input"]])
        regridded = ia.regrid(outfile=out, csys=template["csys"], shape=list(template["shap"]),
                              method=case["method"], decimate=case.get("decimate", 10),
                              asvelocity=False, overwrite=True)
        regridded.done()
        ia.done()
        return describe(out)
    if op == "convolve2d":
        beam = case["beam"]
        ia = image()
        ia.open(paths[case["input"]])
        convolved = ia.convolve2d(outfile=out, major=f"{beam['major_arcsec']}arcsec",
                                  minor=f"{beam['minor_arcsec']}arcsec",
                                  pa=f"{beam['pa_deg']}deg", overwrite=True)
        convolved.done()
        ia.done()
        return describe(out)
    if op == "statistics":
        ia = image()
        ia.open(paths[case["input"]])
        stats = ia.statistics(robust=True, verbose=False, list=False)
        ia.done()
        keys = ["npts", "sum", "mean", "median", "medabsdevmed", "quartile", "q1", "q3",
                "min", "max", "rms", "sigma"]
        return {key: float(np.asarray(stats[key]).flatten()[0]) for key in keys}
    if op == "impbcor":
        impbcor(imagename=paths[case["input"]], pbimage=paths[case["pb"]], outfile=out,
                mode=case["mode"], cutoff=case["cutoff"], overwrite=True)
        return describe(out)
    if op == "immath":
        immath(imagename=[paths[case["input"]]], mode="evalexpr", expr=case["expr"], outfile=out)
        return describe(out)
    if op == "feather":
        feather(imagename=out, highres=paths[case["highres"]], lowres=paths[case["lowres"]])
        return describe(out)
    raise ValueError(f"unknown op {op}")


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--cases", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    with open(args.cases, encoding="utf-8") as handle:
        cases = json.load(handle)
    result = {
        "casa_version": ctsys.version_string(),
        "images": {},
        "cases": {},
    }
    with tempfile.TemporaryDirectory(prefix="image-analysis-oracle-") as work:
        paths = {}
        for name, spec in cases["images"].items():
            paths[name] = os.path.join(work, name + ".image")
            result["images"][name] = build(spec, paths[name])
        for case in cases["cases"]:
            result["cases"][case["name"]] = run(case, paths, work)
    # One line for each list of numbers or booleans, so the file stays small.
    text = re.sub(r"\[[^\[\]{}]*\]", lambda m: re.sub(r"\s+", "", m.group(0)),
                  json.dumps(result, indent=1))
    with open(args.out, "w", encoding="utf-8") as handle:
        handle.write(text + "\n")


if __name__ == "__main__":
    main()
