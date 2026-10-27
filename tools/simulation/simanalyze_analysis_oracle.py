#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA's simanalyze analysis on small images, for casa-rs's port (#841 Phase 6f).

Run with CASA's Python (for example `/Applications/CASA.app/Contents/MacOS/python3 -I`):

    python3 -I tools/simulation/simanalyze_analysis_oracle.py \\
        --out crates/casa-simulation/tests/data

It writes, into `--out`:
- `simanalyze_analysis_inputs.tar.gz`: the images it builds (with
  `tools/images/image_analysis_oracle.py`'s builder, then deterministic
  noise), which casa-rs's tests read as CASA wrote them, and each case's
  convolved model as CASA made it (`<case>.casa.conv`);
- `simanalyze_analysis_casa.json.gz`: what CASA makes of them:
  - `simutil.convimage(model, image)` on a copy of the model: the model's
    restoring beam afterwards, `.regrid` and `.regrid.conv`, or the
    exception;
  - after it, the lines of `task_simanalyze.py` that make `.diff` and
    `.fidelity` and the scalar fidelity, with the floor as they write it
    (`%f`);
  - `simutil.statim(image, plot=False)`, `simutil.cellsize(image)` and
    `simutil.flatimage(image)`.

Images are recorded by `image_analysis_oracle.describe`: shape, pixels and
mask in Fortran order, unit, beam and direction axes.

`--evidence PROJECT` records, into `--out`, the evidence cases instead: the
flat images of a `simanalyze` project made by CASA (for example the rerun of
`regression/sim_multi_arrays_and_TP`), copied to `--out/inputs`, with the
same steps run on them. casa-rs's tests compare them with
`CASA_RS_SIMANALYZE_FIXTURE=<out>`:

    python3 -I tools/simulation/simanalyze_analysis_oracle.py --out EV --evidence work/m51c
    CASA_RS_SIMANALYZE_FIXTURE=EV cargo nextest run -p casa-simulation --test simanalyze_analysis_casa
"""

from __future__ import annotations

import argparse
import glob
import gzip
import json
import os
import shutil
import sys
import tarfile
import tempfile

import numpy as np

from casatasks.private.simutil import simutil
from casatools import ctsys, image

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "images"))
import image_analysis_oracle as images  # noqa: E402  (CASA's Python, beside it)

BEAM = {"major_arcsec": 1.1, "minor_arcsec": 0.8, "pa_deg": 30.0}
REFVAL = [0.6, -0.4]
MODEL_SOURCES = [
    {"x": 30.0, "y": 34.0, "sigma": 4.0, "peak": 1.0},
    {"x": 40.5, "y": 22.0, "sigma": 2.5, "peak": 0.6},
    {"x": 20.0, "y": 45.0, "sigma": 0.0, "peak": 8.0},
    {"x": 48.0, "y": 50.0, "sigma": 6.0, "peak": 0.3},
]
IMAGE_SOURCES = [
    {"x": 15.0, "y": 18.0, "sigma": 3.0, "peak": 90.0},
    {"x": 21.0, "y": 11.0, "sigma": 2.0, "peak": 35.0},
    {"x": 10.0, "y": 25.0, "sigma": 1.5, "peak": 60.0},
]


def flat(shape, inc, refpix, unit, sources, **extra):
    spec = {"axes": "flat", "shape": shape, "frame": "J2000", "refval_rad": REFVAL,
            "refpix": refpix, "inc_arcsec": inc, "unit": unit, "sources": sources}
    spec.update(extra)
    return spec


def scaled(sources, factor):
    return [dict(source, peak=source["peak"] * factor) for source in sources]


# The images, with the noise rms added to each (seeded by its name's order).
INPUTS = {
    # simutil.flatimage's sky model: 0.1 arcsec pixels in Jy/pixel.
    "model.flat": (flat([64, 64, 1], [-0.1, 0.1], [32.0, 32.0], "Jy/pixel", MODEL_SOURCES), 0.0),
    # simanalyze's image flat on a grid aligned with the model (its 0.2 arcsec
    # pixels fall on every other model pixel), masked where a primary beam
    # would be low, with odd sides for statim's corner.
    "image.flat": (flat([33, 35, 1], [-0.2, 0.2], [16.0, 17.0], "Jy/beam", IMAGE_SOURCES,
                        beam=BEAM, mask_boxes=[[0, 30, 6, 34], [29, 0, 32, 3]]), 2.0),
    # The same off the model's pixels.
    "offset.flat": (flat([33, 35, 1], [-0.2, 0.2], [16.3, 17.6], "Jy/beam", IMAGE_SOURCES,
                         beam=BEAM, mask_boxes=[[25, 28, 32, 34]]), 2.0),
    # A model covering only part of the image.
    "small_model.flat": (flat([20, 18, 1], [-0.1, 0.1], [10.0, 9.0], "Jy/pixel",
                              [{"x": 9.0, "y": 8.0, "sigma": 3.0, "peak": 2.0}]), 0.0),
    # A faint pair, whose difference's MAD/sqrt(2) CASA writes as 0.000000.
    "faint_model.flat": (flat([64, 64, 1], [-0.1, 0.1], [32.0, 32.0], "Jy/pixel",
                              scaled(MODEL_SOURCES, 1e-6)), 0.0),
    "faint.flat": (flat([33, 35, 1], [-0.2, 0.2], [16.3, 17.6], "Jy/beam", scaled(IMAGE_SOURCES, 1e-6),
                        beam=BEAM), 3e-7),
    # Linear transforms: rotated past convimage's limit, and within it.
    "rotated.flat": (flat([33, 35, 1], [-0.2, 0.2], [16.0, 17.0], "Jy/beam", IMAGE_SOURCES, beam=BEAM,
                          pc=[[0.955336489, 0.295520207], [-0.295520207, 0.955336489]]), 0.0),
    "tilted.flat": (flat([33, 35, 1], [-0.2, 0.2], [16.0, 17.0], "Jy/beam", IMAGE_SOURCES, beam=BEAM,
                         pc=[[0.99999999875, 5e-5], [-5e-5, 0.99999999875]]), 0.0),
    "beamless.flat": (flat([33, 35, 1], [-0.2, 0.2], [16.0, 17.0], "Jy/beam", IMAGE_SOURCES), 0.0),
    # statim: a corner at nx/4 = 12.5 and ny/4 = 13.5, partly masked; a
    # corner wholly masked; and a unit statim does not know.
    "stats.flat": (flat([50, 54, 1], [-0.2, 0.2], [25.0, 27.0], "Jy/pixel", IMAGE_SOURCES,
                        mask_boxes=[[0, 0, 4, 3], [40, 40, 49, 53]]), 1.0),
    "masked_corner.flat": (flat([24, 20, 1], [-0.2, 0.2], [12.0, 10.0], "Jy/beam", IMAGE_SOURCES,
                                beam=BEAM, mask_boxes=[[0, 0, 8, 7]]), 1.0),
    "kelvin.flat": (flat([24, 20, 1], [-0.2, 0.2], [12.0, 10.0], "K", IMAGE_SOURCES), 1.0),
    # flatimage of a clean cube: the mean of three channels, masked.
    "cube.image": ({"axes": "cube", "shape": [24, 26, 1, 3], "frame": "J2000", "refval_rad": REFVAL,
                    "refpix": [12.0, 13.0], "inc_arcsec": [-0.2, 0.2], "unit": "Jy/beam",
                    "sources": IMAGE_SOURCES, "beam": BEAM, "mask_boxes": [[0, 0, 3, 25]]}, 1.0),
}

# (case, model, image): convimage of the model onto the image, then the
# difference and fidelity. No path in an expression may contain "pa":
# ImageExprCalculator takes any such expression for one of the pa()
# function and labels its output deg, with a position-angle Stokes axis.
CONVIMAGE = [
    ("aligned", "model.flat", "image.flat"),
    ("offset", "model.flat", "offset.flat"),
    ("cropped", "small_model.flat", "offset.flat"),
    ("faint", "faint_model.flat", "faint.flat"),
    ("tilted", "model.flat", "tilted.flat"),
    ("rotated", "model.flat", "rotated.flat"),
    ("beamless", "model.flat", "beamless.flat"),
]
STATIM = ["image.flat", "offset.flat", "stats.flat", "masked_corner.flat", "kelvin.flat", "model.flat"]
CELLSIZE = ["image.flat", "model.flat", "tilted.flat", "rotated.flat"]
FLATIMAGE = ["cube.image"]


def failure(error):
    return {"error": f"{type(error).__name__}: {error}"}


def make_inputs(directory):
    os.makedirs(directory)
    rng = np.random.default_rng(20261027)
    ia = image()
    for name, (spec, noise) in INPUTS.items():
        path = os.path.join(directory, name)
        images.build(spec, path)
        if noise > 0.0:
            ia.open(path)
            pixels = ia.getchunk()
            ia.putchunk((pixels + rng.normal(0.0, noise, pixels.shape)).astype(np.float32))
            ia.done()


def fidelity(imagename, convolved, outflat):
    """The analysis lines of task_simanalyze.py (CASA 6.7.6), as written."""
    ia = image()
    difference = imagename + ".diff"
    diff_ia = ia.imagecalc(difference, "'%s' - '%s'" % (convolved, outflat), overwrite=True)
    diff_ia.setbrightnessunit("Jy/beam")
    diffstats = diff_ia.statistics(robust=True, verbose=False, list=False)
    diff_ia.close()
    maxdiff = diffstats["medabsdevmed"]
    if maxdiff != maxdiff:
        maxdiff = 0.
    if type(maxdiff) != type(0.):
        if maxdiff.__len__() > 0:
            maxdiff = maxdiff[0]
        else:
            maxdiff = 0.
    floor = "%f" % (maxdiff / np.sqrt(2.0))
    absdiff = imagename + ".absdiff"
    calc_ia = ia.imagecalc(absdiff, "max(abs('%s'), %s)" % (difference, floor), overwrite=True)
    calc_ia.close()
    fidelityim = imagename + ".fidelity"
    calc_ia = ia.imagecalc(fidelityim, "abs('%s') / '%s'" % (convolved, absdiff), overwrite=True)
    calc_ia.close()
    absconv = imagename + ".absconv"
    calc_ia = ia.imagecalc(absconv, "abs('%s')" % convolved, overwrite=True)
    calc_ia.close()
    ia.open(absconv)
    modelstats = ia.statistics(robust=True, verbose=False, list=False)
    ia.close()
    maxmodel = modelstats["max"]
    if maxmodel != maxmodel:
        maxmodel = 0.
    if type(maxmodel) != type(0.):
        if maxmodel.__len__() > 0:
            maxmodel = maxmodel[0]
        else:
            maxmodel = 0.
    with np.errstate(divide="ignore", invalid="ignore"):
        scalar = maxmodel / maxdiff
    return {
        "diff": images.describe(difference),
        "fidelity": images.describe(fidelityim),
        "medabsdevmed": float(maxdiff),
        "floor_written": floor,
        "max_model": float(maxmodel),
        "scalar": float(scalar) if np.isfinite(scalar) else str(scalar),
    }


def run_convimage(inputs, work, case, model, outflat):
    """convimage and the fidelity in `work/case`, with the paths relative to
    `work` as simanalyze's are to its working directory (an absolute path
    through a directory named with "pa" would relabel the outputs)."""
    os.makedirs(os.path.join(work, case))
    for name in (model, outflat):
        shutil.copytree(os.path.join(inputs, name), os.path.join(work, case, name))
    model_copy, outflat = os.path.join(case, model), os.path.join(case, outflat)
    here = os.getcwd()
    os.chdir(work)
    try:
        try:
            simutil().convimage(model_copy, outflat)
        except Exception as error:  # noqa: BLE001  (record what CASA raises)
            return {"model": model, "image": os.path.basename(outflat), **failure(error)}
        record = {
            "model": model,
            "image": os.path.basename(outflat),
            "model_after": header(model_copy),
            "regrid": images.describe(model_copy + ".regrid"),
            "convolved": images.describe(model_copy + ".regrid.conv"),
        }
        record.update(fidelity(os.path.join(case, "sim"), model_copy + ".regrid.conv", outflat))
        return record
    finally:
        os.chdir(here)


def run_statim(inputs, name):
    try:
        low, high, rms, unit = simutil().statim(os.path.join(inputs, name), plot=False)
    except Exception as error:  # noqa: BLE001
        return failure(error)
    return {"min": float(low), "max": float(high), "rms": float(rms), "unit": unit}


def run_cellsize(inputs, name):
    try:
        cell = simutil().cellsize(os.path.join(inputs, name))
    except Exception as error:  # noqa: BLE001
        return failure(error)
    return {"cell": [cell[0]["value"], cell[1]["value"]], "units": [cell[0]["unit"], cell[1]["unit"]]}


def run_flatimage(inputs, work, name):
    copy = os.path.join(work, "flat", name)
    shutil.copytree(os.path.join(inputs, name), copy)
    simutil().flatimage(copy)
    return {"flat": images.describe(copy + ".flat")}


def record_cases(inputs, work, convimage, statim, cellsize, flatimage):
    return {
        "casa_version": casa_version(),
        "convimage": {case: run_convimage(inputs, work, case, model, outflat)
                      for case, model, outflat in convimage},
        "statim": {name: run_statim(inputs, name) for name in statim},
        "cellsize": {name: run_cellsize(inputs, name) for name in cellsize},
        "flatimage": {name: run_flatimage(inputs, work, name) for name in flatimage},
    }


def casa_version():
    return ctsys.version_string()


def header(path):
    """An image's record without its pixels and mask."""
    return {key: value for key, value in images.describe(path).items() if key not in ("pixels", "mask")}


def write_record(out, record):
    with gzip.open(os.path.join(out, "simanalyze_analysis_casa.json.gz"), "wt") as handle:
        json.dump(record, handle, separators=(",", ":"), allow_nan=False)


def pack_inputs(out, inputs):
    """`inputs/` as `simanalyze_analysis_inputs.tar.gz`, without the tables'
    lock files (which are not part of an image)."""
    for lock in glob.glob(os.path.join(inputs, "**", "table.lock"), recursive=True):
        os.remove(lock)
    with tarfile.open(os.path.join(out, "simanalyze_analysis_inputs.tar.gz"), "w:gz") as archive:
        for name in sorted(os.listdir(inputs)):
            archive.add(os.path.join(inputs, name), arcname=f"inputs/{name}")


def keep_convolved(inputs, cases, convimage):
    """Each case's `.regrid.conv` as `inputs/<case>.casa.conv`, for the tests
    to take the difference and fidelity of CASA's own convolved model."""
    for case, model, _ in convimage:
        convolved = os.path.join(cases, case, model + ".regrid.conv")
        if os.path.exists(convolved):
            shutil.copytree(convolved, os.path.join(inputs, case + ".casa.conv"))


def oracle(out, work):
    inputs, cases = os.path.join(work, "inputs"), os.path.join(work, "cases")
    make_inputs(inputs)
    record = record_cases(inputs, cases, CONVIMAGE, STATIM, CELLSIZE, FLATIMAGE)
    keep_convolved(inputs, cases, CONVIMAGE)
    pack_inputs(out, inputs)
    write_record(out, record)


def evidence(out, work, project):
    """The flats of a simanalyze project CASA made: each configuration with a
    `.skymodel.flat` and an `.image.flat`, and its `.image` for flatimage."""
    inputs = os.path.join(out, "inputs")
    if os.path.exists(inputs):
        shutil.rmtree(inputs)
    os.makedirs(inputs)
    convimage, statim, flatimage = [], [], []
    for image_flat in sorted(glob.glob(os.path.join(project, "*.image.flat"))):
        root = image_flat[: -len(".image.flat")]
        name = os.path.basename(root)
        model_flat = root + ".skymodel.flat"
        if not os.path.exists(model_flat):
            continue
        for source in (model_flat, image_flat, root + ".image"):
            shutil.copytree(source, os.path.join(inputs, os.path.basename(source)))
        convimage.append((name, os.path.basename(model_flat), os.path.basename(image_flat)))
        statim.append(os.path.basename(image_flat))
        flatimage.append(name + ".image")
    cases = os.path.join(work, "cases")
    record = record_cases(inputs, cases, convimage, statim, [], flatimage)
    keep_convolved(inputs, cases, convimage)
    write_record(out, record)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", required=True)
    parser.add_argument("--work", default=None, help="scratch directory for CASA's outputs")
    parser.add_argument("--evidence", default=None, help="a simanalyze project: record its flats instead")
    args = parser.parse_args()
    os.makedirs(args.out, exist_ok=True)
    work = args.work or tempfile.mkdtemp(prefix="simanalyze_analysis_")
    if os.path.exists(work) and os.listdir(work):
        raise SystemExit(f"{work} is not empty")
    if args.evidence:
        evidence(os.path.abspath(args.out), work, args.evidence)
    else:
        oracle(os.path.abspath(args.out), work)
    print(f"wrote {args.out} (CASA's outputs in {work})")


if __name__ == "__main__":
    main()
