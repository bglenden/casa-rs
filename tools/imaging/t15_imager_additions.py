#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""#841 Phase 6b T1.5: casars-imager against CASA 6.7.6's tclean on
casatestdata's refim_point.ms, for non-square images, uvtaper and
startmodel, and on the 14-pointing 7 m simobserve MS of
regression/sim_multi_arrays_and_TP (m51c.aca.i.ms, the mosaic rows).

Copy this script into a working directory with `data/refim_point.ms`
(casatestdata `unittest/tclean`) and `data/m51c.aca.i.ms` (the 7 m output of
`regression/sim_multi_arrays_and_TP`'s first `simobserve` runs), then run it
with CASA 6.7.6's Python from there:

    python3 -I t15_imager_additions.py start-models   # CASA's start models, inputs/
    python3 -I t15_imager_additions.py casa [case ...]  # CASA's references, casa/<case>/
    T15_TAG=-<commit> python3 -I t15_imager_additions.py rust <casars-imager> [case ...]

`rust` images each case through `casars-imager --json-run` into
rust<TAG>/<case>/, then compares every product both wrote: NRMS is
sqrt(sum((r - c)^2) / sum(c^2)) over pixels valid in both, the restoring
beams to their digits, and sumwt relatively. It writes
results<TAG>/<case>.json and results<TAG>/table.md.

Start models are CASA's own: `start-models` makes `deep.model` (a deep
clean), `fine.model` (it on 4" cells, 200 pixels, Jy/pixel),
`masked.model` (its left half masked), `deep_cube.model` (a deep cube
clean) and `deep_mosaic.model` (a deep mosaic clean, in CASA's flat-noise
units) under inputs/. A case's `vis` names its MS (refim_point.ms
otherwise).
"""

import json
import os
import shutil
import subprocess
import sys

import numpy as np
from casatools import image, quanta, regionmanager

ROOT = os.path.dirname(os.path.abspath(__file__))
TAG = os.environ.get("T15_TAG", "")
MS = os.path.join(ROOT, "data", "refim_point.ms")
MOSAIC_MS = os.path.join(ROOT, "data", "m51c.aca.i.ms")
INPUTS = os.path.join(ROOT, "inputs")
qa = quanta()

COMMON = {"imsize": 100, "cell": "8.0arcsec", "specmode": "mfs", "deconvolver": "hogbom",
          "gridder": "standard", "weighting": "natural", "niter": 0}
HOGBOM = {"niter": 100}
MOSAIC = {"vis": MOSAIC_MS, "imsize": [192, 192], "cell": "0.6arcsec", "gridder": "mosaic",
          "pblimit": 0.2}


def start(name):
    return os.path.join(INPUTS, name)


# case: tclean parameters over COMMON (casa-rs takes the same names)
CASES = {
    "nonsquare-dirty": {"imsize": [120, 80]},
    "nonsquare-hogbom": {"imsize": [120, 80], **HOGBOM},
    "nonsquare-cell-dirty": {"cell": ["8.0arcsec", "6.0arcsec"]},
    "nonsquare-cell-hogbom": {"imsize": [96, 128], "cell": ["8.0arcsec", "6.0arcsec"], **HOGBOM},
    "uvtaper-arcsec-dirty": {"uvtaper": ["60arcsec"]},
    "uvtaper-arcsec-hogbom": {"uvtaper": ["60arcsec"], **HOGBOM},
    "uvtaper-klambda-dirty": {"uvtaper": ["0.5klambda"]},
    "uvtaper-ellipse-dirty": {"uvtaper": ["80arcsec", "40arcsec", "30deg"]},
    "uvtaper-ellipse-lambda-dirty": {"uvtaper": ["800lambda", "400lambda", "30deg"]},
    "uvtaper-briggs-dirty": {"uvtaper": ["60arcsec"], "weighting": "briggs", "robust": 0.5},
    "uvtaper-uniform-hogbom": {"uvtaper": ["60arcsec"], "weighting": "uniform", **HOGBOM},
    "uvtaper-cube-dirty": {"uvtaper": ["60arcsec"], "specmode": "cube"},
    "start-dirty": {"startmodel": start("deep.model")},
    "start-hogbom": {"startmodel": start("deep.model"), **HOGBOM},
    "start-regridded-dirty": {"startmodel": start("fine.model")},
    "start-masked-dirty": {"startmodel": start("masked.model")},
    "start-nonsquare-hogbom": {"startmodel": start("deep.model"), "imsize": [120, 80], **HOGBOM},
    "start-cube-dirty": {"specmode": "cube", "startmodel": start("deep_cube.model")},
    "start-cube-hogbom": {"specmode": "cube", "startmodel": start("deep_cube.model"), **HOGBOM},
    "mosaic-dirty": {**MOSAIC},
    "start-mosaic-dirty": {**MOSAIC, "startmodel": start("deep_mosaic.model")},
    "start-mosaic-hogbom": {**MOSAIC, "startmodel": start("deep_mosaic.model"), **HOGBOM},
}

# casa-rs's names where they differ from tclean's.
RUST_NAMES = {}
RUST_FIXED = {"backend": "cpu", "hogbom_iteration_mode": "casa-inclusive"}


def params(case):
    """The case's tclean parameters and its MS."""
    values = {**COMMON, **CASES[case]}
    return values, values.pop("vis", MS)


def casa(cases):
    from casatasks import tclean
    for case in cases:
        out = os.path.join(ROOT, "casa", case)
        shutil.rmtree(out, ignore_errors=True)
        os.makedirs(out)
        values, vis = params(case)
        tclean(vis=vis, imagename=os.path.join(out, "image"), threshold="0Jy",
               savemodel="none", interactive=False, **values)
        print("CASA", case)


def start_models():
    from casatasks import imregrid, tclean
    os.makedirs(INPUTS, exist_ok=True)
    deep = os.path.join(INPUTS, "deep")
    for ext in (".image", ".model", ".residual", ".psf", ".pb", ".sumwt"):
        shutil.rmtree(deep + ext, ignore_errors=True)
    tclean(vis=MS, imagename=deep, threshold="0Jy", savemodel="none", **{**COMMON, "niter": 500})
    ia = image()
    ia.open(deep + ".model")
    cs = ia.coordsys()
    ia.close()
    cs.setincrement([qa.convert(qa.quantity("-4arcsec"), "rad")["value"],
                     qa.convert(qa.quantity("4arcsec"), "rad")["value"]], "direction")
    cs.setreferencepixel([100.0, 100.0], "direction")
    template = imregrid(imagename=deep + ".model", template="get")
    template["csys"] = cs.torecord()
    template["shap"] = np.array([200, 200, 1, 1])
    shutil.rmtree(start("fine.model"), ignore_errors=True)
    imregrid(imagename=deep + ".model", template=template, output=start("fine.model"),
             interpolation="linear")
    shutil.rmtree(start("masked.model"), ignore_errors=True)
    shutil.copytree(deep + ".model", start("masked.model"))
    ia.open(start("masked.model"))
    ia.calcmask(f"'{start('masked.model')}' == '{start('masked.model')}'", name="right")
    ia.set(pixelmask=False, region=regionmanager().box(blc=[0, 0, 0, 0], trc=[49, 99, 0, 0]))
    ia.close()
    for name, vis, values in (
        ("deep_cube", MS, {**COMMON, "specmode": "cube", "niter": 500}),
        ("deep_mosaic", MOSAIC_MS, {**COMMON, **{k: v for k, v in MOSAIC.items() if k != "vis"},
                                    "niter": 1000}),
    ):
        stem = os.path.join(INPUTS, name)
        for ext in (".image", ".model", ".residual", ".psf", ".pb", ".sumwt", ".weight"):
            shutil.rmtree(stem + ext, ignore_errors=True)
        tclean(vis=vis, imagename=stem, threshold="0Jy", savemodel="none", **values)
    print("start models in", INPUTS)


def read(path):
    ia = image()
    ia.open(path)
    pixels = ia.getchunk().astype(np.float64)
    mask = ia.getchunk(getmask=True)
    beam = ia.restoringbeam()
    ia.close()
    if beam and "major" in beam:
        beam = [round(qa.convert(beam[k], u)["value"], 6)
                for k, u in (("major", "arcsec"), ("minor", "arcsec"), ("positionangle", "deg"))]
    else:
        beam = None
    return pixels, mask, beam


def compare(case):
    rows = {}
    casa_dir, rust_dir = os.path.join(ROOT, "casa", case), os.path.join(ROOT, f"rust{TAG}", case)
    for name in sorted(os.listdir(rust_dir)):
        if not name.startswith("image.") or not os.path.exists(os.path.join(casa_dir, name)):
            continue
        r, rm, rb = read(os.path.join(rust_dir, name))
        c, cm, cb = read(os.path.join(casa_dir, name))
        if r.shape != c.shape:
            rows[name] = {"shape": [list(r.shape), list(c.shape)]}
            continue
        valid = rm & cm & np.isfinite(r) & np.isfinite(c)
        denominator = np.sum(c[valid] ** 2)
        nrms = float(np.sqrt(np.sum((r[valid] - c[valid]) ** 2) / denominator)) if denominator > 0 \
            else float(np.abs(r[valid]).max(initial=0.0))
        rows[name] = {"nrms": nrms, "mask_mismatch": int(np.sum(rm != cm)),
                      "beam": [rb, cb] if (rb or cb) else None}
        if name == "image.sumwt":
            rows[name]["sumwt"] = [float(r.ravel()[0]), float(c.ravel()[0])]
    return rows


def rust(binary, cases):
    results = os.path.join(ROOT, f"results{TAG}")
    os.makedirs(results, exist_ok=True)
    table = ["| case | worst NRMS (product) | mask mismatches | beam rs / CASA | sumwt rel |",
             "|---|---|---|---|---|"]
    for case in cases:
        out = os.path.join(ROOT, f"rust{TAG}", case)
        shutil.rmtree(out, ignore_errors=True)
        os.makedirs(out)
        values, vis = params(case)
        request = {RUST_NAMES.get(k, k): v for k, v in values.items()}
        request.update(RUST_FIXED, vis=vis, imagename=os.path.join(out, "image"))
        with open(os.path.join(out, "request.json"), "w") as handle:
            json.dump({"surface": "tclean", "values": request}, handle)
        run = subprocess.run([binary, "--json-run", os.path.join(out, "request.json")],
                             capture_output=True, text=True, cwd=out)
        with open(os.path.join(out, "run.log"), "w") as handle:
            handle.write(run.stdout + run.stderr)
        if run.returncode != 0:
            print(case, "FAILED", run.stderr[-400:])
            table.append(f"| {case} | refused: {run.stderr.strip().splitlines()[-1][:120]} | | | |")
            continue
        rows = compare(case)
        with open(os.path.join(results, f"{case}.json"), "w") as handle:
            json.dump(rows, handle, indent=1)
        numeric = {k: v for k, v in rows.items() if "nrms" in v}
        worst = max(numeric, key=lambda k: numeric[k]["nrms"])
        beam = rows.get("image.image", {}).get("beam")
        sumwt = rows.get("image.sumwt", {}).get("sumwt")
        relative = abs(sumwt[0] - sumwt[1]) / abs(sumwt[1]) if sumwt else None
        mismatches = sum(v["mask_mismatch"] for v in numeric.values())
        table.append(f"| {case} | {numeric[worst]['nrms']:.2e} ({worst[6:]}) | {mismatches} | "
                     f"{beam} | {relative:.1e} |" if relative is not None else
                     f"| {case} | {numeric[worst]['nrms']:.2e} ({worst[6:]}) | {mismatches} | {beam} | - |")
        print(table[-1])
    with open(os.path.join(results, "table.md"), "w") as handle:
        handle.write("\n".join(table) + "\n")


def main():
    mode, rest = sys.argv[1], sys.argv[2:]
    if mode == "start-models":
        start_models()
    elif mode == "casa":
        casa(rest or list(CASES))
    elif mode == "rust":
        rust(os.path.abspath(rest[0]), rest[1:] or list(CASES))
    else:
        raise SystemExit(__doc__)


if __name__ == "__main__":
    main()
