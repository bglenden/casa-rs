#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""The simulation round trip for tclean (#980): casa-rs's `simobserve`
makes a MeasurementSet, then CASA's and casa-rs's tclean image it with the
call CASA's `simanalyze` makes, and the matrix driver compares them.

Evidence tooling, not a runtime dependency. Run under CASA 6.7.6.14's
Python:

    CASA_PYTHON -I tools/casa-compat/tclean_simulation_roundtrip.py \\
        --out OUT --simobserve TARGET/release/simobserve \\
        --casatestdata /Volumes/home/casatestdata \\
        --casars-python VENV/bin/python --imager-binary TARGET/release/casars-imager

Each scenario of `SCENARIOS` runs casa-rs's `simobserve` task binary
(`crates/casars-simulation`, through `--json-run`) noise-free on a sky
model from casatestdata, with an antenna configuration from CASA's data
directory, in `OUT/sim/<scenario>`. The tclean call is CASA's own:
`simutil.imtclean` with `dryrun=True` writes the call `simanalyze` would
make to `<imagename>.tclean.last`, with simanalyze's choices. Those
choices are the gridder (standard for one source field, mosaic for
several), clark, the model's cell, `imsize` from the model's size through
`getOptimumSize`, the model's direction, and the scenario's niter,
threshold, weighting and stokes. A scenario of several configurations
images their MeasurementSets as one list, as tclean takes it (simanalyze
concatenates them first, which casa-rs does not port). The cases then go
to `tclean_case_matrix.py`, which runs both tclean implementations and
compares their scalars, counts, inventory and products as for CASA's own
test cases. The matrix's receipt is `OUT/matrix/receipt.json`.
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))

# Noise-free observations of casatestdata's simobserve sky models.
CORE = {
    "skymodel": "unittest/simobserve/core5ps.skymodel",
    "setpointings": True,
    "integration": "10s",
    "maptype": "ALMA",
    "obsmode": "int",
    "totaltime": "600s",
    "thermalnoise": "",
}

# name: (observations [(project suffix, simobserve overrides)], imaging).
SCENARIOS = {
    # One 12 m pointing: the standard gridder.
    "alma12_single": (
        [("12m", {"antennalist": "alma.cycle10.1.cfg", "mapsize": ["", ""]})],
        {"niter": 500, "threshold": "0.1mJy", "weighting": "natural"},
    ),
    # A 12 m mosaic: several fields, so the mosaic gridder.
    "alma12_mosaic": (
        [("12m", {"antennalist": "alma.cycle10.1.cfg", "mapsize": ["30arcsec", "30arcsec"]})],
        {"niter": 500, "threshold": "0.1mJy", "weighting": "natural"},
    ),
    # The same mosaic weighted as simanalyze lets a user ask: briggs.
    "alma12_mosaic_briggs": (
        [("12m", {"antennalist": "alma.cycle10.1.cfg", "mapsize": ["30arcsec", "30arcsec"]})],
        {"niter": 500, "threshold": "0.1mJy", "weighting": "briggs"},
    ),
    # 12 m and 7 m mosaics of the same field, imaged together as simalma
    # images them (simalma forces stokes I).
    "alma12_aca7_mosaic": (
        [
            ("12m", {"antennalist": "alma.cycle10.1.cfg", "mapsize": ["30arcsec", "30arcsec"]}),
            ("7m", {"antennalist": "aca.cycle10.cfg", "mapsize": ["30arcsec", "30arcsec"]}),
        ],
        {"niter": 500, "threshold": "0.1mJy", "weighting": "briggs"},
    ),
}


def simulate(name: str, suffix: str, overrides: dict, args) -> str:
    """casa-rs's simobserve for one observation: the noise-free
    MeasurementSet's path, relative to `OUT/sim`."""
    work = os.path.join(args.out, "sim", name)
    os.makedirs(work, exist_ok=True)
    project = f"{name}_{suffix}"
    params = dict(CORE, **overrides)
    params["skymodel"] = os.path.join(args.casatestdata, params["skymodel"])
    params.update(project=project, graphics="none", overwrite=True)
    request = os.path.join(work, project + ".json")
    with open(request, "w", encoding="utf-8") as handle:
        json.dump({"surface": "simobserve", "values": params}, handle, indent=1)
    with open(os.path.join(work, project + ".log"), "w", encoding="utf-8") as log:
        result = subprocess.run(
            [args.simobserve, "--json-run", request],
            cwd=work,
            stdout=subprocess.PIPE,
            stderr=log,
            text=True,
            check=False,
        )
    reply = json.loads(result.stdout or "{}")
    if reply.get("status") != "succeeded":
        raise SystemExit(f"{project}: casa-rs simobserve failed: {reply.get('message')}")
    stem = os.path.splitext(os.path.basename(params["antennalist"]))[0]
    ms = os.path.join(name, project, f"{project}.{stem}.ms")
    if not os.path.isdir(os.path.join(args.out, "sim", ms)):
        raise SystemExit(f"{project}: no MeasurementSet {ms}")
    return ms


def source_fields(ms: str) -> int:
    """The source fields simanalyze counts: SOURCE rows coded `OBJ`."""
    from casatools import table

    tb = table()
    tb.open(os.path.join(ms, "SOURCE"))
    codes = list(tb.getcol("CODE"))
    tb.close()
    return sum(1 for code in codes if code == "OBJ") or len(set(codes))


def simanalyze_call(name: str, ms_paths: list[str], imaging: dict, args) -> dict:
    """The tclean call simanalyze makes for `ms_paths`, from CASA's own
    `simutil.imtclean` (dryrun), with the model's cell, size and direction
    as simanalyze takes them when its own are unset."""
    from casatasks.private.simutil import simutil
    from casatools import image, quanta

    qa = quanta()
    ia = image()
    ia.open(os.path.join(args.casatestdata, CORE["skymodel"]))
    coordinates = ia.coordsys()
    shape = ia.shape()
    increment = coordinates.increment(type="direction", format="q")["quantity"]
    reference = coordinates.referencevalue(type="direction", format="q")["quantity"]
    ia.close()
    cell = [qa.abs(increment["*1"]), qa.abs(increment["*2"])]
    direction = "J2000 " + " ".join(
        qa.formxxx(reference[axis], format=form, prec=6)
        for axis, form in (("*1", "hms"), ("*2", "dms"))
    )
    imsize = [int(shape[0]), int(shape[1])]
    first = os.path.join(args.out, "sim", ms_paths[0])
    gridder = "standard" if source_fields(first) == 1 else "mosaic"
    work = os.path.join(args.out, "calls")
    os.makedirs(work, exist_ok=True)
    here = os.getcwd()
    os.chdir(work)
    try:
        mstoimage = [os.path.join(args.out, "sim", path) for path in ms_paths]
        simutil().imtclean(
            mstoimage if len(mstoimage) > 1 else mstoimage[0],
            name,
            gridder,
            "clark",
            cell,
            imsize,
            direction,
            False,
            imaging["niter"],
            imaging["threshold"],
            imaging["weighting"],
            [],
            True,
            "I",
            modelimage="",
            mask=[],
            dryrun=True,
        )
        with open(name + ".tclean.last", encoding="utf-8") as handle:
            lines = handle.read().splitlines()
    finally:
        os.chdir(here)
    call = {}
    for line in lines:
        if line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        call[key.strip()] = ast.literal_eval(value.strip())
    return call


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", required=True)
    parser.add_argument("--simobserve", required=True)
    parser.add_argument("--casatestdata", required=True)
    parser.add_argument("--casars-python", required=True)
    parser.add_argument("--imager-binary", required=True)
    parser.add_argument("--scenario", action="append", help="run only these scenarios")
    args = parser.parse_args()
    args.out = os.path.abspath(args.out)
    inputs, cases = {}, {}
    for name, (observations, imaging) in SCENARIOS.items():
        if args.scenario and name not in args.scenario:
            continue
        ms_paths = [simulate(name, suffix, overrides, args) for suffix, overrides in observations]
        for path in ms_paths:
            inputs[path] = path
        call = simanalyze_call(name, ms_paths, imaging, args)
        vis = call.pop("vis")
        call.pop("imagename", None)
        names = [os.path.relpath(path, os.path.join(args.out, "sim")) for path in (vis if isinstance(vis, list) else [vis])]
        cases[f"simanalyze.{name}.1"] = {
            "vis": names if len(names) > 1 else names[0],
            "imagename": name,
            "params": call,
            "line": 0,
            "note": f"simanalyze's tclean call (simutil.imtclean) on casa-rs simobserve's {', '.join(names)}",
            "skip_scalars": False,
        }
    case_file = os.path.join(args.out, "cases.json")
    with open(case_file, "w", encoding="utf-8") as handle:
        json.dump({"inputs": inputs, "common": {}, "cases": cases, "unresolved": []}, handle, indent=1)
    command = [
        sys.executable,
        "-I",
        os.path.join(HERE, "tclean_case_matrix.py"),
        "--cases",
        case_file,
        "--data-root",
        os.path.join(args.out, "sim"),
        "--casars-python",
        args.casars_python,
        "--imager-binary",
        args.imager_binary,
        "--revision",
        "simulation-roundtrip",
        "--backend",
        "cpu",
        "--precision",
        "f64",
        "--output",
        os.path.join(args.out, "matrix"),
    ]
    return subprocess.run(command, check=False).returncode


if __name__ == "__main__":
    sys.exit(main())
