#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA 6.7.6.14's multi-image sm.predict DATA for Rust tests (#1053).

Run with CASA's Python, --out under task scratch, and --fixture naming the
committed gzip JSON fixture. Inputs mirror tests/predict_support: eight
antennas, two pointings, four channels and small deterministic point images.
The CASA observe offsets compensate its approved TAI-labelled-UTC difference.
No casa-rs implementation is used to make the reference.
"""

from __future__ import annotations

import argparse
import gzip
import json
import pathlib
import shutil
import subprocess
import sys

import numpy as np

CENTRE = [2.6179938779914944, 0.593411945678072]
OFFSET = [CENTRE[0] + 6e-6 / 0.8338858220678796, CENTRE[1] - 4e-6]
FREQUENCY = 230e9
CHANNEL = 15.625e6
CASES = [
    {"name": "ft_models"},
    {"name": "ft_duplicate", "images": ["a", "a"]},
    {"name": "ft_replace", "initial": True},
    {"name": "ft_increment", "initial": True, "incremental": True},
    {"name": "ft_empty_first", "images": ["zero", "a", "b"]},
    {"name": "ft_components", "components": True},
    {"name": "ft_beam", "beam": True},
    {"name": "wproject_models", "machine": "wproject"},
    {"name": "mosaic_models", "machine": "mosaic"},
    {"name": "mosaic_a", "machine": "mosaic", "images": ["a"]},
    {"name": "mosaic_b", "machine": "mosaic", "images": ["b"]},
    {"name": "mosaic_reverse", "machine": "mosaic", "images": ["b", "a"]},
    {"name": "mosaic_duplicate", "machine": "mosaic", "images": ["a", "a"]},
    {"name": "sd_models", "machine": "sd"},
]


def model(path, name):
    from casatools import coordsys, image

    n, planes = (96, 4) if name == "b" else (64, 1)
    cs = coordsys().newcoordsys(direction=True, spectral=True, stokes="I")
    cs.setunits(["rad", "rad", "", "Hz"])
    cs.setreferencecode("J2000", type="direction")
    cs.setreferencevalue(CENTRE, type="direction")
    cs.setincrement([-2e-6, 2e-6], type="direction")
    cs.setreferencepixel([n // 2, n // 2, 0, 0])
    first, width = (FREQUENCY, CHANNEL) if planes == 4 else (FREQUENCY + 1.5 * CHANNEL, 4 * CHANNEL)
    cs.setreferencecode("LSRK", type="spectral")
    cs.setreferencevalue([first], type="spectral")
    cs.setincrement([width], type="spectral")
    pixels = np.zeros((n, n, 1, planes), dtype=np.float32)
    for plane in range(planes):
        if name == "a":
            pixels[n // 2, n // 2, 0, plane] = 1.25
            pixels[n // 2 + 9, n // 2 - 4, 0, plane] = 0.75
        elif name == "b":
            pixels[n // 2, n // 2, 0, plane] = 0.5 + 0.25 * plane
            pixels[n // 2 + 12, n // 2 + 1, 0, plane] = -0.1
    ia = image()
    ia.fromarray(outfile=str(path), pixels=pixels, csys=cs.torecord(), overwrite=True)
    ia.setbrightnessunit("Jy/pixel")
    ia.done()
    cs.done()


def make_session(path, stokes="XX YY", *, telescope="ALMA", dishes=(12.,)):
    from casatools import measures, simulator

    sm, me = simulator(), measures()
    sm.open(str(path))
    centre = [-1601185.0, -5041977.0, 3554875.0]
    xyz = [[centre[axis] + step * i for i in range(8)] for axis, step in enumerate([20.0, -6.0, 12.0])]
    assert sm.setconfig(telescopename=telescope, x=xyz[0], y=xyz[1], z=xyz[2], dishdiameter=list(dishes),
                        offset=[0.0], mount=["alt-az"], antname=["DA"], padname=["A"], coordsystem="global",
                        referencelocation=me.observatory("ALMA"))
    assert sm.setspwindow(spwname="band6", freq="230GHz", deltafreq="15.625MHz",
                          freqresolution="15.625MHz", refcode="TOPO", nchannels=4, stokes=stokes)
    assert sm.setfeed(mode="perfect X Y")
    for name, direction in (("target", CENTRE), ("offset", OFFSET)):
        assert sm.setfield(sourcename=name, sourcedirection=me.direction(
            "J2000", f"{direction[0]}rad", f"{direction[1]}rad"))
    epoch = me.epoch("UTC", "2024/06/01/00:00:00")
    assert sm.settimes(integrationtime="10s", usehourangle=False, referencetime=epoch)
    # The Rust session's starts are 0 and 20 seconds, at the requested UTC.
    assert sm.observe(sourcename="target", spwname="band6", starttime="-37s", stoptime="-17s")
    assert sm.observe(sourcename="offset", spwname="band6", starttime="-17s", stoptime="3s")
    me.done()
    return sm


def read_data(path):
    from casatools import table

    tb = table()
    tb.open(str(path))
    try:
        return [{
            "antennas": [int(tb.getcell("ANTENNA1", r)), int(tb.getcell("ANTENNA2", r))],
            "field": int(tb.getcell("FIELD_ID", r)),
            "flag_row": bool(tb.getcell("FLAG_ROW", r)),
            "data": [[float(v.real), float(v.imag)] for v in tb.getcell("DATA", r).T.ravel()],
        } for r in range(tb.nrows())]
    finally:
        tb.close()


def main():
    from casatools import componentlist
    import casatasks

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=pathlib.Path, required=True)
    parser.add_argument("--fixture", type=pathlib.Path, required=True)
    parser.add_argument("--only", nargs="+", choices=[case["name"] for case in CASES])
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if casatasks.version_string() != "6.7.6.14":
        raise RuntimeError(f"Expected CASA 6.7.6.14, got {casatasks.version_string()}")
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=True)
    results = []

    def save():
        fixture = {"casa_version": casatasks.version_string(), "work_issue": 1053,
                   "recipe": "tools/simulation/sm_multiple_models_oracle.py", "cases": results}
        args.fixture.parent.mkdir(parents=True, exist_ok=True)
        with args.fixture.open("wb") as raw, gzip.GzipFile(filename="", fileobj=raw, mode="wb", mtime=0) as compressed:
            compressed.write((json.dumps(fixture, separators=(",", ":"), allow_nan=False) + "\n").encode())

    selected = [case for case in CASES if not args.only or case["name"] in args.only]
    if not args.worker:
        # The known CASA mosaic cleanup abort must not lose DATA or stop
        # the other cases. Only that identified post-predict abort is accepted.
        for spec in selected:
            case_out = out / spec["name"]
            child_fixture = case_out / "result.json.gz"
            child_fixture.unlink(missing_ok=True)
            log_path = out / (spec["name"] + ".log")
            command = [sys.executable, __file__, "--out", str(case_out), "--fixture", str(child_fixture),
                       "--worker", "--only", spec["name"]]
            with log_path.open("w") as log:
                process = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=False)
            if not child_fixture.exists():
                raise RuntimeError(f"{spec['name']}: no completed prediction; see {log_path}")
            with gzip.open(child_fixture, "rt") as handle:
                case = json.load(handle)["cases"][0]
            if process.returncode:
                logged = log_path.read_text()
                known_abort = (spec.get("machine") == "mosaic" and case["cleanup"] == "not returned"
                               and ((process.returncode in (-6, 134, 255)
                                     and "Pure virtual function called" in logged)
                                    or (spec["name"] == "mosaic_duplicate" and process.returncode == -10
                                        and "mosaic_duplicate rows 144" in logged)))
                if not known_abort:
                    raise RuntimeError(f"{spec['name']}: unexpected exit {process.returncode}; see {log_path}")
            elif case["cleanup"] != "returned":
                raise RuntimeError(f"{spec['name']}: cleanup outcome was not recorded")
            case["process_exit"] = process.returncode
            results.append(case)
            save()
            print(spec["name"], "rows", len(case["rows"]), "cleanup", case["cleanup"], flush=True)
        return 0

    images = {name: out / f"{name}.image" for name in ("a", "b", "zero")}
    for name, path in images.items():
        model(path, name)
    cl = componentlist()
    assert cl.addcomponent(dir=f"J2000 {CENTRE[0]}rad {CENTRE[1]}rad", flux=0.25, freq="230GHz")
    shutil.rmtree(out / "point.cl", ignore_errors=True)
    cl.rename(str(out / "point.cl"))
    cl.done()
    for spec in selected:
        path = out / (spec["name"] + ".ms")
        shutil.rmtree(path, ignore_errors=True)
        sm = make_session(path)
        machine = spec.get("machine", "ft")
        assert sm.setoptions(ftmachine=machine, gridfunction="BOX" if machine == "sd" else "SF", wprojplanes=16)
        if spec.get("beam"):
            assert sm.setvp(dovp=True, usedefaultvp=False)
        if spec.get("initial"):
            assert sm.predict(imagename=str(images["a"]))
        arguments = {"imagename": [str(images[n]) for n in spec.get("images", ["a", "b"])],
                     "incremental": spec.get("incremental", False)}
        if spec.get("components"):
            arguments["complist"] = str(out / "point.cl")
        assert sm.predict(**arguments), f"predict failed: {spec['name']}"
        rows = read_data(path)
        assert any(any(re != 0 or im != 0 for re, im in r["data"]) for r in rows), spec["name"]
        results.append({**spec, "rows": rows, "cleanup": "not returned"})
        save()
        print(spec["name"], "rows", len(rows), flush=True)
        sm.done()
        results[-1]["cleanup"] = "returned"
        save()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
