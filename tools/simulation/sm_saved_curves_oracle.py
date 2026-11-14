#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Simulated image/component signals through saved radial VPManager curves."""
import argparse
import gzip
import json
import subprocess
import sys
from pathlib import Path
import shutil

from casatasks import version_string
from casatools import simulator, vpmanager

from sm_apply_time_oracle import write
from sm_existing_append_oracle import snapshot
from sm_voltage_patterns_oracle import correct_component_phase

CURVES = {
    "polynomial": ("setpbpoly", dict(coeff=[1., -3e-5, 4e-10])),
    "inverse_polynomial": ("setpbinvpoly", dict(coeff=[1., 6e-5, 1e-9])),
    "cosine_polynomial": ("setpbcospoly", dict(coeff=[.1, .9], scale=[.004, .008])),
    "numeric_power": ("setpbnumeric", dict(vect=[1., .95, .8, .55, .3, .12, .04, 0.])),
    "numeric_voltage": ("setpbnumeric", dict(vect=[1., .8, .3, 0., -.12, -.05, 0.], isthispb="VP")),
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--sky", type=Path, required=True)
    parser.add_argument("--mosaic-worker", action="store_true")
    args = parser.parse_args()
    assert version_string() == "6.7.6.14"
    root = args.out.resolve()
    inputs = root / "inputs"
    if args.mosaic_worker:
        mosaic_case(root, inputs, args.base)
        return
    inputs.mkdir(parents=True)
    for name in ("sky.image", "sky.cl"):
        shutil.copytree(args.sky / name, inputs / name, ignore=shutil.ignore_patterns("table.lock"))
    vp = vpmanager()
    cases = []
    for beam, (method, parameters) in CURVES.items():
        assert vp.reset()
        record = getattr(vp, method)(telescope="ALMA", maxrad="60arcsec", reffreq="150GHz", **parameters)
        assert record, beam
        assert vp.saveastable(str(inputs / f"{beam}.vp"))
        for operation in ("image", "components"):
            name = f"{beam}_{operation}"
            path = root / f"{name}.ms"
            shutil.copytree(args.base, path, ignore=shutil.ignore_patterns("table.lock"))
            before = snapshot(path)
            sm = simulator()
            assert sm.openfromms(str(path))
            assert sm.setvp(dovp=True, usedefaultvp=False, vptable=str(inputs / f"{beam}.vp"), dosquint=False)
            assert sm.setdata(fieldid=[0])
            assert sm.predict(**{"imagename" if operation == "image" else "complist":
                str(inputs / ("sky.image" if operation == "image" else "sky.cl"))}, incremental=False)
            sm.close()
            case = dict(name=name, beam=beam, operation=operation,
                before=before, after=snapshot(path))
            assert any(v for row in case["after"]["rows"] if row["field"] == 0
                for pair in row["data"] for v in pair), name
            if operation == "components":
                correct_component_phase(case)
            cases.append(case)
            write(args.fixture, dict(casa_version=version_string(),
                recipe="tools/simulation/sm_saved_curves_oracle.py", cases=cases))
            print(name, "passed", flush=True)
    log = root / "numeric_power_mosaic.log"
    with log.open("w") as stream:
        result = subprocess.run([sys.executable, __file__, "--out", str(root),
            "--fixture", str(args.fixture), "--base", str(args.base), "--sky", str(args.sky),
            "--mosaic-worker"], stdout=stream, stderr=subprocess.STDOUT)
    with gzip.open(root / "numeric_power_mosaic.json.gz", "rt") as stream:
        case = json.load(stream)["cases"][0]
    assert not result.returncode or (case["cleanup"] == "not returned"
        and "Pure virtual function called" in log.read_text()), log
    cases.append(case)
    write(args.fixture, dict(casa_version=version_string(),
        recipe="tools/simulation/sm_saved_curves_oracle.py", cases=cases))


def mosaic_case(root, inputs, base, beam="numeric_power", name="numeric_power_mosaic"):
    path = root / f"{name}.ms"
    shutil.copytree(base, path, ignore=shutil.ignore_patterns("table.lock"))
    before = snapshot(path)
    sm = simulator()
    assert sm.openfromms(str(path))
    assert sm.setoptions(ftmachine="mosaic")
    assert sm.setvp(dovp=False, usedefaultvp=False,
        vptable=str(inputs / f"{beam}.vp"), dosquint=False)
    assert sm.setdata(fieldid=[0])
    assert sm.predict(imagename=str(inputs / "sky.image"), incremental=False)
    case = dict(name=name, beam=beam, operation="mosaic", dovp=False,
        before=before, after=snapshot(path), cleanup="not returned")
    output = root / f"{name}.json.gz"
    write(output, dict(casa_version=version_string(), cases=[case]))
    sm.close()
    case["cleanup"] = "returned"
    write(output, dict(casa_version=version_string(), cases=[case]))


if __name__ == "__main__":
    main()
