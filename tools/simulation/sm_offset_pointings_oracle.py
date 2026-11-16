#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA selected beam/mosaic/total-power predictions on offset POINTING (SIM-05/06)."""
import argparse
import gzip
import json
from pathlib import Path
import shutil
import subprocess
import sys

import numpy as np
from casatasks import version_string
from casatools import image, simulator, table

from sm_apply_parameters_oracle import initialize
from sm_apply_time_oracle import write
from sm_existing_append_oracle import snapshot
from sm_multiple_models_oracle import make_session


def worker(root, name, corrected=False):
    profile, operation = name.split("_", 1)
    inputs = root / "inputs"
    path = root / f"{name}{'-corrected' if corrected else ''}.ms"
    shutil.copytree(inputs / f"{profile}.ms", path, ignore=shutil.ignore_patterns("table.lock"))
    before = snapshot(path)
    fields = [0]
    if corrected:
        tb = table()
        tb.open(str(path / "FIELD"), nomodify=False)
        originals = [{c: tb.getcell(c, row) for c in tb.colnames()} for row in (0, 1)]
        tb.addrows(2)
        for row, values in enumerate(originals, 2):
            for column, value in values.items():
                tb.putcell(column, row, value)
        tb.close()
        tb.open(str(path), nomodify=False)
        times = tb.getcol("TIME")
        ids = tb.getcol("FIELD_ID")
        for field in (0, 1):
            later = (ids == field) & (times > np.min(times[ids == field]))
            ids[later] = field + 2
        tb.putcol("FIELD_ID", ids)
        tb.close()
        fields = [0, 2]
    sm = simulator()
    assert sm.openfromms(str(path))
    machine = "mosaic" if operation == "mosaic" else "sd" if operation.startswith("sd_") else "ft"
    assert sm.setoptions(ftmachine=machine, gridfunction="PB" if operation == "sd_pb" else "BOX" if machine == "sd" else "SF")
    if "beam" in operation:
        assert sm.setvp(dovp=True, usedefaultvp=False)
    predicate = f"TIME >= {60462. * 86400. + 10.}" if operation.endswith("late") else ""
    assert sm.setdata(fieldid=fields, msselect=predicate)
    arguments = {"incremental": False}
    model = "spectrum.cl" if operation.startswith("components") else "sd.image" if machine == "sd" else "spectrum.image"
    arguments["complist" if operation.startswith("components") else "imagename"] = str(inputs / model)
    assert sm.predict(**arguments), name
    case = dict(name=name, profile=profile, operation=operation, predicate=predicate,
                native_single_dish=machine == "sd",
                before=before, after=snapshot(path), cleanup="not returned")
    output = root / f"{name}{'-corrected' if corrected else ''}.json.gz"
    write(output, dict(casa_version=version_string(), cases=[case]))
    print(name, "prediction completed", flush=True)
    sm.close()
    case["cleanup"] = "returned"
    write(output, dict(casa_version=version_string(), cases=[case]))
    return case


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--worker")
    parser.add_argument("--corrected", action="store_true")
    args = parser.parse_args()
    assert version_string() == "6.7.6.14"
    root = args.out.resolve()
    if args.worker:
        worker(root, args.worker, args.corrected)
        return
    inputs = root / "inputs"
    inputs.mkdir(parents=True, exist_ok=True)
    source = Path(__file__).resolve().parents[2] / "crates/casa-simulation/tests/data/sm_irregular_channels"
    for name in ("spectrum.cl", "spectrum.image"):
        if not (inputs / name).exists():
            shutil.copytree(source / name, inputs / name, ignore=shutil.ignore_patterns("table.lock"))
    ia = image()
    ia.open(str(inputs / "spectrum.image"))
    cs = ia.coordsys()
    cs.setincrement([-2e-5, 2e-5], type="direction")
    pixels = ia.getchunk()
    ia.close()
    x, y = np.indices(pixels.shape[:2])
    pixels[:] = (1. + .01 * x + .02 * y)[:, :, None, None]
    ia.fromarray(outfile=str(inputs / "sd.image"), pixels=pixels, csys=cs.torecord(), overwrite=True)
    ia.setbrightnessunit("Jy/pixel")
    ia.done()
    cs.done()
    for profile in ("fixed", "changing"):
        path = inputs / f"{profile}.ms"
        if path.exists():
            continue
        sm = make_session(path, stokes="XX XY YX YY")
        sm.close()
        initialize(path)
        tb = table()
        tb.open(str(path / "POINTING"), nomodify=False)
        first = float(np.min(tb.getcol("TIME")))
        for row in range(tb.nrows()):
            factor = 1. if profile == "fixed" or int(round((tb.getcell("TIME", row) - first) / 10.)) % 2 == 0 else -1.
            for column in ("DIRECTION", "TARGET"):
                direction = tb.getcell(column, row)
                direction[:, 0] += factor * np.array([5e-5, -2e-5])
                tb.putcell(column, row, direction)
        tb.close()
    cases = []
    operations = ("image_beam", "components_beam", "image_beam_late", "components_beam_late",
                  "mosaic", "sd_box", "sd_pb", "sd_beam")
    for profile in ("fixed", "changing"):
        for operation in operations:
            name = f"{profile}_{operation}"
            log = root / f"{name}.log"
            shutil.rmtree(root / f"{name}.ms", ignore_errors=True)
            with log.open("w") as stream:
                process = subprocess.run([sys.executable, __file__, "--out", str(root), "--fixture", str(args.fixture), "--worker", name], stdout=stream, stderr=subprocess.STDOUT)
            with gzip.open(root / f"{name}.json.gz", "rt") as stream:
                case = json.load(stream)["cases"][0]
            if process.returncode and not (operation == "mosaic" and "Pure virtual function called" in log.read_text() and case["cleanup"] == "not returned"):
                raise RuntimeError(f"{name}: exit {process.returncode}; see {log}")
            if profile == "changing" and ("beam" in operation or operation == "mosaic"):
                corrected_log = root / f"{name}-corrected.log"
                shutil.rmtree(root / f"{name}-corrected.ms", ignore_errors=True)
                with corrected_log.open("w") as stream:
                    corrected = subprocess.run([sys.executable, __file__, "--out", str(root), "--fixture", str(args.fixture), "--worker", name, "--corrected"], stdout=stream, stderr=subprocess.STDOUT)
                with gzip.open(root / f"{name}-corrected.json.gz", "rt") as stream:
                    control = json.load(stream)["cases"][0]
                if corrected.returncode and not (operation == "mosaic" and "Pure virtual function called" in corrected_log.read_text() and control["cleanup"] == "not returned"):
                    raise RuntimeError(f"{name}: corrected control exit {corrected.returncode}")
                case["pointing_cache_corrected_rows"] = control["after"]["rows"]
            case["process_exit"] = process.returncode
            cases.append(case)
            write(args.fixture, dict(casa_version=version_string(), recipe="tools/simulation/sm_offset_pointings_oracle.py", cases=cases))
            print(name, "rows", len(case["after"]["rows"]), "cleanup", case["cleanup"], flush=True)


if __name__ == "__main__":
    main()
