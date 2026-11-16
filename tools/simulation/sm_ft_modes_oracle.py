#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA simulator FT-mode calculations and mode-specific grid functions."""
import argparse
import gzip
import json
from pathlib import Path
import shutil
import subprocess
import sys

from casatasks import version_string
import numpy as np
from casatools import simulator, image

from sm_apply_time_oracle import write
from sm_existing_append_oracle import snapshot

CASES = [("ft", "sf"), ("ft", "box"), ("ft", "invalid"),
    ("wproject", "box"), ("mosaic", "box"),
    ("both", "sf"), ("both", "box"), ("both", "pb"),
    ("pbwproject", "sf"), ("sd", "gauss"), ("sd", "gjinc"), ("sd", "invalid")]
CASES = [(machine, grid, 64) for machine, grid in CASES] + [
    ("ft", "sf", 256), ("both", "pb", 256), ("sd", "pb", 256)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--worker", type=int)
    args = parser.parse_args()
    assert version_string() == "6.7.6.14"
    root, data = args.out.resolve(), args.data.resolve()
    root.mkdir(parents=True, exist_ok=True)
    if args.worker is None:
        inputs = root / "inputs"
        inputs.mkdir(exist_ok=True)
        if not (inputs / "large.image").exists():
            ia = image()
            ia.open(str(data / "sm_telescope_beams/sky.image"))
            values, cs = ia.getchunk(), ia.coordsys()
            pixel = cs.referencepixel()["numeric"]
            pixel[:2] += 96.
            cs.setreferencepixel(pixel)
            ia.done()
            pixels = np.pad(values, ((96, 96), (96, 96), (0, 0), (0, 0)))
            ia.fromarray(outfile=str(inputs / "large.image"), pixels=pixels, csys=cs.torecord())
            ia.setbrightnessunit("Jy/pixel")
            ia.done()
            cs.done()
        if not (inputs / "kernels.image").exists():
            ia = image()
            ia.open(str(data / "sm_telescope_beams/sky.image"))
            values, cs = ia.getchunk(), ia.coordsys()
            pixel = cs.referencepixel()["numeric"]
            pixel[:2] += [.35, -.2]
            cs.setreferencepixel(pixel)
            ia.done()
            ia.fromarray(outfile=str(inputs / "kernels.image"), pixels=values, csys=cs.torecord())
            ia.setbrightnessunit("Jy/pixel")
            ia.done()
            cs.done()
        cases = []
        for index in range(len(CASES)):
            path = root / f"{index}.json.gz"
            if not path.exists():
                with (root / f"{index}.log").open("w") as log:
                    result = subprocess.run([sys.executable, __file__, "--out", str(root),
                        "--fixture", str(args.fixture), "--data", str(data), "--worker", str(index)],
                        stdout=log, stderr=subprocess.STDOUT)
                assert path.exists(), (index, result.returncode)
            with gzip.open(path, "rt") as stream:
                case = json.load(stream)["cases"][0]
            cases.append(case)
            write(args.fixture, dict(casa_version=version_string(),
                recipe="tools/simulation/sm_ft_modes_oracle.py", cases=cases))
            print(case["name"], case["outcome"], case.get("error", ""), flush=True)
        references = {case["name"]: case for case in cases}
        for case in cases:
            if case["machine"] == "sd" and case["returned"]:
                case["native_single_dish"] = True
            if case["machine"] == "both" and case["returned"]:
                rows = json.loads(json.dumps(case["after"]["rows"]))
                for row, reference in zip(rows, references["ft_sf_large" if case.get("large") else "ft_sf"]["after"]["rows"]):
                    if row["field"] == 0 and row["antennas"][0] != row["antennas"][1]:
                        for column in ("data", "corrected_data"):
                            row[column] = reference[column]
                case["corrected_rows"] = rows
                case["casa_defect"] = "GridBoth inherits SDGrid's documented uvw-sign cross-correlation defect. Cross-correlation signals use the independent GridFT calculation; autocorrelations retain the joint calculation (GridBoth GridFT uses usezero=false). Raw CASA output is preserved."
            if case["machine"] == "both" and not case["returned"]:
                case["corrected_rows"] = case["before"]["rows"]
                case["casa_defect"] = "GridBoth zeroes selected DATA despite refusing an image smaller than its PB support. Native refusal preserves the input data instead of reproducing this failed-call data loss; raw CASA output is retained."
        write(args.fixture, dict(casa_version=version_string(), recipe="tools/simulation/sm_ft_modes_oracle.py", cases=cases))
        return
    machine, grid, side = CASES[args.worker]
    name = f"{machine}_{grid}" + ("_large" if side == 256 else "")
    path = root / f"{name}.ms"
    shutil.copytree(data / "sm_mosaic_coverage/outside.ms", path,
        ignore=shutil.ignore_patterns("table.lock"))
    before = snapshot(path)
    sm = simulator()
    assert sm.openfromms(str(path))
    returned, error, outcome = None, None, "Raises"
    try:
        assert sm.setoptions(ftmachine=machine, gridfunction=grid, wprojplanes=16)
        assert sm.setvp(dovp=False, dosquint=False)
        assert sm.setdata(fieldid=[0])
        shifted = machine == "sd" and grid in ("gauss", "gjinc")
        sky = root / "inputs/kernels.image" if shifted else (root / "inputs/large.image" if side == 256 else data / "sm_telescope_beams/sky.image")
        returned = bool(sm.predict(imagename=str(sky), incremental=False))
        outcome = "Success" if returned else "ReturnsFalse"
    except Exception as exc:
        error = str(exc)
    case = dict(name=name, machine=machine, grid=grid, large=side == 256,
        shifted=machine == "sd" and grid in ("gauss", "gjinc"),
        returned=returned, outcome=outcome, error=error, before=before, after=snapshot(path))
    write(root / f"{args.worker}.json.gz", dict(casa_version=version_string(), cases=[case]))
    sm.close()


if __name__ == "__main__":
    main()
