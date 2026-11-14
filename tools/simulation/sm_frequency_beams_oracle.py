#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Simulator consumers of saved frequency-dependent inverse polynomials."""
import argparse
import gzip
import json
from pathlib import Path
import shutil
import subprocess
import sys

import numpy as np
from casatasks import version_string
from casatools import simulator, table, vpmanager

from sm_apply_time_oracle import write
from sm_existing_append_oracle import snapshot
from sm_voltage_patterns_oracle import correct_component_phase
from sm_saved_curves_oracle import mosaic_case


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--sky", type=Path, required=True)
    parser.add_argument("--mosaic-worker", action="store_true")
    parser.add_argument("--beam")
    args = parser.parse_args()
    assert version_string() == "6.7.6.14"
    root = args.out.resolve()
    inputs = root / "inputs"
    if args.mosaic_worker:
        mosaic_case(root, inputs, args.base, "frequency_power", "frequency_power_mosaic")
        return
    inputs.mkdir(parents=True)
    shutil.copytree(args.sky / "sky.image", inputs / "sky.image")
    cases = []
    for beam, is_voltage, frequencies in (
            ("frequency_power", False, [120e9, 180e9, 240e9]),
            ("frequency_voltage", True, [120e9, 180e9, 240e9]),
            ("frequency_single", False, [150e9]),
            ("frequency_mixed_sign", False, [120e9, 240e9])):
        if args.beam and beam != args.beam:
            continue
        vp = vpmanager()
        assert vp.reset()
        assert vp.setpbinvpoly(telescope="ALMA", coeff=[1., 5e-5, 1e-9, 1e-14],
            maxrad="60arcsec", reffreq="150GHz", isthispb="VP" if is_voltage else "PB")
        voltage_table = inputs / f"{beam}.vp"
        assert vp.saveastable(str(voltage_table))
        # The public table is the input to the simulator's PBMath record
        # constructor. VPManager's C++ overload stores this coefficient matrix.
        tb = table()
        tb.open(str(voltage_table), nomodify=False)
        record = tb.getcell("pbdescription", 0)
        record["coeff"] = np.asarray([[1., 1.05, .97], [5e-5, 7e-5, 9e-5],
            [1e-9, .5e-9, 2e-9], [1e-14, 2e-14, .5e-14]])[:, :len(frequencies)]
        record["fitfreqs"] = np.asarray(frequencies)
        if beam == "frequency_mixed_sign":
            record["coeff"] = np.asarray([[1., 1.], [0., -.01], [0., 0.], [0., 0.]])
        tb.putcell("pbdescription", 0, record)
        tb.close()
        for operation in ("image", "components"):
            name = f"{beam}_{operation}"
            path = root / f"{name}.ms"
            shutil.copytree(args.base, path, ignore=shutil.ignore_patterns("table.lock"))
            before = snapshot(path)
            sm = simulator()
            assert sm.openfromms(str(path))
            assert sm.setvp(dovp=True, usedefaultvp=False, vptable=str(voltage_table), dosquint=False)
            assert sm.setdata(fieldid=[0])
            assert sm.predict(**{"imagename" if operation == "image" else "complist":
                str(args.sky / ("sky.image" if operation == "image" else "sky.cl"))}, incremental=False)
            sm.close()
            case = dict(name=name, beam=beam, operation=operation, before=before, after=snapshot(path))
            assert all(np.isfinite(v) for row in case["after"]["rows"] for pair in row["data"] for v in pair), name
            if operation == "components":
                correct_component_phase(case)
            cases.append(case)
            write(args.fixture, dict(casa_version=version_string(),
                recipe="tools/simulation/sm_frequency_beams_oracle.py", cases=cases))
            print(name, "passed", flush=True)
    if args.beam and args.beam != "frequency_power":
        return
    log = root / "frequency_power_mosaic.log"
    with log.open("w") as stream:
        result = subprocess.run([sys.executable, __file__, "--out", str(root),
            "--fixture", str(args.fixture), "--base", str(args.base), "--sky", str(args.sky),
            "--mosaic-worker"], stdout=stream, stderr=subprocess.STDOUT)
    with gzip.open(root / "frequency_power_mosaic.json.gz", "rt") as stream:
        case = json.load(stream)["cases"][0]
    assert not result.returncode or (case["cleanup"] == "not returned"
        and "Pure virtual function called" in log.read_text()), log
    cases.append(case)
    write(args.fixture, dict(casa_version=version_string(),
        recipe="tools/simulation/sm_frequency_beams_oracle.py", cases=cases))


if __name__ == "__main__":
    main()
