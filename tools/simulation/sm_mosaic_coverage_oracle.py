#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA intrinsic mosaics outside the sky and with unknown telescope dish classes."""
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

from sm_apply_time_oracle import write
from sm_existing_append_oracle import snapshot
from sm_multiple_models_oracle import make_session


def worker(root, data, name, inside_sky):
    inputs = root / "inputs"
    path = root / f"{name}.ms"
    profile = "fused" if name == "inside_control" else name
    shutil.copytree(inputs / f"{profile}.ms", path, ignore=shutil.ignore_patterns("table.lock"))
    before = snapshot(path)
    sm = simulator()
    assert sm.openfromms(str(path))
    assert sm.setoptions(ftmachine="mosaic")
    assert sm.setvp(dovp=False, dosquint=False)
    if name == "common_outside":
        assert sm.setvp(dovp=False, dosquint=False, usedefaultvp=False,
            vptable=str(data / "sm_voltage_patterns/gaussian_pb.vp"))
    assert sm.setdata(spwid=[], fieldid=[] if name in ("outside", "fused", "inside_control") else [0])
    sky = inputs / "outside.image" if name == "outside" else inside_sky
    if name == "fused":
        sky = [str(inputs / "outside.image"), str(sky)]
    if name == "common_outside":
        sky = inputs / "common_outside.image"
    returned = bool(sm.predict(imagename=sky if name == "fused" else str(sky), incremental=False))
    case = dict(name=f"mosaic_{name}", profile=name, operation="mosaic", dovp=False,
        all_fields_selected=name in ("outside", "fused", "inside_control"), returned=returned,
        before=before, after=snapshot(path), cleanup="not returned")
    if name == "outside":
        assert returned
        assert any(v for row in before["rows"] for pair in row["data"] for v in pair)
        assert not any(v for row in case["after"]["rows"] for pair in row["data"] for v in pair)
    elif name == "fused":
        assert returned
    elif name == "inside_control":
        assert returned
        assert any(v for row in case["after"]["rows"] for pair in row["data"] for v in pair)
    elif name == "common_outside":
        assert returned
        assert any(v for row in case["after"]["rows"] if row["field"] == 0 for pair in row["data"] for v in pair)
    else:
        assert not returned
    output = root / f"{name}.json.gz"
    write(output, dict(casa_version=version_string(), cases=[case]))
    print(name, "prediction completed", flush=True)
    sm.close()
    case["cleanup"] = "returned"
    write(output, dict(casa_version=version_string(), cases=[case]))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--worker")
    parser.add_argument("--sky", type=Path)
    parser.add_argument("--irregular-base", action="store_true")
    args = parser.parse_args()
    assert version_string() == "6.7.6.14"
    root, data = args.out.resolve(), args.data.resolve()
    inside_sky = args.sky.resolve() if args.sky else data / "sm_telescope_beams/sky.image"
    if args.worker:
        worker(root, data, args.worker, inside_sky)
        return
    inputs = root / "inputs"
    inputs.mkdir(parents=True)
    base = data / "sm_image_layouts/base.ms"
    if not args.irregular_base:
        base = root / "base.ms"
        sm = make_session(base, stokes="XX XY YX YY")
        sm.close()
        tb = table()
        tb.open(str(base), nomodify=False)
        for column in ("MODEL_DATA", "CORRECTED_DATA"):
            if column not in tb.colnames():
                desc = tb.getcoldesc("DATA")
                desc["name"] = column
                desc.pop("dataManagerType", None)
                desc.pop("dataManagerGroup", None)
                tb.addcols({column: desc})
            for row in range(tb.nrows()):
                tb.putcell(column, row, np.zeros_like(tb.getcell("DATA", row)))
        tb.close()
    ia = image()
    ia.open(str(inside_sky))
    pixels, cs = ia.getchunk(), ia.coordsys()
    reference = cs.referencevalue(type="direction")["numeric"]
    reference[0] += .005
    cs.setreferencevalue(reference, type="direction")
    ia.done()
    ia.fromarray(outfile=str(inputs / "outside.image"), pixels=pixels, csys=cs.torecord())
    ia.setbrightnessunit("Jy/pixel")
    ia.done()
    cs.done()
    ia = image()
    ia.open(str(inside_sky))
    pixels, cs = ia.getchunk(), ia.coordsys()
    starts = [(pixels.shape[axis]-32)//2 for axis in (0, 1)]
    pixel = cs.referencepixel()["numeric"]
    pixel[:2] -= starts
    cs.setreferencepixel(pixel)
    reference = cs.referencevalue(type="direction")["numeric"]
    reference[0] += 18. * 2e-6 / np.cos(reference[1])
    cs.setreferencevalue(reference, type="direction")
    ia.done()
    ia.fromarray(outfile=str(inputs / "common_outside.image"),
        pixels=pixels[starts[0]:starts[0]+32, starts[1]:starts[1]+32], csys=cs.torecord())
    ia.setbrightnessunit("Jy/pixel")
    ia.done()
    cs.done()
    cases = []
    for name in ("outside", "fused", "unknown", "unknown_heterogeneous", "common_outside"):
        path = inputs / f"{name}.ms"
        shutil.copytree(base, path, ignore=shutil.ignore_patterns("table.lock"))
        tb = table()
        if name in ("outside", "fused", "common_outside"):
            tb.open(str(path), nomodify=False)
            for row in range(tb.nrows()):
                for column in ("DATA", "CORRECTED_DATA"):
                    tb.putcell(column, row, np.full_like(tb.getcell(column, row), 1.+.2j))
            tb.close()
        else:
            tb.open(str(path / "OBSERVATION"), nomodify=False)
            tb.putcell("TELESCOPE_NAME", 0, "CUSTOM_SIMULATION")
            tb.close()
            if name.endswith("heterogeneous"):
                tb.open(str(path / "ANTENNA"), nomodify=False)
                for row in range(tb.nrows()):
                    tb.putcell("DISH_DIAMETER", row, 7. if row % 2 else 12.)
                tb.close()
        log = root / f"{name}.log"
        with log.open("w") as stream:
            process = subprocess.run([sys.executable, __file__, "--out", str(root),
                "--fixture", str(args.fixture), "--data", str(data), "--sky", str(inside_sky), "--worker", name],
                stdout=stream, stderr=subprocess.STDOUT)
        with gzip.open(root / f"{name}.json.gz", "rt") as stream:
            additions = json.load(stream)["cases"]
        assert not process.returncode or "Pure virtual function called" in log.read_text(), log
        if name == "fused":
            control_log = root / "inside_control.log"
            with control_log.open("w") as stream:
                control = subprocess.run([sys.executable, __file__, "--out", str(root),
                    "--fixture", str(args.fixture), "--data", str(data), "--sky", str(inside_sky), "--worker", "inside_control"],
                    stdout=stream, stderr=subprocess.STDOUT)
            assert not control.returncode or "Pure virtual function called" in control_log.read_text(), control_log
            with gzip.open(root / "inside_control.json.gz", "rt") as stream:
                reference = json.load(stream)["cases"][0]
            additions[0]["corrected_rows"] = reference["after"]["rows"]
            additions[0]["casa_defect"] = "CASA predicts zero for [outside model, valid model], losing the valid signal. The independent control predicts the valid model alone; the outside-only control is zero, so linearity gives the expected sum. Raw fused output is retained."
        cases.extend(additions)
        write(args.fixture, dict(casa_version=version_string(),
            recipe="tools/simulation/sm_mosaic_coverage_oracle.py", cases=cases))
        print(name, "passed", flush=True)


if __name__ == "__main__":
    main()
