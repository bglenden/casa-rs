#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Named default telescope patterns and heterogeneous simulation mosaics."""
import argparse
import gzip
import json
from pathlib import Path
import subprocess
import sys

from casatasks import version_string
from casatools import componentlist

from sm_apply_time_oracle import write
from sm_existing_append_oracle import snapshot
from sm_image_layouts_oracle import earth_orientation
from sm_multiple_models_oracle import CENTRE, OFFSET, make_session, model

CASES = [dict(name=f"{tel.lower()}_{op}", telescope=tel,
    dishes=[dish], operation=op) for tel, dish in (("SMA", 6.), ("ATA", 6.), ("NGVLA", 18.))
    for op in ("image", "components")]
CASES += [dict(name="sma_mosaic", telescope="SMA", dishes=[6.], operation="mosaic"),
    dict(name="ovro_mixed_mosaic", telescope="OVRO", dishes=[10.4, 6.]*4, operation="mosaic"),
    dict(name="alma_mixed_mosaic", telescope="ALMA", dishes=[12., 9.]*4, operation="mosaic")]


def worker(root, spec):
    path = root / f"{spec['name']}.ms"
    sm = make_session(path, stokes="XX XY YX YY", telescope=spec["telescope"], dishes=spec["dishes"])
    before = snapshot(path)
    mosaic = spec["operation"] == "mosaic"
    assert sm.setoptions(ftmachine="mosaic" if mosaic else "ft")
    assert sm.setvp(dovp=not mosaic, dosquint=False)
    inputs = root / "inputs"
    components = spec["operation"] == "components"
    returned = sm.predict(**{"complist" if components else "imagename":
        str(inputs / ("sky.cl" if components else "sky.image"))}, incremental=False)
    case = dict(spec, native_created=True, returned=returned, before=before,
        after=snapshot(path), eop=earth_orientation(before), cleanup="not returned")
    output = root / f"{spec['name']}.json.gz"
    write(output, dict(casa_version=version_string(), cases=[case]))
    assert returned, spec
    sm.close()
    case["cleanup"] = "returned"
    write(output, dict(casa_version=version_string(), cases=[case]))
    return case


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--worker", choices=[s["name"] for s in CASES])
    args = parser.parse_args()
    assert version_string() == "6.7.6.14"
    root = args.out.resolve()
    if args.worker:
        worker(root, next(s for s in CASES if s["name"] == args.worker))
        return
    inputs = root / "inputs"
    inputs.mkdir(parents=True)
    model(inputs / "sky.image", "a")
    cl = componentlist()
    for flux, direction in ((1., CENTRE), (.4, OFFSET)):
        assert cl.addcomponent(flux=flux, freq="230GHz",
            dir=f"J2000 {direction[0]}rad {direction[1]}rad")
    cl.rename(str(inputs / "sky.cl"))
    cl.done()
    cases = []
    for spec in CASES:
        log = root / f"{spec['name']}.log"
        with log.open("w") as stream:
            result = subprocess.run([sys.executable, __file__, "--out", str(root),
                "--fixture", str(args.fixture), "--worker", spec["name"]],
                stdout=stream, stderr=subprocess.STDOUT)
        with gzip.open(root / f"{spec['name']}.json.gz", "rt") as stream:
            case = json.load(stream)["cases"][0]
        assert not result.returncode or (spec["operation"] == "mosaic"
            and case["cleanup"] == "not returned" and "Pure virtual function called" in log.read_text()), (spec, log)
        assert case["returned"], spec
        cases.append(case)
        write(args.fixture, dict(casa_version=version_string(),
            recipe="tools/simulation/sm_telescope_beams_oracle.py", cases=cases))
        print(spec["name"], "passed", flush=True)


if __name__ == "__main__":
    main()
