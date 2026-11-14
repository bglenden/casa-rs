#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Fresh CASA observeMany phase centres and independent beam/mosaic pointings."""
import argparse
import copy
import gzip
import json
from pathlib import Path
import subprocess
import sys

import numpy as np
from casatasks import version_string
from casatools import componentlist, image, measures, simulator, table

from sm_apply_time_oracle import write
from sm_existing_append_oracle import snapshot
from sm_image_layouts_oracle import earth_orientation
from sm_multiple_models_oracle import CENTRE, OFFSET, model

NAMES = [f"fixed_{op}" for op in ("image_plain", "components_plain", "image_beam", "components_beam", "mosaic")]
NAMES += [f"changing_{op}" for op in ("image_beam", "components_beam", "mosaic")]
NAMES += [f"{profile}_{op}" for profile in ("fixed", "changing") for op in ("sd_box", "sd_pb", "sd_beam")]


def observation(path, profile):
    sm, me = simulator(), measures()
    assert sm.open(str(path))
    centre = [-1601185., -5041977., 3554875.]
    xyz = [[centre[axis]+step*i for i in range(8)]
        for axis, step in enumerate((20., -6., 12.))]
    assert sm.setconfig(telescopename="ALMA", x=xyz[0], y=xyz[1], z=xyz[2],
        dishdiameter=[12.], offset=[0.], mount=["alt-az"], antname=["DA"], padname=["A"],
        coordsystem="global", referencelocation=me.observatory("ALMA"))
    assert sm.setspwindow(spwname="band6", freq="230GHz", deltafreq="15.625MHz",
        freqresolution="15.625MHz", refcode="TOPO", nchannels=4, stokes="XX XY YX YY")
    assert sm.setfeed(mode="perfect X Y")
    for name, direction in (("target", CENTRE), ("offset", OFFSET)):
        assert sm.setfield(sourcename=name, sourcedirection=me.direction(
            "J2000", f"{direction[0]}rad", f"{direction[1]}rad"))
    assert sm.settimes(integrationtime="10s", usehourangle=False,
        referencetime=me.epoch("UTC", "2024/06/01/00:00:00"))
    offsets = [(2e-5, -1e-5), (-3e-5, 2e-5) if profile == "changing" else (2e-5, -1e-5)]
    directions = [f"J2000 {CENTRE[0]+ra}rad {CENTRE[1]+dec}rad" for ra, dec in offsets]
    assert sm.observemany(sourcenames=["target", "target"], spwname="band6",
        starttimes=["-37s", "-17s"], stoptimes=["-17s", "3s"], directions=directions,
        state_obs_mode="OBSERVE_TARGET.ON_SOURCE")
    me.done()
    return sm


def split_later_field(path):
    """Independent CASA cache-reset control, as in the resumed-MS oracle."""
    tb = table()
    tb.open(str(path / "FIELD"), nomodify=False)
    original = {column: tb.getcell(column, 0) for column in tb.colnames()}
    row = tb.nrows()
    tb.addrows(1)
    for column, value in original.items():
        tb.putcell(column, row, value)
    tb.close()
    tb.open(str(path), nomodify=False)
    times = tb.getcol("TIME")
    ids = tb.getcol("FIELD_ID")
    ids[times > np.min(times)+15.] = row
    tb.putcol("FIELD_ID", ids)
    tb.close()


def worker(root, name, corrected):
    profile, operation = name.split("_", 1)
    path = root / f"{name}{'-corrected' if corrected else ''}.ms"
    sm = observation(path, profile)
    before = snapshot(path)
    if corrected:
        sm.close()
        split_later_field(path)
        sm = simulator()
        assert sm.openfromms(str(path))
    assert sm.setoptions(ftmachine="sd" if operation.startswith("sd_") else "mosaic" if operation == "mosaic" else "ft",
        gridfunction="PB" if operation == "sd_pb" else "BOX" if operation.startswith("sd_") else "SF")
    assert sm.setvp(dovp=operation.endswith("beam"), dosquint=False)
    inputs = root / "inputs"
    arguments = {"complist" if operation.startswith("components") else "imagename":
        str(inputs / ("sky.cl" if operation.startswith("components") else "sd.image" if operation.startswith("sd_") else "sky.image"))}
    assert sm.predict(**arguments, incremental=False)
    case = dict(name=name, profile=profile, operation=operation, native_created=True,
        returned=True, before=before, after=snapshot(path), eop=earth_orientation(before),
        cleanup="not returned")
    if operation.startswith("sd_"):
        case["native_single_dish"] = True
    output = root / f"{name}{'-corrected' if corrected else ''}.json.gz"
    write(output, dict(casa_version=version_string(), cases=[case]))
    sm.close()
    case["cleanup"] = "returned"
    write(output, dict(casa_version=version_string(), cases=[case]))
    return case


def run(root, name, corrected=False):
    if not name.endswith("mosaic"):
        return worker(root, name, corrected)
    suffix = "-corrected" if corrected else ""
    log = root / f"{name}{suffix}.log"
    command = [sys.executable, __file__, "--out", str(root), "--fixture", str(root/"unused.json.gz"), "--worker", name]
    if corrected:
        command.append("--corrected")
    with log.open("w") as stream:
        process = subprocess.run(command, stdout=stream, stderr=subprocess.STDOUT)
    with gzip.open(root / f"{name}{suffix}.json.gz", "rt") as stream:
        case = json.load(stream)["cases"][0]
    assert not process.returncode or (case["cleanup"] == "not returned"
        and "Pure virtual function called" in log.read_text()), (name, log)
    return case


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--worker", choices=NAMES)
    parser.add_argument("--corrected", action="store_true")
    parser.add_argument("--only", nargs="+", choices=NAMES)
    args = parser.parse_args()
    assert version_string() == "6.7.6.14"
    root = args.out.resolve()
    if args.worker:
        worker(root, args.worker, args.corrected)
        return
    inputs = root / "inputs"
    inputs.mkdir(parents=True)
    model(inputs / "sky.image", "a")
    ia = image()
    ia.open(str(inputs / "sky.image"))
    cs = ia.coordsys()
    ia.done()
    pixel = cs.referencepixel()["numeric"]
    pixel[:2] += 96
    cs.setreferencepixel(pixel)
    x, y = np.indices((256, 256))
    values = (.2 + .8*np.exp(-((x-128.)**2+(y-128.)**2)/160.)).astype(np.float32)
    ia.fromarray(outfile=str(inputs / "sd.image"), pixels=values[:, :, None, None], csys=cs.torecord())
    ia.setbrightnessunit("Jy/pixel")
    ia.done()
    cs.done()
    cl = componentlist()
    for flux, direction in ((1., CENTRE), (.4, OFFSET)):
        assert cl.addcomponent(flux=flux, freq="230GHz",
            dir=f"J2000 {direction[0]}rad {direction[1]}rad")
    cl.rename(str(inputs / "sky.cl"))
    cl.done()
    cases = []
    for name in NAMES:
        if args.only and name not in args.only:
            continue
        case = run(root, name)
        if name.startswith("changing_"):
            control = run(root, name, True)
            rows = copy.deepcopy(case["after"]["rows"])
            for row, reference in zip(rows, control["after"]["rows"]):
                for column in ("data", "model_data", "corrected_data"):
                    row[column] = reference[column]
            case["pointing_cache_corrected_rows"] = rows
        cases.append(case)
        write(args.fixture, dict(casa_version=version_string(),
            recipe="tools/simulation/sm_fresh_pointings_oracle.py", cases=cases))
        print(name, "passed", flush=True)


if __name__ == "__main__":
    main()
