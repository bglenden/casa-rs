#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA controls for saved-mosaic beams, standard fallback and feed mounts."""
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
from sm_squint_oracle import observation


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--mosaic-worker")
    args = parser.parse_args()
    root, data = args.out.resolve(), args.data.resolve()
    assert version_string() == "6.7.6.14"
    root.mkdir(parents=True, exist_ok=True)
    if args.mosaic_worker:
        beam, mode = args.mosaic_worker.rsplit("_", 1)
        mosaic_case(root, data, beam, mode == "vp")
        return
    mosaics = []
    for beam in ("gaussian_pb", "airy"):
        for dovp in (False, True):
            mode = "vp" if dovp else "intrinsic"
            name = f"{beam}_mosaic_{mode}"
            log = root / f"{name}.log"
            with log.open("w") as stream:
                result = subprocess.run([sys.executable, __file__, "--out", str(root),
                    "--data", str(data), "--mosaic-worker", f"{beam}_{mode}"],
                    stdout=stream, stderr=subprocess.STDOUT)
            with gzip.open(root / f"{name}.json.gz", "rt") as stream:
                case = json.load(stream)["cases"][0]
            assert not result.returncode or (case["cleanup"] == "not returned"
                and "Pure virtual function called" in log.read_text()), (name, log)
            mosaics.append(case)
            write(root / "mosaics.json.gz", dict(casa_version=version_string(), cases=mosaics))
            print(name, "passed", flush=True)
    vp = vpmanager()
    assert vp.reset()
    assert vp.setpbgauss(telescope="ALMA", halfwidth="8arcsec", maxrad="60arcsec", reffreq="150GHz")
    assert vp.setuserdefault(vplistnum=-1, telescope="VLA")
    assert vp.saveastable(str(root / "standard.vp"))
    cases = []
    for name, mount, saved, replace in (("vla_saved_standard", "alt-az", True, False),
            ("vla_equatorial", "equatorial", False, False),
            ("vla_replace_squint", "alt-az", False, True)):
        path = root / f"{name}.ms"
        sm = observation(path, "VLA", 1.4e9, mount)
        before = snapshot(path)
        assert sm.setvp(dovp=True, dosquint=True, parangleinc="360deg",
            usedefaultvp=not saved, vptable=str(root / "standard.vp") if saved else "")
        component = str(data / "sm_squint/vla.cl")
        assert sm.predict(complist=component, incremental=False)
        first = snapshot(path)
        if replace:
            assert sm.setvp(dovp=True, dosquint=False)
            assert sm.predict(complist=component, incremental=False)
        sm.close()
        case = dict(name=name, telescope="VLA", frequency_hz=1.4e9, mount=mount,
            squint=True, increment_deg=360., native_created=True, saved_standard=saved,
            replace_squint=replace, before=before, after=snapshot(path))
        if replace:
            case["before_replace"] = first
        cases.append(case)
        write(root / "squint-integrations.json.gz", dict(casa_version=version_string(), cases=cases))
        print(name, "passed", flush=True)


def mosaic_case(root, data, beam, dovp):
    name = f"{beam}_mosaic_{'vp' if dovp else 'intrinsic'}"
    path = root / f"{name}.ms"
    shutil.copytree(data / "sm_image_layouts/base.ms", path,
        ignore=shutil.ignore_patterns("table.lock"))
    before = snapshot(path)
    sm = simulator()
    assert sm.openfromms(str(path))
    assert sm.setoptions(ftmachine="mosaic")
    assert sm.setvp(dovp=dovp, usedefaultvp=False,
        vptable=str(data / f"sm_voltage_patterns/{beam}.vp"), dosquint=False)
    assert sm.setdata(fieldid=[0])
    assert sm.predict(imagename=str(data / "sm_voltage_patterns/sky.image"), incremental=False)
    case = dict(name=name, beam=beam, operation="mosaic", dovp=dovp,
        before=before, after=snapshot(path))
    case["cleanup"] = "not returned"
    output = root / f"{name}.json.gz"
    write(output, dict(casa_version=version_string(), cases=[case]))
    # The tool object may abort at final cleanup; complete references
    # are saved before close, as for the other mosaic oracles.
    sm.close()
    case["cleanup"] = "returned"
    write(output, dict(casa_version=version_string(), cases=[case]))


if __name__ == "__main__":
    main()
