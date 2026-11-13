#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Actual CASA synthetic component visibilities (SIM-08), without plotting."""
import argparse
from pathlib import Path
import shutil

import numpy as np

from casatasks import version_string
from casatools import componentlist, measures, simulator

from sm_apply_time_oracle import write
from sm_existing_append_oracle import snapshot
from sm_multiple_models_oracle import CENTRE, make_session
from sm_limb_disk_reference import add_limb_references
from sm_image_layouts_oracle import EXTRA_FRAMES, earth_orientation


CASES = [dict(name=f"limb_{n:g}", shape="limbdarkeneddisk", exponent=n)
         for n in (0., .05, 1., 2.5, 8.)]
CASES += [dict(name="point_control", shape="point"),
          dict(name="gaussian_control", shape="gaussian"),
          dict(name="limb_incremental", shape="limbdarkeneddisk", exponent=.7,
               incremental=True)]
CASES += [dict(name="point_distance", shape="point", distance=1e6),
          dict(name="gaussian_distance", shape="gaussian", distance=-1e6),
          dict(name="limb_distance", shape="limbdarkeneddisk", exponent=.05, distance=1e6)]
CASES += [dict(name=f"frame_{frame.lower()}", shape="point", frame=frame)
          for frame in ("B1950", "GALACTIC", "ICRS", *EXTRA_FRAMES)]
CASES += [dict(name=f"frame_{frame.lower()}_gaussian", shape="gaussian", frame=frame)
          for frame in ("B1950", "GALACTIC", "HADEC")]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--only", nargs="+", choices=[case["name"] for case in CASES])
    parser.add_argument("--base", type=Path,
        default=Path(__file__).resolve().parents[2] /
        "crates/casa-simulation/tests/data/sm_image_layouts/base.ms")
    args = parser.parse_args()
    assert version_string() == "6.7.6.14"
    root = args.out.resolve()
    inputs = root / "inputs"
    inputs.mkdir(parents=True)
    cases = []
    frame_measures = measures()
    location = frame_measures.observatory("ALMA")
    if any("frame" in spec and (not args.only or spec["name"] in args.only) for spec in CASES):
        base = snapshot(args.base)
        epoch = base["rows"][0]["time"]/86400.
        frame_measures.doframe(frame_measures.epoch("UTC", f"{epoch}d"))
        frame_measures.doframe(location)
    for spec in CASES:
        if args.only and spec["name"] not in args.only:
            continue
        name = spec["name"]
        component = inputs / f"{name}.cl"
        cl = componentlist()
        arguments = dict(flux=[1., .15, -.08, .03], fluxunit="Jy",
            polarization="Stokes", dir=f"J2000 {CENTRE[0]+6e-6}rad {CENTRE[1]-4e-6}rad",
            shape="gaussian" if "exponent" in spec else spec["shape"],
            freq="150GHz", spectrumtype="spectral index",
            index=[-.7, .1, 120., -.02])
        if spec["shape"] != "point":
            arguments.update(majoraxis="1.8arcsec", minoraxis="0.7arcsec",
                positionangle="37deg")
        assert cl.addcomponent(**arguments)
        if "exponent" in spec:
            # The tool's setshape refuses this shape, while its record/table
            # reader and the simulator's C++ predictor implement it.
            record = cl.torecord()
            record["component0"]["shape"]["type"] = "Limbdarkeneddisk"
            record["component0"]["optionalParameters"] = [spec["exponent"]]
            cl.done()
            cl = componentlist()
            assert cl.fromrecord(record)
        if "frame" in spec:
            record = cl.torecord()
            direction = frame_measures.direction("J2000", f"{CENTRE[0]+6e-6}rad", f"{CENTRE[1]-4e-6}rad")
            record["component0"]["shape"]["direction"] = frame_measures.measure(direction, spec["frame"])
            cl.done()
            cl = componentlist()
            assert cl.fromrecord(record)
        assert cl.getcomponent(0)["shape"]["type"].lower() == spec["shape"]
        # addcomponent's index argument keeps only a scalar index. For
        # polarized signals use the explicit four-index method; the scalar
        # CASA matrix sampler otherwise reads past its coefficient vector.
        assert cl.setstokesspectrum(0, "spectral index", index=[-.7, .1, 120., -.02])
        assert len(cl.getcomponent(0)["spectrum"]["stokesindex"]) == 4
        cl.rename(str(component))
        cl.done()
        path = root / f"{name}.ms"
        if "distance" in spec:
            sm = make_session(path, stokes="XX XY YX YY")
        else:
            shutil.copytree(args.base, path, ignore=shutil.ignore_patterns("table.lock"))
            sm = simulator()
            assert sm.openfromms(str(path))
        before = snapshot(path)
        if "frame" in spec:
            assert sm.setoptions(location=location)
        if "distance" in spec:
            direction = measures().direction("J2000", f"{CENTRE[0]}rad", f"{CENTRE[1]}rad")
            assert sm.setfield(sourcename="last", sourcedirection=direction,
                distance=f"{spec['distance']}m")
            # Preserve the focus state: fieldid selection has a separate
            # documented nField/distance-vector bug on the image path.
            assert sm.setdata(fieldid=[], msselect="FIELD_ID == 0")
        else:
            assert sm.setdata(fieldid=[0])
        assert sm.predict(complist=str(component), incremental=False)
        if spec.get("incremental"):
            assert sm.predict(complist=str(component), incremental=True)
        sm.close()
        case = dict(name=name, spec=spec, returned=True, before=before, after=snapshot(path),
            native_created="distance" in spec)
        if "frame" in spec:
            itrf = frame_measures.measure(location, "ITRF")
            lon, lat, radius = [itrf[f"m{i}"]["value"] for i in range(3)]
            case["location_itrf_m"] = [radius*np.cos(lat)*np.cos(lon),
                radius*np.cos(lat)*np.sin(lon), radius*np.sin(lat)]
            case["eop"] = earth_orientation(before)
        cases.append(case)
        write(args.fixture, dict(casa_version=version_string(), cases=cases))
        print(name, "passed", flush=True)
    if any(case["name"] == "point_control" for case in cases) and any(
            case["name"] == "limb_0.05" for case in cases):
        add_limb_references(cases)
    write(args.fixture, dict(casa_version=version_string(), cases=cases))


if __name__ == "__main__":
    main()
