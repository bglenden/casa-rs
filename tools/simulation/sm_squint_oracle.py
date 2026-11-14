#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA VLA/EVLA component beams, squint and parallactic-angle refresh."""
import argparse
from pathlib import Path

import numpy as np

from casatasks import version_string
from casatools import componentlist, measures, simulator, table

from sm_apply_time_oracle import write
from sm_existing_append_oracle import snapshot
from sm_multiple_models_oracle import CENTRE


def observation(path, telescope, frequency, mount="alt-az", *, integration=10., starts=(0., 3600.), duration=20.):
    sm, me = simulator(), measures()
    assert sm.open(str(path))
    centre = [-1601185., -5041977., 3554875.]
    xyz = [[centre[axis] + step*index for index in range(8)]
        for axis, step in enumerate((40., -12., 24.))]
    # Keep the 25 m dishes unshadowed: shadow flags have their own
    # documented simulator/flagger convention, independent of beam squint.
    assert sm.setconfig(telescopename=telescope, x=xyz[0], y=xyz[1], z=xyz[2],
        dishdiameter=[25.], offset=[0.], mount=[mount], antname=["VA"],
        padname=["A"], coordsystem="global", referencelocation=me.observatory("VLA"))
    assert sm.setspwindow(spwname="beam", freq=f"{frequency}Hz",
        deltafreq=f"{frequency*.01}Hz", freqresolution=f"{frequency*.01}Hz",
        refcode="TOPO", nchannels=4, stokes="RR RL LR LL")
    assert sm.setfeed(mode="perfect R L")
    assert sm.setfield(sourcename="target", sourcedirection=me.direction(
        "J2000", f"{CENTRE[0]}rad", f"{CENTRE[1]}rad"))
    assert sm.settimes(integrationtime=f"{integration}s", usehourangle=False,
        referencetime=me.epoch("UTC", "2024/06/01/00:00:00"))
    for start in starts:
        assert sm.observe(sourcename="target", spwname="beam",
            starttime=f"{start-37}s", stoptime=f"{start+duration-37}s")
    me.done()
    return sm


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    args = parser.parse_args()
    assert version_string() == "6.7.6.14"
    root = args.out.resolve()
    inputs = root / "inputs"
    inputs.mkdir(parents=True)
    cases = []
    for telescope, frequency, offset in (("VLA", 1.4e9, .005), ("EVLA", 14.5e9, .0004)):
        component = inputs / f"{telescope.lower()}.cl"
        cl = componentlist()
        for scale, shift in ((1., 0.), (.7, offset)):
            # Real positive circular products isolate the squint from CASA's
            # documented full-spectral complex-phase/sign defect.
            assert cl.addcomponent(flux=[scale, .1*scale, 0., .03*scale],
                fluxunit="Jy", polarization="Stokes", freq=f"{frequency}Hz",
                dir=f"J2000 {CENTRE[0]+shift}rad {CENTRE[1]-shift/2}rad")
        cl.rename(str(component))
        cl.done()
        for mode, squint, increment in (("off", False, 360.),
                ("on", True, 360.), ("refresh", True, .01)):
            name = f"{telescope.lower()}_{mode}"
            path = root / f"{name}.ms"
            sm = observation(path, telescope, frequency)
            before = snapshot(path)
            assert sm.setvp(dovp=True, dosquint=squint, parangleinc=f"{increment}deg")
            assert sm.predict(complist=str(component), incremental=False)
            sm.close()
            tb = table()
            tb.open(str(path / "SPECTRAL_WINDOW"))
            frequencies = tb.getcell("CHAN_FREQ", 0).tolist()
            tb.close()
            cases.append(dict(name=name, telescope=telescope, frequency_hz=frequency,
                frequencies_hz=frequencies,
                squint=squint, increment_deg=increment, native_created=True,
                before=before, after=snapshot(path)))
            write(args.fixture, dict(casa_version=version_string(),
                recipe="tools/simulation/sm_squint_oracle.py", cases=cases))
            print(name, "passed", flush=True)
    for telescope in ("vla", "evla"):
        signals = {mode: np.asarray([row["data"] for row in next(case for case in cases
            if case["name"] == f"{telescope}_{mode}")["after"]["rows"]])
            for mode in ("off", "on", "refresh")}
        for left, right in (("off", "on"), ("on", "refresh")):
            contrast = np.linalg.norm(signals[left]-signals[right])/np.linalg.norm(signals[right])
            # CASA's EVLA profile reports No Beam Squint: the accepted flag
            # is a no-op. VLA controls must exercise a measurable change.
            assert (contrast > 1e-3 if telescope == "vla" else contrast < 1e-3), (
                telescope, left, right, contrast)
            print(telescope, left, right, "contrast", contrast, flush=True)


if __name__ == "__main__":
    main()
