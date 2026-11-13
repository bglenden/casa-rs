#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA simulation image/component signals through saved voltage-pattern tables."""
import argparse
import copy
from pathlib import Path
import shutil

from casatasks import version_string
from casatools import componentlist, image, simulator, vpmanager

from sm_apply_time_oracle import write
from sm_existing_append_oracle import snapshot
from sm_multiple_models_oracle import CENTRE, model

FLUX = [1., .1, .02, .03]


def correct_component_phase(case):
    """Undo the documented BeamSkyJones sign defect's complex-phase form.

    Both input points have the same original correlation flux. CASA's
    full-spectral beam multiplies each by its original phase a second time:
    flux/abs(flux). Remove that factor, retaining raw CASA rows separately.
    """
    i, q, u, v = FLUX
    correlations = [complex(i+q), complex(u, v), complex(u, -v), complex(i-q)]
    phases = [value/abs(value) if value else 1. for value in correlations]
    rows = copy.deepcopy(case["after"]["rows"])
    for row in rows:
        if row["field"] == 0:
            for column in ("data", "corrected_data"):
                for index, pair in enumerate(row[column]):
                    value = complex(*pair)/phases[index % 4]
                    row[column][index] = [value.real, value.imag]
    case["corrected_rows"] = rows
    case["casa_defect"] = "BeamSkyJones full-spectral sampling multiplies a component's original correlation phase twice (the documented negative-flux defect). Both points have identical original flux; dividing raw predicted correlations by their original unit phase corrects the defect."


def make_inputs(inputs):
    model(inputs / "sky.image", "a")
    ia = image()
    ia.open(str(inputs / "sky.image"))
    cs = ia.coordsys()
    # The existing-MS control spans 100--300 GHz. A broad one-plane model
    # must overlap it; a narrow 230 GHz image produces only blank DATA.
    cs.setreferencevalue([200e9], type="spectral")
    cs.setincrement([400e9], type="spectral")
    assert ia.setcoordsys(cs.torecord())
    cs.done()
    ia.done()
    cl = componentlist()
    for index, offset in enumerate((0., 6e-5)):
        assert cl.addcomponent(flux=FLUX, fluxunit="Jy",
            polarization="Stokes", dir=f"J2000 {CENTRE[0]+offset}rad {CENTRE[1]-offset/2}rad",
            freq="150GHz", shape="point")
        assert cl.setstokesspectrum(index, "spectral index", reffreq="150GHz",
            index=[-.7, .1, 0., -.02])
    cl.rename(str(inputs / "sky.cl"))
    cl.done()
    vp = vpmanager()
    for name in ("gaussian_pb", "gaussian_vp", "airy"):
        assert vp.reset()
        if name == "airy":
            record = vp.setpbairy(telescope="ALMA", dishdiam="9m", blockagediam="0.6m",
                maxrad="60arcsec", reffreq="150GHz")
        else:
            record = vp.setpbgauss(telescope="ALMA", halfwidth="8arcsec",
                maxrad="60arcsec", reffreq="150GHz",
                isthispb="PB" if name.endswith("pb") else "VP")
        assert record and record["name"] == ("AIRY" if name == "airy" else "GAUSS")
        assert vp.saveastable(str(inputs / f"{name}.vp"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--base", type=Path, required=True)
    args = parser.parse_args()
    assert version_string() == "6.7.6.14"
    root = args.out.resolve()
    inputs = root / "inputs"
    inputs.mkdir(parents=True)
    make_inputs(inputs)
    cases = []
    for beam in ("gaussian_pb", "gaussian_vp", "airy", "default_override"):
        for operation in ("image", "components"):
            name = f"{beam}_{operation}"
            path = root / f"{name}.ms"
            shutil.copytree(args.base, path, ignore=shutil.ignore_patterns("table.lock"))
            before = snapshot(path)
            sm = simulator()
            assert sm.openfromms(str(path))
            table_name = "gaussian_pb" if beam == "default_override" else beam
            assert sm.setvp(dovp=True, usedefaultvp=beam == "default_override",
                vptable=str(inputs / f"{table_name}.vp"), dosquint=False)
            assert sm.setdata(fieldid=[0])
            arguments = {"imagename" if operation == "image" else "complist":
                str(inputs / ("sky.image" if operation == "image" else "sky.cl"))}
            assert sm.predict(**arguments, incremental=False)
            sm.close()
            after = snapshot(path)
            assert any(value != 0. for row in after["rows"] if row["field"] == 0
                for pair in row["data"] for value in pair), name
            cases.append(dict(name=name, beam=beam, operation=operation,
                before=before, after=after))
            if operation == "components":
                correct_component_phase(cases[-1])
            write(args.fixture, dict(casa_version=version_string(),
                recipe="tools/simulation/sm_voltage_patterns_oracle.py", cases=cases))
            print(name, "passed", flush=True)


if __name__ == "__main__":
    main()
