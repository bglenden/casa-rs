#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Saved Jones-image inputs applied to simulated sky signals."""
import argparse
from pathlib import Path
import shutil

import numpy as np
from casatasks import version_string
from casatools import image, simulator, vpmanager

from sm_apply_time_oracle import write
from sm_existing_append_oracle import snapshot
from sm_voltage_patterns_oracle import correct_component_phase


def beam_image(path, values, complex_image=False):
    ia = image()
    assert ia.fromshape(str(path), shape=list(values.shape), type="c" if complex_image else "f")
    cs = ia.coordsys()
    cs.setstokes(["XX", "XY", "YX", "YY"])
    cs.setreferencecode("AZEL", type="direction", adjust=False)
    cs.setreferencevalue([0., 0.], type="direction")
    cs.setreferencepixel([48., 48.], type="direction")
    cs.setincrement([2e-6, 2e-6], type="direction")
    cs.setreferencevalue([150e9], type="spectral")
    assert ia.setcoordsys(cs.torecord())
    cs.done()
    assert ia.putchunk(values)
    ia.done()


def inputs_for(inputs):
    x, y = np.indices((96, 96))
    profile = np.exp(-((x-48.)/18.)**2-((y-48.)/27.)**2).astype(np.float32)
    re = np.zeros((96, 96, 4, 1), dtype=np.float32)
    im = np.zeros_like(re)
    for pol in (0, 3):
        re[:, :, pol, 0] = .9 * profile
        im[:, :, pol, 0] = .2 * profile
    beam_image(inputs / "real.image", re)
    beam_image(inputs / "imaginary.image", im)
    beam_image(inputs / "complex.image", re + 1j*im, True)
    re[:, :, 3, 0] = .7 * profile
    re[:, :, 1, 0] = .03 * profile
    re[:, :, 2, 0] = -.02 * profile
    im[:, :, 1, 0] = .02 * profile
    im[:, :, 2, 0] = .01 * profile
    beam_image(inputs / "jones.image", re + 1j*im, True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--sky", type=Path, required=True)
    args = parser.parse_args()
    assert version_string() == "6.7.6.14"
    root = args.out.resolve()
    inputs = root / "inputs"
    inputs.mkdir(parents=True)
    inputs_for(inputs)
    cases = []
    for beam, parameters in (
            ("real", dict(realimage=str(inputs / "real.image"))),
            ("pair", dict(realimage=str(inputs / "real.image"), imagimage=str(inputs / "imaginary.image"))),
            ("complex", dict(compleximage=str(inputs / "complex.image"))),
            ("jones", dict(compleximage=str(inputs / "jones.image")))):
        vp = vpmanager()
        assert vp.reset()
        assert vp.setpbimage(telescope="ALMA", **parameters)
        voltage_table = inputs / f"{beam}.vp"
        assert vp.saveastable(str(voltage_table))
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
            assert any(v for row in case["after"]["rows"] if row["field"] == 0
                for pair in row["data"] for v in pair), name
            if operation == "components":
                correct_component_phase(case)
            cases.append(case)
            write(args.fixture, dict(casa_version=version_string(),
                recipe="tools/simulation/sm_image_beams_oracle.py", cases=cases))
            print(name, "passed", flush=True)


if __name__ == "__main__":
    main()
