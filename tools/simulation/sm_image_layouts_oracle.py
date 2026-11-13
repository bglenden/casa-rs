#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""One actual CASA direct-image prediction layout control per invocation."""
import argparse
from pathlib import Path
import shutil

import numpy as np
from casatasks import version_string
from casatools import coordsys, image, regionmanager, simulator

from sm_apply_time_oracle import write
from sm_existing_append_oracle import snapshot
from sm_multiple_models_oracle import CENTRE


def make_model(path, kind):
    polarized = kind.startswith("iquv")
    cs = coordsys().newcoordsys(direction=True, spectral=True, stokes=["I", "Q", "U", "V"] if polarized else ["I"])
    cs.setunits(["rad", "rad", "", "Hz"])
    cs.setreferencecode("J2000", type="direction")
    cs.setreferencevalue(CENTRE, type="direction")
    cs.setincrement([-2e-6, 2e-6], type="direction")
    cs.setreferencepixel([32, 32, 0, 0])
    cs.setreferencecode("LSRK", type="spectral")
    cs.setreferencevalue([100e9], type="spectral")
    cs.setincrement([200e9 / 3], type="spectral")
    pixels = np.zeros((64, 64, 4 if polarized else 1, 4), dtype=np.float32)
    for channel in range(4):
        pixels[32, 32, 0, channel] = channel + 1.
        if polarized:
            pixels[32, 32, 1:, channel] = [.2 * (channel + 1.), -.1 * (channel + 1.), .05 * (channel + 1.)]
        if kind == "iquv_beam":
            pixels[35, 30, :, channel] = np.asarray([.4, .1, -.02, .03]) * (channel + 1.)
    ia = image()
    raw = path.with_name("canonical.image")
    ia.fromarray(outfile=str(raw), pixels=pixels, csys=cs.torecord())
    ia.setbrightnessunit("Jy/pixel")
    if kind in ("spectral_before_stokes", "direction_last"):
        result = ia.transpose(outfile=str(path), order="0132" if kind == "spectral_before_stokes" else "3201")
        result.done()
    elif kind == "no_stokes":
        result = ia.subimage(outfile=str(path), dropdeg=True)
        result.done()
    elif kind == "direction_only":
        region = regionmanager().box(blc=[0, 0, 0, 0], trc=[63, 63, 0, 0])
        result = ia.subimage(outfile=str(path), region=region, dropdeg=True)
        result.done()
    else:
        ia.done()
        shutil.copytree(raw, path)
    ia.done()
    cs.done()
    shutil.rmtree(raw)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--case", required=True, choices=("canonical", "spectral_before_stokes", "direction_last", "no_stokes", "direction_only", "iquv", "iquv_beam"))
    args = parser.parse_args()
    assert version_string() == "6.7.6.14"
    root = args.out.resolve()
    inputs = root / "inputs"
    inputs.mkdir(parents=True)
    source = Path(__file__).resolve().parents[2] / "crates/casa-simulation/tests/data/sm_irregular_channels/ascending.ms"
    shutil.copytree(source, inputs / "base.ms", ignore=shutil.ignore_patterns("table.lock"))
    model = inputs / "model.image"
    make_model(model, args.case)
    before = snapshot(inputs / "base.ms")
    path = root / "predicted.ms"
    shutil.copytree(inputs / "base.ms", path, ignore=shutil.ignore_patterns("table.lock"))
    run_model = root / "run.image"
    shutil.copytree(model, run_model, ignore=shutil.ignore_patterns("table.lock"))
    sm = simulator()
    assert sm.openfromms(str(path))
    assert sm.setdata(fieldid=[0])
    if args.case == "iquv_beam":
        assert sm.setvp(dovp=True, dosquint=False)
    returned = bool(sm.predict(imagename=str(run_model)))
    sm.close()
    case = dict(name=args.case, operation="image", returned=returned, beam=args.case == "iquv_beam", before=before, after=snapshot(path))
    write(args.fixture, dict(casa_version=version_string(), cases=[case]))
    print(args.case, "returned", returned, flush=True)


if __name__ == "__main__":
    main()
