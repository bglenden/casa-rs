#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA simulation prediction/corruption on stored irregular channel grids."""
import argparse
from pathlib import Path
import shutil

import numpy as np
from casatasks import gencal, version_string
from casatools import componentlist, coordsys, image, simulator, table

from sm_apply_parameters_oracle import initialize
from sm_apply_time_oracle import write
from sm_existing_append_oracle import snapshot
from sm_multiple_models_oracle import CENTRE, make_session

ASCENDING = np.asarray([100., 150., 210., 300.]) * 1e9
AXES = {"ascending": ASCENDING, "descending": ASCENDING[::-1],
        "unordered": ASCENDING[[1, 3, 0, 2]]}


def model(path):
    cs = coordsys().newcoordsys(direction=True, spectral=True, stokes="I")
    cs.setunits(["rad", "rad", "", "Hz"])
    cs.setreferencecode("J2000", type="direction")
    cs.setreferencevalue(CENTRE, type="direction")
    cs.setincrement([-2e-6, 2e-6], type="direction")
    cs.setreferencepixel([32, 32, 0, 0])
    cs.setreferencecode("LSRK", type="spectral")
    cs.setreferencevalue([100e9], type="spectral")
    cs.setincrement([200e9 / 3], type="spectral")
    pixels = np.zeros((64, 64, 1, 4), dtype=np.float32)
    pixels[32, 32, 0, :] = [1., 2., 3., 4.]
    ia = image()
    ia.fromarray(outfile=str(path), pixels=pixels, csys=cs.torecord())
    ia.setbrightnessunit("Jy/pixel")
    ia.done()
    cs.done()


def bandpass(vis, path):
    gencal(vis=str(vis), caltable=str(path), caltype="amp", parameter=[1.])
    tb = table()
    tb.open(str(path), nomodify=False)
    for column in ("CPARAM", "FLAG", "PARAMERR", "SNR", "WEIGHT"):
        if column not in tb.colnames() or not tb.iscelldefined(column, 0):
            continue
        values = np.repeat(tb.getcol(column), 4, axis=1)
        if column == "CPARAM":
            values[:] = np.asarray([1., 2., 4., 8.])[None, :, None]
        tb.putcol(column, values)
    tb.putkeyword("VisCal", "B Jones")
    info = tb.info()
    info["subType"] = "B Jones"
    tb.putinfo(info)
    tb.close()
    source = table()
    source.open(str(vis / "SPECTRAL_WINDOW"))
    tb.open(str(path / "SPECTRAL_WINDOW"), nomodify=False)
    for column in tb.colnames():
        if column in source.colnames() and source.iscelldefined(column, 0):
            tb.putcell(column, 0, source.getcell(column, 0))
    tb.putcell("CHAN_FREQ", 0, ASCENDING)
    tb.close()
    source.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    args = parser.parse_args()
    assert version_string() == "6.7.6.14"
    root = args.out.resolve()
    inputs = root / "inputs"
    inputs.mkdir(parents=True)
    cl = componentlist()
    assert cl.addcomponent(dir=f"J2000 {CENTRE[0] + 3e-6}rad {CENTRE[1] - 4e-6}rad", flux=1.,
                           freq="100GHz", spectrumtype="spectral index", index=-1.)
    cl.rename(str(inputs / "spectrum.cl"))
    cl.done()
    model(inputs / "spectrum.image")
    legacy = Path(__file__).resolve().parents[2] / "crates/casa-simulation/tests/data/sm_polynomial/GSPLINE"
    shutil.copytree(legacy, inputs / "gspline.cal", ignore=shutil.ignore_patterns("table.lock"))
    cases = []
    for axis, frequencies in AXES.items():
        base = inputs / f"{axis}.ms"
        sm = make_session(base, stokes="XX XY YX YY")
        sm.close()
        initialize(base)
        tb = table()
        tb.open(str(base / "SPECTRAL_WINDOW"), nomodify=False)
        tb.putcell("CHAN_FREQ", 0, frequencies)
        tb.putcell("CHAN_WIDTH", 0, np.asarray([1., 2., 3., 4.]) * 1e9)
        tb.putcell("EFFECTIVE_BW", 0, np.asarray([1., 2., 3., 4.]) * 1e9)
        tb.putcell("RESOLUTION", 0, np.asarray([1., 2., 3., 4.]) * 1e9)
        tb.putcell("TOTAL_BANDWIDTH", 0, 10e9)
        tb.close()
        if axis == "ascending":
            bandpass(base, inputs / "bandpass.cal")
        for operation in ("components", "image", "bandpass", "gspline"):
            name = f"{axis}_{operation}"
            path = root / f"{name}.ms"
            shutil.copytree(base, path, ignore=shutil.ignore_patterns("table.lock"))
            sm = simulator()
            assert sm.openfromms(str(path))
            assert sm.setdata(fieldid=[0])
            if operation in ("bandpass", "gspline"):
                interpolation = "nearest,linear" if operation == "bandpass" else "linear"
                assert sm.setapply(table=str(inputs / f"{operation}.cal"), interp=interpolation, calwt=False)
                assert sm.corrupt()
            elif operation == "components":
                assert sm.predict(complist=str(inputs / "spectrum.cl")), name
            else:
                assert sm.predict(imagename=str(inputs / "spectrum.image")), name
            sm.close()
            after = snapshot(path)
            if operation == "components":
                row = next(row for row in after["rows"] if row["field"] == 0)
                amplitudes = np.asarray([abs(complex(*row["data"][4 * channel])) for channel in range(4)])
                assert np.ptp(amplitudes) > .5, (name, amplitudes)
                assert np.allclose(amplitudes, 100e9 / frequencies, rtol=1e-3, atol=0.), name
            cases.append(dict(name=name, axis=axis, operation=operation,
                              frequencies_hz=frequencies.tolist(), after=after))
            write(args.fixture, dict(casa_version=version_string(), cases=cases))
            print(name, len(cases[-1]["after"]["rows"]), flush=True)


if __name__ == "__main__":
    main()
