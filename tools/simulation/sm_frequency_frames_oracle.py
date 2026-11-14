#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA simulation with distinct channel/reference-frequency frame metadata."""
import argparse
from pathlib import Path
import shutil

import numpy as np
from casatasks import version_string
from casatools import simulator, table

from sm_apply_time_oracle import write
from sm_existing_append_oracle import snapshot


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    args = parser.parse_args()
    assert version_string() == "6.7.6.14"
    root = args.out.resolve()
    source = Path(__file__).resolve().parents[2] / "crates/casa-simulation/tests/data/sm_irregular_channels"
    inputs = root / "inputs"
    inputs.mkdir(parents=True)
    for name in ("spectrum.cl", "spectrum.image", "bandpass.cal", "gspline.cal"):
        shutil.copytree(source / name, inputs / name, ignore=shutil.ignore_patterns("table.lock"))
    cases = []
    for kind in ("fixed", "mapped"):
        base = inputs / f"{kind}.ms"
        shutil.copytree(source / "ascending.ms", base, ignore=shutil.ignore_patterns("table.lock"))
        tb = table()
        tb.open(str(base / "SPECTRAL_WINDOW"), nomodify=False)
        if kind == "fixed":
            descriptor = dict(type="frequency", Ref="BARY")
        else:
            tb.addcols({"REFERENCE_FREQUENCY_REF": tb.getcoldesc("MEAS_FREQ_REF")})
            tb.putcol("REFERENCE_FREQUENCY_REF", np.full(tb.nrows(), 77, np.int32))
            descriptor = dict(type="frequency", VarRefCol="REFERENCE_FREQUENCY_REF",
                              TabRefTypes=np.asarray(["BARY", "TOPO"]),
                              TabRefCodes=np.asarray([77, 78], np.int32))
        tb.putcolkeyword("REF_FREQUENCY", "MEASINFO", descriptor)
        frequencies = tb.getcell("CHAN_FREQ", 0).tolist()
        tb.close()
        for operation in ("components", "beam_components", "image", "bandpass", "gspline"):
            name = f"{kind}_{operation}"
            path = root / f"{name}.ms"
            shutil.copytree(base, path, ignore=shutil.ignore_patterns("table.lock"))
            sm = simulator()
            assert sm.openfromms(str(path)), name
            assert sm.setdata(fieldid=[0])
            if operation in ("bandpass", "gspline"):
                interp = "nearest,linear" if operation == "bandpass" else "linear"
                assert sm.setapply(table=str(inputs / f"{operation}.cal"), interp=interp, calwt=False)
                assert sm.corrupt()
            elif operation.endswith("components"):
                if operation == "beam_components":
                    assert sm.setvp(dovp=True, dosquint=False)
                assert sm.predict(complist=str(inputs / "spectrum.cl"))
            else:
                assert sm.predict(imagename=str(inputs / "spectrum.image"))
            sm.close()
            cases.append(dict(name=name, axis=kind, operation=operation,
                              frequencies_hz=frequencies, after=snapshot(path)))
            write(args.fixture, dict(casa_version=version_string(), cases=cases))
            print(name, len(cases[-1]["after"]["rows"]), flush=True)


if __name__ == "__main__":
    main()
