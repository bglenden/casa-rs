#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Apply supplied BPOLY/GSPLINE tables to simulated DATA (SIM-16).

Run under CASA 6.7.6.14's Python with --out SCRATCH --fixture FIXTURE.json.gz.
The legacy input tables in SCRATCH/inputs accompany the fixture. CASA's
solvers create their schemas; explicit nonconstant coefficients make an
ignored table, phase-sign error or frequency-independent bandpass fail.
"""
import argparse
from pathlib import Path

import numpy as np
from casatasks import bandpass, gaincal, version_string
from casatools import simulator, table

from sm_apply_parameters_oracle import initialize
from sm_apply_time_oracle import write
from sm_flagged_samples_oracle import read_rows
from sm_multiple_models_oracle import make_session


def make_inputs(root):
    path = root / "solve.ms"
    sm = make_session(path, stokes="XX XY YX YY")
    for index in range(6):
        assert sm.observe(sourcename="target", spwname="band6",
                          starttime=f"{3 + index * 40}s", stoptime=f"{43 + index * 40}s")
    sm.close()
    initialize(path)
    tb = table()
    tb.open(str(path), nomodify=False)
    for row in range(tb.nrows()):
        tb.putcell("MODEL_DATA", row, np.ones((4, 4), np.complex64))
    tb.close()
    inputs = root / "inputs"
    inputs.mkdir()
    common = dict(vis=str(path), field="0", minsnr=0, minblperant=2, refant="0")
    bandpass(caltable=str(inputs / "BPOLY"), solint="inf", combine="scan",
             bandtype="BPOLY", degamp=1, degphase=1, maskcenter=0, maskedge=0, **common)
    gaincal(caltable=str(inputs / "GSPLINE"), gaintype="GSPLINE", splinetime=40.,
            calmode="ap", npointaver=1, **common)
    for kind in ("BPOLY", "GSPLINE"):
        tb.open(str(inputs / kind), nomodify=False)
        for row in range(tb.nrows()):
            antenna = int(tb.getcell("ANTENNA1", row))
            tb.putcell("SCALE_FACTOR", row, complex(1. + .03 * antenna, .02))
            for column, start, stop in (("POLY_COEFF_AMP", .01, .06),
                                         ("POLY_COEFF_PHASE", .04, .12)):
                coefficients = tb.getcell(column, row)
                values = np.linspace(start, stop, coefficients.size) * (antenna + 1)
                tb.putcell(column, row, values.reshape(coefficients.shape))
        tb.close()
    return inputs


def record(args):
    assert version_string() == "6.7.6.14"
    root = args.out.resolve()
    root.mkdir()
    inputs = make_inputs(root)
    cases = []
    for name, kind, extended in (("bpoly", "BPOLY", False), ("gspline", "GSPLINE", False),
                                  ("gspline_extended", "GSPLINE", True),
                                  ("bpoly_perobs", "BPOLY", False),
                                  ("bpoly_perscan", "BPOLY", False)):
        path = root / (name + ".ms")
        sm = make_session(path, stokes="XX XY YX YY")
        if extended:
            assert sm.setspwindow(spwname="band7", freq="300GHz", deltafreq="15.625MHz",
                                  freqresolution="15.625MHz", refcode="TOPO", nchannels=4,
                                  stokes="XX XY YX YY")
            for window, start, stop in (("band6", -120., -80.), ("band7", 40., 80.),
                                        ("band6", 400., 440.)):
                assert sm.observe(sourcename="target", spwname=window,
                                  starttime=f"{start - 37}s", stoptime=f"{stop - 37}s")
        sm.close()
        initialize(path)
        sm = simulator()
        assert sm.openfromms(str(path))
        interp = "linearperobs" if name.endswith("perobs") else "linearperscan" if name.endswith("perscan") else "linear"
        assert sm.setapply(table=str(inputs / kind), interp=interp, calwt=False)
        assert sm.corrupt()
        sm.close()
        rows = read_rows(path)
        tb = table()
        tb.open(str(path))
        times = tb.getcol("TIME")
        tb.close()
        for row, time in zip(rows, times):
            row["time_s"] = float(time)
        change = np.asarray([complex(*value) for row in rows for value in row["data"]])
        assert np.linalg.norm(change - (3. + 4j)) / np.linalg.norm(change) > .01
        cases.append(dict(name=name, kind=kind, extended=extended, interp=interp, rows=rows))
        print(name, "applied", len(rows), "rows", flush=True)
    write(args.fixture, dict(casa_version=version_string(), cases=cases))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    record(parser.parse_args())
