#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Supply saved A Noise and baseline gain tables to simulated DATA (SIM-16).

Run with CASA 6.7.6.14 Python, --out SCRATCH --fixture FILE.json.gz.
The inputs directory contains the actual CASA-produced calibration tables.
This exercises simulation application, without porting any calibration solve.
"""
import argparse
from pathlib import Path
import shutil

import numpy as np
from casatasks import blcal, version_string
from casatools import simulator, table

from sm_apply_parameters_oracle import initialize
from sm_apply_time_oracle import write
from sm_flagged_samples_oracle import read_rows
from sm_multiple_models_oracle import make_session


def record(args):
    assert version_string() == "6.7.6.14"
    root = args.out.resolve()
    root.mkdir()
    inputs = root / "inputs"
    inputs.mkdir()
    cases = []
    for name, kind, shift in (("a_noise", "A Noise", 0.), ("a_noise_linear", "A Noise", 5.),
                               ("m_mueller", "M Mueller", 0.), ("mf_mueller", "Mf Mueller", 0.),
                               ("a_noise_m", "A Noise", 0.), ("a_noise_mf", "A Noise", 0.)):
        path = root / (name + ".ms")
        target = inputs / name
        extra = "m_mueller" if name == "a_noise_m" else "mf_mueller" if name == "a_noise_mf" else None
        if extra:
            target = inputs / "a_noise"
        sm = make_session(path, stokes="XX XY YX YY")
        if kind == "A Noise" and not extra:
            assert sm.setseed(19)
            assert sm.setnoise(mode="simplenoise", simplenoise="0.25Jy", OTF=False,
                               table=str(root / name))
            sm.reset()
        sm.close()
        initialize(path)
        if kind == "A Noise" and not extra:
            shutil.copytree(root / (name + ".A.cal"), target)
        elif kind != "A Noise":
            blcal(vis=str(path), caltable=str(target), solint="inf", freqdep=kind == "Mf Mueller")
        tb = table()
        tb.open(str(path), nomodify=not bool(shift))
        times = tb.getcol("TIME") + shift
        if shift:
            tb.putcol("TIME", times)
        before = np.stack([tb.getcell("DATA", r) for r in range(tb.nrows())])
        tb.close()
        sm = simulator()
        assert sm.openfromms(str(path))
        assert sm.setapply(table=str(target), interp="linear,nearest", calwt=False)
        if extra:
            assert sm.setapply(table=str(inputs / extra), interp="linear,nearest", calwt=False)
        assert sm.corrupt()
        sm.close()
        rows = read_rows(path)
        after = np.asarray([[complex(*v) for v in row["data"]] for row in rows])
        before = before.transpose(0, 2, 1).reshape(after.shape)
        residual = np.linalg.norm(after - before) / np.linalg.norm(before)
        if kind == "A Noise":
            assert residual > .01
        else:
            assert residual < 1e-7  # M corrects zero below the simulator B pivot.
        for row, time in zip(rows, times):
            row["time_s"] = float(time)
        case = dict(name=name, kind=kind, interp="linear,nearest", time_shift=shift,
                    input_fixture="sm_baseline/" + target.name, rows=rows)
        if extra:
            case["extra_fixture"] = "sm_baseline/" + extra
        cases.append(case)
        print(name, "applied", len(rows), "rows; change", residual, flush=True)
    write(args.fixture, dict(casa_version=version_string(), cases=cases))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    record(parser.parse_args())
