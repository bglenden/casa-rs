#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Supplied physical error tables applied to simulated DATA (SIM-16).

CASA creates the standard complex/real table schemas; explicit nonconstant
antenna parameters exercise the remaining error operators. This does not
implement or test calibration solving in casa-rs.
"""
import argparse
from pathlib import Path
import shutil

import numpy as np
from casatasks import gencal, version_string
from casatools import simulator, table

from sm_apply_parameters_oracle import initialize
from sm_apply_time_oracle import write
from sm_flagged_samples_oracle import read_rows
from sm_multiple_models_oracle import make_session

CASES = [("switched_power", "G EVLASWPOW", True),
         ("switched_power_present", "G EVLASWPOW", True),
         ("fringe", "Fringe Jones", True), ("fringe_linear", "Fringe Jones", True),
         ("ionosphere", "F Jones", True), ("ionosphere_circular", "F Jones", True),
         ("accor", "Accor Jones", False), ("single_dish_gain", "SDGAIN_OTFD", False)]


def parameters(name, antenna):
    a = antenna + 1
    if name.startswith("switched_power"):
        return [1.1 + .03 * a, 50., .8 + .02 * a, 75.]
    if name.startswith("fringe"):
        return [.15 * a, .5 * a, 1e-13 * a, .001 * a,
                -.1 * a, -.2 * a, -8e-14 * a, .002 * a]
    if name.startswith("ionosphere"):
        return [1e16 * a]
    return [1.1 + .03 * a + .02j, .8 + .02 * a - .03j]


def record(args):
    assert version_string() == "6.7.6.14"
    root = args.out.resolve()
    root.mkdir()
    inputs = root / "inputs"
    inputs.mkdir()
    cases = []
    for name, kind, real in CASES:
        if args.only and name not in args.only:
            continue
        path, target = root / (name + ".ms"), inputs / name
        stokes = "RR RL LR LL" if name.endswith("circular") else "XX XY YX YY"
        sm = make_session(path, stokes=stokes)
        if not real:
            assert sm.setgain(mode="random", table=str(target), amplitude=[0., 0.])
            sm.reset()
        sm.close()
        initialize(path)
        tb = table()
        tb.open(str(path))
        reference_time = float(min(tb.getcol("TIME"))) - 5.
        tb.close()
        if real:
            gencal(vis=str(path), caltable=str(target), caltype="opac", parameter=[.01])
        tb.open(str(target), nomodify=False)
        info = tb.info()
        info["subType"] = kind
        tb.putinfo(info)
        tb.putkeyword("VisCal", kind)
        if name == "fringe_linear":
            count = tb.nrows()
            original = [{column: tb.getcell(column, row) for column in tb.colnames()
                         if tb.iscelldefined(column, row)}
                        for row in range(count)]
            tb.addrows(count)
            for row, values in enumerate(original, count):
                for column, value in values.items():
                    tb.putcell(column, row, value)
        for row, antenna in enumerate(tb.getcol("ANTENNA1")):
            values = np.asarray(parameters(name, int(antenna)),
                                dtype=np.float32 if real else np.complex64)[:, None]
            if name.startswith("fringe"):
                late = name == "fringe_linear" and row >= count
                tb.putcell("TIME", row, reference_time + (40. if late else 0.))
                if late:
                    values *= 1.3
            flags = np.zeros(values.shape, bool)
            if name.startswith("switched_power"):
                flags[1, 0] = True  # ignored Tsys slot when calwt is off
            for column, value in (("FPARAM" if real else "CPARAM", values),
                                   ("FLAG", flags), ("PARAMERR", np.zeros(values.shape, np.float32)),
                                   ("SNR", np.ones(values.shape, np.float32))):
                tb.putcell(column, row, value)
        tb.close()
        result = dict(name=name, kind=kind, interp="linear", input_fixture="sm_remaining_priors/" + name)
        if stokes.startswith("RR"):
            result["stokes"] = stokes
        if name == "switched_power_present":
            # This constructor takes these table handles, even though applying
            # an already supplied gain reads no SYSPOWER/CALDEVICE rows.
            context = inputs / "switched_power_context"
            context.mkdir()
            tb.open(str(path))
            descriptor = {"ANTENNA_ID": tb.getcoldesc("ANTENNA1")}
            tb.close()
            for extension in ("SYSPOWER", "CALDEVICE"):
                tb.create(str(path / extension), descriptor, nrow=0)
                tb.close()
                shutil.copytree(path / extension, context / extension)
            tb.open(str(path), nomodify=False)
            for extension in ("SYSPOWER", "CALDEVICE"):
                tb.putkeyword(extension, "Table: " + str(path / extension))
            tb.close()
            result["context_fixture"] = "sm_remaining_priors/switched_power_context"
        sm = simulator()
        assert sm.openfromms(str(path))
        stage = "setapply"
        try:
            assert sm.setapply(table=str(target), interp="linear", calwt=False)
            stage = "corrupt"
            assert sm.corrupt()
            result["outcome"] = "returned"
        except RuntimeError as error:
            result.update(outcome="raises", stage=stage, error=str(error))
        finally:
            sm.close()
        rows = read_rows(path)
        tb.open(str(path))
        times = tb.getcol("TIME")
        tb.close()
        for row, time in zip(rows, times):
            row["time_s"] = float(time)
        result["rows"] = rows
        cases.append(result)
        write(args.fixture, dict(casa_version=version_string(), cases=cases))
        print(name, result["outcome"], result.get("stage", ""), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--only", nargs="*")
    record(parser.parse_args())
