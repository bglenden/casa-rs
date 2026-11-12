#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA6.7.6.14 offline Tsys unit-A/scaling controls and native interop (#1292)."""
import argparse
import json
from pathlib import Path
import numpy as np
from casatools import simulator, table
from sm_multiple_models_oracle import make_session
from sm_offline_noise_oracle import read_data, parameters_for_rows, nrms


def gains_for_rows(path, calibration):
    tb = table(); tb.open(str(calibration))
    gains = {(float(tb.getcell("TIME", r)), int(tb.getcell("ANTENNA1", r))):
             tb.getcell("CPARAM", r)[0] for r in range(tb.nrows())}
    tb.close(); tb.open(str(path))
    pairs = []
    for row in range(tb.nrows()):
        time = float(tb.getcell("TIME", row))
        p, q = int(tb.getcell("ANTENNA1", row)), int(tb.getcell("ANTENNA2", row))
        pairs.append(gains[time, p] * gains[time, q].conj())
    tb.close()
    return np.asarray(pairs)[:, None, :]


def verify_native(root):
    cases = sorted(root.glob("*/case.json"))
    assert len(cases) == 2, len(cases)
    for case in cases:
        mode = json.loads(case.read_text())["mode"]
        path = case.parent / "data.ms"
        a_path, t_path = case.parent / "noise.A.cal", case.parent / "noise.T.cal"
        a = parameters_for_rows(path, a_path)
        gains = gains_for_rows(path, t_path)
        before = read_data(path)
        residual = nrms(before, -a / gains)
        assert residual < 1e-3, (mode, residual)
        print("C++ read native", mode, "A/T DATA NRMS", residual, flush=True)
        # Consume each native format through CASA as well as its table reader.
        # A subtracts its parameters; an ordinary supplied T multiplies DATA.
        for calibration, expected in [(a_path, before - a), (t_path, (before - a) * gains)]:
            sm = simulator(); assert sm.openfromms(str(path))
            assert sm.setapply(table=str(calibration), interp="nearest")
            assert sm.corrupt(); sm.close()
            residual = nrms(read_data(path), expected)
            assert residual < 1e-3, (mode, calibration.name, residual)
            print("CASA consumed native", mode, calibration.name, "NRMS", residual, flush=True)


def record_controls(root):
    root.mkdir(parents=True, exist_ok=True)
    for mode in ['tsys-manual','tsys-atm']:
        case=root/mode;case.mkdir();path=case/'data.ms'
        sm=make_session(path,'XX XY YX YY');sm.setseed(19)
        assert sm.setnoise(mode=mode,table=str(case/'noise.cal'),OTF=False,pwv='1mm',tau=0.05)
        assert sm.corrupt();once=read_data(path);assert sm.corrupt();twice=read_data(path);sm.close()
        a=parameters_for_rows(path,case/'noise.A.cal')
        tb=table();tb.open(str(case/'noise.T.cal'))
        gains={(float(tb.getcell('TIME',r)),int(tb.getcell('ANTENNA1',r))):tb.getcell('CPARAM',r)[0] for r in range(tb.nrows())}
        subtype=tb.info()['subType'];tb.close();tb.open(str(path));expected=[]
        for row in range(tb.nrows()):
            t=float(tb.getcell('TIME',row));p=int(tb.getcell('ANTENNA1',row));q=int(tb.getcell('ANTENNA2',row))
            expected.append(-a[row]/gains[t,p]/gains[t,q])
        tb.close();expected=np.asarray(expected);residual=nrms(once,expected);repeat=nrms(twice,2*once)
        assert residual<1e-3,(mode,residual);assert repeat<1e-3,(mode,repeat)
        result=dict(mode=mode,subtype=subtype,nrms=residual,repeat_nrms=repeat,rows=len(once),a_rms=float(np.sqrt(np.mean(a.real**2))))
        (case/'result.json').write_text(json.dumps(result,indent=2));print('RESULT',result,flush=True)


def main():
    import casatasks
    assert casatasks.version_string() == "6.7.6.14", casatasks.version_string()
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--out", type=Path)
    group.add_argument("--verify-native", type=Path)
    args = parser.parse_args()
    if args.verify_native: verify_native(args.verify_native)
    else: record_controls(args.out)


if __name__ == "__main__": main()
