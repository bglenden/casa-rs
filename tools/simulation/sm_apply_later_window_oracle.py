#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA snapshots a calibration term's spectral-window coverage at setapply."""
import argparse
import gzip
import json
from pathlib import Path
import numpy as np
import casatasks
from casatools import table
from sm_multiple_models_oracle import make_session
from sm_apply_parameters_oracle import calibration, initialize
from sm_flagged_samples_oracle import read_rows

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument("--out",type=Path,required=True)
parser.add_argument("--fixture",type=Path,required=True)
parser.add_argument("--noise",action="store_true")
parser.add_argument("--existing",action="store_true")
parser.add_argument("--selected",action="store_true")
parser.add_argument("--repeat",action="store_true")
args=parser.parse_args()
assert casatasks.version_string()=="6.7.6.14"
args.out.mkdir(parents=True)
path=args.out/"applied.ms"
sm=make_session(path,stokes="XX XY YX YY")
tb=table();tb.open(str(path));times=tb.getcol("TIME");tb.close()
terms=args.out/"terms.cal"
info=calibration(sm,terms,"G",(float(times.min()),float(times.max())))
tb.open(str(terms),nomodify=False)
values=tb.getcol("CPARAM");values[:]=2;tb.putcol("CPARAM",values);tb.close()
assert sm.setapply(table=str(terms),interp="nearest",spwmap=[0],calwt=False)
if args.noise:
    assert sm.setseed(5)
    assert sm.setnoise(simplenoise="0.1Jy")
assert sm.setspwindow(spwname="later",freq="230GHz",deltafreq="15.625MHz",freqresolution="15.625MHz",refcode="TOPO",nchannels=4,stokes="XX XY YX YY")
assert sm.observe(sourcename="target",spwname="later",starttime="3s",stoptime="13s")
initialize(path)
if args.existing:
    from casatools import simulator
    sm.close()
    sm=simulator()
    assert sm.openfromms(str(path))
    assert sm.setapply(table=str(terms),interp="nearest",spwmap=[-1],calwt=False)
    if args.noise:
        assert sm.setseed(5)
        assert sm.setnoise(simplenoise="0.1Jy")
if args.selected:
    assert args.existing and not args.repeat
    assert sm.setdata(fieldid=[0])
assert sm.corrupt()
if args.repeat:
    assert args.existing and not args.noise
    sm.reset()
    repeated=args.out/"second.cal"
    calibration(sm,repeated,"G",(float(times.min()),float(times.max())+10))
    tb.open(str(repeated),nomodify=False)
    values=tb.getcol("CPARAM");values[:]=3;tb.putcol("CPARAM",values);tb.close()
    assert sm.setapply(table=str(repeated),interp="nearest",spwmap=[-1],calwt=False)
    assert sm.corrupt()
sm.close()
rows=read_rows(path)
tb.open(str(path));windows=tb.getcol("DATA_DESC_ID");times=tb.getcol("TIME");tb.close()
assert len(rows)==180 and set(windows)=={0,1}
factors=[36.0,9.0] if args.repeat else [4.0,1.0]
for row,window,time in zip(rows,windows,times):
    row.update(window=int(window),time_s=float(time))
    if not args.noise or window==1:
        factor=1.0 if args.selected and row["field"]!=0 else factors[window]
        assert all(value==[3*factor,4*factor] for value in row["data"])
record={"casa_version":casatasks.version_string(),"work_issue":1053,
        "recipe":"tools/simulation/sm_apply_later_window_oracle.py",
        "case":"G_broadcast_before_new_window","calibration":info,
        "noise":args.noise,"existing":args.existing,"selected":args.selected,"repeat":args.repeat,"scalar_gain":2.0,"spwmap":[0],"data_gain_factors_by_window":factors,"rows":rows}
args.fixture.parent.mkdir(parents=True,exist_ok=True)
with args.fixture.open("wb") as raw,gzip.GzipFile(filename="",fileobj=raw,mode="wb",mtime=0) as stream:
    stream.write((json.dumps(record,separators=(",",":"),allow_nan=False)+"\n").encode())
print("CASA later-window coverage:180 rows; original window DATA×4, later window unchanged",flush=True)
