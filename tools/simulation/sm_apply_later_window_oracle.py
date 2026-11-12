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
assert sm.setspwindow(spwname="later",freq="230GHz",deltafreq="15.625MHz",freqresolution="15.625MHz",refcode="TOPO",nchannels=4,stokes="XX XY YX YY")
assert sm.observe(sourcename="target",spwname="later",starttime="3s",stoptime="13s")
initialize(path)
assert sm.corrupt()
sm.close()
rows=read_rows(path)
tb.open(str(path));windows=tb.getcol("DATA_DESC_ID");times=tb.getcol("TIME");tb.close()
assert len(rows)==180 and set(windows)=={0,1}
factors=[4.0,1.0]
for row,window,time in zip(rows,windows,times):
    row.update(window=int(window),time_s=float(time))
    assert all(value==[3*factors[window],4*factors[window]] for value in row["data"])
record={"casa_version":casatasks.version_string(),"work_issue":1053,
        "recipe":"tools/simulation/sm_apply_later_window_oracle.py",
        "case":"G_broadcast_before_new_window","calibration":info,
        "scalar_gain":2.0,"spwmap":[0],"data_gain_factors_by_window":factors,"rows":rows}
args.fixture.parent.mkdir(parents=True,exist_ok=True)
with args.fixture.open("wb") as raw,gzip.GzipFile(filename="",fileobj=raw,mode="wb",mtime=0) as stream:
    stream.write((json.dumps(record,separators=(",",":"),allow_nan=False)+"\n").encode())
print("CASA later-window coverage:180 rows; original window DATA×4, later window unchanged",flush=True)
