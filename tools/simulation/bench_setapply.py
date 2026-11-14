#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Whole native noise/gain/table-application control; explicit CPU W1, no FFT."""
import argparse
import importlib.metadata
import json
from pathlib import Path
import resource
import time
import numpy as np
import casars
from casars.tools import simulator
from casars.data import Table

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument("--out",type=Path,required=True)
parser.add_argument("--revision",required=True)
args=parser.parse_args()
args.out.mkdir(parents=True)
path=args.out/"control.ms";terms=args.out/"g.cal"
policy={"backend":"cpu","workers":1,"memory_budget":2<<30}
antennas,channels,integrations=43,64,30
before=resource.getrusage(resource.RUSAGE_SELF);start=time.perf_counter()
sm=simulator(**policy);assert sm.open(str(path))
assert sm.setconfig(telescopename="VLA",x=[-1601185.4+30*i for i in range(antennas)],y=[-5041977.5-10*i for i in range(antennas)],z=[3554875.9+5*i for i in range(antennas)],dishdiameter=[25.0],mount=["ALT-AZ"],antname=[f"A{i}" for i in range(antennas)],padname=[f"P{i}" for i in range(antennas)],coordsystem="global")
assert sm.setspwindow(spwname="L",freq="1.4GHz",deltafreq="1MHz",freqresolution="1MHz",nchannels=channels,stokes="RR LL")
assert sm.settimes(integrationtime="10s",usehourangle=False,referencetime="UTC 2020/01/01/00:00:00")
assert sm.setfield(sourcename="F",sourcedirection="J2000 23h30m00 34d00m00")
assert sm.observe("F","L",starttime="0s",stoptime=f"{integrations*10}s")
assert sm.setseed(5);assert sm.setnoise(simplenoise="0.1Jy");assert sm.corrupt()
assert sm.setgain(mode="random",amplitude=[0.02,0.01],table=str(terms));assert sm.corrupt();sm.close()
sm=simulator(**policy);assert sm.openfromms(str(path))
assert sm.setapply(table=str(terms),type="G",interp="linear",calwt=True,t=25.0,opacity=0.25)
assert sm.corrupt();sm.close()
wall=time.perf_counter()-start;after=resource.getrusage(resource.RUSAGE_SELF)
# Diagnostic readback is outside the timed application boundary.
table=Table.open(path);assert table.row_count==antennas*(antennas+1)//2*integrations
for column in ("DATA","TIME","FLAG","FLAG_ROW","WEIGHT","SIGMA"):
    value=np.asarray(table.get_column(column));assert column!="DATA" or np.isfinite(value).all()
    np.save(args.out/(column+".npy"),value)
record={"revision":args.revision,"module":str(Path(casars.__file__).resolve()),"package":importlib.metadata.version("casa-rs-python"),"numpy":np.__version__,"backend":"CPU","workers":1,"budget_requested":policy["memory_budget"],"antennas":antennas,"channels":channels,"integrations":integrations,"rows":table.row_count,"wall_s":wall,"user_s":after.ru_utime-before.ru_utime,"system_s":after.ru_stime-before.ru_stime,"process_peak_rss_bytes":after.ru_maxrss,"boundary":"First simulator constructor/open through final existing-MS calibration close; imports and diagnostic readback excluded.","fft":"No FFT: random gains and simple noise only."}
(args.out/"report.json").write_text(json.dumps(record,indent=1)+"\n")
print(json.dumps(record),flush=True)
