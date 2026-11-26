#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Time native spectral measures against CASA 6.7.6.14 in the same Python."""
import argparse
from copy import deepcopy
import gc
import json
import logging
from pathlib import Path
import statistics
import subprocess
import sys
import time
from casaconfig import config
config.data_auto_update=False
config.measures_auto_update=False
from casatools import measures as casa_me, version_string
from casatasks import casalog
casalog.filter('SEVERE')
assert version_string()=='6.7.6-14'
p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--backend',choices=['casa','casars'],required=True)
p.add_argument('--out',type=Path,required=True)
p.add_argument('--repeat',type=int,default=1000)
p.add_argument('--samples',type=int,default=7)
p.add_argument('--cases',nargs='*')
a=p.parse_args()
if a.backend=='casars':from casars.tools import measures as factory
else:factory=casa_me
logging.disable(logging.CRITICAL)
source=casa_me();d=source.doppler('RADIO','10km/s');rv=source.radialvelocity('LSRK','10km/s');f=source.frequency('BARY','1.3GHz');rest=source.frequency('REST','1.42GHz')
cases={}
def add(name,op,count=None,mode='direct call'):
 cases[name]=(op,a.repeat if count is None else count,mode)
add('doppler',lambda m:m.doppler('RADIO','10km/s'))
add('radialvelocity',lambda m:m.radialvelocity('LSRK','10km/s'))
add('todoppler',lambda m:m.todoppler('OPTICAL',f,'1.42GHz'))
add('tofrequency',lambda m:m.tofrequency('LSRK',d,rest))
add('toradialvelocity',lambda m:m.toradialvelocity('LSRK',d))
add('torestfrequency',lambda m:m.torestfrequency(f,d))
for n in [2,128,1024]:
 vector=deepcopy(d);vector['m0']['value']=[10000.+i for i in range(n)]
 count=a.repeat if n==2 else min(a.repeat,20 if n==128 else 3)
 add(f'measure(doppler,{n})',lambda m,v=vector:m.measure(v,'OPTICAL'),count)
 scalar=[{**d,'m0':{**d['m0'],'value':value}} for value in vector['m0']['value']]
 if a.backend=='casa':
  add(f'tofrequency(vector,{n})',lambda m,values=scalar:[m.tofrequency('LSRK',v,rest) for v in values],count,'independent scalar controls; CASA vector path truncates')
 else:add(f'tofrequency(vector,{n})',lambda m,v=vector:m.tofrequency('LSRK',v,rest),count,'bulk vector, same complete outputs as scalar controls')
results={};gc.disable()
try:
 for name in a.cases or cases:
  op,repeat,mode=cases[name];samples=[]
  for sample in range(a.samples+1):
   m=factory();op(m)
   start=time.perf_counter_ns()
   for _ in range(repeat):op(m)
   elapsed=time.perf_counter_ns()-start;m.done()
   if sample:samples.append(elapsed/repeat)
  results[name]={'median_ns':statistics.median(samples),'samples_ns':samples,'calls_per_sample':repeat,'mode':mode}
finally:gc.enable();source.done()
root=Path(__file__).resolve().parents[2]
revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip()
dirty=subprocess.check_output(['git','status','--porcelain','--','crates','libs','tools/simulation'],cwd=root,text=True)
a.out.write_text(json.dumps({'revision':revision,'source_dirty':bool(dirty),'casa_version':version_string(),'python':sys.executable,'backend':a.backend,'command':sys.argv,'cases':results},indent=2)+'\n')
print('wrote',len(results),'cases')
