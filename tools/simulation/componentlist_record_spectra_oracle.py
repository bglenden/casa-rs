#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA component spectral records, missing fields and non-SI quantities."""
from casaconfig import config
config.data_auto_update=False;config.measures_auto_update=False
from casatools import componentlist,version_string
import json,numpy as np
from copy import deepcopy
from pathlib import Path

def clean(v):
 if isinstance(v,dict):return {k:clean(x) for k,x in v.items()}
 if isinstance(v,np.ndarray):return clean(v.tolist())
 if isinstance(v,(list,tuple)):return [clean(x) for x in v]
 if isinstance(v,np.generic):return clean(v.item())
 if isinstance(v,complex):return v.real if v.imag==0 else {'complex':[v.real,v.imag]}
 return v
import argparse
parser=argparse.ArgumentParser(description="Record cl spectral imports from CASA 6.7.6.14")
parser.add_argument('--out',type=Path,required=True)
args=parser.parse_args()
cases=[]
for name,opts in [('index',{'spectrumtype':'spectral index','index':-0.7}),('plp',{'spectrumtype':'plp','index':[1,2,3]}),('tabular',{})]:
 c=componentlist();c.addcomponent(flux=2,dir='J2000 10deg 20deg',**opts)
 if name=='tabular':c.setspectrum(0,type='tabular',tabularfreqs=[1e9,2e9],tabularflux=[2,4],tabularframe='LSRK')
 rec=c.getcomponent(0);c.done();cases.append((name,rec))
 for field in rec['spectrum']:
  if field in ['type','frequency']:continue
  r=deepcopy(rec);del r['spectrum'][field];cases.append((name+'_missing_'+field,r))
 if name=='index':
  for key,value in [('index','bad'),('index',[0.1,0.2]),('error','bad'),('error',[0.1,0.2]),('stokesindex',[1.0,2.0])]:
   r=deepcopy(rec);r['spectrum'][key]=value;cases.append((name+'_'+key+str(value),r))
 if name=='plp':
  for key,value in [('coeffs',2),('error',2),('error',[0.1])]:
   r=deepcopy(rec);r['spectrum'][key]=value;cases.append((name+'_'+key+str(value),r))
for name,rec in list(cases[:1]):
 for suffix,change in [('frequency_mhz',lambda r:r['spectrum']['frequency'].update(m0={'value':1420,'unit':'MHz'})),('direction_degrees',lambda r:r['shape']['direction'].update(m0={'value':10,'unit':'deg'},m1={'value':20,'unit':'deg'})),('missing_pol_no_error',lambda r:(r['flux'].pop('polarisation'),r['flux'].pop('error')))]:
  r=deepcopy(rec);change(r);cases.append((suffix,r))
results=[]
for name,record in cases:
 c=componentlist()
 try:result={'result':c.add(record),'after':clean(c.getcomponent(0))}
 except Exception as e:result={'error':{'type':type(e).__name__,'message':str(e)}}
 results.append({'name':name,'record':clean(record),**result});c.done()
args.out.write_text(json.dumps({'casatools':version_string(),'cases':results},indent=2)+'\n')

