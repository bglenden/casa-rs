#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Probe CASA 6.7.6.14 component record defaults, diagnostics and partial imports."""
from casaconfig import config
config.data_auto_update=False
config.measures_auto_update=False
from casatools import componentlist, logsink, version_string
import numpy as np
from copy import deepcopy
from pathlib import Path
import json
import argparse
parser=argparse.ArgumentParser(description="Record CASA component-list import semantics")
parser.add_argument("--out", type=Path, required=True)
args=parser.parse_args()
root=args.out.resolve().parent
root.mkdir(parents=True,exist_ok=True)
log=logsink(); log.setlogfile(str(root/'componentlist-records-casa.log')); log.showconsole(False)
def clean(v):
 if isinstance(v,dict):return {k:clean(x) for k,x in v.items()}
 if isinstance(v,np.ndarray):return clean(v.tolist())
 if isinstance(v,(tuple,list)):return [clean(x) for x in v]
 if isinstance(v,np.generic):return clean(v.item())
 if isinstance(v,complex):return v.real if v.imag==0 else {'complex':[v.real,v.imag]}
 return v
c=componentlist();c.addcomponent(flux=[3,1,0.5,0.2],dir='J2000 10deg -20deg',label='gauss',shape='Gaussian',majoraxis='4arcsec',minoraxis='2arcsec',positionangle='30deg');normal=c.getcomponent(0);c.done()
cases=[('empty',{}),('normal',normal),('point',{'shape':{'type':'Point'}}),('frequency_current',{'spectrum':{'type':'Constant','frequency':'current'}})]
for field in ['flux','shape','spectrum','label','optionalParameters']:
 for name,value in [('int',5),('text','bad'),('empty',{})]:
  cases.append((field+'_'+name,{field:value}))
for field in ['flux','shape','spectrum']:
 for key in normal[field]:
  rec=deepcopy(normal);del rec[field][key];cases.append((field+'_missing_'+key,rec))
for field,key,vals in [('shape','type',['bad',1]),('spectrum','type',['bad',1]),('flux','polarisation',['bad',1]),('flux','unit',['m',1]),('flux','value',[[],[2], [1,2], [1,2,3,4,5]]),('flux','error',[[1],[1,2]]),('shape','direction',['current',1,{}])]:
 for i,v in enumerate(vals):
  rec=deepcopy(normal);rec[field][key]=v;cases.append((field+'_'+key+'_'+str(i),rec))
rec=deepcopy(normal);del rec['shape']['direction']['error'];cases.append(('no_dir_error',rec))
rec=deepcopy(normal);rec['shape']['type']='Limbdarkeneddisk';rec['optionalParameters']=np.array([0.5]);cases.append(('limbdisk',rec))
rec=deepcopy(normal);rec['flux']['value']=np.array([3,1,0.5,0.2],complex);cases.append(('complex',rec))
results=[]
for name,rec in cases:
 c=componentlist()
 try:res={'result':c.add(rec),'after':clean(c.getcomponent(0))}
 except Exception as e:res={'error':{'type':type(e).__name__,'message':str(e)},'length':c.length()}
 results.append({'name':name,'record':clean(rec),**res});c.done()
for name,rec in [('empty',{}),('zero',{'nelements':0}),('missing',{'nelements':1}),('wrong_component',{'nelements':1,'component0':3}),('no_count',{'component0':{}}),('default_component',{'nelements':1,'component0':{}}),('invalid_second',{'nelements':2,'component0':normal,'component1':{'shape':'bad'}}),('float_count',{'nelements':1.0,'component0':{}}),('bool_count',{'nelements':True,'component0':{}}),('text_count',{'nelements':'1','component0':{}})]:
 c=componentlist();c.addcomponent(label='existing')
 try:res={'result':c.fromrecord(rec)}
 except Exception as e:res={'error':{'type':type(e).__name__,'message':str(e)}}
 results.append({'name':'fromrecord_'+name,'record':clean(rec),**res,'length':c.length(),'after':clean(c.torecord())});c.done()
(args.out).write_text(json.dumps({'casatools':version_string(),'cases':results},indent=2))
print('Wrote',len(results),'cases to',args.out)
