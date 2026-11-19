#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA 6.7.6.14 baseline/uvw/magnetic tools and their record consumers."""
import argparse
from copy import deepcopy
import json
from pathlib import Path
import resource
from casaconfig import config
config.data_auto_update=False
config.measures_auto_update=False
from casatools import measures,version_string
import numpy as np
resource.setrlimit(resource.RLIMIT_CORE,(0,0))
p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--out',type=Path,required=True)
a=p.parse_args()
assert version_string()=='6.7.6-14'

def clean(v):
    if isinstance(v,bytes):return {'__python_bytes__':list(v)}
    if isinstance(v,dict):return {k:clean(x) for k,x in v.items()}
    if isinstance(v,(list,tuple,np.ndarray)):return [clean(x) for x in v]
    if isinstance(v,np.generic):return v.item()
    return v

cases=[]
def capture(name,method,args=None,kwargs=None,frame=None):
    args=[] if args is None else args;kwargs={} if kwargs is None else kwargs
    c={'name':name,'method':method,'args':clean(args),'kwargs':clean(kwargs),'initial_frame':'empty after done; existing owner decision'}
    tool=measures();tool.done()
    try:
        if frame:
            c['frame']=clean(frame)
            for element in frame:assert tool.doframe(element)
        result=getattr(tool,method)(*args,**kwargs)
        c['result']=clean(result)
        c['python_type']=type(result).__name__
    except Exception as e:c['error']={'type':type(e).__name__,'message':str(e)}
    finally:tool.done()
    cases.append(c)

m=measures()
frame=[m.epoch('UTC','2020/01/01'),m.observatory('ALMA'),m.direction('J2000','10deg','20deg')]
records={}
for method,default,unit in [('baseline','ITRF','m'),('uvw','ITRF','m'),('earthmagnetic','IGRF','nT')]:
    capture(method+'_default',method)
    valid=['3nT','10deg','20deg'] if method=='earthmagnetic' else ['1m','2m','3m']
    base=getattr(m,method)(default,*valid)
    records[method]=base
    capture(method+'_codes','listcodes',[base])
    refs=m.listcodes(base)['normal']
    for ref in [*refs,'bad','']:
        capture(method+'_ref_'+ref,method,[ref,*valid],frame=frame)
    values=[['1'+unit,'2'+unit,'3'+unit],['10deg','20deg','3'+unit],['3'+unit,'10deg','20deg'],[1.,2.,3.],[{'value':1.,'unit':unit},{'value':2.,'unit':unit},{'value':3.,'unit':unit}],['bad','2'+unit,'3'+unit],['1Hz','2Hz','3Hz'],[None,[],[]]]
    for i,args in enumerate(values):capture(method+'_values_'+str(i),method,[default,*args],frame=frame)
    if method!='earthmagnetic':
        for n in [1,2,3]:
            q=[{'value':[float(i+j+1) for i in range(n)],'unit':'m'} for j in range(3)]
            capture(method+'_vector_'+str(n),method,[default,*q],frame=frame)
        capture(method+'_bad_vector_lengths',method,[default,{'value':[1.,2.],'unit':'m'},'2m','3m'],frame=frame)
    for i,off in enumerate([base,dict(base,offset=deepcopy(base)),m.frequency('BARY','1GHz'),{'bad':1},{}]):
        capture(method+'_offset_'+str(i),method,[default,*valid,off],frame=frame)
    for i,rf in enumerate([default.encode(),None,3,default+'\0ignored']):
        capture(method+'_protocol_rf_'+str(i),method,[rf,*valid],frame=frame)

for kind,base in records.items():
    variants=[deepcopy(base),deepcopy(base),deepcopy(base)]
    for key in ['m0','m1','m2']:
        if key in variants[1]:variants[1][key]['value']=[base[key]['value'],base[key]['value']*2]
    variants[2]['offset']=deepcopy(base)
    for i,v in enumerate(variants):
        for method in ['getref','gettype','getvalue','ismeasure','show','dirshow','listcodes','getoffset','addxvalue']:
            capture(kind+'_inspect_'+str(i)+'_'+method,method,[v],frame=frame)
        target='J2000' if kind!='earthmagnetic' else 'ITRF'
        capture(kind+'_measure_'+str(i),'measure',[v,target],frame=frame)
        capture(kind+'_touvw_'+str(i),'touvw',[v],frame=frame)
        capture(kind+'_expand_'+str(i),'expand',[v],frame=frame)

for kind in ['baseline','uvw','earthmagnetic']:
    base=deepcopy(records[kind])
    if kind=='earthmagnetic':base=m.earthmagnetic('J2000','3nT','10deg','20deg')
    codes=m.listcodes(base)['normal']
    for ref in codes:
        capture(kind+'_convert_'+ref,'measure',[base,ref],frame=frame)

position=m.position('WGS84','10deg','20deg','100m')
vector=deepcopy(position)
for key in ['m0','m1','m2']:vector[key]['value']=[position[key]['value'],position[key]['value']*1.1]
for i,record in enumerate([position,vector,records['baseline'],records['uvw'],m.direction('J2000','10deg','20deg'),{},dict(position,offset=deepcopy(position))]):
    capture('asbaseline_'+str(i),'asbaseline',[record],frame=frame)
for method in ['touvw','expand']:
    capture(method+'_default',method)
    capture(method+'_empty_record',method,[{}])
    capture(method+'_missing_frame',method,[records['baseline']])
    for i,value in enumerate([None,3,[]]):capture(method+'_protocol_'+str(i),method,[value])
    for n in [2,3,4]:
        v=deepcopy(records['baseline'])
        for j,key in enumerate(['m0','m1','m2']):v[key]['value']=[v[key]['value']*(i+1)+j*.01 for i in range(n)]
        capture(method+'_baseline_vector_'+str(n),method,[v],frame=frame)
m.done()
a.out.write_text(json.dumps({'casatools':version_string(),'cases':cases},indent=2)+'\n')
print(len(cases),'cases')
