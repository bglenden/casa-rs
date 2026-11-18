#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Probe invalid CASA component records in isolated, core-disabled workers."""
from casaconfig import config
config.data_auto_update=False;config.measures_auto_update=False
from casatools import componentlist,version_string
assert version_string() == '6.7.6-14'
import numpy as np,json
from pathlib import Path
from copy import deepcopy
c=componentlist();c.addcomponent(flux=2,dir='J2000 10deg 20deg',shape='Gaussian',majoraxis='4arcsec',minoraxis='2arcsec',positionangle='30deg');base=c.getcomponent(0);c.done()
def clean(v):
 if isinstance(v,dict):return {k:clean(x) for k,x in v.items()}
 if isinstance(v,np.ndarray):return clean(v.tolist())
 if isinstance(v,(tuple,list)):return [clean(x) for x in v]
 if isinstance(v,np.generic):return clean(v.item())
 return v
changes=[('shape.majoraxis',{'value':0,'unit':'arcsec'}),('shape.majoraxis',{'value':-1.0,'unit':'arcsec'}),('shape.minoraxis',{'value':9.0,'unit':'arcsec'}),('shape.majoraxiserror',{'value':-1.0,'unit':'arcsec'}),('shape.positionangle',{'value':500.0,'unit':'deg'}),('shape.direction.error',3),('shape.direction.error.longitude',{'value':-1.0,'unit':'arcsec'}),('shape.direction.error.longitude',{'value':1,'unit':'arcsec'}),('shape.direction.type','frequency'),('spectrum.frequency',{}),('spectrum.frequency.m0',{}),('spectrum.frequency.m0',{'value':1.0,'unit':'m'}),('spectrum.frequency.type','direction'),('flux.unit','invalidFluxUnit'),('shape.majoraxis','1invalidAngleUnit'),('label',['one']),('optionalParameters',[1]),('optionalParameters',[1.0])]
import resource,subprocess,sys
resource.setrlimit(resource.RLIMIT_CORE,(0,0))
if '--worker' in sys.argv:
 record=json.loads(sys.stdin.read());c=componentlist()
 try:result={'result':c.add(record),'after':clean(c.getcomponent(0))}
 except Exception as e:result={'error':{'type':type(e).__name__,'message':str(e)}}
 print('RESULT '+json.dumps(result),flush=True);c.done();raise SystemExit(0)
import argparse
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--out', type=Path, required=True)
args = parser.parse_args()
results=[]
for i,(path,value) in enumerate(changes):
 record=deepcopy(base);parent=record;fields=path.split('.')
 for name in fields[:-1]:parent=parent[name]
 parent[fields[-1]]=value
 child=subprocess.run([sys.executable,__file__,'--worker'],input=json.dumps(clean(record)),capture_output=True,text=True,timeout=20)
 if child.returncode!=0:
  result={'crash':{'returncode':child.returncode,'diagnostic':child.stderr.splitlines()[-1] if child.stderr else ''}}
 else:
  result=json.loads(next(line[7:] for line in child.stdout.splitlines() if line.startswith('RESULT ')))
 results.append({'name':'validation_'+str(i)+'_'+path,'record':clean(record),**result})
args.out.write_text(json.dumps({'casatools':version_string(),'cases':results},indent=2)+'\n')
for c in results:print(c['name'],c.get('error',c.get('crash',c.get('result'))))
