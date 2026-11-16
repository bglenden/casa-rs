#!/usr/bin/env python3
"""CASA 6.7.6-14 measures comet getters when no comet table is loaded."""
import json
import numpy as np
from casaconfig import config
config.data_auto_update=False
config.measures_auto_update=False
from casatools import measures


def clean(x):
    if isinstance(x, dict): return {k:clean(v) for k,v in x.items()}
    if isinstance(x,np.ndarray): return clean(x.tolist())
    if isinstance(x,(list,tuple)): return [clean(v) for v in x]
    if isinstance(x,np.generic): return x.item()
    return x


m=measures()
out=[]
for state in ['fresh','epoch','direction','position','done']:
    if state=='epoch': assert m.doframe(m.epoch('UTC','2020/01/01/00:00:00'))
    if state=='direction': assert m.doframe(m.direction('J2000','10deg','20deg'))
    if state=='position': assert m.doframe(m.position('ITRF','100m','200m','300m'))
    if state=='done': assert m.done()
    for name in ['cometname','comettype','comettopo','cometdist','cometangdiam']:
        out.append({'state':state,'method':name,'result':clean(getattr(m,name)())})
print(json.dumps({'casatools':'6.7.6-14','cases':out},indent=2))
