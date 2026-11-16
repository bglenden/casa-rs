#!/usr/bin/env python3
"""CASA 6.7.6-14 componentlist.convertrefdir comparison."""
import json
import numpy as np
from casaconfig import config
config.data_auto_update = False
config.measures_auto_update = False
from casatools import componentlist


def clean(value):
    if isinstance(value, dict): return {k: clean(v) for k, v in value.items()}
    if isinstance(value, np.ndarray): return clean(value.tolist())
    if isinstance(value, (list, tuple)): return [clean(x) for x in value]
    if isinstance(value, np.generic): return value.item()
    return value


def snap(cl):
    return [clean(cl.getcomponent(i)) for i in range(cl.length())]

cases = [
 ('identity',0,'J2000'), ('galactic',0,'GALACTIC'), ('icrs',0,'ICRS'),
 ('b1950',0,'B1950'), ('supergal',0,'SUPERGAL'), ('second',1,'GALACTIC'),
 ('lower',0,'galactic'), ('prefix',0,'GAL'), ('invalid',0,'bad'),
 ('empty',0,''), ('hadec',0,'HADEC'), ('azel',0,'AZEL'), ('bmean',0,'BMEAN'),
 ('index_neg',-1,'GALACTIC'), ('index_two',2,'GALACTIC'),
 ('index_wrap',2**32,'GALACTIC'), ('invalid_before_index',-1,'bad'),
 ('index_none',None,'GALACTIC'), ('index_float',0.0,'GALACTIC'),
 ('frame_none',0,None), ('frame_int',0,3), ('frame_bytes',0,b'GALACTIC'),
 ('numpy_bool_false',np.bool_(False),'GALACTIC'), ('numpy_bool_true',np.bool_(True),'GALACTIC'),
]
results=[]
for name,which,frame in cases:
    c=componentlist()
    c.addcomponent(flux=3,dir='J2000 10deg 20deg',label='first',freq='LSRK 1.4GHz')
    c.addcomponent(flux=5,dir='J2000 210deg -15deg',label='second',freq='LSRK 2GHz')
    before=snap(c)
    try: out={'result':c.convertrefdir(which,frame)}
    except Exception as e: out={'error':{'type':type(e).__name__,'message':str(e)}}
    results.append({'case':name,'which':clean(which),'frame':frame.decode() if isinstance(frame,bytes) else frame,'before':before,**out,'after':snap(c)})
    c.done()
print(json.dumps({'casatools':'6.7.6-14','cases':results},indent=2))
