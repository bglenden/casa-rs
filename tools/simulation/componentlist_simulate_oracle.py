#!/usr/bin/env python3
"""Record componentlist.simulate behavior on CASA 6.7.6-14.

Run with the CASA Python on the Mac mini; stdout is the JSON fixture.
"""
import json

import numpy as np
from casaconfig import config
config.data_auto_update = False
config.measures_auto_update = False
from casatools import componentlist


def clean(value):
    if isinstance(value, dict):
        return {key: clean(item) for key, item in value.items()}
    if isinstance(value, np.ndarray):
        return clean(value.tolist())
    if isinstance(value, (list, tuple)):
        return [clean(item) for item in value]
    if isinstance(value, np.generic):
        return value.item()
    return value


def snapshot(cl):
    return {'components': [clean(cl.getcomponent(i)) for i in range(cl.length())],
            'selected': clean(cl.selected())}


cases = [
    ('default', (), {}), ('zero', (0,), {}), ('negative', (-3,), {}),
    ('one', (1,), {}), ('three', (3,), {}), ('true', (True,), {}),
    ('false', (False,), {}), ('none', (None,), {}), ('float', (1.0,), {}),
    ('string', ('2',), {}), ('list', ([1],), {}), ('overflow', (2**63,), {}),
    ('underflow', (-2**63-1,), {}), ('log_false', (2, False), {}),
    ('log_none', (2, None), {}), ('log_int', (2, 1), {}),
    ('keyword', (), {'howmany': 2, 'log': True}),
    ('numpy_int', (np.int64(2),), {}), ('numpy_array', (np.array(2),), {}),
    ('numpy_bool', (np.bool_(True),), {}),
]
result = []
for populated in (False, True):
    for name, args, kwargs in cases:
        cl = componentlist()
        if populated:
            assert cl.addcomponent(flux=3.0, dir='J2000 10deg 20deg', label='sentinel', freq='LSRK 2GHz')
            assert cl.select([0])
        before = snapshot(cl)
        try:
            value = cl.simulate(*args, **kwargs)
            outcome = {'result': value}
        except Exception as error:
            outcome = {'error': {'type': type(error).__name__, 'message': str(error)}}
        result.append({'case': name, 'populated': populated, 'args': clean(args),
                       'kwargs': clean(kwargs), 'before': before, **outcome,
                       'after': snapshot(cl)})
        cl.done()
print(json.dumps({'casatools': '6.7.6-14', 'cases': result}, indent=2))
