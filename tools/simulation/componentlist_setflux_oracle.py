#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA setflux semantics, data-loss cases and isolated aborts."""
import base64
import io
import json
import math
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

from casaconfig import config
config.data_auto_update = False
config.measures_auto_update = False
from casatools import componentlist, table, version_string  # noqa: E402
import numpy as np  # noqa: E402


def snapshot(cl):
    return [{'label': cl.getlabel(i), 'value': cl.getfluxvalue(i).tolist(),
             'error': cl.getfluxerror(i).tolist(), 'unit': cl.getfluxunit(i),
             'pol': cl.getcomponent(i)['flux']['polarisation']}
            for i in range(cl.length())]


cases = [
    ([0], {}), ([0, 2], {}), ([0, [1, 2, 3, 4]], {}),
    ([-1, [1, 2, 3, 4]], {'unit': 'mJy', 'error': [0.1, 0.2, 0.3, 0.4]}),
    ([1, [5, 6, 7, 8]], {'polarization': 'Linear'}),
    ([0, [1, 2, 3, 4]], {'polarization': 'Circular'}),
    ([0, [0.1, 0.2, 0.3, 0.4]], {}),
    ([0, []], {'error': []}), ([0, []], {'error': None}),
    ([0, [1, 2]], {}), ([0, [1, 2]], {'error': [1]}),
    ([0, 2], {'polarization': 'Linear'}), ([0, 2], {'polarization': 'bad'}),
    ([0, 2], {'unit': 'm'}), ([0, 2], {'unit': 'bad!!'}),
    ([0, 'bad'], {}), ([0, True], {}), ([0, {'value': 1}], {}),
    ([0, [1, 2, 3, 4]], {'error': [1]}),
    ([0, [1, 2, 3, 4]], {'error': 'ignored'}),
    ([0, [1, 2, 3, 4]], {'error': True}),
    ([2, 2], {}), ([-2, 2], {}), ([2, 2], {'unit': 'm'}),
    ([None, 2], {}), ([2**100, 2], {}), ([2**32, 2], {}),
    ([0, 2], {'unit': None}), ([0, 2], {'polarization': None}),
    ([0, 2], {'log': None}), ([0, 2], {'log': False}),
]
results = []
for args, kwargs in cases:
    cl = componentlist()
    cl.addcomponent(flux=9, label='first')
    cl.addcomponent(flux=8, label='second')
    item = {'args': args, 'kwargs': kwargs, 'before': snapshot(cl)}
    try:
        item['result'] = cl.setflux(*args, **kwargs)
    except Exception as e:
        message = str(e)
        for fragment in ('Index out of range.', 'Error array must have same length as value array',
                         'Unknown polarization.', 'The flux units must have the same dimensions',
                         'The flux must have one or four elements', 'Could not understand the type'):
            if fragment in message:
                message = fragment
                break
        if 'Unknown unit' in message or 'Illegal unit' in message:
            message = 'unit'
        item['error'] = {'type': type(e).__name__, 'message': message}
    item['after'] = snapshot(cl)
    results.append(item)
    cl.done()

arrays = [
    ('float32', np.array([1,2,3,4], dtype=np.float32), None),
    ('int32', np.array([1,2,3,4], dtype=np.int32), None),
    ('matrix', np.array([[1,2],[3,4]], dtype=np.float64), None),
    ('scalar_array', np.array(2, dtype=np.float64), None),
    ('float32_scalar', np.float32(2), None),
    ('empty', np.array([], dtype=np.float64), None),
    ('complex64', np.array([1,2,3,4], dtype=np.complex64), None),
    ('matrix_error', np.array([[1,2],[3,4]], dtype=np.float64), np.array([[.1,.2],[.3,.4]])),
    ('empty_error', np.array([1,2,3,4], dtype=np.float64), np.array([], dtype=np.float64)),
    ('empty_matrix', np.empty((0, 2), dtype=np.float64), None),
    ('complex_matrix', np.array([[1,2],[3,4]], dtype=np.complex64), None),
    ('complex_scalar_array', np.array(2+0j, dtype=np.complex64), None),
]
array_cases = []
for name, value, error in arrays:
    cl = componentlist(); cl.addcomponent(flux=9)
    kwargs = {} if error is None else {'error': error}
    try: result = {'result': cl.setflux(0, value, **kwargs)}
    except Exception as e:
        message = str(e)
        if 'Error array must have same length as value array' in message:
            message = 'Error array must have same length as value array'
        result = {'error': {'type': type(e).__name__, 'message': message}}
    array_cases.append({'case': name, **result, 'after': snapshot(cl)})
    cl.done()

complex_cases = ["{'value':1+2j}", "{'value':[1+2j,2,3,4]}",
                 "{'value':[1,2,3,4],'error':[1+2j,3+4j,5+6j,7+8j]}"]
complex_results = []
for source in complex_cases:
    cl = componentlist(); cl.addcomponent(flux=9)
    cl.setflux(0, **eval(source, {'__builtins__': {}}))
    complex_results.append({'case': source, 'after': snapshot(cl)})
    cl.done()

special = []
for name, value in [('nan', float('nan')), ('inf', float('inf')), ('large_finite', 1e200)]:
    cl = componentlist(); cl.addcomponent(flux=9)
    result = cl.setflux(0, value)
    stored = cl.getfluxvalue(0)[0]
    special.append({'case': name, 'result': result,
                    'stored_nan': bool(math.isnan(stored)), 'stored_inf': bool(math.isinf(stored))})
    cl.done()

probe = """import resource;resource.setrlimit(resource.RLIMIT_CORE,(0,0));from casaconfig import config;config.data_auto_update=False;config.measures_auto_update=False;from casatools import componentlist;c=componentlist();%s"""
crashes = {}
for name, code in [('scalar_error', "c.addcomponent(flux=9);c.setflux(0,2,error=.2)"),
                   ('empty_all', "c.setflux(-1,2)")]:
    crashes[name] = subprocess.run([sys.executable, '-c', probe % code], capture_output=True).returncode

with tempfile.TemporaryDirectory() as tmp:
    path = str(Path(tmp) / 'source.cl')
    cl = componentlist(); cl.addcomponent(flux=9, label='first'); cl.addcomponent(flux=8, label='second')
    cl.rename(path); cl.done()
    tb = table(); tb.open(path, nomodify=False); tb.putkeyword('stable', 17)
    tb.putcell('Direction_Error', 0, np.array([0.001, 0.002])); tb.close()
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode='w:gz') as f:
        f.add(path, arcname='source.cl')
    cl.open(path, nomodify=False); cl.setflux(1, [1,2,3,4], unit='mJy', error=[.1,.2,.3,.4]); cl.done()
    cl.open(path, nomodify=True); persisted = snapshot(cl); cl.done()
    print('CASA_RESULT=' + json.dumps({'casatools': version_string(), 'cases': results,
                                      'array_cases': array_cases, 'complex_cases': complex_results, 'special': special, 'crashes': crashes,
                                      'persisted': persisted}))
    print('CASA_ARCHIVE=' + base64.b64encode(archive.getvalue()).decode())
