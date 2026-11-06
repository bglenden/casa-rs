#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA component shape updates and table persistence."""
import base64
import io
import json
import math
import tarfile
import tempfile
from pathlib import Path

import numpy as np
from casaconfig import config
config.data_auto_update = False
config.measures_auto_update = False
from casatools import componentlist, table, version_string  # noqa: E402


def snap(cl):
    out = []
    for i in range(cl.length()):
        comp = cl.getcomponent(i)
        out.append({'label': cl.getlabel(i), 'flux': cl.getfluxvalue(i).tolist(),
                    'type': cl.shapetype(i), 'shape': cl.getshape(i),
                    'direction': cl.getrefdir(i), 'direction_error': comp['shape']['direction']['error']})
    return out


def initial(cl):
    cl.addcomponent(flux=3, dir='J2000 10deg 20deg', label='first')
    cl.addcomponent(flux=5, dir='GALACTIC 30deg -15deg', label='second', shape='Gaussian',
                    majoraxis='4arcmin', minoraxis='2arcmin', positionangle='30deg')


ell = {'type': 'Gaussian', 'majoraxis': '4arcmin', 'minoraxis': '2arcmin', 'positionangle': '30deg'}
errs = {'majoraxiserror': '6arcsec', 'minoraxiserror': '3arcsec', 'positionangleerror': '2deg'}
calls = [
    ([0], {}), ([1], {}), ([0], ell), ([1], ell),
    ([0], {**ell, 'type': 'Disk'}), ([0], {**ell, **errs}),
    ([0], {**ell, **errs, 'type': 'Disk'}),
    ([0], {**ell, 'majoraxiserror': 'bad'}),
    ([0], {**ell, **errs, 'majoraxiserror': 'bad'}),
    ([0], {**ell, **errs, 'minoraxiserror': '2Jy'}),
    ([0], {**ell, **errs, 'positionangleerror': '-2deg'}),
    ([0], {'type': 'Gaussian'}), ([0], {'type': 'bad', 'majoraxis': 'bad'}),
    ([0], {'type': 'limbdarkeneddisk'}), ([0], {'type': 'Point', 'majoraxis': 'bad'}),
    ([0], {'type': 'Point', 'majoraxis': 5}),
    ([0], {**ell, 'majoraxis': '2Jy'}), ([0], {**ell, 'minoraxis': 'bad'}),
    ([0], {**ell, 'majoraxis': '0arcmin'}), ([0], {**ell, 'majoraxis': '1arcmin'}),
    ([0], {**ell, 'positionangle': '400deg'}),
    ([-1], {}), ([2], {}), ([None], {}), ([2**100], {}), ([2**32], {}), ([0.5], {}),
    ([0], {'type': None}), ([0], {'log': None}), ([0], {'log': False}),
    ([0], {**ell, 'optionalparms': []}), ([0], {**ell, 'optionalparms': None}),
    ([0], {**ell, 'optionalparms': ['bad']}),
]
results = []
for args, kwargs in calls:
    c = componentlist(); initial(c)
    case = {'args': args, 'kwargs': kwargs, 'before': snap(c)}
    try: case['result'] = c.setshape(*args, **kwargs)
    except Exception as e:
        message = str(e)
        if 'The errors must be non-negative angular quantities.' in message:
            message = 'The errors must be non-negative angular quantities.'
        for fragment in ('Failed AlwaysAssert which(i) >= 0', 'Failed AlwaysAssert index < nelements()',
                         'Gaussian2DParam<T>::setMajorAxis', 'Gaussian2DParam<T>::setMinorAxis',
                         'Gaussian2DParam<T>::setPA'):
            if fragment in message:
                message = fragment
                break
        case['error'] = {'type': type(e).__name__, 'message': message}
    case['after'] = snap(c); results.append(case); c.done()

nonfinite = []
for name, kwargs, field in [
    ('angle', {**ell, 'positionangle': {'value': float('nan'), 'unit': 'deg'}}, 'positionangle'),
    ('error', {**ell, **errs, 'majoraxiserror': {'value': float('nan'), 'unit': 'deg'}}, 'majoraxiserror'),
]:
    c = componentlist(); initial(c)
    result = c.setshape(0, **kwargs)
    nonfinite.append({'case': name, 'result': result, 'stored_nan': bool(math.isnan(c.getshape(0)[field]['value']))})
    c.done()

with tempfile.TemporaryDirectory() as directory:
    path = str(Path(directory) / 'source.cl')
    c = componentlist(); initial(c); c.rename(path); c.done()
    tb = table(); tb.open(path, nomodify=False); tb.putkeyword('stable', 17)
    tb.putcell('Direction_Error', 0, np.array([.001, .002])); tb.close()
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode='w:gz') as t: t.add(path, arcname='source.cl')
    c.open(path, nomodify=False); assert c.setshape(0, **ell, **errs); memory = snap(c); c.done()
    c.open(path, nomodify=True); persisted = snap(c); c.done()
    print('CASA_RESULT=' + json.dumps({'casatools': version_string(), 'cases': results,
                                     'memory': memory, 'persisted': persisted, 'nonfinite': nonfinite}))
    print('CASA_ARCHIVE=' + base64.b64encode(archive.getvalue()).decode())
