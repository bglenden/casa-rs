#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA's component-list table concatenation and selection behavior."""
import base64
import io
import json
import tarfile
import tempfile
from pathlib import Path

from casaconfig import config
config.data_auto_update = False
config.measures_auto_update = False
from casatools import componentlist, version_string  # noqa: E402


def labels(cl):
    return [cl.getlabel(i) for i in range(cl.length())]


with tempfile.TemporaryDirectory() as tmp:
    path = str(Path(tmp) / 'source.cl')
    source = componentlist()
    for n in range(3):
        source.addcomponent(flux=n + 1, dir=f'J2000 {10*n}deg {n}deg', label=f'source{n}')
    source.rename(path)
    source.done()
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode='w:gz') as archive:
        archive.add(path, arcname='source.cl')
    cases = []
    for which in (None, [], [-1], [-2, 1], [0], [1], [2, 0], [0, 0], [0, 0, 0, 0], [0, 99], [99, 0], [99]):
        cl = componentlist()
        cl.addcomponent(flux=9, label='existing')
        args = [path] if which is None else [path, which]
        case = {'which': which}
        try:
            case['result'] = cl.concatenate(*args)
        except Exception as e:
            message = str(e)
            if 'Failed AlwaysAssert index < nelements()' in message:
                message = 'Failed AlwaysAssert index < nelements()'
            case['error'] = {'type': type(e).__name__, 'message': message}
        case['labels'] = labels(cl)
        cases.append(case)
        cl.done()
    other = []
    for value in (None, [], 5, False, '', '/no/such/componentlist'):
        cl = componentlist()
        try:
            result = {'result': cl.concatenate(value)}
        except Exception as e:
            result = {'error': {'type': type(e).__name__, 'message': str(e)}}
        other.append({'value': value, **result, 'labels': labels(cl)})
        cl.done()
    pending = []
    for which in (None, 2**100, [2**100]):
        cl = componentlist()
        cl.addcomponent(flux=9, label='existing')
        try:
            result = {'result': cl.concatenate(path, which)}
        except Exception as e:
            result = {'error': {'type': type(e).__name__, 'message': str(e)}}
        pending.append({'which': which, **result, 'labels': labels(cl)})
        cl.done()
    print('CASA_RESULT=' + json.dumps({'casatools': version_string(), 'cases': cases, 'other': other, 'pending': pending}))
    print('CASA_ARCHIVE=' + base64.b64encode(buffer.getvalue()).decode())
