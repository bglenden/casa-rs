#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA flux-polarisation conversion, including complex table values."""
import base64
import io
import json
import shutil
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

import numpy as np
from casaconfig import config
config.data_auto_update = False
config.measures_auto_update = False
from casatools import componentlist, table, version_string  # noqa: E402


def pairs(array):
    return [[float(x.real), float(x.imag)] for x in array]


def cells(path):
    tb = table()
    tb.open(path)
    out = [{'pol': tb.getcell('Flux_Polarisation', i),
            'values': pairs(tb.getcell('Flux', i)),
            'errors': pairs(tb.getcell('Flux_Error', i)),
            'label': tb.getcell('Label', i),
            'direction_error': tb.getcell('Direction_Error', i).tolist(),
            'keyword': tb.getkeyword('stable')} for i in range(tb.nrows())]
    tb.close()
    return out


with tempfile.TemporaryDirectory() as tmp:
    source = str(Path(tmp) / 'source.cl')
    cl = componentlist()
    for name in ('stokes', 'linear', 'circular'):
        cl.addcomponent(flux=1, label=name)
    cl.rename(source)
    cl.done()
    tb = table()
    tb.open(source, nomodify=False)
    tb.putkeyword('stable', 17)
    tb.putcell('Direction_Error', 0, np.array([0.001, 0.002]))
    values = ([5, 2, 3, 1], [4+2j, 3+4j, 1-2j, 2-1j], [7+1j, 2+3j, -1+4j, 1-2j])
    errors = [0.1+0.02j, 0.2+0.03j, 0.3+0.04j, 0.4+0.05j]
    for i, pol in enumerate(('Stokes', 'Linear', 'Circular')):
        tb.putcell('Flux', i, np.array(values[i], dtype=np.complex128))
        tb.putcell('Flux_Error', i, np.array(errors, dtype=np.complex128))
        tb.putcell('Flux_Polarisation', i, pol)
    tb.close()
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode='w:gz') as t:
        t.add(source, arcname='source.cl')
    before = cells(source)
    cases = []
    for which in (0, 1, 2, -1):
        for target in ('Stokes', 'Linear', 'Circular'):
            path = str(Path(tmp) / f'{which}-{target}.cl')
            shutil.copytree(source, path)
            cl = componentlist()
            cl.open(path, nomodify=False)
            result = cl.convertfluxpol(which, target)
            cl.done()
            cases.append({'which': which, 'target': target, 'result': result, 'cells': cells(path)})
    arguments = []
    for args in ((0,), (0, 'linear'), (0, 'LIN'), (0, ''), (0, None),
                 (0, 3), (3, 'bad'), (-2, 'bad'), (0, 'bad'),
                 (None, 'linear'), (2**100, 'linear'), (2**32, 'circular')):
        cl = componentlist()
        cl.addcomponent(flux=[5, 2, 3, 1])
        try:
            outcome = {'result': cl.convertfluxpol(*args)}
        except Exception as exc:
            message = str(exc)
            if 'Index out of range.' in message:
                message = 'Index out of range.'
            elif 'Unknown polarization.' in message:
                message = 'Unknown polarization.'
            outcome = {'error': {'type': type(exc).__name__, 'message': message}}
        arguments.append({'args': args, **outcome,
                          'polarisation': cl.getcomponent(0)['flux']['polarisation']})
        cl.done()
    code = "import resource;resource.setrlimit(resource.RLIMIT_CORE,(0,0));from casaconfig import config;config.data_auto_update=False;config.measures_auto_update=False;from casatools import componentlist;componentlist().convertfluxpol(-1,'Linear')"
    crash = subprocess.run([sys.executable, '-c', code], capture_output=True).returncode
    print('CASA_RESULT=' + json.dumps({'casatools': version_string(), 'before': before,
                                       'cases': cases, 'arguments': arguments,
                                       'empty_all_returncode': crash}))
    print('CASA_ARCHIVE=' + base64.b64encode(archive.getvalue()).decode())
