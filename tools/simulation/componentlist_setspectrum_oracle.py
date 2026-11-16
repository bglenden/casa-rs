#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA component spectral setters, failures and table persistence."""
import base64
import io
import json
import math
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


def clean(value):
    if isinstance(value, np.ndarray): return value.tolist()
    if isinstance(value, np.generic): return value.item()
    if isinstance(value, dict): return {k: clean(v) for k, v in value.items()}
    return value


def snap(cl):
    return [{'label': cl.getlabel(i), 'flux': cl.getfluxvalue(i).tolist(),
             'direction': cl.getrefdir(i), 'spectrum': clean(cl.getspectrum(i))}
            for i in range(cl.length())]


def initial(cl):
    cl.addcomponent(flux=3, dir='J2000 10deg 20deg', label='first', freq='LSRK 1.4GHz')
    cl.addcomponent(flux=5, dir='GALACTIC 30deg -15deg', label='second', freq='LSRK 2GHz',
                    spectrumtype='spectral index', index=-.5)


calls = [
    ([0], {}), ([0], {'type': 'constant'}), ([1], {'type': 'constant'}),
    ([0], {'type': 'spectral', 'index': -.7}), ([1], {'type': 'Spectral Index', 'index': [-.7, .2]}),
    ([0], {'type': 's', 'index': 0}), ([0], {'type': 'spectral', 'index': []}),
    ([0], {'type': 'spectral', 'index': 'bad'}), ([0], {'type': 'plp', 'index': [-.5, .1, .02]}),
    ([0], {'type': 'plp', 'index': 1}), ([0], {'type': 'plp', 'index': []}),
    ([0], {'type': 'tabular'}),
    ([0], {'type': 'tabular', 'tabularfreqs': [1e9, 2e9], 'tabularflux': [1., 2.]}),
    ([0], {'type': 'tabular', 'tabularfreqs': [1e9, 2e9], 'tabularflux': [1., 2.], 'tabularframe': 'REST'}),
    ([0], {'type': 'tabular', 'tabularfreqs': [1e9, 2e9], 'tabularflux': [1., 2.], 'tabularframe': 'TOPO'}),
    ([0], {'type': 'tabular', 'tabularfreqs': [1e9, 2e9], 'tabularflux': [1.]}),
    ([0], {'type': 'tabular', 'tabularfreqs': [1e9, 2e9], 'tabularflux': [1., 2.], 'tabularframe': 'bad'}),
    ([0], {'type': 'bad'}), ([0], {'type': ''}), ([-1], {'type': 'constant'}),
    ([2], {'type': 'constant'}), ([None], {'type': 'constant'}), ([2**100], {'type': 'constant'}),
    ([2**32], {'type': 'constant'}), ([0.5], {'type': 'constant'}), ([0], {'type': None}),
    ([0], {'type': 'constant', 'tabularfreqs': None}), ([1], {'type': 'constant', 'tabularfreqs': None}), ([0], {'type': 'constant', 'tabularflux': None}),
    ([0], {'type': 'constant', 'tabularframe': None}),
]
results = []
for args, kwargs in calls:
    cl = componentlist(); initial(cl)
    case = {'args': args, 'kwargs': kwargs, 'before': snap(cl)}
    try: case['result'] = cl.setspectrum(*args, **kwargs)
    except Exception as e:
        message = str(e)
        for fragment in ('Unrecognized type for index', "for a spectrum='plp'", 'There need to be at least 2 points',
                         'lengths of tabular frequencies and values', 'is an unsupported frequency frame',
                         'Cannot transform frequency', 'Unkinown spectral type',
                         'Failed AlwaysAssert which(i) >= 0', 'Failed AlwaysAssert index < nelements()'):
            if fragment in message:
                message = fragment
                break
        case['error'] = {'type': type(e).__name__, 'message': message}
    case['after'] = snap(cl); results.append(case); cl.done()

vectors = [
    ('strings', ['1', '2'], [1., 2.]),
    ('invalid_string', [1., 'bad'], [1., 2.]),
    ('none_element', [1., None], [1., 2.]),
    ('nested', [[1.,2.],[3.,4.]], [1.,2.,3.,4.]),
    ('numpy_matrix', np.array([[1.,2.],[3.,4.]]), np.array([1.,2.,3.,4.])),
    ('booleans', [True, False], [1.,2.]),
]
vector_cases = []
for name, frequencies, flux in vectors:
    cl = componentlist(); initial(cl)
    case = {'case': name, 'before': snap(cl)}
    try: case['result'] = cl.setspectrum(0, type='tabular', tabularfreqs=frequencies, tabularflux=flux)
    except Exception as e: case['error'] = {'type': type(e).__name__, 'message': str(e)}
    case['after'] = snap(cl); vector_cases.append(case); cl.done()
cycle_code = "import resource;resource.setrlimit(resource.RLIMIT_CORE,(0,0));from casaconfig import config;config.data_auto_update=False;config.measures_auto_update=False;from casatools import componentlist;c=componentlist();c.addcomponent(flux=3);v=[];v.append(v);c.setspectrum(0,type='constant',tabularfreqs=v)"
cycle_returncode = subprocess.run([sys.executable, '-c', cycle_code], capture_output=True).returncode

special = []
for name, kwargs in [('nan_index', {'type': 'spectral', 'index': float('nan')}),
                     ('nan_plp', {'type': 'plp', 'index': [float('nan')]}),
                     ('nan_table', {'type': 'tabular', 'tabularfreqs': [1e9, 2e9], 'tabularflux': [1., float('nan')]})]:
    cl = componentlist(); initial(cl)
    try: result = cl.setspectrum(0, **kwargs); spectrum = cl.getspectrum(0)
    except Exception as e: special.append({'case': name, 'error': type(e).__name__}); cl.done(); continue
    found = any(math.isnan(float(x)) for x in (np.asarray(spectrum.get('coeffs', spectrum.get('ival', [spectrum.get('index', 0)]))).reshape(-1)))
    special.append({'case': name, 'result': result, 'stored_nan': found}); cl.done()

with tempfile.TemporaryDirectory() as directory:
    path = str(Path(directory) / 'source.cl')
    cl = componentlist(); initial(cl); cl.rename(path); cl.done()
    tb = table(); tb.open(path, nomodify=False); tb.putkeyword('stable', 17)
    tb.putcell('Direction_Error', 0, np.array([.001, .002])); tb.putcell('Frequency_Error', 0, 5.)
    tb.putcell('Frequency_Error_Units', 0, 'kHz'); tb.close()
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode='w:gz') as t: t.add(path, arcname='source.cl')
    cl.open(path, nomodify=False); assert cl.setspectrum(0, type='spectral', index=-.7); memory = snap(cl); cl.done()
    tb.open(path); freq_error = float(tb.getcell('Frequency_Error', 0)); tb.close()
    cl.open(path, nomodify=True); persisted = snap(cl); cl.done()
    print('CASA_RESULT=' + json.dumps({'casatools': version_string(), 'cases': results, 'special': special,
                                     'memory': memory, 'persisted': persisted, 'frequency_error_after': freq_error,
                                     'vector_cases': vector_cases, 'cycle_returncode': cycle_returncode}))
    print('CASA_ARCHIVE=' + base64.b64encode(archive.getvalue()).decode())
