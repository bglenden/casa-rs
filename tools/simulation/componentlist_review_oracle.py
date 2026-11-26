#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA 6.7.6.14 regressions identified by the consolidated component review."""
import argparse
from copy import deepcopy
import json
from pathlib import Path
import resource
import subprocess
import tempfile

resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--out', type=Path, required=True)
parser.add_argument('--native-python', required=True)
args = parser.parse_args()
from casaconfig import config
config.data_auto_update = False
config.measures_auto_update = False
from casatools import componentlist, table, version_string
import numpy as np
assert version_string() == '6.7.6-14'


def clean(value):
    if isinstance(value, dict):
        return {k: clean(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, np.ndarray)):
        return [clean(v) for v in value]
    if isinstance(value, np.generic):
        return value.item()
    return value


def capture(action):
    try:
        return {'value': clean(action())}
    except Exception as error:
        return {'error': str(error).split('\n... thrown by')[0].split('\t', 3)[-1]}


root = Path(__file__).resolve().parents[2]
source = json.loads((root / 'frontends/casars-python/python/tests/data/casa_componentlist_record_spectra.json').read_text())
tabular = next(c['record'] for c in source['cases'] if c['name'] == 'tabular')
out = {'casatools': version_string(), 'tabular': [], 'offsets': [], 'sort': {}}
for field, value in [('referenceFreq', 1.8e9), ('minFreq', 1.1e9), ('maxFreq', 1.9e9), ('freqRef', 'LSRK'), ('referenceFreq', 'bad'), ('minFreq', 'bad'), ('maxFreq', 'bad'), ('tabFreqVal', 'bad'), ('ival', 'bad')]:
    record = deepcopy(tabular)
    record['spectrum'][field] = value
    cl = componentlist()
    result = capture(lambda: cl.add(record))
    if 'value' in result:
        result['component'] = clean(cl.getcomponent(0))
    out['tabular'].append({'field': field, 'input': value, **result})
    cl.done()

with tempfile.TemporaryDirectory() as directory:
    path = str(Path(directory) / 'siblings.cl')
    a, b = componentlist(), componentlist()
    a.addcomponent(label='original'); a.rename(path); a.done()
    a.open(path, False); b.open(path, True)
    a.setlabel(0, 'changed'); b.putkeyword('k', 42)
    out['writable_sibling_close'] = capture(a.done)
    b.done(); a.open(path, True)
    out['writable_sibling_close'].update(label=a.getlabel(0), keyword=a.getkeyword('k'))
    a.done()

    source = json.loads((root / 'frontends/casars-python/python/tests/data/casa_componentlist_offsets.json').read_text())
    record = next(c['record'] for c in source['cases'] if c['name'] == 'frequency_identity')
    for kind in ['identity', 'Constant', 'spectral index', 'Power Logarithmic Polynomial']:
        cl = componentlist(); cl.add(record)
        case = {'kind': kind, 'before': clean(cl.getspectrum(0))}
        if kind != 'identity':
            case['setter'] = capture(lambda: cl.setstokesspectrum(0, type=kind, index=[-0.7], reffreq='2GHz', frame='LSRK'))
        case['spectrum'] = clean(cl.getspectrum(0))
        case['component'] = clean(cl.getcomponent(0))
        path = str(Path(directory) / (kind.replace(' ', '_') + '.cl'))
        cl.rename(path); cl.done()
        tb = table(); tb.open(path)
        case['stored_spectral_record'] = clean(tb.getcell('Spectral_Record', 0))
        case['stored_frequency'] = clean(tb.getcell('Reference_Frequency', 0))
        tb.close(); out['offsets'].append(case)

for name, fluxes in [('select_after_sort', [1, 2]), ('repeat_equal_sort', [1, 1, 1]), ('remove_recover', [1, 2, 3])]:
    cl = componentlist()
    for i, flux in enumerate(fluxes):
        cl.addcomponent(flux=flux, label=str(i))
    cl.sort('Flux'); result = {'first': [cl.getlabel(i) for i in range(cl.length())]}
    cl.select([0]); result['selected'] = clean(cl.selected())
    if name == 'repeat_equal_sort':
        cl.sort('Flux'); result['second'] = [cl.getlabel(i) for i in range(cl.length())]
        cl.sort('Position'); result['position'] = [cl.getlabel(i) for i in range(cl.length())]
    if name == 'remove_recover':
        cl.remove([0]); result['after_remove'] = clean(cl.selected())
        result['remaining'] = [cl.getlabel(i) for i in range(cl.length())]
        cl.recover(); result['after_recover'] = clean(cl.selected())
        result['recovered'] = [cl.getlabel(i) for i in range(cl.length())]
    out['sort'][name] = result; cl.done()
cl = componentlist()
for name, longitude in [('far', '0.0004arcsec'), ('near', '0.0002arcsec')]:
    cl.addcomponent(label=name, dir=f'J2000 {longitude} 0deg')
cl.sort('Position'); out['sort']['tiny_position'] = [cl.getlabel(i) for i in range(2)]; cl.done()

worker_code = '''
import json,sys
from casars.tools import table
print('ready',flush=True)
for line in sys.stdin:
 request=json.loads(line); t=table(); t.open(request['path'],nomodify=False,lockoptions={'option':'usernoread'})
 locked=t.lock(write=request['write'],nattempts=1); t.close(); print(json.dumps(locked),flush=True)
'''
worker = subprocess.Popen([args.native_python, '-u', '-c', worker_code], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
assert worker.stdout.readline().strip() == 'ready'
try:
    with tempfile.TemporaryDirectory() as directory:
        a, b = componentlist(), componentlist()
        pa, pb = [str(Path(directory) / name) for name in ['a.cl', 'b.cl']]
        for cl, path in [(a, pa), (b, pb)]:
            cl.addcomponent(); cl.rename(path); cl.done(); cl.open(path, True)
        related = table(); related.open(pa)
        a.putkeyword('pending', 42)
        out['related_table_ok'] = capture(related.ok)
        out['unrelated_table_ok'] = capture(lambda: table().ok())
        def probe(path, write):
            worker.stdin.write(json.dumps({'path': path, 'write': write}) + '\n'); worker.stdin.flush()
            return json.loads(worker.stdout.readline())
        out['unrelated_a_reader'] = probe(pa, False)
        out['unrelated_b_writer'] = probe(pb, True)
        related.done(); a.done(); b.done()
finally:
    worker.stdin.close(); worker.wait(timeout=5)
args.out.write_text(json.dumps(out, indent=2) + '\n')
