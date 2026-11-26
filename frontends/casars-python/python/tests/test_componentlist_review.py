"""Regression evidence from the one consolidated #1252 component review."""
from copy import deepcopy
import json
from pathlib import Path
import subprocess
import sys

import pytest
from casars.tools import componentlist, table
from test_componentlist_records import assert_record

DATA = Path(__file__).with_name('data')
ORACLE = json.loads((DATA / 'casa_componentlist_review.json').read_text())
TABULAR = next(c['record'] for c in json.loads((DATA / 'casa_componentlist_record_spectra.json').read_text())['cases'] if c['name'] == 'tabular')
OFFSET = next(c['record'] for c in json.loads((DATA / 'casa_componentlist_offsets.json').read_text())['cases'] if c['name'] == 'frequency_identity')


@pytest.mark.parametrize('case', ORACLE['tabular'], ids=lambda c: c['field'] + str(c['input']))
def test_tabular_present_fields(case):
    record = deepcopy(TABULAR)
    record['spectrum'][case['field']] = case['input']
    cl = componentlist()
    try:
        if 'error' in case:
            with pytest.raises(RuntimeError) as error:
                cl.add(record)
            assert str(error.value) == case['error']
        else:
            assert cl.add(record) is case['value']
            assert_record(cl.getcomponent(0), case['component'])
    finally:
        cl.done()


def test_writable_close_publishes_sibling_keywords(tmp_path):
    path = str(tmp_path / 'siblings.cl')
    a, b = componentlist(), componentlist()
    a.addcomponent(label='original'); a.rename(path); a.done()
    a.open(path, False); b.open(path, True)
    a.setlabel(0, 'changed'); b.putkeyword('k', 42)
    try:
        assert a.done() is ORACLE['writable_sibling_close']['value']
    finally:
        b.done()
    a.open(path)
    assert a.getlabel(0) == ORACLE['writable_sibling_close']['label']
    assert a.getkeyword('k') == ORACLE['writable_sibling_close']['keyword']
    a.done()


@pytest.mark.parametrize('case', ORACLE['offsets'], ids=lambda c: c['kind'])
def test_spectral_offset_setter_and_disk_record(case, tmp_path):
    cl = componentlist(); cl.add(OFFSET)
    try:
        assert_record(cl.getspectrum(0), case['before'])
        if case['kind'] != 'identity':
            assert cl.setstokesspectrum(0, type=case['kind'], index=[-.7], reffreq='2GHz', frame='LSRK') is case['setter']['value']
        assert_record(cl.getspectrum(0), case['spectrum'])
        assert_record(cl.getcomponent(0), case['component'])
        path = str(tmp_path / 'offset.cl'); cl.rename(path); cl.done()
        tb = table(); tb.open(path)
        try:
            assert_record(tb.getcell('Spectral_Record', 0), case['stored_spectral_record'])
            assert tb.getcell('Reference_Frequency', 0) == pytest.approx(case['stored_frequency'])
        finally:
            tb.close()
    finally:
        cl.done()


@pytest.mark.parametrize('name,fluxes', [('select_after_sort', [1, 2]), ('repeat_equal_sort', [1, 1, 1]), ('remove_recover', [1, 2, 3])])
def test_sort_selection_and_identity(name, fluxes):
    case = ORACLE['sort'][name]
    cl = componentlist()
    labels = lambda: [cl.getlabel(i) for i in range(cl.length())]
    for i, flux in enumerate(fluxes):
        cl.addcomponent(flux=flux, label=str(i))
    cl.sort('Flux'); assert labels() == case['first']
    cl.select([0]); assert list(cl.selected()) == case['selected']
    if name == 'repeat_equal_sort':
        cl.sort('Flux'); assert labels() == case['second']
        cl.sort('Position'); assert labels() == case['position']
    if name == 'remove_recover':
        cl.remove([0]); assert list(cl.selected()) == case['after_remove']
        assert labels() == case['remaining']
        cl.recover(); assert list(cl.selected()) == case['after_recover']
        assert labels() == case['recovered']
    cl.done()


def test_sort_sub_milliarcsecond_positions():
    cl = componentlist()
    for name, longitude in [('far', '0.0004arcsec'), ('near', '0.0002arcsec')]:
        cl.addcomponent(label=name, dir=f'J2000 {longitude} 0deg')
    cl.sort('Position')
    assert [cl.getlabel(i) for i in range(2)] == ORACLE['sort']['tiny_position']
    cl.done()


def test_unopened_table_call_keeps_unrelated_locks(tmp_path):
    # Initialise the persistent independent process before retaining any locks.
    code = '''
import json,sys
from casars.tools import table
print('ready',flush=True)
for line in sys.stdin:
 r=json.loads(line); t=table(); t.open(r['path'],nomodify=False,lockoptions={'option':'usernoread'})
 locked=t.lock(write=r['write'],nattempts=1); t.close(); print(json.dumps(locked),flush=True)
'''
    worker = subprocess.Popen([sys.executable, '-u', '-c', code], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    assert worker.stdout.readline().strip() == 'ready'
    a, b = componentlist(), componentlist()
    try:
        pa, pb = [str(tmp_path / name) for name in ['a.cl', 'b.cl']]
        for cl, path in [(a, pa), (b, pb)]:
            cl.addcomponent(); cl.rename(path); cl.done(); cl.open(path, True)
        related = table(); related.open(pa)
        a.putkeyword('pending', 42)
        assert related.ok() is ORACLE['related_table_ok']['value']
        assert table().ok() is ORACLE['unrelated_table_ok']['value']
        for path, write, key in [(pa, False, 'unrelated_a_reader'), (pb, True, 'unrelated_b_writer')]:
            worker.stdin.write(json.dumps({'path': path, 'write': write}) + '\n'); worker.stdin.flush()
            assert json.loads(worker.stdout.readline()) is ORACLE[key]
    finally:
        related.done(); a.done(); b.done(); worker.stdin.close(); worker.wait(timeout=5)


def test_casa_reads_native_offset_spectral_records(tmp_path):
    # Run explicitly with CASA's Python for the affected on-disk acceptance.
    config = pytest.importorskip('casaconfig.config')
    config.data_auto_update = False
    config.measures_auto_update = False
    casa = pytest.importorskip('casatools')
    assert casa.version_string() == ORACLE['casatools']
    for case in ORACLE['offsets']:
        cl = componentlist(); cl.add(OFFSET)
        if case['kind'] != 'identity':
            cl.setstokesspectrum(0, type=case['kind'], index=[-.7], reffreq='2GHz', frame='LSRK')
        path = str(tmp_path / (case['kind'].replace(' ', '_') + '.cl'))
        cl.rename(path); cl.done()
        tb = casa.table(); tb.open(path)
        try:
            assert_record(tb.getcell('Spectral_Record', 0), case['stored_spectral_record'])
            assert tb.getcell('Reference_Frequency', 0) == pytest.approx(case['stored_frequency'])
        finally:
            tb.done()
        reader = casa.componentlist(); reader.open(path)
        try:
            frequency = reader.getcomponent(0)['spectrum']['frequency']['m0']
            assert casa.quanta().convert(frequency, 'Hz')['value'] == pytest.approx(case['stored_frequency'])
        finally:
            reader.done()
