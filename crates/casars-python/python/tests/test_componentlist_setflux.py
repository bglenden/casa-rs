"""CASA component flux setters, input safety and table persistence."""
import json
import subprocess
import sys
import tarfile
from pathlib import Path

import numpy as np
import pytest

from casars.data import schema_bundle
from casars.tools import componentlist, table

DATA = Path(__file__).parent / 'data'
ORACLE = json.loads((DATA / 'casa_componentlist_setflux.json').read_text())


def components(cl):
    assert cl.addcomponent(flux=9, label='first')
    assert cl.addcomponent(flux=8, label='second')


def snapshot(cl):
    return [{'label': cl.getlabel(i), 'value': cl.getfluxvalue(i).tolist(),
             'error': cl.getfluxerror(i).tolist(), 'unit': cl.getfluxunit(i),
             'pol': cl.getcomponent(i)['flux']['polarisation']}
            for i in range(cl.length())]


def assert_snapshot(actual, expected):
    assert len(actual) == len(expected)
    for a, e in zip(actual, expected):
        assert (a['label'], a['unit'], a['pol']) == (e['label'], e['unit'], e['pol'])
        np.testing.assert_allclose(a['value'], e['value'], rtol=1e-6, atol=1e-14)
        np.testing.assert_allclose(a['error'], e['error'], rtol=1e-14, atol=1e-14)


def test_setflux_matches_casa_and_failed_indices_preserve_values():
    for case in ORACLE['cases']:
        cl = componentlist(); components(cl)
        assert_snapshot(snapshot(cl), case['before'])
        if 'error' in case:
            expected = case['error']
            typ = {'RuntimeError': RuntimeError, 'TypeError': TypeError,
                   'SystemError': SystemError}[expected['type']]
            with pytest.raises(typ) as err:
                cl.setflux(*case['args'], **case['kwargs'])
            assert expected['message'].lower() in str(err.value).lower(), case
            after = case['before'] if typ is SystemError else case['after']
        else:
            assert cl.setflux(*case['args'], **case['kwargs']) is case['result']
            after = case['after']
        assert_snapshot(snapshot(cl), after)
        assert cl.done()


def test_crashes_and_discarded_complex_values_are_handled_safely():
    assert ORACLE['crashes'] == {'scalar_error': -6, 'empty_all': -11}
    cl = componentlist()
    with pytest.raises(RuntimeError, match='empty list'):
        cl.setflux(-1, 2)
    components(cl)
    assert cl.setflux(0, 2, error=0.2)
    assert cl.getfluxvalue(0).tolist() == [2, 0, 0, 0]
    assert cl.getfluxerror(0).tolist() == [0.2, 0, 0, 0]
    before = snapshot(cl)
    for kwargs in ({'value': 1+2j}, {'value': [1+2j, 2, 3, 4]},
                   {'value': [1, 2, 3, 4], 'error': [1+2j, 3+4j, 5+6j, 7+8j]}):
        with pytest.raises(RuntimeError, match='imaginary'):
            cl.setflux(0, **kwargs)
        assert snapshot(cl) == before
    assert ORACLE['complex_cases'][0]['after'][0]['value'][0] == 1
    assert ORACLE['complex_cases'][2]['after'][0]['error'] == [1, 3, 5, 7]
    assert cl.done()


def test_numpy_arrays_match_casa():
    values = {
        'float32': np.array([1, 2, 3, 4], dtype=np.float32),
        'int32': np.array([1, 2, 3, 4], dtype=np.int32),
        'matrix': np.array([[1, 2], [3, 4]], dtype=np.float64),
        'scalar_array': np.array(2, dtype=np.float64),
        'float32_scalar': np.float32(2),
        'empty': np.array([], dtype=np.float64),
        'complex64': np.array([1, 2, 3, 4], dtype=np.complex64),
        'matrix_error': np.array([[1, 2], [3, 4]], dtype=np.float64),
        'empty_error': np.array([1, 2, 3, 4], dtype=np.float64),
        'empty_matrix': np.empty((0, 2), dtype=np.float64),
        'complex_matrix': np.array([[1, 2], [3, 4]], dtype=np.complex64),
        'complex_scalar_array': np.array(2+0j, dtype=np.complex64),
    }
    for case in ORACLE['array_cases']:
        cl = componentlist(); assert cl.addcomponent(flux=9)
        kwargs = {}
        if case['case'] == 'matrix_error':
            kwargs['error'] = np.array([[.1, .2], [.3, .4]])
        elif case['case'] == 'empty_error':
            kwargs['error'] = np.array([], dtype=np.float64)
        if 'error' in case:
            with pytest.raises(RuntimeError, match=case['error']['message']):
                cl.setflux(0, values[case['case']], **kwargs)
        else:
            assert cl.setflux(0, values[case['case']], **kwargs) is case['result']
        assert_snapshot(snapshot(cl), case['after'])
        assert cl.done()


def test_setflux_keeps_input_precision():
    cl = componentlist(); components(cl)
    assert cl.setflux(0, [0.1, 0.2, 0.3, 0.4])
    assert cl.getfluxvalue(0).tolist() == [0.1, 0.2, 0.3, 0.4]
    special = {case['case']: case for case in ORACLE['special']}
    assert special['large_finite']['stored_inf']
    assert cl.setflux(0, 1e200)
    assert cl.getfluxvalue(0)[0] == 1e200
    before = snapshot(cl)
    for number in (float('nan'), float('inf'), -float('inf')):
        with pytest.raises(RuntimeError, match='non-finite'):
            cl.setflux(0, number)
        with pytest.raises(RuntimeError, match='non-finite'):
            cl.setflux(0, 2, error=number)
        assert snapshot(cl) == before
    assert cl.done()


def test_setflux_uses_shared_unit_definitions():
    # Definitions are process-wide; isolate this custom unit from other tests.
    code = ("from casars.tools import componentlist,quanta; "
            "q=quanta(); q.define('casaFlux1053','2Jy'); "
            "c=componentlist(); c.addcomponent(flux=9); "
            "assert c.setflux(0,3,unit='casaFlux1053'); "
            "assert c.getfluxunit(0)=='casaFlux1053'; "
            "assert q.convert('1casaFlux1053','Jy')['value']==2; "
            "assert c.getfluxvalue(0)[0]==3; c.done()")
    subprocess.run([sys.executable, '-c', code], check=True, capture_output=True, text=True)


def test_setflux_persists_without_changing_other_table_data(tmp_path):
    with tarfile.open(DATA / 'casa_componentlist_setflux.cl.tgz') as archive:
        extract = {'filter': 'data'} if sys.version_info >= (3, 12) else {}
        archive.extractall(tmp_path, **extract)
    path = str(tmp_path / 'source.cl')
    cl = componentlist(); assert cl.open(path, nomodify=False)
    assert cl.setflux(1, [1, 2, 3, 4], unit='mJy', error=[.1, .2, .3, .4])
    assert cl.done(); assert cl.open(path, nomodify=True)
    assert_snapshot(snapshot(cl), ORACLE['persisted'])
    assert cl.setflux(0, 5); assert cl.done()
    assert cl.open(path, nomodify=True); assert_snapshot(snapshot(cl), ORACLE['persisted']); assert cl.done()
    tb = table(); assert tb.open(path); assert tb.getkeyword('stable') == 17
    np.testing.assert_allclose(tb.getcell('Direction_Error', 0), [0.001, 0.002], atol=0, rtol=0)
    assert tb.close()


def test_contract_marks_setflux_mutating():
    obj = next(x for x in schema_bundle()['semantic']['objects'] if x['name'] == 'componentlist')
    assert next(x for x in obj['methods'] if x['name'] == 'setflux')['mutating'] is True
