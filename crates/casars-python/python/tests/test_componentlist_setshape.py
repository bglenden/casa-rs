"""CASA shape setters, preserved directions, errors and table persistence."""
import json
import sys
import tarfile
from pathlib import Path

import numpy as np
import pytest

from casars.data import schema_bundle
from casars.tools import componentlist, table

DATA = Path(__file__).parent / 'data'
ORACLE = json.loads((DATA / 'casa_componentlist_setshape.json').read_text())
ELL = {'type': 'Gaussian', 'majoraxis': '4arcmin', 'minoraxis': '2arcmin', 'positionangle': '30deg'}
ERRS = {'majoraxiserror': '6arcsec', 'minoraxiserror': '3arcsec', 'positionangleerror': '2deg'}


def initial(cl):
    assert cl.addcomponent(flux=3, dir='J2000 10deg 20deg', label='first')
    assert cl.addcomponent(flux=5, dir='GALACTIC 30deg -15deg', label='second', shape='Gaussian',
                           majoraxis='4arcmin', minoraxis='2arcmin', positionangle='30deg')


def snapshot(cl):
    out = []
    for i in range(cl.length()):
        comp = cl.getcomponent(i)
        out.append({'label': cl.getlabel(i), 'flux': cl.getfluxvalue(i).tolist(),
                    'type': cl.shapetype(i), 'shape': cl.getshape(i),
                    'direction': cl.getrefdir(i), 'direction_error': comp['shape']['direction']['error']})
    return out


def same_quantities(actual, expected):
    assert actual.keys() == expected.keys()
    for key in actual:
        assert actual[key]['unit'] == expected[key]['unit']
        assert actual[key]['value'] == pytest.approx(expected[key]['value'], rel=1e-14, abs=1e-15)


def assert_snapshot(actual, expected):
    assert len(actual) == len(expected)
    for a, e in zip(actual, expected):
        assert (a['label'], a['type']) == (e['label'], e['type'])
        np.testing.assert_allclose(a['flux'], e['flux'], rtol=1e-14, atol=1e-15)
        same_quantities(a['shape'], e['shape'])
        assert (a['direction']['type'], a['direction']['refer']) == (e['direction']['type'], e['direction']['refer'])
        same_quantities({k:a['direction'][k] for k in ('m0', 'm1')}, {k:e['direction'][k] for k in ('m0', 'm1')})
        same_quantities(a['direction_error'], e['direction_error'])


def test_setshape_matches_casa_and_failed_conversion_preserves_state():
    for case in ORACLE['cases']:
        cl = componentlist(); initial(cl)
        assert_snapshot(snapshot(cl), case['before'])
        if 'error' in case:
            e = case['error']; typ = {'RuntimeError': RuntimeError, 'TypeError': TypeError,
                                     'SystemError': SystemError}[e['type']]
            with pytest.raises(typ) as err: cl.setshape(*case['args'], **case['kwargs'])
            assert e['message'] in str(err.value), case
            after = case['before'] if typ is SystemError else case['after']
        else:
            assert cl.setshape(*case['args'], **case['kwargs']) is case['result']
            after = case['after']
        assert_snapshot(snapshot(cl), after)
        assert cl.done()


def test_shape_units_survive_records_removal_recovery_and_table_round_trip(tmp_path):
    with tarfile.open(DATA / 'casa_componentlist_setshape.cl.tgz') as archive:
        extract = {'filter': 'data'} if sys.version_info >= (3, 12) else {}
        archive.extractall(tmp_path, **extract)
    path = str(tmp_path / 'source.cl')
    cl = componentlist(); assert cl.open(path, nomodify=False)
    direction_errors = snapshot(cl)[0]['direction_error']
    assert direction_errors != ORACLE['memory'][0]['direction_error']  # CASA loses them.
    assert cl.setshape(0, **ELL, **ERRS)
    memory = snapshot(cl)
    assert memory[0]['direction_error'] == direction_errors
    same_quantities(memory[0]['shape'], ORACLE['memory'][0]['shape'])
    for record in (cl.getcomponent(0), cl.torecord()['component0']):
        same_quantities({k:record['shape'][k] for k in memory[0]['shape']}, memory[0]['shape'])
    assert cl.remove([0]); assert cl.recover() is False
    recovered = snapshot(cl)[1]
    same_quantities(recovered['shape'], memory[0]['shape'])
    assert recovered['direction_error'] == direction_errors
    assert cl.done()
    assert cl.open(path, nomodify=True)
    recovered = snapshot(cl)[1]
    same_quantities(recovered['shape'], ORACLE['persisted'][0]['shape'])
    assert recovered['direction_error'] == direction_errors
    persisted = snapshot(cl)
    assert cl.setshape(1, type='Point')
    assert cl.done()
    assert cl.open(path, nomodify=True)
    assert_snapshot(snapshot(cl), persisted)
    assert cl.done()
    tb = table(); assert tb.open(path); assert tb.getkeyword('stable') == 17; assert tb.close()


def test_nonfinite_shape_inputs_are_rejected_without_mutation():
    assert all(case['result'] and case['stored_nan'] for case in ORACLE['nonfinite'])
    cl = componentlist(); initial(cl)
    before = snapshot(cl)
    for kwargs in ({**ELL, 'positionangle': {'value': float('nan'), 'unit': 'deg'}},
                   {**ELL, **ERRS, 'majoraxiserror': {'value': float('nan'), 'unit': 'deg'}}):
        with pytest.raises(RuntimeError, match='non-finite'):
            cl.setshape(0, **kwargs)
        assert_snapshot(snapshot(cl), before)
    assert cl.done()


def test_ignored_optional_parameters_match_casa_and_cycles_are_safe():
    values = {'string': '1.0', 'bad_string': 'bad', 'inf': float('inf'),
              'nested_nan': [[float('nan')]], 'huge': 2**100, 'nested_huge': [[2**100]],
              'nested_mapping_huge': [{'a': 2**100}], 'mapping': {}, 'complex': 1+2j,
              'numpy_scalar': np.float32(2.5), 'numpy_array': np.array([1., 2.])}
    for case in ORACLE['optional_extra']:
        cl = componentlist(); initial(cl)
        if 'error' in case:
            assert case['error']['type'] == 'SystemError'
            with pytest.raises(SystemError, match='returned a result with an exception set'):
                cl.setshape(0, **ELL, optionalparms=values[case['case']])
            expected = case['before']
        else:
            assert cl.setshape(0, **ELL, optionalparms=values[case['case']]) is case['result']
            expected = case['after']
        assert_snapshot(snapshot(cl), expected)
        assert cl.done()
    assert ORACLE['cycle_returncode'] == -11
    cl = componentlist(); initial(cl)
    before = snapshot(cl)
    cyclic = []; cyclic.append(cyclic)
    with pytest.raises(ValueError, match='cyclic'):
        cl.setshape(0, **ELL, optionalparms=cyclic)
    assert_snapshot(snapshot(cl), before)
    assert cl.done()


def test_contract_marks_setshape_mutating():
    obj = next(x for x in schema_bundle()['semantic']['objects'] if x['name'] == 'componentlist')
    assert next(x for x in obj['methods'] if x['name'] == 'setshape')['mutating'] is True
