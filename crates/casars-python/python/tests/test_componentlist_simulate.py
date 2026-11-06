"""CASA default-component simulation, errors, and persistence."""
import json
import sys
import tarfile
from pathlib import Path

import numpy as np
import pytest

from casars.data import schema_bundle
from casars.tools import componentlist, table

DATA = Path(__file__).parent / 'data'
ORACLE = json.loads((DATA / 'casa_componentlist_simulate.json').read_text())


def snapshot(cl):
    return {'components': [cl.getcomponent(i) for i in range(cl.length())],
            'selected': cl.selected()}


def compare(actual, expected):
    if isinstance(expected, dict):
        assert actual.keys() == expected.keys()
        for key in expected:
            compare(actual[key], expected[key])
    elif isinstance(expected, (list, np.ndarray)):
        assert len(actual) == len(expected)
        for left, right in zip(actual, expected):
            compare(left, right)
    elif isinstance(expected, float):
        assert actual == pytest.approx(expected, rel=1e-14, abs=1e-14)
    else:
        assert actual == expected


def test_simulate_matches_casa():
    for case in ORACLE['cases']:
        cl = componentlist()
        if case['populated']:
            assert cl.addcomponent(flux=3.0, dir='J2000 10deg 20deg', label='sentinel', freq='LSRK 2GHz')
            assert cl.select([0])
        compare(snapshot(cl), case['before'])
        args = {'numpy_int': (np.int64(2),), 'numpy_array': (np.array(2),),
                'numpy_bool': (np.bool_(True),)}.get(case['case'], case['args'])
        if 'error' in case:
            error = case['error']
            with pytest.raises({'TypeError': TypeError, 'SystemError': SystemError}[error['type']]) as caught:
                cl.simulate(*args, **case['kwargs'])
            assert str(caught.value) == error['message'], case['case']
        else:
            assert cl.simulate(*args, **case['kwargs']) is case['result']
        compare(snapshot(cl), case['after'])
        assert cl.done()


def test_numpy_integer_inputs():
    for value in (np.int64(2), np.uint64(2), np.array(2)):
        cl = componentlist(); assert cl.simulate(value); assert cl.length() == 2; assert cl.done()


def test_simulate_preserves_table_and_selection(tmp_path):
    with tarfile.open(DATA / 'casa_componentlist_setspectrum.cl.tgz') as archive:
        extract = {'filter': 'data'} if sys.version_info >= (3, 12) else {}
        archive.extractall(tmp_path, **extract)
    path = str(tmp_path / 'source.cl')
    cl = componentlist(); assert cl.open(path, nomodify=False)
    before = cl.getcomponent(0); assert cl.select([0]); assert cl.simulate(2)
    compare(cl.getcomponent(0), before); assert cl.selected().tolist() == [0]
    expected = cl.getcomponent(1); assert cl.done()
    assert cl.open(path, nomodify=True); assert cl.length() == 4
    compare(cl.getcomponent(0), before); compare(cl.getcomponent(1), expected)
    assert cl.simulate(1); assert cl.length() == 5; assert cl.done()
    assert cl.open(path, nomodify=True); assert cl.length() == 4; assert cl.done()
    tb = table(); assert tb.open(path); assert tb.getkeyword('stable') == 17
    assert tb.getcell('Frequency_Error', 0) == 5.0
    assert tb.getcell('Frequency_Error_Units', 0) == 'kHz'
    np.testing.assert_allclose(tb.getcell('Direction_Error', 0), [.001, .002]); assert tb.close()


def test_contract_marks_simulate_mutating():
    obj = next(x for x in schema_bundle()['semantic']['objects'] if x['name'] == 'componentlist')
    assert next(x for x in obj['methods'] if x['name'] == 'simulate')['mutating'] is True


def test_unrepresentable_loop_count_is_rejected_before_mutation():
    cl = componentlist(); assert cl.simulate()
    before = snapshot(cl)
    with pytest.raises(RuntimeError, match='signed loop range'):
        cl.simulate(2**31)
    compare(snapshot(cl), before); assert cl.done()
