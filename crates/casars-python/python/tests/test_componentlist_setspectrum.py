"""CASA spectrum setters, state safety, and table persistence."""
import json
import sys
import tarfile
from pathlib import Path

import numpy as np
import pytest

from casars.data import schema_bundle
from casars.tools import componentlist, table

DATA = Path(__file__).parent / 'data'
ORACLE = json.loads((DATA / 'casa_componentlist_setspectrum.json').read_text())


def initial(cl):
    assert cl.addcomponent(flux=3, dir='J2000 10deg 20deg', label='first', freq='LSRK 1.4GHz')
    assert cl.addcomponent(flux=5, dir='GALACTIC 30deg -15deg', label='second', freq='LSRK 2GHz',
                           spectrumtype='spectral index', index=-.5)


def snapshot(cl):
    return [{'label': cl.getlabel(i), 'flux': cl.getfluxvalue(i).tolist(),
             'direction': cl.getrefdir(i), 'spectrum': cl.getspectrum(i)}
            for i in range(cl.length())]


def compare(actual, expected):
    if isinstance(expected, dict):
        assert actual.keys() == expected.keys()
        for key in expected: compare(actual[key], expected[key])
    elif isinstance(expected, list):
        assert len(actual) == len(expected)
        for a, e in zip(actual, expected): compare(a, e)
    elif isinstance(expected, float):
        assert actual == pytest.approx(expected, rel=1e-14, abs=1e-14)
    else:
        assert actual == expected


def test_spectral_setters_match_casa_and_preserve_state_on_errors():
    for case in ORACLE['cases']:
        cl = componentlist(); initial(cl)
        compare(snapshot(cl), case['before'])
        if 'error' in case:
            e = case['error']; typ = {'RuntimeError': RuntimeError, 'TypeError': TypeError,
                                     'SystemError': SystemError}[e['type']]
            with pytest.raises(typ) as err: cl.setspectrum(*case['args'], **case['kwargs'])
            assert e['message'] in str(err.value), case
            # CASA can change the spectrum before raising these errors.
            after = case['before'] if typ is SystemError or e['message'] == 'Cannot transform frequency' else case['after']
        else:
            assert cl.setspectrum(*case['args'], **case['kwargs']) is case['result']
            after = case['after']
        compare(snapshot(cl), after)
        assert cl.done()


def test_nonfinite_spectra_do_not_replace_existing_components():
    assert all(case['result'] and case['stored_nan'] for case in ORACLE['special'])
    cl = componentlist(); initial(cl); before = snapshot(cl)
    for kwargs in ({'type': 'spectral', 'index': float('nan')},
                   {'type': 'plp', 'index': [float('nan')]},
                   {'type': 'tabular', 'tabularfreqs': [1e9, 2e9], 'tabularflux': [1., float('nan')]}):
        with pytest.raises(RuntimeError, match='non-finite'):
            cl.setspectrum(0, **kwargs)
        compare(snapshot(cl), before)
    assert cl.done()


def test_tabular_vectors_follow_casa_and_reject_lost_values():
    values = {
        'strings': (['1', '2'], [1., 2.]),
        'invalid_string': ([1., 'bad'], [1., 2.]),
        'none_element': ([1., None], [1., 2.]),
        'nested': ([[1., 2.], [3., 4.]], [1., 2., 3., 4.]),
        'numpy_matrix': (np.array([[1., 2.], [3., 4.]]), np.array([1., 2., 3., 4.])),
        'booleans': ([True, False], [1., 2.]),
    }
    for case in ORACLE['vector_cases']:
        cl = componentlist(); initial(cl)
        frequencies, flux = values[case['case']]
        if case['case'] == 'invalid_string':
            assert case['result'] is True  # CASA silently stores zero for 'bad'.
            with pytest.raises(RuntimeError, match='unrepresentable tabular'):
                cl.setspectrum(0, type='tabular', tabularfreqs=frequencies, tabularflux=flux)
            expected = case['before']
        else:
            assert cl.setspectrum(0, type='tabular', tabularfreqs=frequencies, tabularflux=flux) is case['result']
            expected = case['after']
        compare(snapshot(cl), expected)
        assert cl.done()
    assert ORACLE['cycle_returncode'] == -11
    cl = componentlist(); initial(cl); before = snapshot(cl)
    cyclic = []; cyclic.append(cyclic)
    with pytest.raises(ValueError, match='cyclic'):
        cl.setspectrum(0, type='constant', tabularfreqs=cyclic)
    compare(snapshot(cl), before)
    assert cl.done()


def test_spectrum_preserves_frequency_error_and_table_metadata(tmp_path):
    with tarfile.open(DATA / 'casa_componentlist_setspectrum.cl.tgz') as archive:
        extract = {'filter': 'data'} if sys.version_info >= (3, 12) else {}
        archive.extractall(tmp_path, **extract)
    path = str(tmp_path / 'source.cl')
    cl = componentlist(); assert cl.open(path, nomodify=False)
    assert cl.setspectrum(0, type='spectral', index=-.7)
    compare(snapshot(cl), ORACLE['memory']); assert cl.done()
    assert cl.open(path, nomodify=True); compare(snapshot(cl), ORACLE['persisted'])
    before = snapshot(cl); assert cl.setspectrum(0, type='constant'); assert cl.done()
    assert cl.open(path, nomodify=True); compare(snapshot(cl), before); assert cl.done()
    tb = table(); assert tb.open(path); assert tb.getkeyword('stable') == 17
    assert ORACLE['frequency_error_after'] == 0.0  # CASA discards the old error.
    assert tb.getcell('Frequency_Error', 0) == 5.0
    assert tb.getcell('Frequency_Error_Units', 0) == 'kHz'
    np.testing.assert_allclose(tb.getcell('Direction_Error', 0), [.001, .002])
    assert tb.close()


def test_contract_marks_setspectrum_mutating():
    obj = next(x for x in schema_bundle()['semantic']['objects'] if x['name'] == 'componentlist')
    assert next(x for x in obj['methods'] if x['name'] == 'setspectrum')['mutating'] is True
