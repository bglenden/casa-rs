"""Direction-frame conversion against CASA and table persistence."""
import json
import sys
import tarfile
from pathlib import Path

import numpy as np
import pytest

from casars.data import schema_bundle
from casars.tools import componentlist, table

DATA = Path(__file__).parent / 'data'
ORACLE = json.loads((DATA / 'casa_componentlist_convertrefdir.json').read_text())


def compare(actual, expected):
    if isinstance(expected, dict):
        assert actual.keys() == expected.keys()
        for key in expected: compare(actual[key], expected[key])
    elif isinstance(expected, (list, np.ndarray)):
        assert len(actual) == len(expected)
        for left, right in zip(actual, expected): compare(left, right)
    elif isinstance(expected, float):
        assert actual == pytest.approx(expected, rel=1e-9, abs=1e-10)
    else:
        assert actual == expected


def snapshot(cl):
    return [cl.getcomponent(i) for i in range(cl.length())]


def test_conversion_matches_casa():
    for case in ORACLE['cases']:
        c = componentlist()
        assert c.addcomponent(flux=3, dir='J2000 10deg 20deg', label='first', freq='LSRK 1.4GHz')
        assert c.addcomponent(flux=5, dir='J2000 210deg -15deg', label='second', freq='LSRK 2GHz')
        compare(snapshot(c), case['before'])
        frame = case['frame'].encode() if case['case'] == 'frame_bytes' else case['frame']
        which = np.bool_(case['which']) if case['case'].startswith('numpy_bool_') else case['which']
        if 'error' in case:
            error = case['error']
            with pytest.raises({'TypeError': TypeError, 'RuntimeError': RuntimeError}[error['type']]) as caught:
                c.convertrefdir(which, frame)
            msg = error['message'].split(') ')[-1] if 'Failed AlwaysAssert' in error['message'] else error['message']
            assert msg in str(caught.value), case['case']
        else:
            assert c.convertrefdir(which, frame) is case['result'], case['case']
        compare(snapshot(c), case['after']); assert c.done()


def test_table_and_direction_errors_are_preserved(tmp_path):
    with tarfile.open(DATA / 'casa_componentlist_setspectrum.cl.tgz') as archive:
        archive.extractall(tmp_path, **({'filter': 'data'} if sys.version_info >= (3, 12) else {}))
    path = str(tmp_path / 'source.cl'); c = componentlist(); assert c.open(path, nomodify=False)
    before = c.getcomponent(0); assert c.select([0]); assert c.convertrefdir(0, 'GALACTIC')
    assert c.selected().tolist() == [0]; after = c.getcomponent(0)
    assert after['shape']['direction']['refer'] == 'GALACTIC'; assert c.done()
    assert c.open(path, nomodify=True); compare(c.getcomponent(0), after)
    assert c.convertrefdir(0, 'J2000'); assert c.done()
    assert c.open(path, nomodify=True); compare(c.getcomponent(0), after); assert c.done()
    tb=table(); assert tb.open(path); assert tb.getkeyword('stable') == 17
    assert tb.getcell('Frequency_Error', 0) == 5.0
    assert tb.getcell('Frequency_Error_Units', 0) == 'kHz'
    np.testing.assert_allclose(tb.getcell('Direction_Error', 0), [.001, .002]); assert tb.close()
    assert before['flux']['unit'] == after['flux']['unit']


def test_contract_marks_conversion_mutating():
    obj = next(x for x in schema_bundle()['semantic']['objects'] if x['name'] == 'componentlist')
    assert next(x for x in obj['methods'] if x['name'] == 'convertrefdir')['mutating'] is True
