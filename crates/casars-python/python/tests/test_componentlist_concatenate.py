"""CASA component table concatenation, selection safety, and persistence."""
import json
import sys
import tarfile
from pathlib import Path

import pytest

from casars.data import schema_bundle
from casars.tools import componentlist

DATA = Path(__file__).parent / 'data'
ORACLE = json.loads((DATA / 'casa_componentlist_concatenate.json').read_text())


def source_table(tmp_path):
    with tarfile.open(DATA / 'casa_componentlist_concatenate.cl.tgz') as archive:
        extract = {'filter': 'data'} if sys.version_info >= (3, 12) else {}
        archive.extractall(tmp_path, **extract)
    return str(tmp_path / 'source.cl')


def labels(cl):
    return [cl.getlabel(i) for i in range(cl.length())]


def test_table_inputs_match_casa_or_correct_wrong_component(tmp_path):
    path = source_table(tmp_path)
    for case in ORACLE['cases']:
        cl = componentlist()
        assert cl.addcomponent(flux=9, label='existing')
        which = case['which']
        args = [path] if which is None else [path, which]
        assert cl.concatenate(*args) is True
        if which is None or not which or which[0] < 0:
            expected = ['existing', 'source0', 'source1', 'source2']
        else:
            expected = ['existing'] + [f'source{i}' for i in which if 0 <= i < 3]
        assert labels(cl) == expected
        if 'error' in case:
            assert case['error']['message'] == 'Failed AlwaysAssert index < nelements()'
            assert case['labels'] == ['existing', 'source0', 'source1', 'source2']
        elif which in ([1], [2, 0], [0, 0], [99, 0]):
            assert case['labels'] != expected  # CASA copies the wrong rows.
        else:
            assert case['labels'] == expected
        assert cl.done()


def test_invalid_inputs_do_not_change_existing_list(tmp_path):
    path = source_table(tmp_path)
    cl = componentlist()
    assert cl.addcomponent(flux=9, label='existing')
    for case in ORACLE['other']:
        value = case['value']
        if 'error' in case:
            with pytest.raises(RuntimeError, match='Cannot read componentlist'):
                cl.concatenate(value)
        else:
            assert cl.concatenate(value) is case['result']
        assert labels(cl) == ['existing']
    with pytest.raises(NotImplementedError, match='#1053'):
        cl.concatenate({'nelements': 0})
    assert labels(cl) == ['existing']
    for case in ORACLE['pending']:
        assert case['error']['type'] == 'SystemError'
        assert case['labels'] == ['existing', 'source0', 'source1', 'source2']
        with pytest.raises(SystemError):
            cl.concatenate(path, case['which'])
        assert labels(cl) == ['existing']
    assert cl.done()


def test_concatenate_persists_components_and_selection(tmp_path):
    path = source_table(tmp_path)
    out = str(tmp_path / 'target.cl')
    cl = componentlist()
    assert cl.addcomponent(flux=9, label='existing')
    assert cl.select([0])
    assert cl.rename(out)
    assert cl.concatenate(path, [2, 0])
    assert list(cl.selected()) == [0]
    assert cl.getfluxvalue(1)[0].real == pytest.approx(3)
    assert cl.getfluxvalue(2)[0].real == pytest.approx(1)
    assert cl.done()
    assert cl.open(out)
    assert labels(cl) == ['existing', 'source2', 'source0']
    assert cl.getfluxvalue(1)[0].real == pytest.approx(3)
    assert cl.done()


def test_unreadable_directory_is_not_modified(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    cl = componentlist()
    with pytest.raises(RuntimeError, match='Cannot read componentlist'):
        cl.concatenate('')
    with pytest.raises(RuntimeError, match='Cannot read componentlist'):
        cl.concatenate(str(tmp_path))
    assert list(tmp_path.iterdir()) == []
    assert cl.done()


def test_concatenate_contract():
    contract = next(x for x in schema_bundle()['semantic']['objects'] if x['name'] == 'componentlist')
    method = next(x for x in contract['methods'] if x['name'] == 'concatenate')
    assert method['mutating'] is True
