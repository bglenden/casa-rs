"""CASA flux polarisation conversion and preservation of unrelated data."""
import json
import shutil
import subprocess
import sys
import tarfile
from pathlib import Path

import numpy as np
import pytest

from casars.data import schema_bundle
from casars.tools import componentlist, table

DATA = Path(__file__).parent / 'data'
ORACLE = json.loads((DATA / 'casa_componentlist_convertfluxpol.json').read_text())


def source_table(tmp_path):
    with tarfile.open(DATA / 'casa_componentlist_convertfluxpol.cl.tgz') as archive:
        extract = {'filter': 'data'} if sys.version_info >= (3, 12) else {}
        archive.extractall(tmp_path, **extract)
    return tmp_path / 'source.cl'


def cells(path):
    tb = table()
    assert tb.open(str(path))
    result = []
    for i in range(tb.nrows()):
        values = tb.getcell('Flux', i)
        errors = tb.getcell('Flux_Error', i)
        result.append({'pol': tb.getcell('Flux_Polarisation', i),
                       'values': np.array([[z.real, z.imag] for z in values]),
                       'errors': np.array([[z.real, z.imag] for z in errors]),
                       'label': tb.getcell('Label', i),
                       'direction_error': tb.getcell('Direction_Error', i),
                       'keyword': tb.getkeyword('stable')})
    assert tb.close()
    return result


def assert_cells(actual, expected):
    assert len(actual) == len(expected)
    for a, e in zip(actual, expected):
        assert a['pol'] == e['pol']
        assert a['label'] == e['label']
        assert a['keyword'] == e['keyword']
        np.testing.assert_allclose(a['direction_error'], e['direction_error'], rtol=0, atol=0)
        np.testing.assert_allclose(a['values'], e['values'], rtol=1e-14, atol=1e-14)
        np.testing.assert_allclose(a['errors'], e['errors'], rtol=0, atol=0)


def test_convert_flux_pol_matches_casa_for_complex_table_values(tmp_path):
    source = source_table(tmp_path)
    assert_cells(cells(source), ORACLE['before'])
    for i, case in enumerate(ORACLE['cases']):
        destination = tmp_path / f'case-{i}.cl'
        shutil.copytree(source, destination)
        cl = componentlist()
        assert cl.open(str(destination), nomodify=False)
        assert cl.convertfluxpol(case['which'], case['target']) is case['result']
        assert cl.done()
        assert_cells(cells(destination), case['cells'])
    assert_cells(cells(source), ORACLE['before'])


def test_arguments_and_errors_match_casa_without_partial_mutation():
    for case in ORACLE['arguments']:
        cl = componentlist()
        assert cl.addcomponent(flux=[5, 2, 3, 1])
        if 'error' in case:
            expected = case['error']
            error_type = {'RuntimeError': RuntimeError, 'TypeError': TypeError,
                          'SystemError': SystemError}[expected['type']]
            with pytest.raises(error_type) as error:
                cl.convertfluxpol(*case['args'])
            assert expected['message'] in str(error.value)
            # CASA changes all components on a failed index conversion.
            pol = 'Stokes' if expected['type'] == 'SystemError' else case['polarisation']
        else:
            assert cl.convertfluxpol(*case['args']) is case['result']
            pol = case['polarisation']
        assert cl.getcomponent(0)['flux']['polarisation'] == pol
        assert cl.done()


def test_empty_all_is_catchable():
    assert ORACLE['empty_all_returncode'] == -11
    cl = componentlist()
    with pytest.raises(RuntimeError, match='empty list'):
        cl.convertfluxpol(-1, 'Linear')
    assert cl.length() == 0
    assert cl.done()


def test_read_only_table_changes_only_in_memory(tmp_path):
    source = source_table(tmp_path)
    cl = componentlist()
    assert cl.open(str(source), nomodify=True)
    assert cl.convertfluxpol(0, 'Linear')
    assert cl.getcomponent(0)['flux']['polarisation'] == 'Linear'
    assert cl.done()
    assert_cells(cells(source), ORACLE['before'])


def test_nonzero_errors_are_preserved_and_warned(tmp_path, caplog):
    source = source_table(tmp_path)
    cl = componentlist()
    assert cl.open(str(source), nomodify=False)
    assert cl.convertfluxpol(0, 'Linear')
    assert 'errors in the flux are not being changed' in caplog.text
    assert cl.done()
    assert_cells(cells(source), ORACLE['cases'][1]['cells'])


def test_invalid_polarisation_is_logged_without_changing_values(caplog):
    cl = componentlist()
    assert cl.addcomponent(flux=[5, 2, 3, 1])
    with pytest.raises(RuntimeError, match='Unknown polarization'):
        cl.convertfluxpol(0, 'bad')
    assert 'Unknown polarization' in caplog.text
    assert list(cl.getfluxvalue(0)) == [5, 2, 3, 1]
    assert cl.done()


def test_redefined_unit_does_not_discard_existing_flux():
    # Unit definitions are process-wide; keep this redefinition isolated.
    code = ("from casars.tools import componentlist,quanta; "
            "c=componentlist(); c.addcomponent(flux=[5,2,3,1]); "
            "q=quanta(); q.define('Jy','1m'); "
            "assert c.convertfluxpol(0,'Linear'); "
            "assert c.getfluxunit(0)=='Jy'; "
            "assert list(c.getfluxvalue(0))==[7,3,3,3]; c.done()")
    subprocess.run([sys.executable, '-c', code], check=True, capture_output=True, text=True)


def test_contract_marks_conversion_mutating():
    contract = next(x for x in schema_bundle()['semantic']['objects'] if x['name'] == 'componentlist')
    method = next(x for x in contract['methods'] if x['name'] == 'convertfluxpol')
    assert method['mutating'] is True
