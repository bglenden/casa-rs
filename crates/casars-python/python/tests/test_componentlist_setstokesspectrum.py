"""CASA Stokes spectra, validation and table persistence."""
import json
import sys
import tarfile
from pathlib import Path

import numpy as np
import pytest
from casars.data import schema_bundle
from casars.tools import componentlist, table

DATA = Path(__file__).parent / 'data'
ORACLE = json.loads((DATA / 'casa_componentlist_setstokesspectrum.json').read_text())


def compare(actual, expected):
    if isinstance(expected, dict):
        assert actual.keys() == expected.keys()
        for key in expected: compare(actual[key], expected[key])
    elif isinstance(expected, (list, np.ndarray)):
        assert len(actual) == len(expected)
        for left, right in zip(actual, expected): compare(left, right)
    elif isinstance(expected, float):
        assert actual == pytest.approx(expected, rel=1e-13, abs=1e-13)
    else:
        assert actual == expected


def snapshot(c): return [c.getcomponent(i) for i in range(c.length())]


def test_matches_casa_or_preserves_component_after_unsafe_failure():
    for case in ORACLE['cases']:
        c=componentlist(); assert c.addcomponent(flux=3,dir='J2000 10deg 20deg',freq='LSRK 2GHz',label='first')
        assert c.addcomponent(flux=5,dir='GALACTIC 30deg -15deg',freq='LSRK 1.4GHz',label='second')
        compare(snapshot(c), case['before'])
        which=np.bool_(case['which']) if case['case'].startswith('numpy_bool_') else case['which']
        kwargs=case['kwargs'].copy()
        if case['case']=='empty_ref_float': kwargs['reffreq']=np.array([],dtype=float)
        if case['case']=='empty_ref_int': kwargs['reffreq']=np.array([],dtype=int)
        if case['case'] == 'wrong_refunit':
            with pytest.raises(RuntimeError,match='frequency'): c.setstokesspectrum(which,**kwargs)
            expected = case['before']
        elif 'error' in case:
            error=case['error']; typ={'TypeError':TypeError,'SystemError':SystemError,'RuntimeError':RuntimeError}[error['type']]
            with pytest.raises(typ) as caught: c.setstokesspectrum(which,**kwargs)
            msg=error['message'].split(') ')[-1] if 'Failed AlwaysAssert' in error['message'] else error['message']
            assert msg in str(caught.value), case['case']
            expected=case['before'] if case['case'] in ('mismatch_frame','tab_none') else case['after']
        else:
            assert c.setstokesspectrum(which,**kwargs) is case['result'],case['case']
            expected=case['before'] if case['case']=='plp_full' else case['after']
        compare(snapshot(c),expected);assert c.done()


def test_rejects_unsafe_indices_and_nonfinite_values():
    c=componentlist();assert c.simulate();before=snapshot(c)
    for kwargs in ({'index':[]},{'index':[float('nan')]},
                   {'type':'tabular','tabularfreqs':[1e9,2e9],'tabulari':[1,float('nan')],
                    'tabularq':[0,0],'tabularu':[0,0],'tabularv':[0,0]}):
        with pytest.raises(RuntimeError,match='#1053'): c.setstokesspectrum(0,**kwargs)
        compare(snapshot(c),before)
    assert c.done()


@pytest.mark.parametrize('value', [np.bool_(False), np.bool_(True)])
def test_numpy_boolean_index_matches_casa(value):
    c=componentlist(); assert c.simulate(); before=snapshot(c)
    with pytest.raises(RuntimeError,match='which\\(i\\) >= 0'):
        c.setstokesspectrum(value)
    compare(snapshot(c),before); assert c.done()


@pytest.mark.parametrize('reference', [[], np.array([], dtype=float), np.array([], dtype=int)])
def test_empty_reference_uses_casa_default(reference):
    c=componentlist(); assert c.simulate(); assert c.setstokesspectrum(0, reffreq=reference)
    assert c.getspectrum(0)['frequency']['m0']['value'] == pytest.approx(1.4)
    assert c.done()


def test_table_retains_frequency_and_other_metadata(tmp_path):
    with tarfile.open(DATA/'casa_componentlist_setspectrum.cl.tgz') as archive:
        archive.extractall(tmp_path, **({'filter':'data'} if sys.version_info >= (3,12) else {}))
    path=str(tmp_path/'source.cl');c=componentlist();assert c.open(path,nomodify=False)
    assert c.setstokesspectrum(0,type='spectral',index=[-.7,.1,2,.3],reffreq='1.2GHz')
    spectrum=c.getspectrum(0); assert c.done();assert c.open(path,nomodify=True)
    compare(c.getspectrum(0),spectrum);assert c.done()
    tb=table();assert tb.open(path);assert tb.getkeyword('stable')==17
    assert tb.getcell('Frequency_Error',0)==5.;assert tb.getcell('Frequency_Error_Units',0)=='kHz'
    np.testing.assert_allclose(tb.getcell('Direction_Error',0),[.001,.002]);assert tb.close()


def test_contract_marks_mutating():
    obj=next(x for x in schema_bundle()['semantic']['objects'] if x['name']=='componentlist')
    assert next(x for x in obj['methods'] if x['name']=='setstokesspectrum')['mutating'] is True
