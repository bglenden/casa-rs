"""Independent CASA 6.7.6.14 geometry-tool acceptance."""
import json
from pathlib import Path
import numpy as np
import pytest
from casars.tools import measures

CASES=json.loads((Path(__file__).with_name('data')/'casa_measures_geometry.json').read_text())['cases']

def arguments(value):
    if isinstance(value,dict):
        if set(value)=={'__python_bytes__'}:return bytes(value['__python_bytes__'])
        return {k:arguments(v) for k,v in value.items()}
    if isinstance(value,list):return [arguments(v) for v in value]
    return value

def compare(actual,expected):
    if isinstance(expected,dict):
        assert actual.keys()==expected.keys()
        if expected.get('type') in ['baseline','uvw'] and all('m'+str(i) in expected for i in range(3)):
            # Compare physical vectors, retaining exact field/unit/array shapes.
            # Longitude/latitude are undefined when an offset leaves zero length.
            for i in range(3):
                key='m'+str(i)
                assert actual[key].keys()==expected[key].keys()
                assert actual[key]['unit']==expected[key]['unit']
                assert np.asarray(actual[key]['value']).shape==np.asarray(expected[key]['value']).shape
            def xyz(record):
                lon,lat,r=[np.asarray(record['m'+str(i)]['value']) for i in range(3)]
                return np.array([r*np.cos(lat)*np.cos(lon),r*np.cos(lat)*np.sin(lon),r*np.sin(lat)])
            np.testing.assert_allclose(xyz(actual),xyz(expected),rtol=2e-11,atol=2e-7,equal_nan=True)
            for name,value in expected.items():
                if name not in ['m0','m1','m2']:compare(actual[name],value)
        else:
            for name,value in expected.items():compare(actual[name],value)
    elif isinstance(expected,(list,tuple)):
        assert len(actual)==len(expected)
        for left,right in zip(actual,expected):compare(left,right)
    elif isinstance(expected,bool):assert actual is expected
    elif isinstance(expected,(int,float)):
        np.testing.assert_allclose(actual,expected,rtol=2e-11,atol=2e-7,equal_nan=True)
    else:assert actual==expected

@pytest.mark.parametrize('case',CASES,ids=lambda c:c['name'])
def test_geometry_matches_independent_casa(case):
    tool=measures()
    try:
        tool.done()
        for frame in case.get('frame',[]):assert tool.doframe(arguments(frame))
        call=lambda:getattr(tool,case['method'])(*arguments(case['args']),**arguments(case['kwargs']))
        if 'error' in case:
            with pytest.raises({'TypeError':TypeError,'RuntimeError':RuntimeError,'ValueError':ValueError}[case['error']['type']]) as error:call()
            assert str(error.value)==case['error']['message']
        else:
            actual=call()
            assert type(actual).__name__==case['python_type']
            compare(actual,case.get('native_result',case['result']))
    finally:tool.done()
