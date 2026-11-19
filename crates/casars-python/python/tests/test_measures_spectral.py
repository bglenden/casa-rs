"""CASA 6.7.6.14 spectral velocity construction and conversion acceptance."""
import json
import subprocess
import sys
from pathlib import Path

import numpy as np
import pytest
from casars.tools import measures

CASES = json.loads((Path(__file__).with_name('data') / 'casa_measures_spectral.json').read_text())['cases']


def arguments(value):
    if isinstance(value,dict):
        if set(value)=={'__python_bytes__'}:return bytes(value['__python_bytes__'])
        if set(value)=={'__numpy_array__'}:
            array=value['__numpy_array__'];return np.array(array['values'],dtype=array['dtype'])
        return {k:arguments(v) for k,v in value.items()}
    if isinstance(value,list):return [arguments(v) for v in value]
    return value


def compare(actual, expected):
    if isinstance(expected, dict):
        assert set(actual) == set(expected)
        for name, value in expected.items():compare(actual[name], value)
    elif isinstance(expected, bool):
        assert actual is expected
    elif isinstance(expected, list) and (not expected or any(isinstance(v, str) for v in expected)):
        assert list(actual) == expected
    elif isinstance(expected, (int, float, list)):
        np.testing.assert_allclose(actual, expected, rtol=2e-11, atol=2e-7, equal_nan=True)
    else:
        assert actual == expected


@pytest.mark.parametrize('case', CASES, ids=lambda c:c['name'])
def test_spectral_measure_matches_casa(case, caplog):
    if 'unit_definitions' in case:
        # Unit registries are global. Keep the independently captured CASA
        # redefinition case isolated from every other test.
        code = '''
import json,sys
from casars.tools import measures,quanta
case=json.load(sys.stdin)
for name,value in case['unit_definitions']:quanta().define(name,value)
print(json.dumps(getattr(measures(),case['method'])(*case['args'],**case['kwargs'])))
'''
        child=subprocess.run([sys.executable,'-c',code],input=json.dumps(case),capture_output=True,text=True,timeout=10)
        assert child.returncode == 0,child.stderr
        compare(json.loads(child.stdout),case['result'])
        return
    tool = measures()
    try:
        for frame in case.get('frame', []):assert tool.doframe(arguments(frame))
        if 'frame_builder' in case:
            builder=case['frame_builder']
            assert tool.doframe(getattr(tool,builder['method'])(*arguments(builder['args'])))
        if 'error' in case:
            with pytest.raises({'TypeError':TypeError,'RuntimeError':RuntimeError,'ValueError':ValueError}[case['error']['type']]) as error:
                getattr(tool, case['method'])(*arguments(case['args']), **arguments(case['kwargs']))
            assert str(error.value) == case['error']['message']
        else:
            expected = case.get('native_result', case['result'])
            if 'scalar_controls' in case:
                # CASA's vector path rejects/truncates valid data. Its independent
                # scalar calls supply every expected element for the correction.
                controls = case['scalar_controls']
                expected = {**controls[0], **{
                    name: {'unit': value['unit'], 'value': [c[name]['value'] for c in controls]}
                    for name, value in controls[0].items() if name.startswith('m')
                }}
            compare(getattr(tool, case['method'])(*arguments(case['args']), **arguments(case['kwargs'])), expected)
            if 'owner_decision' in case:
                assert case['owner_decision'] == 'measures-tool-default-frame'
                assert any('Cannot convert due to missing frame information' in m for m in caplog.messages)
    finally:
        tool.done()


@pytest.mark.parametrize('expression', [
    "x={};x['self']=x;call=lambda:m.doppler(v0=x)",
    "x=[];x.append(x);call=lambda:m.doppler(v0=x)",
    "x={};x['self']=x;call=lambda:m.todoppler('RADIO',x,'1GHz')",
])
def test_cyclic_spectral_inputs_raise_without_terminating_process(expression):
    import subprocess
    import sys
    code = """
import resource
resource.setrlimit(resource.RLIMIT_CORE,(0,0))
from casars.tools import measures
m=measures()
EXPRESSION
try:
    call()
except ValueError as error:
    assert 'cyclic' in str(error) and '#1252' in str(error)
else:
    raise AssertionError('cyclic input was accepted')
# Capture depth is restored after failure, including repeated calls.
for i in range(2):
    try:call()
    except ValueError:pass
    else:raise AssertionError('repeated cyclic input was accepted')
assert m.doppler()['type']=='doppler'
""".replace('EXPRESSION',expression)
    result = subprocess.run([sys.executable,'-c',code],capture_output=True,text=True,timeout=10)
    assert result.returncode == 0, result.stderr
