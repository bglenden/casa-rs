#!/usr/bin/env python3
"""Have CASA reopen native imhead writes containing NumPy scalars."""
import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import numpy as np
from casatasks import imhead
from casatools import version_string


def typed_scalar(case):
    dtype = case['dtype']; value = case['input']
    kind = 'bool' if dtype == 'bool' else 'integer' if dtype.startswith(('int', 'uint')) else 'float' if dtype.startswith('float') else 'complex' if dtype.startswith('complex') else 'string'
    if kind == 'bool': value = bool(value)
    if kind == 'float': value = float(value)
    scalar = {'kind': kind, 'value': value}
    if dtype in ('float64', 'str_'): return scalar
    return {'kind': 'numpy_scalar', 'value': {'dtype': dtype, 'value': scalar}}


def main():
    binary = Path(sys.argv[1]).resolve()
    root = Path('crates/casars-imexplore/tests/data/imhead_crpix')
    oracle = json.loads((root / 'numpy_scalar_expected.json').read_text())
    assert version_string() == oracle['casa_version']
    reopened = 0
    with tempfile.TemporaryDirectory(prefix='imhead-numpy-scalar-interop-') as temp:
        for index, case in enumerate(oracle['cases']):
            for context in ('root', 'record', 'list'):
                path = Path(temp) / f'{index}-{context}.im'; shutil.copytree(root / 'normal.im', path)
                scalar = typed_scalar(case)
                value = scalar if context == 'root' else {'kind': 'table', 'value': {'value': scalar if context == 'record' else {'kind': 'array', 'value': [scalar, {'kind': 'integer', 'value': 2}]}}}
                request = {'surface': 'imhead', 'values': {'imagename': str(path), 'mode': 'put', 'hdkey': 'custom'}, 'typed_values': {'hdvalue': value}}
                result = subprocess.run([str(binary), 'imhead', '--json-run', '-'], input=json.dumps(request), text=True, capture_output=True)
                expected = case[context]
                assert (result.returncode != 0) == ('error' in expected), (case, context, result.stdout, result.stderr)
                if 'error' in expected: continue
                actual = imhead(str(path), mode='get', hdkey='custom')
                reopened += 1
                if context == 'root':
                    assert type(actual) is type(expected['value']) and actual == expected['value'], (case, actual)
                elif context == 'record':
                    expected_value = expected['value']; assert ('value' in actual) == expected_value['present'], (case, actual)
                    if expected_value['present']:
                        value = complex(*expected_value['value']) if case['dtype'].startswith('complex') else expected_value['value']
                        assert actual['value'] == value, (case, actual)
                else:
                    actual = actual['value']; expected_value = expected['value']
                    assert actual.dtype == np.dtype(expected_value['dtype']), (case, actual.dtype)
                    values = [[z.real, z.imag] for z in actual] if actual.dtype.kind == 'c' else actual.tolist()
                    assert values == expected_value['values'], (case, values)
    print(f"Checked {len(oracle['cases']) * 3} NumPy scalar cases; CASA reopened {reopened} successful writes")


if __name__ == '__main__': main()
