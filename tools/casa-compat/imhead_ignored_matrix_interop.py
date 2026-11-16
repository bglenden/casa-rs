#!/usr/bin/env python3
"""Have CASA reopen native records with ignored multidimensional elements."""
import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import numpy as np
from casatasks import imhead
from casatools import version_string
from imhead_ignored_matrix_oracle import cases


def typed(value):
    if isinstance(value, np.ndarray):
        assert value.ndim == 0
        return {'kind': 'scalar_array', 'value': typed(value.item())}
    if isinstance(value, (np.float64, np.str_)):
        value = value.item()
    elif isinstance(value, np.generic):
        return {'kind': 'numpy_scalar', 'value': {'dtype': value.dtype.name, 'value': typed(value.item())}}
    if isinstance(value, bool): return {'kind': 'bool', 'value': value}
    if isinstance(value, int): return {'kind': 'integer', 'value': value}
    if isinstance(value, float): return {'kind': 'float', 'value': value}
    if isinstance(value, complex): return {'kind': 'complex', 'value': [value.real, value.imag]}
    if isinstance(value, str): return {'kind': 'string', 'value': value}
    if isinstance(value, list): return {'kind': 'array', 'value': [typed(item) for item in value]}
    raise TypeError(type(value))


def main():
    binary = Path(sys.argv[1]).resolve()
    root = Path('crates/casars-imexplore/tests/data/imhead_crpix')
    oracle = json.loads((root / 'ignored_matrix_expected.json').read_text())
    assert version_string() == oracle['casa_version']
    with tempfile.TemporaryDirectory(prefix='imhead-ignored-matrix-interop-') as temp:
        for name, value in cases().items():
            path = Path(temp) / f'{name}.im'; shutil.copytree(root / 'normal.im', path)
            request = {'surface': 'imhead', 'values': {'imagename': str(path), 'mode': 'put', 'hdkey': 'custom'},
                       'typed_values': {'hdvalue': {'kind': 'table', 'value': {'value': typed(value)}}}}
            result = subprocess.run([str(binary), 'imhead', '--json-run', '-'], input=json.dumps(request),
                                    text=True, capture_output=True, check=True)
            assert json.loads(result.stdout)['status'] == 'succeeded', (name, result.stdout)
            actual = imhead(str(path), mode='get', hdkey='custom')['value']; expected = oracle['cases'][name]
            assert actual.dtype == np.dtype(expected['dtype']) and actual.shape == tuple(expected['shape']), (name, actual)
            assert actual.tolist() == expected['values'], (name, actual)
    print(f"CASA reopened {len(oracle['cases'])} array promotion cases")


if __name__ == '__main__': main()
