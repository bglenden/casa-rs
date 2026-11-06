#!/usr/bin/env python3
"""Have CASA reopen zero-dimensional parameter arrays written by casa-rs."""
import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import numpy as np
from casatasks import imhead
from casatools import version_string


def main():
    binary = Path(sys.argv[1]).resolve()
    root = Path('crates/casars-imexplore/tests/data/imhead_crpix')
    oracle = json.loads((root / 'zero_dim_expected.json').read_text())
    assert version_string() == oracle['casa_version']
    kinds = {'bool': 'bool', 'int64': 'integer', 'float64': 'float',
             'complex128': 'complex', 'U3': 'string'}
    with tempfile.TemporaryDirectory(prefix='imhead-zero-interop-') as temp:
        for index, case in enumerate(oracle['cases']):
            path = Path(temp) / f'{index}.im'; shutil.copytree(root / 'normal.im', path)
            scalar = {'kind': kinds[case['input_dtype']], 'value': case['input']}
            typed = {'kind': 'table', 'value': {'value': {'kind': 'scalar_array', 'value': scalar}}}
            request = {'surface': 'imhead', 'values': {'imagename': str(path), 'mode': 'put',
                       'hdkey': 'custom'}, 'typed_values': {'hdvalue': typed}}
            result = subprocess.run([str(binary), 'imhead', '--json-run', '-'], input=json.dumps(request),
                                    text=True, capture_output=True, check=True)
            assert json.loads(result.stdout)['status'] == 'succeeded', result.stdout
            actual = imhead(str(path), mode='get', hdkey='custom')['value']
            assert actual.dtype == np.dtype(case['dtype']), (case, actual.dtype)
            assert actual.shape == tuple(case['shape']) and actual.tolist() == case['values'], (case, actual)
    print(f"CASA reopened {len(oracle['cases'])} zero-dimensional record arrays")


if __name__ == '__main__':
    main()
