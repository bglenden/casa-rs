#!/usr/bin/env python3
"""Record CASA custom-header behavior for zero-dimensional NumPy arrays."""
import json
import shutil
import tempfile
from pathlib import Path

import numpy as np
from casatasks import imhead
from casatools import version_string


def main():
    root = Path('crates/casars-imexplore/tests/data/imhead_crpix')
    samples = [('bool', True), ('int64', 4), ('float64', 1.5),
               ('complex128', [1, 2]), ('U3', 'abc')]
    cases = []
    with tempfile.TemporaryDirectory(prefix='imhead-zero-dim-') as temp:
        for index, (dtype, value) in enumerate(samples):
            path = Path(temp) / f'{index}.im'; shutil.copytree(root / 'normal.im', path)
            scalar = complex(*value) if dtype == 'complex128' else value
            array = np.array(scalar, dtype=dtype)
            imhead(str(path), mode='put', hdkey='custom', hdvalue={'value': array})
            got = imhead(str(path), mode='get', hdkey='custom')['value']
            cases.append({'input_dtype': dtype, 'input': value, 'dtype': str(got.dtype),
                          'shape': list(got.shape), 'values': got.tolist()})
    (root / 'zero_dim_expected.json').write_text(json.dumps(
        {'casa_version': version_string(), 'cases': cases}, indent=2) + '\n')


if __name__ == '__main__':
    main()
