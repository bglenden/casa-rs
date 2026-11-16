#!/usr/bin/env python3
"""Record CASA's conversion of NumPy scalars in imhead values."""
import json
import shutil
import tempfile
from pathlib import Path

import numpy as np
from casatasks import imhead
from casatools import version_string


def main():
    root = Path('crates/casars-imexplore/tests/data/imhead_crpix')
    dtypes = ['bool', 'int8', 'int16', 'int32', 'int64', 'uint8', 'uint16',
              'uint32', 'uint64', 'float32', 'float64', 'complex64', 'complex128', 'str_']
    cases = []
    with tempfile.TemporaryDirectory(prefix='imhead-numpy-scalar-') as temp:
        for dtype in dtypes:
            scalar = getattr(np, dtype)('abc' if dtype == 'str_' else 1 + 2j if dtype.startswith('complex') else 3)
            case = {'dtype': dtype, 'input': [1, 2] if dtype.startswith('complex') else 'abc' if dtype == 'str_' else 3}
            for context in ('root', 'record', 'list'):
                path = Path(temp) / f'{len(cases)}-{context}.im'; shutil.copytree(root / 'normal.im', path)
                given = scalar if context == 'root' else {'value': scalar if context == 'record' else [scalar, 2]}
                try:
                    imhead(str(path), mode='put', hdkey='custom', hdvalue=given)
                    result = imhead(str(path), mode='get', hdkey='custom')
                    if context == 'record':
                        result = {'present': 'value' in result, 'value': result.get('value')}
                        if isinstance(result['value'], complex): result['value'] = [result['value'].real, result['value'].imag]
                    elif context == 'list':
                        result = result['value']; result = {'dtype': str(result.dtype), 'values': [[z.real, z.imag] for z in result] if result.dtype.kind == 'c' else result.tolist()}
                    case[context] = {'value': result}
                except (AssertionError, RuntimeError) as error:
                    case[context] = {'error': type(error).__name__}
            cases.append(case)
    (root / 'numpy_scalar_expected.json').write_text(json.dumps({'casa_version': version_string(), 'cases': cases}, indent=2) + '\n')


if __name__ == '__main__':
    main()
