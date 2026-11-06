#!/usr/bin/env python3
"""Capture CASA custom header array writes, reads, and deletions."""
import json
import shutil
import tempfile
from pathlib import Path
import numpy as np
from casatasks import imhead
from casatools import image, table, version_string


def main():
    root = Path('crates/casars-imexplore/tests/data/imhead_crpix')
    fixtures = {
        'bool': np.array([True, False], dtype=np.bool_),
        'int': np.array([1, 2, 3], dtype=np.int32),
        'float': np.array([1.0, 2.5], dtype=np.float64),
        'string': np.array(['x', 'y']),
        'empty': np.array([], dtype=np.bool_),
    }
    for name, values in fixtures.items():
        path = root / f'custom_array_{name}.im'
        if path.exists(): shutil.rmtree(path)
        shutil.copytree(root / 'normal.im', path)
        ia = image(); ia.open(str(path)); ia.setmiscinfo({'custom': values}); ia.done()
        for lock in path.rglob('table.lock'): lock.unlink()
        for info in path.rglob('table.info'):
            info.write_text('\n'.join(line.rstrip() for line in info.read_text().splitlines()).rstrip() + '\n')
    cases = [
        [('add', [1, 2, 3])], [('put', [1.0, 2.5])], [('add', [True, False])],
        [('add', ['x', 'y'])], [('add', [])], [('add', [1, 2.5])],
        [('add', [True, 2])], [('add', [True, 1.5])],
        [('add', [2147483648, 1.0])],
        [('add', [1.234567890123456, 123456789.1234, 1e-9])],
        [('add', [1, 2]), ('put', [3, 4, 5])],
        [('add', [1, 2]), ('put', [1.0, 2.0])],
        [('add', [1, 2]), ('add', [3, 4])],
        [('add', [1, 2]), ('del', [1, 2])],
        [('add', [1, 2]), ('del', {'ignored': 'yes'})],
    ]
    expected = {'casa_version': version_string(), 'cases': []}
    with tempfile.TemporaryDirectory(prefix='imhead-custom-array-') as temp:
        for index, operations in enumerate(cases):
            path = Path(temp) / f'{index}.im'; shutil.copytree(root / 'normal.im', path)
            case = {'operations': []}
            for mode, value in operations:
                ia = image(); ia.open(str(path)); count = len(ia.history(list=False)); ia.done()
                op = {'mode': mode, 'value': value}
                try: imhead(str(path), mode=mode, hdkey='custom', hdvalue=value)
                except RuntimeError as error: op['error'] = str(error).split(' at File:', 1)[0]
                ia = image(); ia.open(str(path)); op['history'] = [line.replace(str(path), '<IMAGE>') for line in ia.history(list=False)[count:]]; ia.done()
                case['operations'].append(op)
            tb = table(); tb.open(str(path)); info = tb.getkeyword('miscinfo'); tb.close()
            if 'custom' in info:
                value = info['custom']; assert isinstance(value, np.ndarray)
                case['stored'] = {'kind': value.dtype.kind, 'data': value.tolist()}
                try: imhead(str(path), mode='get', hdkey='custom')
                except RuntimeError as error: case['get_error'] = str(error).split(' at File:', 1)[0]
            expected['cases'].append(case)
    (root / 'custom_array_expected.json').write_text(json.dumps(expected, indent=2, sort_keys=True) + '\n')


if __name__ == '__main__':
    main()
