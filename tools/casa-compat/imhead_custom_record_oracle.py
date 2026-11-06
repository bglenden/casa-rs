#!/usr/bin/env python3
"""Capture CASA custom-record header operations and a CASA-created fixture."""
import json
import shutil
import tempfile
from pathlib import Path
import numpy as np
from casatasks import imhead
from casatools import image, version_string


def plain(value):
    if hasattr(value, 'tolist'): return value.tolist()
    if isinstance(value, dict): return {str(k): plain(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)): return [plain(v) for v in value]
    return value


def main():
    root = Path('crates/casars-imexplore/tests/data/imhead_crpix')
    fixture = root / 'custom_record.im'
    if fixture.exists(): shutil.rmtree(fixture)
    shutil.copytree(root / 'normal.im', fixture)
    info = {'custom': {'a': True, 'b': np.array([1, 2], dtype=np.int32),
        'matrix': np.array([[1, 2, 3], [4, 5, 6]], dtype=np.int32),
        'complex': np.array([[1+2j, 3+4j], [5+6j, 7+8j]], dtype=np.complex128),
        'nested': {'x': 'abc'}}}
    ia = image(); ia.open(str(fixture)); ia.setmiscinfo(info); ia.done()
    for lock in fixture.rglob('table.lock'): lock.unlink()
    for path in fixture.rglob('table.info'):
        path.write_text('\n'.join(line.rstrip() for line in path.read_text().splitlines()).rstrip() + '\n')
    records = [{}, {'a':'x','b':2}, {'unit':'GHz','value':1.2}, {'x':True,'y':False},
               {'x':1.2345678901234}, {'a':[True,False], 'b':[]},
               {'a':['x','y'], 'b':[1,2.5]}, {'nested':{}}, {'nested':{'x':'a"b'}},
               {'a':[1,2], 'nested':{'x':3.5}}]
    cases = [[('put', value)] for value in records] + [
        [('add', {'a':1}), ('put', {'b':2})],
        [('add', {'a':1}), ('add', {'b':2})],
        [('add', {'a':1}), ('put', 3)],
        [('add', 3), ('put', {'a':1})],
        [('add', {'a':1}), ('del', '')],
    ]
    expected = {'casa_version': version_string(), 'cases': []}
    with tempfile.TemporaryDirectory(prefix='imhead-custom-record-') as temp:
        for index, operations in enumerate(cases):
            path = Path(temp) / f'{index}.im'; shutil.copytree(root / 'normal.im', path)
            case = {'operations': []}
            for mode, value in operations:
                ia = image(); ia.open(str(path)); count = len(ia.history(list=False)); ia.done()
                op = {'mode':mode, 'value':value}
                try: imhead(str(path), mode=mode, hdkey='custom', hdvalue=value)
                except RuntimeError as error: op['error'] = str(error).split(' at File:', 1)[0]
                ia = image(); ia.open(str(path)); op['history'] = [line.replace(str(path), '<IMAGE>') for line in ia.history(list=False)[count:]]; ia.done()
                case['operations'].append(op)
            try: case['get'] = plain(imhead(str(path), mode='get', hdkey='custom'))
            except RuntimeError as error: case['get_error'] = str(error).split(' at File:', 1)[0]
            expected['cases'].append(case)
    (root / 'custom_record_expected.json').write_text(json.dumps(expected, indent=2, sort_keys=True) + '\n')


if __name__ == '__main__': main()
