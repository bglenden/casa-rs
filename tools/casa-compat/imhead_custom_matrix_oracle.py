#!/usr/bin/env python3
"""Capture CASA custom-header matrix writes and record reads."""
import json
import shutil
import tempfile
from pathlib import Path
import numpy as np
from casatasks import imhead
from casatools import image, table, version_string


def plain(value):
    if hasattr(value, 'tolist'): return value.tolist()
    if isinstance(value, dict): return {str(k): plain(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)): return [plain(v) for v in value]
    return value


def main():
    root = Path('crates/casars-imexplore/tests/data/imhead_crpix')
    matrices = [[[1,2,3],[4,5,6]], [[1],[2],[3]], [[1,2,3]],
                [[True,False],[False,True]], [[1,2.5],[3.2,4]], [['a','b','c'],['d','e','f']]]
    for name, value in [('int', matrices[0]), ('string', matrices[-1])]:
        path = root / f'custom_matrix_{name}.im'
        if path.exists(): shutil.rmtree(path)
        shutil.copytree(root / 'normal.im', path)
        imhead(str(path), mode='put', hdkey='custom', hdvalue=value)
        for lock in path.rglob('table.lock'): lock.unlink()
        for info in path.rglob('table.info'):
            info.write_text('\n'.join(line.rstrip() for line in info.read_text().splitlines()).rstrip() + '\n')
    cases = [('add', value) for value in matrices] + [('put', {'matrix':value}) for value in matrices]
    expected = {'casa_version':version_string(), 'cases':[]}
    with tempfile.TemporaryDirectory(prefix='imhead-custom-matrix-') as temp:
        for index,(mode,value) in enumerate(cases):
            path = Path(temp) / f'{index}.im'; shutil.copytree(root / 'normal.im', path)
            ia = image(); ia.open(str(path)); count = len(ia.history(list=False)); ia.done()
            case = {'mode':mode, 'value':value}
            try: imhead(str(path), mode=mode, hdkey='custom', hdvalue=value)
            except RuntimeError as error: case['error'] = str(error).split(' at File:', 1)[0]
            ia = image(); ia.open(str(path)); case['history'] = [line.replace(str(path), '<IMAGE>') for line in ia.history(list=False)[count:]]; ia.done()
            tb = table(); tb.open(str(path)); stored = tb.getkeyword('miscinfo')['custom']; tb.close()
            if isinstance(stored, np.ndarray):
                case['stored'] = {'kind':stored.dtype.kind,'data':stored.tolist(),'shape':list(stored.shape)}
                try: imhead(str(path), mode='get', hdkey='custom')
                except RuntimeError as error: case['get_error'] = str(error).split(' at File:', 1)[0]
            else: case['get'] = plain(imhead(str(path), mode='get', hdkey='custom'))
            expected['cases'].append(case)
    (root / 'custom_matrix_expected.json').write_text(json.dumps(expected, indent=2, sort_keys=True) + '\n')


if __name__ == '__main__': main()
