#!/usr/bin/env python3
"""Capture CASA's mixed string and numeric custom arrays."""
import json
import shutil
import tempfile
from pathlib import Path
import numpy as np
from casatasks import imhead
from casatools import image, table, version_string


def plain(value):
    if hasattr(value, 'tolist'): return value.tolist()
    if isinstance(value, dict): return {str(k):plain(v) for k,v in value.items()}
    if isinstance(value, (tuple,list)): return [plain(v) for v in value]
    return value


def main():
    root = Path('crates/casars-imexplore/tests/data/imhead_crpix')
    arrays = [[1,'a'], [True,'a'], [False,'a'], [1.0,'a'], [-0.0,'a'], [1e-5,'a'],
              [1e20,'a'], [1.2345678901234567,'a'], [9223372036854775807,'a'],
              [0.0000005,'a'], [[1,'a'],[2.5,'b']], [[True,'a'],[1,'b']]]
    cases = arrays + [{'mixed':a} for a in arrays]
    expected = {'casa_version':version_string(), 'cases':[]}
    with tempfile.TemporaryDirectory(prefix='imhead-custom-mixed-') as temp:
        for index,value in enumerate(cases):
            path = Path(temp) / f'{index}.im'; shutil.copytree(root / 'normal.im',path)
            ia = image(); ia.open(str(path)); count = len(ia.history(list=False)); ia.done()
            imhead(str(path), mode='put', hdkey='custom', hdvalue=value)
            ia = image(); ia.open(str(path)); history = [line.replace(str(path), '<IMAGE>') for line in ia.history(list=False)[count:]]; ia.done()
            tb = table(); tb.open(str(path)); stored = tb.getkeyword('miscinfo')['custom']; tb.close()
            case = {'value':value, 'history':history, 'stored':plain(stored)}
            if isinstance(stored, np.ndarray): case['shape'] = list(stored.shape)
            else: case['get'] = plain(imhead(str(path), mode='get', hdkey='custom'))
            expected['cases'].append(case)
    (root / 'custom_mixed_expected.json').write_text(json.dumps(expected,indent=2,sort_keys=True)+'\n')


if __name__ == '__main__': main()
