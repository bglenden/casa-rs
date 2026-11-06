#!/usr/bin/env python3
"""Capture CASA imhead add for missing and existing rest frequencies."""
import json
import shutil
import tempfile
from pathlib import Path
import numpy as np
from casatasks import imhead
from casatools import image, table, version_string


def main():
    root = Path('crates/casars-imexplore/tests/data/imhead_crpix')
    zero = root / 'restfreq_zero.im'
    if zero.exists(): shutil.rmtree(zero)
    shutil.copytree(root / 'normal.im', zero)
    tb = table(); tb.open(str(zero), nomodify=False)
    coords = tb.getkeyword('coords'); spectral = coords['spectral2']
    spectral['restfreq'] = 0.0; spectral['restfreqs'] = np.array([0.0])
    tb.putkeyword('coords', coords); tb.close()
    for lock in zero.rglob('table.lock'): lock.unlink()
    cases = [('restfreq_zero', 'restfreq', '1.2GHz'),
             ('restfreq_zero', 'RESTFREQ', {'unit': 'MHz', 'value': 1500.0}),
             ('restfreq_zero', 'restfreq', '0Hz'),
             ('restfreq_zero', 'restfreq', 'bad'),
             ('normal', 'restfreq', '1GHz'),
             ('normal', 'RESTFREQ', 'bad')]
    expected = {'casa_version': version_string(), 'cases': []}
    with tempfile.TemporaryDirectory(prefix='imhead-restfreq-add-') as temp:
        for index, (source, key, value) in enumerate(cases):
            path = Path(temp) / f'{index}.im'; shutil.copytree(root / f'{source}.im', path)
            ia = image(); ia.open(str(path)); count = len(ia.history(list=False)); ia.done()
            case = {'image': source, 'key': key, 'value': value}
            try: imhead(str(path), mode='add', hdkey=key, hdvalue=value)
            except RuntimeError as error: case['error'] = str(error).split(' at File:', 1)[0]
            case['get'] = imhead(str(path), mode='get', hdkey='restfreq')
            ia = image(); ia.open(str(path)); case['history'] = [s.replace(str(path), '<IMAGE>') for s in ia.history(list=False)[count:]]; ia.done()
            expected['cases'].append(case)
    (root / 'restfreq_add_expected.json').write_text(json.dumps(expected, indent=2, sort_keys=True) + '\n')


if __name__ == '__main__':
    main()
