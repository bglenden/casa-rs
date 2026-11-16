#!/usr/bin/env python3
"""Capture CASA imhead axis-unit edits on the shared image fixtures."""
import json
import shutil
import tempfile
from pathlib import Path
from casatasks import imhead
from casatools import image, table, version_string
import numpy as np


def plain(value):
    return value.tolist() if hasattr(value, 'tolist') else value


def main():
    root = Path('crates/casars-imexplore/tests/data/imhead_crpix')
    for label, selector in [('selector_lo', 1.45e9), ('selector_hi', 1.55e9)]:
        path = root / f'{label}.im'
        if path.exists(): shutil.rmtree(path)
        shutil.copytree(root / 'normal.im', path)
        tb = table(); tb.open(str(path), nomodify=False); coords = tb.getkeyword('coords')
        coords['spectral2']['restfreq'] = selector
        coords['spectral2']['restfreqs'] = np.array([1e9, 1.4e9, 1.6e9])
        tb.putkeyword('coords', coords); tb.close()
        for lock in path.rglob('table.lock'): lock.unlink()
    cases = [
        ('normal', 'cunit1', 'deg'), ('normal', 'cunit1', ' deg'), ('normal', 'cunit1', 'deg '), ('normal', 'cunit2', 'rad'),
        ('normal', 'cunit1', 'arcsec'), ('normal', 'cunit1', 'Hz'),
        ('normal', 'cunit1', 'bad'), ('normal', 'cunit1', ''),
        ('normal', 'cunit3', 'deg'), ('normal', 'cunit4', 'km/s'),
        ('normal', 'cunit4', 'Hz'), ('normal', 'cunit1', 42),
        ('ghz', 'cunit4', 'Hz'),
        ('linear', 'cunit1', 'm'), ('linear', 'cunit1', 'Hz'),
        ('transposed', 'cunit3', 'deg'),
        ('transposed', 'cunit1', 'deg'),
        ('selector_lo', 'cunit1', 'deg'), ('selector_hi', 'cunit1', 'deg'),
    ]
    expected = {'casa_version': version_string(), 'cases': []}
    with tempfile.TemporaryDirectory(prefix='imhead-axis-unit-') as temp:
        for index, (source, key, value) in enumerate(cases):
            path = Path(temp) / f'{index}.im'; shutil.copytree(root / f'{source}.im', path)
            case = {'image': source, 'key': key, 'value': value}
            try:
                case['return'] = imhead(str(path), mode='put', hdkey=key, hdvalue=value)
            except RuntimeError as error:
                case['error'] = str(error).split(' at File:', 1)[0]
            summary = imhead(str(path), mode='summary')
            case['after'] = {name: plain(summary[name]) for name in ['axisnames', 'axisunits', 'incr', 'refpix', 'refval', 'shape']}
            if source != 'linear':
                case['restfreq'] = imhead(str(path), mode='get', hdkey='restfreq')
                ia = image(); ia.open(str(path)); coords = ia.coordsys().torecord()['spectral2']
                case['frequencies'] = coords['restfreqs'].tolist(); ia.done()
            if 'error' not in case:
                case['get'] = imhead(str(path), mode='get', hdkey=key)
                ia = image(); ia.open(str(path)); case['history'] = [line.replace(str(path), '<IMAGE>') for line in ia.history(list=False)[-3:]]; ia.done()
            expected['cases'].append(case)
    (root / 'axis_unit_expected.json').write_text(json.dumps(expected, indent=2, sort_keys=True) + '\n')


if __name__ == '__main__':
    main()
