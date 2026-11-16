#!/usr/bin/env python3
"""Capture CASA imhead axis increment and reference-value quantity writes."""
import json
import shutil
import tempfile
from pathlib import Path
from casatasks import imhead
from casatools import image, version_string


def plain(value):
    return value.tolist() if hasattr(value, 'tolist') else value


def main():
    root = Path('crates/casars-imexplore/tests/data/imhead_crpix')
    linear = root / 'linear.im'
    if linear.exists(): shutil.rmtree(linear)
    ia = image(); ia.fromshape(str(linear), [4, 4], linear=True, overwrite=True); ia.done()
    for lock in linear.rglob('table.lock'): lock.unlink()
    for projection in ['CAR', 'SFL', 'MER', 'AIT']:
        path = root / f'{projection.lower()}.im'
        if path.exists(): shutil.rmtree(path)
        shutil.copytree(root / 'normal.im', path)
        ia.open(str(path)); cs = ia.coordsys(); cs.setprojection(type=projection)
        ia.setcoordsys(cs.torecord()); cs.done(); ia.done()
        for lock in path.rglob('table.lock'): lock.unlink()
    cases = [
        ('normal', 'cdelt1', '2arcsec'), ('normal', 'cdelt2', '2'),
        ('normal', 'cdelt4', '2MHz'), ('normal', 'cdelt4', '-2MHz'),
        ('normal', 'cdelt1', {'unit': 'deg', 'value': 2.0}),
        ('normal', 'cdelt1', {'unit': 'deg', 'value': 2.0, 'extra': 7}),
        ('normal', 'crval4', {'unit': 'GHz', 'value': 1.5, 'error': 0.2}),
        ('normal', 'cdelt1', {'unit': 'arcsec', 'value': 2.0, 'extra': {'note': 'checked'}}),
        ('normal', 'crval1', '1deg'), ('normal', 'crval2', '-20deg'),
        ('normal', 'crval4', '1.42GHz'), ('normal', 'crval1', '1'),
        ('normal', 'cdelt1', '2Hz'), ('normal', 'crval1', '1.42GHz'),
        ('normal', 'cdelt1', '0'), ('normal', 'crval2', '100deg'),
        ('normal', 'crval2', '90.0000000001deg'), ('normal', 'crval2', '90.000000001deg'),
        ('normal', 'cdelt3', '1'), ('normal', 'crval1', 100),
        ('normal', 'cdelt1', True), ('normal', 'crval1', 'bad'),
        ('transposed', 'crval2', '1.5GHz'), ('transposed', 'cdelt3', '3arcsec'),
        ('transposed', 'crval4', '-3deg'), ('transposed', 'cdelt1', '2'),
        ('linear', 'cdelt1', '2Hz'), ('linear', 'crval1', '1deg'),
        ('ghz', 'crval4', '1.5GHz'), ('ghz', 'cdelt4', '2MHz'),
        ('ghz', 'crval4', '2'), ('ghz', 'cdelt4', '2'),
        ('car', 'crval4', '1.5GHz'), ('sfl', 'crval4', '1.5GHz'),
        ('mer', 'crval4', '1.5GHz'), ('ait', 'crval4', '1.5GHz'),
    ]
    expected = {'casa_version': version_string(), 'cases': []}
    with tempfile.TemporaryDirectory(prefix='imhead-axis-quantity-') as temp:
        for index, (source, key, value) in enumerate(cases):
            path = Path(temp) / f'{index}.im'; shutil.copytree(root / f'{source}.im', path)
            case = {'image': source, 'key': key, 'value': value}
            try:
                case['return'] = imhead(str(path), mode='put', hdkey=key, hdvalue=value)
            except RuntimeError as error:
                case['error'] = str(error).split(' at File:', 1)[0]
            summary = imhead(str(path), mode='summary')
            case['after'] = {name: plain(summary[name]) for name in ['axisnames', 'axisunits', 'incr', 'refpix', 'refval', 'shape']}
            if 'error' not in case:
                case['get'] = imhead(str(path), mode='get', hdkey=key)
                ia = image(); ia.open(str(path))
                case['history'] = [line.replace(str(path), '<IMAGE>') for line in ia.history(list=False)[-3:]]
                ia.done()
            expected['cases'].append(case)
    (root / 'axis_quantity_expected.json').write_text(json.dumps(expected, indent=2, sort_keys=True) + '\n')


if __name__ == '__main__':
    main()
