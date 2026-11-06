#!/usr/bin/env python3
"""Record CASA reference-pixel edits on normal and transposed images."""
import json
import shutil
import tempfile
from pathlib import Path
from casatasks import imhead
from casatools import image, version_string


def plain(value):
    if hasattr(value, 'tolist'):
        return value.tolist()
    if isinstance(value, dict):
        return {key: plain(item) for key, item in value.items()}
    return value


def main():
    root = Path('crates/casars-imexplore/tests/data/imhead_crpix'); root.mkdir(parents=True, exist_ok=True)
    base = root / 'normal.im'; transposed = root / 'transposed.im'
    ia = image(); ia.fromshape(str(base), [4, 4, 1, 2], overwrite=True); ia.done()
    if transposed.exists(): shutil.rmtree(transposed)
    ia.open(str(base)); out = ia.transpose(outfile=str(transposed), order='2301'); out.done(); ia.done()
    for path in [base, transposed]:
        for lock in path.rglob('table.lock'): lock.unlink()
    actions = [
        ('normal', 'crpix1', 5.5), ('normal', 'crpix2', -3), ('normal', 'crpix4', 0.25),
        ('normal', 'crpix1', '3.25'), ('normal', 'crpix1', ' 3.25 '), ('normal', 'crpix1', True), ('normal', 'crpix1', 'bad'),
        ('normal', 'crpix3', 5), ('normal', 'crpix0', 1), ('normal', 'crpix5', 1),
        ('normal', 'crpix1x', 4), ('normal', 'crpix001foo', 4), ('normal', 'crpix1.0', 4),
        ('normal', 'crpix1 2', 4), ('normal', 'crpix+1', 4),
        ('transposed', 'crpix2', 0.75), ('transposed', 'crpix3', 3.5), ('transposed', 'crpix4', -2.0), ('transposed', 'crpix1', 1),
    ]
    result = {'casa_version': version_string(), 'cases': []}
    with tempfile.TemporaryDirectory(prefix='imhead-crpix-') as temp:
        for index, (source, key, value) in enumerate(actions):
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
                ia=image();ia.open(str(path));case['history']=[line.replace(str(path), '<IMAGE>') for line in ia.history(list=False)[-3:]];ia.done()
            result['cases'].append(case)
    (root / 'expected.json').write_text(json.dumps(result, indent=2, sort_keys=True) + '\n')


if __name__ == '__main__':
    main()
