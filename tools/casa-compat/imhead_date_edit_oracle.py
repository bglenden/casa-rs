#!/usr/bin/env python3
"""Capture CASA imhead observation-date edits using the existing date images."""
import json
import shutil
import tempfile
from pathlib import Path
from casatasks import imhead
from casatools import image, version_string


def main():
    root = Path('crates/casars-imexplore/tests/data/imhead_dates')
    cases = [
        ('noon', 'date-obs', '2020/01/02/12:34:56'),
        ('j2000', 'epoch', '2023/02/25/02:57:46.123456'),
        ('fraction', 'EPOCH', '2000/01/01'),
        ('missing', 'date-obs', {'unit': 'd', 'value': 59000.25}),
        ('fraction', 'epoch', {'unit': 's', 'value': 86400.0}),
        ('noon', 'epoch', '1s'),
        ('noon', 'epoch', '59000d'),
        ('noon', 'epoch', '59000'),
        ('noon', 'epoch', ''),
        ('noon', 'epoch', '1GHz'),
        ('noon', 'epoch', 59000.0),
        ('noon', 'date-obs', {'unit': 'Hz', 'value': 59000.0}),
    ]
    expected = {'casa_version': version_string(), 'cases': []}
    with tempfile.TemporaryDirectory(prefix='imhead-date-edit-') as temp:
        for index, (source, key, value) in enumerate(cases):
            path = Path(temp) / f'{index}.im'
            shutil.copytree(root / f'{source}.im', path)
            ia = image(); ia.open(str(path)); count = len(ia.history(list=False)); ia.done()
            case = {'image': source, 'key': key, 'value': value}
            try:
                imhead(str(path), mode='put', hdkey=key, hdvalue=value)
            except RuntimeError as error:
                case['error'] = str(error).split(' at File:', 1)[0]
            case['get'] = imhead(str(path), mode='get', hdkey='date-obs')
            ia = image(); ia.open(str(path))
            case['history'] = [line.replace(str(path), '<IMAGE>') for line in ia.history(list=False)[count:]]
            ia.done(); expected['cases'].append(case)
    (root / 'edit_expected.json').write_text(json.dumps(expected, indent=2, sort_keys=True) + '\n')


if __name__ == '__main__':
    main()
