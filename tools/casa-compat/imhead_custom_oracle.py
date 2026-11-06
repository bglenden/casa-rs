#!/usr/bin/env python3
"""Record CASA behavior for string-valued custom image header keys."""
import json
import shutil
import tempfile
from pathlib import Path
from casatasks import imhead
from casatools import image, version_string


def main():
    root = Path('crates/casars-imexplore/tests/data/imhead_put')
    result = {'casa_version': version_string(), 'cases': []}
    cases = [
        [('put', 'My Key', 'alpha'), ('get', 'My Key', ''), ('get', 'my key', ''), ('put', 'My Key', 'beta'), ('get', 'My Key', ''), ('del', 'My Key', ''), ('get', 'My Key', '')],
        [('add', 'lower', 'one'), ('get', 'LOWER', ''), ('add', 'LOWER', 'two'), ('del', 'LOWER', ''), ('get', 'lower', '')],
        [('put', 'My Key', ''), ('get', 'My Key', ''), ('add', 'My Key', 'again')],
        [('put', '', 'empty name'), ('get', '', ''), ('del', '', '')],
        [('put', 'counter', 42), ('get', 'counter', ''), ('put', 'counter', 13), ('get', 'counter', ''), ('put', 'counter', 3.5), ('get', 'counter', '')],
        [('add', 'real', 3.25), ('get', 'real', ''), ('put', 'real', -0.75), ('get', 'real', '')],
        [('put', 'flag', True), ('get', 'flag', ''), ('put', 'flag', False), ('del', 'flag', 123)],
        [('put', 'precise', 1.23456789), ('put', 'precise', 1e-7), ('put', 'precise', 1e20), ('put', 'precise', -0.0), ('put', 'precise', 1e-4), ('put', 'precise', 1e16), ('put', 'precise', 1e-5)],
    ]
    with tempfile.TemporaryDirectory(prefix='imhead-custom-oracle-') as tmp:
        for index, actions in enumerate(cases):
            path = Path(tmp) / f'{index}.im'; shutil.copytree(root / 'base.im', path)
            sequence = []
            for mode, key, value in actions:
                case = {'mode': mode, 'key': key, 'value': value}
                try:
                    case['return'] = imhead(str(path), mode=mode, hdkey=key, hdvalue=value)
                    if mode != 'get':
                        ia=image();ia.open(str(path));case['history']=[v.replace(str(path), '<IMAGE>') for v in ia.history(list=False)[-3:]];ia.done()
                except RuntimeError as error:
                    case['error'] = str(error).split(' at File:',1)[0]
                sequence.append(case)
            result['cases'].append(sequence)
    numeric = root / 'numeric_custom.im'
    if numeric.exists():
        shutil.rmtree(numeric)
    shutil.copytree(root / 'base.im', numeric)
    result['numeric'] = {}
    for key, value in [('integer', 42), ('real', 3.25)]:
        imhead(str(numeric), mode='put', hdkey=key, hdvalue=value)
        result['numeric'][key] = imhead(str(numeric), mode='get', hdkey=key)
    for lock in numeric.rglob('table.lock'):
        lock.unlink()
    (root / 'custom_expected.json').write_text(json.dumps(result, indent=2)+'\n')


if __name__ == '__main__':
    main()
