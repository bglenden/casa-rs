#!/usr/bin/env python3
"""Record CASA imhead add/delete behavior for simple string metadata."""
import json
import shutil
import tempfile
from pathlib import Path
from casatasks import imhead
from casatools import image, version_string


def main():
    root = Path('crates/casars-imexplore/tests/data/imhead_put')
    cases = []
    with tempfile.TemporaryDirectory(prefix='imhead-add-del-oracle-') as temp:
        for key in ('bunit', 'object', 'observer', 'telescope'):
            for mode, initial, value in [('add', '', 'New value' if key != 'bunit' else 'K'), ('add', 'K' if key == 'bunit' else 'Existing', 'K' if key == 'bunit' else 'New value'), ('del', 'K' if key == 'bunit' else 'Existing', 'ignored'), ('del', '', '')]:
                path = Path(temp) / f'{len(cases)}.im'
                shutil.copytree(root / 'base.im', path)
                imhead(str(path), mode='put', hdkey=key, hdvalue=initial)
                case = {'mode': mode, 'key': key, 'initial': initial, 'value': value}
                try:
                    case['return'] = imhead(str(path), mode=mode, hdkey=key, hdvalue=value)
                except RuntimeError as error:
                    case['error'] = str(error).split(' at File:', 1)[0]
                case['readback'] = imhead(str(path), mode='get', hdkey=key)
                if 'error' not in case:
                    ia = image(); ia.open(str(path)); case['history'] = [v.replace(str(path), '<IMAGE>') for v in ia.history(list=False)[-3:]]; ia.done()
                cases.append(case)
    (root / 'add_del_expected.json').write_text(json.dumps({'casa_version': version_string(), 'cases': cases}, indent=2) + '\n')


if __name__ == '__main__':
    main()
