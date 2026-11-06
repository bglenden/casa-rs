#!/usr/bin/env python3
"""Capture CASA imhead rest-frequency writes against the shared image fixture."""
import json
import shutil
import tempfile
from pathlib import Path
from casatasks import imhead
from casatools import image, version_string


def main():
    root = Path('crates/casars-imexplore/tests/data/imhead_crpix')
    values = ['1.42GHz', '1.5e9Hz', '1420000000', '0Hz', '-1Hz', '2m', 'bad',
              {'unit': 'GHz', 'value': 1.42}, {'value': 1.42},
              {'unit': '', 'value': 1.42}, 1500000000.0]
    ghz = root / 'ghz.im'
    if ghz.exists(): shutil.rmtree(ghz)
    shutil.copytree(root / 'normal.im', ghz)
    ia = image(); ia.open(str(ghz)); cs = ia.coordsys()
    cs.setrestfrequency([1e9, 1.4e9, 1.6e9], which=1)
    cs.setunits(value=['GHz'], type='spectral')
    ia.setcoordsys(cs.torecord()); cs.done(); ia.done()
    for lock in ghz.rglob('table.lock'): lock.unlink()
    cases = [('normal', value) for value in values] + [
        ('ghz', '2'), ('ghz', {'unit': '', 'value': 2.0}),
        ('ghz', '1.5GHz'), ('ghz', {'unit': 'MHz', 'value': 1600.0}),
    ]
    expected = {'casa_version': version_string(), 'cases': []}
    with tempfile.TemporaryDirectory(prefix='imhead-restfreq-') as temp:
        for index, (source, value) in enumerate(cases):
            path = Path(temp) / f'{index}.im'; shutil.copytree(root / f'{source}.im', path)
            case = {'image': source, 'value': value}
            try:
                case['return'] = imhead(str(path), mode='put', hdkey='restfreq', hdvalue=value)
            except RuntimeError as error:
                case['error'] = str(error).split(' at File:', 1)[0]
            case['get'] = imhead(str(path), mode='get', hdkey='restfreq')
            ia = image(); ia.open(str(path)); coords = ia.coordsys().torecord()['spectral2']
            case['frequencies'] = coords['restfreqs'].tolist(); ia.done()
            if 'error' not in case:
                ia = image(); ia.open(str(path))
                case['history'] = [line.replace(str(path), '<IMAGE>') for line in ia.history(list=False)[-3:]]
                ia.done()
            expected['cases'].append(case)
    (root / 'restfreq_expected.json').write_text(json.dumps(expected, indent=2, sort_keys=True) + '\n')


if __name__ == '__main__':
    main()
