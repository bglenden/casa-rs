#!/usr/bin/env python3
"""Capture CASA imhead polarization reference-value edits."""
import json
import shutil
import tempfile
from pathlib import Path
from casatasks import imhead
from casatools import image, version_string


def main():
    root = Path('crates/casars-imexplore/tests/data/imhead_crpix')
    multi = root / 'stokes_four.im'
    if multi.exists(): shutil.rmtree(multi)
    ia = image(); ia.fromshape(str(multi), [4,4,4,4]); ia.done()
    for lock in multi.rglob('table.lock'): lock.unlink()
    for info in multi.rglob('table.info'):
        info.write_text('\n'.join(line.rstrip() for line in info.read_text().splitlines()).rstrip() + '\n')
    cases = [('normal', 'Q'), ('normal','q'), ('normal','RR'), ('normal',['V']),
             ('normal',['Q','U']), ('normal',[]), ('normal',1),
             ('transposed','U'), ('stokes_four',['V','U','Q','I']),
             ('stokes_four',['I','I','Q','V']), ('stokes_four','Q'),
             ('stokes_four',['I','Q']), ('stokes_four',['i','I','Q','V'])]
    expected = {'casa_version':version_string(), 'cases':[]}
    with tempfile.TemporaryDirectory(prefix='imhead-stokes-') as temp:
        for index,(source,value) in enumerate(cases):
            path = Path(temp) / f'{index}.im'; shutil.copytree(root / f'{source}.im',path)
            ia = image(); ia.open(str(path)); count = len(ia.history(list=False)); cs = ia.coordsys()
            axis = int(cs.findcoordinate('stokes')['pixel'][0]); cs.done(); ia.done()
            key = f'crval{axis+1}'; case = {'image':source,'key':key,'value':value}
            try: imhead(str(path), mode='put', hdkey=key, hdvalue=value)
            except RuntimeError as error: case['error'] = str(error).split(' at File:', 1)[0]
            case['get'] = imhead(str(path), mode='get', hdkey=key).tolist()
            ia = image(); ia.open(str(path)); case['history'] = [line.replace(str(path), '<IMAGE>') for line in ia.history(list=False)[count:]]; ia.done()
            expected['cases'].append(case)
    (root / 'stokes_edit_expected.json').write_text(json.dumps(expected, indent=2, sort_keys=True) + '\n')


if __name__ == '__main__': main()
