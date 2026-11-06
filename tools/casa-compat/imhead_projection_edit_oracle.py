#!/usr/bin/env python3
"""Capture CASA imhead zenithal projection edits."""
import json
import shutil
import tempfile
from pathlib import Path
import numpy as np
from casatasks import imhead
from casatools import image, quanta, version_string


def main():
    root = Path('crates/casars-imexplore/tests/data/imhead_crpix')
    pointing = root / 'projection_pointing.im'
    if pointing.exists(): shutil.rmtree(pointing)
    shutil.copytree(root / 'normal.im', pointing)
    ia = image(); ia.open(str(pointing)); cs = ia.coordsys()
    cs.setreferencevalue(value=[10200.0, -3600.0], type='direction')
    cs.setlineartransform(type='direction', value=np.array([[0.8660254037844386, -0.5], [0.5, 0.8660254037844386]]))
    ia.setcoordsys(cs.torecord()); cs.done(); ia.done()
    for lock in pointing.rglob('table.lock'): lock.unlink()
    for info in pointing.rglob('table.info'):
        info.write_text('\n'.join(line.rstrip() for line in info.read_text().splitlines()).rstrip() + '\n')
    cases = [(source, 'projection', value) for source in ('normal', 'transposed') for value in ('SIN', 'TAN', 'ARC', 'ZEA', 'STG', 'tan')]
    cases += [('projection_pointing', 'projection', value) for value in ('TAN', 'ARC', 'ZEA', 'STG')]
    cases += [('normal', 'PROJECTION', 'ARC'), ('normal', 'projection', 'NCP'), ('normal', 'projection', 'bad'), ('normal', 'projection', 'T'), ('car', 'projection', 'CAR')]
    expected = {'casa_version': version_string(), 'cases': []}
    with tempfile.TemporaryDirectory(prefix='imhead-projection-') as temp:
        for index, (source, key, value) in enumerate(cases):
            path = Path(temp) / f'{index}.im'; shutil.copytree(root / f'{source}.im', path)
            ia = image(); ia.open(str(path)); count = len(ia.history(list=False)); ia.done()
            case = {'image': source, 'key': key, 'value': value}
            try: imhead(str(path), mode='put', hdkey=key, hdvalue=value)
            except RuntimeError as error: case['error'] = str(error).split(' at File:', 1)[0]
            case['get'] = imhead(str(path), mode='get', hdkey='projection')
            ia = image(); ia.open(str(path)); case['history'] = [line.replace(str(path), '<IMAGE>') for line in ia.history(list=False)[count:]]
            cs = ia.coordsys(); rec = cs.torecord(); cs.done()
            pmap, wmap = rec['pixelmap0'], rec['worldmap0']
            qa = quanta(); case['world'] = []
            for direction_pixel in ([0.0, 0.0], [2.5, 1.0]):
                pixel = [0.0] * len(ia.shape())
                for axis in range(2): pixel[int(pmap[axis])] = direction_pixel[axis]
                values = ia.toworld(pixel, format='q')['quantity']
                case['world'].append([qa.convert(values[f'*{int(wmap[axis]) + 1}'], 'rad')['value'] for axis in range(2)])
            ia.done()
            expected['cases'].append(case)
    (root / 'projection_edit_expected.json').write_text(json.dumps(expected, indent=2, sort_keys=True) + '\n')


if __name__ == '__main__':
    main()
