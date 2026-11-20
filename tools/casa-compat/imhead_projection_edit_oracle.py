#!/usr/bin/env python3
"""Capture CASA imhead projection edits and their saved parameters."""
import json
import shutil
import tempfile
from pathlib import Path
import numpy as np
from casatasks import imhead
from casatools import image, quanta, table, version_string


def make_projection_fixture(root, name, projection, parameters):
    target = root / f'{name}.im'
    if target.exists(): shutil.rmtree(target)
    shutil.copytree(root / 'normal.im', target)
    ia = image(); ia.open(str(target)); cs = ia.coordsys()
    cs.setprojection(type=projection, parameters=parameters)
    ia.setcoordsys(cs.torecord()); cs.done(); ia.done()
    for lock in target.rglob('table.lock'): lock.unlink()
    for info in target.rglob('table.info'):
        info.write_text('\n'.join(line.rstrip() for line in info.read_text().splitlines()).rstrip() + '\n')


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
    make_projection_fixture(root, 'projection_high_slant', 'SIN', [120.0, 0.0])
    make_projection_fixture(root, 'projection_slant_zenithal', 'SZP', [2.0, 30.0, 45.0])
    cases = [(source, 'projection', value) for source in ('normal', 'transposed') for value in ('SIN', 'TAN', 'ARC', 'ZEA', 'STG', 'tan')]
    cases += [('projection_pointing', 'projection', value) for value in ('TAN', 'ARC', 'ZEA', 'STG')]
    cases += [('normal', 'PROJECTION', 'ARC'), ('normal', 'projection', 'NCP'), ('normal', 'projection', 'bad'), ('normal', 'projection', 'T'), ('car', 'projection', 'CAR')]
    cases += [(source, 'projection', target) for source in ('normal', 'car')
              for target in ('AZP', 'SZP', 'AIR')]
    cases += [('../imhead_projection/slant', 'projection', 'TAN'),
              ('../imhead_projection/ncp', 'projection', 'ARC')]
    cases += [(source, 'projection', 'SZP') for source in ('../imhead_projection/slant', '../imhead_projection/ncp')]
    cases = [(*case, None) for case in cases]
    cases += [('car', 'projection', target, 'AIR') for target in ('SIN', 'AZP')]
    cases += [(source, 'projection', 'AZP', None)
              for source in ('../imhead_projection/slant', '../imhead_projection/ncp')]
    cases += [('../imhead_projection/slant', 'projection', 'AIR', initial)
              for initial in (None, 'AZP')]
    cases += [('projection_high_slant', 'projection', 'AIR', None)]
    cases += [('projection_slant_zenithal', 'projection', 'AZP', None),
              ('projection_slant_zenithal', 'projection', 'SZP', 'AZP')]
    expected = {'casa_version': version_string(), 'cases': []}
    with tempfile.TemporaryDirectory(prefix='imhead-projection-') as temp:
        for index, (source, key, value, initial) in enumerate(cases):
            path = Path(temp) / f'{index}.im'; shutil.copytree(root / f'{source}.im', path)
            if initial is not None:
                imhead(str(path), mode='put', hdkey='projection', hdvalue=initial)
            ia = image(); ia.open(str(path)); count = len(ia.history(list=False)); ia.done()
            case = {'image': source, 'key': key, 'value': value}
            if initial is not None:
                case['initial'] = initial
            try: imhead(str(path), mode='put', hdkey=key, hdvalue=value)
            except RuntimeError as error:
                case['error'] = str(error).splitlines()[0]
            case['get'] = imhead(str(path), mode='get', hdkey='projection')
            tb = table()
            tb.open(str(path))
            stored_direction = tb.getkeyword('coords')['direction0']
            case['parameters'] = stored_direction['projection_parameters'].tolist()
            case['latpole'] = stored_direction['latpole']
            tb.close()
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
