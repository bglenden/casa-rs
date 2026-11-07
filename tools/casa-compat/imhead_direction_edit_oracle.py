#!/usr/bin/env python3
"""Capture CASA direction-reference edits and their world/FITS coordinates."""
import json
import shutil
import tempfile
from pathlib import Path
from casatasks import imhead, exportfits
from casatools import image, quanta, version_string


def fits_fields(path):
    raw = path.read_bytes()[:28800]
    cards = [raw[i:i+80].decode('ascii',errors='replace') for i in range(0,len(raw),80)]
    found = {}
    for card in cards:
        key = card[:8].strip()
        if key in ('CTYPE1','CTYPE2','RADESYS','EQUINOX'):
            value = card[10:].split('/',1)[0].strip()
            found[key] = value.strip("' ") if value.startswith("'") else float(value)
    return found


def main():
    root = Path('crates/casars-imexplore/tests/data/imhead_crpix')
    cases = [(source, 'equinox', value, None) for source in ('normal','transposed','projection_pointing')
             for value in ('J2000','GALACTIC','ICRS','B1950','ECLIPTIC','SUPERGAL')]
    cases += [('normal','equinox','GAL',None), ('normal','EQUINOX','j2',None), ('normal','equinox','J',None), ('normal','equinox','bad',None),
              ('normal','equinox','ICRS','GALACTIC'), ('normal','equinox','J2000','GALACTIC')]
    expected = {'casa_version':version_string(), 'cases':[]}
    with tempfile.TemporaryDirectory(prefix='imhead-direction-') as temp:
        for index,(source,key,value,initial) in enumerate(cases):
            path = Path(temp) / f'{index}.im'; shutil.copytree(root / f'{source}.im',path)
            if initial is not None: imhead(str(path), mode='put', hdkey='equinox', hdvalue=initial)
            ia = image(); ia.open(str(path)); count = len(ia.history(list=False)); ia.done()
            case = {'image':source, 'key':key, 'value':value}
            if initial is not None: case['initial'] = initial
            try: imhead(str(path), mode='put', hdkey=key, hdvalue=value)
            except RuntimeError as error: case['error'] = str(error).split(' at File:', 1)[0]
            case['get'] = imhead(str(path), mode='get', hdkey='equinox')
            case['minpos'] = imhead(str(path), mode='get', hdkey='minpos')
            ia = image(); ia.open(str(path)); case['history'] = [line.replace(str(path),'<IMAGE>') for line in ia.history(list=False)[count:]]
            cs = ia.coordsys(); rec = cs.torecord(); cs.done(); pmap, wmap = rec['pixelmap0'], rec['worldmap0']
            qa = quanta(); case['world'] = []; case['inverse'] = []
            for point in ([0.0,0.0],[2.5,1.0]):
                pixel = [0.0] * len(ia.shape())
                for axis in range(2): pixel[int(pmap[axis])] = point[axis]
                world = ia.toworld(pixel,format='q'); values = world['quantity']
                case['world'].append([qa.convert(values[f'*{int(wmap[axis])+1}'],'rad')['value'] for axis in range(2)])
                inverse = ia.topixel(world)['numeric']
                case['inverse'].append([float(inverse[int(pmap[axis])]) for axis in range(2)])
            ia.done()
            fits = Path(temp)/f'{index}.fits'
            try:
                exportfits(str(path), fitsimage=str(fits), overwrite=True)
                case['fits'] = fits_fields(fits)
            except RuntimeError:
                case['fits_error'] = True
            expected['cases'].append(case)
    (root / 'direction_edit_expected.json').write_text(json.dumps(expected,indent=2,sort_keys=True)+'\n')


if __name__ == '__main__': main()
