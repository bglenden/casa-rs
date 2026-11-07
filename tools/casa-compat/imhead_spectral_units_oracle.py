#!/usr/bin/env python3
"""Capture CASA spectral-unit edits, coordinate conversions and FITS cards."""
import json
import shutil
import tempfile
from pathlib import Path

from casatasks import casalog, exportfits, imhead
from casatools import image, version_string


ROOT = Path('crates/casars-imexplore/tests/data/imhead_crpix')
CASES = [
    ('normal', 4, 'GHz'),
    ('normal', 4, 'MHz'),
    ('ghz', 4, 'MHz'),
    ('ghz', 4, 'Hz'),
    ('transposed', 2, 'GHz'),
    ('conversion', 4, 'GHz'),
]


def fits_spectral(path, ctype='FREQ'):
    cards = {}
    with path.open('rb') as stream:
        while True:
            block = stream.read(2880)
            if not block:
                raise ValueError('FITS header has no END card')
            finished = False
            for start in range(0, len(block), 80):
                card = block[start:start + 80].decode('ascii')
                key = card[:8].strip()
                if key == 'END':
                    finished = True
                    break
                if card[8:10] != '= ':
                    continue
                raw = card[10:].strip()
                if raw.startswith("'"):
                    value = raw[1:raw.index("'", 1)].strip()
                else:
                    try:
                        value = float(raw.split('/', 1)[0].strip().replace('D', 'E'))
                    except ValueError:
                        continue
                cards[key] = value
            if finished:
                break
    axis = next(i for i in range(1, 10) if str(cards.get(f'CTYPE{i}', '')).startswith(ctype))
    return {key: cards[f'{key}{axis}'] for key in ('CTYPE', 'CUNIT', 'CRVAL', 'CDELT')} | {
        'RESTFRQ': cards['RESTFRQ'], 'SPECSYS': cards.get('SPECSYS')
    }


def main():
    casalog.setlogfile('/tmp/casa-977-spectral-units.log')
    casalog.showconsole(False)
    expected = {'casa_version': version_string(), 'cases': []}
    with tempfile.TemporaryDirectory(prefix='imhead-spectral-units-') as temp:
        for index, (source, axis, unit) in enumerate(CASES):
            print(f"checking {source} axis {axis} as {unit}", flush=True)
            path = Path(temp) / f'{index}.im'
            fits = Path(temp) / f'{index}.fits'
            shutil.copytree(ROOT / f'{source}.im', path)
            assert imhead(str(path), mode='put', hdkey=f'cunit{axis}', hdvalue=unit) is None
            summary = imhead(str(path), mode='summary')
            rest = imhead(str(path), mode='get', hdkey='restfreq')
            ia = image(); ia.open(str(path)); cs = ia.coordsys()
            pixel = [0.0] * len(summary['shape']); pixel[axis - 1] = 1.0
            world = cs.toworld(pixel)['numeric'].tolist()
            restored = cs.topixel(world)['numeric'].tolist()
            cs.done(); ia.done()
            try:
                exportfits(imagename=str(path), fitsimage=str(fits), overwrite=True)
                fits_result = fits_spectral(fits)
            except RuntimeError as error:
                if source != 'transposed':
                    raise
                fits_result = {'error': str(error).split(' at File:', 1)[0]}
            velocity_result = None
            if source != 'transposed':
                exportfits(imagename=str(path), fitsimage=str(fits), velocity=True, overwrite=True)
                velocity_result = fits_spectral(fits, 'VRAD')
            expected['cases'].append({
                'image': source, 'axis': axis, 'unit': unit,
                'axisunits': summary['axisunits'],
                'refval': summary['refval'].tolist(),
                'incr': summary['incr'].tolist(),
                'refpix': summary['refpix'].tolist(),
                'restfreq': rest, 'pixel': pixel, 'world': world,
                'roundtrip_pixel': restored, 'fits': fits_result, 'velocity_fits': velocity_result,
            })
    (ROOT / 'spectral_unit_expected.json').write_text(
        json.dumps(expected, indent=2, sort_keys=True, default=lambda value: value.tolist()) + '\n'
    )
    print(f"CASA {expected['casa_version']} captured {len(expected['cases'])} spectral-unit cases")


if __name__ == '__main__':
    main()
