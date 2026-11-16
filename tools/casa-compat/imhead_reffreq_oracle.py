#!/usr/bin/env python3
"""Capture CASA imhead reference-frequency frame edits."""
import json
import shutil
import tempfile
from pathlib import Path
from casatasks import imhead
from casatools import coordsys, image, table, version_string
import numpy as np


def plain(value):
    return value.tolist() if hasattr(value, 'tolist') else value


def main():
    root = Path('crates/casars-imexplore/tests/data/imhead_crpix')
    conversion = root / 'conversion.im'
    if conversion.exists(): shutil.rmtree(conversion)
    shutil.copytree(root / 'normal.im', conversion)
    tb = table(); tb.open(str(conversion), nomodify=False); coords = tb.getkeyword('coords')
    coords['spectral2']['conversion']['system'] = 'BARY'
    tb.putkeyword('coords', coords); tb.close()
    for lock in conversion.rglob('table.lock'): lock.unlink()
    multi = root / 'multi.im'; multi_transposed = root / 'multi_transposed.im'
    for path in [multi, multi_transposed]:
        if path.exists(): shutil.rmtree(path)
    cs = coordsys().newcoordsys(spectral=True); cs.addcoordinate(spectral=True)
    ia = image(); ia.fromshape(str(multi), [3, 3], csys=cs.torecord(), overwrite=True); ia.done(); cs.done()
    tb = table(); tb.open(str(multi), nomodify=False); coords = tb.getkeyword('coords')
    for index, (frame, frequency) in enumerate([('LSRK', 1e9), ('BARY', 2e9)]):
        spectral = coords[f'spectral{index}']; spectral['system'] = frame
        spectral['conversion']['system'] = frame; spectral['restfreq'] = frequency
        spectral['restfreqs'] = np.array([frequency])
    tb.putkeyword('coords', coords); tb.close()
    ia.open(str(multi)); transposed = ia.transpose(outfile=str(multi_transposed), order='10'); transposed.done(); ia.done()
    for path in [multi, multi_transposed]:
        for lock in path.rglob('table.lock'): lock.unlink()
    cases = [('normal', 'reffreqtype', value) for value in ['LSRK', 'lsrk', 'BARY', 'barycentric', 'BARYCENTER',
             'GEO', 'GEOCENTER', 'TOPO', 'TOPOCENTER', 'GALACTO', 'GALACTOCENTER', 'LGROUP', 'CMB', 'REST',
             'UNDEFINED', 'B', 'BARYextra', 'LSRKanything', 'L', 'G', 'UNDEF', 'BAD', '', True, 42]]
    cases += [('normal', 'REFFREQTYPE', 'BARY'), ('ghz', 'reffreqtype', 'BARY'),
              ('transposed', 'reffreqtype', 'TOPO'), ('linear', 'reffreqtype', 'BARY')]
    cases += [('conversion', 'reffreqtype', value) for value in ['LSRK', 'BARY', 'TOPO', 'UNDEFINED']]
    cases += [('multi', 'reffreqtype', 'BARY'), ('multi_transposed', 'reffreqtype', 'TOPO'),
              ('multi_transposed', 'reffreqtype', 'LSRK')]
    expected = {'casa_version': version_string(), 'cases': [], 'restfreq_cases': []}
    with tempfile.TemporaryDirectory(prefix='imhead-reffreq-') as temp:
        for index, (source, key, value) in enumerate(cases):
            path = Path(temp) / f'{index}.im'; shutil.copytree(root / f'{source}.im', path)
            ia = image(); ia.open(str(path)); count = len(ia.history(list=False)); ia.done()
            case = {'image': source, 'key': key, 'value': value}
            try:
                case['return'] = imhead(str(path), mode='put', hdkey=key, hdvalue=value)
            except RuntimeError as error:
                case['error'] = str(error).split(' at File:', 1)[0]
            summary = imhead(str(path), mode='summary')
            case['after'] = {name: plain(summary[name]) for name in ['axisnames', 'axisunits', 'incr', 'refpix', 'refval', 'shape']}
            if source != 'linear':
                case['get'] = imhead(str(path), mode='get', hdkey='reffreqtype')
            if 'error' not in case:
                ia = image(); ia.open(str(path)); coords = ia.coordsys().torecord()
                rec = coords['spectral0' if source.startswith('multi') else 'spectral2']
                case['system'] = rec['system']; case['conversion_system'] = rec.get('conversion', {}).get('system')
                if source.startswith('multi'):
                    case['systems'] = [coords['spectral0']['system'], coords['spectral1']['system']]
                case['history'] = [line.replace(str(path), '<IMAGE>') for line in ia.history(list=False)[count:]]; ia.done()
            expected['cases'].append(case)
        for index, source in enumerate(['multi', 'multi_transposed']):
            path = Path(temp) / f'restfreq-{index}.im'; shutil.copytree(root / f'{source}.im', path)
            imhead(str(path), mode='put', hdkey='restfreq', hdvalue='1.5GHz')
            ia = image(); ia.open(str(path)); coords = ia.coordsys().torecord(); ia.done()
            expected['restfreq_cases'].append({'image': source, 'get': imhead(str(path), mode='get', hdkey='restfreq'),
                'frequencies': [coords['spectral0']['restfreq'], coords['spectral1']['restfreq']]})
    (root / 'reffreq_expected.json').write_text(json.dumps(expected, indent=2, sort_keys=True) + '\n')


if __name__ == '__main__':
    main()
