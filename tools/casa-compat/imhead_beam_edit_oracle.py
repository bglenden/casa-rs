#!/usr/bin/env python3
"""Capture CASA imhead edits of restoring-beam header fields."""
import json
import shutil
import tempfile
from pathlib import Path
from casatasks import imhead
from casatools import image, table, version_string
import numpy as np


def plain(value):
    if hasattr(value, 'tolist'): return value.tolist()
    if isinstance(value, dict): return {str(k): plain(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)): return [plain(v) for v in value]
    return value


def main():
    root = Path('crates/casars-imexplore/tests/data/imhead_crpix')
    beam = root / 'beam.im'
    if beam.exists(): shutil.rmtree(beam)
    shutil.copytree(root / 'normal.im', beam)
    ia = image(); ia.open(str(beam)); ia.setrestoringbeam(major='1arcmin', minor='20arcsec', pa='120deg'); ia.done()
    for lock in beam.rglob('table.lock'): lock.unlink()
    singleton = root / 'beam_singleton.im'
    if singleton.exists(): shutil.rmtree(singleton)
    shutil.copytree(beam, singleton)
    tb = table(); tb.open(str(singleton), nomodify=False); info = tb.getkeyword('imageinfo')
    info['restoringbeam']['major']['value'] = np.array([60.0]); info['restoringbeam']['major']['unit'] = 'arcsec'
    tb.putkeyword('imageinfo', info); tb.close()
    for lock in singleton.rglob('table.lock'): lock.unlink()
    spelling = root / 'beam_spelling.im'
    if spelling.exists(): shutil.rmtree(spelling)
    shutil.copytree(beam, spelling)
    tb.open(str(spelling), nomodify=False); info = tb.getkeyword('imageinfo')
    info['restoringbeam']['major']['value'] = 60.0; info['restoringbeam']['major']['unit'] = 'arcsec^1'
    tb.putkeyword('imageinfo', info); tb.close()
    for lock in spelling.rglob('table.lock'): lock.unlink()
    cases = [
        ('normal', 'put', 'beammajor', '3arcsec'), ('normal', 'put', 'beamminor', '2arcsec'),
        ('normal', 'put', 'beampa', '30deg'), ('normal', 'add', 'bmaj', '1arcmin'),
        ('normal', 'add', 'bpa', '30deg'), ('normal', 'del', 'bmaj', ''),
        ('normal', 'put', 'bmaj', '0arcsec'), ('normal', 'put', 'bmaj', '-1arcsec'),
        ('beam', 'put', 'bmaj', '2arcmin'), ('beam', 'put', 'bmaj', '15arcsec'),
        ('beam', 'put', 'bmin', '100arcsec'), ('beam', 'put', 'bmin', '60arcsec'),
        ('beam', 'put', 'bpa', '190deg'), ('beam', 'put', 'bpa', '-400deg'), ('beam', 'put', 'bpa', '2rad'),
        ('beam', 'put', 'beammajor', '3'), ('beam', 'put', 'bmaj', '0arcsec'),
        ('beam', 'put', 'bpa', '1Hz'), ('beam', 'put', 'bmaj', {'unit':'arcsec', 'value':40.0}),
        ('beam', 'put', 'bmaj', {'unit':'arcsec^1', 'value':40.0}),
        ('beam', 'put', 'bmaj', {'unit':'arcsec*1', 'value':40.0}),
        ('beam', 'put', 'bmaj', {'unit':' arcsec', 'value':40.0}),
        ('beam', 'put', 'bmaj', {'unit':'mas', 'value':19999.999999999996}),
        ('beam', 'put', 'bmaj', 3), ('beam', 'add', 'bmaj', '3arcsec'),
        ('beam', 'del', 'bmaj', ''), ('beam', 'del', 'bpa', ''),
        ('beam', 'del', 'bpa', {'unit':'deg', 'value':1.0}), ('beam', 'del', 'bpa', [1,2]),
        ('multi', 'put', 'bmaj', '3arcsec'), ('multi', 'add', 'bmaj', '3arcsec'),
        ('multi', 'del', 'bmaj', ''), ('multi', 'del', 'bpa', ''),
        ('beam_singleton', 'put', 'bpa', '15deg'), ('beam_singleton', 'del', 'bmaj', ''),
        ('beam_spelling', 'put', 'bpa', '15deg'), ('beam_spelling', 'del', 'bmaj', ''),
        ('single_dual', 'put', 'bpa', '15deg'), ('single_dual', 'del', 'bmin', ''),
    ]
    expected = {'casa_version': version_string(), 'cases': []}
    with tempfile.TemporaryDirectory(prefix='imhead-beam-edit-') as temp:
        for index, (source, mode, key, value) in enumerate(cases):
            src = root.parent / 'imhead_beams' / ('multiple.im' if source == 'multi' else 'single_dual.im') if source in ['multi', 'single_dual'] else root / f'{source}.im'
            path = Path(temp) / f'{index}.im'; shutil.copytree(src, path)
            ia = image(); ia.open(str(path)); count = len(ia.history(list=False)); ia.done()
            case = {'image': source, 'mode': mode, 'key': key, 'value': value}
            try:
                case['return'] = imhead(str(path), mode=mode, hdkey=key, hdvalue=value)
            except RuntimeError as error:
                case['error'] = str(error).split(' at File:', 1)[0]
            ia = image(); ia.open(str(path)); case['beam'] = plain(ia.restoringbeam())
            if 'error' not in case: case['history'] = [line.replace(str(path), '<IMAGE>') for line in ia.history(list=False)[count:]]
            ia.done(); expected['cases'].append(case)
    (root / 'beam_edit_expected.json').write_text(json.dumps(expected, indent=2, sort_keys=True) + '\n')


if __name__ == '__main__':
    main()
