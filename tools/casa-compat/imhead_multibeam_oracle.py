#!/usr/bin/env python3
"""Record CASA's per-plane beam records, including units and median beam."""
import json
import copy
import shutil
import tempfile
from pathlib import Path
from casatasks import imhead
from casatools import image, table, version_string


def plain(value):
    if hasattr(value, 'tolist'):
        return value.tolist()
    if isinstance(value, dict):
        return {key: plain(item) for key, item in value.items()}
    return value


def main():
    root = Path('crates/casars-imexplore/tests/data/imhead_beams')
    path = root / 'multiple_mixed.im'
    ia = image(); ia.fromshape(str(path), [4, 4, 2, 2], overwrite=True)
    values = [('2arcsec', '1arcsec', '0deg'), ('4arcsec', '2arcsec', '3.5rad'), ('0.5arcmin', '2arcsec', '110deg'), ('1arcmin', '3arcsec', '0.3rad')]
    for index, (major, minor, pa) in enumerate(values):
        ia.setrestoringbeam(major=major, minor=minor, pa=pa, channel=index % 2, polarization=index // 2)
    ia.done()
    tied = root / 'nearly_equal.im'
    ia = image(); ia.fromshape(str(tied), [2, 2, 1, 2], overwrite=True)
    for channel, (major, minor) in enumerate([(91.1866202824633, 85.65483850596318), (115.42127669563983, 67.67015110043427)]):
        ia.setrestoringbeam(major=f'{major}arcsec', minor=f'{minor}arcsec', pa=f'{channel}deg', channel=channel, polarization=0)
    ia.done()
    for lock in tied.rglob('table.lock'):
        lock.unlink()
    dual = root / 'single_dual.im'
    if dual.exists():
        shutil.rmtree(dual)
    shutil.copytree(root / 'multiple.im', dual)
    tab = table(); tab.open(str(dual), nomodify=False)
    info = tab.getkeyword('imageinfo'); beams = info['perplanebeams']
    info['restoringbeam'] = copy.deepcopy(beams['*0'])
    beams['*0'] = copy.deepcopy(beams['*1']); del beams['*1']; beams['nChannels'] = 1
    tab.putkeyword('imageinfo', info); tab.close()
    for lock in dual.rglob('table.lock'):
        lock.unlink()
    for name in ['spaced_units', 'invalid_minor', 'invalid_single_minor', 'invalid_single_plane', 'invalid_channels', 'invalid_stokes', 'invalid_missing_channels', 'invalid_missing_stokes', 'invalid_negative', 'invalid_major_less', 'invalid_nan_major', 'invalid_inf_pa', 'invalid_extra_field']:
        target = root / f'{name}.im'
        if target.exists():
            shutil.rmtree(target)
        source = 'degrees.im' if name == 'invalid_single_minor' else ('single_dual.im' if name == 'invalid_single_plane' else 'multiple.im')
        shutil.copytree(root / source, target)
        tab = table(); tab.open(str(target), nomodify=False)
        info = tab.getkeyword('imageinfo'); beams = info.get('perplanebeams', {}); beam = info['restoringbeam'] if name == 'invalid_single_minor' else beams['*0']
        if name == 'spaced_units':
            for key in ['major', 'minor', 'positionangle']:
                beam[key]['unit'] = ' ' + beam[key]['unit'] + ' '
        elif name in ['invalid_minor', 'invalid_single_minor', 'invalid_single_plane']: beam['minor']['value'] = 0.0
        elif name == 'invalid_channels':
            beams['nChannels'] = 3; beams['*2'] = copy.deepcopy(beams['*1'])
        elif name == 'invalid_stokes':
            beams['nStokes'] = 2; beams['*2'] = copy.deepcopy(beams['*0']); beams['*3'] = copy.deepcopy(beams['*1'])
        elif name == 'invalid_missing_channels': del beams['nChannels']
        elif name == 'invalid_missing_stokes': del beams['nStokes']
        elif name == 'invalid_negative': beam['major']['value'] = -2.0
        elif name == 'invalid_major_less': beam['major']['value'] = 0.5
        elif name == 'invalid_nan_major': beam['major']['value'] = float('nan')
        elif name == 'invalid_inf_pa': beam['positionangle']['value'] = float('inf')
        elif name == 'invalid_extra_field': beam['extra'] = 'a'
        tab.putkeyword('imageinfo', info); tab.close()
        for lock in target.rglob('table.lock'):
            lock.unlink()
    expected = {'casa_version': version_string(), 'cases': {}}
    with tempfile.TemporaryDirectory(prefix='imhead-multibeam-') as temp:
        for name in ['multiple', 'multiple_mixed', 'spaced_units', 'nearly_equal']:
            copied = Path(temp) / f'{name}.im'; shutil.copytree(root / f'{name}.im', copied)
            expected['cases'][name] = {mode: plain(imhead(str(copied), mode=mode)['perplanebeams']) for mode in ['summary', 'list']}
        copied = Path(temp) / 'single_dual.im'; shutil.copytree(dual, copied)
        summary = imhead(str(copied), mode='summary'); listed = imhead(str(copied), mode='list')
        expected['single_dual'] = {'summary': plain(summary['restoringbeam']), 'list': {key: plain(listed[key]) for key in ['beammajor', 'beamminor', 'beampa']}}
        expected['invalid'] = {}
        for name in ['invalid_minor', 'invalid_single_minor', 'invalid_single_plane', 'invalid_channels', 'invalid_stokes', 'invalid_missing_channels', 'invalid_missing_stokes', 'invalid_negative', 'invalid_major_less', 'invalid_nan_major', 'invalid_inf_pa', 'invalid_extra_field']:
            copied = Path(temp) / f'{name}.im'; shutil.copytree(root / f'{name}.im', copied)
            try:
                imhead(str(copied), mode='summary')
            except RuntimeError as error:
                expected['invalid'][name] = str(error).split(' at File:', 1)[0].replace(str(copied), '<IMAGE>')
            else:
                raise AssertionError(f'CASA accepted invalid beam {name}')
    for lock in path.rglob('table.lock'):
        lock.unlink()
    (root / 'multi_expected.json').write_text(json.dumps(expected, indent=2, sort_keys=True) + '\n')


if __name__ == '__main__':
    main()
