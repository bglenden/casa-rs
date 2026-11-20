#!/usr/bin/env python3
"""Have CASA reopen native imhead projection edits and check coordinate records."""
import argparse
import json
import math
import shutil
import subprocess
import tempfile
from pathlib import Path
from astropy.io import fits
from casatasks import imhead
from casatools import image, quanta, table, version_string


def plain(value):
    if hasattr(value, 'tolist'): return value.tolist()
    if isinstance(value, dict): return {str(k): plain(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)): return [plain(v) for v in value]
    return value


def check_world(ia, case):
    cs = ia.coordsys(); rec = cs.torecord(); cs.done(); qa = quanta()
    for point, expected_world in zip(([0.0, 0.0], [2.5, 1.0]), case['world']):
        pixel = [0.0] * len(ia.shape())
        for axis in range(2): pixel[int(rec['pixelmap0'][axis])] = point[axis]
        values = ia.toworld(pixel, format='q')['quantity']
        world = [qa.convert(values[f'*{int(rec["worldmap0"][axis]) + 1}'], 'rad')['value'] for axis in range(2)]
        assert abs(math.remainder(world[0] - expected_world[0], math.tau)) < 1e-12, (case, world)
        assert abs(world[1] - expected_world[1]) < 1e-12, (case, world)


def check_fits_export(binary, path, case):
    exported = path.with_suffix('.fits')
    request = {'surface': 'exportfits', 'values': {'imagename': str(path), 'fitsimage': str(exported)}}
    result = subprocess.run([str(binary), '--json-run', '-'], input=json.dumps(request), text=True, capture_output=True)
    assert result.returncode == 0, result.stderr
    header = fits.getheader(exported)
    parameter_count = {'AZP': 2, 'AIR': 1, 'SZP': 3}[case['get']]
    for index, value in enumerate(case['parameters'][:parameter_count], 1):
        assert math.isclose(header[f'PV2_{index}'], value, rel_tol=1e-12, abs_tol=0), (case, header)
    ia = image(); ia.open(str(exported)); check_world(ia, case); ia.done()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('binary', type=lambda path: Path(path).resolve())
    parser.add_argument('--exportfits', type=lambda path: Path(path).resolve())
    args = parser.parse_args()
    root = Path('crates/casars-imexplore/tests/data/imhead_crpix')
    expected = json.loads((root / 'projection_edit_expected.json').read_text())
    assert version_string() == expected['casa_version']
    with tempfile.TemporaryDirectory(prefix='imhead-projection-interop-') as temp:
        for index, case in enumerate(expected['cases']):
            path = Path(temp) / f'{index}.im'; shutil.copytree(root / f"{case['image']}.im", path)
            if 'initial' in case:
                imhead(str(path), mode='put', hdkey='projection', hdvalue=case['initial'])
            tb = table(); tb.open(str(path)); before = plain(tb.getkeyword('coords')); tb.close()
            ia = image(); ia.open(str(path)); count = len(ia.history(list=False)); ia.done()
            request = {'surface': 'imhead', 'values': {'imagename': str(path), 'mode': 'put', 'hdkey': case['key'], 'hdvalue': case['value']}}
            result = subprocess.run([str(args.binary), 'imhead', '--json-run', '-'], input=json.dumps(request), text=True, capture_output=True)
            assert (result.returncode != 0) == ('error' in case), (case, result.stderr)
            assert imhead(str(path), mode='get', hdkey='projection') == case['get'], case
            tb.open(str(path)); after = plain(tb.getkeyword('coords')); tb.close()
            before['direction0']['projection'] = case['get']
            before['direction0']['projection_parameters'] = case['parameters']
            # WCSLIB and the native transform can round the pole differently.
            saved_pole = after['direction0']['latpole']
            assert math.isclose(saved_pole, case['latpole'], rel_tol=0, abs_tol=1e-12), (case, saved_pole)
            before['direction0']['latpole'] = saved_pole
            assert before == after, (case, before, after)
            ia = image(); ia.open(str(path)); history = ia.history(list=False)[count:]
            assert len(history) == len(case['history']), (case, history)
            if len(history) == 3: assert history[0] == case['history'][0], (case, history)
            check_world(ia, case)
            ia.done()
            if args.exportfits and case['get'] in ('AZP', 'AIR', 'SZP'):
                check_fits_export(args.exportfits, path, case)
    print(f"CASA {version_string()} reopened {len(expected['cases'])} projection edit results")


if __name__ == '__main__':
    main()
