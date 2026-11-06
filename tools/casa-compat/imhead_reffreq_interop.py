#!/usr/bin/env python3
"""Have CASA reopen native imhead spectral-reference edits."""
import argparse
import json
import math
import shutil
import subprocess
import tempfile
from pathlib import Path
from casatasks import imhead
from casatools import image, version_string


def plain(value):
    return value.tolist() if hasattr(value, 'tolist') else value


def close(actual, expected):
    if isinstance(expected, list):
        return len(actual) == len(expected) and all(close(a, b) for a, b in zip(actual, expected))
    if isinstance(expected, (int, float)):
        return math.isclose(actual, expected, rel_tol=5e-14, abs_tol=5e-14)
    return actual == expected


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('binary', type=lambda p: Path(p).resolve()); args = parser.parse_args()
    root = Path('crates/casars-imexplore/tests/data/imhead_crpix')
    expected = json.loads((root / 'reffreq_expected.json').read_text()); assert version_string() == expected['casa_version']
    with tempfile.TemporaryDirectory(prefix='imhead-reffreq-interop-') as temp:
        for index, case in enumerate(expected['cases']):
            path = Path(temp) / f'{index}.im'; shutil.copytree(root / f"{case['image']}.im", path)
            ia = image(); ia.open(str(path)); count = len(ia.history(list=False)); ia.done()
            request = {'surface': 'imhead', 'values': {'imagename': str(path), 'mode': 'put', 'hdkey': case['key'], 'hdvalue': case['value']}}
            result = subprocess.run([str(args.binary), 'imhead', '--json-run', '-'], input=json.dumps(request), text=True, capture_output=True)
            assert (result.returncode != 0) == ('error' in case), (case, result.stderr)
            summary = imhead(str(path), mode='summary')
            for key, value in case['after'].items(): assert close(plain(summary[key]), value), (case, key, plain(summary[key]), value)
            if 'get' in case: assert imhead(str(path), mode='get', hdkey='reffreqtype') == case['get'], case
            if 'error' not in case:
                ia = image(); ia.open(str(path)); coords = ia.coordsys().torecord(); history = ia.history(list=False)[count:]; ia.done()
                rec = coords['spectral0' if case['image'].startswith('multi') else 'spectral2']
                assert rec['system'] == case['system'], (case, rec['system'])
                assert rec.get('conversion', {}).get('system') == case['conversion_system'], case
                if 'systems' in case: assert [coords['spectral0']['system'], coords['spectral1']['system']] == case['systems'], case
                assert len(history) == len(case['history']), (case, history)
                if len(history) == 3: assert history[0] == case['history'][0], (case, history)
                assert history[-1] == case['history'][-1].replace('<IMAGE>', str(path)), (case, history)
        for index, case in enumerate(expected['restfreq_cases']):
            path = Path(temp) / f'restfreq-{index}.im'; shutil.copytree(root / f"{case['image']}.im", path)
            request = {'surface': 'imhead', 'values': {'imagename': str(path), 'mode': 'put', 'hdkey': 'restfreq', 'hdvalue': '1.5GHz'}}
            result = subprocess.run([str(args.binary), 'imhead', '--json-run', '-'], input=json.dumps(request), text=True, capture_output=True)
            assert result.returncode == 0, result.stderr
            assert imhead(str(path), mode='get', hdkey='restfreq') == case['get'], case
            ia = image(); ia.open(str(path)); coords = ia.coordsys().torecord(); ia.done()
            assert close([coords['spectral0']['restfreq'], coords['spectral1']['restfreq']], case['frequencies']), case
    print(f"CASA {version_string()} reopened {len(expected['cases'])} spectral-reference results")


if __name__ == '__main__':
    main()
