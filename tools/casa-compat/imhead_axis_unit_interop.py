#!/usr/bin/env python3
"""Have CASA reopen native imhead axis-unit edits."""
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
    if isinstance(expected, dict):
        return actual.keys() == expected.keys() and all(close(actual[k], v) for k, v in expected.items())
    if isinstance(expected, list):
        return len(actual) == len(expected) and all(close(a, b) for a, b in zip(actual, expected))
    if isinstance(expected, (int, float)):
        return math.isclose(actual, expected, rel_tol=5e-14, abs_tol=5e-14)
    return actual == expected


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('binary', type=lambda p: Path(p).resolve()); args = parser.parse_args()
    root = Path('crates/casars-imexplore/tests/data/imhead_crpix')
    expected = json.loads((root / 'axis_unit_expected.json').read_text()); assert version_string() == expected['casa_version']
    with tempfile.TemporaryDirectory(prefix='imhead-axis-unit-interop-') as temp:
        for index, case in enumerate(expected['cases']):
            path = Path(temp) / f'{index}.im'; shutil.copytree(root / f"{case['image']}.im", path)
            request = {'surface': 'imhead', 'values': {'imagename': str(path), 'mode': 'put', 'hdkey': case['key'], 'hdvalue': case['value']}}
            result = subprocess.run([str(args.binary), 'imhead', '--json-run', '-'], input=json.dumps(request), text=True, capture_output=True)
            assert (result.returncode != 0) == ('error' in case), (case, result.stderr)
            summary = imhead(str(path), mode='summary')
            for key, value in case['after'].items(): assert close(plain(summary[key]), value), (case, key, plain(summary[key]), value)
            if 'restfreq' in case:
                actual = imhead(str(path), mode='get', hdkey='restfreq')
                assert close(actual, case['restfreq']), (case, actual)
                ia = image(); ia.open(str(path)); coords = ia.coordsys().torecord()['spectral2']; ia.done()
                assert close(coords['restfreqs'].tolist(), case['frequencies']), case
            if 'error' not in case:
                ia = image(); ia.open(str(path)); history = ia.history(list=False); ia.done()
                assert history[-3] == case['history'][0], (case, history[-3:])
                assert history[-1] == case['history'][2].replace('<IMAGE>', str(path)), (case, history[-3:])
    print(f"CASA {version_string()} reopened {len(expected['cases'])} axis-unit results")


if __name__ == '__main__':
    main()
