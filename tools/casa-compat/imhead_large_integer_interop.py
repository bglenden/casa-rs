#!/usr/bin/env python3
"""Check CASA reads imhead large-integer writes made by casa-rs."""
import argparse
import json
import math
import shutil
import subprocess
import tempfile
from pathlib import Path

from casatasks import imhead
from casatools import version_string


def plain(value):
    if isinstance(value, dict):
        return {key: plain(item) for key, item in value.items()}
    if hasattr(value, 'tolist'):
        return value.tolist()
    return value


def equal(actual, expected):
    if isinstance(expected, dict):
        return actual.keys() == expected.keys() and all(equal(actual[k], v) for k, v in expected.items())
    if isinstance(expected, list):
        return len(actual) == len(expected) and all(equal(a, b) for a, b in zip(actual, expected))
    if isinstance(expected, float):
        return math.isclose(actual, expected, rel_tol=1e-12, abs_tol=1e-12)
    return actual == expected


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('binary', type=lambda p: Path(p).resolve())
    args = parser.parse_args()
    root = Path('crates/casars-imexplore/tests/data/imhead_crpix')
    oracle = json.loads((root / 'large_integer_expected.json').read_text())
    assert version_string() == oracle['casa_version']
    with tempfile.TemporaryDirectory(prefix='imhead-large-int-interop-') as temp:
        for index, case in enumerate(oracle['cases']):
            path = Path(temp) / f'{index}.im'
            shutil.copytree(root / 'normal.im', path)
            request = {'surface': 'imhead', 'values': {'imagename': str(path), 'mode': case['mode'],
                       'hdkey': case['key'], 'hdvalue': case['value']}}
            result = subprocess.run([str(args.binary), 'imhead', '--json-run', '-'],
                                    input=json.dumps(request), text=True, capture_output=True, check=True)
            reply = json.loads(result.stdout)
            assert reply['status'] == 'succeeded', (index, reply)
            actual = plain(imhead(str(path), mode='get', hdkey=case['key']))
            assert equal(actual, case['get']), (index, actual, case['get'])
    print(f"CASA reopened {len(oracle['cases'])} large-integer writes")


if __name__ == '__main__':
    main()
