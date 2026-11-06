#!/usr/bin/env python3
"""Have CASA reopen native imhead axis-quantity edits."""
import argparse
import ast
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


def task_arguments(text):
    call = ast.parse(text, mode='eval').body
    assert isinstance(call, ast.Call) and isinstance(call.func, ast.Name) and call.func.id == 'imhead'
    return {keyword.arg: ast.literal_eval(keyword.value) for keyword in call.keywords}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('binary', type=lambda p: Path(p).resolve()); args = parser.parse_args()
    root = Path('crates/casars-imexplore/tests/data/imhead_crpix')
    expected = json.loads((root / 'axis_quantity_expected.json').read_text()); assert version_string() == expected['casa_version']
    with tempfile.TemporaryDirectory(prefix='imhead-axis-quantity-interop-') as temp:
        for index, case in enumerate(expected['cases']):
            path = Path(temp) / f'{index}.im'; shutil.copytree(root / f"{case['image']}.im", path)
            request = {'surface': 'imhead', 'values': {'imagename': str(path), 'mode': 'put', 'hdkey': case['key'], 'hdvalue': case['value']}}
            result = subprocess.run([str(args.binary), 'imhead', '--json-run', '-'], input=json.dumps(request), text=True, capture_output=True)
            assert (result.returncode != 0) == ('error' in case), (case, result.stderr)
            summary = imhead(str(path), mode='summary')
            for key, value in case['after'].items(): assert close(plain(summary[key]), value), (case, key, plain(summary[key]), value)
            if 'error' not in case:
                ia = image(); ia.open(str(path)); history = ia.history(list=False); ia.done()
                assert history[-3] == case['history'][0], (case, history[-3:])
                expected_task = case['history'][2].replace('<IMAGE>', str(path))
                assert task_arguments(history[-1]) == task_arguments(expected_task), (case, history[-3:])
    print(f"CASA {version_string()} reopened {len(expected['cases'])} axis-quantity results")


if __name__ == '__main__':
    main()
