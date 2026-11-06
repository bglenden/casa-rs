#!/usr/bin/env python3
"""Have CASA reopen native imhead beam metadata edits."""
import argparse
import json
import math
import shutil
import subprocess
import tempfile
from pathlib import Path
from casatools import image, version_string


def plain(value):
    if hasattr(value, 'tolist'): return value.tolist()
    if isinstance(value, dict): return {str(k): plain(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)): return [plain(v) for v in value]
    return value


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
    expected = json.loads((root / 'beam_edit_expected.json').read_text()); assert version_string() == expected['casa_version']
    with tempfile.TemporaryDirectory(prefix='imhead-beam-edit-interop-') as temp:
        for index, case in enumerate(expected['cases']):
            src = root.parent / 'imhead_beams' / ('multiple.im' if case['image'] == 'multi' else 'single_dual.im') if case['image'] in ['multi', 'single_dual'] else root / f"{case['image']}.im"
            path = Path(temp) / f'{index}.im'; shutil.copytree(src, path)
            ia = image(); ia.open(str(path)); count = len(ia.history(list=False)); ia.done()
            request = {'surface': 'imhead', 'values': {'imagename': str(path), 'mode': case['mode'], 'hdkey': case['key'], 'hdvalue': case['value']}}
            result = subprocess.run([str(args.binary), 'imhead', '--json-run', '-'], input=json.dumps(request), text=True, capture_output=True)
            assert (result.returncode != 0) == ('error' in case), (case, result.stderr)
            ia = image(); ia.open(str(path)); beam = plain(ia.restoringbeam()); history = ia.history(list=False)[count:]; ia.done()
            assert close(beam, case['beam']), (case, beam)
            if 'error' not in case:
                assert len(history) == len(case['history']), (case, history)
                if len(history) == 3: assert history[0] == case['history'][0], (case, history)
                if history: assert history[-1] == case['history'][-1].replace('<IMAGE>', str(path)), (case, history)
    print(f"CASA {version_string()} reopened {len(expected['cases'])} beam-edit results")


if __name__ == '__main__':
    main()
