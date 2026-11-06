#!/usr/bin/env python3
"""Have CASA reopen reference-pixel edits written by the native task."""
import argparse
import json
import shutil
import subprocess
import tempfile
from pathlib import Path
from casatasks import imhead
from casatools import image, version_string


def plain(value):
    return value.tolist() if hasattr(value, 'tolist') else value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('binary', type=lambda p: Path(p).resolve()); args = parser.parse_args()
    root = Path('crates/casars-imexplore/tests/data/imhead_crpix')
    expected = json.loads((root / 'expected.json').read_text()); assert version_string() == expected['casa_version']
    with tempfile.TemporaryDirectory(prefix='imhead-crpix-interop-') as temp:
        for index, case in enumerate(expected['cases']):
            path = Path(temp) / f'{index}.im'; shutil.copytree(root / f"{case['image']}.im", path)
            request = {'surface': 'imhead', 'values': {'imagename': str(path), 'mode': 'put', 'hdkey': case['key'], 'hdvalue': case['value']}}
            result = subprocess.run([str(args.binary), 'imhead', '--json-run', '-'], input=json.dumps(request), text=True, capture_output=True)
            assert (result.returncode != 0) == ('error' in case), (case, result.stderr)
            summary = imhead(str(path), mode='summary')
            for key, value in case['after'].items(): assert plain(summary[key]) == value, (case, key, plain(summary[key]), value)
            if 'error' not in case:
                ia=image();ia.open(str(path));history=ia.history(list=False);ia.done()
                assert history[-3] == case['history'][0], (case, history[-3:])
                assert history[-1] == case['history'][2].replace('<IMAGE>', str(path)), (case, history[-3:])
    print(f"CASA {version_string()} reopened {len(expected['cases'])} native reference-pixel results")


if __name__ == '__main__':
    main()
