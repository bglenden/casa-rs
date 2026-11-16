#!/usr/bin/env python3
"""Have CASA reopen native imhead observation-date edits."""
import argparse
import json
import shutil
import subprocess
import tempfile
from pathlib import Path
from casatasks import imhead
from casatools import image, version_string


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('binary', type=lambda path: Path(path).resolve())
    args = parser.parse_args()
    root = Path('crates/casars-imexplore/tests/data/imhead_dates')
    expected = json.loads((root / 'edit_expected.json').read_text())
    assert version_string() == expected['casa_version']
    with tempfile.TemporaryDirectory(prefix='imhead-date-edit-interop-') as temp:
        for index, case in enumerate(expected['cases']):
            path = Path(temp) / f'{index}.im'
            shutil.copytree(root / f"{case['image']}.im", path)
            ia = image(); ia.open(str(path)); count = len(ia.history(list=False)); ia.done()
            request = {'surface': 'imhead', 'values': {'imagename': str(path), 'mode': 'put', 'hdkey': case['key'], 'hdvalue': case['value']}}
            result = subprocess.run([str(args.binary), 'imhead', '--json-run', '-'], input=json.dumps(request), text=True, capture_output=True)
            assert (result.returncode != 0) == ('error' in case), (case, result.stderr)
            assert imhead(str(path), mode='get', hdkey='date-obs') == case['get'], case
            ia = image(); ia.open(str(path)); history = ia.history(list=False)[count:]; ia.done()
            if 'error' not in case:
                assert history[0] == case['history'][0], (case, history)
                assert history[-1] == case['history'][-1].replace('<IMAGE>', str(path)), (case, history)
    print(f"CASA {version_string()} reopened {len(expected['cases'])} observation-date edit results")


if __name__ == '__main__':
    main()
