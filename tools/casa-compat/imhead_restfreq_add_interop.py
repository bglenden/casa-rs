#!/usr/bin/env python3
"""Have CASA reopen native imhead rest-frequency additions."""
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
    root = Path('crates/casars-imexplore/tests/data/imhead_crpix')
    expected = json.loads((root / 'restfreq_add_expected.json').read_text())
    assert version_string() == expected['casa_version']
    with tempfile.TemporaryDirectory(prefix='imhead-rest-add-interop-') as temp:
        for index, case in enumerate(expected['cases']):
            path = Path(temp) / f'{index}.im'; shutil.copytree(root / f"{case['image']}.im", path)
            ia = image(); ia.open(str(path)); count = len(ia.history(list=False)); ia.done()
            request = {'surface': 'imhead', 'values': {'imagename': str(path), 'mode': 'add', 'hdkey': case['key'], 'hdvalue': case['value']}}
            result = subprocess.run([str(args.binary), 'imhead', '--json-run', '-'], input=json.dumps(request), text=True, capture_output=True)
            assert (result.returncode != 0) == ('error' in case), (case, result.stderr)
            value = imhead(str(path), mode='get', hdkey='restfreq')
            assert value == case['get'], (case, value)
            ia = image(); ia.open(str(path)); history = ia.history(list=False)[count:]; ia.done()
            if 'error' not in case:
                assert history[0] == case['history'][0], (case, history)
                assert history[-1] == case['history'][-1].replace('<IMAGE>', str(path)), (case, history)
    print(f"CASA {version_string()} reopened {len(expected['cases'])} rest-frequency addition results")


if __name__ == '__main__':
    main()
