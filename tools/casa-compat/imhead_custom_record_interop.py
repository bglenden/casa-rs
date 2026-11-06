#!/usr/bin/env python3
"""Have CASA reopen native imhead record operations."""
import argparse
import json
import shutil
import subprocess
import tempfile
from pathlib import Path
from casatasks import imhead
from casatools import image, version_string


def plain(value):
    if hasattr(value, 'tolist'): return value.tolist()
    if isinstance(value, dict): return {str(k): plain(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)): return [plain(v) for v in value]
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('binary', type=lambda path: Path(path).resolve())
    args = parser.parse_args()
    root = Path('crates/casars-imexplore/tests/data/imhead_crpix')
    expected = json.loads((root / 'custom_record_expected.json').read_text())
    assert version_string() == expected['casa_version']
    with tempfile.TemporaryDirectory(prefix='imhead-custom-record-interop-') as temp:
        for index, case in enumerate(expected['cases']):
            path = Path(temp) / f'{index}.im'; shutil.copytree(root / 'normal.im', path)
            for op in case['operations']:
                ia = image(); ia.open(str(path)); count = len(ia.history(list=False)); ia.done()
                request = {'surface': 'imhead', 'values': {'imagename': str(path), 'mode': op['mode'], 'hdkey': 'custom', 'hdvalue': op['value']}}
                result = subprocess.run([str(args.binary), 'imhead', '--json-run', '-'], input=json.dumps(request), text=True, capture_output=True)
                assert (result.returncode != 0) == ('error' in op), (index, op, result.stderr)
                ia = image(); ia.open(str(path)); history = ia.history(list=False)[count:]; ia.done()
                assert len(history) == len(op['history']), (index, history)
                if history: assert history[0] == op['history'][0], (index, history)
            try: value = plain(imhead(str(path), mode='get', hdkey='custom'))
            except RuntimeError: assert 'get_error' in case, index
            else: assert value == case['get'], (index, value)
    print(f"CASA {version_string()} reopened {len(expected['cases'])} custom-record sequences")


if __name__ == '__main__': main()
