#!/usr/bin/env python3
"""Have CASA reopen native custom-array imhead operations."""
import argparse
import json
import shutil
import subprocess
import tempfile
from pathlib import Path
from casatasks import imhead
from casatools import image, table, version_string


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('binary', type=lambda path: Path(path).resolve())
    args = parser.parse_args()
    root = Path('crates/casars-imexplore/tests/data/imhead_crpix')
    expected = json.loads((root / 'custom_array_expected.json').read_text())
    assert version_string() == expected['casa_version']
    with tempfile.TemporaryDirectory(prefix='imhead-custom-array-interop-') as temp:
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
            tb = table(); tb.open(str(path)); info = tb.getkeyword('miscinfo'); tb.close()
            if 'stored' not in case: assert 'custom' not in info, (index, info)
            else:
                stored = info['custom']; assert stored.dtype.kind == case['stored']['kind'] and stored.tolist() == case['stored']['data'], (index, stored)
                try: imhead(str(path), mode='get', hdkey='custom')
                except RuntimeError as error: assert 'Unhandled data type' in str(error), (index, error)
                else: raise AssertionError(f'{index}: CASA unexpectedly read an array')
    print(f"CASA {version_string()} reopened {len(expected['cases'])} custom-array sequences")


if __name__ == '__main__':
    main()
