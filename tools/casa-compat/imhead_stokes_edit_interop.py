#!/usr/bin/env python3
"""Have CASA reopen native imhead polarization edits."""
import argparse
import json
import shutil
import subprocess
import tempfile
from pathlib import Path
from casatasks import imhead
from casatools import image, table, version_string


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
    expected = json.loads((root / 'stokes_edit_expected.json').read_text())
    assert version_string() == expected['casa_version']
    with tempfile.TemporaryDirectory(prefix='imhead-stokes-interop-') as temp:
        for index, case in enumerate(expected['cases']):
            path = Path(temp) / f'{index}.im'; shutil.copytree(root / f"{case['image']}.im", path)
            tb = table(); tb.open(str(path)); before = plain(tb.getkeyword('coords')); tb.close()
            ia = image(); ia.open(str(path)); count = len(ia.history(list=False)); ia.done()
            request = {'surface':'imhead', 'values':{'imagename':str(path), 'mode':'put', 'hdkey':case['key'], 'hdvalue':case['value']}}
            result = subprocess.run([str(args.binary), 'imhead', '--json-run', '-'], input=json.dumps(request), text=True, capture_output=True)
            assert (result.returncode != 0) == ('error' in case), (index, result.stderr)
            assert imhead(str(path), mode='get', hdkey=case['key']).tolist() == case['get'], index
            ia = image(); ia.open(str(path)); history = ia.history(list=False)[count:]; ia.done()
            assert len(history) == len(case['history']), (index, history)
            if history: assert history[0] == case['history'][0], (index, history)
            tb.open(str(path)); after = plain(tb.getkeyword('coords')); tb.close()
            # Only the selected Stokes coordinate is replaced; maps and all
            # other coordinates retain their original stored values.
            before.pop('stokes1', None); after.pop('stokes1', None)
            assert before == after, index
    print(f"CASA {version_string()} reopened {len(expected['cases'])} polarization edit results")


if __name__ == '__main__': main()
