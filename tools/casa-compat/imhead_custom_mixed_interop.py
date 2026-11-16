#!/usr/bin/env python3
"""Have CASA reopen native mixed-type custom-array writes."""
import argparse
import json
import shutil
import subprocess
import tempfile
from pathlib import Path
from casatools import image, table, version_string


def plain(value):
    if hasattr(value, 'tolist'): return value.tolist()
    if isinstance(value, dict): return {str(k):plain(v) for k,v in value.items()}
    if isinstance(value, (tuple,list)): return [plain(v) for v in value]
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('binary', type=lambda path:Path(path).resolve())
    args = parser.parse_args()
    root = Path('crates/casars-imexplore/tests/data/imhead_crpix')
    expected = json.loads((root / 'custom_mixed_expected.json').read_text())
    assert version_string() == expected['casa_version']
    with tempfile.TemporaryDirectory(prefix='imhead-custom-mixed-interop-') as temp:
        for index,case in enumerate(expected['cases']):
            path = Path(temp) / f'{index}.im'; shutil.copytree(root / 'normal.im', path)
            ia = image(); ia.open(str(path)); count = len(ia.history(list=False)); ia.done()
            request = {'surface':'imhead', 'values':{'imagename':str(path), 'mode':'put', 'hdkey':'custom', 'hdvalue':case['value']}}
            result = subprocess.run([str(args.binary),'imhead','--json-run','-'], input=json.dumps(request), text=True, capture_output=True)
            assert result.returncode == 0, (index,result.stderr)
            ia = image(); ia.open(str(path)); history = ia.history(list=False)[count:]; ia.done()
            assert history[0] == case['history'][0], (index,history)
            tb = table(); tb.open(str(path)); stored = tb.getkeyword('miscinfo')['custom']; tb.close()
            assert plain(stored) == case['stored'], (index,stored)
            if 'shape' in case: assert list(stored.shape) == case['shape'], (index,stored.shape)
    print(f"CASA {version_string()} reopened {len(expected['cases'])} mixed-array results")


if __name__ == '__main__': main()
