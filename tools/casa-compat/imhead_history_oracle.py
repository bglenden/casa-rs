#!/usr/bin/env python3
"""Record CASA's timestamped rendering of an existing image history."""
import json
import shutil
import tempfile
from pathlib import Path
from casatasks import casalog, imhead
from casatools import image, version_string


def main():
    root = Path('crates/casars-imexplore/tests/data/imhead_put')
    with tempfile.TemporaryDirectory(prefix='imhead-history-oracle-') as temp:
        path = Path(temp) / 'history.im'; shutil.copytree(root / 'base.im', path)
        ia = image(); ia.open(str(path)); messages = ia.history(list=False); ia.done()
        log = Path(temp) / 'casa.log'; casalog.setlogfile(str(log))
        assert imhead(str(path), mode='history') is None
        lines = log.read_text().splitlines()
        output = []
        for message in messages:
            matches = [line for line in lines if line.split('\t', 3)[-1] == message]
            assert len(matches) == 1, (message, matches)
            output.append(matches[0])
    (root / 'history_expected.json').write_text(json.dumps({'casa_version': version_string(), 'lines': output}, indent=2) + '\n')


if __name__ == '__main__':
    main()
