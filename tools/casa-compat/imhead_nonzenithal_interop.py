#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Reopen native direction-coordinate edits with CASA and compare the result."""

import json
import shutil
import tempfile
from pathlib import Path

import numpy as np
from casatasks import casalog, imhead
from casatools import image, quanta

from imhead_spectral_units_interop import ROOT, run


def main():
    casalog.setlogfile('/tmp/casa-977-direction-interop.log')
    casalog.showconsole(False)
    cases = json.loads((ROOT / 'nonzenithal_expected.json').read_text())['cases']
    qa = quanta()
    with tempfile.TemporaryDirectory(prefix='imhead-direction-interop-') as scratch:
        for index, case in enumerate(cases):
            if 'error' in case:
                continue
            path = Path(scratch) / f'{index}.im'
            shutil.copytree(ROOT / f"{case['image']}.im", path)
            run('imexplore', 'imhead', {'imagename': str(path), 'mode': 'put',
                'hdkey': case['key'], 'hdvalue': case['value']})
            summary = imhead(str(path), mode='summary')
            assert list(summary['axisunits']) == case['axisunits']
            for key in ('refval', 'refpix', 'incr'):
                np.testing.assert_allclose(summary[key], case[key], rtol=1e-10, atol=1e-10)
            ia = image(); ia.open(str(path)); coords = ia.coordsys()
            direction = next(v for k, v in coords.torecord().items() if k.startswith('direction'))
            assert direction['projection'] == case['projection']
            np.testing.assert_allclose([direction['longpole'], direction['latpole']],
                [case['longpole'], case['latpole']], rtol=1e-10, atol=1e-10)
            for pair, expected in zip(([0.0, 0.0], [2.5, 1.0]), case['worlds']):
                world = ia.toworld([*pair, 0.0, 0.0], format='q')['quantity']
                actual = [qa.convert(world[f'*{axis}'], 'rad')['value'] for axis in (1, 2)]
                np.testing.assert_allclose(actual, expected, rtol=1e-10, atol=1e-10)
            coords.done(); ia.done()
    print(f"CASA reopened {sum('error' not in case for case in cases)} native direction edits")


if __name__ == '__main__':
    main()
