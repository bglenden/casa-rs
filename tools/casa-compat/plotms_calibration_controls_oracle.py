#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Check calibration-table PlotMS headers and unchanged spectral coordinates."""

import argparse
from contextlib import chdir
import os
from pathlib import Path
import subprocess
from unittest.mock import patch

import casatasks as casa
from casaplotms import plotms as reference
from PIL import Image

from calibration_smoothcal_oracle import fixture, opened
from plotms_calibration_oracle import plotms_launcher


def paired_text(root, native, request, options, name):
    outputs = [root / f'{name}.{engine}.txt' for engine in ('casa', 'native')]
    for api, dest in zip((reference, native), outputs, strict=True):
        assert api(**request, **options, plotfile=str(dest), expformat='txt') is None
    first, second = [path.read_bytes() for path in outputs]
    assert first == second
    return first


def figures(root, native, request, headers):
    from casars.tasks import _plotms_plot

    reference_path = root / 'header.casa.png'
    native_path = root / 'header.native.png'
    options = dict(expformat='png', width=1000, height=400, dpi=100, title='Calibration')
    assert reference(**request, **options, headeritems=headers, plotfile=str(reference_path)) is None
    rendered = []
    configure = _plotms_plot.configure

    def capture(figure, axes, document, params):
        configure(figure, axes, document, params)
        rendered.extend(text.get_text() for text in figure.texts)

    with patch.object(_plotms_plot, 'configure', capture):
        assert native(**request, **options, headeritems=headers, plotfile=str(native_path)) is None
    upstream = subprocess.run(['tesseract', str(reference_path), 'stdout', '--psm', '11'],
                              check=True, capture_output=True, text=True).stdout
    actual = '\n'.join(rendered)
    for label in ('File:', 'Y Column(s):', 'Observation date:', 'Observation time:', 'Observer:', 'Project ID:'):
        assert label.lower() in upstream.lower(), upstream
        assert label in actual, actual
    for text in ('File: bandpass.cal', 'Y Column(s): Amp', 'Observer: Calibration Observer',
                 'Project ID: Calibration Project', 'Telescope: ALMA', 'Target name: FLUX',
                 'Target direction: 00:00:00.000000 -30.00.00.00000 J2000'):
        assert text in actual, actual
    date = actual.split('Observation date:', 1)[1].split('Observation time:', 1)[0]
    assert ':' not in date and date.strip()
    assert 'Unrecognized' not in actual
    with Image.open(native_path) as image:
        assert image.size == (1000, 400)
    print('CASA page labels, native table metadata and the requested PNG dimensions verified.', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--native-bin', type=Path, required=True)
    parser.add_argument('--app-run', type=Path)
    args = parser.parse_args()
    root = args.work.resolve()
    root.mkdir()
    os.environ['CASARS_MSEXPLORE_BIN'] = str(args.native_bin.resolve())
    from casars.tasks import plotms as native

    assert casa.version_string() == '6.7.6.14'
    vis, product = root / 'source.ms', root / 'bandpass.cal'
    fixture(vis, root / 'gain.cal')
    casa.bandpass(str(vis), str(product), refant='A0', solint='inf', minsnr=0.0)
    with opened(product / 'OBSERVATION', True) as metadata:
        metadata.putcell('OBSERVER', 0, 'Calibration Observer')
        metadata.putcell('PROJECT', 0, 'Calibration Project')
    controls = dict(caltable=str(product), showgui=False, overwrite=True, xaxis='frequency',
                    yaxis='amp', antenna='A0', field='FLUX', spw='0')
    headers = 'filename,ycolumn,obsdate,obstime,observer,projid,telescope,targname,targdir,Unrecognized'
    transforms = dict(freqframe='BARY', restfreq='90GHz', veldef='OPTICAL')
    with chdir(root), plotms_launcher(args.app_run):
        base = paired_text(root, native, controls, {}, 'default')
        assert paired_text(root, native, controls, dict(headeritems=headers), 'header') == base
        assert paired_text(root, native, controls, dict(transform=False, **transforms), 'inactive') == base
        # The pinned application aborts when these transforms are enabled on
        # a current calibration table. CalCache documents that they are ignored.
        for name, settings in [('frame', dict(freqframe='BARY')), ('velocity', transforms)]:
            output = root / f'{name}.native.txt'
            assert native(**controls, **settings, plotfile=str(output), expformat='txt') is None
            assert output.read_bytes() == base
        figures(root, native, controls, headers)
    print('Three paired exports and both active native transform cases preserved the calibration coordinates.', flush=True)


if __name__ == '__main__':
    main()
