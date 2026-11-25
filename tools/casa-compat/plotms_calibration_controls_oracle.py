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
import numpy as np
from PIL import Image

from calibration_smoothcal_oracle import copy_ms, fixture, opened
from plotms_calibration_oracle import plotms_launcher


def paired_text(root, native, request, options, name):
    outputs = [root / f'{name}.{engine}.txt' for engine in ('casa', 'native')]
    for api, dest in zip((reference, native), outputs, strict=True):
        assert api(**request, **options, plotfile=str(dest), expformat='txt') is None
    first, second = [path.read_bytes() for path in outputs]
    assert first == second
    return first


def time_presentation(root, native, gain):
    from casars.tasks import _plotms_plot

    with opened(gain, True) as table:
        times = table.getcol("TIME")
        table.putcol("TIME", times.min() + 300 * np.searchsorted(np.unique(times), times))
    title = "Gain amplitude and phase over time"
    request = dict(caltable=str(gain), xaxis="time", yaxis=["gainamp", "gainphase"],
                   yaxislocation=["left", "right"], antenna="A0,A2,A3",
                   showgui=False, overwrite=True, title=title)
    paired_text(root, native, request, {}, "clock")
    export = request | dict(expformat="png", width=940, height=610, dpi=100)
    reference(**export, plotfile=str(root / "clock.casa.png"))
    configure = _plotms_plot.configure
    captures = []

    def capture(figure, axis, document, params):
        configure(figure, axis, document, params)
        figure.canvas.draw()
        titles = [text.get_text() for text in figure.texts] + [axis.get_title()]
        assert titles.count(title) == 1
        assert axis.get_xlabel().startswith("Time (from ")
        clock = axis.xaxis.get_major_formatter()
        assert all(clock(tick).endswith(":00") for tick in axis.get_xticks())
        assert axis.get_position().width > 0.83
        captures.append(figure)

    with patch.object(_plotms_plot, "configure", capture):
        native(**export, plotfile=str(root / "clock.native.png"))
    assert len(captures) == 1
    print("Calibration clock labels, single title and data area verified.", flush=True)


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
        figure.canvas.draw()
        bounds = [item.get_window_extent() for item in figure.texts
                  if item.get_text() in document['header_lines']]
        assert len(bounds) == len(document['header_lines'])
        assert all(0 <= box.x0 < box.x1 <= figure.bbox.width for box in bounds)
        assert all(0 <= box.y0 < box.y1 <= figure.bbox.height for box in bounds)
        assert all(upper.y0 > lower.y1 for upper, lower in zip(bounds, bounds[1:]))

    with patch.object(_plotms_plot, 'configure', capture):
        assert native(**request, **options, headeritems=headers, plotfile=str(native_path)) is None
        high_resolution = options | dict(dpi=-1, highres=True)
        high_path = root / 'header.high.png'
        assert native(**request, **high_resolution, headeritems=headers, plotfile=str(high_path)) is None
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
    for path in native_path, high_path:
        with Image.open(path) as image:
            assert image.size == (1000, 400)
    print('CASA page labels, native table metadata and the requested PNG dimensions verified.', flush=True)


def page_metadata(root, native, request, product):
    from casars.tasks import _plotms_plot

    captures = []
    configure = _plotms_plot.configure

    def capture(figure, axes, document, params):
        configure(figure, axes, document, params)
        captures.append(' '.join(' '.join(document['header_lines']).split()))

    base = request | dict(field='', headeritems='observer,projid,targname,targdir', expformat='png')
    with patch.object(_plotms_plot, 'configure', capture):
        assert native(**base, iteraxis='field', exprange='all', plotfile=str(root / 'pages.png')) is None
        assert len(captures) == 2
        assert 'Target name: FLUX' in captures[0] and '-30.00.00.00000' in captures[0]
        assert 'Target name: PHASE' in captures[1] and '01:00:00.000000' in captures[1]

        unknown = root / 'wildcard.cal'
        copy_ms(product, unknown)
        with opened(unknown, True) as table:
            for column in ('FIELD_ID', 'OBSERVATION_ID'):
                values = table.getcol(column)
                values.fill(-1)
                table.putcol(column, values)
        assert native(**(base | dict(caltable=str(unknown))), plotfile=str(root / 'wildcard.png')) is None
        assert captures[-1] == 'Observer: Project ID: Target name: Target direction:', captures[-1]

        polynomial = root / 'polynomial.cal'
        copy_ms(product, polynomial)
        with opened(polynomial / 'FIELD', True) as field:
            field.putcell('NUM_POLY', 0, 1)
            field.putcell('PHASE_DIR', 0, np.array([[0.0, 0.0001], [-np.pi / 6, 0.0002]]))
            field.putcolkeyword('PHASE_DIR', 'MEASINFO', dict(
                type='direction', VarRefCol='NUM_POLY', TabRefTypes=['J2000', 'B1950'],
                TabRefCodes=np.array([0, 1], dtype=np.uint32),
            ))
        assert native(**(base | dict(caltable=str(polynomial), field='FLUX')),
                      plotfile=str(root / 'polynomial.png')) is None
        assert '00:00:00.000000 -30.00.00.00000 B1950' in captures[-1], captures[-1]
    print('Independent field pages, wildcard metadata and polynomial direction verified.', flush=True)


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
        page_metadata(root, native, controls, product)
        time_presentation(root, native, root / "gain.cal")
    print('Three paired exports and both active native transform cases preserved the calibration coordinates.', flush=True)


if __name__ == '__main__':
    main()
