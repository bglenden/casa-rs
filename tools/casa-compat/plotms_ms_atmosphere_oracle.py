#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare MeasurementSet PlotMS atmospheric curves with pinned CASA."""

import argparse
from contextlib import chdir
import os
from pathlib import Path
from unittest.mock import patch

import casatasks as casa
from casaplotms import plotms as reference
import numpy as np
from matplotlib.colors import to_rgba
from PIL import Image

from plotms_atmosphere_oracle import atmosphere_inputs, fixture, opened
from plotms_calibration_oracle import plotms_launcher
from calibration_smoothcal_oracle import copy_ms
from plotms_callibrary_oracle import snapshot, verify_source


def read_sections(path):
    headers, sections = [], []
    for line in path.read_text().splitlines():
        if line.startswith('#'):
            headers.append(line)
            if line.endswith('MJD(seconds) GHz None None None'):
                label = line.split(' None', 1)[0]
                sections.append((label, []))
        elif line.strip():
            sections[-1][1].append(line.split())
    return headers, sections


def arrays(rows):
    return np.array([row[:2] for row in rows], dtype=float)


def paired_text(root, native, name, request):
    casa_path = root / f'{name}.casa.txt'
    native_path = root / f'{name}.native.txt'
    assert reference(**request, plotfile=str(casa_path), expformat='txt') is None
    assert native(**request, plotfile=str(native_path), expformat='txt') is None
    headers, expected = read_sections(casa_path)
    actual_headers, actual = read_sections(native_path)
    assert actual_headers == headers, (actual_headers, headers)
    points, nrms = compare_sections(actual, expected)
    print(f'{name}: {points} points, maximum overlay NRMS {nrms:.6g}', flush=True)
    return expected, points, nrms


def compare_sections(actual, expected):
    nrms = 0.0
    points = 0
    for (label, a), (expected_label, b) in zip(actual, expected, strict=True):
        assert label == expected_label, (label, expected_label)
        assert a and len(a) == len(b), (label, len(a), len(b))
        assert [row[2:] for row in a] == [row[2:] for row in b], label
        observed, target = arrays(a), arrays(b)
        atmospheric = any(axis in label for axis in ('Atm Transmission', 'Tsky', 'Image Sideband'))
        np.testing.assert_array_equal(observed[:, 0], target[:, 0], err_msg=label)
        np.testing.assert_allclose(observed[:, 1], target[:, 1], rtol=2e-4 if atmospheric else 1.2e-5,
                                   atol=1e-7, err_msg=label)
        y = target[:, 1]
        finite = np.isfinite(y)
        if atmospheric and np.linalg.norm(y[finite]):
            nrms = max(nrms, np.linalg.norm((observed[:, 1] - y)[finite]) / np.linalg.norm(y[finite]))
        points += len(a)
    return points, nrms


def images(root, native, name, request, expected, *, colored=False, pages=False):
    from casars.tasks import _plotms_plot

    directories = [root / f'{name}-{engine}' for engine in ('casa', 'native')]
    for directory in directories:
        directory.mkdir()
    captured = []
    configure = _plotms_plot.configure

    def record(fig, axes, document, options):
        configure(fig, axes, document, options)
        visible = [line for axis in fig.axes for line in axis.lines if len(line.get_xydata())]
        assert visible
        for line in visible:
            assert line.get_marker() not in (None, '', 'None', ' ')
            assert line.get_markersize() > 0.0
            assert line.get_linestyle() in ('None', '', ' ')
        captured.extend((axis.get_ylabel(), [(to_rgba(line.get_color()), line.get_xydata())
                                            for line in axis.lines]) for axis in fig.axes
                        if axis.get_ylabel().startswith('Atm Transmission'))

    options = dict(expformat='png', width=500, height=360, dpi=100)
    assert reference(**request, **options, plotfile=str(directories[0] / 'overlay.png')) is None
    with patch.object(_plotms_plot, 'configure', record):
        assert native(**request, **options, plotfile=str(directories[1] / 'overlay.png')) is None
    filenames = [sorted(path.name for path in directory.glob('*.png')) for directory in directories]
    assert filenames[0] and filenames[0] == filenames[1], filenames
    assert len(filenames[0]) == (2 if pages else 1), filenames
    for filename in filenames[1]:
        with Image.open(directories[1] / filename) as image:
            assert image.size == (500, 360)
    corr_colors = request.get('coloraxis') == 'corr'
    reference_curves = []
    for label, rows in expected:
        if 'Atm Transmission' not in label:
            continue
        if corr_colors:
            rows = sorted(rows, key=lambda row: row[-2])
        reference_curves.append(arrays(rows))
    colors = []
    if pages:
        assert len(captured) == len(reference_curves) == 2
    else:
        assert len(captured) == len(reference_curves) == 1
    for (label, lines), values in zip(captured, reference_curves, strict=True):
        assert label == 'Atm Transmission (%)'
        assert len(lines) == (2 if colored else 1)
        colors.extend(color for color, _ in lines)
        np.testing.assert_allclose(np.concatenate([points for _, points in lines]), values,
                                   rtol=2e-4, atol=1e-7)
    if colored:
        assert len(set(colors)) == 2
        with Image.open(directories[0] / filenames[0][0]) as image:
            rgb = np.asarray(image.convert('RGB'))
            # CASA's uncolored atmosphere uses opaque magenta. Both scan
            # color groups replace that curve in the colorized image.
            assert not np.any(np.all(rgb == [255, 0, 255], axis=-1))
            if corr_colors:
                assert all(np.any(np.all(rgb == color, axis=-1))
                           for color in ([211, 179, 235], [239, 186, 127]))
    print(f'{name}: {len(filenames[0])} matching figure names and '
          f'{sum(map(len, reference_curves))} rendered points', flush=True)


def combined_atmosphere(root, native):
    from grpc import RpcError, StatusCode
    from plotms_ms_spw_average_oracle import make_fixture

    vis = make_fixture(root, channel_count=8, integrations=4)
    with opened(vis, True) as table:
        times = table.getcol('TIME')
        boundary = np.median(np.unique(times))
        table.putcol('SCAN_NUMBER', (times >= boundary).astype(np.int32) + 1)
        table.removecols(['MODEL_DATA'])
    assert casa.setjy(str(vis), field='FLUX', standard='manual', usescratch=False,
                      fluxdensity=[7.25, 0, 0, 0])
    atmosphere_inputs(vis)
    before = snapshot(vis)
    base = dict(vis=str(vis), xaxis='frequency', yaxis='real', antenna='A0&A1',
                spw='0:0;3;6', correlation='XX', showgui=False, overwrite=True, showatm=True)
    dual = base | dict(yaxis=['real', 'imag'], ydatacolumn=['model', 'corrected'],
                       yaxislocation=['left', 'right'])
    crashed = root / 'virtual-ordinate-overlay.casa.txt'
    try:
        reference(**dual, plotfile=str(crashed), expformat='txt')
    except RpcError as error:
        assert error.code() == StatusCode.UNAVAILABLE
        assert 'Socket closed' in error.details()
        assert not crashed.exists()
    else:
        raise AssertionError('Pinned CASA unexpectedly exported the combined overlay')
    print('virtual-ordinate-overlay: pinned CASA loses the server process', flush=True)

    # CASA can export the two independent ordinates without the overlay, and can
    # export the same atmosphere with a single visibility axis on the same grid.
    values = root / 'virtual-ordinates.casa.txt'
    curve = root / 'single-virtual-overlay.casa.txt'
    assert reference(**(dual | dict(showatm=False)), plotfile=str(values), expformat='txt') is None
    assert reference(**(base | dict(ydatacolumn='model')), plotfile=str(curve), expformat='txt') is None
    expected_headers, expected = read_sections(values)
    curve_headers, (model, atmosphere) = read_sections(curve)
    assert model == expected[0]
    assert [section[0] for section in expected] == ['# Frequency Real', '# Frequency Imag']
    assert [len(rows) for _, rows in expected] == [12, 12]
    assert atmosphere[0] == '# Frequency Atm Transmission' and len(atmosphere[1]) == 12
    assert all(float(row[1]) == 7.25 for row in model[1])

    result = root / 'virtual-ordinate-overlay.native.txt'
    assert native(**dual, plotfile=str(result), expformat='txt') is None
    headers, actual = read_sections(result)
    assert headers[:len(expected_headers)] == expected_headers
    assert headers[-2:] == curve_headers[-2:]
    count, residual = compare_sections(actual, expected + [atmosphere])
    assert count == 36 and residual <= 2e-4
    print(f'virtual-ordinate-overlay: {count} matching coordinates from CASA components, '
          f'overlay NRMS {residual:.6g}', flush=True)

    cases = (
        ('combined-spw', base | dict(spw='0:0~3,1:0~3', avgspw=True, ydatacolumn='corrected'),
         [('# Frequency Real', 16), ('# Frequency Atm Transmission', 16)]),
        ('both-curves', base | dict(showtsky=True),
         [('# Frequency Real', 12), ('# Frequency Atm Transmission', 12)]),
        ('image-only', base | dict(showatm=False, showimage=True), [('# Frequency Real', 12)]),
    )
    for name, options, sections in cases:
        actual, _, residual = paired_text(root, native, name, options)
        assert residual <= 2e-4
        assert [(label, len(rows)) for label, rows in actual] == sections
    verify_source(vis, before)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--native-bin', type=Path, required=True)
    parser.add_argument('--calibrate-bin', type=Path, required=True)
    parser.add_argument('--app-run', type=Path)
    parser.add_argument('--only', choices=('combined',))
    args = parser.parse_args()
    root = args.work.resolve()
    root.mkdir()
    os.environ['CASARS_MSEXPLORE_BIN'] = str(args.native_bin.resolve())
    os.environ['CASARS_CALIBRATE_BIN'] = str(args.calibrate_bin.resolve())
    from casars.tasks import plotms as native

    assert casa.version_string() == '6.7.6.14'
    if args.only == 'combined':
        with chdir(root), plotms_launcher(args.app_run):
            combined_atmosphere(root, native)
        return
    ms, cal, wide = (root / name for name in ('source.ms', 'amplitude.cal', 'wide.ms'))
    fixture(ms, root / 'gain.cal')
    cal_options = dict(vis=str(ms), caltable=str(cal), caltype='amp', parameter=[2.0])
    casa.gencal(**cal_options)
    bandpass = root / 'bandpass.cal'
    casa.bandpass(str(ms), str(bandpass), refant='A0', solint='inf', minsnr=0.0)
    atmosphere_inputs(ms)
    copy_ms(ms, wide)
    with opened(wide / 'SPECTRAL_WINDOW', True) as table:
        table.putcell('CHAN_FREQ', 0, np.linspace(179e9, 186e9, 8))
        table.putcell('CHAN_WIDTH', 0, np.full(8, 1e9))
        table.putcell('REF_FREQUENCY', 0, 182.5e9)
    with opened(wide / 'ASDM_RECEIVER', True) as table:
        table.putcell('freqLO', 0, np.array([182e9]))
    base = dict(vis=str(ms), yaxis='amp', spw='0', antenna='A0&A1', correlation='XX',
                showgui=False, overwrite=True)
    freq = dict(xaxis='frequency', showatm=True)
    sky = dict(xaxis='frequency', showtsky=True)
    scans = dict(iteraxis='scan', gridrows=2)
    color = dict(coloraxis='scan', colorizeoverlay=True)
    cases = dict(
        default=dict(showatm=True), channel=dict(xaxis='channel', showatm=True),
        frequency=freq, image=freq | dict(showimage=True), sky=sky,
        gaps=freq | dict(spw='0:2;4;5'), scan=freq | scans, avg=freq | dict(avgchannel='2'),
        average_gaps=freq | dict(spw='0:0~2;5~7', avgchannel='2'),
        all=freq | dict(antenna='', correlation=''), both_correlations=freq | dict(correlation=''),
        scan_color=freq | color, time_average=freq | dict(avgtime='30'),
        wide=sky | dict(vis=str(wide), showimage=True),
        wide_gaps=sky | dict(vis=str(wide), spw='0:0;3;6'),
        callibrary=freq | dict(ydatacolumn='corrected',
                               callib=f"caltable='{cal}' calwt=False tinterp='nearest' finterp='linear'"),
    )
    with chdir(root), plotms_launcher(args.app_run):
        original = snapshot(ms)
        results = {name: paired_text(root, native, name, base | options)
                   for name, options in cases.items()}
        images(root, native, 'frequency', base | freq, results['frequency'][0])
        images(root, native, 'scan-colors', base | freq | color, results['scan_color'][0], colored=True)
        corr_colors = dict(correlation='', coloraxis='corr', colorizeoverlay=True)
        images(root, native, 'correlation-colors', base | freq | corr_colors,
               results['both_correlations'][0], colored=True)
        images(root, native, 'scan-pages', base | freq | dict(iteraxis='scan', exprange='all'),
               results['scan'][0], pages=True)
        cal_request = dict(caltable=str(bandpass), yaxis='amp', antenna='A0', spw='0',
                           showgui=False, overwrite=True) | freq | color
        results['calibration_color'] = paired_text(root, native, 'calibration-color', cal_request)
        images(root, native, 'calibration-colors', cal_request,
               results['calibration_color'][0], colored=True)
        verify_source(ms, original)
        combined = root / "combined"
        combined.mkdir()
        combined_atmosphere(combined, native)
    print(f'{len(results)} paired exports and {sum(item[1] for item in results.values())} points matched. '
          f'Maximum overlay NRMS {max(item[2] for item in results.values()):.6g}.', flush=True)


if __name__ == '__main__':
    main()
