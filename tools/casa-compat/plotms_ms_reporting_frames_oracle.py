# SPDX-License-Identifier: LGPL-3.0-or-later
"""Pair public spectral PlotMS axes with pinned CASA and its measures tool."""

from pathlib import Path
from unittest.mock import patch

import numpy as np
from casatools import measures, quanta

from plotms_ms_columns_oracle import compare_text, opened, reference, rows
from plotms_ms_layout_oracle import occupied_marker_cells
from plotms_ms_spw_average_oracle import make_fixture

LIGHT_SPEED_KM_S = 299_792.458
SPECTRAL_LABELS = {
    'frequency': 'Frequency (GHz)', 'velocity': 'Velocity (km/s)',
    'uwave': 'Uwave (λ)', 'vwave': 'Vwave (λ)', 'wwave': 'Wwave (λ)', 'uvwave': 'UVwave (λ)',
}


def compare_reporting_frames(work, native):
    work.mkdir()
    vis = make_fixture(work, integrations=4)
    with opened(vis, True) as table:
        flags = table.getcol('FLAG')
        flags[0, 1, 0] = flags[0, 0, 1] = flags[0, 2, 5] = True
        table.putcol('FLAG', flags)
    with opened(vis / 'SPECTRAL_WINDOW') as table:
        np.testing.assert_array_equal(table.getcol('MEAS_FREQ_REF'), [5, 5])
    base = dict(vis=str(vis), xaxis='frequency', yaxis='amp', ydatacolumn='data', antenna='A0&A1',
                spw='0:0~3,1:0~3', correlation='XX', showgui=False, overwrite=True,
                customsymbol=True, symbolshape='square', symbolsize=5, symbolcolor='dd2255',
                symbolfill='fill', symboloutline=False)
    raw = paired_inactive(work, native, 'inactive-frequency', base | dict(freqframe='BARY'), 29)
    time_channels = dict(averagedata=True, avgchannel='2', avgtime='20')
    averaged_raw = paired_inactive(work, native, 'inactive-channel-time', base | time_channels, 8)
    me, qa = measures(), quanta()
    assert me.doframe(me.observatory('VLA'))
    assert me.doframe(me.direction('J2000', '12h00m00s', '40d00m00s'))
    try:
        freqs = {frame: reporting_frequencies(raw, frame, me, qa) for frame in ('TOPO', 'LSRK', 'BARY')}
        for frame, values in freqs.items():
            check_active(work, native, f'frequency-{frame.lower()}', base | dict(freqframe=frame),
                         raw, values, values)
        rest = 95.0
        for definition, values in velocities(freqs['LSRK'], rest).items():
            options = base | dict(xaxis='velocity', freqframe='LSRK', restfreq=f'{rest}GHz', veldef=definition)
            check_active(work, native, f'velocity-{definition.lower()}', options, raw, values, freqs['LSRK'])
        values = reporting_frequencies(averaged_raw, 'LSRK', me, qa)
        options = base | time_channels | dict(freqframe='LSRK')
        check_active(work, native, 'lsrk-channel-time', options, averaged_raw, values, values)
    finally:
        me.done()
        qa.done()

    inactive_velocity = base | dict(xaxis='velocity', freqframe='LSRK', restfreq='95GHz', veldef='TRUE')
    paired_inactive(work, native, 'inactive-velocity', inactive_velocity, 29)
    selected = inactive_velocity | dict(spw='0:0;1,1:1;3')
    points = paired_inactive(work, native, 'inactive-velocity-selected', selected, 14)
    for spw, position in (('0', '1'), ('1', '3')):
        middle = [float(p[0]) for p in points if (p[11], p[2]) == (spw, position)]
        assert middle and all(value == 0.0 for value in middle)


def paired_inactive(work, native, name, options, count):
    expected, actual = (work / f'{name}.{engine}.txt' for engine in ('casa', 'native'))
    kwargs = dict(transform=False, expformat='txt')
    assert reference(**options, **kwargs, plotfile=str(expected)) is None
    assert native(**options, **kwargs, plotfile=str(actual)) is None
    assert compare_text(actual, expected) == count
    print(f'{name}: {count} directly paired samples in the stored frame', flush=True)
    return rows(expected)


def compare_wavelength_reporting_frames(work, native):
    work.mkdir()
    vis = make_fixture(work)
    with opened(vis, True) as table:
        times = table.getcol('TIME')
        assert len(np.unique(times)) == 2
        later = times == max(times)
        table.putcol('UVW', np.array([3 + 2 * later, 4 + 2 * later, -12 - 4 * later], dtype=float))
        flags = table.getcol('FLAG')
        flags[0, 1, 0] = True
        table.putcol('FLAG', flags)
    base = dict(vis=str(vis), yaxis='amp', ydatacolumn='data', antenna='A0&A1', spw='0:1~3^2',
                correlation='XX', showgui=False, overwrite=True, freqframe='LSRK',
                customsymbol=True, symbolshape='square', symbolsize=5, symbolcolor='dd2255',
                symbolfill='fill', symboloutline=False)
    me, qa = measures(), quanta()
    assert me.doframe(me.observatory('VLA'))
    assert me.doframe(me.direction('J2000', '12h00m00s', '40d00m00s'))
    try:
        for wavelength, metres in (('uwave', 'u'), ('vwave', 'v'), ('wwave', 'w'), ('uvwave', 'uvdist')):
            for suffix, averaging, count in (('raw', {}, 3), ('time', dict(averagedata=True, avgtime='20'), 2)):
                name = f'{wavelength}-{suffix}'
                options = base | averaging
                check_wavelength(work, native, name, options, (wavelength, metres, count), me, qa)
        check_wavelength(work, native, 'uwave-bary', base | dict(freqframe='BARY'), ('uwave', 'u', 3), me, qa)
    finally:
        me.done()
        qa.done()


def check_wavelength(work, native, name, options, axes, me, qa):
    wavelength, metres, count = axes
    control = work / f'{name}-metres.casa.txt'
    assert reference(**options, xaxis=metres, transform=False, expformat='txt', plotfile=str(control)) is None
    points = rows(control)
    assert len(points) == count
    assert {int(point[2]) for point in points} == {1, 3}
    freqs = reporting_frequencies(points, options['freqframe'], me, qa)
    values = np.asarray([float(point[0]) for point in points]) * freqs * 1e6 / LIGHT_SPEED_KM_S
    check_active(work, native, name, options | dict(xaxis=wavelength), points, values, freqs)


def compare_averaged_reporting_epochs(work, native):
    work.mkdir()
    me, qa = measures(), quanta()
    assert me.doframe(me.observatory('VLA'))
    assert me.doframe(me.direction('J2000', '12h00m00s', '40d00m00s'))
    try:
        vis = make_fixture(work, 'irregular-frequency', integrations=4)
        set_epochs(vis, (0, 10, 20, 6000))
        base = dict(vis=str(vis), xaxis='frequency', yaxis='amp', ydatacolumn='data', antenna='A0&A1',
                    spw='0:0~3', correlation='XX', showgui=False, overwrite=True,
                    averagedata=True, avgchannel='2', avgtime='7200', freqframe='LSRK',
                    customsymbol=True, symbolshape='square', symbolsize=5, symbolcolor='dd2255',
                    symbolfill='fill', symboloutline=False)
        expected = work / 'irregular-frequency-stored.casa.txt'
        assert reference(**base, transform=False, expformat='txt', plotfile=str(expected)) is None
        points = rows(expected)
        assert len(points) == 2
        freqs = reporting_frequencies(points, 'LSRK', me, qa)
        check_active(work, native, 'irregular-frequency', base, points, freqs, freqs)
        velocity = base | dict(xaxis='velocity', restfreq='95GHz', veldef='OPTICAL')
        check_active(work, native, 'irregular-velocity', velocity, points,
                     velocities(freqs, 95.0)['OPTICAL'], freqs)

        vis = make_fixture(work, 'unequal-baseline-epochs', antenna_count=3, integrations=3)
        set_epochs(vis, (0, 10, 6000))
        with opened(vis, True) as table:
            times = table.getcol('TIME')
            start = min(times)
            late_second = (table.getcol('ANTENNA1') == 0) & (table.getcol('ANTENNA2') == 2) & (times == max(times))
            flags = table.getcol('FLAG')
            flags[:, :, late_second] = True
            table.putcol('FLAG', flags)
            uvw = np.zeros_like(table.getcol('UVW'))
            uvw[0, :] = 10
            table.putcol('UVW', uvw)
        options = base | dict(vis=str(vis), xaxis='u', spw='0:1', antenna='A0&A1;A0&A2', avgchannel='')
        expected = work / 'unequal-baseline-metres.casa.txt'
        assert reference(**options, transform=False, expformat='txt', plotfile=str(expected)) is None
        points = rows(expected)
        assert [p[6] for p in points] == ['1', '2']
        assert [float(p[9]) for p in points] == [start + 3000] * 2
        meta_frequencies = reporting_frequencies(points, 'LSRK', me, qa)
        axis_points = [p[:9] + [str(start + delta)] + p[10:] for p, delta in zip(points, (3000, 5), strict=True)]
        axis_frequencies = reporting_frequencies(axis_points, 'LSRK', me, qa)
        coordinates = axis_frequencies * 10e6 / LIGHT_SPEED_KM_S
        check_active(work, native, 'unequal-baseline-wavelengths', options | dict(xaxis='uwave'),
                     points, coordinates, meta_frequencies, artist_atol=1e-5)
    finally:
        me.done()
        qa.done()


def set_epochs(vis, offsets):
    with opened(vis, True) as table:
        times = table.getcol('TIME')
        unique = np.unique(times)
        assert len(unique) == len(offsets)
        new_times = unique[0] + np.asarray(offsets)[np.searchsorted(unique, times)]
        for column in ('TIME', 'TIME_CENTROID'):
            table.putcol(column, new_times)


def compare_channel_frequency_transform(work, native, base, raw):
    from casars import _plot_document

    vis = Path(base['vis'])
    me, qa = measures(), quanta()
    with opened(vis / 'OBSERVATION') as table:
        assert me.doframe(me.observatory(table.getcell('TELESCOPE_NAME', 0)))
    with opened(vis / 'FIELD') as table:
        longitude, latitude = table.getcell('PHASE_DIR', 0)[:, 0]
        frame = table.getcolkeywords('PHASE_DIR')['MEASINFO']['Ref']
        assert me.doframe(me.direction(frame, qa.quantity(longitude, 'rad'), qa.quantity(latitude, 'rad')))
    try:
        freqs = reporting_frequencies(raw, 'LSRK', me, qa)
    finally:
        me.done()
        qa.done()
    options = base | dict(xaxis='channel', yaxis=['frequency'], transform=True, freqframe='LSRK',
                          customsymbol=True, symbolshape='square', symbolsize=5, symbolcolor='dd2255',
                          symbolfill='fill', symboloutline=False)
    text = work / 'reporting-frame.native.txt'
    assert native(**options, plotfile=str(text)) is None
    plotted = rows(text)
    assert [p[0:1] + p[2:10] + p[11:] for p in plotted] == [p[0:1] + p[2:10] + p[11:] for p in raw]
    assert all(p[1] == p[10] for p in plotted)
    np.testing.assert_allclose([float(p[1]) for p in plotted], freqs, rtol=0, atol=1e-7)

    render = _plot_document.render_document
    captured = []

    def record(*args, **kwargs):
        figure, axis = render(*args, **kwargs)
        assert (axis.get_xlabel(), axis.get_ylabel()) == ('Channel', 'Frequency (GHz) LSRK')
        points = axis.lines[0].get_xydata()
        np.testing.assert_array_equal(points[:, 0], [int(p[0]) for p in raw])
        np.testing.assert_allclose(points[:, 1], freqs, rtol=0, atol=1e-7)
        captured.append(len(points))
        return figure, axis

    expected, actual = (work / f'reporting-frame.{engine}.png' for engine in ('casa', 'native'))
    image = options | dict(expformat='png', width=500, height=360, dpi=100)
    assert reference(**image, plotfile=str(expected)) is None
    with patch.object(_plot_document, 'render_document', record):
        assert native(**image, plotfile=str(actual)) is None
    assert captured == [14]
    for path in (expected, actual):
        assert occupied_marker_cells(path, (1, 2)) == {(0, 0), (0, 1)}
    print('reporting Channel/Frequency: 14 matching selected samples and the same LSRK grid', flush=True)


def reporting_frequencies(points, frame, me, qa):
    converted = []
    for point in points:
        assert me.doframe(me.epoch('UTC', qa.quantity(float(point[9]) / 86400, 'd')))
        source = me.frequency('TOPO', qa.quantity(float(point[10]) * 1e9, 'Hz'))
        result = me.measure(source, frame)['m0']
        assert result['unit'] == 'Hz'
        converted.append(result['value'] / 1e9)
    return np.asarray(converted)


def velocities(frequencies, rest):
    ratio = frequencies / rest
    return {
        'RADIO': LIGHT_SPEED_KM_S * (1 - ratio),
        'OPTICAL': LIGHT_SPEED_KM_S * (1 / ratio - 1),
        'TRUE': LIGHT_SPEED_KM_S * (1 - ratio ** 2) / (1 + ratio ** 2),
    }


def check_active(work, native, name, options, raw, coordinates, freqs, *, artist_atol=None):
    # The pinned transformed TXT path stalls in the export header. Pair the public
    # PNG and compare its scientific inputs with pinned measures and untransformed TXT.
    options = options | dict(transform=True, title=name)
    expected, actual = (work / f'{name}.{engine}.png' for engine in ('casa', 'native'))
    figure = dict(expformat='png', width=500, height=360, dpi=100)
    assert reference(**options, **figure, plotfile=str(expected)) is None
    text = work / f'{name}.native.txt'
    assert native(**options, expformat='txt', plotfile=str(text)) is None
    plotted = rows(text)
    assert [point[2:10] + point[11:] for point in plotted] == [point[2:10] + point[11:] for point in raw]
    assert [p[1] for p in plotted] == [p[1] for p in raw]
    np.testing.assert_allclose([float(p[10]) for p in plotted], freqs, rtol=0, atol=1e-7)
    velocity = options['xaxis'] == 'velocity'
    wavelength = options['xaxis'].endswith('wave')
    np.testing.assert_allclose([float(p[0]) for p in plotted], coordinates, rtol=0,
                               atol=0.051 if velocity else (0.01 if wavelength else 1e-7))
    check_active_figure(native, actual, options, figure, coordinates, raw, artist_atol)
    for path in (expected, actual):
        cells = occupied_marker_cells(path, (1, 2))
        # CASA can place both wavelength groups on one half with its own axis range.
        if not wavelength and len({p[11] for p in raw}) > 1:
            assert cells == {(0, 0), (0, 1)}
    print(f'{name}: {len(raw)} selected coordinates and metadata agree with CASA measures', flush=True)


def check_active_figure(native, output, options, figure, coordinates, raw, artist_atol):
    from casars import _plot_document

    render = _plot_document.render_document
    captured = []
    label = SPECTRAL_LABELS[options['xaxis']]
    velocity = options['xaxis'] == 'velocity'
    wavelength = options['xaxis'].endswith('wave')
    if not wavelength:
        label += f" {options['freqframe']}"

    def record(*args, **kwargs):
        page, axis = render(*args, **kwargs)
        assert axis.get_xlabel() == label
        assert axis.get_ylabel() == 'Amp'
        points = axis.lines[0].get_xydata()
        tolerance = artist_atol if artist_atol is not None else (1e-4 if velocity else 1e-7)
        relative = 1.2e-5 if wavelength and artist_atol is None else 0
        np.testing.assert_allclose(points[:, 0], coordinates, rtol=relative, atol=tolerance)
        np.testing.assert_allclose(points[:, 1], [float(p[1]) for p in raw], rtol=1.2e-5)
        captured.append(len(points))
        return page, axis

    with patch.object(_plot_document, 'render_document', record):
        assert native(**options, **figure, plotfile=str(output)) is None
    assert captured == [len(raw)]
