# SPDX-License-Identifier: LGPL-3.0-or-later
"""Pair public PlotMS phase shifts with the pinned CASA visibility iterator."""

from unittest.mock import patch

from grpc import RpcError, StatusCode
import numpy as np
from casatools import measures, mstransformer, quanta
from PIL import Image

from plotms_ms_columns_oracle import (
    casa, compare_figure, compare_text, copy_ms, fixture, opened, reference, request, rows, write_model,
)
from plotms_ms_reporting_frames_oracle import reporting_frequencies
from record_casa_returns import make_cal_ms


GALACTIC_RMS = 1.2e-5
MODEL_RMS = 1.2e-6
MARKERS = dict(customsymbol=True, symbolshape='circle', symbolcolor='dd2255',
               symbolsize=5, symbolfill='fill', symboloutline=False)
PNG = dict(expformat='png', width=500, height=360, dpi=100)


def marker_mask(path):
    with Image.open(path) as image:
        assert image.size == (500, 360)
        return np.all(np.asarray(image.convert('RGB')) == [0xdd, 0x22, 0x55], axis=-1)


def compare_phasecenter_compositions(root, native, phasecenter):
    root.mkdir()
    vis, original = fixture(root)
    with opened(vis, True) as table:
        row = np.arange(table.nrows())
        uvw = np.array([-30 - row / 16, 42 + row / 8, 4 + row / 32])
        table.putcol('UVW', uvw)
    base = request(vis) | dict(xaxis='real', yaxis='imag', spw='0:1~3^2')
    galactic = galactic_direction(phasecenter)
    frame_coordinates = []
    shifted_centers = {}
    for name, target in (('j2000', phasecenter), ('galactic', galactic)):
        shifted = materialize_shift(root / name, vis, target)
        shifted_centers[name] = shifted
        reference_text = root / f'{name}.casa.txt'
        assert reference(**(base | dict(vis=str(shifted))), plotfile=str(reference_text), expformat='txt') is None
        actual = root / f'{name}.native.txt'
        assert native(**base, phasecenter=target, transform=True, plotfile=str(actual), expformat='txt') is None
        if name == 'galactic':
            # The independent frame conversions differ by about 3e-11 radians.
            compare_rotated_text(actual, reference_text, GALACTIC_RMS)
        else:
            assert compare_text(actual, reference_text) == 12
        frame_coordinates.append(np.asarray([p[:2] for p in rows(reference_text)], dtype=float))
    # CASA's frame round trip changes the direction by a small numerical amount.
    # The paired cases use the same strings, and the physical check uses normalized RMS.
    j2000_xy, galactic_xy = frame_coordinates
    assert np.linalg.norm(j2000_xy - galactic_xy) / np.linalg.norm(j2000_xy) < 0.001
    options = base | dict(phasecenter=galactic, transform=True, title='Galactic phase center')
    compare_figure(root, native, options, reference_text, ('Galactic phase center', 'Real', 'Imag:corrected'),
                   coordinate_rms=GALACTIC_RMS)
    snapshots = original | {'UVW': uvw}
    compare_polynomial_shift(root, native, vis, phasecenter, snapshots)
    compare_reductions(root, native, vis, shifted_centers['j2000'], phasecenter)
    compare_polynomial_time_chunks(root, native, phasecenter)
    compare_scalar_noop(root, native, vis, shifted_centers['j2000'], phasecenter)
    compare_scan_iteration(root, native, vis, phasecenter, snapshots)
    compare_reporting_frame(root, native, vis, phasecenter)
    compare_virtual_model(root, native, vis, phasecenter, snapshots)
    compare_combined_refusal(root, native, vis, phasecenter)
    unchanged_columns(vis, snapshots)


def unchanged_columns(vis, snapshots):
    with opened(vis) as table:
        for name, values in snapshots.items():
            np.testing.assert_array_equal(table.getcol(name), values)


def galactic_direction(phasecenter):
    me = measures()
    try:
        target = me.measure(me.direction(*phasecenter.split()), 'GALACTIC')
        return 'GALACTIC ' + ' '.join(f"{target[key]['value']:.17g}{target[key]['unit']}" for key in ('m0', 'm1'))
    finally:
        me.done()


def compare_rotated_text(actual, expected, tolerance):
    headers = lambda path: [line for line in path.read_text().splitlines() if line.startswith('#')]
    assert headers(actual) == headers(expected)
    native_rows, casa_rows = rows(actual), rows(expected)
    assert len(native_rows) == len(casa_rows) == 12
    assert [p[2:] for p in native_rows] == [p[2:] for p in casa_rows]
    native_xy, casa_xy = (np.asarray([p[:2] for p in points], dtype=float) for points in (native_rows, casa_rows))
    # Compare each component in units of its RMS over the selected samples.
    # Roundoff in the complex rotation can be large relative to an isolated near-zero value.
    error = np.linalg.norm(native_xy - casa_xy, axis=0) / np.linalg.norm(casa_xy, axis=0)
    assert np.all(error < tolerance), error
    print(f'{expected.name}: 12 points with relative RMS {error}', flush=True)


def materialize_shift(folder, vis, phasecenter):
    folder.mkdir()
    output = folder / vis.name
    assert casa.phaseshift(vis=str(vis), outputvis=str(output), phasecenter=phasecenter,
                           datacolumn='all') is None
    return output


def materialize_iterator(folder, vis, phasecenter, **options):
    folder.mkdir()
    output = folder / vis.name
    configuration = dict(inputms=str(vis), outputms=str(output), datacolumn='ALL', reindex=False,
                         tviphaseshift=True, tviphaseshiftlib={'phasecenter': phasecenter})
    tool = mstransformer()
    try:
        tool.config(configuration | options)
        tool.open()
        tool.run()
    finally:
        tool.done()
    return output


def write_polynomial_field(vis):
    with opened(vis / 'FIELD', True) as table:
        coefficients = {}
        table.putcell('NUM_POLY', 0, 1)
        for name in ('PHASE_DIR', 'DELAY_DIR', 'REFERENCE_DIR'):
            constant = table.getcell(name, 0)
            assert constant.shape == (2, 1)
            coefficients[name] = np.concatenate((constant, [[3e-8], [-5e-8]]), axis=1)
            table.putcell(name, 0, coefficients[name])
    return coefficients


def unchanged_polynomial_field(vis, coefficients):
    with opened(vis / 'FIELD') as table:
        assert table.getcell('NUM_POLY', 0) == 1
        for name, values in coefficients.items():
            np.testing.assert_array_equal(table.getcell(name, 0), values)


def compare_polynomial_shift(root, native, vis, phasecenter, snapshots):
    work = root / 'polynomial'
    work.mkdir()
    source = work / vis.name
    copy_ms(vis, source)
    with opened(source, True) as table:
        times = table.getcol('TIME')
        scans = np.where(times < np.unique(times)[3], 1, 2).astype(np.int32)
        table.putcol('SCAN_NUMBER', scans)
    coefficients = write_polynomial_field(source)
    # The wrapper's final FIELD update cannot read the whole mixed-shape column.
    shifted = materialize_iterator(work / 'shifted', source, phasecenter)
    base = request(source) | dict(xaxis='real', yaxis='imag', spw='0:1~3^2')
    expected, actual = (work / f'polynomial.{engine}.txt' for engine in ('casa', 'native'))
    assert reference(**(base | dict(vis=str(shifted))), plotfile=str(expected), expformat='txt') is None
    options = base | MARKERS | dict(phasecenter=phasecenter, transform=True,
                                   title='Polynomial FIELD', plotrange=[-5, 5, -15, 15])
    assert native(**options, plotfile=str(actual), expformat='txt') is None
    assert compare_text(actual, expected) == 12
    assert [point[3] for point in rows(expected)] == ['1'] * 6 + ['2'] * 6
    compare_figure(work, native, options, expected, ('Polynomial FIELD', 'Real', 'Imag:corrected'))
    compare_control_markers(work, 'polynomial', options, base | dict(vis=str(shifted)))
    unchanged_columns(source, snapshots | {'SCAN_NUMBER': scans})
    unchanged_polynomial_field(source, coefficients)


def compare_reductions(root, native, vis, shifted, phasecenter):
    base = request(vis) | dict(xaxis='real', yaxis='imag', ydatacolumn='data', spw='0:0~3')
    time = dict(averagedata=True, avgtime='20')
    channel = dict(averagedata=True, avgchannel='2')
    # MSTransformManager places PhaseShiftingTVI above the time averager
    # and below the channel averager. Keep the source cell shape when
    # materializing the time iterator, then select the four plotted channels.
    time_shifted = materialize_iterator(
        root / 'time-shifted', vis, phasecenter, datacolumn='DATA', spw='0',
        field=base['field'], antenna=base['antenna'], timeaverage=True,
        timebin='20', timespan='state',
    )
    for name, reduction, control, count in (
        ('channel', channel, base | channel | dict(vis=str(shifted)), 12),
        ('time', time, base | dict(vis=str(time_shifted)), 9),
    ):
        options = base | reduction | MARKERS | dict(
            transform=True, phasecenter=phasecenter, title=name, plotrange=[-5, 5, -5, 5],
        )
        actual, expected = (root / f'{name}.{engine}.txt' for engine in ('native', 'casa'))
        assert reference(**control, expformat='txt', plotfile=str(expected)) is None
        if name == 'time':
            # The materialized MS already contains averaged samples. Take
            # the request's averaging header from a public unshifted export.
            headers = time_average_headers(root, base | time, expected)
            expected.write_text(''.join(headers) + ''.join(' '.join(point) + '\n' for point in rows(expected)))
        assert native(**options, expformat='txt', plotfile=str(actual)) is None
        assert compare_text(actual, expected) == count
        expected_channels = ['0', '1'] if name == 'channel' else ['0', '1', '3']
        assert [p[2] for p in rows(expected)] == expected_channels * (count // len(expected_channels))
        compare_figure(root, native, options, expected, (name, 'Real', 'Imag'))
        compare_control_markers(root, name, options, control)


def time_average_headers(root, options, materialized):
    unshifted = root / 'time-unshifted.casa.txt'
    assert reference(**options, expformat='txt', plotfile=str(unshifted)) is None
    headers = lambda path: [line for line in path.read_text().splitlines(keepends=True) if line.startswith('#')]
    result = headers(unshifted)
    assert headers(materialized) == [
        line for line in result if not line.startswith(('# channel average:', '# time average:'))
    ]
    return result


def compare_polynomial_time_chunks(root, native, phasecenter):
    work = root / 'time-chunks'
    work.mkdir()
    source = work / 'columns.ms'
    make_cal_ms(str(source), channels=4, stokes='XX')
    casa.clearcal(str(source), addmodel=True)
    with opened(source, True) as table:
        _, channel, row = np.indices(table.getcol('DATA').shape)
        table.putcol('DATA', (1 + channel + row / 256).astype(np.complex64))
        table.putcol('FLAG', channel == 2)
        table.putcol('FLAG_ROW', np.zeros(table.nrows(), dtype=bool))
        row = np.arange(table.nrows())
        table.putcol('UVW', np.array([-30 - row / 16, 42 + row / 8, 4 + row / 32]))
        sigma = (1 + row % 7 / 8).astype(np.float32)[None, :]
        table.putcol('SIGMA', sigma)
        table.putcol('WEIGHT', 1.0 / sigma**2)
        times = table.getcol('TIME')
        field = table.getcol('FIELD_ID') == 0
        epochs, integrations = np.unique(times[field], return_inverse=True)
        start = epochs[0]
        times[field] = start + np.array([0, 10, 25, 40, 43, 58])[integrations]
        table.putcol('TIME', times)
        table.putcol('TIME_CENTROID', times)
        missing = field & (times == start + 25) & (table.getcol('ANTENNA1') == 0) & (table.getcol('ANTENNA2') == 2)
        assert np.count_nonzero(missing) == 1
    # Keep a gap on A0&A2 at the start of the middle chunk. The unaligned
    # path would also include the late 58 s integration in the middle average.
    vis = work / 'selected' / source.name
    vis.parent.mkdir()
    with opened(source) as table:
        selection = table.selectrows(np.flatnonzero(~missing).tolist())
        try:
            selection.copy(str(vis), deep=True, valuecopy=True).done()
        finally:
            selection.done()
    coefficients = write_polynomial_field(vis)
    with opened(vis) as table:
        snapshots = {name: table.getcol(name) for name in ('DATA', 'UVW', 'TIME', 'FLAG', 'FLAG_ROW', 'SIGMA', 'WEIGHT')}
    base = request(vis) | dict(xaxis='real', yaxis='imag', ydatacolumn='data', spw='0', antenna='A0&A1;A0&A2')
    shifted = materialize_iterator(
        work / 'shifted', vis, phasecenter, datacolumn='DATA', spw='0', field=base['field'],
        antenna=base['antenna'], timeaverage=True, timebin='20', timespan='state',
    )
    control = base | dict(vis=str(shifted))
    time = base | dict(averagedata=True, avgtime='20')
    options = time | MARKERS | dict(transform=True, phasecenter=phasecenter,
                                   title='Polynomial time chunks', plotrange=[-5, 5, -5, 5])
    expected, actual = (work / f'time-chunks.{engine}.txt' for engine in ('casa', 'native'))
    assert reference(**control, expformat='txt', plotfile=str(expected)) is None
    points = rows(expected)
    assert len(points) == 18
    assert [p[2] for p in points] == ['0', '1', '3'] * 6
    assert [p[6] for p in points[::3]] == ['1', '2'] * 3
    np.testing.assert_allclose([float(p[9]) - start for p in points[::3]],
                               [5, 5, 34, 41.5, 58, 58], rtol=0, atol=.00051)
    # MSCache records vb->time()(0) for each buffer. The materialized table
    # stores each baseline's own centroid, including the later A0&A2 epoch.
    for offset in (0, 6, 12):
        for point in points[offset:offset + 6]:
            point[9] = points[offset][9]
    headers = time_average_headers(work, time, expected)
    expected.write_text(''.join(headers) + ''.join(' '.join(point) + '\n' for point in points))
    assert native(**options, expformat='txt', plotfile=str(actual)) is None
    assert compare_text(actual, expected) == 18
    compare_figure(work, native, options, expected, ('Polynomial time chunks', 'Real', 'Imag'))
    compare_control_markers(work, 'time-chunks', options, control)
    unchanged_columns(vis, snapshots)
    unchanged_polynomial_field(vis, coefficients)


def compare_scalar_noop(root, native, vis, shifted, phasecenter):
    base = request(vis) | dict(xaxis='real', yaxis='imag', ydatacolumn='data', spw='0:0~3')
    for name, reduction, count in (('scalar-raw', {}, 18), ('scalar-channel', dict(avgchannel='2'), 12)):
        controls = base | reduction | dict(vis=str(shifted), averagedata=True, scalar=True)
        options = controls | MARKERS | dict(vis=str(vis), transform=True, phasecenter=phasecenter,
                                            title=name, plotrange=[-5, 5, -5, 5])
        expected, actual = (root / f'{name}.{engine}.txt' for engine in ('casa', 'native'))
        assert reference(**controls, expformat='txt', plotfile=str(expected)) is None
        assert native(**options, expformat='txt', plotfile=str(actual)) is None
        assert compare_text(actual, expected) == count
        compare_figure(root, native, options, expected, (name, 'Real', 'Imag'))
        compare_control_markers(root, name, options, controls | dict(scalar=False))


def compare_control_markers(root, name, options, control):
    output = root / f'{name}.control.png'
    paint = MARKERS | dict(title=options['title'], plotrange=options['plotrange'])
    assert reference(**(control | paint | PNG), plotfile=str(output)) is None
    masks = [marker_mask(path) for path in (root / 'casa' / f'{name}.png', output)]
    assert np.any(masks[0]) and np.array_equal(*masks), name


def compare_scan_iteration(root, native, vis, phasecenter, snapshots):
    from casars import _plot_document

    work = root / 'scan'
    work.mkdir()
    source = work / vis.name
    copy_ms(vis, source)
    with opened(source, True) as table:
        times = table.getcol('TIME')
        scans = np.where(times < np.unique(times)[3], 1, 2).astype(np.int32)
        table.putcol('SCAN_NUMBER', scans)
    shifted = materialize_shift(work / 'shifted', source, phasecenter)
    base = request(source) | dict(xaxis='real', yaxis='imag', spw='0:1~3^2',
                                  iteraxis='scan', gridrows=1, gridcols=2, exprange='all')
    control = base | dict(vis=str(shifted))
    options = base | dict(transform=True, phasecenter=phasecenter)
    actual, expected = (work / f'scan.{engine}.txt' for engine in ('native', 'casa'))
    assert reference(**control, expformat='txt', plotfile=str(expected)) is None
    assert native(**options, expformat='txt', plotfile=str(actual)) is None
    assert compare_text(actual, expected) == 12
    points = rows(expected)
    assert [p[3] for p in points] == ['1'] * 6 + ['2'] * 6
    render = _plot_document.render_document
    panels = []

    def record(*args, **kwargs):
        figure, axes = render(*args, **kwargs)
        panels.append([(axis.get_subplotspec().colspan.start, axis.lines[0].get_xydata()) for axis in axes])
        return figure, axes

    for engine, task in (('casa', reference), ('native', native)):
        output = work / engine
        output.mkdir()
        if engine == 'native':
            with patch.object(_plot_document, 'render_document', record):
                assert task(**options, **PNG, plotfile=str(output / 'scan.png')) is None
        else:
            assert task(**options, **PNG, plotfile=str(output / 'scan.png')) is None
        [image] = output.glob('*.png')
        assert image.name == 'scan_Scan1,2.png'
        with Image.open(image) as raster:
            assert raster.size == (500, 360)
    [page] = panels
    assert [column for column, _ in page] == [0, 1]
    for index, (_, xy) in enumerate(page):
        expected_xy = np.array([p[:2] for p in points[index * 6:(index + 1) * 6]], dtype=float)
        np.testing.assert_allclose(xy, expected_xy, rtol=1.2e-5, atol=1e-7)
    unchanged_columns(source, snapshots | {'SCAN_NUMBER': scans})
    print('phasecenter scan: 12 matching points in two ordered panels', flush=True)


def compare_reporting_frame(root, native, vis, phasecenter):
    # Leave FIELD at the input direction so pinned PlotMS converts frequency
    # using the same reference as the active phase-shifting request.
    shifted = materialize_iterator(root / 'reporting-shifted', vis, phasecenter)
    base = request(vis) | dict(xaxis='real', yaxis='imag', spw='0:1~3^2')
    control = base | dict(vis=str(shifted), transform=True, freqframe='LSRK')
    options = base | MARKERS | dict(transform=True, freqframe='LSRK', phasecenter=phasecenter,
                                   title='LSRK shift', plotrange=[-5, 5, -15, 15])
    actual, expected = (root / f'lsrk.{engine}.txt' for engine in ('native', 'casa'))
    assert reference(**(control | dict(transform=False)), expformat='txt', plotfile=str(expected)) is None
    assert native(**options, expformat='txt', plotfile=str(actual)) is None
    measured, original = rows(actual), rows(expected)
    assert len(measured) == len(original) == 12
    assert [p[2:10] + p[11:] for p in measured] == [p[2:10] + p[11:] for p in original]
    np.testing.assert_allclose(np.array([p[:2] for p in measured], dtype=float),
                               np.array([p[:2] for p in original], dtype=float), rtol=1.2e-5, atol=1e-7)
    me, qa = measures(), quanta()
    try:
        with opened(vis / 'OBSERVATION') as table:
            assert me.doframe(me.observatory(table.getcell('TELESCOPE_NAME', 0)))
        with opened(vis / 'FIELD') as table:
            longitude, latitude = table.getcell('PHASE_DIR', 0)[:, 0]
        assert me.doframe(me.direction('J2000', qa.quantity(longitude, 'rad'), qa.quantity(latitude, 'rad')))
        frequencies = reporting_frequencies(original, 'LSRK', me, qa)
    finally:
        me.done()
        qa.done()
    np.testing.assert_allclose([float(p[10]) for p in measured], frequencies, rtol=0, atol=1e-7)
    compare_figure(root, native, options, expected, ('LSRK shift', 'Real', 'Imag:corrected'))
    compare_control_markers(root, 'lsrk', options, control)


def compare_virtual_model(root, native, vis, phasecenter, snapshots):
    work = root / 'virtual'
    work.mkdir()
    source, physical = work / vis.name, work / 'physical' / vis.name
    physical.parent.mkdir()
    copy_ms(vis, source)
    copy_ms(vis, physical)
    with opened(source, True) as table:
        table.removecols('MODEL_DATA')
    with opened(source / 'FIELD') as table:
        direction = table.getcell('PHASE_DIR', 0)[:, 0]
    with opened(source / 'SPECTRAL_WINDOW') as table:
        frequency = table.getcell('CHAN_FREQ', 0)[0]
    image = work / 'point.im'
    write_model(image, direction, frequency, 2.0, cell=5)
    for ms, store in ((source, False), (physical, True)):
        assert casa.ft(vis=str(ms), model=str(image), field='FLUX', usescratch=store) is None
    with opened(source / 'SOURCE') as table:
        key = table.getkeyword('definedmodel_field_0')
        model_row = table.getkeyword(key)
        model = table.getcell('SOURCE_MODEL', model_row)
    shifted = materialize_shift(work / 'shifted', physical, phasecenter)
    base = request(source) | dict(xaxis='real', yaxis='imag', xdatacolumn='model',
                                  ydatacolumn='model', spw='0:1~3^2')
    options = base | MARKERS | dict(transform=True, phasecenter=phasecenter,
                                   title='Virtual MODEL shift', plotrange=[-3, 3, -3, 3])
    control = base | dict(vis=str(shifted))
    actual, expected = (work / f'model.{engine}.txt' for engine in ('native', 'casa'))
    assert reference(**control, expformat='txt', plotfile=str(expected)) is None
    assert native(**options, expformat='txt', plotfile=str(actual)) is None
    # One Float rounding step in the 2 Jy model remains visible when its
    # rotated imaginary component is close to zero.
    compare_rotated_text(actual, expected, MODEL_RMS)
    # Figure coordinates retain more digits than the six-significant-digit reference TXT.
    compare_figure(work, native, options, expected, ('Virtual MODEL shift', 'Real:model', 'Imag:model'),
                   coordinate_rms=3e-6)
    compare_control_markers(work, 'model', options, control)
    with opened(source) as table:
        assert 'MODEL_DATA' not in table.colnames()
    unchanged_columns(source, {name: values for name, values in snapshots.items() if name != 'MODEL_DATA'})
    with opened(source / 'SOURCE') as table:
        assert table.getkeyword('definedmodel_field_0') == key
        assert table.getkeyword(key) == model_row
        np.testing.assert_equal(table.getcell('SOURCE_MODEL', model_row), model)


def compare_combined_refusal(root, native, vis, phasecenter):
    from casars.tasks import TaskParameterError

    options = request(vis) | dict(xaxis='real', yaxis='imag', transform=True, phasecenter=phasecenter,
                                  averagedata=True, avgchannel='2', avgtime='20', spw='0:0~3')
    for column in ('data', 'corrected'):
        actual, expected = (root / f'combined-{column}.{engine}.png' for engine in ('native', 'casa'))
        controls = options | PNG | dict(ydatacolumn=column)
        try:
            reference(**controls, plotfile=str(expected))
        except RpcError as error:
            assert error.code() == StatusCode.INVALID_ARGUMENT and error.details() == 'plot parameters failed'
        else:
            raise AssertionError('CASA accepted combined time and channel averaging with a phase shift')
        try:
            native(**controls, plotfile=str(actual))
        except TaskParameterError as error:
            assert 'simultaneous time and channel averaging is refused by CASA' in str(error), error
        else:
            raise AssertionError('casa-rs accepted combined time and channel averaging with a phase shift')
        assert not actual.exists() and not expected.exists()
