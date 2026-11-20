#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare public imager SDGrid and GridBoth scratch predictions."""

import argparse
import json
import math
from pathlib import Path
import shutil
import subprocess
import sys

import casatasks
from casatools import image, imager, measures, table
import numpy as np
from scipy.special import j1

from gaincal_oracle import relative_rms
from imager_custom_mosaic_oracle import copy_input
from imager_mosaic_oracle import fixture


def model_image(path, *, wide=False):
    tool = image()
    assert tool.open(str(path))
    if wide:
        coords = tool.coordsys()
        assert coords.setincrement(value='-1.2arcsec 1.2arcsec', type='direction')
        assert tool.setcoordsys(coords.torecord()) and coords.done()
    x, y = np.mgrid[:320, :320]
    surface = (.01 + np.exp(-((x - 160.) / 65.)**2 - ((y - 160.) / 62.)**2) / 1200)
    assert tool.putchunk(surface.astype(np.float32)[:, :, None, None]) and tool.done()


def prepare_ms(source, target, mode, *, scratch=True):
    copy_input(source, target, scratch)
    tool = table()
    assert tool.open(str(target), nomodify=False)
    ant1 = tool.getcol('ANTENNA1')
    if mode == 'sd':
        assert tool.putcol('ANTENNA2', ant1)
        assert tool.putcol('UVW', np.zeros_like(tool.getcol('UVW')))
    assert tool.done()


def setup(factory, target, mode, kernel, *, pinned, scratch=True, spw=''):
    tool = factory()
    assert tool.open(str(target), usescratch=scratch) and tool.selectvis(field='0,1', spw=spw)
    cell = '1.2arcsec' if kernel == 'PB' else '.3arcsec'
    assert tool.defineimage(nx=320, ny=320, cellx=cell, celly=cell,
                            phasecenter=0, outframe='TOPO')
    if pinned:
        dimensions = {
            'GAUSS': {'truncate': f'{3. * math.sqrt(math.log(2.))}pixel'},
            'GJINC': {'truncate': '-1pixel'},
        }
        assert tool.setsdoptions(convertfirst='NEVER', **dimensions.get(kernel, {}))
    assert tool.setoptions(ftmachine=mode, gridfunction=kernel, numthreads=1)
    return tool


def predict(factory, target, model, mode, kernel, *, pinned, scratch=True, spw=''):
    tool = setup(factory, target, mode, kernel, pinned=pinned, scratch=scratch, spw=spw)
    assert tool.ft(str(model))
    (target.parent / (target.name + '.prediction-complete')).touch()
    assert tool.done()


def read_virtual(path, mode, kernel):
    from casars.tools import ms

    table_tool = table()
    assert table_tool.open(str(path / 'SOURCE'))
    keyword = table_tool.getkeyword('definedmodel_field_0')
    model = table_tool.getcell('SOURCE_MODEL', table_tool.getkeyword(keyword))
    entries = [entry['container'] for key, entry in model.items() if key.startswith('ft_')]
    assert sorted(entry['name'] for entry in entries) == (
        ['SDGrid'] if mode == 'sd' else ['GridFT', 'SDGrid'])
    for entry in entries:
        if entry['name'] == 'SDGrid':
            assert entry['casars_sd_kernel'] == kernel
        stored = image()
        assert stored.open(entry['diskimage'])
        assert np.iscomplexobj(stored.getchunk()) and stored.done()
    assert table_tool.done()
    assert table_tool.open(str(path))
    assert 'MODEL_DATA' not in table_tool.colnames()
    corrected = table_tool.getcol('CORRECTED_DATA')
    assert table_tool.done()
    tool = ms()
    assert tool.open(str(path), nomodify=False) and tool.uvsub() and tool.done()
    assert table_tool.open(str(path))
    delta = corrected - table_tool.getcol('CORRECTED_DATA')
    assert table_tool.done()
    return delta


def correlations(path):
    tool = table()
    assert tool.open(str(path))
    flags = tool.getcol('FLAG')
    auto = tool.getcol('ANTENNA1') == tool.getcol('ANTENNA2')
    model = tool.getcol('MODEL_DATA')
    assert tool.done()
    return auto, model, flags


def analytic_gaussian(source, model, kernel, shape):
    """Evaluate CASA's documented kernel at the fixture's two pointings."""
    tool = table()
    assert tool.open(str(source / 'POINTING'))
    times = tool.getcol('TIME')
    entries = tool.getcol('DIRECTION')[:, 0, :]
    assert tool.done()
    assert tool.open(str(source))
    visibility_times = tool.getcol('TIME')
    assert tool.done()
    sky = image()
    assert sky.open(str(model))
    pixels = sky.getchunk()[:, :, 0, 0]
    coordinates = sky.coordsys()
    reference = coordinates.referencevalue()['numeric']
    result = np.zeros(shape, dtype=np.complex64)
    for time in np.unique(visibility_times):
        longitude, latitude = entries[:, np.flatnonzero(times == time)[0]]
        pixel = coordinates.topixel([longitude, latitude, *reference[2:]])['numeric'][:2]
        position = np.asarray(pixel + 1., dtype=np.float32)
        center = np.rint(position).astype(int)
        offset = np.rint((center - position) * 100.).astype(int)
        sampling = 100
        width = math.sqrt(math.log(2.))
        if kernel == 'GAUSS':
            support, end = 3, int(3. * width * sampling + .5)
        else:
            support, end = 4, int(3.8317059702 * 1.55 / math.pi * sampling)
            width *= 2.52
        value = 0.
        for dy in range(-support, support + 1):
            for dx in range(-support, support + 1):
                radius = int(math.hypot(dx * sampling + offset[0], dy * sampling + offset[1]))
                if radius > end:
                    continue
                weight = math.exp(-math.log(2.) * (radius / sampling / width)**2)
                if kernel == 'GJINC' and radius:
                    argument = math.pi * radius / sampling / 1.55
                    weight *= 2. * j1(argument) / argument
                value += weight * pixels[center[0] + dx - 1, center[1] + dy - 1]
        mask = visibility_times == time
        result[0, :, mask] = value
        result[-1, :, mask] = value
    assert coordinates.done() and sky.done()
    return result


def run(root):
    from casars.tools import imager as native

    assert casatasks.version_string() == '6.7.6.14'
    root.mkdir(parents=True)
    phase_center, offset, model = fixture(root)
    model_image(model)
    primary_beam_model = root / 'wide.im'
    tool = image()
    assert tool.open(str(model))
    copy = tool.subimage(str(primary_beam_model))
    assert copy.done() and tool.done()
    model_image(primary_beam_model, wide=True)
    results = {}
    for mode, kernel, source in (
        ('sd', 'SF', offset), ('sd', 'BOX', phase_center),
        ('sd', 'GAUSS', offset), ('sd', 'GJINC', offset),
        ('sd', 'PB', offset),
        ('both', 'SF', offset), ('both', 'BOX', phase_center),
    ):
        case_model = primary_beam_model if kernel == 'PB' else model
        expected_path = root / f'{mode}-{kernel}-casa.ms'
        actual_path = root / f'{mode}-{kernel}-native.ms'
        prepare_ms(source, expected_path, mode)
        prepare_ms(source, actual_path, mode)
        reference = subprocess.run([sys.executable, __file__, '--casa', str(expected_path),
                                    str(case_model), mode, kernel], check=False)
        assert reference.returncode in (0, -11), reference.returncode
        assert (root / (expected_path.name + '.prediction-complete')).exists()
        predict(native, actual_path, case_model, mode, kernel, pinned=False)
        auto, expected, flags = correlations(expected_path)
        native_auto, actual, native_flags = correlations(actual_path)
        assert np.array_equal(auto, native_auto) and np.array_equal(flags, native_flags)
        use_analytic = kernel in ('GAUSS', 'GJINC')
        if use_analytic:
            # SDGrid's copy constructor leaves its Gaussian widths and
            # cutoff uninitialized. Use the explicit source kernel instead.
            expected = analytic_gaussian(source, case_model, kernel, expected.shape)
        virtual_path = root / f'{mode}-{kernel}-virtual.ms'
        prepare_ms(source, virtual_path, mode, scratch=False)
        predict(native, virtual_path, case_model, mode, kernel, pinned=False, scratch=False)
        virtual = read_virtual(virtual_path, mode, kernel)
        for row_type, mask in [('autocorrelations', auto), ('cross-correlations', ~auto)]:
            if not mask.any():
                continue
            pinned_values = expected[..., mask]
            values = actual[..., mask]
            assert np.abs(pinned_values).max() > 1.e-3, (mode, kernel, row_type)
            error = relative_rms(values, pinned_values)
            virtual_error = relative_rms(virtual[..., mask], pinned_values)
            results[f'{mode}-{kernel}-{row_type}'] = {'normalized_rms': error,
                                                       'virtual_normalized_rms': virtual_error,
                                                       'complex_samples': values.size,
                                                       'reference': 'analytic' if use_analytic else 'CASA'}
            print(mode, kernel, row_type, error, virtual_error, flush=True)
            assert error < 1.e-3, (mode, kernel, row_type, error)
            assert virtual_error < 1.e-3, (mode, kernel, row_type, virtual_error)
    (root / 'results.json').write_text(json.dumps(results, indent=2) + '\n')


def compare_scratch(root, source, model, label, kernel='SF', spw='', reference_source=None):
    from casars.tools import imager as native

    reference = root / f'{label}-casa.ms'
    actual = root / f'{label}-native.ms'
    prepare_ms(reference_source or source, reference, 'sd')
    prepare_ms(source, actual, 'sd')
    process = subprocess.run([sys.executable, __file__, '--casa', str(reference),
                              str(model), 'sd', kernel, '--spw', spw], check=False)
    assert process.returncode in (0, -11) and (root / f'{reference.name}.prediction-complete').exists(), process.returncode
    predict(native, actual, model, 'sd', kernel, pinned=False, spw=spw)
    _, expected, _ = correlations(reference)
    _, output, _ = correlations(actual)
    error = relative_rms(output, expected)
    print(label, error, 'max', float(np.abs(expected).max()), flush=True)
    assert np.abs(expected).max() > 1.e-3 and error < 1.e-3
    return expected, output


def invalid_replacement(root, source, model, wide_model):
    from casars.tools import imager as native

    target = root / 'replacement.ms'
    prepare_ms(source, target, 'sd', scratch=False)
    predict(native, target, wide_model, 'sd', 'PB', pinned=False, scratch=False)
    storage = target / 'SOURCE'
    grids = sorted(path.name for path in storage.glob('FT_MODEL*'))
    assert grids
    tool = setup(native, target, 'sd', 'PB', pinned=False, scratch=False)
    assert not tool.ft(str(model)) and tool.done()
    assert grids == sorted(path.name for path in storage.glob('FT_MODEL*'))
    assert np.abs(read_virtual(target, 'sd', 'PB')).max() > 1.

    table_tool = table()
    assert table_tool.open(str(target))
    first_antenna = table_tool.getcell('ANTENNA1', 0)
    assert table_tool.done()
    assert table_tool.open(str(target / 'POINTING'), nomodify=False)
    lost = np.flatnonzero(table_tool.getcol('ANTENNA_ID') == first_antenna).tolist()
    assert lost and len(lost) < table_tool.nrows()
    assert table_tool.removerows(lost) and table_tool.done()
    tool = setup(native, target, 'sd', 'PB', pinned=False, scratch=False)
    assert not tool.ft(str(wide_model)) and tool.done()
    assert grids == sorted(path.name for path in storage.glob('FT_MODEL*'))
    print('invalid PB replacements preserved', grids, flush=True)


def lsrk_cube(root, source, model):
    from casars.tools import imager as native

    target = root / 'lsrk.ms'
    prepare_ms(source, target, 'sd', scratch=False)
    tb = table()
    assert tb.open(str(target / 'SPECTRAL_WINDOW'))
    frequencies = tb.getcell('CHAN_FREQ', 0)
    assert tb.done()
    assert tb.open(str(target / 'FIELD'))
    centers = [tb.getcell('PHASE_DIR', field)[:, 0] for field in (0, 1)]
    assert tb.done()
    assert tb.open(str(target))
    fields, epochs = (tb.getcol(column) for column in ('FIELD_ID', 'TIME'))
    assert tb.done()
    m = measures()
    assert m.doframe(m.observatory('ALMA'))
    converted = np.empty((frequencies.size, fields.size))
    for field, epoch in sorted(set(zip(fields, epochs, strict=True))):
        longitude, latitude = centers[field]
        assert m.doframe(m.direction('J2000', f'{longitude}rad', f'{latitude}rad'))
        assert m.doframe(m.epoch('UTC', f'{epoch:.9f}s'))
        mask = (fields == field) & (epochs == epoch)
        for channel, frequency in enumerate(frequencies):
            value = m.measure(m.frequency('TOPO', f'{frequency:.9f}Hz'), 'LSRK')['m0']['value']
            converted[channel, mask] = value
    assert m.done()
    first, width = float(converted.min() - 3.3e5), 7.0e5
    planes = 1 + int(np.rint((converted.max() - first) / width))
    assert planes >= 2
    expected = 1. + np.rint((converted - first) / width)
    assert len(np.unique(expected)) >= 2
    raw_index = np.rint((frequencies[:, None] - first) / width)
    raw = np.broadcast_to(np.where((0 <= raw_index) & (raw_index < planes), 1. + raw_index, 0.), expected.shape)
    assert relative_rms(raw, expected) > .5
    ia = image()
    assert ia.open(str(model))
    coords = ia.coordsys()
    assert ia.done()
    assert coords.setreferencecode('LSRK', type='spectral', adjust=False)
    assert coords.setreferencepixel(value=[0.], type='spectral')
    assert coords.setreferencevalue(value=f'{first}Hz', type='spectral')
    assert coords.setincrement(value=f'{width}Hz', type='spectral')
    cube = root / 'lsrk.im'
    pixels = np.broadcast_to(np.arange(1, planes + 1, dtype=np.float32), (320, 320, 1, planes)).copy()
    assert ia.fromarray(str(cube), pixels, csys=coords.torecord())
    assert coords.done() and ia.done()
    predict(native, target, cube, 'sd', 'BOX', pinned=False, scratch=False)
    raw_output = read_virtual(target, 'sd', 'BOX')
    assert relative_rms(raw_output[0], raw) < 1.e-3
    assert relative_rms(raw_output[3], raw) < 1.e-3

    assert tb.open(str(target / 'SOURCE'), nomodify=False)
    keyword = tb.getkeyword('definedmodel_field_0')
    row = tb.getkeyword(keyword)
    recipe = tb.getcell('SOURCE_MODEL', row)
    recipe['ft_0']['container']['freqframevalid'] = True
    assert tb.putcell('SOURCE_MODEL', row, recipe) and tb.done()
    output = read_virtual(target, 'sd', 'BOX')
    assert relative_rms(output[0], expected) < 1.e-3
    assert relative_rms(output[3], expected) < 1.e-3
    assert not output[1:3].any()
    print('LSRK conversion matched pinned measures on', expected.size, 'channels and rows', flush=True)


def shifted_field_control(source, destination):
    shutil.copytree(source, destination)
    tb = table()
    assert tb.open(str(destination / 'FIELD'), nomodify=False)
    m = measures()
    east, north = -4.e-5, -1.5e-5
    for field in range(tb.nrows()):
        for name in ('PHASE_DIR', 'DELAY_DIR', 'REFERENCE_DIR'):
            values = tb.getcell(name, field)
            longitude, latitude = values[:, 0]
            original = m.direction('J2000', f'{longitude}rad', f'{latitude}rad')
            shifted = m.shift(original, f'{math.hypot(east, north)}rad',
                              f'{math.atan2(east, north)}rad')
            values[:, 0] = [shifted[key]['value'] for key in ('m0', 'm1')]
            assert tb.putcell(name, field, values)
    assert m.done() and tb.done()


def recipe_interchange(root, source, model):
    from casars.tools import imager as native, ms

    for mode in ('sd', 'both'):
        pinned = root / f'{mode}-pinned-virtual.ms'
        target = root / f'{mode}-native-virtual.ms'
        for path in (pinned, target):
            prepare_ms(source, path, mode, scratch=False)
        process = subprocess.run([sys.executable, __file__, '--casa', str(pinned),
                                  str(model), mode, 'SF', '--virtual'], check=False)
        assert process.returncode in (0, -11)
        assert (root / f'{pinned.name}.prediction-complete').exists()
        predict(native, target, model, mode, 'SF', pinned=False, scratch=False)
        for path in (pinned, target):
            try:
                casatasks.uvsub(str(path))
            except RuntimeError as exception:
                assert 'Unsupported FTMachine' in str(exception)
            else:
                raise AssertionError(f'pinned uvsub accepted {path.name}')
        tool = ms()
        assert tool.open(str(pinned), nomodify=False)
        try:
            tool.uvsub()
        except RuntimeError as exception:
            assert 'no single-dish convolution settings' in str(exception)
        else:
            raise AssertionError(f'native uvsub accepted incomplete {pinned.name}')
        assert tool.done()
        assert np.abs(read_virtual(target, mode, 'SF')).max() > 1.e-3
        print(mode, 'virtual writer and reader matrix', flush=True)


def review_cases(root):
    from bandpass_oracle import source_data

    root.mkdir(parents=True)
    phase_center, offset, model = fixture(root)
    model_image(model)
    gradient = root / 'gradient.im'
    shutil.copytree(model, gradient)
    ia = image()
    assert ia.open(str(gradient))
    x, y = np.mgrid[:320, :320]
    brightness = (1.e-3 + x * 1.e-4 + y * 3.e-5).astype(np.float32)
    assert ia.putchunk(brightness[:, :, None, None]) and ia.done()

    scanning = root / 'fast-scan.ms'
    shutil.copytree(offset, scanning)
    tb = table()
    assert tb.open(str(scanning), nomodify=False)
    times = tb.getcol('TIME')
    assert tb.putcol('TIME', times + 2.5)
    assert tb.putcol('TIME_CENTROID', times + 2.5)
    assert tb.putcol('INTERVAL', np.ones_like(times)) and tb.done()
    assert tb.open(str(scanning / 'POINTING'), nomodify=False)
    times = tb.getcol('TIME')
    pointings = tb.getcol('DIRECTION')
    pointings[0, 0, :] += 3.e-5 * np.sin((times - times.min()) / 14.)
    assert tb.putcol('DIRECTION', pointings) and tb.done()
    compare_scratch(root, scanning, gradient, 'fast-scan', 'BOX')

    fallback = root / 'feeds.ms'
    shutil.copytree(phase_center, fallback)
    assert tb.open(str(fallback / 'ANTENNA'), nomodify=False)
    assert tb.putcol('MOUNT', ['EQUATORIAL'] * tb.nrows()) and tb.done()
    assert tb.open(str(fallback / 'FEED'), nomodify=False)
    offsets = tb.getcol('BEAM_OFFSET')
    offsets[0, 0, :] = 4.e-5
    offsets[1, 0, :] = -1.5e-5
    assert tb.putcol('BEAM_OFFSET', offsets) and tb.done()
    shifted = root / 'shifted-field.ms'
    shifted_field_control(phase_center, shifted)
    compare_scratch(root, fallback, gradient, 'feed-fallback', 'BOX', reference_source=shifted)

    expected, actual = compare_scratch(root, offset, model, 'disjoint', spw='0:0~2;6~8')
    assert np.abs(expected[0, :3]).max() > 1.e-3
    assert np.array_equal(actual[:, 3:6], expected[:, 3:6])

    stokes = root / 'stokes.ms'
    source_data(stokes, stokes='I')
    assert tb.open(str(stokes / 'POLARIZATION'))
    assert np.array_equal(tb.getcell('CORR_TYPE', 0), [1]) and tb.done()
    expected, _ = compare_scratch(root, stokes, model, 'stokes-I', 'BOX')
    assert expected.shape[0] == 1

    wide = root / 'wide.im'
    shutil.copytree(model, wide)
    model_image(wide, wide=True)
    invalid_replacement(root, offset, model, wide)
    lsrk_cube(root, phase_center, model)
    recipe_interchange(root, offset, model)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path)
    parser.add_argument('--casa', nargs=4, metavar=('MS', 'IMAGE', 'MODE', 'KERNEL'))
    parser.add_argument('--review-cases', action='store_true')
    parser.add_argument('--spw', default='')
    parser.add_argument('--virtual', action='store_true')
    args = parser.parse_args()
    if args.casa:
        ms, model, mode, kernel = args.casa
        predict(imager, Path(ms), Path(model), mode, kernel, pinned=True,
                scratch=not args.virtual, spw=args.spw)
    elif args.review_cases:
        review_cases(args.work.resolve())
    else:
        run(args.work.resolve())


if __name__ == '__main__':
    main()
