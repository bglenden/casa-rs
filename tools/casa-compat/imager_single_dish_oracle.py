#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare public imager SDGrid and GridBoth scratch predictions."""

import argparse
import json
import math
from pathlib import Path
import subprocess
import sys

import casatasks
from casatools import image, imager, table
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


def predict(factory, target, model, mode, kernel, *, pinned, scratch=True):
    tool = factory()
    assert tool.open(str(target), usescratch=scratch) and tool.selectvis(field='0,1')
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path)
    parser.add_argument('--casa', nargs=4, metavar=('MS', 'IMAGE', 'MODE', 'KERNEL'))
    args = parser.parse_args()
    if args.casa:
        ms, model, mode, kernel = args.casa
        predict(imager, Path(ms), Path(model), mode, kernel, pinned=True)
    else:
        run(args.work.resolve())


if __name__ == '__main__':
    main()
