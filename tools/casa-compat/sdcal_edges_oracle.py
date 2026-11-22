#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Paired CASA results for selected total-power raster and OTF edges."""

import argparse
import json
from pathlib import Path
import subprocess
import sys

import casatasks as casa
import numpy as np

from applycal_oracle import copy_ms
from calibration_smoothcal_oracle import opened
from sdcal_apply_oracle import attach_mapped_tsys, compare_products
from sdcal_position_switch_oracle import make_position_switch


def reference_sdcal(vis, mode, **options):
    if not mode.startswith('otfraster'):
        return casa.sdcal(vis, mode, **options)
    # The pinned raster solver can reuse an uninitialized boundary slot
    # between calls. A fresh process also isolates reference crashes.
    script = ('import json,sys; from casatasks import sdcal; '
              'assert sdcal(sys.argv[1], sys.argv[2], **json.loads(sys.argv[3])) is None')
    subprocess.run([sys.executable, '-c', script, vis, mode, json.dumps(options)],
                   check=True, timeout=40)


def regular_map(root):
    vis, _, _ = make_position_switch(root)
    with opened(vis / 'STATE') as state:
        on = next(i for i, mode in enumerate(state.getcol('OBS_MODE'))
                  if mode == 'OBSERVE_TARGET#ON_SOURCE')
    with opened(vis, True) as main:
        time = main.getcol('TIME')
        time[0] = time[1] - 1.
        main.putcol('TIME', time)
        main.putcol('TIME_CENTROID', time)
        main.putcol('STATE_ID', np.full(time.size, on, dtype=np.int32))
        main.putcol('SCAN_NUMBER', np.ones(time.size, dtype=np.int32))
        exposure = 1. + .25 * (np.arange(time.size) % 3)
        main.putcol('EXPOSURE', exposure)
        flags = main.getcol('FLAG')
        flags[..., :] = False
        flags[0, 1, np.arange(time.size) % 11 == 0] = True
        main.putcol('FLAG', flags)
    return vis


def raster_map(source, output):
    copy_ms(source, output)
    with opened(output / 'POINTING', True) as pointings:
        time = pointings.getcol('TIME')
        order = np.argsort(time, kind='stable')
        strips = np.empty(len(time), dtype=np.int32)
        strips[order] = np.arange(len(time)) // 11
        pointings.putcol('TIME', time + strips * 20.)
    with opened(output, True) as main:
        samples = main.getcol('TIME')
        nearest = np.abs(samples[:, None] - time).argmin(axis=1)
        samples += strips[nearest] * 20.
        main.putcol('TIME', samples)
        main.putcol('TIME_CENTROID', samples)
    return output


def check_raster(vis, product, *, count=None, fraction=None, allow_omitted_tail=False):
    with opened(vis) as main:
        times = main.getcol('TIME')
        exposures = main.getcol('EXPOSURE')
        flags = main.getcol('FLAG')
        data = main.getcol('DATA').real
    split = np.flatnonzero(np.diff(times) > 5. * np.median(np.diff(times))) + 1
    rows = []
    for line in np.split(np.arange(times.size), split):
        if count is not None:
            nedge = count
        elif fraction:
            nedge = max(1, int(fraction * line.size))
        else:
            nedge = max(1, int(np.sqrt(line.size + 1) - 1))
        rows.extend([line[:nedge], line[-nedge:]])
    with opened(product) as table:
        assert table.getkeyword('VisCal') == 'SDSKY_RASTER'
        if allow_omitted_tail and table.nrows() == len(rows) - 2:
            rows = rows[:-2]
        np.testing.assert_allclose(table.getcol('TIME'), [times[row].mean() for row in rows],
                                   rtol=0., atol=1e-6)
        np.testing.assert_allclose(table.getcol('INTERVAL'), [exposures[row].sum() for row in rows],
                                   rtol=0., atol=1e-6)
        for index, row in enumerate(rows):
            weights = np.where(flags[..., row], 0., exposures[row])
            present = weights.sum(axis=-1) != 0
            expected = np.divide((data[..., row] * weights).sum(axis=-1), weights.sum(axis=-1),
                                 out=np.zeros(present.shape), where=present)
            np.testing.assert_allclose(table.getcell('FPARAM', index), expected, atol=8e-7)
            np.testing.assert_array_equal(table.getcell('FLAG', index), ~present)
    print(f'Raster rows match exposure, flags, and physical spectra: {len(rows)} solutions', flush=True)


def compare_tables(reference, result):
    with opened(reference) as casa_table, opened(result) as native_table:
        assert casa_table.getkeyword('VisCal') == native_table.getkeyword('VisCal')
        for column in ('TIME', 'INTERVAL', 'SCAN_NUMBER', 'FIELD_ID', 'OBSERVATION_ID',
                       'ANTENNA1', 'ANTENNA2', 'SPECTRAL_WINDOW_ID', 'FLAG', 'PARAMERR', 'SNR'):
            np.testing.assert_array_equal(casa_table.getcol(column), native_table.getcol(column),
                                          err_msg=column)
        for column in ('FPARAM', 'WEIGHT'):
            np.testing.assert_allclose(casa_table.getcol(column), native_table.getcol(column),
                                       rtol=2e-6, atol=8e-7, err_msg=column)


def compare_implementation(root, engines, vis, mode, label, options, raster_count=None):
    products = {}
    for engine, task in engines.items():
        product = root / f'{mode}-{label}-{engine}.cal'
        assert task(str(vis), mode, outfile=str(product), **options) is None
        if mode == 'otfraster':
            check_raster(vis, product, count=options.get('noff'), fraction=raster_count,
                         allow_omitted_tail=engine == 'casa')
        products[engine] = product
    if 'native' in products and label != 'automatic':
        compare_tables(products['casa'], products['native'])
    return products


def compare_application(root, engines, vis, mode, products, options):
    expected = None
    for writer, product in products.items():
        for reader, task in engines.items():
            work = root / f'{mode}-direct-{writer}-{reader}.ms'
            copy_ms(vis, work)
            assert task(str(work), 'apply', applytable=str(product), interp='linear') is None
            with opened(work) as output:
                got = {column: output.getcol(column) for column in ('CORRECTED_DATA', 'FLAG', 'WEIGHT')}
            if expected is None:
                expected = got
            else:
                compare_products(got, expected)
    for reader, task in engines.items():
        work = root / f'{mode}-composite-{reader}.ms'
        copy_ms(vis, work)
        assert task(str(work), mode + ',apply', interp='linear', **options) is None
        with opened(work) as output:
            got = {column: output.getcol(column) for column in expected}
        compare_products(got, expected)
    with_tsys = root / f'{mode}-temperature.ms'
    copy_ms(vis, with_tsys)
    attach_mapped_tsys(root, with_tsys)
    reference = None
    for reader, task in engines.items():
        work = root / f'{mode}-composite-tsys-{reader}.ms'
        copy_ms(with_tsys, work)
        assert task(str(work), mode + ',tsys,apply', spwmap={1: [0]}, interp='linear', **options) is None
        with opened(work) as output:
            got = {column: output.getcol(column) for column in expected}
        if reference is None:
            reference = got
        else:
            np.testing.assert_allclose(got['CORRECTED_DATA'], reference['CORRECTED_DATA'],
                                       rtol=4e-6, atol=2e-4)
            np.testing.assert_array_equal(got['FLAG'], reference['FLAG'])
            np.testing.assert_allclose(got['WEIGHT'], reference['WEIGHT'], rtol=5e-6)
    print(f'{mode} tables interchange and both composite modes match', flush=True)


def check_ephemeris(root, engines, source):
    """CASA's unittest/sdcal/otf_ephem.ms fixture tracks Mars."""
    products = {}
    for engine, task in engines.items():
        vis = root / f'mars-{engine}.ms'
        copy_ms(source, vis)
        table = root / f'mars-{engine}.cal'
        assert task(str(vis), 'otf', outfile=str(table)) is None
        products[engine] = table
        with opened(table) as calibration:
            assert calibration.getcol('FPARAM').shape == (2, 1, 10)
            np.testing.assert_array_equal(calibration.getcol('FPARAM'), 1.)
        assert task(str(vis), 'otf,apply') is None
        with opened(vis) as output:
            corrected = output.getcol('CORRECTED_DATA')
            signal = np.ones(corrected.shape, dtype=corrected.dtype)
            assert signal.shape[-1] == 40
            signal[..., 3::4] = 0.
            np.testing.assert_array_equal(corrected, signal)
    if 'native' in products:
        compare_tables(products['casa'], products['native'])
    print('Mars map edges and corrected spectra match the upstream CASA case', flush=True)


def check_interpolated_float_map(root, engines, source):
    vis = root / 'float-offset.ms'
    copy_ms(source, vis)
    with opened(vis, True) as main:
        description = main.getcoldesc('DATA')
        description.update(name='FLOAT_DATA', valueType='float', dataManagerType='StandardStMan',
                           dataManagerGroup='FloatMap')
        data = main.getcol('DATA').real
        main.addcols({'FLOAT_DATA': description})
        main.putcol('FLOAT_DATA', data)
        main.putcolkeyword('FLOAT_DATA', 'QuantumUnits', ['K'])
        main.removecols('DATA')
        times = main.getcol('TIME') + .23
        main.putcol('TIME', times)
        main.putcol('TIME_CENTROID', times)
    products = compare_implementation(root, engines, vis, 'otf', 'float', {'fraction': '.25'})
    expected = None
    for reader, task in engines.items():
        work = root / f'otf-float-{reader}.ms'
        copy_ms(vis, work)
        assert task(str(work), 'apply', applytable=str(products['casa'])) is None
        with opened(work) as output:
            result = {name: output.getcol(name) for name in ('CORRECTED_DATA', 'FLAG', 'WEIGHT')}
        if expected is None:
            expected = result
        else:
            compare_products(result, expected)
    print('FLOAT_DATA edges agree between pointing knots', flush=True)


def check_selected_map(root, engines, source):
    vis = root / 'selected-map.ms'
    copy_ms(source, vis)
    with opened(vis, True) as main:
        scan = main.getcol('SCAN_NUMBER')
        scan[scan.size // 2:] = 2
        main.putcol('SCAN_NUMBER', scan)
    products = compare_implementation(
        root, engines, vis, 'otf', 'selected',
        {'fraction': '15%', 'field': '0', 'scan': '2', 'spw': '0:1~2'},
    )
    for product in products.values():
        with opened(product) as table:
            assert table.nrows() > 0 and table.getcol('FPARAM').shape[1] == 3
            np.testing.assert_array_equal(table.getcol('SCAN_NUMBER'), 2)
    print('Selected field, channel interval and second scan match CASA', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=Path)
    parser.add_argument('--reference-only', action='store_true')
    parser.add_argument('--ephemeris', type=Path, help='staged CASA otf_ephem.ms fixture')
    args = parser.parse_args()
    root = args.work.resolve()
    root.mkdir()
    assert casa.version_string() == '6.7.6.14'
    engines = {'casa': reference_sdcal}
    if not args.reference_only:
        from casars.tasks import sdcal
        engines['native'] = sdcal
    mapping = regular_map(root)
    raster = raster_map(mapping, root / 'raster.ms')
    for label, options in (('explicit', {'noff': 2}), ('fraction', {'fraction': '32%'}),
                           ('automatic', {'fraction': 0.})):
        products = compare_implementation(root, engines, raster, 'otfraster', label, options,
                                          raster_count=.32 if label == 'fraction' else None)
        if label == 'explicit':
            compare_application(root, engines, raster, 'otfraster', products, options)
    options = {'fraction': '15%'}
    sky = compare_implementation(root, engines, mapping, 'otf', 'default', options)
    custom = compare_implementation(root, engines, mapping, 'otf', 'options',
                                    dict(options, width=.33, elongated=True))
    for engine in engines:
        compare_tables(sky[engine], custom[engine])
    compare_application(root, engines, mapping, 'otf', sky, options)
    check_interpolated_float_map(root, engines, mapping)
    check_selected_map(root, engines, mapping)
    if args.ephemeris:
        check_ephemeris(root, engines, args.ephemeris.resolve())


if __name__ == '__main__':
    main()
