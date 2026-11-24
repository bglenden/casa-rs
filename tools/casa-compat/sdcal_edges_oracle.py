#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Paired CASA results for selected total-power raster and OTF edges."""

import argparse
import json
from pathlib import Path
import subprocess
import sys

import casatasks as casa
from casatools import measures
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


def raster_strips(times):
    gaps = np.diff(times)
    median = np.median(gaps) if gaps.size <= 100 else np.sort(gaps)[(gaps.size - 1) // 2]
    split = np.flatnonzero(gaps > 5. * median) + 1
    return np.split(np.arange(times.size), split)


def check_raster(vis, product, *, count=None, fraction=None, allow_omitted_tail=False):
    with opened(vis) as main:
        times = main.getcol('TIME')
        exposures = main.getcol('EXPOSURE')
        flags = main.getcol('FLAG')
        data = main.getcol('DATA').real
    rows = []
    for line in raster_strips(times):
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
            if engine == 'casa':
                with opened(product) as table:
                    rows = table.nrows()
                with opened(vis) as main:
                    expected = 2 * len(raster_strips(main.getcol('TIME')))
                for attempt in range(2):
                    if rows == expected:
                        break
                    product = root / f'{mode}-{label}-{engine}-fresh-{attempt}.cal'
                    assert task(str(vis), mode, outfile=str(product), **options) is None
                    check_raster(vis, product, count=options.get('noff'), fraction=raster_count,
                                 allow_omitted_tail=True)
                    with opened(product) as table:
                        rows = table.nrows()
                assert rows == expected, 'CASA did not produce a complete deterministic reference'
        products[engine] = product
    if 'native' in products:
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
    for label, extra in (('default', {}), ('unused-intent', {'intent': 'UNUSED_INTENT'})):
        for reader, task in engines.items():
            work = root / f'{mode}-composite-tsys-{label}-{reader}.ms'
            copy_ms(with_tsys, work)
            assert task(str(work), mode + ',tsys,apply', spwmap={1: [0]}, interp='linear',
                        **options, **extra) is None
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
    mixed = root / 'mixed-epoch-map.ms'
    copy_ms(vis, mixed)
    with opened(mixed / 'ANTENNA', True) as antennas:
        assert antennas.nrows() == 1
        antennas.addrows(1)
        for column in antennas.colnames():
            if antennas.iscelldefined(column, 0):
                antennas.putcell(column, 1, antennas.getcell(column, 0))
        antennas.putcell('NAME', 1, 'UNSELECTED')
    with opened(mixed / 'POINTING', True) as pointing:
        first = pointing.getcell('TIME', 0)
        tool = measures()
        converted = tool.measure(tool.epoch('UTC', f'{first}s'), 'TAI')['m0']
        assert converted['unit'] == 'd'
        offset = round(converted['value'] * 86400. - first)
        assert offset > 30
        pointing.putcol('TIME', pointing.getcol('TIME') + offset)
        info = pointing.getcolkeyword('TIME', 'MEASINFO')
        info['Ref'] = 'TAI'
        pointing.putcolkeyword('TIME', 'MEASINFO', info)
        count = pointing.nrows()
        pointing.addrows(3)
        for index in range(3):
            for column in pointing.colnames():
                if pointing.iscelldefined(column, index):
                    pointing.putcell(column, count + index, pointing.getcell(column, index))
            pointing.putcell('ANTENNA_ID', count + index, 1)
    compare_implementation(root, engines, mixed, 'otf', 'mixed-epochs', {'fraction': '.25'})
    print('UTC MAIN and TAI POINTING match with an unselected short pointing series', flush=True)


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


def check_map_intent(root, engines, mapping, raster):
    for mode, source, options in (('otf', mapping, {'fraction': '15%'}),
                                  ('otfraster', raster, {'noff': 2})):
        vis = root / f'{mode}-intent.ms'
        copy_ms(source, vis)
        intent = 'CALIBRATE_FOCUS#ON_SOURCE'
        with opened(vis / 'STATE', True) as state:
            tagged = state.nrows()
            state.addrows(1)
            state.putcell('OBS_MODE', tagged, f'OBSERVE_TARGET#ON_SOURCE,{intent}')
            state.putcell('SIG', tagged, True)
            state.putcell('REF', tagged, False)
        with opened(vis, True) as main:
            scan = main.getcol('SCAN_NUMBER')
            states = main.getcol('STATE_ID')
            # Split after two complete raster lines of 11 integrations each.
            selected = np.argsort(main.getcol('TIME'), kind='stable')[22:]
            scan[selected] = 2
            states[selected] = tagged
            assert main.putcol('SCAN_NUMBER', scan) and main.putcol('STATE_ID', states)

        intent_products = {}
        for engine, task in engines.items():
            by_scan = root / f'{mode}-intent-{engine}-scan.cal'
            by_intent = root / f'{mode}-intent-{engine}.cal'
            assert task(str(vis), mode, outfile=str(by_scan), scan='2', **options) is None
            assert task(str(vis), mode, outfile=str(by_intent), intent=intent, **options) is None
            with opened(by_intent) as product:
                assert product.nrows() > 0
                np.testing.assert_array_equal(product.getcol('SCAN_NUMBER'), 2)
            compare_tables(by_scan, by_intent)
            intent_products[engine] = by_intent
        if 'native' in intent_products:
            compare_tables(intent_products['casa'], intent_products['native'])
        print(f'{mode} intent selects the expected scan in both implementations', flush=True)

        expected = None
        for engine, task in engines.items():
            for method in ('direct', 'composite'):
                work = root / f'{mode}-intent-{method}-{engine}.ms'
                copy_ms(vis, work)
                if method == 'direct':
                    assert task(str(work), 'apply', applytable=str(intent_products[engine]), interp='linear') is None
                else:
                    assert task(str(work), mode + ',apply', intent=intent, interp='linear', **options) is None
                with opened(work) as output:
                    got = {column: output.getcol(column) for column in ('CORRECTED_DATA', 'FLAG', 'WEIGHT')}
                if expected is None:
                    expected = got
                else:
                    compare_products(got, expected)
        print(f'{mode} composite intent agrees with explicit sky selection', flush=True)


def check_shared_pointings(root, engines, source):
    vis = root / 'shared-pointing-map.ms'
    copy_ms(source, vis)
    with opened(vis) as main:
        times = main.getcol('TIME')
    with opened(vis / 'POINTING', True) as pointing:
        assert pointing.nrows() == times.size
        pointing.putcol('TIME', times)
        pointing.putcol('ANTENNA_ID', np.full(times.size, -1, dtype=np.int32))
    products = compare_implementation(root, engines, vis, 'otf', 'shared', {'fraction': '15%'})
    with opened(products['casa']) as table:
        assert table.nrows() > 0
    print('Array-wide POINTING samples match CASA', flush=True)


def check_fractional_pixel_edge(root, engines, source):
    vis = root / 'fractional-pixel.ms'
    copy_ms(source, vis)
    with opened(vis, True) as main:
        times = main.getcol('TIME')[:10]
        scan = main.getcol('SCAN_NUMBER')
        scan[:10] = 2
        main.putcol('SCAN_NUMBER', scan)
    offsets = np.array([(8, 3), (4, 2), (4, 1), (8, 5), (4, 0),
                        (0, 1), (6, 5), (6, 2), (8, 2), (6, 1)])
    with opened(vis / 'POINTING', True) as pointing:
        for row, coords in enumerate(np.array([1., .5]) + 1e-4 * offsets):
            pointing.putcell('TIME', row, times[row])
            pointing.putcell('DIRECTION', row, coords[:, None])
    products = compare_implementation(root, engines, vis, 'otf', 'pixel',
                                      {'fraction': '.1', 'scan': '2'})
    for product in products.values():
        with opened(product) as table:
            expected = [times[:4].mean(), times[5:].mean()]
            np.testing.assert_allclose(table.getcol('TIME'), expected, rtol=0., atol=1e-6)
    print('Negative fractional pixels do not create a reference sample', flush=True)


def check_large_raster_median(root, engines, source):
    vis = root / 'median-lower.ms'
    with opened(source) as main:
        rows = main.selectrows(list(range(103)))
        rows.copy(str(vis), deep=True, valuecopy=True).close()
        rows.close()
    with opened(vis, True) as main:
        t0 = main.getcell('TIME', 0)
        gaps = np.asarray([1.] * 51 + [6.] + [2.] * 50)
        times = t0 + np.concatenate(([0.], np.cumsum(gaps)))
        main.putcol('TIME', times)
        main.putcol('TIME_CENTROID', times)
    products = compare_implementation(root, engines, vis, 'otfraster', 'median', {'noff': 1})
    for product in products.values():
        with opened(product) as table:
            np.testing.assert_array_equal(table.getcol('TIME'), times[[0, 51, 52, 102]])
    print('Long raster lists use the lower midpoint for gap detection', flush=True)


def check_lunar_tracking(root, engines, source):
    vis = root / 'lunar-map.ms'
    copy_ms(source, vis)
    with opened(vis / 'ANTENNA') as antennas:
        position = antennas.getcell('POSITION', 0)
    tool = measures()
    assert tool.doframe(tool.position('ITRF', *(f'{axis}m' for axis in position)))
    with opened(vis, True) as main:
        t0 = main.getcell('TIME', 0)
        times = t0 + 40. * np.arange(main.nrows())
        main.putcol('TIME', times)
        main.putcol('TIME_CENTROID', times)
    with opened(vis / 'SOURCE', True) as sources:
        sources.putcol('NAME', np.full(sources.nrows(), 'Moon'))
    with opened(vis / 'POINTING', True) as pointing:
        offsets = pointing.getcol('DIRECTION')
        offsets -= offsets.mean(axis=-1, keepdims=True)
        for row, time in enumerate(times):
            assert tool.doframe(tool.epoch('UTC', f'{time}s'))
            apparent = tool.measure(tool.direction('MOON'), 'AZEL')
            celestial = tool.measure(apparent, 'J2000')
            target = np.asarray([celestial['m0']['value'], celestial['m1']['value']])
            pointing.putcell('DIRECTION', row, offsets[..., row] + target[:, None])
        pointing.putcol('TIME', times)
    products = compare_implementation(root, engines, vis, 'otf', 'lunar', {'fraction': '15%'})
    with opened(products['casa']) as table:
        assert table.nrows() > 0
    print('Topocentric Moon map matches the site-dependent CASA track', flush=True)


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
    check_map_intent(root, engines, mapping, raster)
    check_shared_pointings(root, engines, mapping)
    check_fractional_pixel_edge(root, engines, mapping)
    check_large_raster_median(root, engines, mapping)
    check_lunar_tracking(root, engines, mapping)
    if args.ephemeris:
        check_ephemeris(root, engines, args.ephemeris.resolve())


if __name__ == '__main__':
    main()
