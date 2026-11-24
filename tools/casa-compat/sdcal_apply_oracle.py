#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA comparisons for single-dish sky application and exposure weights."""

import argparse
from pathlib import Path

import casatasks as casa
from casatools import table as casa_table
import numpy as np

from applycal_oracle import copy_ms
from calibration_smoothcal_oracle import opened
from record_casa_returns import make_cal_ms
from sdcal_position_switch_oracle import make_position_switch


def make_on_off(root):
    vis, _, _ = make_position_switch(root)
    with opened(vis, True) as main:
        flags = np.zeros_like(main.getcol('FLAG'))
        main.putcol('FLAG', flags)
        exposure = main.getcol('EXPOSURE')
        exposure[-1] = 8.
        main.putcol('EXPOSURE', exposure)
        data = main.getcol('DATA')
        data[..., 2:-2] += .6
        main.putcol('DATA', data)
        main.putcol('SIGMA', np.full_like(main.getcol('SIGMA'), 2.))
        main.putcol('WEIGHT', np.full_like(main.getcol('WEIGHT'), .25))
        if 'CORRECTED_DATA' in main.colnames():
            main.removecols('CORRECTED_DATA')
    floating = root / 'float-input.ms'
    copy_ms(vis, floating)
    with opened(floating, True) as main:
        description = main.getcoldesc('DATA')
        description.update(name='FLOAT_DATA', valueType='float', dataManagerType='StandardStMan',
                           dataManagerGroup='FloatSkyInput')
        data = main.getcol('DATA')
        main.addcols({'FLOAT_DATA': description})
        main.putcol('FLOAT_DATA', data.real)
        main.putcolkeyword('FLOAT_DATA', 'QuantumUnits', ['K'])
        main.removecols('DATA')
    return {'data': vis, 'float': floating}


def inspect_outputs(ms, sky, interpolation, input_kind):
    with opened(sky) as table:
        left, right = table.getcol('TIME')
        left_exp, right_exp = table.getcol('INTERVAL')
        assert (left_exp, right_exp) == (5., 11.)
        spectra = table.getcol('FPARAM')
    with opened(ms / 'HISTORY') as history:
        assert 'sdcal' in history.getcol('ORIGIN')
    with opened(ms) as main:
        row = 73
        time = main.getcell('TIME', row)
        exposure = main.getcell('EXPOSURE', row)
        alpha = (time - left) / (right - left)
        assert .1 < alpha < .4
        if interpolation.startswith('nearest'):
            assert alpha < .5
            off = spectra[..., 0]
        else:
            off = (1. - alpha) * spectra[..., 0] + alpha * spectra[..., 1]
        if interpolation == 'nearest':
            expected_weight = .25 * left_exp / (exposure + left_exp)
        else:
            variance = (1. - alpha)**2 / left_exp + alpha**2 / right_exp
            expected_weight = .25 / (1. + exposure * variance)
        raw_name = 'FLOAT_DATA' if input_kind == 'float' else 'DATA'
        raw = main.getcell(raw_name, row)
        corrected = main.getcell('CORRECTED_DATA', row)
        assert np.max(np.abs((raw - off) / off)) > .05
        np.testing.assert_allclose(corrected, (raw - off) / off, atol=3e-7)
        np.testing.assert_allclose(main.getcell('WEIGHT', row), expected_weight, rtol=5e-6)
        if input_kind == 'float':
            assert main.getcolkeyword('CORRECTED_DATA', 'QuantumUnits')[0] == 'K'
        return {key: main.getcol(key) for key in ('CORRECTED_DATA', 'FLAG', 'WEIGHT')}


def compare_products(got, expected, *, corrected_rtol=1e-7):
    np.testing.assert_array_equal(got['FLAG'], expected['FLAG'])
    np.testing.assert_allclose(got['WEIGHT'], expected['WEIGHT'], rtol=5e-6)
    np.testing.assert_allclose(got['CORRECTED_DATA'], expected['CORRECTED_DATA'],
                               rtol=corrected_rtol, atol=8e-7)


def attach_mapped_tsys(root, vis):
    with opened(vis) as main:
        times = np.unique(main.getcol('TIME'))[[0, 73, -1]]
    target = vis / 'SYSCAL'
    with opened(vis / 'SPECTRAL_WINDOW') as spw:
        channels = spw.getcell('NUM_CHAN', 1)
    samples = np.linspace(90., 145., 3 * 2 * channels).reshape(3, -1)
    rows = root / 'mapped-tsys.txt'
    rows.write_text(''.join(' '.join(map(str, [0, 0, 1, time + .5, 1., *sample])) + '\n'
                            for time, sample in zip(times, samples)))
    tool = casa_table()
    try:
        assert tool.fromascii(
            str(target), str(rows), sep=' ',
            columnnames=['ANTENNA_ID', 'FEED_ID', 'SPECTRAL_WINDOW_ID', 'TIME', 'INTERVAL',
                         'TSYS_SPECTRUM'],
            datatypes=['I', 'I', 'I', 'D', 'D', f'R2,{channels}'],
        )
    finally:
        tool.close()
    with opened(target, True) as syscal:
        syscal.putcolkeywords('TIME', {'QuantumUnits': ['s'], 'MEASINFO': {
            'type': 'epoch', 'Ref': 'UTC'}})
        syscal.putcolkeywords('INTERVAL', {'QuantumUnits': ['s']})
        syscal.putcolkeywords('TSYS_SPECTRUM', {'QuantumUnits': ['K']})
    with opened(vis, True) as main:
        main.putkeyword('SYSCAL', f'Table: {target}')
    return samples[1].reshape(2, channels, order='F')


def check_chain(root, source, engines, gencal):
    vis = root / 'mapped-apply.ms'
    copy_ms(source, vis)
    temperature = attach_mapped_tsys(root, vis)
    with opened(vis / 'ANTENNA') as antennas:
        antenna = antennas.getcell('NAME', 0)
    factors = root / 'apply-factors.csv'
    factors.write_text(f'MS,Antenna,SPW,Pol,Factor\n{vis.name},{antenna},0,I,4\n')
    products = {}
    for engine, task in engines.items():
        sky = root / f'{engine}-data.cal'
        tsys = root / f'{engine}-mapped-tsys.cal'
        jyk = root / f'{engine}-jyk.cal'
        assert task(str(vis), 'tsys', outfile=str(tsys)) is None
        assert gencal[engine](str(vis), str(jyk), 'jyperk', str(factors)) is None
        products[engine] = (sky, tsys, jyk)
    row = 73
    expected = None
    for writer, (sky, tsys, jyk) in products.items():
        for reader, task in engines.items():
            ms = root / f'chain-{writer}-{reader}.ms'
            copy_ms(vis, ms)
            tables = [str(tsys), str(jyk), str(sky)] if writer == reader else [str(sky), str(tsys), str(jyk)]
            mapping = {1: [0]} if reader == 'casa' else [1, 1]
            assert task(str(ms), 'apply', applytable=tables, spwmap=mapping, interp='linear') is None
            with opened(ms / 'HISTORY') as history:
                messages = [message
                            for origin, message in zip(history.getcol('ORIGIN'),
                                                       history.getcol('MESSAGE'))
                            if origin == 'sdcal']
                assert len(messages) == 2
                assert messages[0] == 'taskname=sdcal' and messages[1].startswith('version: ')
            with opened(ms) as output, opened(sky) as references:
                corrected = output.getcell('CORRECTED_DATA', row)
                times = references.getcol('TIME')
                spectra = references.getcol('FPARAM')
                alpha = (output.getcell('TIME', row) - times[0]) / (times[1] - times[0])
                off = (1. - alpha) * spectra[..., 0] + alpha * spectra[..., 1]
                want = 4 * temperature * (output.getcell('DATA', row) - off) / off
                np.testing.assert_allclose(corrected, want, rtol=4e-6, atol=1e-4)
                got = {key: output.getcol(key) for key in ('CORRECTED_DATA', 'FLAG', 'WEIGHT')}
                if expected is None:
                    expected = got
                else:
                    np.testing.assert_allclose(got['CORRECTED_DATA'], expected['CORRECTED_DATA'],
                                               rtol=4e-6, atol=2e-4)
                    np.testing.assert_allclose(got['WEIGHT'], expected['WEIGHT'], rtol=5e-6)
                    np.testing.assert_array_equal(got['FLAG'], expected['FLAG'])
    _, _, jyk = products['casa']
    for reader, task in engines.items():
        ms = root / f'chain-composite-{reader}.ms'
        copy_ms(vis, ms)
        assert task(str(ms), 'ps,tsys,apply', applytable=[str(jyk)], spwmap={1: [0]},
                    interp='linear') is None
        with opened(ms) as output:
            np.testing.assert_allclose(output.getcol('CORRECTED_DATA'), expected['CORRECTED_DATA'],
                                       rtol=4e-6, atol=2e-4)
            np.testing.assert_allclose(output.getcol('WEIGHT'), expected['WEIGHT'], rtol=5e-6)
            np.testing.assert_array_equal(output.getcol('FLAG'), expected['FLAG'])
    print('Mapped Tsys and Jy/K chains agree across supplied order and composite calibration', flush=True)
    sky, tsys, jyk = products['casa']
    dual_tsys = root / 'dual-window-tsys.cal'
    copy_ms(tsys, dual_tsys)
    with opened(dual_tsys, True) as calibration:
        count = calibration.nrows()
        calibration.addrows(count)
        for index in range(count):
            for key in calibration.colnames():
                if not calibration.iscelldefined(key, index):
                    continue
                value = calibration.getcell(key, index)
                if key == 'SPECTRAL_WINDOW_ID':
                    value = 0
                elif key == 'FPARAM':
                    value = 2 * value
                calibration.putcell(key, count + index, value)
    with opened(dual_tsys / 'SPECTRAL_WINDOW', True) as windows:
        for key in windows.colnames():
            if windows.iscelldefined(key, 1):
                windows.putcell(key, 0, windows.getcell(key, 1))
    chain = [str(path) for path in (sky, dual_tsys, jyk)]
    for index, mapping in enumerate(({0: [1], 1: [0]}, {1: [0], 0: [1]})):
        reference = None
        for reader, task in engines.items():
            ms = root / f'ordered-spwmap-{index}-{reader}.ms'
            copy_ms(vis, ms)
            assert task(str(ms), 'apply', applytable=chain, spwmap=mapping, interp='linear') is None
            with opened(ms) as output:
                got = {key: output.getcol(key) for key in ('CORRECTED_DATA', 'FLAG', 'WEIGHT')}
            np.testing.assert_allclose(got['CORRECTED_DATA'][..., row],
                                       (index + 1) * expected['CORRECTED_DATA'][..., row],
                                       rtol=4e-6, atol=2e-4)
            if reference is None:
                reference = got
            else:
                np.testing.assert_allclose(got['CORRECTED_DATA'], reference['CORRECTED_DATA'],
                                           rtol=4e-6, atol=2e-4)
                np.testing.assert_array_equal(got['FLAG'], reference['FLAG'])
                np.testing.assert_allclose(got['WEIGHT'], reference['WEIGHT'], rtol=5e-6)
    print('Overlapping dictionary mappings follow CASA insertion order', flush=True)


def check_zero_reference(root, vis, engines):
    product = root / 'zero-off.cal'
    with opened(root / 'casa-data.cal') as reference:
        reference.copy(str(product), deep=True, valuecopy=True).close()
    with opened(product, True) as calibration:
        sky = calibration.getcell('FPARAM', 0)
        sky[0, 1] = 0.
        calibration.putcell('FPARAM', 0, sky)
    for engine, task in engines.items():
        ms = root / f'{engine}-zero-off.ms'
        copy_ms(vis, ms)
        assert task(str(ms), 'apply', applytable=str(product), interp='nearest') is None
        with opened(ms) as corrected:
            flux = corrected.getcell('CORRECTED_DATA', 73)
            flag = corrected.getcell('FLAG', 73)
            original = corrected.getcell('DATA', 73)
            assert np.isfinite(flux[0, 2]) and not flag[0, 2]
            if engine == 'casa':
                assert np.isinf(flux[0, 1]) and not flag[0, 1]
            else:
                assert flux[0, 1] == original[0, 1] and flag[0, 1]
    print('Zero-reference protection preserves usable channels', flush=True)


def check_flagged_reference(root, source, engines):
    product = root / 'flagged-off.cal'
    copy_ms(root / 'casa-data.cal', product)
    with opened(product, True) as sky:
        flags = np.zeros_like(sky.getcol('FLAG'))
        flags[0, 1, :] = True
        sky.putcol('FLAG', flags)
        sky.putcol('FPARAM', np.full(sky.getcol('FPARAM').shape, 2., dtype=np.float32))
    for reader, task in engines.items():
        ms = root / f'{reader}-flagged-off.ms'
        copy_ms(source, ms)
        assert task(str(ms), 'apply', applytable=str(product), interp='linear,linearflag') is None
        with opened(ms) as output:
            raw = output.getcol('DATA')
            np.testing.assert_allclose(output.getcol('CORRECTED_DATA'), (raw - 2.) / 2.,
                                       atol=8e-7)
            flags = np.zeros_like(raw, dtype=bool)
            flags[0, 1, :] = True
            np.testing.assert_array_equal(output.getcol('FLAG'), flags)
    print('A flagged finite OFF spectrum still corrects the affected samples', flush=True)


def check_errors(root, source, engines):
    for engine, task in engines.items():
        ms = root / f'{engine}-apply-errors.ms'
        copy_ms(source, ms)
        for arguments, expected in (
            ({}, 'Applytable name must be specified.'),
            ({'applytable': str(root / 'missing.cal')}, "Table doesn't exist: "),
            ({'applytable': str(root / 'casa-data.cal'), 'spwmap': 'invalid'},
             'Spwmap type must be list or dictionary.'),
        ):
            try:
                task(str(ms), 'apply', **arguments)
            except Exception as error:
                assert type(error) is Exception and str(error).startswith(expected)
            else:
                raise AssertionError(arguments)
        try:
            task(str(ms), 'apply', applytable=str(root / 'casa-data.cal'), interp='sinusoid')
        except RuntimeError as error:
            assert str(error) == 'Error in Calibrater::setapply.'
        else:
            raise AssertionError('unknown time interpolator was accepted')
        if engine == 'native':
            with opened(ms) as original:
                assert 'CORRECTED_DATA' not in original.colnames()
    print('Required tables and invalid apply maps use CASA error classes', flush=True)


def check_spectral_weights(root, source, engines):
    expected = None
    for engine, task in engines.items():
        ms = root / f'{engine}-spectral-weights.ms'
        copy_ms(source, ms)
        with opened(ms, True) as main:
            shape = main.getcol('DATA').shape
            sigma = np.broadcast_to(np.linspace(1., 3., shape[1], dtype=np.float32)[None, :, None],
                                    shape).copy()
            for column, cells in (('SIGMA_SPECTRUM', sigma),
                                  ('WEIGHT_SPECTRUM', np.full(shape, 99., dtype=np.float32))):
                if column not in main.colnames():
                    description = main.getcoldesc('DATA')
                    description.update(name=column, valueType='float',
                                       dataManagerType='StandardStMan',
                                       dataManagerGroup=column+'SkyTest')
                    main.addcols({column: description})
                main.putcol(column, cells)
        assert task(str(ms), 'apply', applytable=str(root / 'casa-data.cal'), interp='linear') is None
        with opened(ms) as output:
            np.testing.assert_array_equal(output.getcol('SIGMA_SPECTRUM'), sigma)
            weights = output.getcol('WEIGHT_SPECTRUM')
            np.testing.assert_allclose(weights[0, :, 73], [.9057027, .2264257, .1006336],
                                       rtol=5e-6)
            if expected is None:
                expected = weights
            else:
                np.testing.assert_allclose(weights, expected, rtol=5e-6)
    print('Per-channel weight spectra agree and SIGMA_SPECTRUM is unchanged', flush=True)


def check_frequency_interpolation(root, source, engines, applycal):
    product = root / 'frequency-offset.cal'
    copy_ms(root / 'casa-data.cal', product)
    with opened(product / 'SPECTRAL_WINDOW', True) as spw:
        frequencies = spw.getcell('CHAN_FREQ', 0)
        step = (frequencies[-1] - frequencies[0]) / 2.
        offset_grid = frequencies[0] + step * np.arange(-.4, 3., .6)
        spw.putcell('NUM_CHAN', 0, len(offset_grid))
        spw.putcell('CHAN_FREQ', 0, offset_grid)
        for column in ('CHAN_WIDTH', 'EFFECTIVE_BW', 'RESOLUTION'):
            values = spw.getcell(column, 0)
            spw.putcell(column, 0, np.full(len(offset_grid), .6 * values[0]))
        spw.putcell('TOTAL_BANDWIDTH', 0, abs(step) * .6 * len(offset_grid))
    with opened(product, True) as sky:
        for row in range(sky.nrows()):
            powers = 1.5 + .08 * np.arange(len(offset_grid))**2 + .1 * row
            spectra = np.stack((powers, powers + .2)).astype(np.float32)
            for column, values in (
                ('FPARAM', spectra), ('FLAG', np.zeros(spectra.shape, dtype=bool)),
                ('PARAMERR', np.zeros_like(spectra)), ('SNR', np.ones_like(spectra)),
                ('WEIGHT', np.ones_like(spectra)),
            ):
                sky.putcell(column, row, values)
    linear = None
    for frequency in ('linear', '3', 'bogus', 'cspline'):
        expected = None
        for reader, task in engines.items():
            ms = root / f'frequency-{frequency}-{reader}.ms'
            copy_ms(source, ms)
            assert task(str(ms), 'apply', applytable=str(product),
                        interp=f'linear,{frequency}') is None
            with opened(ms) as table:
                result = {key: table.getcol(key) for key in ('CORRECTED_DATA', 'FLAG', 'WEIGHT')}
            if expected is None:
                expected = result
            else:
                compare_products(result, expected)
        if frequency == 'linear':
            linear = expected
        elif frequency in ('3', 'bogus'):
            compare_products(expected, linear)
        else:
            difference = expected['CORRECTED_DATA'] - linear['CORRECTED_DATA']
            assert np.max(np.abs(difference)) > .01
    print('Off-grid linear and cubic frequencies agree, including CASA fallback', flush=True)
    with opened(product, True) as sky:
        flags = sky.getcol('FLAG')
        flags[0, 2, :] = True
        sky.putcol('FLAG', flags)
    for interpolation in ('linear', 'linear,linear'):
        label = 'default' if interpolation == 'linear' else 'explicit'
        expected = None
        for reader, task in engines.items():
            for name in ('sdcal', 'applycal'):
                ms = root / f'flag-frequency-{label}-{reader}-{name}.ms'
                copy_ms(source, ms)
                if name == 'sdcal':
                    assert task(str(ms), 'apply', applytable=str(product),
                                interp=interpolation) is None
                else:
                    assert applycal[reader](str(ms), gaintable=[str(product)], interp=interpolation,
                                            calwt=True, flagbackup=False) is None
                with opened(ms) as table:
                    result = {key: table.getcol(key) for key in ('CORRECTED_DATA', 'FLAG', 'WEIGHT')}
                assert np.all(result['FLAG'][0, 1] == (interpolation == 'linear'))
                if expected is None:
                    expected = result
                else:
                    compare_products(result, expected)
    print('Both apply tasks honor the sky frequency flag default and explicit linear override',
          flush=True)


def check_observed_columns(root, applycal):
    source = root / 'mixed-data-and-float.ms'
    make_cal_ms(str(source))
    gain = root / 'amplitude.cal'
    assert casa.gencal(str(source), str(gain), 'amp', parameter=[2.]) is None
    with opened(source, True) as ms:
        antenna1 = ms.getcol('ANTENNA1')
        antenna2 = ms.getcol('ANTENNA2')
        row = int(np.flatnonzero(antenna1 == 0)[0])
        other = int(np.flatnonzero((antenna1 != 0) & (antenna2 != 0))[0])
        antenna2[row] = 0
        ms.putcol('ANTENNA2', antenna2)
        complex_data = np.full(ms.getcol('DATA').shape, 6.+4.j, dtype=np.complex64)
        ms.putcol('DATA', complex_data)
        description = ms.getcoldesc('DATA')
        description.update(name='FLOAT_DATA', valueType='float', dataManagerType='StandardStMan',
                           dataManagerGroup='DualColumnTest')
        ms.addcols({'FLOAT_DATA': description})
        ms.putcol('FLOAT_DATA', np.full(complex_data.shape, 12., dtype=np.float32))
        if 'CORRECTED_DATA' in ms.colnames():
            ms.removecols('CORRECTED_DATA')
        ms.putcol('FLAG', np.zeros(complex_data.shape, dtype=bool))
        ms.putcol('FLAG_ROW', np.zeros(complex_data.shape[-1], dtype=bool))
    for antenna in ('A0&&&', ''):
        for engine, task in applycal.items():
            ms = root / f'{engine}-mixed-{"auto" if antenna else "all"}.ms'
            copy_ms(source, ms)
            for _ in range(2):
                assert task(str(ms), gaintable=[str(gain)], antenna=antenna,
                            calwt=False, flagbackup=False) is None
                with opened(ms) as output:
                    corrected = output.getcol('CORRECTED_DATA')
                    np.testing.assert_allclose(corrected[..., row], 3.+0.j)
                    expected = 12. if antenna else 3.
                    np.testing.assert_allclose(corrected[..., other], expected)
    print('Ordinary applycal prioritizes FLOAT_DATA consistently across selections',
          flush=True)


def run(root, engines):
    sources = make_on_off(root)
    for kind, vis in sources.items():
        products = {}
        for writer, task in engines.items():
            cal = root / f'{writer}-{kind}.cal'
            assert task(str(vis), outfile=str(cal)) is None
            products[writer] = cal
        for interpolation in ('linear', 'nearest'):
            expected = None
            for writer, cal in products.items():
                for reader, task in engines.items():
                    ms = root / f'{kind}-{interpolation}-{writer}-{reader}.ms'
                    copy_ms(vis, ms)
                    assert task(str(ms), 'apply', applytable=str(cal), interp=interpolation) is None
                    result = inspect_outputs(ms, cal, interpolation, kind)
                    if expected is None:
                        expected = result
                    else:
                        compare_products(result, expected)
            if interpolation == 'linear':
                for reader, task in engines.items():
                    ms = root / f'{kind}-composite-{reader}.ms'
                    copy_ms(vis, ms)
                    assert task(str(ms), 'ps,apply', interp=interpolation) is None
                    with opened(ms) as completed:
                        result = {key: completed.getcol(key) for key in expected}
                    compare_products(result, expected)
            print(f'{kind} {interpolation} sky application and weights agree', flush=True)
        if kind == 'data':
            expected = None
            for reader, task in engines.items():
                ms = root / f'nearestperobs-{reader}.ms'
                copy_ms(vis, ms)
                cal = products['casa']
                assert task(str(ms), 'apply', applytable=str(cal), interp='nearestperobs') is None
                result = inspect_outputs(ms, cal, 'nearestperobs', kind)
                if expected is None:
                    expected = result
                else:
                    compare_products(result, expected)
            print('nearestperobs uses nearest signal with linear exposure weights', flush=True)
    return sources


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=Path)
    parser.add_argument('--reference-only', action='store_true')
    args = parser.parse_args()
    assert casa.version_string() == '6.7.6.14'
    root = args.work.resolve()
    root.mkdir()
    engines = {'casa': casa.sdcal}
    generators = {'casa': casa.gencal}
    applycal = {'casa': casa.applycal}
    if not args.reference_only:
        from casars.tasks import sdcal, gencal, applycal as applycal_native
        engines['native'] = sdcal
        generators['native'] = gencal
        applycal['native'] = applycal_native
    sources = run(root, engines)
    check_chain(root, sources['data'], engines, generators)
    check_zero_reference(root, sources['data'], engines)
    check_flagged_reference(root, sources['data'], engines)
    check_errors(root, sources['data'], engines)
    check_spectral_weights(root, sources['data'], engines)
    check_frequency_interpolation(root, sources['data'], engines, applycal)
    check_observed_columns(root, applycal)


if __name__ == '__main__':
    main()
