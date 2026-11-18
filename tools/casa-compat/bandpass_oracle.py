#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare ordinary B solutions and their formal errors with pinned CASA."""

import argparse
import json
import os
from pathlib import Path

import casatasks as casa
from casatools import table
import numpy as np

from applycal_oracle import copy_ms
from calibration_lifecycle_oracle import contents
from gaincal_oracle import relative_rms
from record_casa_returns import make_cal_ms


def source_data(path, stokes='XX YY'):
    make_cal_ms(str(path), channels=12, stokes=stokes)
    rng = np.random.default_rng(719)
    tool = table()
    tool.open(str(path), nomodify=False)
    try:
        data = tool.getcol('DATA')
        a, b = (tool.getcol(key) for key in ('ANTENNA1', 'ANTENNA2'))
        ch = np.arange(data.shape[1])[:, None]
        for receptor in range(data.shape[0]):
            gain_a = (1 + .07*a + .03*ch) * np.exp(1j * (.03+.012*ch) * a)
            gain_b = (1 + .07*b + .03*ch) * np.exp(1j * (.03+.012*ch) * b)
            noise = rng.normal(size=data.shape[1:]) + 1j*rng.normal(size=data.shape[1:])
            data[receptor] = gain_a * gain_b.conj() * (1.+.12*receptor) + .035*noise
        tool.putcol('DATA', data)
        tool.putcol('FLAG', np.zeros(data.shape, dtype=bool))
        tool.putcol('FLAG_ROW', np.zeros(len(a), dtype=bool))
    finally:
        tool.close()


def gapped_data(source, target):
    copy_ms(source, target)
    tool = table()
    tool.open(str(target), nomodify=False)
    try:
        data = tool.getcol('DATA')
        a, b = (tool.getcol(key) for key in ('ANTENNA1', 'ANTENNA2'))
        # The bounded single-channel gap crosses the phase wrap for A4.
        data *= np.exp(.24j * np.arange(data.shape[1])[:, None] * (a-b))
        tool.putcol('DATA', data)
        flags = tool.getcol('FLAG')
        flags[:, [0, 3, 6, 7, 11]] = True
        tool.putcol('FLAG', flags)
    finally:
        tool.close()


def compare(root, source, native, normalize, fillgaps=0, remaining_flags=None, s_model=(1.,),
            solint='inf', interval_count=None, parang=False, minsnr=0., spw='', bins=None):
    products, corrected, returns, windows = [], [], [], []
    for name, engine in [('casa', casa), ('native', native)]:
        ms, output = root/f'{name}.ms', root/f'{name}.B'
        copy_ms(source, ms)
        result = engine.bandpass(str(ms), str(output), field='0', spw=spw, refant='A0',
                                 smodel=list(s_model), minsnr=minsnr, solnorm=normalize,
                                 fillgaps=fillgaps, solint=solint, parang=parang)
        if interval_count is not None:
            np.testing.assert_array_equal(result['solvestats']['expected'], [interval_count]*2)
        np.testing.assert_array_equal(result.pop('solve_tables'), [str(output)])
        returns.append(result)
        products.append(contents(output))
        if bins is not None:
            windows.append(contents(output/'SPECTRAL_WINDOW'))
        engine.applycal(str(ms), field='0', gaintable=[str(output)], flagbackup=False, spw=spw,
                        parang=parang)
        corrected.append(contents(ms))
    np.testing.assert_equal(returns[1], returns[0])
    expected, actual = products
    for column in ('FLAG', 'ANTENNA1', 'ANTENNA2', 'FIELD_ID', 'SPECTRAL_WINDOW_ID'):
        np.testing.assert_array_equal(actual[column], expected[column], err_msg=column)
    np.testing.assert_allclose(actual['TIME'], expected['TIME'], rtol=0., atol=1e-4)
    if interval_count is not None:
        assert len(np.unique(actual['TIME'])) == interval_count
    if remaining_flags is None:
        assert np.all(expected['PARAMERR'][~expected['FLAG']] > 0.)
    else:
        np.testing.assert_array_equal(np.flatnonzero(actual['FLAG'].all(axis=(0, 2))), remaining_flags)
    if minsnr > 0.:
        assert np.any(expected['FLAG']) and np.any(~expected['FLAG'])
        assert np.any(abs(expected['CPARAM'][expected['FLAG']] - 1.) > .1)
    errors = {key: relative_rms(actual[key], expected[key]) for key in ('CPARAM', 'PARAMERR', 'SNR')}
    expected_ms, actual_ms = corrected
    for column in ('FLAG', 'FLAG_ROW'):
        np.testing.assert_array_equal(actual_ms[column], expected_ms[column], err_msg=column)
    for column in ('CORRECTED_DATA', 'WEIGHT', 'SIGMA'):
        errors[column] = relative_rms(actual_ms[column], expected_ms[column])
    if bins is not None:
        compare_bins(windows, contents(source/'SPECTRAL_WINDOW'), bins)
        assert actual['CPARAM'].shape[1] == len(bins)
    return errors


def compare_bins(windows, original, bins):
    for key in ('NUM_CHAN', 'FLAG_ROW', 'CHAN_FREQ', 'CHAN_WIDTH', 'EFFECTIVE_BW',
                'RESOLUTION', 'REF_FREQUENCY', 'TOTAL_BANDWIDTH'):
        np.testing.assert_allclose(windows[1][key], windows[0][key], atol=.001, rtol=0.,
                                   err_msg=key)
    centers = [np.mean(original['CHAN_FREQ'][channels, 0]) for channels in bins]
    widths = [sum(abs(original['CHAN_WIDTH'][channels, 0])) for channels in bins]
    np.testing.assert_allclose(windows[0]['CHAN_FREQ'][:, 0], centers, atol=.001, rtol=0.)
    np.testing.assert_array_equal(windows[0]['CHAN_WIDTH'][:, 0], widths)


def frequency_cases(root, source, native):
    descending = root/'descending.ms'
    copy_ms(source, descending)
    tool = table()
    tool.open(str(descending/'SPECTRAL_WINDOW'), nomodify=False)
    try:
        count = tool.getcell('NUM_CHAN', 0)
        first_freq = tool.getcell('CHAN_FREQ', 0)[0]
        frequencies = first_freq-np.cumsum(np.arange(count)*1.e5)
        widths = -np.arange(1, count+1)*1.e6
        tool.putcell('CHAN_FREQ', 0, frequencies)
        tool.putcell('CHAN_WIDTH', 0, widths)
        tool.putcell('EFFECTIVE_BW', 0, abs(widths))
        tool.putcell('RESOLUTION', 0, abs(widths))
    finally:
        tool.close()
    cases = [
        ('bins-channels', source, 'inf,2ch', '0', [[i, i+1] for i in range(0, 12, 2)], 1),
        ('bins-bandwidth', source, '25s,3MHz', '0', [list(range(i, i+3)) for i in range(0, 12, 3)], 3),
        ('bins-descending', descending, 'inf,5ch', '0',
         [list(range(5)), list(range(5, 10)), [10, 11]], 1),
        ('bins-disjoint', descending, 'int,2.7ch', '0:1~3;7~10',
         [[1, 2], [3, 7], [8, 9], [10]], 6),
    ]
    results = {}
    for label, dataset, interval, spw, bins, count in cases:
        folder = root/label
        folder.mkdir()
        results[label] = compare(folder, dataset, native, False, solint=interval,
                                 interval_count=count, spw=spw, bins=bins)
    weighted = root/'binned-weights.ms'
    gapped_data(source, weighted)
    casa.clearcal(str(weighted), addmodel=True)
    tool.open(str(weighted), nomodify=False)
    try:
        shape = tool.getcol('DATA').shape
        for column in ('WEIGHT_SPECTRUM', 'SIGMA_SPECTRUM'):
            if column in tool.colnames():
                tool.removecols(column)
            description = tool.getcoldesc('DATA')
            description.update(valueType='float', dataManagerType='StandardStMan',
                               dataManagerGroup='StandardStMan', option=0)
            description.pop('shape', None)
            tool.addcols({column: description})
        sigmas = np.broadcast_to(.5 + np.arange(shape[1])[None, :, None]/10
                                + (np.arange(shape[2]) % 7)[None, None, :]/15, shape)
        tool.putcol('SIGMA_SPECTRUM', sigmas.astype(np.float32))
        tool.putcol('WEIGHT_SPECTRUM', np.ones(shape, dtype=np.float32))
        model = tool.getcol('MODEL_DATA')
        model[:, 4, ::2] = 0.
        tool.putcol('MODEL_DATA', model)
    finally:
        tool.close()
    folder = root/'bins-weights-and-model'
    folder.mkdir()
    results[folder.name] = compare(folder, weighted, native, False, remaining_flags=[3], s_model=(),
                                   solint='inf,2ch', spw='0', bins=cases[0][4])
    return results


def model_cases(root, source, native):
    scratch = root/'scratch-model.ms'
    copy_ms(source, scratch)
    casa.clearcal(str(scratch), addmodel=True)
    tool = table()
    tool.open(str(scratch), nomodify=False)
    try:
        model = tool.getcol('MODEL_DATA')
        channel = np.arange(model.shape[1])[:, None]
        model[0] = 1.4 + .03*channel
        model[1] = 1.7 + .02*channel
        times = tool.getcol('TIME')
        model[[0, 1], 3, :] *= times != times.min()
        tool.putcol('MODEL_DATA', model)
    finally:
        tool.close()
    virtual = root/'virtual-model.ms'
    copy_ms(scratch, virtual)
    casa.setjy(str(virtual), field='0', standard='manual', fluxdensity=[3., 0., 0., 0.],
               usescratch=False)

    results = {}
    for label, dataset, point in [('scratch-model', scratch, ()), ('virtual-model', virtual, ()),
                                   ('polarized-point', source, (1.1, .24, .19, .15))]:
        folder = root/label
        folder.mkdir()
        results[label] = compare(folder, dataset, native, False, s_model=point)
        reference = contents(folder/'casa.B')['CPARAM']
        no_model = contents(root/'standard'/'casa.B')['CPARAM']
        assert relative_rms(reference, no_model) > .08, label
    linear = root/'full-linear.ms'
    source_data(linear, stokes='XX XY YX YY')
    folder = root/'parang-point'
    folder.mkdir()
    results[folder.name] = compare(folder, linear, native, False, s_model=(1.1, .24, .19, .15),
                                   parang=True)
    cross_weighted = root/'cross-weighted.ms'
    copy_ms(linear, cross_weighted)
    casa.clearcal(str(cross_weighted), addmodel=True)
    tool.open(str(cross_weighted), nomodify=False)
    try:
        model = tool.getcol('MODEL_DATA')
        time = tool.getcol('TIME')
        cross_amplitude = .15 + 1.2*(time-time.min())/np.ptp(time)
        model[1:3] = cross_amplitude
        tool.putcol('MODEL_DATA', model)
    finally:
        tool.close()
    folder = root/'cross-weighted'
    folder.mkdir()
    results[folder.name] = compare(folder, cross_weighted, native, False, s_model=())
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--native-bin', type=Path, required=True)
    args = parser.parse_args()
    assert casa.version_string() == '6.7.6.14'
    os.environ['CASARS_CALIBRATE_BIN'] = str(args.native_bin.resolve())
    os.environ['CASARS_REQUIRE_TASK_BINARIES'] = '1'
    from casars import tasks as native

    root = args.work.resolve()
    root.mkdir(parents=True)
    source = root/'source.ms'
    source_data(source)
    gapped = root/'gapped.ms'
    gapped_data(source, gapped)
    results = {}
    for label, normalize in [('standard', False), ('normalized', True)]:
        folder = root/label
        folder.mkdir()
        results[label] = compare(folder, source, native, normalize)
    for gaps, flags in [(-1, [0, 3, 6, 7, 11]), (1, [0, 6, 7, 11]), (2, [0, 11])]:
        folder = root/f'fill-{gaps}'
        folder.mkdir()
        results[folder.name] = compare(folder, gapped, native, False, gaps, flags)
    cutoff = root/'snr-cutoff'
    cutoff.mkdir()
    results[cutoff.name] = compare(cutoff, source, native, False, minsnr=165.)
    results.update(model_cases(root, source, native))
    for interval, count in [('int', 6), ('25s', 3), ('inf,none', 1)]:
        folder = root/f"interval-{interval.replace(',', '-')}"
        folder.mkdir()
        results[folder.name] = compare(folder, source, native, False, solint=interval,
                                       interval_count=count)
    results.update(frequency_cases(root, source, native))
    (root/'results.json').write_text(json.dumps(results, indent=2)+'\n')
    for name, report in results.items():
        assert max(report.values()) < 1e-3, (name, report)


if __name__ == '__main__':
    main()
