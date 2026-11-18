#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare CASA and native gaincal delay solves and downstream application."""

import argparse
import json
import os
from pathlib import Path

import casatasks as casa
from casatools import table
import numpy as np

from gaincal_oracle import contents, copy_ms, relative_rms
from record_casa_returns import make_cal_ms

ANTENNA_DELAYS_NS = np.array([[0., .41, -.56, .78, -.24], [0., -.28, .42, .10, -.69]])
REFERENCE_HZ = 100e9


def duplicate_rows(tool, path):
    tool.open(str(path), nomodify=False)
    try:
        count = tool.nrows()
        tool.addrows(count)
        for column in tool.colnames():
            for row in range(count):
                if tool.iscelldefined(column, row):
                    tool.putcell(column, row + count, tool.getcell(column, row))
        return count
    finally:
        tool.close()


def delay_data(vis, family, second_width=None, *, stokes='XX XY YX YY'):
    telescope = 'VLA' if stokes.startswith('RR') else 'ALMA'
    make_cal_ms(str(vis), stokes=stokes, telescope=telescope)
    tool = table()
    frequencies = [(REFERENCE_HZ, 250e6)]
    if second_width is not None:
        frequencies.append((REFERENCE_HZ + 3.125e9, second_width))
        assert duplicate_rows(tool, vis/'SPECTRAL_WINDOW') == 1
        assert duplicate_rows(tool, vis/'DATA_DESCRIPTION') == 1
        tool.open(str(vis/'DATA_DESCRIPTION'), nomodify=False)
        try:
            tool.putcell('SPECTRAL_WINDOW_ID', 1, 1)
        finally:
            tool.close()
        row_count = duplicate_rows(tool, vis)
    else:
        tool.open(str(vis))
        try:
            row_count = tool.nrows()
        finally:
            tool.close()
    for spw, (first, width) in enumerate(frequencies):
        centers = first + width*np.arange(8)
        tool.open(str(vis/'SPECTRAL_WINDOW'), nomodify=False)
        try:
            tool.putcell('CHAN_FREQ', spw, centers)
            tool.putcell('REF_FREQUENCY', spw, first)
            for key in ('CHAN_WIDTH', 'RESOLUTION', 'EFFECTIVE_BW'):
                tool.putcell(key, spw, np.full(centers.size, width))
            tool.putcell('TOTAL_BANDWIDTH', spw, width*centers.size)
        finally:
            tool.close()
        start = spw*row_count
        tool.open(str(vis), nomodify=False)
        try:
            a1, a2 = (tool.getcol(key, start, row_count) for key in ('ANTENNA1', 'ANTENNA2'))
            data = np.empty_like(tool.getcol('DATA', start, row_count))
            for correlation, (first_receptor, second_receptor) in enumerate(
                    ((0, 0), (0, 1), (1, 0), (1, 1))):
                if family == 'K':
                    delay = (ANTENNA_DELAYS_NS[first_receptor, a1]
                             - ANTENNA_DELAYS_NS[second_receptor, a2])
                else:
                    delay = np.full(a1.shape, .63*(second_receptor - first_receptor))
                data[correlation] = np.exp(2j*np.pi*(centers[:, None]-REFERENCE_HZ)*1e-9*delay)
            tool.putcol('DATA', data, start, row_count)
            tool.putcol('DATA_DESC_ID', np.full(row_count, spw, dtype=np.int32), start, row_count)
            tool.putcol('FLAG', np.zeros(data.shape, dtype=bool), start, row_count)
            tool.putcol('FLAG_ROW', np.zeros(a1.shape, dtype=bool), start, row_count)
        finally:
            tool.close()


def solutions(path):
    tool = table()
    tool.open(str(path))
    try:
        columns = ('FPARAM', 'FLAG', 'PARAMERR', 'SNR', 'ANTENNA1', 'ANTENNA2', 'FIELD_ID',
                   'OBSERVATION_ID', 'SPECTRAL_WINDOW_ID', 'TIME', 'INTERVAL', 'SCAN_NUMBER')
        assert all(not tool.iscelldefined('WEIGHT', row) for row in range(tool.nrows()))
        result = {column: tool.getcol(column) for column in columns}
        result['VisCal'] = tool.getkeyword('VisCal')
        return result
    finally:
        tool.close()


def compare(root, vis, native, family, mode, combined, *, interchange=False,
            delay_atol=None, **overrides):
    products, corrected, returns = [], [], []
    selection = '0,1' if combined else '0'
    options = dict(gaintype=family, calmode=mode, field='0', spw=selection,
                   combine='spw' if combined else '', refant='A0', solint='inf',
                   minsnr=0., smodel=[1., 0., 0., 0.])
    options.update(overrides)
    apply_options = dict(field='0', spw=options['spw'], spwmap=[[0, 0]] if combined else [],
                         flagbackup=False)
    engines = [('casa', casa), ('native', native)]
    for name, task in engines:
        dataset, output = root/f'{name}.ms', root/f'{name}.cal'
        copy_ms(vis, dataset)
        preload = os.environ.pop('LD_PRELOAD', None) if task is native else None
        try:
            if options.get('append'):
                first = dict(options, field='1', append=False)
                task.gaincal(vis=str(dataset), caltable=str(output), **first)
            result = task.gaincal(vis=str(dataset), caltable=str(output), **options)
            task.applycal(vis=str(dataset), gaintable=[str(output)], **apply_options)
        finally:
            if preload is not None:
                os.environ['LD_PRELOAD'] = preload
        products.append(solutions(output))
        corrected.append(contents(dataset))
        np.testing.assert_array_equal(result.pop('solve_tables'), [str(output)])
        returns.append(result)

    np.testing.assert_equal(returns[1], returns[0])
    expected, actual = products
    for column in expected.keys() - {'FPARAM', 'TIME'}:
        np.testing.assert_array_equal(actual[column], expected[column], err_msg=column)
    np.testing.assert_allclose(actual['TIME'], expected['TIME'], rtol=0., atol=1e-4)
    delays = actual['FPARAM']
    if delay_atol is not None:
        np.testing.assert_allclose(delays, expected['FPARAM'], rtol=0., atol=delay_atol)
    elif mode == 'a':
        # The Fourier peak of amplitude-only data has zero delay.
        np.testing.assert_allclose(delays, expected['FPARAM'], rtol=0., atol=1e-6)
    else:
        assert relative_rms(delays, expected['FPARAM']) < 1e-3
    if family == 'K' and mode != 'a' and not overrides:
        np.testing.assert_allclose(delays[:, 0], ANTENNA_DELAYS_NS, atol=2e-4)
    if family == 'KCROSS' and mode != 'a' and not overrides:
        np.testing.assert_allclose(delays[0, 0], .63, atol=2e-4)
        np.testing.assert_allclose(delays[1, 0], 0., atol=1e-7)

    expected, actual = corrected
    for column in ('FLAG', 'FLAG_ROW'):
        np.testing.assert_array_equal(actual[column], expected[column])
    errors = {column: relative_rms(actual[column], expected[column])
              for column in ('CORRECTED_DATA', 'WEIGHT', 'SIGMA')}
    assert max(errors.values()) < 1e-3, errors
    errors['delay_absolute_ns'] = float(np.max(abs(delays-products[0]['FPARAM'])))
    if interchange:
        for (name, engine), (other, _) in zip(engines, reversed(engines)):
            dataset = root/f'{name}-apply-{other}.ms'
            copy_ms(vis, dataset)
            engine.applycal(vis=str(dataset), gaintable=[str(root/f'{other}.cal')], **apply_options)
            observed = contents(dataset)
            for column in ('FLAG', 'FLAG_ROW'):
                np.testing.assert_array_equal(observed[column], expected[column])
            for column in ('CORRECTED_DATA', 'WEIGHT', 'SIGMA'):
                error = relative_rms(observed[column], expected[column])
                assert error < 1e-3, (name, other, column, error)
                errors[f'{name}_apply_{other}_{column}'] = error
    return errors


def selection_and_averaging(root, source, native, family):
    damaged = root/f'{family}-flagged.ms'
    copy_ms(source, damaged)
    tool = table()
    tool.open(str(damaged), nomodify=False)
    try:
        data, flags = tool.getcol('DATA'), tool.getcol('FLAG')
        flags[0, 1] = flags[1, 2] = flags[2, 4] = True
        data[flags] = 1e4 + 1e4j
        tool.putcol('DATA', data)
        tool.putcol('FLAG', flags)
    finally:
        tool.close()
    cases = [
        ('reference-A2', source, dict(refant='A2')),
        ('gapped', source, dict(spw='0:0~2;5~7')),
        ('cuts', source, dict(minblperant=12, minsnr=50.)),
        ('flags-shared', damaged, dict(corrdepflags=False)),
        ('flags-independent', damaged, dict(corrdepflags=True)),
        ('integration', source, dict(solint='int')),
        ('preaverage', source, dict(preavg=12.)),
        ('finite-interval', source, dict(solint='22s')),
    ]
    results = {}
    for label, vis, options in cases:
        folder = root/f'{family}-{label}'
        folder.mkdir()
        results[folder.name] = compare(folder, vis, native, family, 'ap', False, **options)
    return results


def make_priors(root, source):
    """Produce nontrivial antenna and channel corrections in CASA."""
    vis = root/'prior.ms'
    copy_ms(source, vis)
    tool = table()
    tool.open(str(vis), nomodify=False)
    try:
        data = tool.getcol('DATA')
        a, b = (tool.getcol(key) for key in ('ANTENNA1', 'ANTENNA2'))
        channel = np.arange(data.shape[1])[:, None]
        phase = (.03 + .08*channel + .01*channel**2) * (a-b)
        amplitude = (1+.18*a)*(1+.18*b)*(1+.022*channel*a)*(1+.022*channel*b)
        data[:] = np.exp(1j*phase)*amplitude
        tool.putcol('DATA', data)
    finally:
        tool.close()
    gains, bandpass = (str(root/f'prior.{suffix}') for suffix in ('G', 'B'))
    casa.gaincal(str(vis), gains, refant='A0', minsnr=0., smodel=[1.])
    casa.bandpass(str(vis), bandpass, refant='A0', minsnr=0., smodel=[1.], gaintable=[gains])
    library = root/'prior.callib'
    library.write_text(''.join(f"caltable='{path}' tinterp='nearest' calwt=True\n"
                               for path in (gains, bandpass)))
    return [gains, bandpass], str(library)


def workflow_and_ambiguity(root, native, family, priors, library):
    source = root/f'{family}-single.ms'
    circular = root/f'{family}-circular.ms'
    delay_data(circular, family, stokes='RR RL LR LL')
    ambiguous = root/f'{family}-ambiguous.ms'
    copy_ms(source, ambiguous)
    tool = table()
    tool.open(str(ambiguous), nomodify=False)
    try:
        if family == 'K':
            # Put the reference baseline's peak on the FFT boundary.
            data = tool.getcol('DATA')
            data[:, :, tool.getcol('ANTENNA2') == 1] = (-1.)**np.arange(data.shape[1])[None, :, None]
            tool.putcol('DATA', data)
        else:
            flags = tool.getcol('FLAG')
            flags[1:3] = True
            tool.putcol('FLAG', flags)
    finally:
        tool.close()
    prior_delay = str(root/f'{family}-single-ap'/'casa.cal')
    stored = root/f'{family}-scratch-model.ms'
    copy_ms(source, stored)
    casa.clearcal(str(stored), addmodel=True)
    tool.open(str(stored), nomodify=False)
    try:
        model = tool.getcol('MODEL_DATA')
        for correlation, visibility in enumerate([1.13, .31 + .05j, .31 - .05j, .87]):
            model[correlation] = visibility
        model[1:3, 1::2] = 0
        tool.putcol('MODEL_DATA', model)
    finally:
        tool.close()
    cases = [
        ('traditional', source, dict(gaintable=priors, gainfield=['0','0'],
                                     interp=['nearest','nearest'], spwmap=[[0],[0]])),
        ('callib', source, dict(docallib=True, callib=library)),
        # CASA puts both K and KCROSS priors on the model side of a delay
        # solve. A zero cross-hand model must leave that visibility's phase
        # slope intact. Nonzero model terms are divided out as usual.
        ('delay-prior', source, dict(gaintable=[prior_delay], delay_atol=1e-6)),
        ('delay-prior-polarized', source, dict(gaintable=[prior_delay], delay_atol=1e-6,
                                               smodel=[1., .16, -.31, .22])),
        ('delay-prior-scratch-model', stored, dict(gaintable=[prior_delay], delay_atol=1e-6,
                                                  smodel=[])),
        ('circular', circular, dict(append=True, interchange=True)),
        ('polarized', source, dict(smodel=[1., .16, -.31, .22])),
        ('zero-receptor', source, dict(smodel=[1., 1., 0., 0.])),
        ('parang', source, dict(smodel=[1., .16, -.31, .22], parang=True)),
        ('parang-unpolarized', source, dict(parang=True)),
        ('inactive-controls', source, dict(refant='A3,A2', refantmode='strict', solnorm=True,
                                          normtype='median', solmode='L1R', rmsthresh=[.9],
                                          splinetime=30., npointaver=5, phasewrap=90.)),
        ('ambiguity', ambiguous, dict(corrdepflags=True)),
    ]
    results = {}
    for label, vis, options in cases:
        folder = root/f'{family}-{label}'
        folder.mkdir()
        results[folder.name] = compare(folder, vis, native, family, 'ap', False, **options)
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
    results = {}
    for family in ('K', 'KCROSS'):
        for width in (None, 250e6, 125e6):
            label = 'single' if width is None else f'combined-{int(width/1e6)}'
            source = root/f'{family}-{label}.ms'
            delay_data(source, family, width)
            for mode in ('ap', 'p', 'a') if width is None else ('ap',):
                case = root/f'{family}-{label}-{mode}'
                case.mkdir()
                results[case.name] = compare(case, source, native, family, mode, width is not None)
            if width is None:
                results.update(selection_and_averaging(root, source, native, family))
    priors, library = make_priors(root, root/'K-single.ms')
    for family in ('K', 'KCROSS'):
        results.update(workflow_and_ambiguity(root, native, family, priors, library))
    (root/'results.json').write_text(json.dumps(results, indent=2)+'\n')


if __name__ == '__main__':
    main()
