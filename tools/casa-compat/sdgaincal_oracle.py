#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Paired double-circle gain calibration, prior application and table interchange."""

import argparse
from pathlib import Path
import signal
import subprocess
import sys

import casatasks as casa
import numpy as np

from applycal_oracle import copy_ms
from calibration_smoothcal_oracle import opened
from sdcal_apply_oracle import attach_mapped_tsys
from sdcal_edges_oracle import regular_map


def generated_map(root):
    mapping = regular_map(root)
    vis = root / 'double-circle.ms'
    casa.split(str(mapping), str(vis), spw='0:0', datacolumn='data')
    with opened(vis) as main:
        assert main.nrows() == 242
        assert main.getcol('DATA').shape[:2] == (2, 1)
    return vis


def make_priors(root, vis):
    gain = root / 'variable-prior.cal'
    casa.gencal(str(vis), str(gain), 'amp', parameter=[1.])
    with opened(vis) as main:
        endpoints = main.getcol('TIME')[[0, -1]]
    with opened(gain, True) as table:
        assert table.nrows() == 1
        table.addrows(1)
        for column in table.colnames():
            if table.iscelldefined(column, 0):
                table.putcell(column, 1, table.getcell(column, 0))
        table.putcol('TIME', endpoints)
        table.putcell('CPARAM', 1, table.getcell('CPARAM', 0) * 1.5)
    with opened(vis / 'SPECTRAL_WINDOW', True) as spectral:
        spectral.addrows(1)
        for column in spectral.colnames():
            if spectral.iscelldefined(column, 0):
                spectral.putcell(column, 1, spectral.getcell(column, 0))
    attach_mapped_tsys(root, vis)
    tsys = root / 'mapped-tsys.cal'
    casa.sdcal(str(vis), 'tsys', outfile=str(tsys))
    return tsys, gain


def compare_and_read(reference, native):
    with opened(reference / 'SPECTRAL_WINDOW') as casa_axis, opened(native / 'SPECTRAL_WINDOW') as native_axis:
        assert casa_axis.nrows() == native_axis.nrows()
        np.testing.assert_array_equal(native_axis.getcol('FLAG_ROW'), casa_axis.getcol('FLAG_ROW'))
        for column in ('NUM_CHAN', 'CHAN_FREQ', 'CHAN_WIDTH', 'RESOLUTION',
                       'EFFECTIVE_BW', 'TOTAL_BANDWIDTH', 'REF_FREQUENCY'):
            np.testing.assert_array_equal(native_axis.getcell(column, 0),
                                          casa_axis.getcell(column, 0), err_msg=column)
    with opened(reference) as casa_table, opened(native) as native_table:
        assert casa_table.getkeyword('VisCal') == native_table.getkeyword('VisCal') == 'SDGAIN_OTFD'
        assert casa_table.getkeyword('ParType') == native_table.getkeyword('ParType') == 'Complex'
        assert native_table.nrows() == casa_table.nrows()
        if not casa_table.nrows():
            return
        for name in ('TIME', 'INTERVAL', 'SCAN_NUMBER', 'OBSERVATION_ID', 'FIELD_ID',
                     'SPECTRAL_WINDOW_ID', 'ANTENNA1', 'ANTENNA2', 'FLAG', 'PARAMERR', 'SNR'):
            np.testing.assert_array_equal(native_table.getcol(name), casa_table.getcol(name),
                                          err_msg=name)
        np.testing.assert_allclose(native_table.getcol('CPARAM'), casa_table.getcol('CPARAM'),
                                   rtol=2e-6, atol=4e-7)
        for row in range(casa_table.nrows()):
            assert native_table.iscelldefined('WEIGHT', row) == casa_table.iscelldefined('WEIGHT', row)


def solve(root, vis, tasks, label, **options):
    results = {}
    for reader, task in tasks.items():
        output = root / f'gain-{label}-{reader}.cal'
        assert task(str(vis), outfile=str(output), **options) is None
        results[reader] = output
    if 'native' in results:
        compare_and_read(results['casa'], results['native'])
    with opened(results['casa']) as table:
        print(label, table.nrows(), 'gain rows', flush=True)
    return results


def normalized_power(vis, cal, smooth):
    with opened(cal) as table, opened(vis) as ms:
        target_times = table.getcol('TIME')
        rows = np.searchsorted(ms.getcol('TIME'), target_times)
        np.testing.assert_array_equal(ms.getcol('TIME')[rows], target_times)
        data = ms.getcol('DATA')[..., rows]
        flags = ms.getcol('FLAG')[..., rows]
        got = table.getcol('CPARAM')
    np.testing.assert_array_equal(got.imag, data.imag)
    assert len(rows) == 138
    half_width = round(65. / 1.5)
    count = len(rows)
    for receptor in range(data.shape[0]):
        power = data[receptor, 0].real.astype(np.float64)
        flagged = flags[receptor, 0]
        smoothed = power.copy()
        if smooth:
            for index in range(count):
                if not flagged[index]:
                    indices = np.arange(max(0, index-half_width), min(count, index+half_width+1))
                    smoothed[index] = power[indices[~flagged[indices]]].mean()
        normalized = np.sqrt(smoothed / smoothed[~flagged].mean())
        np.testing.assert_allclose(got[receptor, 0].real, normalized, rtol=2e-6, atol=4e-7)
    return got


def readers(root, source, products, tasks):
    expected = None
    for writer, product in products.items():
        for reader, task in tasks.items():
            output = root / f'interchange-{writer}-{reader}.ms'
            copy_ms(source, output)
            assert task(str(output), gaintable=[str(product)], flagbackup=False) is None
            with opened(output) as ms:
                corrected = ms.getcol('CORRECTED_DATA')
                flags = ms.getcol('FLAG')
                weights = ms.getcol('WEIGHT')
            if expected is None:
                expected = corrected, flags, weights
            else:
                np.testing.assert_allclose(corrected, expected[0], rtol=2e-6, atol=4e-7)
                np.testing.assert_array_equal(flags, expected[1])
                np.testing.assert_allclose(weights, expected[2], rtol=2e-6)
    print('Gain tables interchange in both directions', flush=True)


def test_selection(root, tasks, vis):
    selected = root / 'gain-selected.ms'
    copy_ms(vis, selected)
    with opened(selected / 'STATE', True) as states:
        source_mode = next(index for index, value in enumerate(states.getcol('OBS_MODE'))
                           if value.startswith('OBSERVE_TARGET#ON_SOURCE'))
        row = states.nrows()
        states.addrows(1)
        for column in states.colnames():
            if states.iscelldefined(column, source_mode):
                states.putcell(column, row, states.getcell(column, source_mode))
        states.putcell('OBS_MODE', row, 'OBSERVE_TARGET#ON_SOURCE,CALIBRATE_PHASE#ON_SOURCE')
    with opened(selected, True) as ms:
        ids = ms.getcol('STATE_ID')
        ids[:195] = row
        ms.putcol('STATE_ID', ids)
        scan = ms.getcol('SCAN_NUMBER')
        scan[200:] = 2
        ms.putcol('SCAN_NUMBER', scan)
    with opened(selected / 'ANTENNA') as antennas:
        name = antennas.getcell('NAME', 0)
    result = solve(root, selected, tasks, 'selected', radius='65', smooth=False, antenna=name,
                   field='0', spw='0', scan='1', intent='CALIBRATE_PHASE#ON_SOURCE')
    with opened(result['casa']) as table:
        assert 100 <= table.nrows() < 138


def test_single_polarization(root, tasks, vis):
    subset = root / 'single-receptor.ms'
    casa.split(str(vis), str(subset), spw='0', correlation='XX', datacolumn='data')
    with opened(subset, True) as ms:
        correlation = ms.getcol('DATA').real
        description = ms.getcoldesc('DATA')
        description.update(name='FLOAT_DATA', valueType='float', dataManagerType='StandardStMan',
                           dataManagerGroup='FloatGain')
        ms.addcols({'FLOAT_DATA': description})
        ms.putcol('FLOAT_DATA', correlation)
        ms.putcolkeyword('FLOAT_DATA', 'QuantumUnits', ['K'])
        ms.removecols('DATA')
    products = solve(root, subset, tasks, 'single', radius='65arcsec', smooth=False)
    with opened(products['casa']) as table:
        assert table.nrows() == 138
        np.testing.assert_array_equal(table.getcol('FLAG')[1], True)


def test_channel_selection(root, tasks, central):
    source = root / 'tp/tp.aca.tp.sd.ms'
    products = solve(root, source, tasks, 'channel', radius=65, smooth=False, spw='0:1')
    with opened(products['casa'] / 'SPECTRAL_WINDOW') as axis, opened(source / 'SPECTRAL_WINDOW') as observed:
        assert axis.getcell('NUM_CHAN', 0) == 1
        np.testing.assert_array_equal(axis.getcell('CHAN_FREQ', 0), observed.getcell('CHAN_FREQ', 0)[1:2])
    with opened(central) as reference, opened(products['casa']) as gain, opened(source) as ms:
        times = ms.getcol('TIME')
        center = np.isin(times, reference.getcol('TIME'))
        valid = ~ms.getcol('FLAG')[:, 1].any(axis=0)
        assert np.count_nonzero(center & ~valid) == 13
        np.testing.assert_array_equal(gain.getcol('TIME'), times[center & valid])
    # A scalar coefficient is required where pinned CASA aborts on two
    # selected channels. Run that reference in its own process.
    script = ('import sys; from casatasks import sdgaincal; '
              'sdgaincal(sys.argv[1], outfile=sys.argv[2], radius=65, spw="0:1~2")')
    casa_output = root / 'multi-channel-casa.cal'
    failed = subprocess.run([sys.executable, '-c', script, str(source), str(casa_output)],
                            capture_output=True, text=True, timeout=30)
    assert failed.returncode == -signal.SIGABRT and 'ArrayShapeError' in failed.stderr
    if 'native' in tasks:
        output = root / 'multi-channel-native.cal'
        try:
            tasks['native'](str(source), outfile=str(output), radius=65, spw='0:1~2')
        except RuntimeError as error:
            assert str(error) == 'Double-circle gain needs one selected channel per spectral window.'
        else:
            raise AssertionError('Multi-channel gain input was accepted')
        assert not output.exists()
    print('Selected scalar frequency matches and the multi-channel case is catchable', flush=True)


def test_errors(root, tasks, vis, products, prior):
    for implementation, task in tasks.items():
        output = root / f'gain-refuse-{implementation}.cal'
        scenarios = (
            ({'outfile': ''}, ValueError, 'outfile is empty.'),
            ({'calmode': 'otf'}, AssertionError, "{'calmode': ['unallowed value otf']}"),
            ({'radius': '-30arcsec'}, RuntimeError, 'Error in Calibrater::setsolve.'),
            ({'radius': 'metaphor'}, RuntimeError, 'Error in Calibrater::setsolve.'),
            ({'applytable': str(root / 'missing.cal')}, RuntimeError,
             f"Table {root / 'missing.cal'} does not exist."),
            ({'applytable': str(prior), 'interp': 42}, AssertionError, ''),
        )
        for arguments, kind, message in scenarios:
            try:
                task(str(vis), **dict({'outfile': str(output)}, **arguments))
            except Exception as error:
                assert type(error) is kind and str(error) == message, (arguments, error)
            else:
                raise AssertionError(f'Unexpected success with {arguments!r}')
        assert not output.exists()
        prior_output = products[implementation]
        try:
            task(str(vis), outfile=str(prior_output))
        except RuntimeError as error:
            assert str(error) == f'{prior_output} exists.'
        else:
            raise AssertionError('An existing gain table was replaced without overwrite')
        assert task(str(vis), outfile=str(prior_output), overwrite=True) is None
        with opened(prior_output) as table:
            assert table.nrows() == 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=Path)
    parser.add_argument('--reference-only', action='store_true')
    args = parser.parse_args()
    root = args.work.resolve()
    root.mkdir()
    assert casa.version_string() == '6.7.6.14'
    tasks = {'casa': casa.sdgaincal}
    applycal = {'casa': casa.applycal}
    if not args.reference_only:
        from casars.tasks import sdgaincal, applycal as native_apply
        tasks['native'] = sdgaincal
        applycal['native'] = native_apply
    vis = generated_map(root)
    default = solve(root, vis, tasks, 'default')
    with opened(default['casa']) as output:
        assert output.nrows() == 0
    plain = solve(root, vis, tasks, 'plain', radius='65arcsec', smooth=False)
    smooth = solve(root, vis, tasks, 'smooth', radius=65, smooth=True)
    raw = normalized_power(vis, plain['casa'], False)
    filtered = normalized_power(vis, smooth['casa'], True)
    assert np.max(np.abs(raw - filtered)) > .01
    tsys, prior = make_priors(root, vis)
    flat = solve(root, vis, tasks, 'scalar-prior', radius='65', smooth=False,
                 applytable=str(prior), interp='linear', spwmap=[-1])
    with opened(flat['casa']) as output:
        assert np.max(np.abs(output.getcol('CPARAM') - raw)) > .1
    tuned = solve(root, vis, tasks, 'priors', radius='65', smooth=False,
                  applytable=[str(tsys), str(prior)], interp=['nearest', 'linear'],
                  spwmap=[[1], [-1]])
    with opened(tuned['casa']) as output:
        assert np.max(np.abs(output.getcol('CPARAM') - raw)) > .1
    readers(root, vis, tuned, applycal)
    test_selection(root, tasks, vis)
    test_single_polarization(root, tasks, vis)
    test_channel_selection(root, tasks, plain['casa'])
    test_errors(root, tasks, vis, plain, prior)


if __name__ == '__main__':
    main()
