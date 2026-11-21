#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare cross-hand phase and fitted source polarization with CASA 6.7.6.14."""

import argparse
from pathlib import Path

import casatasks as casa
from casatools import table as table_tool
import numpy as np

from applycal_oracle import copy_ms
from calibration_smoothcal_oracle import opened
from gaincal_delay_oracle import duplicate_rows
from polcal_cross_hand_oracle import plant_cross_hand
from polcal_leakage_solve_oracle import compare_apply, plant, put_observations
from polcal_standard_leakage_oracle import compare


def solve(root, vis, api, mode, stokes, **options):
    root.mkdir()
    products = {label: root/f'{label}.cal' for label in ('casa', 'native')}
    results = {}
    for label, task in [('casa', casa.polcal), ('native', api.polcal)]:
        results[label] = task(str(vis), str(products[label]), field='0', poltype=mode,
                              smodel=stokes, refant='A2', **options)
    gain, error, snr = compare(products['casa'], products['native'], gain_tolerance=1e-4,
                               time_tolerance=1e-4)
    with opened(products['casa']) as want, opened(products['native']) as got:
        expected = want.getkeyword('QU')['QU']
        fitted = got.getkeyword('QU')['QU']
        np.testing.assert_allclose(fitted, expected, atol=8e-5, rtol=0.)
        phases = got.getcol('CPARAM')
    if mode == 'Xfparang+QU':
        compare_stokes_result(results)
    else:
        assert results['native'] is results['casa'] is None
    print(root.parent.name, root.name, 'gain/error/SNR normalized RMS', (gain, error, snr),
          'maximum Q/U difference', np.max(np.abs(fitted-expected)))
    return products, phases


def compare_stokes_result(results):
    assert results['native'].keys() == results['casa'].keys() == {'FLUX'}
    reference, actual = (results[label]['FLUX'] for label in ('casa', 'native'))
    assert reference.keys() == actual.keys()
    for key in reference:
        assert actual[key].dtype == reference[key].dtype
        np.testing.assert_allclose(actual[key], reference[key], atol=8e-5, rtol=0.)
    windows = [stokes for key, stokes in actual.items() if key != 'SpwAve']
    np.testing.assert_allclose(actual['SpwAve'], np.mean(windows, axis=0), atol=1e-7, rtol=0.)


def compare_windows(root, source, api, stokes):
    root.mkdir()
    vis = root/'two-windows.ms'
    copy_ms(source, vis)
    tool = table_tool()
    assert duplicate_rows(tool, vis/'SPECTRAL_WINDOW') == 1
    assert duplicate_rows(tool, vis/'DATA_DESCRIPTION') == 1
    rows = duplicate_rows(tool, vis)
    with opened(vis/'DATA_DESCRIPTION', True) as description:
        description.putcell('SPECTRAL_WINDOW_ID', 1, 1)
    with opened(vis/'SPECTRAL_WINDOW', True) as windows:
        for key in ('CHAN_FREQ', 'REF_FREQUENCY'):
            windows.putcell(key, 1, windows.getcell(key, 0) + 32e6)
    with opened(vis, True) as table:
        table.putcol('DATA_DESC_ID', np.ones(rows, np.int32), rows, rows)
        data = table.getcol('DATA', rows, rows)
        data[1:3] *= .61
        table.putcol('DATA', data, rows, rows)
    products, _ = solve(root/'both', vis, api, 'Xfparang+QU', stokes, spw='0,1')
    with opened(products['native']) as table:
        qu = table.getkeyword('QU')['QU']
        np.testing.assert_allclose(qu[:, 1], .61 * qu[:, 0], atol=8e-5, rtol=0.)

    products, _ = solve(root/'append', vis, api, 'Xfparang+QU', stokes, spw='0')
    results = {}
    for label, task in [('casa', casa.polcal), ('native', api.polcal)]:
        results[label] = task(str(vis), str(products[label]), field='0', spw='1',
                              poltype='Xfparang+QU', smodel=stokes, append=True)
        with opened(products[label]) as table:
            assert table.nrows() == 10
            np.testing.assert_array_equal(table.getkeyword('QU')['QU'][:, 1], 0.)
    compare_stokes_result(results)
    assert results['native']['FLUX'].keys() == {'Spw1', 'SpwAve'}
    compare(products['casa'], products['native'], gain_tolerance=1e-4, time_tolerance=1e-4)


def compare_degenerate_coverage(root, vis, native, basis, stokes, sky, phase):
    with opened(vis/'ANTENNA', True) as antennas:
        antennas.putcol('MOUNT', np.full(antennas.nrows(), 'EQUATORIAL'))
    with opened(vis, True) as table:
        table.putcol('FLAG', np.zeros_like(sky, dtype=bool))
    put_observations(vis, sky, [phase])
    for label, task in [('casa', casa.polcal), ('native', native.polcal)]:
        output = root/f'{label}-constant-parallactic-angle.cal'
        try:
            result = task(str(vis), str(output), field='0', poltype='Xfparang+QU', smodel=stokes)
        except RuntimeError:
            assert label == 'native' or basis.startswith('RR')
            assert not output.exists()
        else:
            # CASA's linear least-squares call can return zero source Q/U
            # and unflagged phases set by rounding of a constant signal.
            assert label == 'casa' and basis.startswith('XX')
            with opened(output) as table:
                assert not table.getcol('FLAG').any()
            np.testing.assert_array_equal(result['FLUX']['Spw0'], [1., 0., 0., 0.])


def compare_irregular_sampling(root, source, native, stokes):
    root.mkdir()
    vis = root/'irregular.ms'
    copy_ms(source, vis)
    with opened(vis, True) as table:
        fields = table.getcol('FIELD_ID')
        times = table.getcol('TIME')
        first, _, _, fourth = np.unique(times[fields == 0])[:4]
        baseline = (table.getcol('ANTENNA1') == 0) & (table.getcol('ANTENNA2') == 1)
        fields[(fields == 0) & ((baseline & (times == first)) | (times == fourth))] = 1
        table.putcol('FIELD_ID', fields)
        data = table.getcol('DATA')
        data[1, :, baseline] *= np.exp(.9j)
        data[2, :, baseline] *= np.exp(-.9j)
        table.putcol('DATA', data)
    solve(root/'joint-flush', vis, native, 'Xfparang+QU', stokes, preavg=1300.)


def compare_exact_linear_phases(root, vis, native, stokes, sky):
    stokes = [*stokes[:3], 0.]
    for name, phase in [('real', 1.), ('imaginary', 1j)]:
        data = put_observations(vis, sky.real.astype(np.complex64), [])
        data[1] *= phase
        data[2] *= np.conjugate(phase)
        with opened(vis, True) as table:
            table.putcol('DATA', data)
        for label, task in [('casa', casa.polcal), ('native', native.polcal)]:
            output = root/f'{label}-{name}.cal'
            solved = task(str(vis), str(output), field='0', poltype='Xfparang+QU', smodel=stokes)
            with opened(output) as table:
                gain = table.getcol('CPARAM')
                if label == 'native':
                    np.testing.assert_allclose(gain, phase, atol=3e-5, rtol=0.)
                    np.testing.assert_allclose(solved['FLUX']['Spw0'][1:3],
                                               np.asarray(stokes[1:3])/stokes[0], atol=8e-5)
                else:
                    assert np.max(np.abs(gain-phase)) > 1.


def compare_zero_circular_signal(root, vis, native, stokes, sky, phase, expected):
    data = put_observations(vis, sky, [phase])
    data[1:3, 0] = 0.
    with opened(vis, True) as table:
        table.putcol('DATA', data)
    for label, task in [('casa', casa.polcal), ('native', native.polcal)]:
        output = root/f'{label}-zero-signal.cal'
        solved = task(str(vis), str(output), field='0', poltype='Xfparang+QU', smodel=stokes)
        with opened(output) as table:
            gains, flags = table.getcol('CPARAM'), table.getcol('FLAG')
            qu = solved['FLUX']['Spw0'][1:3]
            if label == 'native':
                assert flags[:, 0].all() and not flags[:, 1:].any()
                np.testing.assert_array_equal(gains[:, 0], 1.)
                np.testing.assert_allclose(gains[:, 1:], expected[:, 1:], atol=3e-5, rtol=0.)
                np.testing.assert_allclose(qu, [np.hypot(*stokes[1:3])/stokes[0], 0.],
                                           atol=8e-5, rtol=0.)
            else:
                assert np.isnan(gains[:, 0]).all() and not flags[:, 0].any()
                assert np.isnan(qu).all()


def compare_basis(root, basis, api):
    root.mkdir()
    vis, leak, gains, stokes, sky = plant(root, basis)
    with opened(vis) as ms:
        time = ms.getcol('TIME')
        baselines = (ms.getcol('ANTENNA1'), ms.getcol('ANTENNA2'))
    planted = root/'planted-phase.cal'
    phase = np.linspace(.94, 1.93, sky.shape[1], dtype=np.float32)
    gains_at_phase = plant_cross_hand(vis, planted, 5, time.min())
    gains_at_phase[:] = np.exp(1j * phase)[None, :, None]
    with opened(planted, True) as table:
        table.putcol('CPARAM', gains_at_phase)
    put_observations(vis, sky, [planted])
    products, _ = solve(root/'scalar', vis, api, 'Xparang+QU', stokes)
    compare_apply(root/'scalar', vis, products, api, [], cross_hand_tolerance=1e-4)
    products, actual = solve(root/'spectrum', vis, api, 'Xfparang+QU', stokes)
    np.testing.assert_allclose(actual, gains_at_phase, atol=4e-5, rtol=0.)
    compare_apply(root/'spectrum', vis, products, api, [], cross_hand_tolerance=1e-4)
    if basis.startswith('XX'):
        compare_windows(root/'windows', vis, api, stokes)
        compare_irregular_sampling(root/'irregular', vis, api, stokes)
        compare_exact_linear_phases(root, vis, api, stokes, sky)
    else:
        compare_zero_circular_signal(root, vis, api, stokes, sky, planted, gains_at_phase)

    priors = [gains, leak]
    put_observations(vis, sky, [*priors, planted])
    products, _ = solve(root/'preaveraged-priors', vis, api, 'Xfparang+QU', [],
                        gaintable=[str(table) for table in priors], preavg=1300.,
                        spw='0:2~6', solint='inf,2chan')
    compare_apply(root/'preaveraged-priors', vis, products, api, priors, cross_hand_tolerance=1e-4)

    # Unit Stokes I isolates baseline flag extension from CASA's known
    # pointer defect when normalizing across flagged, nonunit channels.
    unit_stokes = [stokes_component / stokes[0] for stokes_component in stokes]
    data = put_observations(vis, sky / stokes[0], [planted])
    outlier = (baselines[0] == 0) & (baselines[1] == 1)
    data[1, 4, outlier] *= np.exp(.7j)
    data[2, 4, outlier] *= np.exp(-.7j)
    with opened(vis, True) as ms:
        flags = ms.getcol('FLAG')
        flags[:, 4, np.flatnonzero(outlier)[4]] = True
        ms.putcol('FLAG', flags)
        ms.putcol('DATA', data)
    _, actual = solve(root/'extended-baseline-flags', vis, api, 'Xfparang+QU', unit_stokes)
    np.testing.assert_allclose(actual, gains_at_phase, atol=4e-5, rtol=0.)

    for label, task in [('casa', casa.polcal), ('native', api.polcal)]:
        for case, options in [('averaged-interval', dict(preavg=0.)),
                              ('single-integration', dict(solint='int'))]:
            output = root/f'{label}-{case}.cal'
            try:
                task(str(vis), str(output), field='0', poltype='Xfparang+QU',
                     smodel=unit_stokes, **options)
            except RuntimeError:
                assert not output.exists()
            else:
                raise AssertionError(f'{label} accepted {case}')

    compare_degenerate_coverage(root, vis, api, basis, stokes, sky, planted)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    args = parser.parse_args()
    assert casa.version_string() == '6.7.6.14'
    from casars import tasks

    root = args.work.resolve()
    root.mkdir()
    compare_basis(root/'linear', 'XX XY YX YY', tasks)
    compare_basis(root/'circular', 'RR RL LR LL', tasks)


if __name__ == '__main__':
    main()
