#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare the ordinary D and Df solvers with CASA 6.7.6.14."""

import argparse
from pathlib import Path

import casatasks as casa
from casatools import table as table_tool
import numpy as np

from calibration_smoothcal_oracle import opened
from gaincal_delay_oracle import duplicate_rows
from polcal_leakage_solve_oracle import compare_apply, plant, put_observations


def normalized_rms(got, want):
    scale = np.linalg.norm(want)
    return float(np.linalg.norm(got - want) / scale) if scale else float(np.linalg.norm(got))


def compare(reference, native):
    with opened(reference) as want, opened(native) as got:
        assert want.nrows() == got.nrows() > 0
        assert want.info()['subType'] == got.info()['subType']
        for column in ('FIELD_ID', 'SPECTRAL_WINDOW_ID', 'SCAN_NUMBER', 'OBSERVATION_ID',
                       'ANTENNA1', 'ANTENNA2', 'FLAG'):
            np.testing.assert_array_equal(got.getcol(column), want.getcol(column), err_msg=column)
        for column in ('TIME', 'INTERVAL'):
            np.testing.assert_allclose(got.getcol(column), want.getcol(column),
                                       atol=2e-5, rtol=0., err_msg=column)
        valid = ~want.getcol('FLAG')
        gain = normalized_rms(got.getcol('CPARAM')[valid], want.getcol('CPARAM')[valid])
        error = normalized_rms(got.getcol('PARAMERR'), want.getcol('PARAMERR'))
        snr = normalized_rms(got.getcol('SNR'), want.getcol('SNR'))
        assert gain < 1e-3 and error < 1e-3 and snr < 1e-3, (gain, error, snr)
    with opened(reference/'SPECTRAL_WINDOW') as want, opened(native/'SPECTRAL_WINDOW') as got:
        for column in ('NUM_CHAN', 'CHAN_FREQ', 'REF_FREQUENCY', 'FLAG_ROW'):
            np.testing.assert_allclose(got.getcell(column, 0), want.getcell(column, 0),
                                       atol=1e-4, rtol=0., err_msg=column)
    return gain, error, snr


def solve(root, vis, api, stokes, **options):
    root.mkdir()
    outputs = {label: root/f'{label}.cal' for label in ('casa', 'native')}
    params = dict(field='0', refant='A0', smodel=stokes)
    params.update(options)
    for label, task in [('casa', casa.polcal), ('native', api.polcal)]:
        assert task(str(vis), str(outputs[label]), **params) is None
    difference = compare(outputs['casa'], outputs['native'])
    print(root.parent.name, root.name, 'gain, formal error and SNR normalized RMS', difference)
    return outputs


def compare_basis(root, basis, api):
    root.mkdir()
    vis, leak, gains, stokes, sky = plant(root, basis)
    data = put_observations(vis, sky, [leak])
    rng = np.random.default_rng(1522)
    noise = (.002 * (rng.normal(size=data.shape) + 1j*rng.normal(size=data.shape))).astype(np.complex64)
    with opened(vis, True) as ms:
        ms.putcol('DATA', data + noise)
    d = solve(root/'D', vis, api, stokes, poltype='D')
    compare_apply(root/'D', vis, d, api, [])
    df = solve(root/'Df', vis, api, stokes, poltype='Df')
    compare_apply(root/'Df', vis, df, api, [])
    solve(root/'Df-selected-width', vis, api, [], poltype='Df', spw='0:2~6', solint='inf,2chan')
    with opened(d['casa']) as table:
        snr = np.sort(table.getcol('SNR').ravel())
        threshold = float(np.mean(snr[3:5]))
    cut = solve(root/'SNR-cut', vis, api, stokes, poltype='D', minsnr=threshold, refant='')
    with opened(cut['native']) as table:
        flags = table.getcol('FLAG')
        assert flags.any() and not flags.all()

    cut = solve(root/'all-below-SNR', vis, api, stokes, poltype='D', minsnr=1e20)
    with opened(cut['native']) as table:
        assert table.getcol('FLAG').all()
        np.testing.assert_array_equal(table.getcol('ANTENNA2'), -1)
    if basis.startswith('XX'):
        with opened(d['casa']) as table:
            snr = table.getcol('SNR')[:, 0]
        assert snr[0].min() > snr[1].max()
        threshold = float((snr[0].min() + snr[1].max()) / 2.)
        cut = solve(root/'first-receptor-reference', vis, api, stokes, poltype='D', minsnr=threshold)
        with opened(cut['native']) as table:
            assert not table.getcol('FLAG')[0].any()
            assert table.getcol('FLAG')[1].all()
            np.testing.assert_array_equal(table.getcol('ANTENNA2'), 0)

    with opened(vis, True) as ms:
        flags = ms.getcol('FLAG')
        flags[:, 2] = True
        ms.putcol('FLAG', flags)
    missing = solve(root/'missing-channel', vis, api, stokes, poltype='Df')
    with opened(missing['native']) as table:
        assert table.getcol('FLAG')[:, 2].all()
        np.testing.assert_array_equal(table.getcol('SNR')[:, 2], table.getcol('SNR')[:, 3])
    for label, task in [('casa', casa.polcal), ('native', api.polcal)]:
        output = root/f'{label}-insufficient-baselines.cal'
        assert task(str(vis), str(output), field='0', poltype='D',
                    smodel=stokes, minblperant=20) is None
        assert not output.exists()

    with opened(vis, True) as ms:
        left = ms.getcol('ANTENNA1')
        right = ms.getcol('ANTENNA2')
        flags = np.zeros_like(flags)
        flags[:, :, ((left == 4) | (right == 4)) & (left != 0) & (right != 0)] = True
        ms.putcol('FLAG', flags)
    cut = solve(root/'baseline-cut', vis, api, stokes, poltype='D', minblperant=2)
    with opened(cut['native']) as table:
        solution_flags = table.getcol('FLAG')
        assert solution_flags[:, :, table.getcol('ANTENNA1') == 4].all()
        assert not solution_flags[:, :, table.getcol('ANTENNA1') != 4].any()

    with opened(vis, True) as ms:
        flags[:, :, (left == 4) | (right == 4)] = True
        ms.putcol('FLAG', flags)
    solve(root/'zero-Hessian-antenna', vis, api, stokes, poltype='D',
          minblperant=0, preavg=0., refant='')

    with opened(vis, True) as ms:
        ms.putcol('FLAG', np.zeros_like(flags))
    calibrated = put_observations(vis, sky, [gains, leak])
    with opened(vis, True) as ms:
        ms.putcol('DATA', calibrated + noise)
    prior = solve(root/'prior', vis, api, stokes, poltype='D', gaintable=[str(gains)])
    compare_apply(root/'prior', vis, prior, api, [gains])

    if basis.startswith('RR'):
        unpolarized = np.zeros_like(sky)
        unpolarized[::3] = 1.
        data = put_observations(vis, unpolarized, [leak]) + noise
        uncorrupted = put_observations(vis, unpolarized, [])
        data[:, -1] = uncorrupted[:, -1]
        with opened(vis, True) as ms:
            ms.putcol('DATA', data)
        identity = solve(root/'exact-channel', vis, api, [1., 0., 0., 0.], poltype='Df')
        with opened(identity['native']) as table:
            assert not table.getcol('FLAG')[:, -1].any()
            np.testing.assert_array_equal(table.getcol('CPARAM')[:, -1], 0.)
            np.testing.assert_array_equal(table.getcol('SNR')[:, -1], 0.)

        with opened(vis, True) as ms:
            ms.putcol('MODEL_DATA', np.zeros_like(sky))
        for label, task in [('casa', casa.polcal), ('native', api.polcal)]:
            output = root/f'{label}-zero-Hessian-model.cal'
            assert task(str(vis), str(output), field='0', poltype='D') is None
            assert not output.exists()

    if basis.startswith('XX'):
        with opened(vis, True) as ms:
            sigma = ms.getcol('SIGMA')
            times = ms.getcol('TIME')
            sigma[0] = 0.
            ms.putcol('SIGMA', sigma)
            ms.putcol('DATA', data + noise)
        solve(root/'unweighted-parallel-hand', vis, api, stokes, poltype='Df', solint='int',
              msselect=f'abs(TIME-{times.min():.9f}) < 0.001')
        assert duplicate_rows(table_tool(), vis/'OBSERVATION') == 1
        with opened(vis, True) as ms:
            ms.putcol('SIGMA', np.ones_like(sigma))
            fields = ms.getcol('FIELD_ID')
            short = fields == 1
            times[short] = times.min() + 100.
            ms.putcol('TIME', times)
            ms.putcol('TIME_CENTROID', times)
            ms.putcol('OBSERVATION_ID', fields)
            ms.putcol('SCAN_NUMBER', np.zeros_like(fields))
        apparent = put_observations(vis, sky, [leak])
        with opened(vis, True) as ms:
            flags.fill(False)
            flags[:, :, short & ((left == 0) | (right == 0))] = True
            ms.putcol('FLAG', flags)
            ms.putcol('DATA', apparent + noise)
        products = solve(root/'reference-centroid-order', vis, api, stokes, poltype='D', field='',
                         refant='A0,A1', minblperant=2, combine='scan,field')
        with opened(products['native']) as table:
            np.testing.assert_array_equal(table.getcol('ANTENNA2'), 1)


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
