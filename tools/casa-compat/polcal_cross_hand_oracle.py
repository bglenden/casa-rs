#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare the X Jones cross-hand phase solve and application with pinned CASA."""

import argparse
from pathlib import Path

import casatasks as casa
import numpy as np

from calibration_leakage_apply_oracle import CHANNELS, compare_application, make_table
from calibration_smoothcal_oracle import opened
from polcal_leakage_solve_oracle import compare_apply, plant, put_observations
from polcal_standard_leakage_oracle import solve
from record_casa_returns import make_cal_ms


def plant_cross_hand(vis, output, antennas, epoch):
    make_table(vis, output, 'Xf Jones', np.ones((antennas, 1), np.complex64), epoch)
    phase = np.linspace(.18, .92, CHANNELS, dtype=np.float32)
    terms = np.broadcast_to(np.exp(1j * phase)[None, :, None], (1, CHANNELS, antennas)).copy()
    with opened(output, True) as table:
        table.putcol('CPARAM', terms)
        table.putcol('FLAG', np.zeros(terms.shape, bool))
        table.putcol('PARAMERR', np.zeros(terms.shape, np.float32))
        table.putcol('SNR', np.full(terms.shape, 10., np.float32))
    with opened(vis/'SPECTRAL_WINDOW') as ms, opened(output/'SPECTRAL_WINDOW', True) as table:
        for name in ('NUM_CHAN', 'CHAN_FREQ', 'CHAN_WIDTH', 'EFFECTIVE_BW', 'RESOLUTION'):
            table.putcell(name, 0, ms.getcell(name, 0))
    return terms


def compare_single_correlation(root, api):
    root.mkdir()
    vis = root/'single-correlation.ms'
    make_cal_ms(str(vis), stokes='RR')
    with opened(vis/'POLARIZATION', True) as polarization:
        polarization.putcell('CORR_TYPE', 0, np.array([8], np.int32))
        polarization.putcell('CORR_PRODUCT', 0, np.ones((2, 1), np.int32))
    with opened(vis) as ms:
        epoch = float(ms.getcol('TIME').min())
        raw = ms.getcol('DATA')
    table = root/'antenna-phases.cal'
    terms = plant_cross_hand(vis, table, 5, epoch)
    terms *= np.exp(0.33j * np.arange(5))
    with opened(table, True) as calibration:
        calibration.putcol('CPARAM', terms)
    difference, corrected = compare_application(root, vis, api, 'LL', [table], False)
    assert difference < 1e-5
    assert np.linalg.norm(corrected-raw) > .2 * np.linalg.norm(raw)


def compare_basis(root, basis, api):
    root.mkdir()
    vis, leak, gains, stokes, sky = plant(root, basis)
    with opened(vis) as ms:
        epoch = float(ms.getcol('TIME').min())
    with opened(vis/'ANTENNA') as antennas:
        count = antennas.nrows()
    planted = root/'planted-x.cal'
    expected = plant_cross_hand(vis, planted, count, epoch)
    put_observations(vis, sky, [planted])
    scalar = solve(root/'Xj', vis, api, stokes, poltype='Xj', refant='A3',
                   minsnr=1e20, minblperant=20, time_tolerance=1e-4)
    compare_apply(root/'Xj', vis, scalar, api, [])
    spectrum = solve(root/'Xf', vis, api, [], poltype='Xf', time_tolerance=1e-4)
    with opened(spectrum['native']) as table:
        np.testing.assert_array_equal(table.getcol('ANTENNA2'), -1)
        np.testing.assert_allclose(table.getcol('CPARAM'), expected, atol=2e-5, rtol=0.)
    compare_apply(root/'Xf', vis, spectrum, api, [])
    for label, product in spectrum.items():
        difference, _ = compare_application(root/'Xf', vis, api, f'calwt-{label}', [product],
                                            True, parang=True, field='0')
        assert difference < 1e-5

    put_observations(vis, sky, [gains, leak, planted])
    products = solve(root/'selected-priors', vis, api, stokes, poltype='Xf',
                     gaintable=[str(gains), str(leak)], spw='0:2~6', solint='inf,2chan',
                     time_tolerance=1e-4)
    compare_apply(root/'selected-priors', vis, products, api, [gains, leak])

    with opened(vis) as ms:
        times, fields = ms.getcol('TIME'), ms.getcol('FIELD_ID')
    flux = sky.copy()
    flux[:, 0, times > np.median(times[fields == 0])] *= 3
    put_observations(vis, flux, [planted])
    solve(root/'preaveraged-centroid', vis, api, [], poltype='Xf', preavg=0.,
          time_tolerance=1e-4)

    data = put_observations(vis, sky, [planted])
    with opened(vis, True) as ms:
        data[1:3, 3] = 0.
        flags = ms.getcol('FLAG')
        flags[1, 4] = True
        ms.putcol('DATA', data)
        ms.putcol('FLAG', flags)
    flagged = solve(root/'missing-cross-hands', vis, api, stokes, poltype='Xf',
                    time_tolerance=1e-4)
    with opened(flagged['native']) as table:
        flags = table.getcol('FLAG')
        assert flags[:, 3:5].all() and not flags[:, [0, 1, 2, 5, 6, 7]].any()
        np.testing.assert_array_equal(table.getcol('CPARAM')[:, 3:5], 1.)

    for label, task in [('casa', casa.polcal), ('native', api.polcal)]:
        for case, options in (
            ('legacy-mueller', dict(poltype='X')),
            ('invalid-reference', dict(poltype='Xf', refant='JUNK')),
        ):
            output = root/f'{label}-{case}.cal'
            try:
                task(str(vis), str(output), field='0', smodel=stokes, **options)
            except RuntimeError:
                assert not output.exists()
            else:
                raise AssertionError(f'{label} accepted {case}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    args = parser.parse_args()
    assert casa.version_string() == '6.7.6.14'
    from casars import tasks

    root = args.work.resolve()
    root.mkdir()
    compare_basis(root/'circular', 'RR RL LR LL', tasks)
    compare_basis(root/'linear', 'XX XY YX YY', tasks)
    compare_single_correlation(root/'single-correlation', tasks)


if __name__ == '__main__':
    main()
