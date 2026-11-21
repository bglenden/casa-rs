#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Paired CASA EOP calibration, table interchange and fringe-rate controls."""

import argparse
import os
from pathlib import Path

import casatasks as casa
from casacore.tables import makearrcoldesc, makescacoldesc, maketabdesc, table
import numpy as np

from applycal_oracle import copy_ms
from calibration_smoothcal_oracle import columns, compare_mapping, opened
from record_casa_returns import make_cal_ms


def rms(got, want):
    return float(np.linalg.norm(got - want) / max(1e-30, np.linalg.norm(want)))


def seed(vis):
    make_cal_ms(str(vis), channels=4, frequency='5GHz', stokes='RR RL LR LL', telescope='VLA')
    with opened(vis, True) as main:
        minimum = float(main.getcol('TIME').min()/86400)
        main.putcol('FLAG', np.zeros_like(main.getcol('FLAG')))
        main.putcol('FLAG_ROW', np.zeros_like(main.getcol('FLAG_ROW')))
    day = int(minimum)
    desc = maketabdesc([
        makescacoldesc('TIME', 0.), makescacoldesc('OBSERVATION_ID', 0),
        makearrcoldesc('PM', 0., ndim=1), makescacoldesc('UT1_UTC', 0.),
    ])
    with table(str(vis/'EARTH_ORIENTATION'), desc, nrow=3, ack=False) as eop:
        for row in range(3):
            eop.putcell('TIME', row, (day - 1 + row) * 86400.)
            eop.putcell('OBSERVATION_ID', row, 0)
            eop.putcell('PM', row, np.deg2rad(np.array([.13 + .01*row, .32 - .02*row]) / 3600.))
            eop.putcell('UT1_UTC', row, -.16 + .003*row)
        with table(str(vis), readonly=False, ack=False) as main:
            main.putkeyword('EARTH_ORIENTATION', eop)
    return day


def inputs(work, day):
    values = [(day - 1 + row, .13 + .02*row, .34 - .01*row, -.12 + .004*row)
              for row in range(3)]
    c04 = work/'iers-c04.txt'
    c04.write_text('# Revised IERS rows\n' + ''.join(
        f'2020 01 01  {date:7d} {x:10.6f} {y:10.6f} {ut1:11.7f}\n'
        for date, x, y, ut1 in values))
    products = {'c04': c04, 'installed': None}
    for label, (width, seconds) in (('2.0', (7, 9)), ('2.1', (8, 11))):
        path = work/f'usno-{label}.txt'
        path.write_text(f'EOP-MOD Ver {label}\n' + ''.join(
            f'{date + 2400000.5:9.1f} {10*x:{width}.3f} {10*y:{width}.3f} {(ut1-37)*1e6:{seconds}.0f}\n'
            for date, x, y, ut1 in values))
        products[label] = path
    return products


def compare_calibration(native, reference):
    got, want = columns(native), columns(reference)
    got_values, want_values = got.pop('FPARAM'), want.pop('FPARAM')
    assert got_values.shape == want_values.shape == (8, 1, 20)
    assert not got['FLAG'].any()
    np.testing.assert_array_equal(got['PARAMERR'], -1.)
    np.testing.assert_array_equal(got['WEIGHT'], 1.)
    np.testing.assert_array_equal(got_values[::4], got_values[0:1].repeat(2, axis=0))
    np.testing.assert_array_equal(got_values[3::4], 0.)
    compare_mapping(got, want, False)
    for slots in ([0,4], [1,5], [2,6]):
        error = rms(got_values[slots], want_values[slots])
        assert error < 2e-4, (slots, error, got_values[slots], want_values[slots])
        print(f'Fringe coefficients {slots} normalized RMS {error:.3g}', flush=True)
    with opened(native/'SPECTRAL_WINDOW') as target, opened(reference/'SPECTRAL_WINDOW') as oracle:
        for key in ('CHAN_FREQ', 'CHAN_WIDTH', 'NUM_CHAN', 'REF_FREQUENCY'):
            np.testing.assert_array_equal(target.getcol(key), oracle.getcol(key))
    for path in (native, reference):
        with opened(path) as source:
            assert source.getkeyword('VisCal') == source.info()['subType'] == 'Fringe Jones'
    return got_values


def generate(work, vis, engines, files):
    result = {}
    measured = {}
    for case, source in files.items():
        tables = {}
        for label, api in engines.items():
            cal = work/f'{label}-{case}.cal'
            kwargs = {'infile':str(source)} if source else {}
            assert api.gencal(str(vis), str(cal), 'eop', spw='ignored', pol='ignored',
                              antenna='ignored', parameter=[33.], uniform=False, **kwargs) is None
            tables[label] = cal
        measured[case] = compare_calibration(tables['native'], tables['casa'])
        result[case] = tables
    assert rms(measured['c04'], measured['installed']) > .1
    return result


def apply_and_solve(work, vis, engines, tables):
    references = {}
    for writer, cal in tables.items():
        for reader, api in engines.items():
            ms = work/f'{writer}-{reader}.ms'
            copy_ms(vis, ms)
            assert api.applycal(str(ms), gaintable=[str(cal)], calwt=[True], flagbackup=False) is None
            with opened(ms) as applied, opened(vis) as raw:
                data = applied.getcol('CORRECTED_DATA')
                flags = applied.getcol('FLAG')
                np.testing.assert_array_equal(flags, raw.getcol('FLAG'))
                np.testing.assert_array_equal(applied.getcol('WEIGHT'), raw.getcol('WEIGHT'))
                assert rms(data, raw.getcol('DATA')) > 1e-3
            gain = work/f'{writer}-{reader}-gain.cal'
            result = api.gaincal(str(ms), str(gain), field='PHASE', gaintable=[str(cal)],
                                 solint='int', refant='A0', minsnr=0., minblperant=2)
            assert isinstance(result, dict)
            with opened(gain) as solved:
                solution = solved.getcol('CPARAM')
                assert not solved.getcol('FLAG').any()
            for key, observed in (('corrected', data), ('preapply', solution)):
                if key not in references:
                    references[key] = observed
                    continue
                error = rms(observed, references[key])
                assert error < 2e-4, (writer, reader, key, error)
                print(f'{writer} to {reader} {key} normalized RMS {error:.3g}', flush=True)


def check_rate(work, vis, engines, calibration):
    control = work/'rate-only.cal'
    with opened(calibration) as source:
        copied = source.copy(str(control), deep=True, valuecopy=True)
        copied.close()
    with opened(control, True) as c:
        antennas = c.getcol('ANTENNA1')
        times = c.getcol('TIME')
        gains = np.zeros_like(c.getcol('FPARAM'))
        rate = np.float32(2e-12)
        gains[2, 0, antennas == 0] = rate
        gains[6, 0, antennas == 0] = rate
        c.putcol('FPARAM', gains)
    with opened(control/'SPECTRAL_WINDOW') as spectral:
        frequency = float(spectral.getcell('CHAN_FREQ', 0)[0])
    with opened(vis) as raw:
        left, right = raw.getcol('ANTENNA1'), raw.getcol('ANTENNA2')
        when = raw.getcol('TIME')
        data = raw.getcol('DATA')
    for label, api in engines.items():
        output = work/f'{label}-rate.ms'
        copy_ms(vis, output)
        api.applycal(str(output), gaintable=[str(control)], interp=['nearest'], flagbackup=False)
        with opened(output) as applied:
            corrected = applied.getcol('CORRECTED_DATA')
        solutions = np.unique(times)
        row = int(np.flatnonzero((left == 0) & (right != 0) & (when == solutions[0] + 10.))[0])
        phase = 2*np.pi*float(rate)*frequency*10.
        want = data[0, 0, row] * np.exp(-1j * phase)
        assert abs(want - data[0, 0, row]) > .05
        np.testing.assert_allclose(corrected[0, 0, row], want, atol=4e-6)


def check_publication(work, vis, engines, calibration):
    empty = work/'empty.c04'
    empty.write_text('# no rows\n')
    for label, api in engines.items():
        for case, source in (('empty', empty), ('missing', work/'missing.c04')):
            product = work/f'{label}-{case}-protected.cal'
            with opened(calibration) as cal:
                cal.copy(str(product), deep=True, valuecopy=True).close()
            before = columns(product)
            try:
                assert api.gencal(str(vis), str(product), 'eop', str(source)) is None
            except FileNotFoundError as error:
                assert case == 'missing' and str(source) in str(error)
            else:
                assert case == 'empty'
            if label == 'native':
                compare_mapping(columns(product), before, False)
            else:
                assert not product.exists()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=Path)
    args = parser.parse_args()
    from casars import tasks as native
    assert casa.version_string() == '6.7.6.14'
    root = args.work.resolve()
    root.mkdir()
    vis = root/'seed.ms'
    day = seed(vis)
    engines = {'casa':casa, 'native':native}
    products = generate(root, vis, engines, inputs(root, day))
    apply_and_solve(root, vis, engines, products['c04'])
    check_rate(root, vis, engines, products['c04']['casa'])
    check_publication(root, vis, engines, products['c04']['casa'])


if __name__ == '__main__':
    main()
