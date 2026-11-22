#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare pulse and cable calibration with CASA 6.7.6.14."""

import argparse
from contextlib import chdir
from datetime import datetime, timedelta, timezone
from pathlib import Path
import shutil
import time

import casatasks as casa
from casatools import table as casa_table
import numpy as np

from calibration_smoothcal_oracle import opened
from fringefit_oracle import compare_consumers, copy_selected_rows, source


def run(root, label, vis, tasks, **options):
    products, records = {}, {}
    for engine, task in tasks.items():
        out = root / f'{label}-{engine}.pccor'
        start = time.perf_counter()
        records[engine] = task(str(vis), str(out), **options)
        print(label, engine, f'{time.perf_counter() - start:.3f}s', flush=True)
        products[engine] = out
    return products, records


def compare_records(reference, actual, fitted=False):
    assert list(actual) == list(reference)
    for antenna, ref_windows in reference.items():
        assert list(actual[antenna]) == list(ref_windows)
        for window, ref_values in ref_windows.items():
            probe = actual[antenna][window]
            assert list(probe) == list(ref_values)
            for name, expected in ref_values.items():
                values = probe[name]
                assert type(values) is np.ndarray
                assert (values.dtype, values.shape) == (expected.dtype, expected.shape), (
                    antenna, window, name, values.dtype, values.shape, expected.dtype, expected.shape)
                rtol = 0.
                if name in ('phase', 'ff_phase'):
                    diff = np.angle(np.exp(1j * (values - expected)))
                    np.testing.assert_allclose(diff, 0., atol=2e-3 if fitted else 2e-7,
                                               err_msg=f'{antenna}:{window}:{name}')
                    continue
                if name in ('delay', 'ff_delay'):
                    atol = 2e-11 if fitted else 2e-15
                elif name in ('pc_mean_delay', 'pc_refant_delay'):
                    atol = 1e-18
                elif name in ('pc_mean_phase', 'pc_refant_phase'):
                    atol = 1e-12
                elif name == 'time' and fitted:
                    atol = 2e-5
                else:
                    atol = 0.
                np.testing.assert_allclose(values, expected, atol=atol, rtol=rtol,
                                           err_msg=f'{antenna}:{window}:{name}')


def compare_tables(reference, actual, fitted=False):
    with opened(reference) as left, opened(actual) as right:
        assert left.info() == right.info()
        assert left.nrows() == right.nrows()
        assert sorted(left.colnames()) == sorted(right.colnames())
        assert left.getkeyword('VisCal') == right.getkeyword('VisCal') == 'Fringe Jones'
        assert left.getkeyword('ParType') == right.getkeyword('ParType') == 'Float'
        for name in left.colnames():
            assert left.iscelldefined(name, 0) == right.iscelldefined(name, 0)
            if not left.iscelldefined(name, 0):
                assert name == 'PARAMERR'
                continue
            lhs, rhs = left.getcol(name), right.getcol(name)
            if name == 'FPARAM':
                diff = np.angle(np.exp(1j * (rhs[0::4] - lhs[0::4])))
                np.testing.assert_allclose(diff, 0., atol=2e-3 if fitted else 5e-7)
                np.testing.assert_allclose(rhs[1::4], lhs[1::4], rtol=0.,
                                           atol=.02 if fitted else 2e-5)
                np.testing.assert_array_equal(rhs[2::4], 0.)
                np.testing.assert_array_equal(rhs[3::4], 0.)
            elif name == 'TIME' and fitted:
                np.testing.assert_allclose(rhs, lhs, rtol=0., atol=2e-5)
            else:
                np.testing.assert_array_equal(rhs, lhs, err_msg=name)
    with opened(reference / 'SPECTRAL_WINDOW') as left, opened(actual / 'SPECTRAL_WINDOW') as right:
        for name in ('CHAN_FREQ', 'REF_FREQUENCY', 'CHAN_WIDTH', 'EFFECTIVE_BW', 'RESOLUTION',
                     'TOTAL_BANDWIDTH', 'FLAG_ROW', 'NUM_CHAN'):
            np.testing.assert_array_equal(right.getcol(name), left.getcol(name), err_msg=name)


def compare(products, records, fitted=False):
    if 'native' in records:
        compare_records(records['casa'], records['native'], fitted)
        compare_tables(products['casa'], products['native'], fitted)


def write_tones(vis, times, tone_count=2):
    types = dict(ANTENNA_ID='int', SPECTRAL_WINDOW_ID='int', TIME='double', CABLE_CAL='double',
                 TONE_FREQUENCY='double', PHASE_CAL='dcomplex')
    columns = {
        name: dict(valueType=kind, dataManagerType='StandardStMan', dataManagerGroup='StandardStMan',
                   option=0, maxlen=0, comment='', keywords={})
        for name, kind in types.items()
    }
    for key in ('TONE_FREQUENCY', 'PHASE_CAL'):
        columns[key].update(ndim=2)
        if isinstance(tone_count, int):
            columns[key].update(option=4, shape=np.array([2, tone_count], dtype=np.int32))
    proc_rows = [(ant, tick, when) for ant in range(4) for tick, when in enumerate(times)]
    tone_frequencies = np.array([[8.4e9, 8.408e9], [8.4e9, 8.408e9]])
    phases, delays, cables, phasors = {}, {}, {}, {}
    path = vis / 'PHASE_CAL'
    tool = casa_table()
    tool.create(str(path), columns, nrow=len(proc_rows))
    try:
        for row, (ant, tick, when) in enumerate(proc_rows):
            count = tone_count if isinstance(tone_count, int) else tone_count[ant]
            first = np.array([.4, -.7]) + (.21 + .02 * ant) * tick
            delay = (2.2 + .8 * ant + .25 * tick + np.array([0., .4])) * 1e-9
            phase = np.column_stack((first, first + 2. * np.pi * 8e6 * delay))
            phasor = (1. + .6 * tick) * np.exp(1j * phase)
            cable = (.1 + .3 * tick + .2 * ant) * 1e-9
            for name, value in zip(types, (ant, 0, when, cable, tone_frequencies[:, :count],
                                           phasor[:, :count]),
                                   strict=True):
                tool.putcell(name, row, value)
            phases[ant, tick], delays[ant, tick] = first, delay
            cables[ant, tick], phasors[ant, tick] = cable, phasor
    finally:
        tool.close()
    with opened(vis, True) as table:
        table.putkeyword('PHASE_CAL', f'Table: {path}')
    return phases, delays, cables, phasors


def check_analytic(root, tasks):
    vis, _ = source(root)
    with opened(vis) as ms:
        scan = int(ms.getcell('SCAN_NUMBER', 0))
        first = ms.getcell('TIME', 0)
    times = first + np.array([0., 16., 38.])
    phases, delays, cables, phasors = write_tones(vis, times)
    fringe = root / 'analytic-fringe.cal'
    casa.fringefit(str(vis), str(fringe), field='FLUX', scan=str(scan), refant='A0',
                  minsnr=0., zerorates=True)
    products, records = run(root, 'analytic', vis, tasks, refant='A0', scan=str(scan),
                            ff_table=str(fringe), cablecal_correction=True)
    reference = records['casa']
    for engine, record in records.items():
        for ant in range(4):
            out = record[f'ant_A{ant}']['spw_0']
            mean_phase = np.angle(np.mean([phasors[ant, tick] for tick in range(3)], axis=0))
            mean_delay = np.mean([delays[ant, tick] for tick in range(3)], axis=0)
            reference_delay = np.mean([delays[0, tick] for tick in range(3)], axis=0)
            np.testing.assert_allclose(out['pc_mean_phase'], mean_phase, atol=1e-12)
            np.testing.assert_allclose(out['pc_mean_delay'], mean_delay, atol=1e-18)
            np.testing.assert_array_equal(out['time'], times)
            for tick in range(3):
                expected_phase = phases[ant, tick] + out['ff_phase'] - mean_phase[:, 0]
                expected_delay = (delays[ant, tick] + out['ff_delay'] -
                                  (mean_delay - reference_delay) + cables[ant, tick])
                np.testing.assert_allclose(
                    np.angle(np.exp(1j * (out['phase'][:, tick] - expected_phase))), 0., atol=2e-7)
                np.testing.assert_allclose(out['delay'][:, tick], expected_delay, atol=1e-18)
        fallback = record['ant_A4']['spw_0']
        assert fallback['time'].shape == (1,)
        np.testing.assert_array_equal(fallback['delay'][:, 0], fallback['ff_delay'])
        np.testing.assert_array_equal(fallback['phase'][:, 0], fallback['ff_phase'])
        assert list(record) == list(reference)
    compare(products, records)
    missing = root / 'missing-tones.ms'
    shutil.copytree(vis, missing, ignore=shutil.ignore_patterns('PHASE_CAL'))
    different_ref = root / 'different-ref.cal'
    shutil.copytree(fringe, different_ref)
    with opened(different_ref, True) as table:
        table.putcol('ANTENNA2', np.ones(table.nrows(), dtype=np.int32))
    duplicate = root / 'duplicate-fringe.cal'
    shutil.copytree(fringe, duplicate)
    with opened(duplicate, True) as table:
        # Duplicate one integration row without changing the overall time selection.
        index = table.nrows()
        table.addrows(1)
        for column in table.colnames():
            if table.iscelldefined(column, 0):
                table.putcell(column, index, table.getcell(column, 0))
    for engine, task in tasks.items():
        for dataset, kwargs, kind, message in [
            (vis, {}, ValueError, 'Scan and timerange cannot be both undefined'),
            (vis, {'scan': 'invalid'}, ValueError, 'valid single scan number'),
            (vis, {'scan': 1}, AssertionError, 'must be of cStr type'),
            (vis, {'scan': str(scan), 'spw': 0}, AssertionError, 'must be of cStr type'),
            (vis, {'scan': str(scan), 'cablecal_correction': 1}, AssertionError, 'must be of cBool type'),
            (vis, {'scan': str(scan), 'ff_table': None}, AssertionError, 'null value not allowed'),
            (vis, {'scan': str(scan), 'spw': '!0, 0'}, ValueError, 'Mixing of SPWs'),
            (vis, {'scan': str(scan), 'antenna': '!A0'}, RuntimeError, 'Reference antenna must be included'),
            (missing, {'scan': str(scan)}, RuntimeError, 'does not contain a PHASE_CAL subtable'),
            (vis, {'scan': str(scan), 'ff_table': str(different_ref)}, Exception, 'Different reference antenna'),
            (vis, {'scan': str(scan), 'ff_table': str(duplicate)}, Exception, 'Expected a single fringe fit'),
        ]:
            bad = root / f'{engine}-invalid.pccor'
            try:
                task(str(dataset), str(bad), refant='A0', **kwargs)
            except kind as error:
                assert type(error) is kind
                assert message in str(error), str(error)
                assert not bad.exists()
            else:
                raise AssertionError(f'{engine} accepted {kwargs}')
    check_review_edges(root, vis, fringe, times, scan, tasks)


def check_review_edges(root, vis, fringe, times, scan, tasks):
    options = dict(refant='A0', ff_table=str(fringe))
    epoch = datetime(1858, 11, 17, tzinfo=timezone.utc)
    first = epoch + timedelta(seconds=float(times[0]))
    end = first + timedelta(seconds=120)
    short = f'{first:%H:%M}~{end:%H:%M}'
    subsecond = first + timedelta(microseconds=100)
    for label, selectors, row_count in [
        ('repeated-windows', dict(spw='0,0', scan=str(scan)), 26),
        ('repeated-antennas', dict(antenna='A0,A1,A0', scan=str(scan)), 9),
        ('repeated-range-marker', dict(spw='0~0~0', scan=str(scan)), 13),
        ('hour-and-minute', dict(timerange=short), 13),
        ('subsecond-boundary', dict(timerange=f'{subsecond:%H:%M:%S.%f}~{end:%H:%M:%S}'), 13),
        ('named-month', dict(timerange=f'{first:%Y-%b-%d/%H:%M:00}~{end:%Y-%b-%d/%H:%M:00}'), 13),
    ]:
        products, records = run(root, label, vis, tasks, **(options | selectors))
        for path in products.values():
            with opened(path) as table:
                assert table.nrows() == row_count
        if label == 'subsecond-boundary':
            for result in records.values():
                assert result['ant_A4']['spw_0']['pc_mean_phase'].shape == (2, 1)
        compare(products, records)

    one_tone = root / 'one-tone.ms'
    shutil.copytree(vis, one_tone, ignore=shutil.ignore_patterns('PHASE_CAL'))
    write_tones(one_tone, times, 1)
    products, records = run(root, 'one-tone', one_tone, tasks, **options, scan=str(scan))
    for result in records.values():
        fallback = result['ant_A4']['spw_0']
        np.testing.assert_array_equal(fallback['time'], [times[0]])
        assert np.isnan(fallback['delay']).all()
        assert np.isnan(fallback['pc_mean_delay']).all()
    compare(products, records)

    variable = root / 'variable-tones.ms'
    shutil.copytree(vis, variable, ignore=shutil.ignore_patterns('PHASE_CAL'))
    write_tones(variable, times, [2, 2, 2, 1])
    missing_reference = root / 'reference-outside-interval.ms'
    shutil.copytree(vis, missing_reference)
    with opened(missing_reference / 'PHASE_CAL', True) as table:
        timestamps = table.getcol('TIME')
        timestamps[table.getcol('ANTENNA_ID') == 0] = times[0] - 10
        table.putcol('TIME', timestamps)
    missing_window = root / 'fringe-without-spectral-window.cal'
    shutil.copytree(fringe, missing_window, ignore=shutil.ignore_patterns('SPECTRAL_WINDOW'))
    empty_fringe = root / 'empty-fringe.cal'
    shutil.copytree(fringe, empty_fringe)
    with opened(empty_fringe, True) as table:
        table.removerows(list(range(table.nrows())))
    invalid = [
        ('missing-window', vis, dict(ff_table=str(missing_window)), RuntimeError, ''),
        ('empty-fringe', vis, dict(ff_table=str(empty_fringe)), IndexError, 'out of bounds'),
        ('bracketed-time', vis, dict(timerange=f'[{short}]'), ValueError, 'Start time in timerange'),
        ('empty-exclusion', vis, dict(spw='!0'), IndexError, 'list index out of range'),
        ('empty-range', vis, dict(spw='2~1'), IndexError, 'list index out of range'),
        ('variable-tones', variable, dict(antenna='A0,A1'), RuntimeError, 'does not contain a PHASE_CAL'),
        ('one-tone-cable', one_tone, dict(cablecal_correction=True), ValueError, 'broadcast shape'),
        ('path-fringe', vis, dict(ff_table=fringe), AssertionError, 'must be of cStr type'),
        ('path-caltable', vis, dict(pccor_caltable=root / 'not-written.pccor'), AssertionError,
         'must be of cStr type'),
        ('path-vis', vis, dict(vis=vis), AssertionError, 'the vis parameter must be a path'),
        ('null-vis', vis, dict(vis=None), AssertionError, 'null value not allowed'),
    ]
    for engine, task in tasks.items():
        for label, dataset, selectors, kind, message in invalid:
            bad = root / f'{engine}-{label}.pccor'
            kwargs = dict(vis=str(dataset), pccor_caltable=str(bad), scan=str(scan)) | options | selectors
            try:
                task(**kwargs)
            except kind as error:
                assert type(error) is kind and message in str(error), (label, error)
                if engine == 'native' or not label.startswith('empty-') or label == 'empty-fringe':
                    assert not bad.exists()
                else:
                    with opened(bad) as table:
                        assert table.nrows() == 0
            else:
                raise AssertionError(f'{engine} accepted {label}')
        try:
            task(str(missing_reference), str(root / f'{engine}-no-ref.pccor'), **options, scan=str(scan))
        except (RuntimeError, UnboundLocalError) as error:
            assert type(error) is (UnboundLocalError if engine == 'casa' else RuntimeError)
        else:
            raise AssertionError(f'{engine} accepted a reference without in-interval pulses')
    if 'native' in tasks:
        destination = root / 'analytic-native.pccor'
        try:
            tasks['native'](str(vis), str(destination), **options, scan=str(scan), spw='!0')
        except IndexError:
            with opened(destination) as table:
                assert table.nrows() == 13
        else:
            raise AssertionError('Native PCCOR replaced a product after an empty selection')


def check_archive(root, recorded, tasks, appliers):
    vis = root / recorded.name
    shutil.copytree(recorded, vis)
    selection = dict(refant='LA', scan='177', timerange='2011/09/16/21:36:00~21:37:00')
    products, records = run(root, 'archive', vis, tasks, **selection)
    compare(products, records, fitted=True)
    with opened(products['casa']) as table:
        assert table.nrows() == 38048
    # The native solve overwrites the CASA-generated .mpc file in this work
    # directory. Use CASA's independently generated table for supplied input.
    fringe = root / 'archive-reference.mpc'
    casa.fringefit(str(vis), str(fringe), **selection, minsnr=3., zerorates=True,
                  delaywindow=[-1e6, 1e6], ratewindow=[-1e6, 1e6])
    supplied = dict(refant='LA', timerange='21:36:00~21:37:00', ff_table=str(fringe))
    for label, options in [
        ('positive-filters', dict(spw='0~2', antenna='LA,PT', cablecal_correction=True)),
        ('negative-filters', dict(spw='!3', antenna='!BR', cablecal_correction=False)),
    ]:
        compare(*run(root, label, vis, tasks, **(supplied | options)))
    with opened(vis) as ms:
        scans = ms.getcol('SCAN_NUMBER')
        mask = scans == 177
        field, = np.unique(ms.getcol('FIELD_ID')[mask])
    application = root / 'archive-apply.ms'
    copy_selected_rows(vis, application, mask)
    compare_consumers(root, application, products, appliers, 'archive', str(field), field,
                      interp=['linear'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=Path)
    parser.add_argument('--recorded', type=Path)
    parser.add_argument('--reference-only', action='store_true')
    args = parser.parse_args()
    assert casa.version_string() == '6.7.6.14'
    root = args.work.resolve()
    root.mkdir()
    tasks, appliers = {'casa': casa.pccor}, {'casa': casa.applycal}
    if not args.reference_only:
        from casars.tasks import applycal, pccor
        tasks['native'], appliers['native'] = pccor, applycal
    with chdir(root):
        check_analytic(root, tasks)
        if args.recorded:
            check_archive(root, args.recorded.resolve(), tasks, appliers)
    print('PCCOR pulse, cable, fringe and table checks passed.', flush=True)


if __name__ == '__main__':
    main()
