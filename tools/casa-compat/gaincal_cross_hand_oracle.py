#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare the public XY+QU and XYf+QU gaincal modes with CASA 6.7.6.14."""

import argparse
import json
import os
from pathlib import Path
import subprocess

import casatasks as casa
from casatools import table as table_tool
import numpy as np

from applycal_oracle import copy_ms
from calibration_leakage_apply_oracle import compare_application
from calibration_smoothcal_oracle import opened
from gaincal_delay_oracle import duplicate_rows
from polcal_cross_hand_oracle import plant_cross_hand
from polcal_leakage_solve_oracle import compare_apply, plant, put_observations
from polcal_standard_leakage_oracle import compare


def compare_record(want, got):
    assert got.keys() == want.keys()
    for key, value in want.items():
        if isinstance(value, dict):
            compare_record(value, got[key])
        elif key == 'solve_tables':
            assert got[key].shape == value.shape == (1,)
        else:
            assert got[key].dtype.kind == value.dtype.kind
            np.testing.assert_array_equal(got[key], value, err_msg=key)


def solve(root, vis, api, mode, **overrides):
    root.mkdir()
    inputs = dict(field='0', smodel=[1.6, 0., 0., 0.], refant='A3', preavg=300.)
    inputs.update(overrides)
    tables = {label: root/f'{label}.cal' for label in ('casa', 'native')}
    results = {}
    for name, task in (('casa', casa.gaincal), ('native', api.gaincal)):
        results[name] = task(str(vis), str(tables[name]), gaintype=mode, **inputs)
    compare_record(results['casa'], results['native'])
    errors = compare(tables['casa'], tables['native'], gain_tolerance=3e-4, time_tolerance=1e-4)
    with opened(tables['casa']) as want, opened(tables['native']) as got:
        qu = got.getkeyword('QU')['QU']
        np.testing.assert_allclose(qu, want.getkeyword('QU')['QU'], atol=1e-4, rtol=0.)
        np.testing.assert_array_equal(got.getcol('ANTENNA2'), -1)
        gain = got.getcol('CPARAM')
        np.testing.assert_array_equal(gain[1], 1.)
    print(root.parent.name, root.name, 'gain/error/SNR normalized RMS', errors)
    return tables, gain


def rejected_intervals(root, vis, api, linear):
    for label, task in (('casa', casa.gaincal), ('native', api.gaincal)):
        for name, values in (
            ('default', {}),
            ('full-preaverage', {'preavg': 0.}),
            ('integration', {'preavg': 300., 'solint': 'int'}),
        ):
            output = root/f'{label}-{name}.cal'
            try:
                task(str(vis), str(output), field='0', gaintype='XYf+QU', **values)
            except RuntimeError:
                assert not output.exists()
            else:
                raise AssertionError(f'{label} accepted {name}')
    if linear:
        for label, task in (('casa', casa.gaincal), ('native', api.gaincal)):
            output = root/f'{label}-rank-deficient.cal'
            try:
                task(str(vis), str(output), field='0', gaintype='XYf+QU', preavg=3601.,
                     smodel=[1.6, 0., 0., 0.])
            except RuntimeError:
                assert label == 'native' and not output.exists()
            else:
                assert label == 'casa' and output.exists()


def compare_unusable_windows(root, source, api):
    root.mkdir()
    vis = root/'two-windows.ms'
    copy_ms(source, vis)
    tool = table_tool()
    assert duplicate_rows(tool, vis/'SPECTRAL_WINDOW') == 1
    assert duplicate_rows(tool, vis/'DATA_DESCRIPTION') == 1
    with opened(vis/'DATA_DESCRIPTION', True) as description:
        description.putcell('SPECTRAL_WINDOW_ID', 1, 1)
    with opened(vis, True) as table:
        windows = table.getcol('FIELD_ID')
        table.putcol('DATA_DESC_ID', windows)
        flags = np.zeros_like(table.getcol('FLAG'))
        flags[:, :, windows == 1] = True
        table.putcol('FLAG', flags)

    def run(name, selection, writes_table):
        results = {}
        for label, task in (('casa', casa.gaincal), ('native', api.gaincal)):
            output = root/f'{label}-{name}.cal'
            results[label] = task(str(vis), str(output), spw=selection, gaintype='XYf+QU',
                                  preavg=300., smodel=[1.6, 0., 0., 0.])
            assert results[label]['solve_tables'][0] == str(output)
            assert output.exists() == writes_table
            if writes_table:
                with opened(output) as table:
                    assert table.nrows() == 5
                    np.testing.assert_array_equal(table.getcol('SPECTRAL_WINDOW_ID'), 0)
        compare_record(results['casa'], results['native'])

    run('selected-good', '0', True)
    run('both', '0,1', True)
    run('fully-flagged', '1', False)
    with opened(vis, True) as table:
        table.putcol('FLAG', np.zeros_like(flags))
        for name, value in [('SIGMA', np.inf), ('WEIGHT', 0.)]:
            values = table.getcol(name)
            values[:, windows == 1] = value
            table.putcol(name, values)
    run('zero-weight', '1', False)


def compare_single_segment(root, vis, api, reference):
    with opened(vis, True) as table:
        times = table.getcol('TIME')
        flags = table.getcol('FLAG')
        flags[:, 3] = times != times.min()
        table.putcol('FLAG', flags)
    for label, task in (('casa', casa.gaincal), ('native', api.gaincal)):
        output = root/f'{label}-single-segment.cal'
        task(str(vis), str(output), field='0', gaintype='XYf+QU', preavg=300.,
             smodel=[1.6, 0., 0., 0.])
        with opened(output) as table:
            flags = table.getcol('FLAG')
            if label == 'casa':
                assert not flags[:, 3].any()
            else:
                assert flags[:, 3].all()
                phases = table.getcol('CPARAM')
                np.testing.assert_array_equal(phases[:, 3], 1.)
                indices = [0, 1, 2, 4, 5, 6, 7]
                np.testing.assert_allclose(phases[:, indices], reference[:, indices],
                                           atol=3e-5, rtol=0.)


def compare_cli_report(root, vis):
    raw = root/'cli.cal'
    report = root/'cli-report.json'
    arguments = [os.environ['CASARS_CALIBRATE_BIN'], 'gaincal', '--vis', str(vis), '--field', '0',
                 '--gaintype', 'XYf+QU', '--preavg', '300', '--smodel', '1.6,0,0,0',
                 '--format', 'json', '--no-save-last']
    argv = [*arguments, '--caltable', str(raw), '-o', str(report)]
    subprocess.run(argv, check=True, capture_output=True, text=True)
    stored = json.loads(report.read_text())
    assert stored['gain_type'] == 'XYf+QU' and stored['solution_row_count'] == 5
    assert stored['output_table'] == str(raw) and raw.exists()
    blocked = root/'cli-blocked.cal'
    for path in (report, blocked):
        rejected = subprocess.run([*arguments, '--caltable', str(blocked), '-o', str(path)],
                                  capture_output=True, text=True)
        assert rejected.returncode and not blocked.exists()


def compare_basis(root, basis, api):
    root.mkdir()
    vis, leakage, gains, _, sky = plant(root, basis)
    with opened(vis) as table:
        time = table.getcol('TIME')
    phase = root/'planted-phase.cal'
    wanted = plant_cross_hand(vis, phase, 5, time.min())
    put_observations(vis, sky, [phase])

    products, _ = solve(root/'scalar', vis, api, 'XY+QU')
    compare_apply(root/'scalar', vis, products, api, [])
    products, values = solve(root/'spectrum', vis, api, 'XYf+QU',
                             minblperant=50, minsnr=1e20)
    if basis.startswith('XX'):
        np.testing.assert_allclose(values[0], wanted[0], atol=3e-5, rtol=0.)
    compare_apply(root/'spectrum', vis, products, api, [])

    put_observations(vis, sky, [gains, phase])
    products, _ = solve(root/'prior', vis, api, 'XYf+QU', gaintable=[str(gains)],
                        spw='0:2~6', solint='inf,2chan', preavg=1300.)
    compare_apply(root/'prior', vis, products, api, [gains])
    for label, table in products.items():
        diff, _ = compare_application(root/'prior', vis, api, f'calwt-{label}',
                                      [table], True, field='0')
        assert diff < 1e-5

    put_observations(vis, sky, [phase])
    solve(root/'parang', vis, api, 'XYf+QU', parang=True)
    if basis.startswith('XX'):
        solve(root/'amplitude-mode', vis, api, 'XYf+QU', calmode='a', solnorm=True)
        solve(root/'phase-mode', vis, api, 'XYf+QU', calmode='p')
        solve(root/'one-baseline', vis, api, 'XYf+QU', antenna='A0&A1')
        compare_unusable_windows(root/'unusable-windows', vis, api)
        compare_cli_report(root, vis)
    with opened(vis, True) as table:
        flags = table.getcol('FLAG')
        flags[1, 3, ::6] = True
        table.putcol('FLAG', flags)
    solve(root/'dependent-flags', vis, api, 'XYf+QU', corrdepflags=True)
    rejected_intervals(root, vis, api, basis.startswith('XX'))
    if basis.startswith('XX'):
        compare_single_segment(root, vis, api, values)


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
