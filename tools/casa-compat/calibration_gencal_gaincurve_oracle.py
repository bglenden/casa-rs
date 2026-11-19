#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare VLA gencal curves and efficiency with pinned CASA on a real MS."""

import argparse
import json
import os
from pathlib import Path
import shutil

import casatasks as casa
from casatools import table
import numpy as np

from applycal_oracle import copy_ms
from calibration_gencal_specify_oracle import (
    apply_both, columns, compare_tables, generate, with_second_window,
)


def retune(vis, window, frequency):
    tool = table()
    assert tool.open(str(vis / 'SPECTRAL_WINDOW'), nomodify=False)
    try:
        channels = tool.getcell('CHAN_FREQ', window)
        tool.putcell('CHAN_FREQ', window, channels + frequency - channels[len(channels) // 2])
        tool.putcell('REF_FREQUENCY', window, frequency)
    finally:
        tool.close()


def attach_power_curves(vis, raw, *, destination=None, missing_antenna=None):
    tool = table()
    assert tool.open(str(vis / 'ANTENNA'))
    antenna_count = tool.nrows()
    tool.close()
    times = columns(vis, ('TIME',))['TIME']
    midpoint = float(times.min() + times.max()) / 2.
    duration = float(times.max() - times.min()) + 2.
    types = ('POWER(ZA)', 'POWER(EL)', 'VOLTAGE(ZA)', 'VOLTAGE(EL)')
    raw.write_text(''.join(
        f'{midpoint} {duration} {antenna} 0 {types[antenna % 4]} 2 1.1 1.2 .001 .002 0 0 0 0 1.3 1.5\n'
        for antenna in range(antenna_count) if antenna != missing_antenna
    ))
    path = destination if destination is not None else vis / 'GAIN_CURVE'
    assert tool.fromascii(
        str(path), str(raw), sep=' ',
        columnnames=['TIME', 'INTERVAL', 'ANTENNA_ID', 'SPECTRAL_WINDOW_ID', 'TYPE',
                     'NUM_POLY', 'GAIN', 'SENSITIVITY'],
        datatypes=['D', 'D', 'I', 'I', 'A', 'I', 'R2,4', 'R2'],
    )
    tool.close()
    assert tool.open(str(vis), nomodify=False)
    try:
        tool.putkeyword('GAIN_CURVE', f'Table: {path}')
    finally:
        tool.close()


def compare_case(root, case, vis, kind, options, native, check_apply=False):
    output = root / case
    output.mkdir()
    reference, candidate = output / 'casa.cal', output / 'native.cal'
    for module, path in ((casa, reference), (native, candidate)):
        generate(module, vis, path, [(kind, options)])
    result = {'table_nrms': compare_tables(candidate, reference)}
    if check_apply:
        result['apply_nrms'] = apply_both(output, vis, reference, candidate, native)
    values = columns(reference, ('SPECTRAL_WINDOW_ID', 'FPARAM'))
    return result, values


def refuses(root, case, vis, kind, options, native):
    for module in (casa, native):
        out = root / f'{case}-{module.__name__}.cal'
        try:
            generate(module, vis, out, [(kind, options)])
        except RuntimeError:
            assert not out.exists()
        else:
            raise AssertionError(f'{module.__name__} accepted {case}')
    return {'refused': True}


def protects_prior_input(root, vis, source, native, kind):
    input_file = source / 'table.dat'
    expected = input_file.read_bytes()
    caltable = root / 'must-not-publish.cal'
    for output_table, options in ((caltable, {'output': str(input_file), 'overwrite': True}),
                                  (source / 'inside.cal', {})):
        try:
            generate(native, vis, output_table, [(kind, options)])
        except (ValueError, RuntimeError) as error:
            assert 'overlap' in str(error)
        else:
            raise AssertionError('gencal accepted an output inside its prior input')
        assert not output_table.exists()
        assert input_file.read_bytes() == expected
    return {'refused': True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', required=True, type=Path, help='One-window NGC 5921 VLA MS')
    parser.add_argument('--gaincurves', required=True, type=Path, help='CASA runtime VLA GainCurves table')
    parser.add_argument('--work', required=True, type=Path)
    parser.add_argument('--native-bin', required=True, type=Path)
    args = parser.parse_args()
    assert casa.version_string() == '6.7.6.14'
    os.environ['CASARS_CALIBRATE_BIN'] = str(args.native_bin.resolve())
    os.environ['CASARS_REQUIRE_TASK_BINARIES'] = '1'
    os.environ['CASA_RS_MEASURESPATH'] = str(args.gaincurves.resolve().parents[2])
    from casars import tasks as native

    root = args.work.resolve()
    root.mkdir(parents=True, exist_ok=True)
    assert not any(root.iterdir()), 'work directory must be empty'
    source = root / args.source.name
    copy_ms(args.source.resolve(), source)
    curves = str(args.gaincurves.resolve())
    results, default = {}, {}
    for kind in ('gc', 'gceff', 'eff'):
        results[kind], values = compare_case(root, kind, source, kind, {}, native, check_apply=True)
        default[kind] = values['FPARAM']
    factor = default['eff'][0, 0, 0]
    assert 0 < factor < 1
    np.testing.assert_array_equal(default['gceff'], default['gc'].astype(np.float32) * np.float32(factor))
    np.testing.assert_array_equal(default['eff'][1:4], 0.)
    np.testing.assert_array_equal(default['eff'][5:8], 0.)

    for kind in ('gc', 'gceff', 'eff'):
        explicit_case = f'{kind}-explicit-file'
        results[explicit_case], explicit = compare_case(root, explicit_case, source, kind,
                                                        {'infile': curves}, native)
        if kind == 'eff':
            np.testing.assert_array_equal(default[kind], explicit['FPARAM'])
        else:
            assert not np.array_equal(default[kind], explicit['FPARAM'])
        missing_case = f'{kind}-missing-file'
        results[missing_case] = refuses(root, missing_case, source, kind,
                                       {'infile': str(root / 'absent')}, native)
    missing_band = root / 'curves-with-no-bandname.tbl'
    shutil.copytree(args.gaincurves.resolve(), missing_band)
    tool = table()
    assert tool.open(str(missing_band), nomodify=False)
    try:
        tool.removecols(['BANDNAME'])
    finally:
        tool.close()
    for kind in ('gc', 'gceff'):
        case = f'{kind}-missing-bandname'
        results[case] = refuses(root, case, source, kind, {'infile': str(missing_band)}, native)
    results['eff-missing-bandname'], _ = compare_case(root, 'eff-missing-bandname', source, 'eff',
                                                     {'infile': str(missing_band)}, native)
    results['inactive-selectors'], selected = compare_case(
        root, 'inactive-selectors', source, 'gc',
        {'spw': '0', 'antenna': '1', 'pol': 'Y', 'uniform': False, 'parameter': [1.7]}, native)
    np.testing.assert_array_equal(selected['FPARAM'], default['gc'])
    for label, frequency in (('below', .5e9), ('above', 55e9)):
        vis = root / f'{label}.ms'
        copy_ms(source, vis)
        retune(vis, 0, frequency)
        results[f'eff-{label}'], _ = compare_case(root, f'eff-{label}', vis, 'eff', {}, native)

    vis = root / 'one-unmatched-window.ms'
    copy_ms(source, vis)
    with_second_window(vis)
    retune(vis, 1, 100e9)
    results['unmatched-window'], partial = compare_case(root, 'unmatched-window', vis, 'gc', {}, native)
    missing = partial['FPARAM'][..., partial['SPECTRAL_WINDOW_ID'] == 1]
    expected = np.zeros_like(missing)
    expected[[0, 4]] = 1.
    np.testing.assert_array_equal(missing, expected)
    unmatched = root / 'unmatched.ms'
    copy_ms(source, unmatched)
    retune(unmatched, 0, 100e9)
    results['all-windows-unmatched'] = refuses(root, 'all-windows-unmatched', unmatched, 'gc', {}, native)

    for module in (casa, native):
        path = root / f'overwrite-{module.__name__}.cal'
        generate(module, source, path, [('gceff', {}), ('gc', {})])
        compare_tables(path, root / 'gc' / 'casa.cal')
    results['absolute-overwrite'] = {'table_nrms': 0.0}

    embedded = root / 'embedded.ms'
    copy_ms(source, embedded)
    attach_power_curves(embedded, root / 'embedded.txt')
    results['embedded-gc'], power = compare_case(root, 'embedded-gc', embedded, 'gc', {},
                                                native, check_apply=True)
    unweighted = root / 'embedded-gc-without-calwt'
    unweighted.mkdir()
    results['embedded-gc-without-calwt'] = {'apply_nrms': apply_both(
        unweighted, embedded, root / 'embedded-gc' / 'casa.cal', root / 'embedded-gc' / 'native.cal',
        native, calwt=False)}
    results['embedded-gceff'], combined = compare_case(root, 'embedded-gceff', embedded, 'gceff',
                                                     {'infile': str(root / 'absent')}, native)
    np.testing.assert_array_equal(combined['FPARAM'], power['FPARAM'])
    assert power['FPARAM'].shape[0] == 16
    results['embedded-eff'], flat = compare_case(root, 'embedded-eff', embedded, 'eff', {}, native)
    np.testing.assert_array_equal(flat['FPARAM'], default['eff'])

    sparse = root / 'sparse.ms'
    external = root / 'sparse.tbl'
    copy_ms(source, sparse)
    attach_power_curves(sparse, root / 'sparse.txt', destination=external, missing_antenna=1)
    assert b'./sparse.tbl' in (sparse / 'table.dat').read_bytes()
    results['sparse-external'], _ = compare_case(root, 'sparse-external', sparse, 'gc', {}, native,
                                                 check_apply=True)
    sparse_unweighted = root / 'sparse-external-without-calwt'
    sparse_unweighted.mkdir()
    results['sparse-external-without-calwt'] = {'apply_nrms': apply_both(
        sparse_unweighted, sparse, root / 'sparse-external' / 'casa.cal',
        root / 'sparse-external' / 'native.cal', native, calwt=False)}
    results['external-input-protection'] = protects_prior_input(root, sparse, external, native, 'gc')

    (root / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
    print(json.dumps(results, indent=2))


if __name__ == '__main__':
    main()
