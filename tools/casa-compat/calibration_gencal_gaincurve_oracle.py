#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare VLA gencal curves and efficiency with pinned CASA on a real MS."""

import argparse
import json
import os
from pathlib import Path

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

    results['explicit-file'], explicit = compare_case(root, 'explicit-file', source, 'gceff',
                                                       {'infile': curves}, native)
    assert not np.array_equal(default['gceff'], explicit['FPARAM'])
    results['inactive-selectors'], selected = compare_case(
        root, 'inactive-selectors', source, 'gc',
        {'spw': '0', 'antenna': '1', 'pol': 'Y', 'uniform': False, 'parameter': [1.7]}, native)
    np.testing.assert_array_equal(selected['FPARAM'], default['gc'])
    results['missing-file'] = refuses(root, 'missing-file', source, 'gceff',
                                      {'infile': str(root / 'absent')}, native)
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
    (root / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
    print(json.dumps(results, indent=2))


if __name__ == '__main__':
    main()
