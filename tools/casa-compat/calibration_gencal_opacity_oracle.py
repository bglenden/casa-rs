#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare gencal's scalar atmospheric opacity priors with pinned CASA."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

import casatasks as casa
from casatools import table

from applycal_oracle import copy_ms
from calibration_gencal_gaincurve_oracle import compare_case, refuses
from calibration_gencal_specify_oracle import compare_tables, generate, with_second_window


def pinned_crash(vis, output, options):
    command = (
        'import json,resource,sys; resource.setrlimit(resource.RLIMIT_CORE,(0,0)); '
        'from casatasks import gencal; gencal(**json.load(sys.stdin))'
    )
    data = dict(vis=str(vis), caltable=str(output), caltype='opac', parameter=.05, **options)
    reference = subprocess.run([sys.executable, '-c', command], input=json.dumps(data), text=True,
                               capture_output=True, timeout=30)
    assert reference.returncode < 0, reference.stderr
    assert not output.exists()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', required=True, type=Path, help='One-window NGC 5921 VLA MS')
    parser.add_argument('--work', required=True, type=Path)
    parser.add_argument('--native-bin', required=True, type=Path)
    args = parser.parse_args()
    assert casa.version_string() == '6.7.6.14'
    os.environ['CASARS_CALIBRATE_BIN'] = str(args.native_bin.resolve())
    os.environ['CASARS_REQUIRE_TASK_BINARIES'] = '1'
    from casars import tasks as native

    root = args.work.resolve()
    root.mkdir(parents=True, exist_ok=True)
    assert not any(root.iterdir()), 'work directory must be empty'
    vis = root / 'opacity.ms'
    copy_ms(args.source.resolve(), vis)
    with_second_window(vis)
    tool = table()
    assert tool.open(str(vis / 'SPECTRAL_WINDOW'), nomodify=False)
    try:
        for window, name in enumerate(('primary', 'secondary')):
            tool.putcell('NAME', window, name)
    finally:
        tool.close()

    choices = {
        'broadcast': {'parameter': .05},
        'per-antenna': {'antenna': '27,1', 'parameter': [.15, .25]},
        'window-and-antenna-order': {'spw': '1,0', 'antenna': '27,1',
                                     'parameter': [.15, .25, .35, .45]},
        'ranges': {'spw': '0~1', 'antenna': '1~3', 'parameter': .05},
        'named-window': {'spw': 'secondary', 'parameter': .05, 'pol': 'R'},
        'repeat-window': {'spw': '0:0~5;10~12', 'parameter': [.1, .2]},
        'negative-depth': {'parameter': -.05, 'pol': 'X'},
        'inactive-options': {'parameter': .05, 'uniform': False, 'infile': str(root / 'unused')},
    }
    results = {
        case: compare_case(root, case, vis, 'opac', options, native,
                           check_apply=case == 'per-antenna')[0]
        for case, options in choices.items()
    }
    for case, options in {
        'incomplete-grid': {'spw': '0,1', 'antenna': '27,1', 'parameter': [.05, .10]},
        'invalid-polarization': {'parameter': .05, 'pol': 'both'},
        'missing-parameter': {},
    }.items():
        results[case] = refuses(root, case, vis, 'opac', options, native)

    for axis in ('spw', 'antenna'):
        pinned_crash(vis, root / f'casa-wildcard-{axis}.cal', {axis: '*'})
        output = root / f'native-wildcard-{axis}.cal'
        generate(native, vis, output, [('opac', {axis: '*', 'parameter': .05})])
        results[f'wildcard-{axis}'] = {
            'table_nrms': compare_tables(output, root / 'broadcast' / 'casa.cal')}

    for pol in ('L', 'X,Y'):
        pinned_crash(vis, root / f'casa-{pol}.cal', {'pol': pol})
        output = root / f'native-{pol}.cal'
        existing = root / 'broadcast' / 'native.cal'
        copy_ms(existing, output)
        try:
            generate(native, vis, output, [('opac', {'pol': pol, 'parameter': .05})])
        except RuntimeError:
            compare_tables(output, existing)
        else:
            raise AssertionError(f'opac accepted {pol!r} for a scalar caltable')
        results[f'preserve-on-{pol}'] = {'refused': True}

    initial = [('opac', {'parameter': 1e8})]
    ordered = [('opac', {'spw': '0:0~5;10~12', 'parameter': [-1e8, 1.]})]
    reference = root / 'accumulated-casa.cal'
    generate(casa, vis, reference, initial + ordered)
    errors = []
    for first_name, first in (('casa', casa), ('native', native)):
        for second_name, second in (('casa', casa), ('native', native)):
            output = root / f'append-{first_name}-{second_name}.cal'
            generate(first, vis, output, initial)
            generate(second, vis, output, ordered)
            errors.append(compare_tables(output, reference))
    results['ordered-accumulation'] = {'table_nrms': max(errors)}

    (root / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
    print(json.dumps(results, indent=2))


if __name__ == '__main__':
    main()
