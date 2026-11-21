#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Exercise RFlag threshold files and matrices on CASA-written calibration tables."""
import argparse
import ast
from contextlib import chdir
import json
import os
from pathlib import Path
import shutil
import subprocess

import casatasks as casa
import numpy as np
from casatasks.private.flaghelper import Parser

from flagdata_calibration_oracle import fixture, flags, outcome
from flagdata_calibration_rfi_oracle import compare_report
from flagmanager_calibration_oracle import assert_flags, original_columns, preserved


def check_files(root, source, column, engines):
    names = ('timedev', 'freqdev')
    opts = dict(mode='rflag', datacolumn=column, flagbackup=False, extendflags=False)
    products = {}
    before = flags(source)
    for key, task in engines.items():
        case = root / f'{column}-{key}'
        case.mkdir()
        vis = case / source.name
        shutil.copytree(source, vis)
        original = original_columns(vis)
        with chdir(case):
            default = outcome(task, vis, action='calculate', **opts)
            for axis in names:
                path = Path(f'rflag_output_thresholds_{axis}0.txt')
                record = ast.literal_eval(path.read_text())
                assert record['name'] == 'Rflag'
                np.testing.assert_array_equal(record[axis], default['report0'][axis])
            files = dict(zip(names, ['custom-time.txt', 'custom-freq.txt'], strict=True))
            named = outcome(task, vis, action='calculate', **files, **opts)
            compare_report(default, named)
            assert_flags(vis, before)
            preserved(vis, original)
            products[key] = (default, {axis: case / path for axis, path in files.items()})
    compare_report(products['casa'][0], products['native'][0])
    for writer, reader in [('casa', 'native'), ('native', 'casa')]:
        for mirror in (True, False):
            vis = root / f'{column}-{writer}-read-{reader}-{mirror}.cal'
            shutil.copytree(source, vis)
            original = original_columns(vis)
            supplied = products[writer][1]
            if mirror:
                supplied = {axis: str(path) for axis, path in supplied.items()}
            else:
                supplied = {axis: products[writer][0]['report0'][axis] for axis in names}
            assert outcome(engines[reader], vis, **supplied, **opts) == {}
            if writer == 'casa' and mirror:
                expected = flags(vis)
                assert np.any(expected != before)
            else:
                assert_flags(vis, expected)
            preserved(vis, original)
    return 4


def check_arrays(root, source, column, engines):
    matrices = [
        ('sparse', [[0, 0, 1.0]], [[0, 0, 1.2], [1, 0, 12.0]], 'apply'),
        ('flat', [0, 0, 1.0], [[0, 0, 1.2]], 'apply'),
        ('duplicate', [[0, 0, 1.0], [0, 0, 0.2]], [[0, 0, 1.2], [0, 0, 3.5]], 'apply'),
        ('unmatched', [[99, 1, 2.0]], [[99, 1, 4.0]], 'apply'),
        ('unmatched-scale', 100.0, [[99, 0, 1.2]], 'apply'),
        ('missing', 'missing-time.txt', 'missing-freq.txt', 'apply'),
        ('malformed', 'bad-time.txt', [], 'apply'),
        ('reported', [[0, 0, 1.0]], [], 'calculate'),
        ('tuple', ((0, 0, 0.1),), ((0, 0, 1.2),), 'apply'),
        ('numpy-float', 100.0, np.float32(0.2), 'apply'),
        ('numpy-int32', 100.0, np.int32(4), 'apply'),
        ('numpy-int64', 100.0, np.int64(4), 'apply'),
        ('numpy-empty', 100.0, np.empty((0, 3)), 'apply'),
        ('numpy-scalar-array', 100.0, np.array(0.1), 'apply'),
        ('flat-calculate', [], [0.0, 0.0, 0.2], 'calculate'),
        ('edited-file', 100.0, 'edited.txt', 'apply'),
        ('directory-input', 100.0, 'input-dir', 'apply'),
        ('missing-parent', 'kept.txt', 'missing-parent/freq.txt', 'calculate'),
    ]
    for name, time, frequency, action in matrices:
        result = {}
        for engine, task in engines.items():
            case = root / f'{column}-{name}-{engine}'
            case.mkdir()
            vis = case / source.name
            shutil.copytree(source, vis)
            original = original_columns(vis)
            with chdir(case):
                Path('bad-time.txt').write_text('No threshold list\n')
                Path('edited.txt').write_text("{'name': 'Rflag', 'freqdev': [[0, 0, .2],],}\n")
                Path('input-dir').mkdir()
                Path('kept.txt').write_text('previous thresholds\n')
                report = outcome(task, vis, mode='rflag', datacolumn=column,
                                 action=action, timedev=time, freqdev=frequency,
                                 freqdevscale=-1.0 if name == 'unmatched-scale' else 5.0,
                                 flagbackup=name == 'malformed', extendflags=False)
                if name == 'missing-parent' and engine == 'native':
                    assert Path('kept.txt').read_text() == 'previous thresholds\n'
            result[engine] = (report, flags(vis))
            preserved(vis, original)
            if name == 'malformed':
                versions = vis.with_name(f'{vis.name}.flagversions')
                assert_flags(versions / 'flags.flagdata_1', flags(source), version=True)
        compare_report(result['casa'][0], result['native'][0])
        np.testing.assert_array_equal(result['native'][1], result['casa'][1])
        if action == 'calculate' or name in ('flat', 'malformed'):
            np.testing.assert_array_equal(result['native'][1], flags(source))
    return len(matrices)


def check_saved_commands(root, source, column, engines):
    cases = [
        ('apply', dict(action='apply', timedev=0.1, freqdev=[[0, 0, 1.2]])),
        ('calculate', dict(action='calculate')),
        ('calculate-file', dict(action='calculate', timedev='saved-time.txt', freqdev='saved-freq.txt')),
        ('save-only', dict(action='', timedev=0.1, freqdev=[[0, 0, 1.2]])),
        ('invalid-file', dict(action='apply', timedev='missing-time.txt', freqdev='flat-freq.txt')),
    ]
    for name, options in cases:
        commands, reports, observed, replay = {}, {}, {}, {}
        for engine, task in engines.items():
            case = root / f'{column}-save-{name}-{engine}'
            case.mkdir()
            vis = case / source.name
            shutil.copytree(source, vis)
            original = original_columns(vis)
            with chdir(case):
                Path('flat-freq.txt').write_text("{'name':'Rflag','freqdev':[0,0,0.2]}\n")
                reports[engine] = outcome(task, vis, mode='rflag', datacolumn=column,
                                          savepars=True, outfile='saved.txt', cmdreason='threshold test',
                                          flagbackup=False, extendflags=False, **options)
                saved = Path('saved.txt')
                assert saved.is_file(), (engine, name, reports[engine])
                commands[engine] = Parser(' ', '=').parse2Dictionary(saved.read_text().strip())
                if options['action'] == 'calculate' or name == 'invalid-file':
                    # The reference task reads saved commands from both engines.
                    assert_flags(vis, flags(source))
                    replay[engine] = outcome(casa.flagdata, vis, mode='list',
                                             inpfile='saved.txt', flagbackup=False)
                observed[engine] = flags(vis)
                preserved(vis, original)
        compare_report(reports['casa'], reports['native'])
        if replay:
            assert replay['casa'] == replay['native'], replay
        assert commands['casa'].keys() == commands['native'].keys()
        for key, expected in commands['casa'].items():
            actual = commands['native'][key]
            if key in ('timedev', 'freqdev') and isinstance(expected, list):
                np.testing.assert_allclose(actual, expected, rtol=1e-3, atol=1e-9)
            else:
                assert actual == expected, (name, key, actual, expected)
        np.testing.assert_array_equal(observed['native'], observed['casa'])
    return len(cases)


def check_cli_and_scale(root, source, column, native, native_bin):
    case = root / f'{column}-cli'
    case.mkdir()
    vis = case / source.name
    shutil.copytree(source, vis)
    prior, metadata = flags(vis), original_columns(vis)
    command = [
        str(native_bin), '--vis', str(vis), '--mode', 'rflag',
        '--datacolumn', column, '--action', 'calculate', '--no-flagbackup',
        '--timedev', '1.5', '--freqdev', '[[0,0,1.2]]',
    ]
    run = subprocess.run(command, cwd=case, text=True, capture_output=True, check=True)
    report = json.loads(run.stdout)['rflag_report']
    assert report['timedev'] == [] and report['freqdev'][0] == [0.0, 0.0, 1.2]
    assert (case / 'rflag_output_thresholds_freqdev0.txt').is_file()
    assert_flags(vis, prior)
    result = outcome(native, vis, mode='rflag', datacolumn=column, flagbackup=False,
                     freqdev=[[0, 0, 1.2]], freqdevscale=-1.0)
    assert result == (
        'TaskParameterError',
        "flagdata refused its parameters (parameter 'freqdevscale'): "
        "freqdevscale must be positive when its threshold is positive",
    )
    assert_flags(vis, prior)
    preserved(vis, metadata)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--native-bin', type=Path, required=True)
    args = parser.parse_args()
    root = args.work.resolve()
    root.mkdir()
    native_bin = args.native_bin.resolve()
    os.environ['CASARS_FLAGDATA_BIN'] = str(native_bin)
    from casars.tasks import flagdata as native
    assert casa.version_string() == '6.7.6.14'
    engines = {'casa': casa.flagdata, 'native': native}
    with chdir(root):
        sources = fixture(root)
        checks = sum(check_files(root, source, column, engines)
                     + check_arrays(root, source, column, engines)
                     + check_saved_commands(root, source, column, engines)
                     for source, column in sources)
        for source, column in sources:
            check_cli_and_scale(root, source, column, native, native_bin)
    print(f'{checks} paired RFlag threshold and cross-engine interchange checks. '
          'Two CLI matrix cases and two safe scale refusal controls passed.')


if __name__ == '__main__':
    main()
