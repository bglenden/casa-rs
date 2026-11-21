#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare CASA calibration flag command programs and staged summaries."""
import argparse
from ast import literal_eval
from contextlib import chdir
from datetime import datetime, timedelta
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys

import casatasks as casa
import numpy as np

from flagdata_calibration_oracle import fixture, flags, opened, outcome
from flagmanager_calibration_oracle import assert_flags, original_columns, preserved


COMMANDS = [
    "mode='manual' antenna='A1' spw='0:2~3' reason='science'",
    "mode='summary' name='middle' reason='inspect'",
    "mode='unflag' antenna='A1' spw='0:3' reason='undo'",
    "mode='summary' name='final' reason='inspect'",
]


def cases(column, time):
    yield 'apply-text', dict(inpfile=COMMANDS)
    yield 'apply-file', dict(inpfile='commands.txt')
    yield 'apply-files', dict(inpfile=['commands.txt'])
    yield 'calculate', dict(inpfile=COMMANDS, action='calculate')
    yield 'only-science', dict(inpfile=COMMANDS, reason='science')
    yield 'reason-list', dict(inpfile=COMMANDS, reason=['science', 'inspect'])
    yield 'empty-reason-list', dict(inpfile=COMMANDS, reason=[])
    yield 'reason-index', dict(inpfile=["mode='manual'", "mode='summary' reason=''"], reason='')
    yield 'tuple-commands', dict(inpfile=tuple(COMMANDS), reason=('science', 'inspect'))
    yield 'numpy-commands', dict(inpfile=np.asarray(COMMANDS), reason=np.asarray(['science', 'inspect']))
    yield 'scalar-array-commands', dict(inpfile=np.asarray("mode='summary'"))
    yield 'scalar-array-reason', dict(inpfile=COMMANDS, reason=np.asarray('science'))
    yield 'invalid-input-type', dict(inpfile=[4])
    yield 'invalid-buffer-type', dict(inpfile=COMMANDS, tbuff=np.asarray(1.0))
    yield 'unknown-reason', dict(inpfile=COMMANDS, reason='none')
    yield 'save-only', dict(inpfile=COMMANDS, action='', savepars=True, outfile='saved.txt')
    yield 'apply-save', dict(inpfile=COMMANDS, savepars=True, outfile='saved.txt')
    yield 'save-reason', dict(inpfile=["mode='manual'  antenna=A2 reason='old'", "mode='summary'"],
                              action='', savepars=True, outfile='saved.txt', cmdreason='replacement text')
    yield 'save-existing', dict(inpfile=COMMANDS, action='', savepars=True,
                               outfile='saved.txt', overwrite=False)
    yield 'apply-existing', dict(inpfile=COMMANDS, savepars=True,
                                outfile='saved.txt', overwrite=False)
    yield 'replace-existing', dict(inpfile=COMMANDS, savepars=True,
                                  outfile='saved.txt', overwrite=True)
    yield 'missing', dict(inpfile='missing.txt')
    yield 'malformed', dict(inpfile=["mode='manual' spw"])
    yield 'summary-alone', dict(inpfile=[COMMANDS[1], COMMANDS[3]])
    yield 'default-summary-names', dict(inpfile=["mode='summary'", "mode='summary'"])
    yield 'unknown-parameters', dict(inpfile=["mode='manual' nonexistent=True"])
    yield 'unknown-mode', dict(inpfile=["mode='nonsense'", "mode='summary'"])
    yield 'wrong-command-type', dict(inpfile=["mode='manual' autocorr='bad'", "mode='summary'"])
    yield 'no-supported-agents', dict(inpfile=["mode='clip' datacolumn='DATA'"])
    yield 'inactive-agents', dict(inpfile=[
        "mode='clip' datacolumn='DATA'", "mode='shadow'", "mode='tfcrop' datacolumn='DATA'",
        "mode='rflag' datacolumn='DATA'", "mode='elevation'", "mode='quack' quackmode='oops'",
        "mode='summary' correlation='Sol1'", "mode='summary' name='survived'",
    ])
    yield 'clip-empty', dict(inpfile=[f"mode='clip' datacolumn='{column}' clipminmax=[]", "mode='summary'"])
    yield 'clip-three', dict(inpfile=[f"mode='clip' datacolumn='{column}' clipminmax=[0, 1.5, 3]", "mode='summary'"])
    yield 'relative-filter', dict(inpfile=COMMANDS, minabs=3, maxrel=0.5)
    rflag = f"mode='rflag' datacolumn='{column}' extendflags=False"
    yield 'rflag-preview', dict(action='calculate', inpfile=[
        "mode='summary' name='before'",
        rflag,
        "mode='summary' name='after-rflag'",
    ])
    yield 'rflag-named-files', dict(action='calculate', savepars=True, outfile='saved.txt', inpfile=[
        f"{rflag} timedev='time.txt' freqdev='frequency.txt' reason='run'",
        "mode='summary' reason='run'",
    ], reason=['run'])
    yield 'rflag-float-literals', dict(inpfile=[f"{rflag} timedev=0.001 freqdev=0.02", "mode='summary'"], flagbackup=True)
    yield 'rflag-apply-matrices', dict(inpfile=[
        f"{rflag} timedev=[[0., 0., .001], [1., 0., .001]] freqdev=[[0., 0., .02], [1., 0., .02]]",
        "mode='summary'",
    ])
    yield 'rflag-apply-files', dict(inpfile=[
        f"{rflag} timedev='time-input.txt' freqdev='frequency-input.txt'", "mode='summary'",
    ])
    stamp = lambda seconds: (time + timedelta(seconds=seconds)).strftime('%Y/%m/%d/%H:%M:%S.%f')[:-3]
    for label, buffer in [('scalar', 2.0), ('asymmetric', [2.0, 3.0]),
                          ('one-element', [2.0]), ('empty', []), ('numpy', np.asarray([2, 3]))]:
        yield f'tbuff-{label}', dict(
            inpfile=[f"mode='manual'  timerange='{stamp(1)}~{stamp(2)}' reason='raw'", "mode='summary'"],
            tbuff=buffer, action='calculate', savepars=True, outfile='saved.txt',
        )
    yield 'apply-with-backup', dict(inpfile=COMMANDS, flagbackup=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--native-bin', type=Path, required=True)
    args = parser.parse_args()
    root = args.work.resolve()
    root.mkdir()
    os.environ['CASARS_FLAGDATA_BIN'] = str(args.native_bin.resolve())
    from casars.tasks import flagdata as native
    assert casa.version_string() == '6.7.6.14'
    checked = 0
    with chdir(root):
        for source, column in fixture(root):
            initial = flags(source)
            with opened(source) as table:
                seconds = sorted(set(table.getcol('TIME')))[0]
            time = datetime(1858, 11, 17) + timedelta(seconds=float(seconds))
            for name, options in cases(column, time):
                reports, products, saved, thresholds = {}, {}, {}, {}
                for engine, task in [('casa', casa.flagdata), ('native', native)]:
                    case = root / f'{column}-{name}-{engine}'
                    case.mkdir()
                    path = case / source.name
                    shutil.copytree(source, path)
                    metadata = original_columns(path)
                    with chdir(case):
                        Path('commands.txt').write_text('# commands\n' + '\n'.join(COMMANDS) + '\n')
                        if name == 'rflag-apply-files':
                            Path('time-input.txt').write_text("{'name':'Rflag','timedev':[[0.0,0.0,0.001],[1.0,0.0,0.001]]}\n")
                            Path('frequency-input.txt').write_text("{'name':'Rflag','freqdev':[[0.0,0.0,0.02],[1.0,0.0,0.02]]}\n")
                        if name.endswith('existing'):
                            Path('saved.txt').write_text('original\n')
                        opts = {'flagbackup': False, **options}
                        reports[engine] = outcome(task, path, mode='list', **opts)
                        if Path('saved.txt').exists():
                            saved[engine] = Path('saved.txt').read_text()
                        thresholds[engine] = {item.name: literal_eval(item.read_text()) for item in (
                            *Path('.').glob('rflag_output_thresholds_*'), *Path('.').glob('time.txt'),
                            *Path('.').glob('frequency.txt'),
                        )}
                    products[engine] = flags(path)
                    preserved(path, metadata)
                    if options.get('flagbackup'):
                        version = path.with_name(f'{path.name}.flagversions') / 'flags.flagdata_1'
                        assert_flags(version, initial, version=True)
                assert reports['native'] == reports['casa'], (column, name, reports)
                np.testing.assert_array_equal(products['native'], products['casa'])
                assert saved.get('native') == saved.get('casa'), (column, name, saved)
                assert thresholds['native'].keys() == thresholds['casa'].keys(), (column, name, thresholds)
                for file, expected in thresholds['casa'].items():
                    actual = thresholds['native'][file]
                    assert actual.keys() == expected.keys()
                    for key in expected:
                        if key == 'name':
                            assert actual[key] == expected[key], (file, actual, expected)
                        else:
                            np.testing.assert_allclose(actual[key], expected[key], rtol=1e-6, atol=1e-9)
                if options.get('action') in ('calculate', ''):
                    np.testing.assert_array_equal(products['native'], initial)
                if name in ('apply-text', 'calculate'):
                    middle, final = (10, 6) if column == 'CPARAM' else (14, 8)
                    assert reports['casa']['report0']['flagged'] == middle
                    assert reports['casa']['report1']['flagged'] == final
                checked += 1
                print(column, name, 'passed', flush=True)
            interoperability(root, source, column, native)
            safe_refusals(root, source, column, native)
    print(f'{checked} paired calibration command program cases.')


def interoperability(root, source, column, native):
    for producer, consumer, name in [(casa.flagdata, native, 'casa-file'), (native, casa.flagdata, 'native-file')]:
        path = root / f'{column}-{name}.cal'
        shutil.copytree(source, path)
        destination = root / f'{column}-{name}.txt'
        assert producer(str(path), mode='list', inpfile=COMMANDS, action='', savepars=True,
                        outfile=str(destination), flagbackup=False) == {}
        assert_flags(path, flags(source))
        received = consumer(str(path), mode='list', inpfile=str(destination), flagbackup=False)
        expected = flags(source).copy()
        with opened(path) as table:
            expected[:, 2, table.getcol('ANTENNA1') == 1] = True
            expected[:, 3, table.getcol('ANTENNA1') == 1] = False
        assert_flags(path, expected)
        assert received['report1']['flagged'] == float(expected.sum())


def safe_refusals(root, source, column, native):
    path = root / f'{column}-safe-refusal.cal'
    shutil.copytree(source, path)
    before = flags(source)
    metadata = original_columns(path)
    for selector in ("missing", "A2,missing"):
        result = outcome(native, path, mode='list', flagbackup=False, inpfile=[
            COMMANDS[0], f"mode='manual' antenna='{selector}'", COMMANDS[1],
        ])
        assert result[0] == 'RuntimeError', result
        assert_flags(path, before)
    settings = dict(mode='antint', antint_ref_antenna='A1')
    code = ("from casatasks import flagdata; import sys; "
            "flagdata(sys.argv[1], mode='antint', antint_ref_antenna='A1', flagbackup=False)")
    original = subprocess.run([sys.executable, '-c', code, str(path)], capture_output=True, timeout=30)
    assert original.returncode == -signal.SIGABRT.value, original.stderr.decode()
    for options in (settings, dict(mode='list', inpfile=[COMMANDS[0], "mode='antint' antint_ref_antenna='A1'"])):
        result = outcome(native, path, flagbackup=False, **options)
        assert result == ('ValueError', 'antint requires a MeasurementSet with visibility rows'), result
        assert_flags(path, before)
    preserved(path, metadata)


if __name__ == '__main__':
    main()
