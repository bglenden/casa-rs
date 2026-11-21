#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare CASA calibration flag command programs and staged summaries."""
import argparse
from ast import literal_eval
from contextlib import chdir
from datetime import datetime, timedelta
import json
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
    yield 'local-antenna-pair', dict(inpfile=[
        "mode='manual' antenna='A0&A1'", "mode='summary'",
    ])
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
    yield 'malformed-with-backup', dict(inpfile=["mode='manual' bogus=True"], flagbackup=True)
    yield 'summary-alone', dict(inpfile=[COMMANDS[1], COMMANDS[3]])
    yield 'default-summary-names', dict(inpfile=["mode='summary'", "mode='summary'"])
    yield 'unknown-parameters', dict(inpfile=["mode='manual' nonexistent=True"])
    yield 'unknown-mode', dict(inpfile=["mode='nonsense'", "mode='summary'"])
    yield 'empty-mode', dict(inpfile=["mode='' antenna='A1'", "mode='summary'"])
    yield 'wrong-command-type', dict(inpfile=["mode='manual' autocorr='bad'", "mode='summary'"])
    yield 'no-supported-agents', dict(inpfile=["mode='clip' datacolumn='DATA'"])
    yield 'inactive-save', dict(inpfile=["mode='shadow'"], savepars=True, outfile='saved.txt')
    yield 'unknown-mode-save', dict(inpfile=["mode='nonsense'"], savepars=True, outfile='saved.txt')
    yield 'missing-addantenna', dict(inpfile=[
        "mode='shadow' addantenna='missing-antennas.txt'", COMMANDS[0], COMMANDS[1],
    ], flagbackup=True)
    yield 'saved-addantenna', dict(inpfile=["mode='shadow' addantenna='antennas.txt'"],
                                  savepars=True, outfile='saved.txt', action='')
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
    for mode in ('clip', 'tfcrop', 'rflag'):
        yield f'{mode}-average-with-summary', dict(inpfile=[
            f"mode='{mode}' datacolumn='{column}' channelavg=True chanbin=2", "mode='summary'",
        ], flagbackup=True)
    for suffix, correlation in [('default', ''), ('empty', " correlation=''")]:
        yield f'tfcrop-{suffix}-correlation', dict(inpfile=[
            f"mode='tfcrop' datacolumn='{column}' flagdimension='freq' freqfit='line' "
            f"freqcutoff=0.25 extendflags=False{correlation}", "mode='summary'",
        ])
    stamp = lambda seconds: (time + timedelta(seconds=seconds)).strftime('%Y/%m/%d/%H:%M:%S.%f')[:-3]
    for label, buffer in [('scalar', 2.0), ('asymmetric', [2.0, 3.0]),
                          ('one-element', [2.0]), ('empty', []), ('numpy', np.asarray([2, 3]))]:
        yield f'tbuff-{label}', dict(
            inpfile=[f"mode='manual'  timerange='{stamp(1)}~{stamp(2)}' reason='raw'", "mode='summary'"],
            tbuff=buffer, action='calculate', savepars=True, outfile='saved.txt',
        )
    yield 'tbuff-inline-scalar', dict(
        inpfile=f"mode='manual' antenna='A1' timerange='{stamp(-1)}~{stamp(1)}'", tbuff=1.0,
    )
    if column == 'FPARAM':
        for label, center in [('gapped-time-buffer', 0), ('reversed-time-buffer', 10)]:
            yield label, dict(inpfile=[
                f"mode='manual' timerange='{stamp(center-1)}~{stamp(center+1)}'",
                "mode='summary'",
            ])
    yield 'apply-with-backup', dict(inpfile=COMMANDS, flagbackup=True)


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
                    set_buffer_fixture(path, name)
                    metadata = original_columns(path)
                    with chdir(case):
                        Path('commands.txt').write_text('# commands\n' + '\n'.join(COMMANDS) + '\n')
                        if name == 'saved-addantenna':
                            Path('antennas.txt').write_text('name = A6\ndiameter = 16.0\nposition = [1.0, 2.0, 3.0]\n')
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
        check_cli(root, native_bin)
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
    result = outcome(native, path, mode='list', flagbackup=False, inpfile=[
        COMMANDS[0], "mode='manual' field='missing'", COMMANDS[1],
    ])
    assert result[0] == 'RuntimeError', result
    assert_flags(path, before)
    settings = dict(mode='antint', antint_ref_antenna='A1')
    for task in (casa.flagdata, native):
        assert outcome(task, path, flagbackup=False, **settings) == (
            'ValueError', 'Failed to parse parameters for mode antint',
        )
    command = f"mode='antint' antint_ref_antenna='A1' datacolumn='{column}'"
    code = ("from casatasks import flagdata; import sys; "
            "flagdata(sys.argv[1], mode='list', inpfile=[sys.argv[2]], flagbackup=False)")
    original = subprocess.run([sys.executable, '-c', code, str(path), command],
                              capture_output=True, timeout=30)
    assert original.returncode == -signal.SIGABRT.value, original.stderr.decode()
    for commands in ([command], [COMMANDS[0], command]):
        result = outcome(native, path, flagbackup=False, mode='list', inpfile=commands)
        assert result == ('ValueError', 'antint requires a MeasurementSet with visibility rows'), result
        assert_flags(path, before)
    preserved(path, metadata)
    if column == 'FPARAM':
        negation_control(root, source, native)


def set_buffer_fixture(path, name):
    if name not in ('gapped-time-buffer', 'reversed-time-buffer'):
        return
    with opened(path, True) as table:
        times = table.getcol('TIME')
        if name == 'gapped-time-buffer':
            gap = (times == times.min()) & (table.getcol('ANTENNA1') == 2)
            assert gap.sum() == 1
            table.removerows(np.flatnonzero(gap).tolist())
        else:
            first, second = np.unique(times)[:2]
            swapped = times.copy()
            swapped[times == first] = second
            swapped[times == second] = first
            table.putcol('TIME', swapped)


def negation_control(root, source, native):
    for factory, tag in ((casa.flagdata, 'casa'), (native, 'native')):
        for plural in (True, False):
            path = root / f'negation-{tag}-{plural}.cal'
            shutil.copytree(source, path)
            expected = flags(path)
            with opened(path) as table:
                excluded = table.getcol('ANTENNA1') == 1
            commands = ["mode='manual' antenna='!A1' spw='0:1'"]
            if plural:
                commands.append("mode='summary'")
            assert isinstance(factory(str(path), mode='list', flagbackup=False, inpfile=commands), dict)
            if tag == 'native':
                expected[:, 1, ~excluded] = True
            elif plural:
                expected[:, 1, excluded] = True
            assert_flags(path, expected)


def check_cli(root, binary):
    work = root / 'command-line'
    work.mkdir()
    with chdir(work):
        Path('commands.txt').write_text('\n'.join(COMMANDS) + '\n')
        path = root / 'bandpass.cal'
        original = flags(path)
        settings = ['--mode', 'list', '--inpfile', 'commands.txt', '--no-save-last']
        completed = subprocess.run([
            str(binary), '--vis', str(path), *settings, '--action', 'calculate', '--no-flagbackup',
        ], capture_output=True, text=True, check=True)
        summaries = [row['flagged_samples'] for row in json.loads(completed.stdout)
                     if row['mode'] == 'summary']
        assert summaries == [10, 6]
        assert_flags(path, original)
        vis = root / 'source.ms'
        rejected = subprocess.run([
            str(binary), '--vis', str(vis), *settings, '--flagbackup', '--savepars',
            '--outfile', 'refused.txt',
        ], capture_output=True, text=True)
        assert rejected.returncode != 0 and '#978' in rejected.stderr
        assert not Path('refused.txt').exists()
        assert not (root / 'source.ms.flagversions').exists()


if __name__ == '__main__':
    main()
