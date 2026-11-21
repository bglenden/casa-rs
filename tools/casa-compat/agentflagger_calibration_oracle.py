#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Calibration agentflagger state, staged sequencing, and snapshot interoperability."""

import argparse
from contextlib import chdir
from datetime import datetime, timedelta
from pathlib import Path
import shutil
import signal
import subprocess
import sys

from casatools import agentflagger as casa_agent
import numpy as np

from calibration_smoothcal_oracle import fixture, opened
from flagdata_calibration_oracle import fixture as bandpass_fixture, flags
from flagmanager_calibration_oracle import (
    LOCKED_WRITER, assert_flags, original_columns, preserved, set_flags,
)


def sequence(factory, path, sequential, write):
    state = factory()
    assert state.open(str(path))
    try:
        assert state.selectdata()
        assert state.parsesummaryparameters(name='before')
        assert state.parsemanualparameters(antenna='A1')
        assert state.parsesummaryparameters(name='middle')
        assert state.parseagentparameters({'mode': 'unflag', 'antenna': 'A0'})
        assert state.parsesummaryparameters(name='after')
        assert state.init()
        report = state.run(writeflags=write, sequential=sequential)
        assert report['nreport'] == 3
        assert [report[f'report{i}']['name'] for i in range(3)] == ['before', 'middle', 'after']
        expect = [16, 24, 18] if sequential else [10, 18, 18]
        assert [report[f'report{i}']['flagged'] for i in range(3)] == expect, report
        assert state.run(writeflags=write, sequential=sequential) == {}
    finally:
        assert state.done()
    return report


def lifecycle(factory, path):
    state = factory()
    assert state.selectdata()
    assert state.parsemanualparameters()
    assert not state.init()
    assert state.run(writeflags=False) == {}
    assert state.getflagversionlist(printflags=False) == []
    assert state.printflagselection()
    assert state.done()
    assert state.open(str(path))
    try:
        assert state.selectdata(antenna='A1')
        assert state.selectdata(field='FLUX')
        assert state.run(writeflags=False) == {}
        assert not state.init()
        assert state.parsesummaryparameters(name='take1')
        assert not state.run(writeflags=False)
        assert state.init()
        first = state.run(writeflags=False)
        assert first['report0']['name'] == 'take1'
        assert first['report0']['total'] == 40
        assert state.parsesummaryparameters(name='take2')
        assert state.run(writeflags=False) == {}
        assert state.init()
        second = state.run(writeflags=False)
        assert second['report0']['name'] == 'take2'
        assert state.open(str(path))
        assert not state.init()
        assert state.run(writeflags=False) == {}
    finally:
        assert state.done()
        assert state.done()
    return first, second


def same_value(actual, expected):
    if isinstance(expected, dict):
        assert actual.keys() == expected.keys(), (actual, expected)
        for key, value in expected.items():
            same_value(actual[key], value)
    elif isinstance(expected, np.ndarray):
        assert isinstance(actual, np.ndarray)
        assert actual.dtype == expected.dtype
        np.testing.assert_allclose(actual, expected, rtol=1e-6, atol=1e-9)
    else:
        assert actual == expected, (actual, expected)


def agent_cases(source, column):
    with opened(source) as table:
        time = datetime(1858, 11, 17) + timedelta(seconds=float(table.getcol('TIME').min()))
    utc = time.strftime('%Y/%m/%d/%H:%M:%S.%f')
    tfcrop = dict(datacolumn=column, ntime=13.0, combinescans=True, timecutoff=3.0, freqcutoff=2.5,
                  timefit='poly', freqfit='line', maxnpieces=3, flagdimension='freq',
                  usewindowstats='sum', halfwin=2, extendflags=False, channelavg=True, chanbin=2,
                  timeavg=True, timebin='2s')
    cases = [
        ('manual-apply', 'parsemanualparameters', dict(antenna='A1', field='FLUX', spw='0:0~2'), True),
        ('manual-unflag', 'parsemanualparameters', dict(antenna='A0', apply=False), True),
        ('manual-autocorr', 'parsemanualparameters', dict(autocorr=True, time=utc), False),
        ('manual-autocorr-range', 'parsemanualparameters', dict(
            autocorr=True, time='~'.join((time + timedelta(seconds=value)).strftime(
                '%Y/%m/%d/%H:%M:%S.%f') for value in (-1, 1))), False),
        ('clip-apply', 'parseclipparameters', dict(datacolumn=column, clipminmax=[0.1, 3.0],
                                                correlation='ABS_SOL1', spw='0:1~3'), True),
        ('clip-numpy-range', 'parseclipparameters', dict(datacolumn=column,
                                                  clipminmax=np.asarray([0.1, 3.0])), True),
        ('clip-unflag', 'parseclipparameters', dict(datacolumn=column, clipminmax=[0.1, 3.0],
                                                 apply=False), True),
        ('clip-average', 'parseclipparameters', dict(datacolumn=column, clipminmax=[0.1, 3.0],
                                                clipoutside=False, channelavg=True, chanbin=2,
                                                timeavg=True, timebin='10s', clipzeros=True), False),
        ('clip-average-alone', 'parseclipparameters', dict(datacolumn=column, clipminmax=[0.1, 3.0],
                                                clipoutside=False, channelavg=True, chanbin=2,
                                                timeavg=True, timebin='10s', clipzeros=True), True),
        ('clip-no-column', 'parseclipparameters', {}, False),
        ('quack-apply', 'parsequackparameters', dict(quackinterval=3.0, quackmode='endb',
                                                  quackincrement=True, scan='1'), True),
        ('quack-unflag', 'parsequackparameters', dict(quackinterval=1.0, quackmode='beg',
                                                   apply=False), True),
        ('quack-bad-mode', 'parsequackparameters', dict(quackmode='bad'), False),
        ('elevation', 'parseelevationparameters', dict(lowerlimit=5.0, upperlimit=86.0), False),
        ('tfcrop-average', 'parsetfcropparameters', tfcrop, True),
        ('tfcrop-average-alone', 'parsetfcropparameters', tfcrop, True),
        ('tfcrop-unflag-alone', 'parsetfcropparameters', dict(tfcrop, apply=False), True),
        ('tfcrop-absent-correlation-alone', 'parseagentparameters', dict(
            aparams=dict(tfcrop, mode='tfcrop', freqcutoff=0.25)), True),
        ('tfcrop-empty-correlation-alone', 'parseagentparameters', dict(
            aparams=dict(tfcrop, mode='tfcrop', freqcutoff=0.25, correlation='')), True),
        ('tfcrop-no-column', 'parsetfcropparameters', {}, False),
        ('antint', 'parseantintparameters', dict(antint_ref_antenna='A1', minchanfrac=0.2,
                                              verbose=True, apply=False), False),
        ('extend-apply', 'parseextendparameters', dict(ntime=1., combinescans=True,
           extendpols=False, growtime=10.0, growfreq=15.0, growaround=True,
           flagneartime=True, flagnearfreq=True), True),
        ('extend-unflag', 'parseextendparameters', dict(growtime=0.0, growfreq=10.0, apply=False), True),
        ('summary-dimensions', 'parsesummaryparameters', dict(
            spw='0:0~2', spwchan=True, spwcorr=True, basecnt=True, fieldcnt=True, name='axes'), False),
        ('summary-correlation', 'parsesummaryparameters', dict(correlation='Sol1'), False),
        ('summary-feed', 'parsesummaryparameters', dict(feed='9'), False),
        ('summary-observation', 'parsesummaryparameters', dict(observation='99'), False),
        ('summary-array', 'parsesummaryparameters', dict(array='9'), False),
        ('summary-uvrange', 'parsesummaryparameters', dict(uvrange='>100km'), False),
        ('summary-intent', 'parsesummaryparameters', dict(intent='NONE'), False),
        ('generic-empty', 'parseagentparameters', dict(aparams={}), False),
        ('generic-extra', 'parseagentparameters', dict(aparams={'mode': 'manual', 'extra': 9}), False),
        ('generic-numeric-unflag', 'parseagentparameters', dict(
            aparams={'mode': 'manual', 'antenna': 'A1', 'apply': 0}), True),
        ('generic-numpy-switch', 'parseagentparameters', dict(
            aparams={'mode': 'manual', 'antenna': 'A1', 'apply': np.bool_(False)}), True),
        ('generic-invalid-switch', 'parseagentparameters', dict(
            aparams={'mode': 'manual', 'apply': 'False'}), True),
        ('generic-invalid-selector', 'parseagentparameters', dict(
            aparams={'mode': 'manual', 'antenna': 2}), True),
        ('generic-shadow', 'parseagentparameters', dict(aparams={'mode': 'shadow'}), False),
        ('generic-elevation', 'parseagentparameters', dict(aparams={'mode': 'elevation'}), False),
        ('generic-unavailable', 'parseagentparameters', dict(
            aparams={'mode': 'clip', 'datacolumn': 'DATA'}), False),
    ]
    rflag = {'mode': 'rflag', 'datacolumn': column}
    cases.extend([
        ('rflag-apply-configuration-preview', 'parseagentparameters', dict(aparams=rflag), False),
        ('rflag-calculate-configuration-preview', 'parseagentparameters', dict(
            aparams=dict(rflag, writeflags=False, extendflags=False, agentname='raw report')), False),
        ('rflag-calculate-with-write', 'parseagentparameters', dict(
            aparams=dict(rflag, writeflags=False, extendflags=False)), True),
        ('rflag-average', 'parseagentparameters', dict(
            aparams=dict(rflag, writeflags=False, extendflags=False, channelavg=True)), False),
        ('rflag-threshold-apply', 'parseagentparameters', dict(aparams=dict(
            rflag, winsize=3, timedev=0.01, freqdev=0.1, timedevscale=2.0, freqdevscale=3.0,
            spectralmin=0.0, spectralmax=100000., extendflags=False)), True),
    ])
    return cases


def run_agent(factory, path, label, method, arguments, write):
    tool = factory()
    assert tool.open(str(path), ntime=3.0)
    try:
        assert tool.selectdata()
        parsed = getattr(tool, method)(**arguments)
        # Keep a summary after active agents so preview mutations are visible.
        if method != 'parsesummaryparameters' and not label.endswith('-alone'):
            assert tool.parsesummaryparameters(name='after')
        if label in ('clip-average', 'tfcrop-average', 'rflag-average'):
            try:
                tool.init()
            except RuntimeError as error:
                return parsed, False, str(error)
            raise AssertionError('expected an init refusal for averaging plus summary')
        initialized = tool.init()
        result = tool.run(writeflags=write)
        assert tool.run(writeflags=write) == {}
        return parsed, initialized, result
    finally:
        assert tool.done()


def compare_agents(root, engines, source, column):
    cases = agent_cases(source, column)
    for label, method, arguments, write in cases:
        traces, files = {}, {}
        for name, factory in engines.items():
            path = root / f'{column}-{label}-{name}.cal'
            shutil.copytree(source, path)
            before = original_columns(path)
            traces[name] = run_agent(factory, path, label, method, arguments, write)
            files[name] = flags(path)
            preserved(path, before)
            if not write:
                assert_flags(path, flags(source))
        if 'native' in traces:
            assert traces['native'][:2] == traces['casa'][:2], (column, label, traces)
            same_value(traces['native'][2], traces['casa'][2])
            np.testing.assert_array_equal(files['native'], files['casa'], err_msg=f'{column} {label}')
        print(column, label, 'passed', flush=True)
    return len(cases)


def selection_cases(root, engines, source, column):
    examples = [
        ({'config': {'antenna': 'A0'}, 'antenna': 'A1'}, {}),
        ({'config': {'unused': 1}, 'antenna': 'A1'}, {}),
        ({'spw': '0:0~1'}, {'spw': '0:6~7'}),
        ({'antenna': 'A1'}, {'antenna': 'A2'}),
        ({'field': 'FLUX'}, {'field': 'PHASE'}),
        ({'correlation': 'Sol1'}, {}),
        ({'array': '9'}, {}),
        ({'uvrange': '>100km'}, {}),
        ({'intent': 'NONE'}, {}),
        ({'observation': '99'}, {}),
        ({'feed': '9'}, {}),
        ({'config': None}, {}),
        ({'config': {'field': 3}}, {}),
        ({'observation': '0', 'scan': '1'}, {'spw': '0:2~4'}),
    ]
    for index, (global_selection, local_selection) in enumerate(examples):
        results = {}
        for name, factory in engines.items():
            path = root / f'{column}-selection-{index}-{name}.cal'
            shutil.copytree(source, path)
            tool = factory()
            assert tool.open(str(path))
            try:
                try:
                    assert tool.selectdata(**global_selection)
                except (RuntimeError, TypeError) as error:
                    results[name] = (type(error).__name__, str(error))
                else:
                    assert tool.parsesummaryparameters(**local_selection)
                    assert tool.init()
                    results[name] = tool.run(writeflags=False)
            finally:
                assert tool.done()
            assert_flags(path, flags(source))
        if 'native' in results:
            same_value(results['native'], results['casa'])
    return len(examples)


def versions(root, engines, source, column):
    for name, writer in engines.items():
        for other, reader in engines.items():
            path = root / f'{column}-versions-{name}-{other}.cal'
            shutil.copytree(source, path)
            before = original_columns(path)
            initial = flags(path)
            snapshot = path.with_name(f'{path.name}.flagversions')
            a, b = writer(), reader()
            assert a.open(str(path))
            assert a.selectdata(antenna='A1')
            try:
                assert a.getflagversionlist(printflags=False) == [
                    f'\nMS : {path}\n', 'main : working copy in main table',
                ]
                assert a.saveflagversion()
                assert_flags(snapshot / 'flags.', initial, version=True)
                assert a.saveflagversion('initial', 'comment: staged baseline')
                assert a.saveflagversion('main')
            finally:
                assert a.done()
            assert b.open(str(path))
            try:
                assert ' : ' in b.getflagversionlist()
                assert 'initial : comment: staged baseline' in b.getflagversionlist(printflags=False)
                assert b.deleteflagversion(['', 'main'])
                assert (snapshot / 'flags.').exists()
                for merge, combine in [('replace', lambda saved, current: saved),
                                       ('or', np.logical_or), ('and', np.logical_and)]:
                    current = set_flags(path, 1)
                    assert b.restoreflagversion(['initial'], merge=merge)
                    assert_flags(path, combine(initial, current))
                set_flags(path, 0)
                try:
                    b.restoreflagversion(['initial', 'missing'], merge='replace')
                except RuntimeError as error:
                    assert str(error) == 'Flag version missing does not exist'
                else:
                    raise AssertionError('expected missing version')
                assert_flags(path, initial)
                assert b.deleteflagversion(['missing', 'initial'])
                assert b.restoreflagversion()
                assert b.deleteflagversion()
            finally:
                assert b.done()
            assert not (snapshot / 'flags.initial').exists()
            preserved(path, before)
    return len(engines) ** 2


def safe_native_runs(root, factory, source, column):
    path = root / f'{column}-native-safe.caltable'
    shutil.copytree(source, path)
    initial = flags(path)
    code = """
from casatools import agentflagger
import sys
a = agentflagger()
assert a.open(sys.argv[1])
assert a.selectdata()
assert a.parseagentparameters({'mode': 'antint', 'datacolumn': sys.argv[2],
                               'antint_ref_antenna': 'A1'})
assert a.init()
a.run(writeflags=False)
"""
    rejected = subprocess.run([sys.executable, '-c', code, str(path), column],
                              capture_output=True, timeout=30)
    assert rejected.returncode == -signal.SIGABRT.value, rejected.stderr.decode()
    tool = factory()
    assert tool.open(str(path))
    try:
        assert tool.selectdata()
        assert tool.parsemanualparameters(antenna='A2')
        assert tool.parseagentparameters({'mode': 'antint', 'datacolumn': column,
                                          'antint_ref_antenna': 'A1'})
        assert tool.init()
        try:
            tool.run()
        except RuntimeError as error:
            assert str(error) == 'antint requires a MeasurementSet with visibility rows'
        else:
            raise AssertionError('expected a safe antint refusal')
        assert_flags(path, initial)
        assert tool.run(writeflags=False) == {}
        assert tool.parsemanualparameters(antenna='A2')
        assert tool.parsemanualparameters(antenna='missing')
        assert tool.init()
        try:
            tool.run()
        except RuntimeError as error:
            assert 'No match found for token(s) "missing"' in str(error)
        else:
            raise AssertionError('expected an unmatched-selector refusal')
        assert_flags(path, initial)
    finally:
        assert tool.done()


def locked_snapshot(root, source):
    path = root / 'agent-writer-lock.caltable'
    shutil.copytree(source, path)
    code = """
from casars.tools import agentflagger
import sys
a = agentflagger()
print('saving', flush=True)
assert a.open(sys.argv[1])
assert a.saveflagversion('locked')
assert a.done()
"""
    with subprocess.Popen([sys.executable, '-c', LOCKED_WRITER, str(path)], text=True,
                          stdin=subprocess.PIPE, stdout=subprocess.PIPE) as writer:
        assert writer.stdout.readline().strip() == 'locked'
        with subprocess.Popen([sys.executable, '-c', code, str(path)], text=True,
                              stdout=subprocess.PIPE) as saver:
            assert saver.stdout.readline().strip() == 'saving'
            try:
                saver.wait(timeout=0.5)
            except subprocess.TimeoutExpired:
                pass
            finally:
                writer.stdin.write('finish\n')
                writer.stdin.flush()
            assert saver.returncode is None
            assert writer.wait(timeout=15) == 0
            assert saver.wait(timeout=30) == 0
    expected = flags(path)
    assert np.all(expected)
    sidecar = path.with_name(f'{path.name}.flagversions')
    assert_flags(sidecar / 'flags.locked', expected, version=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', required=True, type=Path)
    parser.add_argument('--reference-only', action='store_true')
    args = parser.parse_args()
    root = args.work.resolve()
    root.mkdir()
    engines = {'casa': casa_agent}
    if not args.reference_only:
        from casars.tools import agentflagger as native_agent
        engines['native'] = native_agent
    with chdir(root):
        source = root / 'seed.cal'
        fixture(root / 'seed.ms', source)
        with opened(source, True) as table:
            time = table.getcol('TIME')
            table.removerows(np.flatnonzero(~np.isin(time, np.unique(time)[::2])).tolist())
            antennas = table.getcol('ANTENNA1')
            seed = table.getcol('FLAG')
            seed[:] = False
            for antenna, count in [(0, 3), (1, 2), (2, 3)]:
                seed[..., np.flatnonzero(antennas == antenna)[:count]] = True
            table.putcol('FLAG', seed)
        initial = flags(source)
        with opened(source) as table:
            antennas = table.getcol('ANTENNA1')
        for sequential in (True, False):
            for write in (True, False):
                results = {}
                for name, tool in engines.items():
                    path = root / f'{name}-{sequential}-{write}.cal'
                    shutil.copytree(source, path)
                    metadata = original_columns(path)
                    results[name] = sequence(tool, path, sequential, write)
                    wanted = initial.copy()
                    if write:
                        wanted[..., antennas == 1] = True
                        wanted[..., antennas == 0] = False
                    assert_flags(path, wanted)
                    preserved(path, metadata)
                if not args.reference_only:
                    assert results['casa'] == results['native']
        results = {name: lifecycle(tool, source) for name, tool in engines.items()}
        if not args.reference_only:
            assert results['casa'] == results['native']
        np.testing.assert_array_equal(flags(source), initial)
        table_root = root / 'automatic-tables'
        table_root.mkdir()
        products = bandpass_fixture(table_root)
        agents, selections, version_workflows = 0, 0, 0
        for source, column in products:
            agents += compare_agents(root, engines, source, column)
            selections += selection_cases(root, engines, source, column)
            version_workflows += versions(root, engines, source, column)
            if 'native' in engines:
                safe_native_runs(root, engines['native'], source, column)
        if 'native' in engines:
            locked_snapshot(root, products[0][0])
    print(f'Four sequencing cases, lifecycle, {agents} agent cases, {selections} selections, '
          f'and {version_workflows} version workflows passed.')


if __name__ == '__main__':
    main()
