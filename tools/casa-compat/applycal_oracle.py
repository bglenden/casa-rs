#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Same-input CASA/public-Python applycal comparisons, with hard failures.

The initial small corpus establishes numerical, mutation and API/state gaps.
Its recorded latencies are not dataset-scale performance acceptance.
"""
from __future__ import annotations

import argparse
import contextlib
import inspect
import io
import json
import os
from pathlib import Path
import resource
import time

import numpy as np
import casatasks as casa
from casatools import table

from calibration_lifecycle_oracle import contents, equal, snapshot
from record_casa_returns import make_cal_ms


def plain(value):
    if isinstance(value, dict):
        return {str(key): plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [plain(item) for item in value]
    return value.tolist() if hasattr(value, 'tolist') else value


def history(vis):
    return contents(vis/'HISTORY')


def copy_ms(source, destination, endian='aipsrc'):
    tool = table()
    tool.open(str(source))
    copied = tool.copy(str(destination), deep=True, valuecopy=True, endian=endian)
    copied.close()
    tool.close()


def run(root, native_bin):
    from casars import tasks as native
    assert casa.version_string() == '6.7.6.14'
    reference = inspect.signature(casa.applycal)
    actual = {name: parameter for name, parameter in inspect.signature(native.applycal).parameters.items()
              if parameter.kind != inspect.Parameter.KEYWORD_ONLY}
    assert list(actual) == list(reference.parameters)
    for name, parameter in reference.parameters.items():
        assert actual[name].kind == parameter.kind, name
        assert actual[name].default == parameter.default, name
    os.environ['CASARS_CALIBRATE_BIN'] = str(native_bin.resolve())
    os.environ['CASARS_REQUIRE_TASK_BINARIES'] = '1'
    os.environ['CASA_RS_CALIBRATION_PERF'] = '1'
    os.environ['CASA_RS_CALIBRATION_PERF_DIR'] = str(root/'profiles')
    os.environ['CASA_RS_CALIBRATION_TIMING'] = '1'
    (root/'profiles').mkdir()
    source, constant, solved = root/'source.ms', root/'constant.gcal', root/'solved.gcal'
    channelized = root/'channelized.bcal'
    make_cal_ms(str(source))
    casa.gencal(vis=str(source), caltable=str(constant), caltype='amp', parameter=[2.])
    casa.gaincal(vis=str(source), caltable=str(solved), refant='A0', solint='int')
    casa.bandpass(vis=str(source), caltable=str(channelized), refant='A0', solint='inf', minsnr=0.)
    tool = table()
    tool.open(str(channelized), nomodify=False)
    gains = tool.getcol('CPARAM')
    gains[:] = np.arange(1, gains.shape[1]+1)[None, :, None]
    tool.putcol('CPARAM', gains)
    flags = tool.getcol('FLAG')
    flags[:] = False
    flags[:, 4, :] = True
    tool.putcol('FLAG', flags)
    tool.close()
    phase_bandpass = root/'phase.bcal'
    descending_bandpass = root/'descending.bcal'
    for destination in [phase_bandpass, descending_bandpass]:
        copy_ms(channelized, destination)
    tool.open(str(phase_bandpass), nomodify=False)
    phases = np.deg2rad([0., 10., 20., 170., -170., -160., -150., -140.])
    phase_gains = np.ones_like(gains)
    phase_gains[:, :, tool.getcol('ANTENNA1') == 0] *= np.exp(1.j*phases)[None, :, None]
    tool.putcol('CPARAM', phase_gains)
    tool.close()
    tool.open(str(descending_bandpass), nomodify=False)
    tool.putcol('CPARAM', gains[:, ::-1, :])
    tool.putcol('FLAG', flags[:, ::-1, :])
    tool.close()
    tool.open(str(descending_bandpass/'SPECTRAL_WINDOW'), nomodify=False)
    tool.putcol('CHAN_FREQ', tool.getcol('CHAN_FREQ')[::-1, :])
    tool.putcol('CHAN_WIDTH', -tool.getcol('CHAN_WIDTH')[::-1, :])
    tool.close()
    unavailable = root/'unavailable.gcal'
    copy_ms(constant, unavailable)
    tool.open(str(unavailable), nomodify=False)
    tool.putcol('SPECTRAL_WINDOW_ID', np.ones_like(tool.getcol('SPECTRAL_WINDOW_ID')))
    tool.close()
    tool.open(str(unavailable/'SPECTRAL_WINDOW'), nomodify=False)
    tool.addrows(1)
    for column in tool.colnames():
        if tool.iscelldefined(column, 0):
            tool.putcell(column, 1, tool.getcell(column, 0))
    tool.close()
    tool.open(str(source), nomodify=False)
    if 'CORRECTED_DATA' in tool.colnames():
        tool.removecols(['CORRECTED_DATA'])
    tool.close()
    polarized_sources = {}
    for basis, correlations in [('linear', 'XX XY YX YY'), ('circular', 'RR RL LR LL')]:
        path = root/('source-'+basis+'.ms')
        make_cal_ms(str(path), stokes=correlations)
        tool.open(str(path), nomodify=False)
        data = tool.getcol('DATA')
        for corr, value in enumerate([1.+.2j, .3+.4j, -.2+.5j, 2.-.1j]):
            data[corr, :, :] = value*(np.arange(data.shape[1])+1)[:, None]
        tool.putcol('DATA', data)
        flags = tool.getcol('FLAG')
        flags[1, 2, 0] = True
        flags[2, 3, 1] = True
        tool.putcol('FLAG', flags)
        if 'CORRECTED_DATA' in tool.colnames():
            tool.removecols(['CORRECTED_DATA'])
        tool.close()
        polarized_sources[basis] = path

    selection_source = root/'source-selection.ms'
    copy_ms(source, selection_source)
    tool.open(str(selection_source/'STATE'), nomodify=False)
    state_start = tool.nrows()
    tool.addrows(2)
    for index, mode in enumerate(['CALIBRATE_PHASE#ON_SOURCE,CALIBRATE_WVR#ON_SOURCE',
                                  'OBSERVE_TARGET#ON_SOURCE']):
        for column, value in [('SIG', True), ('REF', False), ('CAL', 0.), ('LOAD', 0.),
                              ('SUB_SCAN', 0), ('OBS_MODE', mode), ('FLAG_ROW', False)]:
            tool.putcell(column, state_start+index, value)
    tool.close()
    tool.open(str(selection_source), nomodify=False)
    tool.putcol('STATE_ID', np.where(tool.getcol('FIELD_ID') == 0, state_start, state_start+1))
    tool.close()

    result = dict(casa_version=casa.version_string(), signature=str(reference), cases={}, failures=[],
                  timing_scope='Small-fixture public-task latency and CPU only; not dataset-scale acceptance')

    def case(name, extra=None, gain=constant, precorrected=False, repetitions=1, weight_seed=None, spectral_sigma=False, endian='aipsrc', descending_ms=False, input_vis=None):
        records, paths = {}, {}
        before_gain = snapshot(gain)
        for engine, module in [('casa', casa), ('native', native)]:
            path = root/f'{name}.{engine}.ms'
            paths[engine] = path
            copy_ms(input_vis or source, path, endian)
            if descending_ms:
                tool.open(str(path/'SPECTRAL_WINDOW'), nomodify=False)
                tool.putcol('CHAN_FREQ', tool.getcol('CHAN_FREQ')[::-1, :])
                tool.putcol('CHAN_WIDTH', -tool.getcol('CHAN_WIDTH')[::-1, :])
                tool.close()
            if precorrected:
                tool.open(str(path), nomodify=False)
                descriptor = tool.getcoldesc('DATA')
                descriptor['name'] = 'CORRECTED_DATA'
                descriptor['dataManagerType'] = 'StandardStMan'
                descriptor['dataManagerGroup'] = 'CorrectedDataTest'
                tool.addcols({'CORRECTED_DATA': descriptor})
                tool.putcol('CORRECTED_DATA', tool.getcol('DATA')*(7.+2.j))
                tool.close()
            if weight_seed is not None:
                tool.open(str(path), nomodify=False)
                for column, value in zip(['WEIGHT', 'SIGMA'], weight_seed):
                    tool.putcol(column, np.full_like(tool.getcol(column), value))
                tool.close()
            if spectral_sigma:
                tool.open(str(path), nomodify=False)
                data = tool.getcol('DATA')
                for column in ['WEIGHT_SPECTRUM', 'SIGMA_SPECTRUM']:
                    descriptor = tool.getcoldesc('DATA')
                    descriptor.update(name=column, valueType='float', dataManagerType='StandardStMan',
                                      dataManagerGroup=column+'Test')
                    tool.addcols({column: descriptor})
                    values = np.full(data.shape, 99., dtype=np.float32) if column == 'WEIGHT_SPECTRUM' else np.broadcast_to(np.linspace(1., 3., data.shape[1], dtype=np.float32)[None, :, None], data.shape).copy()
                    tool.putcol(column, values)
                tool.close()
            before = history(path)['TIME'].size
            arguments = dict(vis=str(path), gaintable=[str(gain)], flagbackup=False)
            arguments.update(extra or {})
            captured = io.StringIO()
            preload = os.environ.pop('LD_PRELOAD', None) if engine == 'native' else None
            cpu_start = sum(resource.getrusage(kind).ru_utime+resource.getrusage(kind).ru_stime
                            for kind in [resource.RUSAGE_SELF, resource.RUSAGE_CHILDREN])
            epoch_start = time.time()
            start = time.perf_counter()
            try:
                with contextlib.redirect_stderr(captured):
                    for _ in range(repetitions):
                        value = module.applycal(**arguments)
                record = dict(value=plain(value))
            except (ValueError, TypeError, RuntimeError, OSError) as error:
                record = dict(error=type(error).__name__, exception_bases=[kind.__name__ for kind in type(error).__mro__], message=str(error))
            finally:
                if preload is not None:
                    os.environ['LD_PRELOAD'] = preload
            record['wall_seconds'] = time.perf_counter()-start
            record['epoch_bounds'] = [epoch_start, time.time()]
            record['cpu_seconds'] = sum(resource.getrusage(kind).ru_utime+resource.getrusage(kind).ru_stime
                                        for kind in [resource.RUSAGE_SELF, resource.RUSAGE_CHILDREN])-cpu_start
            record['stderr'] = captured.getvalue()
            record['history'] = {column: plain(values[..., before:]) for column, values in history(path).items()}
            backup = Path(str(path)+'.flagversions')
            record['flag_versions'] = sorted(item.name for item in backup.glob('flags.*')) if backup.exists() else []
            records[engine] = record
        assert before_gain == snapshot(gain), f'{name}: input gain table mutated'
        def check(component, operation):
            try:
                operation()
            except (AssertionError, KeyError) as error:
                result['failures'].append(dict(case=name, component=component, message=str(error)))
        def compare_main():
            a, b = contents(paths['casa']), contents(paths['native'])
            assert a.keys() == b.keys(), (a.keys(), b.keys())
            equal(paths['casa'], paths['native'])
            for column in ['TIME', 'INTERVAL', 'TIME_CENTROID']:
                np.testing.assert_allclose(a[column], b[column], rtol=0, atol=1e-4, err_msg=column)
        def compare_return():
            assert records['casa'].get('value') == records['native'].get('value')
            assert ('error' in records['casa']) == ('error' in records['native']), records
            if 'error' in records['casa']:
                # casars.TaskExecutionError subclasses RuntimeError; callers'
                # CASA exception handlers must still catch the native exception.
                assert records['casa']['error'] in records['native']['exception_bases'], records
        def compare_history():
            left, right = records['casa']['history'], records['native']['history']
            assert len(left['TIME']) == len(right['TIME']), (len(left['TIME']), len(right['TIME']))
            for column in ['APPLICATION', 'ORIGIN', 'PRIORITY', 'OBSERVATION_ID', 'OBJECT_ID', 'APP_PARAMS', 'CLI_COMMAND']:
                np.testing.assert_array_equal(left[column], right[column], err_msg=column)
            normalized = {}
            for engine, record in records.items():
                bounds = np.asarray(record['epoch_bounds']) + 40587.*86400.
                times = np.asarray(record['history']['TIME'])
                assert np.all(np.isfinite(times))
                assert np.all((times >= bounds[0]-1.) & (times <= bounds[1]+1.)), (engine, times, bounds)
                messages = []
                for message in record['history']['MESSAGE']:
                    if message.startswith('version: '):
                        assert message.startswith('version: 6.7.6-14 ' if engine == 'casa' else 'version: casa-rs '), message
                        message = '<implementation version>'
                    messages.append(message.replace(str(paths[engine]), '<vis>'))
                normalized[engine] = messages
            assert normalized['casa'] == normalized['native'], normalized
        def compare_backup():
            assert len(records['casa']['flag_versions']) == len(records['native']['flag_versions'])
        for component, operation in [('return', compare_return), ('MAIN', compare_main),
                                     ('HISTORY', compare_history), ('flagbackup', compare_backup)]:
            check(component, operation)
        result['cases'][name] = dict(arguments=plain(extra or {}), repetitions=repetitions,
                                    weight_seed=weight_seed, spectral_sigma=spectral_sigma, endian=endian, descending_ms=descending_ms, records=records)
        (root/'results.json').write_text(json.dumps(result, indent=2)+'\n')
        print(name, 'FAILED' if any(item['case'] == name for item in result['failures']) else 'passed', flush=True)

    case('default')
    case('no-calwt', dict(calwt=False))
    case('repeat-apply', repetitions=2)
    case('chain-history', dict(gaintable=[str(constant), str(constant)], calwt=[True], interp=['', 'nearest'], spwmap=[0]))
    case('blank-applymode', dict(applymode=''))
    case('weight-from-sigma', weight_seed=(3., 2.))
    case('spectral-sigma', spectral_sigma=True)
    case('calonly', dict(applymode='calonly'))
    case('trial', dict(applymode='trial'))
    case('field', dict(field='PHASE'))
    case('preserve-unselected', dict(field='PHASE'), precorrected=True)
    case('channels', dict(spw='0:2~5'))
    case('channels-field', dict(field='PHASE', spw='0:2~5'), precorrected=True)
    case('channels-overlap', dict(spw='0:0~2;2~4'), precorrected=True, spectral_sigma=True)
    case('channels-existing', dict(spw='0:0~2'), precorrected=True)
    case('channels-spectral', dict(spw='0:0~2'), precorrected=True, spectral_sigma=True)
    case('channels-stride', dict(spw='0:0~6^2'), precorrected=True, spectral_sigma=True)
    case('channels-calonly', dict(spw='0:0~6^2', applymode='calonly'), precorrected=True)
    case('channels-no-calwt', dict(spw='0:0~6^2', calwt=False), precorrected=True, spectral_sigma=True)
    case('channels-trial', dict(spw='0:0~6^2', applymode='trial'), precorrected=True, spectral_sigma=True)
    case('channels-bandpass', dict(spw='0:2~6^2'), gain=channelized, precorrected=True)
    case('channels-bandpass-no-calwt', dict(spw='0:2~6^2', calwt=False), gain=channelized, precorrected=True)
    case('bandpass-full', gain=channelized)
    case('channels-bandpass-spectral', dict(spw='0:2~6^2'), gain=channelized, precorrected=True, spectral_sigma=True)
    case('channels-bandpass-phase', dict(spw='0:2~6^2'), gain=phase_bandpass, precorrected=True, spectral_sigma=True)
    case('channels-bandpass-descending', dict(spw='0:2~6^2'), gain=descending_bandpass, precorrected=True)
    for calwt in [False, True]:
        case('channels-bandpass-ms-descending-'+str(calwt), dict(spw='0:0~6^2', interp=['nearest,linear'], calwt=calwt), gain=channelized, precorrected=True, descending_ms=True)
    for endian in ['big', 'little']:
        case('channels-'+endian, dict(spw='0:0~6^2'), precorrected=True, spectral_sigma=True, endian=endian)
    case('flagbackup', dict(flagbackup=True))
    case('solved-gains', gain=solved)
    for mode in ['calflagstrict', 'flagonly', 'flagonlystrict']:
        case(mode, dict(applymode=mode), gain=solved)
        case(mode+'-existing', dict(applymode=mode, flagbackup=True), gain=solved, precorrected=True)
        case(mode+'-channels', dict(applymode=mode, spw='0:0~6^2'), gain=solved, precorrected=True, spectral_sigma=True)
    for mode in ['calflag', 'calonly', 'calflagstrict', 'flagonly', 'flagonlystrict', 'trial']:
        case('unavailable-'+mode, dict(applymode=mode), gain=unavailable, precorrected=True, weight_seed=(3., 2.), spectral_sigma=True)
    case('unavailable-new', gain=unavailable)
    case('parang-alone', dict(gaintable=[], parang=True))
    case('parang-alone-trial', dict(gaintable=[], parang=True, applymode='trial'))
    for basis, input_vis in polarized_sources.items():
        case('parang-'+basis, dict(gaintable=[], parang=True), input_vis=input_vis)
        case('parang-'+basis+'-channels', dict(gaintable=[], parang=True, spw='0:0~6^2'), input_vis=input_vis, precorrected=True)
        for mode in ['trial', 'flagonly']:
            case('parang-'+basis+'-'+mode, dict(gaintable=[], parang=True, applymode=mode), input_vis=input_vis)
    for name, selector in [
            ('phase-exact', 'CALIBRATE_PHASE#ON_SOURCE'),
            ('phase-prefix', 'CALIBRATE_PHASE'),
            ('phase-glob', '*PHASE*'),
            ('calibrate', 'CALIBRATE*'),
            ('target', 'OBSERVE_TARGET#ON_SOURCE'),
            ('union', 'CALIBRATE_PHASE*,OBSERVE_TARGET*'),
            ('state-id', str(state_start))]:
        case('selection-intent-'+name, dict(intent=selector), input_vis=selection_source, precorrected=True)
    for name, selector in [('less-m', '<35m'), ('greater-m', '>35m'),
                           ('range-m', '20~50m'), ('range-km', '0.02~0.05km'),
                           ('lambda', '<15000lambda'), ('klambda', '<15klambda'),
                           ('union', '0~25m,40~60m')]:
        case('selection-uv-'+name, dict(uvrange=selector), input_vis=selection_source, precorrected=True)
    case('selection-combined', dict(intent='*PHASE*', uvrange='<50m', spw='0:2~5', antenna='A0&'), input_vis=selection_source, precorrected=True)
    case('selection-new-corrected', dict(intent='*PHASE*', uvrange='<50m'), input_vis=selection_source)
    case('selection-disabled', dict(selectdata=False, intent='*PHASE*', uvrange='invalid', timerange='invalid', antenna='invalid', scan='invalid', observation='invalid', msselect='invalid'), input_vis=selection_source, precorrected=True)
    case('selection-disabled-field-spw', dict(selectdata=False, field='PHASE', intent='OBSERVE_TARGET*', spw='0:2~5', uvrange='invalid'), input_vis=selection_source, precorrected=True)
    for name, arguments in [
            ('unknown-intent', dict(intent='NO_SUCH_INTENT')),
            ('invalid-uv', dict(uvrange='invalid')),
            ('empty-uv', dict(uvrange='>1000000000m')),
            ('empty-intersection', dict(intent='*PHASE*', field='PHASE')),
            ('disabled-intent-active', dict(selectdata=False, intent='NO_SUCH_INTENT')),
            ('backup-before-error', dict(uvrange='invalid', flagbackup=True)),
            ('trial-error', dict(uvrange='invalid', applymode='trial'))]:
        case('selection-'+name, arguments, input_vis=selection_source)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--native-bin', type=Path, required=True)
    args = parser.parse_args()
    root = args.work.resolve()
    root.mkdir(parents=True, exist_ok=True)
    assert not any(root.iterdir()), 'work directory must be empty'
    native_bin = args.native_bin.resolve()
    os.chdir(root)
    result = run(root, native_bin)
    assert not result['failures'], [(item['case'], item['component'], item['message'][:180]) for item in result['failures']]


if __name__ == '__main__':
    main()
