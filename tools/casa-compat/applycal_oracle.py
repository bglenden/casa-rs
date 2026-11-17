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
import re
import subprocess
import sys
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


def run(root, native_bin, only=None):
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
    tool.addrows(4)
    for index, mode in enumerate(['CALIBRATE_PHASE#ON_SOURCE,CALIBRATE_WVR#ON_SOURCE',
                                  'OBSERVE_TARGET#ON_SOURCE', 'none',
                                  'CALIBRATE_PHASE#ON_SOURCE']):
        for column, value in [('SIG', True), ('REF', False), ('CAL', 0.), ('LOAD', 0.),
                              ('SUB_SCAN', 0), ('OBS_MODE', mode), ('FLAG_ROW', index == 3)]:
            tool.putcell(column, state_start+index, value)
    tool.close()
    tool.open(str(selection_source), nomodify=False)
    states = np.where(tool.getcol('FIELD_ID') == 0, state_start, state_start+1)
    states[:2] = [state_start+2, state_start+3]
    tool.putcol('STATE_ID', states)
    tool.close()

    chain_source = root/'source-chain.ms'
    copy_ms(source, chain_source)
    tool.open(str(chain_source), nomodify=False)
    tool.putcol('FIELD_ID', np.ones_like(tool.getcol('FIELD_ID')))
    tool.close()
    time_gain = root/'time-fields.gcal'
    copy_ms(solved, time_gain)
    tool.open(str(time_gain), nomodify=False)
    fields, antennas, times = (tool.getcol(column) for column in ['FIELD_ID', 'ANTENNA1', 'TIME'])
    values = tool.getcol('CPARAM')
    for row, (field, antenna, epoch) in enumerate(zip(fields, antennas, times)):
        amplitude = (2. if field == 0 else 4.) + .15*np.sin((epoch-times.min())/13.)
        phase = (.3 if field == 0 else -.5) if antenna == 0 else 0.
        values[:, :, row] = amplitude*np.exp(1.j*phase)
    tool.putcol('CPARAM', values)
    tool.putcol('FLAG', np.zeros_like(tool.getcol('FLAG')))
    tool.putcol('TIME', times+5.)
    tool.close()

    # Separate contexts and shifted grids make interpolation modifiers observable.
    observation_source, observation_gain = root/'source-observations.ms', root/'observations.gcal'
    for original, destination in [(chain_source, observation_source), (time_gain, observation_gain)]:
        copy_ms(original, destination)
        tool.open(str(destination/'OBSERVATION'), nomodify=False)
        tool.addrows(1)
        for column in tool.colnames():
            if tool.iscelldefined(column, 0):
                tool.putcell(column, 1, tool.getcell(column, 0))
        tool.close()
        tool.open(str(destination), nomodify=False)
        tool.putcol('OBSERVATION_ID', (tool.getcol('SCAN_NUMBER') == 2).astype(np.int32))
        tool.close()
    shifted_source = root/'source-shifted.ms'
    copy_ms(chain_source, shifted_source)
    tool.open(str(shifted_source/'SPECTRAL_WINDOW'), nomodify=False)
    frequencies = tool.getcol('CHAN_FREQ')
    tool.putcol('CHAN_FREQ', frequencies + .35*(frequencies[1]-frequencies[0]))
    tool.close()
    phase_delay_source = root/'source-phase-delay.ms'
    copy_ms(chain_source, phase_delay_source)
    tool.open(str(phase_delay_source/'SPECTRAL_WINDOW'), nomodify=False)
    tool.putcol('CHAN_FREQ', tool.getcol('CHAN_FREQ')*2.)
    tool.putcol('REF_FREQUENCY', tool.getcol('REF_FREQUENCY')*2.)
    tool.close()
    curved_bandpass, flagged_curved_bandpass = root/'curved.bcal', root/'curved-flagged.bcal'
    for destination, flagged in [(curved_bandpass, False), (flagged_curved_bandpass, True)]:
        copy_ms(channelized, destination)
        tool.open(str(destination), nomodify=False)
        values = tool.getcol('CPARAM')
        x = np.arange(values.shape[1])
        values[:] = (1.5+.1*x+.025*x*x)[None, :, None]
        values[:, :, tool.getcol('ANTENNA1') == 0] *= np.exp(1.j*(.12*x+.03*x*x))[None, :, None]
        tool.putcol('CPARAM', values)
        flags = np.zeros_like(tool.getcol('FLAG'))
        if flagged:
            flags[:, [3, 6], :] = True
        tool.putcol('FLAG', flags)
        tool.close()

    from fluxscale_oracle import multi_window
    multi_source, multi_gain = multi_window(chain_source, time_gain, root, name='chain-multi')
    tool.open(str(multi_source/'DATA_DESCRIPTION'), nomodify=False)
    descriptor_row = {column: tool.getcell(column, 0) for column in tool.colnames() if tool.iscelldefined(column, 0)}
    tool.addrows(2)
    for index in [1, 2]:
        for column, value in descriptor_row.items():
            tool.putcell(column, index, index if column == 'SPECTRAL_WINDOW_ID' else value)
    tool.close()
    tool.open(str(multi_source), nomodify=False)
    tool.putcol('DATA_DESC_ID', np.arange(tool.nrows(), dtype=np.int32)%3)
    tool.close()
    missing_window_gain = root/'missing-window.gcal'
    selected_window_gain = root/'selected-window.gcal'
    missing_antenna_gain = root/'missing-antenna.gcal'
    tied_gain = root/'tied-fields.gcal'
    for destination in [missing_window_gain, selected_window_gain]:
        copy_ms(multi_gain, destination)
        tool.open(str(destination), nomodify=False)
        windows, fields = tool.getcol('SPECTRAL_WINDOW_ID'), tool.getcol('FIELD_ID')
        remove = windows == 2 if destination == missing_window_gain else windows != fields
        tool.removerows(np.flatnonzero(remove).tolist())
        tool.close()
    copy_ms(observation_gain, missing_antenna_gain)
    tool.open(str(missing_antenna_gain), nomodify=False)
    tool.removerows(np.flatnonzero((tool.getcol('ANTENNA1') == 1) & (tool.getcol('OBSERVATION_ID') == 1)).tolist())
    tool.close()
    copy_ms(time_gain, tied_gain)
    tool.open(str(tied_gain/'FIELD'), nomodify=False)
    for column in ['PHASE_DIR', 'REFERENCE_DIR', 'DELAY_DIR']:
        tool.putcell(column, 1, tool.getcell(column, 0))
    tool.close()
    scalar_bandpass = root/'scalar.bcal'
    copy_ms(curved_bandpass, scalar_bandpass)
    tool.open(str(scalar_bandpass), nomodify=False)
    for column in ['CPARAM', 'FLAG', 'PARAMERR', 'SNR', 'WEIGHT']:
        if column in tool.colnames() and tool.iscelldefined(column, 0):
            tool.putcol(column, tool.getcol(column)[:, :1, :])
    values = tool.getcol('CPARAM'); values[:] = 1.
    values[:, :, tool.getcol('ANTENNA1') == 0] = np.exp(.4j)
    tool.putcol('CPARAM', values)
    tool.close()
    tool.open(str(scalar_bandpass/'SPECTRAL_WINDOW'), nomodify=False)
    for column in ['CHAN_FREQ', 'CHAN_WIDTH', 'EFFECTIVE_BW', 'RESOLUTION']:
        tool.putcol(column, tool.getcol(column)[:1, :])
    tool.putcol('NUM_CHAN', np.ones_like(tool.getcol('NUM_CHAN')))
    tool.close()
    compressed_source, compressed_bandpass = root/'source-compressed.ms', root/'compressed.bcal'
    make_cal_ms(str(compressed_source), channels=3)
    tool.open(str(compressed_source/'SPECTRAL_WINDOW'), nomodify=False)
    tool.putcell('CHAN_FREQ', 0, np.array([1., 2., 4.])*1e9)
    tool.close()
    tool.open(str(compressed_source), nomodify=False)
    if 'CORRECTED_DATA' in tool.colnames():
        tool.removecols(['CORRECTED_DATA'])
    tool.close()
    copy_ms(curved_bandpass, compressed_bandpass)
    tool.open(str(compressed_bandpass), nomodify=False)
    for column in ['CPARAM', 'FLAG', 'PARAMERR', 'SNR', 'WEIGHT']:
        if column in tool.colnames() and tool.iscelldefined(column, 0):
            tool.putcol(column, tool.getcol(column)[:, :4, :])
    flags = tool.getcol('FLAG'); flags[:, 2, :] = True; tool.putcol('FLAG', flags)
    tool.close()
    tool.open(str(compressed_bandpass/'SPECTRAL_WINDOW'), nomodify=False)
    for column in ['CHAN_WIDTH', 'EFFECTIVE_BW', 'RESOLUTION']:
        tool.putcol(column, tool.getcol(column)[:4, :])
    tool.putcell('CHAN_FREQ', 0, np.arange(1., 5.)*1e9)
    tool.putcol('NUM_CHAN', np.full_like(tool.getcol('NUM_CHAN'), 4))
    tool.close()

    result = dict(casa_version=casa.version_string(), signature=str(reference), case_filter=only, cases={}, failures=[],
                  timing_scope='Small-fixture public-task latency and CPU only; not dataset-scale acceptance')

    def case(name, extra=None, gain=constant, precorrected=False, repetitions=1, weight_seed=None, spectral_sigma=False, endian='aipsrc', descending_ms=False, input_vis=None):
        if only is not None and re.search(only, name) is None:
            return
        records, paths = {}, {}
        supplied_tables = (extra or {}).get('gaintable', [str(gain)])
        supplied_tables = [supplied_tables] if isinstance(supplied_tables, str) else supplied_tables
        before_gain = {Path(path): snapshot(Path(path)) if Path(path).exists() else None
                       for path in supplied_tables if path}
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
            except Exception as error:
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
        for path, before in before_gain.items():
            assert before == (snapshot(path) if path.exists() else None), f'{name}: input table {path} mutated'
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
            ('state-id', str(state_start)),
            ('quoted-pattern', '"CALIBRATE_{PHASE,WVR}*"'),
            ('regex', '/CALIBRATE_(PHASE|WVR)#ON_SOURCE/'),
            ('intersection', 'CALIBRATE_PHASE*&*WVR*'),
            ('state-range', f'{state_start}~{state_start+3}'),
            ('bounds', f'>{state_start}&<{state_start+3}'),
            ('less-bounds-with-flag', f'<{state_start+4}'),
            ('greater-bounds-with-flag', f'>{state_start+1}'),
            ('bounds-with-flag', f'>{state_start}&<{state_start+4}'),
            ('parentheses', f'({state_start},{state_start+1})'),
            ('flagged-id', str(state_start+3)),
            ('literal-none', 'none'),
            ('partial-name-list', 'CALIBRATE_PHASE*,NO_SUCH_INTENT'),
            ('partial-number-list', f'{state_start},999'),
            ('spaces', 'CALIBRATE_PHASE#ON_SOURCE, OBSERVE_TARGET#ON_SOURCE')]:
        case('selection-intent-'+name, dict(intent=selector), input_vis=selection_source, precorrected=True)
    for name, selector in [('less-m', '<35m'), ('greater-m', '>35m'),
                           ('range-m', '20~50m'), ('range-km', '0.02~0.05km'),
                           ('lambda', '<15000lambda'), ('klambda', '<15klambda'),
                           ('union', '0~25m,40~60m'),
                           ('default-unit', '20~50'),
                           ('spaces', ' 20~50 m '),
                           ('mixed-units', '0~25m,15~20klambda')]:
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
            ('missing-table-backup', dict(gaintable=[str(root/'missing.gcal')], flagbackup=True)),
            ('literal-none-uv', dict(uvrange='none')),
            ('literal-auto-intent', dict(intent='auto')),
            ('invalid-intent-logic', dict(intent=f'{state_start}&CALIBRATE_PHASE*')),
            ('empty-intent-bound-union', dict(intent=f'>{state_start}&<{state_start+1},{state_start}')),
            ('trial-error', dict(uvrange='invalid', applymode='trial'))]:
        case('selection-'+name, arguments, input_vis=selection_source)
    case('chain-default-fields', gain=time_gain, input_vis=chain_source, precorrected=True)
    for name, selector in [('id0', '0'), ('id1', '1'), ('name0', 'FLUX'), ('name1', 'PHASE'),
                           ('glob', 'FL*'), ('all', '*'), ('list', '0,1'), ('range', '0~1'),
                           ('nearest', 'nearest'), ('uppercase-name', 'NEAREST')]:
        case('chain-gainfield-'+name, dict(gainfield=[selector]), gain=time_gain, input_vis=chain_source, precorrected=True)
    case('chain-gainfield-scalar', dict(gainfield='FLUX'), gain=time_gain, input_vis=chain_source, precorrected=True)
    for method in ['nearest', 'linear', 'cubic', 'nearestperobs', 'linearperobs',
                   'nearestperscan', 'linearperscan', 'linearperobsperscan', 'linearPD']:
        case('chain-time-'+method, dict(interp=[method]), gain=time_gain, input_vis=chain_source, precorrected=True)
    case('chain-spwmap-identity', dict(spwmap=[0]), gain=time_gain, input_vis=chain_source, precorrected=True)
    case('chain-spwmap-too-long', dict(spwmap=[0, 0]), gain=time_gain, input_vis=chain_source, precorrected=True)
    case('chain-two-tables', dict(gaintable=[str(time_gain), str(constant)], gainfield=['0,1', ''], interp=['linear', 'nearest'], calwt=[True, False]), gain=time_gain, input_vis=chain_source, precorrected=True)
    case('chain-spwmap-remap', dict(spwmap=[1]), gain=unavailable, input_vis=chain_source, precorrected=True)
    for method in ['nearest', 'linear', 'cubic', 'nearestperobs', 'linearperobs']:
        case('chain-observations-'+method, dict(interp=[method]), gain=observation_gain, input_vis=observation_source, precorrected=True)
    case('chain-observations-mismatch', dict(interp=['linearperobs']), gain=observation_gain, input_vis=chain_source, precorrected=True)
    for method in ['nearestPD', 'linearPD', 'cubicPD']:
        case('chain-phase-delay-'+method, dict(interp=[method]), gain=time_gain, input_vis=phase_delay_source, precorrected=True)
    case('chain-phase-delay-channel-selection', dict(interp=['linearPD'], spw='0:1~3'), gain=time_gain, input_vis=phase_delay_source, precorrected=True)
    for method in ['nearest', 'linear', 'cubic', 'spline', 'nearestflag', 'linearflag', 'cubicflag', 'splineflag']:
        case('chain-frequency-'+method, dict(interp=['linear,'+method]), gain=flagged_curved_bandpass if 'flag' in method else curved_bandpass, input_vis=shifted_source, precorrected=True)
    case('chain-frequency-descending', dict(interp=['linear,cubic']), gain=curved_bandpass, input_vis=shifted_source, descending_ms=True, precorrected=True)
    case('chain-frequency-relative', dict(interp=['linear,linearrel']), gain=curved_bandpass, input_vis=phase_delay_source, precorrected=True)
    for method in ['LINEAR', 'aipslin', 'linear,aipslin', 'bogus']:
        case('chain-interpolation-invalid-'+method.replace(',', '-'), dict(interp=[method]), gain=time_gain, input_vis=chain_source)
    case('chain-frequency-unknown-linear-fallback', dict(interp=['linear,unknown']), gain=curved_bandpass, input_vis=shifted_source, precorrected=True)
    case('review-scalar-bandpass-PD', dict(interp=['linearPD']), gain=scalar_bandpass, input_vis=phase_delay_source, precorrected=True)
    case('review-nearest-perobs-scope', dict(gainfield=['nearest'], interp=['linearperobs']), gain=observation_gain, input_vis=observation_source, precorrected=True)
    case('review-nearest-field-tie', dict(gainfield=['nearest']), gain=tied_gain, input_vis=chain_source)
    for name, mapping in [('broadcast', [0]), ('prefix', [1, 0]), ('auto', [-999]), ('identity', [-1]), ('invalid-unselected', [0, 999]), ('negative-unselected', [0, -1])]:
        case('review-multispw-'+name, dict(spwmap=mapping, spw='0' if 'unselected' in name else ''), gain=multi_gain, input_vis=multi_source, precorrected=True)
    for mode in ['calflag', 'calflagstrict', 'calonly']:
        case('review-map-empty-window-'+mode, dict(spwmap=[2], applymode=mode), gain=missing_window_gain, input_vis=multi_source, precorrected=True)
        case('review-selected-window-'+mode, dict(gainfield=['0'], spw='1', applymode=mode), gain=selected_window_gain, input_vis=multi_source, precorrected=True)
        case('review-missing-antenna-'+mode, dict(interp=['linearperobs'], applymode=mode), gain=missing_antenna_gain, input_vis=observation_source, precorrected=True)
    for method in ['cubic', 'cubicflag']:
        case('review-compressed-grid-'+method, dict(interp=['linear,'+method]), gain=compressed_bandpass, input_vis=compressed_source, precorrected=True)
    if only is not None:
        return result
    # CASA crashes when perscan is combined with multiple observation IDs.
    # Its documented/source partition is only by scan. Compare native results
    # to the same samples/scan series under CASA's working one-observation control.
    result['casa_crash_controls'] = {}
    for method in ['nearestperscan', 'linearperscan', 'linearperobsperscan', 'spline']:
        crash_vis = root/('crash-'+method+'.ms')
        copy_ms(observation_source if method != 'spline' else chain_source, crash_vis)
        arguments = dict(vis=str(crash_vis), gaintable=[str(observation_gain if method != 'spline' else time_gain)], interp=[method], flagbackup=False)
        probe = subprocess.run([sys.executable, '-c',
            'import json,resource,sys; resource.setrlimit(resource.RLIMIT_CORE,(0,0)); '
            'from casatasks import applycal; applycal(**json.load(sys.stdin))'],
            input=json.dumps(arguments), text=True, capture_output=True, timeout=45)
        assert probe.returncode == -11, (method, probe.returncode, probe.stderr)
        record = dict(casa_returncode=probe.returncode, casa_arguments=arguments,
                      casa_stderr=probe.stderr, corrected_casa_crash=True)
        if method != 'spline':
            native_vis = root/('native-control-'+method+'.ms')
            copy_ms(observation_source, native_vis)
            preload = os.environ.pop('LD_PRELOAD', None)
            try:
                native.applycal(vis=str(native_vis), gaintable=[str(observation_gain)], interp=[method], flagbackup=False)
            finally:
                if preload is not None:
                    os.environ['LD_PRELOAD'] = preload
            reference_vis = root/('chain-time-'+method+'.casa.ms')
            native_columns = contents(native_vis)
            equal(reference_vis, native_vis, [column for column in native_columns if column != 'OBSERVATION_ID'])
            np.testing.assert_array_equal(native_columns['OBSERVATION_ID'], contents(observation_source)['OBSERVATION_ID'])
            record.update(native_matches_casa_single_observation_control=True,
                          reference_vis=str(reference_vis), native_vis=str(native_vis))
        else:
            record['native_reference'] = 'apply_execute::spline_time_apply_matches_the_closed_form_natural_curve'
        result['casa_crash_controls'][method] = record
        (root/'results.json').write_text(json.dumps(result, indent=2)+'\n')
    return result
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--native-bin', type=Path, required=True)
    parser.add_argument('--only', help='Run case names matching this regular expression; crash controls require the full corpus')
    args = parser.parse_args()
    root = args.work.resolve()
    root.mkdir(parents=True, exist_ok=True)
    assert not any(root.iterdir()), 'work directory must be empty'
    native_bin = args.native_bin.resolve()
    os.chdir(root)
    result = run(root, native_bin, args.only)
    assert not result['failures'], f"{len(result['failures'])} failed comparisons; see {root/'results.json'}"


if __name__ == '__main__':
    main()
