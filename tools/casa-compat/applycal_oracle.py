#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Same-input CASA/public-Python applycal comparisons, with hard failures.

The initial small corpus establishes numerical, mutation and API/state gaps.
Its recorded latencies are not dataset-scale performance acceptance.
"""
from __future__ import annotations

import argparse
import contextlib
from copy import deepcopy
import inspect
import io
import json
import os
from pathlib import Path
import resource
import shutil
import re
import subprocess
import sys
import time

import numpy as np
import casatasks as casa
from casatools import measures, table

from calibration_lifecycle_oracle import contents, equal, snapshot
from record_casa_returns import make_cal_ms


def plain(value):
    if isinstance(value, Path):
        return str(value)
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


def geometry_measure_fixtures(base, root):
    field_ms=root/'source-mscal-direction.ms'
    copy_ms(base, field_ms)
    t=table()
    t.open(str(field_ms/'FIELD'), nomodify=False)
    for name in ('CUSTOM_DIR', 'NO_DIRECTION', 'PLUTO'):
        t.addcols({name: {'valueType': 'double', 'ndim': 2, 'shape': [2, 1]}})
        t.putcol(name, np.broadcast_to(np.array([10., -20.])[:,None,None], (2, 1, t.nrows())).copy())
        if name != 'NO_DIRECTION':
            t.putcolkeyword(name, 'MEASINFO', {'type': 'direction', 'Ref': 'J2000'})
            t.putcolkeyword(name, 'QuantumUnits', ['deg', 'deg'])
    t.addcols({'DIR_1D': {'valueType': 'double', 'ndim': 1, 'shape': [2]}})
    t.putcol('DIR_1D', np.broadcast_to(np.array([10., -20.])[:, None], (2, t.nrows())).copy())
    t.putcolkeyword('DIR_1D', 'MEASINFO', {'type': 'direction', 'Ref': 'J2000'})
    t.putcolkeyword('DIR_1D', 'QuantumUnits', ['deg', 'deg'])
    t.close()

    time_ms=root/'source-mscal-time-ref.ms'
    copy_ms(base, time_ms)
    t=table()
    t.open(str(time_ms), nomodify=False)
    t.addcols({'TIME_FRAME': {'valueType': 'int'}})
    time=t.getcol('TIME')
    times=np.unique(time)
    codes=np.where(np.isin(time, times[::2]), 7, 6).astype(np.int32)
    t.putcol('TIME_FRAME', codes)
    t.putcolkeyword('TIME', 'MEASINFO', {'type':'epoch', 'VarRefCol':'TIME_FRAME',
                  'TabRefTypes':['UTC','TAI'], 'TabRefCodes':np.array([6,7],dtype=np.uint32)})
    t.close()

    antenna_ms=root/'source-mscal-antenna-ref.ms'
    copy_ms(base, antenna_ms)
    tool = measures()
    t=table()
    t.open(str(antenna_ms/'ANTENNA'), nomodify=False)
    positions=t.getcol('POSITION')
    for i in range(t.nrows()):
        coord = tool.position('ITRF', *(f'{value}m' for value in positions[:,i]))
        wgs=tool.measure(coord, 'WGS84')
        lon, lat, height = (wgs[key]['value'] for key in ('m0', 'm1', 'm2'))
        positions[:,i] = height*np.array([np.cos(lat)*np.cos(lon),np.cos(lat)*np.sin(lon),np.sin(lat)])
    t.putcol('POSITION', positions)
    t.putcolkeyword('POSITION', 'MEASINFO', {'type': 'position', 'Ref': 'WGS84'})
    t.close()
    tool.done()
    return field_ms, time_ms, antenna_ms

def isolated_applycal(arguments):
    return subprocess.run([sys.executable, '-c',
        'import json,resource,sys; resource.setrlimit(resource.RLIMIT_CORE,(0,0)); '
        'from casatasks import applycal; applycal(**json.load(sys.stdin))'],
        input=json.dumps(arguments), text=True, capture_output=True, timeout=45)


def bpoly_nearest_control(root, source, poly, native):
    paths = {engine: root/f'family-BPOLY-nearest.{engine}.ms'
             for engine in ['crash', 'casa-control', 'native']}
    for path in paths.values():
        copy_ms(source, path)
    arguments = dict(vis=str(paths['crash']), gaintable=[str(poly)], gainfield=['nearest'], flagbackup=False)
    probe = isolated_applycal(arguments)
    assert probe.returncode in [-6, -11], (probe.returncode, probe.stderr)
    assert casa.applycal(vis=str(paths['casa-control']), gaintable=[str(poly)], flagbackup=False) is None
    preload = os.environ.pop('LD_PRELOAD', None)
    try:
        assert native.applycal(**dict(arguments, vis=str(paths['native']))) is None
    finally:
        if preload is not None:
            os.environ['LD_PRELOAD'] = preload
    equal(paths['casa-control'], paths['native'])
    return dict(casa_returncode=probe.returncode, casa_arguments=arguments,
                casa_stderr=probe.stderr, corrected_casa_crash=True,
                native_matches_casa_unscoped_control=True)


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

    scalar_time_gain = root/'time-scalar-t.gcal'
    casa.gaincal(vis=str(source), caltable=str(scalar_time_gain), refant='A0', solint='int', gaintype='T', minsnr=0.)
    tool.open(str(scalar_time_gain), nomodify=False)
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

    family_tables = {}
    for label, caltype, parameters, antennas in [
            ('K', 'sbd', [5.], 'A0'),
            ('antpos', 'antpos', [.001, .002, .003], 'A0'),
            ('opacity', 'opac', [.15], '')]:
        path = root/('family-'+label+'.cal')
        casa.gencal(vis=str(source), caltable=str(path), caltype=caltype, parameter=parameters, antenna=antennas)
        family_tables[label] = path
    vla_source = root/'source-vla.ms'
    make_cal_ms(str(vla_source), stokes='RR LL', telescope='VLA', frequency='1.5GHz', antenna_names=['ea01','ea02','ea03','ea04','ea05'])
    gaincurve = root/'family-gaincurve.cal'
    casa.gencal(vis=str(vla_source), caltable=str(gaincurve), caltype='gc')
    family_tables['gaincurve'] = gaincurve
    poly = root/'family-poly.bcal'
    casa.bandpass(vis=str(source), caltable=str(poly), bandtype='BPOLY', refant='A0', solint='inf', minsnr=0., degamp=1, degphase=1, maskedge=0)
    family_tables['BPOLY'] = poly
    varying_family_tables = {}
    for label, original in family_tables.items():
        if label == 'BPOLY':
            continue
        destination = root/('family-'+label+'-time.cal')
        copy_ms(original, destination)
        input_ms = vla_source if label == 'gaincurve' else source
        tool.open(str(input_ms))
        epoch = float(tool.getcol('TIME').min())-5.
        tool.close()
        tool.open(str(destination), nomodify=False)
        count = tool.nrows()
        records = [{column: tool.getcell(column, row) for column in tool.colnames() if tool.iscelldefined(column, row)} for row in range(count)]
        tool.addrows(count*3)
        for knot, multiplier in enumerate([1., 2., .5, 1.25]):
            for row, record in enumerate(records):
                index = knot*count+row
                for column, value in record.items():
                    tool.putcell(column, index, value)
                tool.putcell('TIME', index, epoch+40.*knot)
                tool.putcell('FPARAM', index, record['FPARAM']*multiplier)
        tool.close()
        varying_family_tables[label] = destination

    result = dict(casa_version=casa.version_string(), signature=str(reference), case_filter=only, cases={}, failures=[],
                  timing_scope='Small-fixture public-task latency and CPU only; not dataset-scale acceptance')

    def case(name, extra=None, gain=constant, precorrected=False, repetitions=1, weight_seed=None, spectral_sigma=False, endian='aipsrc', descending_ms=False, input_vis=None, casa_control_callib=None, positional=False, vis_as_list=False, casa_control_msselect=None):
        if only is not None and re.search(only, name) is None:
            return
        records, paths = {}, {}
        supplied_tables = (extra or {}).get('gaintable', [str(gain)])
        if isinstance(supplied_tables, (str, Path)):
            supplied_tables = [supplied_tables]
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
                if 'CORRECTED_DATA' not in tool.colnames():
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
            arguments = dict(vis=[str(path)] if vis_as_list else str(path), gaintable=[str(gain)], flagbackup=False)
            arguments.update(deepcopy(extra or {}))
            if casa_control_msselect is not None and engine == 'casa':
                arguments['msselect'] = casa_control_msselect
            captured = io.StringIO()
            preload = os.environ.pop('LD_PRELOAD', None) if engine == 'native' else None
            cpu_start = sum(resource.getrusage(kind).ru_utime+resource.getrusage(kind).ru_stime
                            for kind in [resource.RUSAGE_SELF, resource.RUSAGE_CHILDREN])
            epoch_start = time.time()
            start = time.perf_counter()
            try:
                with contextlib.redirect_stderr(captured):
                    for _ in range(repetitions):
                        if positional:
                            value = module.applycal(*(arguments.get(key, deepcopy(param.default))
                                                      for key, param in reference.parameters.items()))
                        else:
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
            if name == 'review-nearest-field-tie':
                # Sort::Ascending=-1 enables NoDuplicates in CASA's mistaken
                # options mask, so its explicit ambiguity guard never fires.
                # Native honors that evident intent and refuses before correction.
                original = contents(input_vis or source)
                for column in original:
                    np.testing.assert_array_equal(b[column], original[column], err_msg=column)
                np.testing.assert_array_equal(b['CORRECTED_DATA'], b['DATA'])
                return
            if casa_control_callib is not None:
                # The raw joint-map task skipped every valid buffer. Keep that
                # result, and use explicitly constrained CASA slices as the
                # independent reference for the intended joint partition.
                np.testing.assert_allclose(a['CORRECTED_DATA'], a['DATA']*(7.+2.j), rtol=1e-6)
                control = root/(name+'.casa-control.ms')
                copy_ms(paths['casa'], control)
                casa.applycal(vis=str(control), docallib=True, callib=str(casa_control_callib), flagbackup=False)
                equal(control, paths['native'])
                return
            equal(paths['casa'], paths['native'])
            for column in ['TIME', 'INTERVAL', 'TIME_CENTROID']:
                np.testing.assert_allclose(a[column], b[column], rtol=0, atol=1e-4, err_msg=column)
        def compare_return():
            if name == 'review-nearest-field-tie':
                assert 'error' not in records['casa']
                assert 'RuntimeError' in records['native']['exception_bases']
                assert 'minimum angular distance' in records['native']['message']
                return
            assert records['casa'].get('value') == records['native'].get('value')
            assert ('error' in records['casa']) == ('error' in records['native']), records
            if 'error' in records['casa']:
                # casars.TaskExecutionError subclasses RuntimeError; callers'
                # CASA exception handlers must still catch the native exception.
                assert records['casa']['error'] in records['native']['exception_bases'], records
        def compare_history():
            left, right = records['casa']['history'], records['native']['history']
            if name == 'review-nearest-field-tie':
                assert len(left['TIME']) == 23 and len(right['TIME']) == 0
                return
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
                    if casa_control_msselect is not None:
                        key, sep, value = message.partition(' = ')
                        if key.strip() == 'msselect':
                            selector = casa_control_msselect if engine == 'casa' else extra['msselect']
                            assert value == f'"{selector}"', message
                            message = key + sep + '<equivalent MS predicate>'
                    if message.startswith('version: '):
                        assert message.startswith('version: 6.7.6-14 ' if engine == 'casa' else 'version: casa-rs '), message
                        message = '<implementation version>'
                    if name == 'api-arrays' and engine == 'casa':
                        # CASA prints NumPy constructors in these HISTORY
                        # rows. The stored selector and map values are equal.
                        key, sep, value = message.partition(' = ')
                        parameter = key.strip()
                        if parameter in ['gaintable', 'gainfield', 'interp']:
                            array = extra[parameter]
                            assert value == repr(list(array)), message
                            message = key + sep + repr(array.tolist())
                        elif parameter == 'spwmap':
                            array = extra[parameter]
                            assert value == repr([array]), message
                            message = key + sep + repr([array.tolist()])
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
        if name == 'review-nearest-field-tie':
            result['cases'][name]['casa_bug_correction'] = 'Restore the explicit nearest-field ambiguity guard disabled by Sort::Ascending (-1) in genSort options; native rejects without applying an arbitrary tied gain.'
        if casa_control_callib is not None:
            result['cases'][name]['casa_bug_correction'] = 'CLPatchPanel records an absent scan for all requested observations rather than its current observation. Native retains valid joint contexts; explicit obs/scan CASA slices give an independent reference.'
            result['cases'][name]['casa_control_callib'] = str(casa_control_callib)
        if casa_control_msselect is not None:
            result['cases'][name]['casa_control_msselect'] = casa_control_msselect
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
    if only is None or 'literal' in only:
        literal_source = root/'literal-selectors.ms'
        copy_ms(source, literal_source)
        for subtable, row in [('FIELD', 1), ('ANTENNA', 0), ('SPECTRAL_WINDOW', 0)]:
            tool.open(str(literal_source/subtable), nomodify=False)
            tool.putcell('NAME', row, 'none')
            tool.close()
        for name in ['field', 'spw', 'antenna', 'scan', 'timerange', 'msselect']:
            case(f'api-literal-{name}', {name: 'none', 'flagbackup': True},
                 input_vis=literal_source, precorrected=True)
        library = root/'none'
        library.write_text(f"caltable='{constant}'\n")
        case('api-literal-callib', dict(docallib=True, callib='none'), precorrected=True)

    for label, arguments in [
        ('field-wildcard', dict(field='PH*')),
        ('field-id-range', dict(field='0~1')),
        ('field-bound', dict(field='>0')),
        ('spw-wildcard', dict(spw='*:1~5')),
        ('time-exact', dict(timerange='2019/12/31/21:52:15.3')),
        ('time-range', dict(timerange='21:52:15~21:52:37')),
        ('time-duration', dict(timerange='21:52:15+00:00:20')),
        ('time-bound', dict(timerange='>21:53:15')),
        ('time-bracketed', dict(timerange='[21:52:17~21:52:27]')),
        ('time-buffer-width', dict(timerange='3.0[21:52:17~21:52:27]')),
        ('time-wildcard', dict(timerange='2019/12/*/21:52:15.3')),
        ('time-invalid', dict(timerange='invalid', flagbackup=True)),
        ('antenna-union', dict(antenna='A0&A1;A3&A4')),
        ('antenna-exclude', dict(antenna='!A0')),
        ('antenna-length', dict(antenna='<50m')),
        ('antenna-invalid', dict(antenna='not-an-antenna', flagbackup=True)),
        ('scan-range', dict(scan='1~2')),
        ('scan-bound', dict(scan='>1')),
        ('scan-missing', dict(scan='999', flagbackup=True)),
        ('observation-int', dict(observation=1)),
        ('observation-union', dict(observation='0,1')),
        ('observation-missing', dict(observation='99', flagbackup=True)),
        ('taql', dict(msselect='ANTENNA1==0 AND SCAN_NUMBER==2')),
        ('taql-empty', dict(msselect='ANTENNA1==999', flagbackup=True)),
        ('taql-invalid', dict(msselect='NONEXISTENT_COLUMN==0', flagbackup=True)),
        ('mscal-antenna-name', dict(msselect='mscal.ant1name() == "A0"')),
        ('mscal-antenna2-name', dict(msselect='derivedmscal.ant2name() == "A4"')),
        ('mscal-antenna-column', dict(msselect='mscal.ant1col("DISH_DIAMETER") > 0 AND ANTENNA1 > 0')),
        ('mscal-column-units', dict(msselect='mscal.ant1col("DISH_DIAMETER") != 1200cm OR ANTENNA1 == 0')),
        ('mscal-column-unit-arithmetic', dict(msselect='mscal.ant1col("DISH_DIAMETER") + 10cm > 12m AND ANTENNA1 == 0')),
        ('mscal-column-unit-product', dict(msselect='mscal.ant1col("DISH_DIAMETER") * 2 == 24m AND ANTENNA1 == 0')),
        ('mscal-column-unit-ratio', dict(msselect='mscal.ant1col("DISH_DIAMETER") / 600cm == 2 AND ANTENNA1 == 0')),
        ('mscal-column-unit-membership', dict(msselect='mscal.ant1col("DISH_DIAMETER") IN [12m, 3m] AND ANTENNA1 == 0')),
        ('mscal-column-incompatible-units', dict(msselect='mscal.ant1col("DISH_DIAMETER") != 12s')),
        ('mscal-array-column-units', dict(msselect='mscal.ant1col("POSITION")[1] > 1km AND ANTENNA1 == 0')),
        ('mscal-field-column', dict(msselect='mscal.fieldcol(upper("name")) == "PHASE"')),
        ('mscal-spw-column', dict(msselect='mscal.spwcol("NUM_CHAN") > 0')),
        ('mscal-polarization-column', dict(msselect='mscal.polcol("NUM_CORR") == 2')),
        ('mscal-subtable-column', dict(msselect='mscal.subcol("ANTENNA", "NAME", ANTENNA2) IN ["A3", "A4"]')),
        ('mscal-nested-lookup', dict(msselect='mscal.subcol("SPECTRAL_WINDOW", "NUM_CHAN", mscal.subcol("DATA_DESCRIPTION", "SPECTRAL_WINDOW_ID", DATA_DESC_ID)) == 8')),
        ('mscal-processor-default', dict(msselect='mscal.proccol("TYPE") == ""')),
        ('mscal-invalid-row-argument', dict(msselect='mscal.subcol("ANTENNA", "NAME", 0.5) == "A0"')),
        ('mscal-invalid-metadata-argument', dict(msselect='mscal.ant1col(ANTENNA1) == "A0"')),
        ('mscal-random-metadata-argument', dict(msselect='mscal.ant1col(iif(rand() < 1, "NAME", "STATION")) == "A0"')),
    ]:
        case(f'api-select-{label}', arguments, input_vis=observation_source)
    if only is None or re.search(only, 'api-select-mscal-processor-array-invalid-id'):
        processor_source = root/'mscal-processor-array.ms'
        copy_ms(observation_source, processor_source)
        tool.open(str(processor_source/'PROCESSOR'), nomodify=False)
        try:
            assert tool.addcols({'EXTENSION_COEFF': {'valueType': 'double', 'ndim': 1, 'shape': [2]}})
        finally:
            tool.close()
        case('api-select-mscal-processor-array-invalid-id',
             dict(msselect='mscal.proccol("EXTENSION_COEFF")[1] == 0 OR ANTENNA1 == 0'),
             input_vis=processor_source)

    for label, predicate in [
        ('baseline', 'mscal.baseline("A0&A1")'),
        ('baseline-unmatched', 'mscal.baseline("DOES_NOT_EXIST") OR ANTENNA1 == 0'),
        ('combined', 'NOT derivedmscal.baseline("A0&A1") AND mscal.obs("0")'),
        ('field', 'mscal.field(upper("phase")) AND ANTENNA1 == 0'),
        ('field-unmatched', 'mscal.field("DOES_NOT_EXIST") OR ANTENNA1 == 0'),
        ('spw-channels', 'mscal.spw("0:1~2")'),
        ('array-bound', 'mscal.array("<1")'),
        ('array-unmatched', 'mscal.array("999") OR ANTENNA1 == 0'),
        ('state', 'mscal.state("DOES_NOT_EXIST,*") AND ANTENNA1 == 0'),
        ('state-unmatched', 'mscal.state("DOES_NOT_EXIST") OR ANTENNA1 == 0'),
        ('observation', 'mscal.obs(">0")'),
        ('observation-unmatched', 'mscal.obs("999") OR ANTENNA1 == 0'),
        ('empty', 'mscal.obs("")'),
        ('row-dependent', 'mscal.baseline(ANTENNA1)'),
    ]:
        case(f'api-select-mscal-selector-{label}', dict(msselect=predicate), input_vis=observation_source)
    # Pinned CASA crashes in the scan and time UDF grammars. Its UV-distance
    # UDF works in a table query but hangs in applycal with the MS open for
    # calibration. Ordinary TaQL predicates supply numeric and mutation controls.
    distance_sq = '(SQUARE(UVW[1]) + SQUARE(UVW[2]))'
    case('api-select-mscal-selector-uvdist', dict(msselect='mscal.uvdist("<40m")'),
         input_vis=observation_source, casa_control_msselect=f'{distance_sq} <= 1600')
    tool.open(str(observation_source/'SPECTRAL_WINDOW'))
    try:
        reference_freq_hz = float(tool.getcell('REF_FREQUENCY', 0))
    finally:
        tool.close()
    radius_sq = float(np.float32(200. * 299792458. / reference_freq_hz) ** np.float32(2.))
    case('api-select-mscal-selector-uvdist-wavelength',
         dict(msselect='mscal.uvdist("<0.2klambda") OR ANTENNA1 == 0'),
         input_vis=observation_source, casa_control_msselect=f'{distance_sq} <= {radius_sq} OR ANTENNA1 == 0')
    case('api-select-mscal-selector-scan', dict(msselect='mscal.scan("2")'),
         input_vis=observation_source, casa_control_msselect='SCAN_NUMBER == 2')
    time_bound = float(np.datetime64('2019-12-31T21:53:15', 's').astype(np.int64)) + 40587. * 86400.
    case('api-select-mscal-selector-time', dict(msselect='mscal.time(">2019/12/31/21:53:15")'),
         input_vis=observation_source, casa_control_msselect=f'TIME >= {time_bound}')

    for label, predicate in [
        ('reference', 'mscal.uvwwvl()[1] < -7500'),
        ('channel', 'derivedmscal.uvwwvls()[2,7] < -5000'),
        ('channel-frequency', 'abs(mscal.uvwwvls()[1,7] - mscal.uvwwvl()[1]) > 0.5'),
        ('channel-shape', 'nelements(mscal.uvwwvls()) == 24 AND ANTENNA1 == 0'),
        ('invalid-argument', 'mscal.uvwwvl(1)[1] < -7500'),
    ]:
        case(f'api-select-mscal-wavelength-{label}', dict(msselect=predicate), input_vis=observation_source)
    case('api-select-mscal-wavelength-multiple-windows',
         dict(msselect='DATA_DESC_ID == 1 AND mscal.uvwwvls()[2,7] < -5000', spwmap=[0, 1]),
         input_vis=multi_source, gain=multi_gain, precorrected=True)

    for label, predicate in [
        ('ha', 'mscal.ha() < -0.256rad'),
        ('ha1', 'mscal.ha1() < -0.256'),
        ('ha2', 'mscal.ha2() < -0.255'),
        ('hadec', 'mscal.hadec()[1] < -0.256'),
        ('hadec1', 'mscal.hadec1()[1] < -0.256'),
        ('hadec2', 'mscal.hadec2()[1] < -0.255'),
        ('azel', 'mscal.azel()[2] > 1.312rad'),
        ('azel1', 'mscal.azel1()[2] > 1.312'),
        ('azel2', 'mscal.azel2()[2] > 1.312'),
        ('last', 'mscal.last() > 65582.0014'),
        ('last1', 'mscal.last1() > 65582.0014'),
        ('last2', 'mscal.last2() > 65582.0014'),
        ('pa1', 'mscal.pa1() < -1.14rad'),
        ('pa2', 'mscal.pa2() < -1.14rad'),
        ('itrf', 'mscal.itrf()[1] < -0.927'),
        ('delay', 'mscal.delay() > 10ns'),
        ('delay1', 'mscal.delay1() > 10ns'),
        ('delay2', 'mscal.delay2() > 10ns'),
        ('uvw-j2000-metres', 'mscal.uvwj2000()[1] > 20m'),
        ('uvw-apparent-metres', 'mscal.uvwapp()[2] > 15m'),
        ('uvw-j2000-reference', 'mscal.uvwj2000wvl()[1] > 7000'),
        ('uvw-j2000-channel', 'mscal.uvwj2000wvls()[2,7] > 5000'),
        ('uvw-apparent-reference', 'mscal.uvwappwvl()[1] > 7000'),
        ('uvw-apparent-channel', 'mscal.uvwappwvls()[2,7] > 5000'),
        ('uvw-apparent-rotation',
         'abs(mscal.uvwappwvl()[1] - mscal.uvwj2000wvl()[1]) > 3.33564095'),
        ('field-column', 'mscal.ha("DELAY_DIR") < -0.256'),
        ('fixed-direction', 'mscal.ha([10deg,-20deg]) < -0.169rad'),
        ('solar-direction', 'mscal.azel("SUN")[1] < -1rad AND ANTENNA1 == 0'),
        ('row-dependent-direction', 'mscal.ha(FIELD_ID) < -0.256'),
        ('wrong-length-direction', 'mscal.ha([10deg]) < -0.256'),
        ('absolute-delay', 'abs(mscal.delay()) > 10ns'),
        ('absolute-uvw', 'any(abs(mscal.uvwj2000()) > 35m)'),
        ('sine-hour-angle', 'sin(mscal.ha()) < -0.253'),
        ('cosine-hour-angle', 'cos(mscal.ha()) > 0.9675'),
        ('tangent-elevation', 'tan(mscal.azel()[2]) > 3.78'),
        ('sine-delay-time-as-angle', 'sin(mscal.delay()) < 0'),
        ('sine-array', 'any(sin(mscal.azel()) > 0.965)'),
        ('invalid-trig-units', 'sin(mscal.uvwj2000()[1]) > 0'),
    ]:
        case(f'api-select-mscal-geometry-{label}', dict(msselect=predicate), input_vis=observation_source)

    if only is None or any(re.search(only, f'api-select-mscal-geometry-{label}') for label in (
            'degree-field-direction', 'rank-one-field-direction', 'not-direction-column', 'time-reference-column',
            'wgs84-antenna-uvw', 'wgs84-antenna-delay', 'pluto', 'explicit-field-name')):
        field_ms, time_ms, antenna_ms = geometry_measure_fixtures(observation_source, root)
        for label, vis, predicate in [
            ('degree-field-direction', field_ms, 'mscal.ha("CUSTOM_DIR") < -0.169rad'),
            ('rank-one-field-direction', field_ms, 'mscal.ha("DIR_1D") < -0.169rad'),
            ('not-direction-column', field_ms, 'mscal.ha("NO_DIRECTION") > -4rad'),
            ('pluto', field_ms, 'mscal.ha("PLUTO") < 1.156rad'),
            ('explicit-field-name', field_ms, r'mscal.ha("\PLUTO") < -0.169rad'),
            ('time-reference-column', time_ms, 'mscal.ha() < -0.257rad'),
            ('wgs84-antenna-uvw', antenna_ms, 'mscal.uvwapp()[2] > 15m'),
            ('wgs84-antenna-delay', antenna_ms, 'mscal.delay1() > 10ns'),
        ]:
            case(f'api-select-mscal-geometry-{label}', dict(msselect=predicate), input_vis=vis)

    for label, arguments in [
        ('scalar-map', dict(spwmap=0, flagbackup=True)),
        ('empty-text-map', dict(spwmap='')),
        ('empty-record-map', dict(spwmap={})),
        ('record-map', dict(spwmap={'0': [0]}, flagbackup=True)),
        ('unused-malformed-map', dict(spwmap=[[0], 'unused'])),
        ('invalid-observation-type', dict(observation=1.5, flagbackup=True)),
        ('invalid-observation-int16', dict(observation=np.int16(0), flagbackup=True)),
        ('invalid-tuple-spwmap', dict(spwmap=(0,), flagbackup=True)),
        ('invalid-tuple-uvrange', dict(uvrange=(), flagbackup=True)),
        ('observation-bool', dict(observation=True, flagbackup=True)),
        ('uvrange-list', dict(uvrange=['<50m', '>60m'])),
        ('uvrange-empty-list', dict(uvrange=[])),
        ('scalar-lists', dict(gaintable=str(time_gain), interp='nearest', gainfield='1', calwt=False, spwmap=[0])),
        ('tuple', dict(gaintable=(str(time_gain), str(constant)), interp=('linear', 'nearest'),
                       gainfield=('1', ''), calwt=(True, False), spwmap=[[0], [0]])),
        ('arrays', dict(gaintable=np.array([str(time_gain), str(constant)]), interp=np.array(['linear', 'nearest']),
                        gainfield=np.array(['1', '']), calwt=[True, False], spwmap=np.array([0]))),
        ('skip-empty-table', dict(gaintable=['', str(time_gain)], gainfield=['unknown', '1'], calwt=[False, True])),
        ('no-tables', dict(gaintable=[], flagbackup=True)),
        ('blank-table', dict(gaintable=[''], flagbackup=True)),
        ('gain-missing', dict(gaintable=[str(root/'absent.gcal')], flagbackup=True)),
        ('vis-missing', dict(vis=str(root/'absent.ms'), flagbackup=True)),
        ('vis-empty', dict(vis='')),
        ('calwt-short', dict(gaintable=[str(time_gain)]*3, calwt=[True, False], flagbackup=True)),
        ('calwt-empty', dict(calwt=[], flagbackup=True)),
        ('invalid-mode', dict(applymode='invalid', flagbackup=True)),
        ('invalid-bool', dict(selectdata=1, flagbackup=True)),
        ('negative-observation', dict(observation=-1, flagbackup=True)),
        ('variant-uvrange', dict(uvrange=50)),
    ]:
        case(f'api-{label}', arguments, input_vis=chain_source)
    case('api-vis-list', dict(flagbackup=True), vis_as_list=True)
    case('api-positional', dict(field='PHASE', interp='nearest'), gain=time_gain, positional=True)

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
    case('chain-T-default', gain=scalar_time_gain, input_vis=chain_source, precorrected=True)
    case('chain-T-nearest-fields', dict(gainfield=['0,1'], interp=['nearest']), gain=scalar_time_gain, input_vis=chain_source, precorrected=True)
    case('chain-T-cubic', dict(interp=['cubic']), gain=scalar_time_gain, input_vis=chain_source, precorrected=True)
    case('chain-T-PD', dict(interp=['linearPD']), gain=scalar_time_gain, input_vis=phase_delay_source, precorrected=True)
    case('chain-T-B', dict(gaintable=[str(scalar_time_gain), str(curved_bandpass)], gainfield=['0,1', ''], interp=['cubic', 'linear,spline'], calwt=[True, True]), input_vis=shifted_source, precorrected=True)
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
    for label, gain in family_tables.items():
        input_vis = vla_source if label == 'gaincurve' else source
        case('family-'+label+'-default', gain=gain, input_vis=input_vis, precorrected=True)
        case('family-'+label+'-channels', dict(spw='0:1~5^2'), gain=gain, input_vis=input_vis, precorrected=True)
        case('family-'+label+'-no-calwt', dict(calwt=False), gain=gain, input_vis=input_vis, precorrected=True, spectral_sigma=True)
        case('family-'+label+'-trial', dict(applymode='trial'), gain=gain, input_vis=input_vis, precorrected=True)
    for label, gain in varying_family_tables.items():
        input_vis = vla_source if label == 'gaincurve' else source
        flagged = root/f'family-{label}-flagged.cal'
        copy_ms(gain, flagged)
        tool.open(str(flagged), nomodify=False)
        flags = tool.getcol('FLAG')
        flags[:, :, tool.getcol('ANTENNA1') == 0] = True
        tool.putcol('FLAG', flags)
        tool.close()
        for mode in ['calflag', 'calonly', 'flagonly', 'calflagstrict', 'flagonlystrict']:
            case(f'family-{label}-flagged-{mode}',
                 dict(interp=['linear'], applymode=mode, spw='0:1~5^2'),
                 gain=flagged, input_vis=input_vis, precorrected=True, spectral_sigma=True)
        library = root/f'family-{label}-library.txt'
        library.write_text(f"caltable='{gain}' tinterp='cubic' calwt=True\n")
        case(f'family-{label}-library', dict(docallib=True, callib=str(library)),
             input_vis=input_vis, precorrected=True, spectral_sigma=True)
        case('family-'+label+'-time-cubic', dict(interp=['cubic']), gain=gain,
             input_vis=input_vis, precorrected=True)
    for label, input_vis in [('opacity', source), ('gaincurve', vla_source)]:
        if only is not None and re.search(only, f'family-{label}-same-time-windows') is None:
            continue
        vis, gain = multi_window(input_vis, family_tables[label], root, windows=2,
                                 name=f'family-{label}-multi')
        tool.open(str(vis/'DATA_DESCRIPTION'), nomodify=False)
        row = {column: tool.getcell(column, 0) for column in tool.colnames()}
        tool.addrows(1)
        for column, value in row.items():
            tool.putcell(column, 1, 1 if column == 'SPECTRAL_WINDOW_ID' else value)
        tool.close()
        tool.open(str(vis), nomodify=False)
        tool.putcol('DATA_DESC_ID', np.arange(tool.nrows(), dtype=np.int32) % 2)
        tool.close()
        tool.open(str(gain), nomodify=False)
        parameters = tool.getcol('FPARAM')
        parameters[:, :, tool.getcol('SPECTRAL_WINDOW_ID') == 1] *= 2
        tool.putcol('FPARAM', parameters)
        tool.close()
        case(f'family-{label}-same-time-windows', dict(calwt=False), gain=gain, input_vis=vis)

    for selector in ['0', '1', 'PHASE', 'NO_SUCH_FIELD']:
        case('family-BPOLY-gainfield-'+selector, dict(gainfield=[selector]), gain=poly, precorrected=True)

    family_chain = [family_tables[label] for label in ['K', 'antpos', 'opacity', 'BPOLY']]
    for label, chain in [('forward', family_chain), ('reverse', family_chain[::-1])]:
        case(f'family-chain-{label}', dict(gaintable=[str(gain) for gain in [constant, *chain]],
             calwt=[gain in {constant, family_tables['opacity']} for gain in [constant, *chain]]),
             precorrected=True, spectral_sigma=True)
    poly_library = root/'family-BPOLY-library.txt'
    poly_library.write_text(f"caltable='{poly}'\n")
    case('family-BPOLY-library', dict(docallib=True, callib=str(poly_library)), precorrected=True)

    if only is None or re.search(only, 'family-BPOLY-nearest'):
        result.setdefault('casa_crash_controls', {})['BPOLY-nearest'] = bpoly_nearest_control(root, source, poly, native)
        (root/'results.json').write_text(json.dumps(result, indent=2)+'\n')

    # Callibrary task parsing uses CalLibraryGram/CLPatchPanel, distinct from
    # the private Python helper's read/eval implementation.
    mixed_observations = root/'source-mixed-observations.ms'
    copy_ms(observation_source, mixed_observations)
    tool.open(str(mixed_observations), nomodify=False)
    tool.putcol('OBSERVATION_ID', (np.arange(tool.nrows()) % 2).astype(np.int32))
    tool.close()
    quoted_gain = root/"O'Brien.gcal"
    shutil.copytree(time_gain, quoted_gain)
    callib_cases = [
        ('default', '', chain_source, time_gain),
        ('review-observation-buffer', " obsmap='self'", mixed_observations, observation_gain),
        ('review-observation-buffer-short', ' obsmap=[0]', mixed_observations, observation_gain),
        ('review-empty-invalid-time', " field='FLUX' tinterp='invalid'", chain_source, time_gain),
        ('review-active-invalid-time', " field='PHASE' tinterp='invalid'", chain_source, time_gain),
        ('review-frequency-aipslin', " finterp='aipslin'", shifted_source, curved_bandpass),
        ('T-default', '', chain_source, scalar_time_gain),
        ('B-default', " tinterp='linear' finterp='cubicflag'", shifted_source, flagged_curved_bandpass),
        ('empty-slice', " field='FLUX'", chain_source, time_gain),
        ('unknown-field', " field='NO_SUCH_FIELD'", source, time_gain),
        ('unknown-spw', " spw='999'", source, time_gain),
        ('unknown-scan', " scan='999'", source, time_gain),
        ('unknown-obs', " obs='999'", source, time_gain),
        ('unknown-intent', " intent='NO_SUCH_INTENT'", selection_source, time_gain),
        ('string-spwmap', " spwmap='[0]'", multi_source, multi_gain),
        ('calwt-true', ' calwt=True', chain_source, time_gain),
        ('calwt-false', ' calwt=False', chain_source, time_gain),
        ('field-name', " field='PHASE'", source, time_gain),
        ('field-pattern', " field='PH*'", source, time_gain),
        ('field-spw', " field='PHASE' spw='0:2~5'", source, time_gain),
        ('observation', " obs='1'", observation_source, observation_gain),
        ('intent', " intent='CALIBRATE_PHASE*'", selection_source, time_gain),
        ('reach', " reach='10s'", chain_source, time_gain),
        ('fldmap-name', " fldmap='PHASE'", chain_source, time_gain),
        ('fldmap-selected', " fldmap='0,1'", chain_source, time_gain),
        ('fldmap-nearest', " fldmap='nearest'", chain_source, time_gain),
        ('fldmap-array', ' fldmap=[1,0]', source, time_gain),
        ('obsmap-array', ' obsmap=[1,0]', observation_source, observation_gain),
        ('obsmap-self', " obsmap='self'", observation_source, observation_gain),
        ('obsmap-short', ' obsmap=[1]', observation_source, observation_gain),
        ('obsmap-missing', ' obsmap=[999,0]', observation_source, observation_gain),
        ('scan', " scan='2'", observation_source, observation_gain),
        ('scanmap-self', " scanmap='self'", observation_source, observation_gain),
        ('scanmap-array', ' scanmap=[0,2,1]', observation_source, observation_gain),
        ('scanmap-short', ' scanmap=[1]', observation_source, observation_gain),
        ('scanmap-missing', ' scanmap=[0,999,1]', observation_source, observation_gain),
        ('scanmap-invalid', " scanmap='perobs'", observation_source, observation_gain),
        ('obs-scan-self', " obsmap='self' scanmap='self'", observation_source, observation_gain),
        ('obs-scan-crossed', " obsmap=[1,0] scanmap='self'", observation_source, observation_gain),
        ('fldmap-short', ' fldmap=[0]', source, time_gain),
        ('fldmap-missing', ' fldmap=[999,0]', source, time_gain),
        ('antmap-missing', ' antmap=[999]', chain_source, time_gain),
        ('spwmap-missing', ' spwmap=[999]', chain_source, time_gain),
        ('obsmap-perobs', " obsmap='perobs'", observation_source, observation_gain),
        ('antmap-array', ' antmap=[1,0,2,3,4]', chain_source, time_gain),
        ('antmap-broadcast', ' antmap=[0]', chain_source, time_gain),
        ('spwmap-broadcast', ' spwmap=[0]', multi_source, multi_gain),
        ('spwmap-prefix', ' spwmap=[1,0]', multi_source, multi_gain),
        ('tinterp-cubic', " tinterp='cubic'", chain_source, time_gain),
        ('finterp-cubic', " finterp='cubic'", shifted_source, curved_bandpass),
        ('unknown-string', " unrecognized='text'", chain_source, time_gain),
        ('empty-map', ' antmap=[]', chain_source, time_gain),
    ]
    for name, directives, input_vis, gain in callib_cases:
        path = root/('library-'+name+'.txt')
        path.write_text(f"caltable='{gain}'{directives}\n")
        control = None
        if name == 'obs-scan-self':
            control = root/'library-joint-control.txt'
            control.write_text(f"caltable='{gain}' obs='0' scan='1' obsmap='self' scanmap='self'\ncaltable='{gain}' obs='1' scan='2' obsmap='self' scanmap='self'\n")
        case('callib-'+name, dict(docallib=True, callib=str(path)), gain=gain, input_vis=input_vis, precorrected=True, casa_control_callib=control)
    case('review-buffer-traditional-perobs', dict(interp=['linearperobs']), gain=observation_gain, input_vis=mixed_observations, precorrected=True)
    grammar_cases = [
        ('review-array-reach', "caltable='{table}' reach=[1]\n"),
        ('review-array-finterp', "caltable='{table}' finterp=[1]\n"),
        ('review-array-after-string', "caltable='{table}' antmap='unused' antmap=[0]\n"),
        ('review-empty-caltable', "caltable='' field='PHASE'\ncaltable='{table}'\n"),
        ('review-alias-entries', "caltable='{table}'\ncaltable='{table_alias}'\n"),
        ('review-apostrophe-path', 'caltable="{quoted_table}"\n'),
        ('review-wildcard-overlap', "caltable='{table}' field='FLUX'\ncaltable='{table}' scan='2'\n"),
        ('empty-file', ''),
        ('comments-only', '# an empty library\n'),
        ('quoted-calwt', "caltable='{table}' calwt='False'\n"),
        ('string-calwt', "caltable='{table}' calwt=yes\n"),
        ('numeric-calwt', "caltable='{table}' calwt=1\n"),
        ('key-case', "caltable='{table}' CALWT=False FIELD='PHASE'\n"),
        ('bool-selector', "caltable='{table}' field=True\n"),
        ('quoted-key', "'caltable'='{table}' 'calwt'=False\n"),
        ('space-equals', "caltable = '{table}' calwt = False\n"),
        ('orphan-selector', "field='PHASE'\ncaltable='{table}'\n"),
        ('orphan-calwt', "calwt=False\ncaltable='{table}'\n"),
        ('array-spaces', "caltable='{table}' antmap=[1 0 2 3 4]\n"),
        ('array-multiline', "caltable='{table}' antmap=[1,\n0,2,3,4]\n"),
        ('array-negative', "caltable='{table}' antmap=[-1]\n"),
        ('fldmap-empty', "caltable='{table}' fldmap=''\n"),
        ('inline-comment', "caltable='{table}' # comment\n"),
        ('comma-entries', "caltable='{table}' field='FLUX',caltable='{table}' field='PHASE'\n"),
        ('overlap', "caltable='{table}'\ncaltable='{table}'\n"),
    ]
    for name, text in grammar_cases:
        path = root/('library-grammar-'+name+'.txt')
        path.write_text(text.format(table=time_gain, table_alias=str(time_gain.parent)+'/./'+time_gain.name, quoted_table=quoted_gain))
        case('callib-grammar-'+name, dict(docallib=True, callib=str(path)), gain=time_gain, input_vis=source, precorrected=True)
    for directive in ['obsmap=[999,0]', 'scanmap=[0,999,1]', 'fldmap=[999,0]', 'antmap=[999]']:
        path = root/('library-missing-'+directive.split('=')[0]+'.txt')
        path.write_text(f"caltable='{observation_gain}' {directive}\n")
        for mode in ['calflagstrict', 'calonly', 'flagonly', 'trial']:
            case('callib-missing-'+directive.split('=')[0]+'-'+mode,
                 dict(docallib=True, callib=str(path), applymode=mode), gain=observation_gain,
                 input_vis=observation_source, precorrected=True, spectral_sigma=True)
    active = root/'library-default.txt'
    case('callib-inactive-traditional', dict(docallib=True, callib=str(active), gaintable=[str(root/'absent.gcal')],
         gainfield=['NO_SUCH_FIELD'], interp=['invalid'], spwmap=[999], calwt=[False]), gain=time_gain, input_vis=chain_source, precorrected=True)
    case('callib-inactive-scalar-spwmap', dict(docallib=True, callib=str(active), spwmap=0), input_vis=chain_source)
    case('callib-empty-path', dict(docallib=True), gain=time_gain, input_vis=chain_source)
    duplicate = root/'library-repeat.txt'
    duplicate.write_text(f"caltable='{time_gain}' field='FLUX' fldmap='FLUX' calwt=False\ncaltable='{time_gain}' field='PHASE' fldmap='PHASE' calwt=True\n")
    case('callib-repeated-calwt-first', dict(docallib=True, callib=str(duplicate)), gain=time_gain, input_vis=source, precorrected=True)
    case('callib-inline', dict(docallib=True, callib=f"caltable='{time_gain}' calwt=True"), gain=time_gain, input_vis=chain_source, precorrected=True)
    nested = root/'library-nested'; nested.mkdir()
    relative = nested/'relative.txt'; relative.write_text("caltable='time-fields.gcal'\n")
    case('callib-relative-cwd', dict(docallib=True, callib=str(relative)), gain=time_gain, input_vis=chain_source, precorrected=True)
    case('callib-missing-file', dict(docallib=True, callib=str(root/'no-library.txt')), gain=time_gain, input_vis=chain_source)
    if only is not None:
        return result
    # CASA crashes when perscan is combined with multiple observation IDs.
    # Its documented/source partition is only by scan. Compare native results
    # to the same samples/scan series under CASA's working one-observation control.
    result.setdefault('casa_crash_controls', {})
    for method in ['nearestperscan', 'linearperscan', 'linearperobsperscan', 'spline']:
        crash_vis = root/('crash-'+method+'.ms')
        copy_ms(observation_source if method != 'spline' else chain_source, crash_vis)
        arguments = dict(vis=str(crash_vis), gaintable=[str(observation_gain if method != 'spline' else time_gain)], interp=[method], flagbackup=False)
        probe = isolated_applycal(arguments)
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
