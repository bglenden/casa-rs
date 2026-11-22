#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare direct calibrater.specifycal prescriptions with CASA 6.7.6.14."""

import argparse
from contextlib import chdir
import os
from pathlib import Path

import casatasks
from casatools import calibrater as casa_calibrater
import numpy as np

from blcal_oracle import copy_ms, nrms, opened, source
from calibrater_apply_tool_oracle import result, same_record
from calibrater_solve_tool_oracle import compare_readers
from calibration_gencal_specify_oracle import compare_tables, with_second_window
from calibration_gencal_tsys_oracle import attach_syscal
from gencal_switched_power_oracle import MODES, compare_columns, read_calibration, seed
from gencal_tecim_oracle import compare_tables as compare_tec, make_image
from record_casa_returns import make_cal_ms


ERROR = (False, ('RuntimeError', 'Error in Calibrater::specifycal.'))
TUPLE_ERROR = (False, ('SystemError', '<built-in function calibrater_specifycal> '
                                    'returned a result with an exception set'))


def prescribe(work, engines, vis, name, stages, *, legacy=False, product=True,
              compare=compare_tables):
    paths = {}
    for engine, make_tool in engines.items():
        tool = make_tool()
        path = work/f'{name}-{engine}.cal'
        paths[engine] = path
        assert tool.setvi(legacy, True)
        assert tool.specifycal() is False
        assert tool.open(str(vis), addcorr=False, addmodel=False)
        assert tool.selectvis(baseline='1&2')
        assert tool.setsolve(type='G', table=str(path.with_suffix('.solve')), refant='1')
        before, activity = tool.returndict(), tool.activityrec()
        for parameters, expectation in stages:
            if isinstance(parameters, tuple):
                observed = result(tool.specifycal, str(path), *parameters)
            else:
                observed = result(tool.specifycal, caltable=str(path), **parameters)
            assert observed == expectation, (name, engine, observed, expectation)
        same_record(tool.returndict(), before)
        same_record(tool.activityrec(), activity)
        assert path.is_dir() == product, (name, engine)
        assert tool.close() and tool.specifycal() is False
    if product:
        error = compare(paths['native'], paths['casa'])
        compare_readers(paths)
        print(name, 'parameter NRMS', error, flush=True)
    else:
        print(name, 'rejected', flush=True)
    return paths


def compare_switched_power(native, casa):
    actual, expected = read_calibration(native), read_calibration(casa)
    compare_columns(actual, expected)
    return nrms(actual['FPARAM'], expected['FPARAM'])


def derived_prescriptions(work, engines, vla_source, gaincurves):
    ok = (True, True)
    switched = work/'switched.ms'
    seed(switched)
    for index, kind in enumerate(MODES):
        prescribe(work, engines, switched, f'switched-{index}', [(dict(caltype=kind), ok)],
                   compare=compare_switched_power)
    for kind in ('swp/rq-extra', 'rq-extra'):
        prescribe(work, engines, switched, kind.replace('/', '-'), [(dict(caltype=kind), ok)],
                   compare=compare_switched_power)
    for kind in ('swp', 'wts'):
        prescribe(work, engines, switched, f'{kind}-mode-error',
                   [(dict(caltype=kind), ERROR)], product=False)
    ionosphere_ms = work/'ionosphere.ms'
    make_cal_ms(str(ionosphere_ms), channels=4, frequency='332MHz',
                stokes='RR RL LR LL', telescope='VLA')
    with opened(ionosphere_ms) as table:
        epoch = table.getcol('TIME').min()
    tec_image = work/'ionosphere.tec'
    make_image(tec_image, epoch)
    prescribe(work, engines, ionosphere_ms, 'ionosphere', [
        (dict(caltype='TECIM', parameter=[600.], infile=str(tec_image)), ok),
    ], compare=lambda native, casa: compare_tec(native, casa)[0])
    prescribe(work, engines, ionosphere_ms, 'ionosphere-mode-error', [
        (dict(caltype='TEC', parameter=[600.], infile=str(tec_image)), ERROR),
    ], product=False)
    if vla_source is None:
        return
    vla = work/'vla.ms'
    copy_ms(vla_source, vla)
    for kind in ('gc', 'gceff', 'eff'):
        prescribe(work, engines, vla, kind, [(dict(caltype=kind), ok)])
        prescribe(work, engines, vla, f'{kind}-infile', [
            (dict(caltype=kind, infile=str(gaincurves)), ok),
        ])
    prescribe(work, engines, vla, 'antposvla', [
        (dict(caltype='antposvla', antenna='1', parameter=[.04, -.01, .02]), ok),
    ])
    attach_syscal(vla, work/'syscal.txt', spectrum=True)
    for uniform in (False, True):
        prescribe(work, engines, vla, f'tsys-uniform-{uniform}', [
            (dict(caltype='TSYS', uniform=uniform), ok),
        ])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=Path)
    parser.add_argument('--vla-source', type=Path, help='NGC 5921 VLA MeasurementSet')
    parser.add_argument('--gaincurves', type=Path, help='Installed CASA VLA GainCurves table')
    args = parser.parse_args()
    if args.vla_source is not None and args.gaincurves is None:
        parser.error('--vla-source requires --gaincurves')
    if args.gaincurves is not None:
        args.gaincurves = args.gaincurves.resolve()
        os.environ['CASA_RS_MEASURESPATH'] = str(args.gaincurves.parents[2])
    work = args.work.resolve()
    from casars.tools import calibrater as native_calibrater

    assert casatasks.version_string() == '6.7.6.14'
    work.mkdir()
    vis = source(work)
    with_second_window(vis)
    engines = {'casa': casa_calibrater, 'native': native_calibrater}
    ok = (True, True)
    paths = prescribe(work, engines, vis, 'amplitude-update', [
        (dict(caltype='amp', parameter=1.5, antenna='A4', pol='R'), ok),
        (dict(caltype='G', parameter=[3.], antenna=b'A4', pol='R'), ok),
        (dict(caltype='delay'), ERROR),
    ])
    for path in paths.values():
        with opened(path) as table:
            antenna, parameters = (table.getcol(key) for key in ('ANTENNA1', 'CPARAM'))
            np.testing.assert_array_equal(parameters[0, 0, antenna == 4], 4.5)
            np.testing.assert_array_equal(parameters[0, 0, antenna != 4], 1.)
            np.testing.assert_array_equal(parameters[1, ...], 1.)
    prescribe(work, engines, vis, 'positional', [
        (('2020/01/01/00:00:00', '1', 'A4', 'R', 'G', [2.], '', False), ok),
        (dict(caltype='phase', time='IGNORED', spw='1', antenna='A4', pol='R', parameter=[32.]), ok),
    ])
    for kind in ('PH', 'K', 'sbd', 'delay', 'mbd'):
        prescribe(work, engines, vis, kind.lower(), [
            (dict(caltype=kind, spw='0,1', antenna='A2', pol='L,R',
                  parameter=[2., 3., 5., 7.]), ok),
            (dict(caltype=kind, antenna='A2', pol='L,R', parameter=np.array([2., 3.])), ok),
        ], legacy=kind in ('sbd', 'PH'))
    prescribe(work, engines, vis, 'antpos', [
        (dict(caltype='antpos', antenna='A1', parameter=[.04, -.01, .02]), ok),
        (dict(caltype='antpos', antenna='A1', parameter=[-.02, .04, -.01]), ok),
    ])
    prescribe(work, engines, vis, 'antpos-broadcast', [
        (dict(caltype='ANTPOS', parameter=[.03, .01, -.02]), ok),
        (dict(caltype='antpos', antenna='A4', parameter=[.04, -.01, .02],
              infile=str(work/'absent')), ok),
    ])
    prescribe(work, engines, vis, 'opacity', [
        (dict(caltype='opacity', spw='1', antenna='A1', parameter=[.05]), ok),
        (dict(caltype='opac', spw='1', antenna='A1', parameter=[.03]), ok),
    ])
    defaults = prescribe(work, engines, vis, 'default-parameters', [
        (dict(caltype='G', infile=str(work/'absent'), uniform=True), ok),
    ])
    for engine, make_tool in engines.items():
        path = defaults[engine]
        tool = make_tool()
        assert tool.open(str(vis), addcorr=False, addmodel=False)
        with chdir(path):
            assert result(tool.specifycal, caltype='G', parameter=[3.]) == ERROR
            assert result(tool.specifycal, '', caltype='G', parameter=[3.]) == ERROR
        assert tool.close()
        with opened(path) as table:
            np.testing.assert_array_equal(table.getcol('CPARAM'), 1.)
    native = native_calibrater()
    assert native.open(str(vis), addcorr=False, addmodel=False)
    wildcard = work/'wildcard-native.cal'
    assert native.specifycal(str(wildcard), antenna='*', spw='*', caltype='G') and native.close()
    assert compare_tables(wildcard, defaults['casa']) == 0.
    compare_readers({'casa': defaults['casa'], 'native': wildcard})
    prescribe(work, engines, vis, 'tuple-publishes', [
        (dict(caltype='amp', parameter=(2.,)), TUPLE_ERROR),
    ])
    for legacy in (False, True):
        for family in ('SDSKY_PS', 'SDSKY_RASTER', 'SDSKY_OTF'):
            allowed = legacy or family != 'SDSKY_OTF'
            prescribe(work, engines, vis, f'{family.lower()}-legacy-{legacy}', [
                (dict(caltype=family, parameter=[1.5]), ok if allowed else ERROR),
                (dict(caltype=family, parameter=[2.25]), ERROR),
            ], legacy=legacy, product=allowed)
    for name, options, expectation in [
        ('factory-error', {'caltype': 'UNRECOGNIZED'}, ERROR),
        ('antenna-error', {'antenna': 'MISSING'}, ERROR),
        ('spw-error', {'spw': '100'}, ERROR),
        ('pol-error', {'pol': 'P'}, ERROR),
        ('parameter-error', {'parameter': [1., 2., 3.]}, ERROR),
        ('parameter-none', {'parameter': None}, ERROR),
        ('time-none', {'time': None}, (False, ('TypeError', 'argument _time must be a string'))),
        ('uniform-int', {'uniform': 1}, (False, ('TypeError',
                         "in method 'calibrater_specifycal', argument 10 of type 'bool'"))),
        ('tec-missing', {'caltype': 'TECIM', 'infile': str(work/'absent')}, ERROR),
    ]:
        prescribe(work, engines, vis, name, [(dict(caltype='amp') | options, expectation)], product=False)
    derived_prescriptions(work, engines, args.vla_source, args.gaincurves)


if __name__ == '__main__':
    main()
