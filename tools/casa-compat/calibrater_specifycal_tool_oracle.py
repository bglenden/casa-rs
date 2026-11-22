#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare direct calibrater.specifycal prescriptions with CASA 6.7.6.14."""

import argparse
from pathlib import Path

import casatasks
from casatools import calibrater as casa_calibrater
import numpy as np

from blcal_oracle import opened, source
from calibrater_apply_tool_oracle import result, same_record
from calibrater_solve_tool_oracle import compare_readers
from calibration_gencal_specify_oracle import compare_tables, with_second_window


ERROR = (False, ('RuntimeError', 'Error in Calibrater::specifycal.'))
TUPLE_ERROR = (False, ('SystemError', '<built-in function calibrater_specifycal> '
                                    'returned a result with an exception set'))


def prescribe(work, engines, vis, name, stages, *, legacy=False, product=True):
    paths = {}
    for engine, make_tool in engines.items():
        tool = make_tool()
        path = work/f'{name}-{engine}.cal'
        paths[engine] = path
        assert tool.setvi(legacy, True)
        assert tool.specifycal() is False
        assert tool.open(str(vis), addcorr=False, addmodel=False)
        assert tool.selectvis(baseline='A1&A2')
        assert tool.setsolve(type='G', table=str(path.with_suffix('.solve')), refant='A1')
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
        error = compare_tables(paths['native'], paths['casa'])
        compare_readers(paths)
        print(name, 'parameter NRMS', error, flush=True)
    else:
        print(name, 'rejected', flush=True)
    return paths


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=Path)
    work = parser.parse_args().work.resolve()
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
    prescribe(work, engines, vis, 'opacity', [
        (dict(caltype='opacity', spw='1', antenna='A1', parameter=[.05]), ok),
        (dict(caltype='opac', spw='1', antenna='A1', parameter=[.03]), ok),
    ])
    prescribe(work, engines, vis, 'default-parameters', [
        (dict(caltype='G', infile=str(work/'absent'), uniform=True), ok),
    ])
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
        ('tec-missing', {'caltype': 'TEC', 'infile': str(work/'absent')}, ERROR),
    ]:
        prescribe(work, engines, vis, name, [(dict(caltype='amp') | options, expectation)], product=False)


if __name__ == '__main__':
    main()
