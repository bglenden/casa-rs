#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare direct BPOLY and GSPLINE workflows with CASA 6.7.6.14."""

import argparse
from pathlib import Path
import shutil
from time import perf_counter

import casatasks
import numpy as np
from casatools import calibrater as casa_calibrater

from bandpass_oracle import source_data
from blcal_oracle import copy_ms, nrms, opened
from calibrater_apply_tool_oracle import result, same_record
from gspline_oracle import compare_tables, source as spline_source


def compare_bandpass(reference, candidate):
    with opened(reference) as expected, opened(candidate) as actual:
        assert actual.info() == expected.info()
        assert sorted(actual.colnames()) == sorted(expected.colnames())
        assert actual.nrows() == expected.nrows() > 0
        order = [np.lexsort([tool.getcol(key) for key in ('ANTENNA1', 'FIELD_ID', 'TIME')])
                 for tool in (expected, actual)]
        for key in ('ANTENNA1', 'FIELD_ID', 'CAL_DESC_ID', 'REF_ANT', 'N_POLY_AMP',
                    'N_POLY_PHASE', 'VALID_DOMAIN', 'PHASE_UNITS', 'SOLUTION_OK'):
            np.testing.assert_array_equal(actual.getcol(key)[..., order[1]],
                                          expected.getcol(key)[..., order[0]], err_msg=key)
        np.testing.assert_allclose(actual.getcol('TIME')[order[1]],
                                   expected.getcol('TIME')[order[0]], atol=1e-4, rtol=0)
        for key in ('POLY_COEFF_AMP', 'POLY_COEFF_PHASE', 'SCALE_FACTOR'):
            before, after = [np.concatenate([np.asarray(tool.getcell(key, int(row))).ravel()
                                             for row in rows])
                             for tool, rows in zip((expected, actual), order, strict=True)]
            assert nrms(after, before) < 1e-3, (key, nrms(after, before))


def verify_readers(path, columns):
    from casars.tools import table

    native = table()
    for location in (path, path/'CAL_DESC', path/'CAL_HISTORY'):
        assert native.open(str(location))
        try:
            with opened(location) as casa:
                assert native.info() == casa.info()
                assert native.nrows() == casa.nrows()
                assert sorted(native.colnames()) == sorted(casa.colnames())
                if location != path:
                    continue
                for row in range(casa.nrows()):
                    for name in columns:
                        np.testing.assert_array_equal(native.getcell(name, row), casa.getcell(name, row))
        finally:
            assert native.close()


def field_rows(path, field):
    with opened(path) as table:
        return np.flatnonzero(table.getcol('FIELD_ID') == field).tolist()


def compare_spline(reference, candidate, *, before=None):
    field = 0 if before is None else 1
    rows = (field_rows(reference, field), field_rows(candidate, field))
    assert len(rows[0]) == len(rows[1]) == 5
    compare_tables(reference, candidate, rows, logical_splines=True)
    with opened(candidate) as table:
        expected = [0] * 5 if before is None else [0] * 5 + [1] * 5
        np.testing.assert_array_equal(table.getcol('FIELD_ID'), expected)
    if before is not None:
        preserved = (field_rows(before, 0), field_rows(candidate, 0))
        compare_tables(before, candidate, preserved, logical_splines=True)


def run_case(root, vis, tools, method, label, legacy, options, append=None, *, empty=False):
    paths, returns, activity = {}, {}, {}
    bandpass = method == 'setsolvebandpoly'
    native_before = None
    for engine, factory in tools.items():
        tool = factory()
        setup = getattr(tool, method)
        assert result(setup, table='unused.cal') == (True, False)
        assert tool.setvi(legacy, True) and tool.open(str(vis), addcorr=False, addmodel=False)
        assert tool.selectvis(field='FLUX', spw='0:1~10')
        assert tool.setptmodel([1, 0, 0, 0])
        path = root/f'{label}-{engine}.cal'
        paths[engine] = path
        assert result(setup) == (False, ('RuntimeError', 'Error in Calibrater::setsolve.'))
        assert setup(table=str(path), **options)
        start = perf_counter()
        succeeded = result(tool.solve)
        seconds = perf_counter()-start
        if legacy or empty:
            assert succeeded == (True, True) and path.exists() != empty, (label, engine, succeeded)
            if append is not None:
                if not bandpass:
                    if engine == 'native':
                        native_before = root/'gspline-before-append.cal'
                        shutil.copytree(path, native_before)
                    assert tool.selectvis(field='PHASE', spw='0:1~10')
                assert setup(table=str(path), append=True, **append)
                assert tool.solve()
        else:
            assert succeeded == (False, ('RuntimeError', 'Error in Calibrater::solve.'))
            assert not path.exists()
        activity[engine] = tool.activityrec()
        returns[engine] = tool.returndict()
        assert tool.close()
        print(label, engine, 'seconds', seconds, flush=True)
    same_record(activity['native'], activity['casa'])
    if bandpass and empty and not legacy:
        same_record(returns['native']['solvestats'], returns['casa']['solvestats'])
    else:
        assert all('solvestats' not in record for record in returns.values())
    for engine, record in returns.items():
        assert list(record['solve_tables']) == ([str(paths[engine])] if legacy or empty else [])
    if legacy and not empty:
        if bandpass:
            compare_bandpass(paths['casa'], paths['native'])
            columns = ('TIME', 'ANTENNA1', 'N_POLY_AMP', 'POLY_COEFF_PHASE')
        else:
            compare_spline(paths['casa'], paths['native'], before=native_before)
            columns = ('TIME', 'ANTENNA1', 'POLY_MODE', 'POLY_COEFF_AMP', 'SPLINE_KNOTS_PHASE')
        for path in paths.values():
            verify_readers(path, columns)
    print(label, activity['casa'], flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=Path)
    args = parser.parse_args()
    from casars.tools import calibrater as native_calibrater

    assert casatasks.version_string() == '6.7.6.14'
    root = args.work.resolve()
    root.mkdir()
    bandpass = root/'bandpass.ms'
    source_data(bandpass)
    spline_work = root/'spline'
    spline_work.mkdir()
    spline, _ = spline_source(spline_work, 'ap', channels=12, integration_seconds=4)
    tools = {'casa': casa_calibrater, 'native': native_calibrater}
    run_case(root, bandpass, tools, 'setsolvebandpoly', 'bpoly-vi2', False, {})
    run_case(root, spline, tools, 'setsolvegainspline', 'gspline-vi2', False, {})
    run_case(root, bandpass, tools, 'setsolvebandpoly', 'bpoly-default', True, {})
    band_options = dict(t='int', combine='scan', degamp=2, degphase=2, visnorm=True,
                        solnorm=False, maskcenter=2, maskedge=10., refant=['A0'])
    run_case(root, bandpass, tools, 'setsolvebandpoly', 'bpoly-configured', True, band_options)
    run_case(root, bandpass, tools, 'setsolvebandpoly', 'bpoly-append', True, {}, append=band_options)
    run_case(root, bandpass, tools, 'setsolvebandpoly', 'bpoly-channel-bin', True,
             dict(t='inf,2ch', degamp=1, degphase=1, refant='A0'))
    flagged = root/'flagged.ms'
    copy_ms(bandpass, flagged)
    with opened(flagged, True) as table:
        flags = table.getcol('FLAG')
        flags[:] = True
        assert table.putcol('FLAG', flags)
    for legacy in (True, False):
        run_case(root, flagged, tools, 'setsolvebandpoly', f'bpoly-flagged-vi{1 if legacy else 2}',
                 legacy, {}, empty=True)
    run_case(root, spline, tools, 'setsolvegainspline', 'gspline-default', True, {})
    spline_options = dict(splinetime=40., preavg=8., npointaver=3, phasewrap=180., refant=['A0'])
    for mode in ('a', 'phase', 'AP', 'AMP&PHAS', 'unknown'):
        run_case(root, spline, tools, 'setsolvegainspline', f'gspline-{mode}', True,
                 dict(mode=mode, **spline_options))
    run_case(root, spline, tools, 'setsolvegainspline', 'gspline-append', True, spline_options,
             append=dict(mode='AP', **spline_options))


if __name__ == '__main__':
    main()
