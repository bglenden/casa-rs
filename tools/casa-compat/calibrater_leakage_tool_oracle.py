#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare direct leakage solves, retained configuration and caltable interchange."""

import argparse
from contextlib import chdir
from pathlib import Path
import resource
import subprocess
import sys

import casatasks
from casatools import calibrater as casa_calibrater
import numpy as np

from blcal_oracle import copy_ms, opened
from calibrater_apply_tool_oracle import result, same_record
from calibrater_solve_tool_oracle import compare_readers
from polcal_leakage_solve_oracle import plant, put_observations
from polcal_standard_leakage_oracle import compare


def solve(factory, vis, output, options, *, legacy=False, selection=None, stokes=None,
          prior=None, append=False, corrdep=False):
    tool = factory()
    assert tool.setvi(legacy, True)
    assert tool.open(str(vis), addcorr=False, addmodel=False)
    assert tool.selectvis(**({'field': '0'} | (selection or {})))
    if stokes is not None:
        assert tool.setptmodel(stokes)
    assert tool.setapply(type='P')
    if prior:
        assert tool.setapply(table=str(prior), calwt=False)
    assert tool.setcorrdepflags(corrdep)
    effective = {'t': 'inf', 'refant': 'A0'} | options
    assert tool.setsolve(table=str(output), **effective)
    assert list(tool.returndict()['solve_tables']) == [str(output)]
    solved = result(tool.solve)
    assert solved == (True, True), (output.name, solved)
    if append:
        assert tool.selectvis(field='1')
        assert tool.setsolve(table=str(output), append=True, **effective)
        assert tool.solve()
    records = tool.returndict()
    assert list(records['solve_tables']) == [str(output)]
    assert 'solvestats' in records if not legacy else 'solvestats' not in records
    activity = tool.activityrec()
    assert tool.reset(False, True) and list(tool.returndict()['solve_tables']) == []
    if prior:
        assert list(tool.returndict()['apply_tables']) == [str(prior)]
    assert tool.close()
    return activity, records.get('solvestats')


def case(work, engines, vis, name, kind, *, no_product=False, casa_modern=False,
         legacy=False, options=None, **selectors):
    products, activities, stats = {}, {}, {}
    for engine, factory in engines.items():
        output = work/f'{name}-{engine}.cal'
        activity, values = solve(factory, vis, output, {'type': kind} | (options or {}),
                                 legacy=legacy and not (casa_modern and engine == 'casa'),
                                 **selectors)
        assert output.is_dir() != no_product, (name, engine)
        products[engine] = output
        activities[engine] = activity
        stats[engine] = values
    same_record(activities['native'], activities['casa'])
    if not legacy:
        same_record(stats['native'], stats['casa'])
    if no_product:
        print(name, 'no product', activities['casa'], flush=True)
    else:
        difference = compare(products['casa'], products['native'])
        compare_readers(products)
        print(name, 'CPARAM, parameter error and SNR normalized RMS', difference, flush=True)


def fixture(work, correlations):
    work.mkdir()
    vis, leakage, prior, stokes, sky = plant(work, correlations)
    observed = put_observations(vis, sky, [leakage])
    rng = np.random.default_rng(1522)
    noise = (.002 * (rng.normal(size=observed.shape) + 1j*rng.normal(size=observed.shape))).astype(np.complex64)
    with opened(vis, True) as table:
        assert table.putcol('DATA', observed + noise)
    return vis, leakage, prior, stokes, sky, noise


def assert_legacy_abort(work, vis, kind):
    command = [sys.executable, __file__, str(work), '--legacy-reference', str(vis), kind]
    process = subprocess.run(command, capture_output=True, text=True, timeout=30)
    assert process.returncode == -11 and 'configured' in process.stdout, (
        kind, process.returncode, process.stdout, process.stderr)
    print(kind, 'pinned VI1 terminates after successful setup', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=Path)
    parser.add_argument('--legacy-reference', nargs=2)
    args = parser.parse_args()
    work = args.work.resolve()
    if args.legacy_reference:
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        path, kind = args.legacy_reference
        tool = casa_calibrater()
        assert tool.setvi(True, True) and tool.open(path, addcorr=False, addmodel=False)
        assert tool.selectvis(field='0') and tool.setapply(type='P')
        assert tool.setsolve(type=kind, table=str(work/f'{kind}-abort.cal'), t='inf', refant='A0')
        print('configured', flush=True)
        tool.solve()
        return

    from casars.tools import calibrater as native_calibrater

    assert casatasks.version_string() == '6.7.6.14'
    work.mkdir()
    engines = {'casa': casa_calibrater, 'native': native_calibrater}
    with chdir(work):
        vis, leakage, prior, stokes, sky, noise = fixture(work/'circular', 'RR RL LR LL')
        for kind in ('D', 'Df', 'DLLS', 'DfLLS', 'Dgen', 'Dfgen', 'D+QU', 'Df+QU', 'D+X',
                     'Df+X', 'Dgen+QU', 'Dfgen+QU', 'Dgen+X', 'Dfgen+X', 'D JONES', 'DFGENERAL'):
            case(work, engines, vis, kind.replace(' ', '-'), kind)
        for kind in ('DLLS', 'DfLLS'):
            case(work, engines, vis, f'{kind}-legacy', kind, legacy=True)
        for kind in ('D', 'Dgen'):
            assert_legacy_abort(work, vis, kind)
            case(work, engines, vis, f'{kind}-legacy-native', kind, legacy=True, casa_modern=True)
        case(work, engines, vis, 'Df-binned', 'Df', selection={'spw': '0:2~6'},
             options={'t': 'inf,2chan', 'preavg': 0.})
        case(work, engines, vis, 'Df-baseline-cut', 'Df', options={'minblperant': 20}, no_product=True)
        case(work, engines, vis, 'D-snr-cut', 'D', options={'minsnr': 1e20})
        case(work, engines, vis, 'D-append', 'D', append=True)
        flagged = work/'flagged.ms'
        copy_ms(vis, flagged)
        with opened(flagged, True) as table:
            assert table.putcol('FLAG', np.ones_like(table.getcol('FLAG')))
        case(work, engines, flagged, 'D-flagged', 'D', no_product=True)
        observed = put_observations(vis, sky, [prior, leakage])
        with opened(vis, True) as table:
            assert table.putcol('DATA', observed + noise)
        case(work, engines, vis, 'D-point-and-prior', 'D', stokes=stokes, prior=prior)
        linear, _, _, _, _, _ = fixture(work/'linear', 'XX XY YX YY')
        case(work, engines, linear, 'D-linear', 'D')
        case(work, engines, linear, 'Df-linear', 'Df')


if __name__ == '__main__':
    main()
