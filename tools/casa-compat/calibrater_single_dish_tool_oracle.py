#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare direct single-dish calibrater solves in both visibility iterators."""

import argparse
from contextlib import chdir
from itertools import product
from pathlib import Path
from select import select
from shutil import copytree
import subprocess
import sys

import casatasks
from casatools import calibrater as casa_calibrater
import numpy as np

from calibration_smoothcal_oracle import columns, compare_mapping, opened
from calibrater_apply_tool_oracle import result, same_record
from calibrater_state_oracle import casa_log, native_log
from sdcal_edges_oracle import raster_map, regular_map
from sdcal_position_switch_oracle import make_position_switch
from sdgaincal_oracle import compare_and_read, generated_map


FAMILIES = ('SDSKY_PS', 'SDSKY_OTF', 'SDSKY_RASTER', 'SDGAIN_OTFD')
SOLVE_ERROR = (False, ('RuntimeError', 'Error in Calibrater::solve.'))
HOLD_OUTPUT_LOCK = '''
import sys
from casatools import table
tool = table()
assert tool.open(sys.argv[1], nomodify=False, lockoptions={'option': 'user'})
tool.lock(write=True, nattempts=1)
assert tool.haslock(write=True)
print('locked', flush=True)
sys.stdin.readline()
assert tool.close()
'''


def fixture(work, kind):
    work.mkdir()
    with chdir(work):
        if kind == 'SDSKY_PS':
            vis, _, _ = make_position_switch(work)
            return vis, {'scan': '1,3'}, {}
        if kind == 'SDGAIN_OTFD':
            return generated_map(work), {}, {'radius': '65arcsec', 'smooth': True}
        vis = regular_map(work)
        if kind == 'SDSKY_RASTER':
            return raster_map(vis, work/'raster.ms'), {}, {'numedge': 2}
        return vis, {}, {'fraction': .15}


def solve(factory, log, kind, old, vis, product, selection, controls,
          *, rounds=1, corrdep=False, expected_error=None):
    tool = factory()
    assert tool.setvi(old=old, quiet=True) and tool.open(str(vis))
    try:
        assert tool.selectvis(field='0', **({'spw': '0'} | selection))
        if corrdep:
            assert tool.setcorrdepflags(True)
        configured = result(tool.setsolve, kind, 'inf', str(product), **controls)
        if (kind == 'SDSKY_OTF' and not old) or expected_error == 'setsolve':
            assert configured == (False, ('RuntimeError', 'Error in Calibrater::setsolve.'))
            assert list(tool.returndict()['solve_tables']) == []
            return log(tool.state), tool.activityrec(), None
        assert configured == (True, True)
        shown = log(tool.state)
        state = shown[0], [line.replace(str(product), '<output>') for line in shown[1]]
        expect_product = old == kind.startswith('SDSKY') and expected_error is None
        if expect_product:
            for _ in range(rounds):
                assert result(tool.solve) == (True, True) and product.exists()
            recorded = [str(product)] if kind.startswith('SDSKY') else []
            assert list(tool.returndict()['solve_tables']) == recorded
        else:
            assert result(tool.solve) == SOLVE_ERROR
            assert not product.exists() and list(tool.returndict()['solve_tables']) == []
        return state, tool.activityrec(), tool.returndict().get('solvestats')
    finally:
        assert tool.close()


def compare_products(paths, kind):
    if kind == 'SDGAIN_OTFD':
        compare_and_read(paths['casa'], paths['native'])
    else:
        compare_mapping(columns(paths['native']), columns(paths['casa']), False)
        with opened(paths['casa']/'SPECTRAL_WINDOW') as expected, \
                opened(paths['native']/'SPECTRAL_WINDOW') as actual:
            assert expected.nrows() == actual.nrows()
            for row in range(expected.nrows()):
                for name in ('NUM_CHAN', 'CHAN_FREQ', 'CHAN_WIDTH', 'RESOLUTION',
                             'TOTAL_BANDWIDTH', 'FLAG_ROW'):
                    np.testing.assert_array_equal(actual.getcell(name, row),
                                                  expected.getcell(name, row), err_msg=name)


def compare_case(work, engines, label, kind, old, vis, selection, controls, *, seed=None, **kwargs):
    paths = {engine: work/f'{label}-{engine}.cal' for engine in engines}
    values = {}
    with chdir(work):
        for engine, factory in engines.items():
            if seed is not None:
                copytree(seed[engine], paths[engine])
            capture = casa_log(work/f'{label}.log') if engine == 'casa' else native_log()
            with capture as logger:
                values[engine] = solve(factory, logger, kind, old, vis, paths[engine],
                                       selection, controls, **kwargs)
    if 'native' in engines:
        expected, actual = (values[engine] for engine in ('casa', 'native'))
        assert actual[0] == expected[0], (label, actual[0], expected[0])
        same_record(actual[1], expected[1])
        same_record(actual[2], expected[2])
        if paths['casa'].exists():
            compare_products(paths, kind)
    print(label, 'matched' if 'native' in engines else 'reference', flush=True)
    return paths


def gain_cases(work, engines, kind, vis, selection, seed):
    for label, controls, kwargs in (
        ('implicit-radius', {}, {}),
        ('unsmoothed', {'radius': '65arcsec', 'smooth': False}, {}),
        ('phase', {'radius': '65arcsec', 'apmode': 'P'}, {}),
        ('amplitude', {'radius': '65arcsec', 'apmode': 'A'}, {}),
        ('append', {'radius': '65arcsec', 'append': True}, {'seed': seed}),
        ('append-missing', {'radius': '65arcsec', 'append': True}, {'expected_error': 'solve'}),
        ('invalid-radius', {'radius': '-1arcsec'}, {'expected_error': 'setsolve'}),
        ('invalid-mode', {'radius': '65arcsec', 'apmode': 'unknown'}, {'expected_error': 'solve'}),
    ):
        compare_case(work, engines, label, kind, False, vis, selection, controls, **kwargs)
    flagged = work/'gain-flagged.ms'
    copytree(vis, flagged)
    with opened(flagged, True) as table:
        flags = table.getcol('FLAG')
        flags[0, :, ::17] = True
        flags[:, :, ::43] = True
        assert table.putcol('FLAG', flags)
    for corrdep in (False, True):
        compare_case(work, engines, f'flagged-{corrdep}', kind, False, flagged,
                     selection, {'radius': '65arcsec'}, corrdep=corrdep)
    for engine, factory in engines.items():
        clear_stats_after_failure(factory, work, engine, vis)


def clear_stats_after_failure(factory, work, engine, vis):
    with chdir(work):
        tool = factory()
        assert tool.open(str(vis)) and tool.selectvis(field='0', spw='0')
        try:
            seed = work/f'stats-{engine}.cal'
            assert tool.setsolve('SDGAIN_OTFD', 'inf', str(seed), radius='65arcsec')
            assert tool.solve() and seed.exists()
            assert 'solvestats' in tool.returndict()
            activity = tool.activityrec()
            failed = work/f'failed-{engine}.cal'
            assert tool.setsolve('SDGAIN_OTFD', 'inf', str(failed), radius='65arcsec', apmode='unknown')
            assert result(tool.solve) == SOLVE_ERROR and not failed.exists()
            record = tool.returndict()
            assert 'solvestats' not in record and list(record['solve_tables']) == []
            same_record(tool.activityrec(), activity)
        finally:
            assert tool.close()


def protect_busy_output(work, factory, kind, old, vis, selection, controls, output):
    with chdir(work):
        before = columns(output)
        command = [sys.executable, '-c', HOLD_OUTPUT_LOCK, str(output)]
        with subprocess.Popen(command, text=True, stdin=subprocess.PIPE,
                              stdout=subprocess.PIPE) as peer:
            try:
                assert select([peer.stdout], [], [], 15.)[0]
                assert peer.stdout.readline().strip() == 'locked'
                tool = factory()
                assert tool.setvi(old, True) and tool.open(str(vis))
                try:
                    assert tool.selectvis(field='0', **({'spw': '0'} | selection))
                    for append in (False, True):
                        assert tool.setsolve(kind, 'inf', str(output), append=append, **controls)
                        assert result(tool.solve) == SOLVE_ERROR
                finally:
                    assert tool.close()
            finally:
                peer.communicate('\n', timeout=15)
            assert peer.returncode == 0
        compare_mapping(columns(output), before, False)
    print(kind, 'busy replace and append protected', flush=True)


def run(args):
    assert casatasks.version_string() == '6.7.6.14'
    args.work.mkdir()
    engines = {'casa': casa_calibrater}
    if not args.reference_only:
        from casars.tools import calibrater as native_calibrater

        engines['native'] = native_calibrater
    for kind, old in product(FAMILIES, (False, True)):
        label = f'{kind}-VI{1 if old else 2}'
        if args.only is not None and label != args.only:
            continue
        vis, selection, controls = fixture(args.work/label, kind)
        paths = compare_case(args.work, engines, label, kind, old, vis, selection, controls)
        if 'native' in engines and (kind, old) in (('SDSKY_PS', True), ('SDGAIN_OTFD', False)):
            # A separate CASA process holds the output. The CASA solver may
            # wait for that lock and is covered by the ordinary solve cases.
            protect_busy_output(args.work, engines['native'], kind, old, vis, selection,
                                controls, paths['native'])
        if kind == 'SDGAIN_OTFD' and not old:
            gain_cases(args.work, engines, kind, vis, selection, paths)
        if kind == 'SDSKY_PS' and old:
            compare_case(args.work, engines, 'sky-append', kind, old, vis,
                         selection | {'spw': '0:0'}, {'append': True}, rounds=2)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=lambda path: Path(path).resolve())
    parser.add_argument('--reference-only', action='store_true')
    parser.add_argument('--only', choices=[f'{kind}-VI{version}' for kind in FAMILIES
                                          for version in (1, 2)])
    run(parser.parse_args())
