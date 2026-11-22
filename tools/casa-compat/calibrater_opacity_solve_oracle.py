#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare opacity solve setup and isolate the pinned CASA solve crashes."""

import argparse
from contextlib import chdir
from itertools import product
from pathlib import Path
from shutil import copytree
from signal import SIGSEGV
import subprocess
import sys

import casatasks
from casatools import calibrater as casa_calibrater

from calibrater_apply_tool_oracle import result
from calibrater_state_oracle import casa_log, native_log
from record_casa_returns import make_cal_ms


FAMILIES = ('TOpac', 'TfOpac')
REFUSAL = (False, ('RuntimeError', 'Error in Calibrater::solve.'))


def probe(engine, family, old, vis, output):
    if engine == 'native':
        from casars.tools import calibrater
    else:
        calibrater = casa_calibrater
    tool = calibrater()
    assert tool.setvi(old, True) and tool.open(str(vis), addcorr=old, addmodel=old)
    try:
        assert tool.selectvis(field='0', spw='0:3') and tool.setptmodel([1., 0., 0., 0.])
        refant = '' if old else 'A0'
        assert tool.setsolve(family, 'inf', str(output), refant=refant, minblperant=2)
        print('configured opacity solver' if family in FAMILIES else 'configured control', flush=True)
        if engine == 'native':
            assert result(tool.solve) == REFUSAL
            assert not output.exists() and list(tool.returndict()['solve_tables']) == []
            print('recoverable solve refusal and cleared state', flush=True)
        else:
            assert tool.solve() and output.exists()
    finally:
        assert tool.close()


def setup_cases(factory, capture, vis, path, family, old):
    tool = factory()
    assert tool.setvi(old, True) and tool.open(str(vis), addcorr=False, addmodel=False)
    try:
        assert tool.selectvis(field='0', spw='0:3')
        retained = []
        for t, controls, expect in (
            ('inf', {}, True),
            ('10s,2ch', {'append': True, 'apmode': 'P', 'minsnr': .25}, True),
            ('bad,time', {}, False),
        ):
            configured = result(tool.setsolve, family, t, str(path), **controls)
            assert configured == ((True, True) if expect else
                                  (False, ('RuntimeError', 'Error in Calibrater::setsolve.')))
            assert list(tool.returndict()['solve_tables']) == ([str(path)] if expect else [])
            retained.append(capture(tool.state))
        return retained
    finally:
        assert tool.close()


def isolated(work, engine, family, old, vis):
    output = work/f'{family}-{old}-{engine}.cal'
    completed = subprocess.run([sys.executable, str(Path(__file__).resolve()), '--probe',
                                engine, family, str(old), str(vis), str(output)],
                               cwd=work, capture_output=True, text=True, check=False)
    expected = -SIGSEGV if engine == 'casa' and family in FAMILIES else 0
    assert completed.returncode == expected, (family, old, completed.stdout, completed.stderr)
    assert 'configured' in completed.stdout
    assert output.exists() == (family == 'T')
    print(engine, family, 'VI1' if old else 'VI2', 'exit', completed.returncode, flush=True)


def run(args):
    assert casatasks.version_string() == '6.7.6.14'
    args.work.mkdir()
    with chdir(args.work):
        vis = args.work/'modern.ms'
        make_cal_ms(str(vis))
        legacy = args.work/'legacy.ms'
        copytree(args.legacy_dataset, legacy)
    isolated(args.work, 'casa', 'T', False, vis)
    for family, old in product(FAMILIES, (False, True)):
        dataset = legacy if old else vis
        path = args.work/f'{family}-{old}-setup.cal'
        with casa_log(args.work/f'{family}-{old}-setup.log') as capture:
            reference = setup_cases(casa_calibrater, capture, dataset, path, family, old)
        if not args.reference_only:
            from casars.tools import calibrater as native_calibrater

            with native_log() as capture:
                native = setup_cases(native_calibrater, capture, dataset, path, family, old)
            assert native == reference, (family, old, native, reference)
            isolated(args.work, 'native', family, old, dataset)
        isolated(args.work, 'casa', family, old, dataset)


if __name__ == '__main__':
    if len(sys.argv) == 7 and sys.argv[1] == '--probe':
        _, _, engine, family, old, vis, output = sys.argv
        probe(engine, family, old == 'True', Path(vis), Path(output))
    else:
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument('work', type=lambda path: Path(path).resolve())
        parser.add_argument('--legacy-dataset', required=True, type=lambda path: Path(path).resolve())
        parser.add_argument('--reference-only', action='store_true')
        run(parser.parse_args())
