#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare the retained calibrater equation reported by state() with CASA."""

import argparse
from contextlib import chdir, contextmanager
import logging
from pathlib import Path

import casatasks
from casatasks.private.callibrary import callibrary
from casatools import calibrater as casa_calibrater

from polcal_leakage_solve_oracle import plant


@contextmanager
def casa_log(path):
    sink = casatasks.casalog
    original = sink.logfile()
    assert sink.setlogfile(str(path))
    try:
        def capture(method):
            start = path.stat().st_size
            result = method()
            with path.open() as stream:
                stream.seek(start)
                return result, [line.rstrip().split('\t')[-1] for line in stream]

        yield capture
    finally:
        sink.setlogfile(original)


@contextmanager
def native_log():
    logger = logging.getLogger('casars.tools.calibrater')
    level = logger.level
    messages = []

    class Capture(logging.Handler):
        def emit(self, record):
            messages.append(record.getMessage().rstrip())

    handler = Capture()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    try:
        def capture(method):
            messages.clear()
            result = method()
            return result, messages.copy()

        yield capture
    finally:
        logger.removeHandler(handler)
        logger.setLevel(level)


def collect(factory, capture, vis, leakage, gain, output, legacy):
    tool = factory()
    assert tool.setvi(legacy, True)
    states = {'unattached': capture(tool.state)}
    assert tool.open(str(vis), addcorr=False, addmodel=False)
    try:
        states['open'] = capture(tool.state)
        assert tool.selectvis(field='0')
        assert tool.setapply(type='P') and tool.setapply(type='P')
        assert tool.setapply(table=str(leakage), interp='nearest,cubic', calwt=True)
        assert tool.setapply(table=str(gain), field='0', interp='nearest', spwmap=[0])
        states['traditional'] = capture(tool.state)

        library = callibrary()
        library.add(caltable=str(gain), field='0', calwt=True, tinterp='nearest')
        assert tool.setcallib(library.cld)
        states['library'] = capture(tool.state)

        options = (
            ('gain', 'G', '20s', {'refant': 'A3,A0', 'refantmode': 'strict', 'minsnr': 2.3}),
            ('bandpass', 'B', 'inf,2chan', {'apmode': 'p', 'solnorm': True, 'normtype': 'median'}),
            ('default-freq', 'B', 'inf', {}),
            ('leakage', 'Df', 'inf', {'append': True, 'apmode': 'unknown'}),
        )
        for label, kind, interval, kwargs in options:
            assert tool.setsolve(kind, interval, str(output), **kwargs)
            states[label] = capture(tool.state)
        assert tool.setsolvebandpoly(str(output), t='inf', refant='A1')
        states['bandpoly'] = capture(tool.state)
        if legacy:
            assert tool.setsolvegainspline(str(output), refant='A1')
            states['spline'] = capture(tool.state)

        assert tool.reset(False, True)
        states['reset-solve'] = capture(tool.state)
        assert tool.reset(True, True)
        states['reset-all'] = capture(tool.state)
    finally:
        assert tool.close()
    states['closed'] = capture(tool.state)
    assert tool.open(str(vis), addcorr=False, addmodel=False)
    try:
        states['reopened'] = capture(tool.state)
    finally:
        assert tool.close()
    assert not output.exists()
    return states


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=Path)
    args = parser.parse_args()
    assert casatasks.version_string() == '6.7.6.14'
    from casars.tools import calibrater as native_calibrater

    work = args.work.resolve()
    work.mkdir()
    with chdir(work):
        vis, leakage, gain, _, _ = plant(work, 'RR RL LR LL')
        args = (vis, leakage, gain, work/'unused.cal')
        for legacy in (False, True):
            with casa_log(work/f'casa-old{legacy}.log') as capture:
                wanted = collect(casa_calibrater, capture, *args, legacy)
            with native_log() as capture:
                received = collect(native_calibrater, capture, *args, legacy)
            assert received.keys() == wanted.keys()
            for name in wanted:
                assert received[name] == wanted[name], (legacy, name, received[name], wanted[name])
                print('VI1' if legacy else 'VI2', name, 'matched', flush=True)


if __name__ == '__main__':
    main()
