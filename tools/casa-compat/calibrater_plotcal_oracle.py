#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare the retired calibrater.plotcal entry point with pinned CASA."""

import argparse
import inspect
import io
import logging
from pathlib import Path
from shutil import copytree
from tempfile import TemporaryDirectory

import numpy as np
from casatasks import casalog
from casatools import calibrater as casa_calibrater
from casars.tools import calibrater as native_calibrater


def probe(factory, seed, root):
    tool = factory()
    assert tool.plotcal([], [], []) is False
    for old in (False, True):
        ms = root / f'{factory.__module__}-{old}.ms'
        copytree(seed, ms)
        assert tool.setvi(old=old, quiet=True)
        assert tool.open(str(ms), addcorr=False, addmodel=False)
        assert tool.plotcal([], [], []) is False
        existing = root / 'existing.ps'
        existing.write_bytes(b'existing listing\n')
        assert tool.plotcal([0], 0., np.array([[0]], dtype=np.int32), 'PHASE',
                            'unavailable.cal', 2, True, 2, 3, str(existing)) is False
        assert existing.read_bytes() == b'existing listing\n'
        absent = root / 'unwritten.ps'
        assert tool.plotcal(antennas=[True], fields=['0'], spwids=[None], psfile=str(absent)) is False
        assert not absent.exists()
        assert tool.close() is True
        assert tool.plotcal([], [], []) is False
    assert tool.done() is True
    assert tool.plotcal([], [], []) is False


def errors(factory):
    tool = factory()
    for arguments, keyword, kind in (
        (('0', [], []), {}, TypeError),
        (([], b'0', []), {}, TypeError),
        (([], [], (0,)), {}, SystemError),
        ((None, [], []), {}, SystemError),
        (([], [], []), {'tablename': None}, TypeError),
        (([], [], []), {'multiplot': 1}, TypeError),
        (([], [], []), {'multiplot': np.bool_(True)}, TypeError),
    ):
        try:
            tool.plotcal(*arguments, **keyword)
        except kind:
            pass
        else:
            raise AssertionError((factory, arguments, keyword))
    tool.done()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('seed', type=Path)
    args = parser.parse_args()
    assert inspect.signature(casa_calibrater().plotcal) == inspect.signature(native_calibrater().plotcal)
    with TemporaryDirectory(prefix='calibrater-plotcal-') as tmp:
        root = Path(tmp)
        prior_log = casalog.logfile()
        casa_log = root / 'casa.log'
        casalog.setlogfile(str(casa_log))
        try:
            probe(casa_calibrater, args.seed, root)
        finally:
            casalog.setlogfile(prior_log)
        expected = casa_log.read_text()

        capture = io.StringIO()
        logger = logging.getLogger('casars.tools.calibrater')
        handler = logging.StreamHandler(capture)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        try:
            probe(native_calibrater, args.seed, root)
        finally:
            logger.removeHandler(handler)
        actual = capture.getvalue()
        for text, count in (('Must first open a MeasurementSet.', 4), ('Sorry not implemented yet', 6)):
            assert expected.count(text) == actual.count(text) == count, (text, expected, actual)
    for factory in (casa_calibrater, native_calibrater):
        errors(factory)
    print('10 paired lifecycle returns with exact messages, two file laws and seven errors')


if __name__ == '__main__':
    main()
