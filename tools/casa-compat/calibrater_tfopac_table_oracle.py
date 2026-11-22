#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare channelized opacity table application with CASA 6.7.6.14."""

import argparse
from contextlib import chdir
from itertools import product
from pathlib import Path
from shutil import copytree

import casatasks
from casatools import calibrater as casa_calibrater
import numpy as np

from calibration_smoothcal_oracle import opened
from calibrater_apply_tool_oracle import same_record
from calibrater_state_oracle import casa_log, native_log
from record_casa_returns import make_cal_ms


def prior(vis, name):
    casatasks.gencal(vis=str(vis), caltable=str(name), caltype='opac', parameter=[.05])
    with opened(name, True) as table:
        info = table.info()
        info['subType'] = 'TfOpac'
        table.putinfo(info)
        assert table.putkeyword('VisCal', 'TfOpac')
        for row in range(table.nrows()):
            values = np.linspace(.01, .17, 8, dtype=np.float32)[None, :]
            assert table.putcell('FPARAM', row, values)
            for column in ('PARAMERR', 'SNR', 'WEIGHT'):
                if column in table.colnames():
                    assert table.putcell(column, row, np.ones_like(values))
            flags = np.zeros(values.shape, bool)
            if row == 0:
                flags[0, 4] = True
            assert table.putcell('FLAG', row, flags)
    with opened(name/'SPECTRAL_WINDOW', True) as target, opened(vis/'SPECTRAL_WINDOW') as original:
        for column in ('NUM_CHAN', 'CHAN_FREQ', 'CHAN_WIDTH', 'EFFECTIVE_BW', 'RESOLUTION',
                       'TOTAL_BANDWIDTH'):
            assert target.putcell(column, 0, original.getcell(column, 0))


def apply(factory, capture, vis, name, legacy, weight, method):
    tool = factory()
    assert tool.setvi(old=legacy, quiet=True) and tool.open(str(vis))
    try:
        assert tool.selectvis(field='0')
        assert tool.setapply(table=str(name), calwt=weight)
        state = capture(tool.state)
        if method == 'correct':
            assert tool.correct('calflag')
        else:
            assert tool.setptmodel([1., 0., 0., 0.])
            assert tool.corrupt()
        activity = tool.activityrec()
    finally:
        assert tool.close()
    with opened(vis) as table:
        rows = (table.getcol('FIELD_ID') == 0) & (table.getcol('ANTENNA1') != table.getcol('ANTENNA2'))
        column = 'CORRECTED_DATA' if method == 'correct' else 'MODEL_DATA'
        science = table.getcol(column)
        selected = science[..., rows]
        assert np.max(np.abs(selected[0, 0] - selected[0, 7])) > .05
        columns = {key: table.getcol(key) for key in ('FLAG', 'FLAG_ROW', 'WEIGHT')}
    return state, activity, (science, columns)


def run(args):
    assert casatasks.version_string() == '6.7.6.14'
    args.work.mkdir()
    factories = {'casa': casa_calibrater}
    if not args.reference_only:
        from casars.tools import calibrater as native_calibrater

        factories['native'] = native_calibrater
    with chdir(args.work):
        vis, table = args.work/'input.ms', args.work/'channelized.cal'
        make_cal_ms(str(vis))
        prior(vis, table)
        for legacy, method, weight in product((False, True), ('correct', 'corrupt'), (False, True)):
            label = f'{method}-{legacy}-{weight}'
            results = {}
            for engine, factory in factories.items():
                target = args.work/f'{label}-{engine}.ms'
                copytree(vis, target)
                capture = casa_log(target.with_suffix('.log')) if engine == 'casa' else native_log()
                with capture as logger:
                    results[engine] = apply(factory, logger, target, table, legacy, weight, method)
            if not args.reference_only:
                actual, expected = (results[engine] for engine in ('native', 'casa'))
                assert actual[0] == expected[0], (label, actual[0], expected[0])
                same_record(actual[1], expected[1])
                np.testing.assert_allclose(actual[2][0], expected[2][0], rtol=1e-4, atol=2e-6)
                same_record(actual[2][1], expected[2][1])
            print(label, 'reference' if args.reference_only else 'matched', flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=lambda path: Path(path).resolve())
    parser.add_argument('--reference-only', action='store_true')
    run(parser.parse_args())
