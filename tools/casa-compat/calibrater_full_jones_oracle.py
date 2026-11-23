#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare J and Jf calibration application with CASA and matrix algebra."""

import argparse
from contextlib import chdir
from itertools import product
from pathlib import Path
from shutil import copytree

import casatasks
from casatools import calibrater as casa_calibrater
import numpy as np

from calibration_smoothcal_oracle import opened
from calibrater_apply_tool_oracle import result, same_record
from calibrater_state_oracle import casa_log, native_log
from record_casa_returns import make_cal_ms


DATA = np.asarray([[1.7, .2+.3j], [.2-.3j, .7]], np.complex64)
MODEL = np.asarray([[1.2+.1j, -.1+.2j], [.04-.2j, .8+.03j]], np.complex64)


def source(work, name, layout):
    path = work/f'{name}.ms'
    make_cal_ms(str(path), stokes=layout, autocorrelation_weight=1.)
    with opened(path, True) as table:
        shape = table.getcol('DATA').shape
        for column, matrix in (('DATA', DATA), ('MODEL_DATA', MODEL)):
            vector = matrix.ravel() if shape[0] == 4 else matrix.diagonal()
            values = np.broadcast_to(vector[:, None, None], shape).copy()
            assert table.putcol(column, values)
        for column, value in (('FLAG', False), ('FLAG_ROW', False), ('WEIGHT', 7.), ('SIGMA', 2.)):
            assert table.putcol(column, np.full_like(table.getcol(column), value))
    return path


def make_prior(work, vis, kind, variant):
    path = work/f'{kind}-{variant}.cal'
    subtype = kind+' Jones'
    casatasks.gencal(vis=str(vis), caltable=str(path), caltype='amp', parameter=[1.])
    matrices = {}
    channels = 1
    if kind == 'Jf':
        with opened(path/'SPECTRAL_WINDOW', True) as target, opened(vis/'SPECTRAL_WINDOW') as source:
            channels = source.getcell('NUM_CHAN', 0)
            for name in ('CHAN_FREQ', 'CHAN_WIDTH', 'EFFECTIVE_BW', 'RESOLUTION',
                         'NUM_CHAN', 'TOTAL_BANDWIDTH'):
                assert target.putcell(name, 0, source.getcell(name, 0))
    with opened(path, True) as table:
        table.putinfo(table.info() | {'subType': subtype})
        assert table.putkeyword('VisCal', subtype)
        for row in range(table.nrows()):
            antenna = table.getcell('ANTENNA1', row)
            value = np.asarray([1.+.06*antenna, .04j*(1+antenna), .02-.01j*antenna,
                                .8+.03*antenna], np.complex64)
            values = np.tile(value[:, None], (1, channels))
            values[0] *= np.linspace(1., 1.3, channels)
            flags = np.zeros(values.shape, bool)
            if antenna == 1 and variant == 'table-flag':
                flags[2, :] = True
            if antenna == 1 and variant == 'singular':
                values[:] = np.array([1, 2, 2, 4])[:, None]
            matrices[antenna] = values.T.reshape(channels, 2, 2)
            assert table.putcell('CPARAM', row, values)
            assert table.putcell('FLAG', row, flags)
            for name in ('PARAMERR', 'SNR', 'WEIGHT'):
                if name in table.colnames():
                    assert table.putcell(name, row, np.ones(values.shape, np.float32))
    return path, matrices


def check_matrix(columns, matrices, field, first, second, kind, variant, method, selected):
    correct = method == 'correct'
    data = columns['CORRECTED_DATA' if correct else 'MODEL_DATA']
    flags = columns['FLAG']
    np.testing.assert_array_equal(columns['WEIGHT'], 7.)
    np.testing.assert_array_equal(columns['SIGMA'], 2.)
    for row in np.flatnonzero(field == 0):
        for channel in (range(1, 7) if selected else range(data.shape[1])):
            plane = channel if kind == 'Jf' else 0
            antennas = first[row], second[row]
            invalid = variant == 'table-flag' or (correct and variant == 'singular')
            bad = [invalid and antenna == 1 for antenna in antennas]
            left, right = [np.eye(2) if flag else (
                np.linalg.inv(matrices[antenna][plane]) if correct else matrices[antenna][plane]
            ) for flag, antenna in zip(bad, antennas)]
            expected = left @ (DATA if correct else MODEL) @ right.conj().T
            np.testing.assert_allclose(data[:, channel, row], expected.ravel(), atol=3e-6, rtol=3e-6)
            if correct:
                np.testing.assert_array_equal(flags[:, channel, row], any(bad) or (
                    variant == 'data-flag' and channel == 1))


def apply_case(work, vis, engines, kind, variant, method, legacy, prior, *, spw='', refused=False):
    name = f'{kind}-{variant}-{legacy}-{method}'
    table_path, matrices = prior
    results = {}
    for engine, factory in engines.items():
        path = work/f'{name}-{engine}.ms'
        copytree(vis, path)
        if variant == 'data-flag':
            with opened(path, True) as table:
                flags = table.getcol('FLAG')
                flags[2, 1, :] = True
                assert table.putcol('FLAG', flags)
        tool = factory()
        assert tool.setvi(legacy, True) and tool.open(str(path), addmodel=True)
        assert tool.selectvis(field='FLUX', spw=spw)
        assert tool.setapply(table=str(table_path), calwt=True)
        capture = casa_log(work/f'{name}.log') if engine == 'casa' else native_log()
        with capture as log:
            state = log(tool.state)
        executed = result(tool.correct, 'calflag') if method == 'correct' else result(tool.corrupt)
        if refused:
            operation = 'corrupt2' if method == 'corrupt' and not legacy else method
            assert executed == (False, ('RuntimeError', f'Error in Calibrater::{operation}.'))
            assert tool.close()
            continue
        assert executed == (True, True), (name, engine, executed)
        activity = tool.activityrec()
        assert tool.close()
        with opened(path) as table:
            columns = {column: table.getcol(column) for column in (
                'CORRECTED_DATA', 'MODEL_DATA', 'FLAG', 'FLAG_ROW', 'WEIGHT', 'SIGMA',
            )}
            rows = tuple(table.getcol(key) for key in ('FIELD_ID', 'ANTENNA1', 'ANTENNA2'))
        check_matrix(columns, matrices, *rows, kind, variant, method, bool(spw))
        results[engine] = state, activity, columns
    if 'native' in results:
        reference, candidate = results['casa'], results['native']
        assert candidate[0] == reference[0], (name, candidate[0], reference[0])
        same_record(candidate[1], reference[1])
        for column, values in reference[2].items():
            np.testing.assert_allclose(candidate[2][column], values, rtol=3e-6, atol=3e-6,
                                       err_msg=f'{name} {column}')
    print(name, 'matched' if 'native' in engines else 'reference', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=lambda path: Path(path).resolve())
    parser.add_argument('--reference-only', action='store_true')
    args = parser.parse_args()
    assert casatasks.version_string() == '6.7.6.14'
    engines = {'casa': casa_calibrater}
    if not args.reference_only:
        from casars.tools import calibrater

        engines['native'] = calibrater
    work = args.work
    work.mkdir()
    with chdir(work):
        circular = source(work, 'circular', 'RR RL LR LL')
        linear = source(work, 'linear', 'XX XY YX YY')
        two_hand = source(work, 'parallel', 'RR LL')
        for kind, vis in [('J', circular), ('Jf', linear)]:
            prior = make_prior(work, vis, kind, 'default')
            for legacy, method in product((True, False), ('correct', 'corrupt')):
                apply_case(work, vis, engines, kind, 'default', method, legacy, prior)
            for legacy in (True, False):
                apply_case(work, two_hand, engines, kind, 'two-hand', 'correct', legacy, prior,
                           refused=True)
            apply_case(work, vis, engines, kind, 'selected', 'correct', False, prior, spw='0:1~6')
        for variant in ('table-flag', 'data-flag', 'singular'):
            prior = make_prior(work, circular, 'J', variant)
            for legacy in (True, False):
                apply_case(work, circular, engines, 'J', variant, 'correct', legacy, prior)


if __name__ == '__main__':
    main()
