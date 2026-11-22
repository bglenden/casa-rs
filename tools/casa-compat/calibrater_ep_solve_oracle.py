#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare solved legacy pointing offsets and their table readers with CASA 6.7.6.14."""

import argparse
from contextlib import chdir
from pathlib import Path
from shutil import copytree
from time import perf_counter

import casatasks
from casatools import calibrater as casa_calibrater
import numpy as np

from calibration_smoothcal_oracle import opened
from calibrater_apply_tool_oracle import result, same_record
from calibrater_state_oracle import casa_log, native_log


SOLUTION_COLUMNS = ('ANTENNA1', 'FIELD_ID', 'TIME', 'INTERVAL', 'GAIN', 'FLAG',
                    'SOLUTION_OK', 'FIT', 'FIT_WEIGHT', 'SNR', 'CAL_DESC_ID')


def solve(factory, capture, dataset, model, cache, product, interval):
    tool = factory()
    assert tool.setvi(old=True, quiet=True) and tool.open(str(dataset))
    try:
        assert tool.setsolve('EP', interval, str(product), cfcache=str(cache), painc=360.)
        state = capture(tool.state)
        assert tool.setmodel(str(model))
        start = perf_counter()
        assert tool.solve() and product.is_dir()
        print(product.name, 'solve seconds', perf_counter()-start, flush=True)
        assert list(tool.returndict()['solve_tables']) == [str(product)]
        return state[0], [line.replace(str(product), '<output>') for line in state[1]], \
            tool.activityrec()
    finally:
        assert tool.close()


def solution(path, epochs):
    with opened(path) as table:
        assert table.info() == {'readme': '', 'subType': 'EP Jones', 'type': 'Calibration'}
        assert Path(table.getkeyword('CAL_DESC').removeprefix('Table: ')) == path/'CAL_DESC'
        assert Path(table.getkeyword('CAL_HISTORY').removeprefix('Table: ')) == path/'CAL_HISTORY'
        cells = {name: table.getcol(name) for name in SOLUTION_COLUMNS}
    with opened(path/'CAL_DESC') as description:
        assert description.nrows() == 1
        np.testing.assert_array_equal(description.getcell('SPECTRAL_WINDOW_ID', 0), [0])
    assert np.unique(cells['TIME']).size == epochs
    assert cells['GAIN'].shape[:2] == (4, 1)
    assert np.abs(cells['GAIN']).max() > .001
    return cells


def compare_reader(path, cells):
    from casars.tools import table as native_table

    reader = native_table()
    assert reader.open(str(path))
    try:
        for name, expected in cells.items():
            np.testing.assert_array_equal(reader.getcol(name), expected, err_msg=name)
    finally:
        assert reader.close()


def setup_cases(args, factories):
    cases = (
        {}, {'t': None}, {'t': 3.}, {'t': '3'}, {'t': '1min'}, {'t': '10Hz'},
        {'t': '-1s'}, {'t': 'int'}, {'t': 'inf'}, {'t': '2s,3chan'},
        {'append': True}, {'phaseonly': True}, {'apmode': 'a'},
        {'refant': '1,2', 'refantmode': 'strict'},
        {'minsnr': 9., 'solnorm': True, 'preavg': 2.}, {'table': ''}, {'type': 'EP Jones'},
    )
    expected = []
    for engine, factory in factories.items():
        dataset = args.work/f'setup-{engine}.ms'
        cache = args.work/f'setup-{engine}-cache'
        copytree(args.dataset, dataset)
        copytree(args.cache, cache)
        tool = factory()
        assert tool.setvi(old=True, quiet=True) and tool.open(str(dataset))
        try:
            for index, options in enumerate(cases):
                kwargs = dict(type='EP', table=str(args.work/'setup.EP'), cfcache=str(cache)) | options
                ready = result(tool.setsolve, **kwargs)
                capture = casa_log(args.work/f'setup-{index}.log') if engine == 'casa' else native_log()
                with capture as logger:
                    configured = ready, logger(tool.state), list(tool.returndict()['solve_tables'])
                if engine == 'casa':
                    expected.append(configured)
                else:
                    assert configured == expected[index], (options, configured, expected[index])
        finally:
            assert tool.close()
    print(len(cases), 'setup cases', 'reference' if args.reference_only else 'matched', flush=True)


def run(args):
    assert casatasks.version_string() == '6.7.6.14'
    args.work.mkdir()
    factories = {'casa': casa_calibrater}
    if not args.reference_only:
        from casars.tools import calibrater as native_calibrater

        factories['native'] = native_calibrater
    with chdir(args.work):
        setup_cases(args, factories)
        if args.setup_only:
            return
        for name, interval, epochs in (('one', '0s', 1), ('three', '1s', 3)):
            products = {engine: args.work/f'{name}-{engine}.EP' for engine in ('casa', 'native')}
            results, tables = {}, {}
            for engine, factory in factories.items():
                dataset = args.work/f'{name}-{engine}.ms'
                cache = args.work/f'{name}-{engine}-cache'
                copytree(args.dataset, dataset)
                copytree(args.cache, cache)
                capture = casa_log(dataset.with_suffix('.log')) if engine == 'casa' else native_log()
                with capture as logger:
                    results[engine] = solve(factory, logger, dataset, args.model, cache,
                                            products[engine], interval)
                tables[engine] = solution(products[engine], epochs)
            if not args.reference_only:
                assert results['native'][:2] == results['casa'][:2]
                same_record(results['native'][2], results['casa'][2])
                for column in SOLUTION_COLUMNS:
                    expected, actual = (tables[label][column] for label in ('casa', 'native'))
                    if column == 'GAIN':
                        np.testing.assert_allclose(actual, expected, rtol=1e-3, atol=1e-7)
                    elif column == 'TIME':
                        np.testing.assert_allclose(actual, expected, rtol=0., atol=1e-4)
                    else:
                        np.testing.assert_array_equal(actual, expected, err_msg=column)
                for engine, cells in tables.items():
                    compare_reader(products[engine], cells)
            print(name, interval, epochs, 'reference' if args.reference_only else 'matched',
                  flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('work', 'dataset', 'model', 'cache'):
        parser.add_argument(f'--{name}', type=lambda path: Path(path).resolve(), required=True)
    parser.add_argument('--reference-only', action='store_true')
    parser.add_argument('--setup-only', action='store_true')
    run(parser.parse_args())
