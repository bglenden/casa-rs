#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare solved legacy pointing offsets and their table readers with CASA 6.7.6.14."""

import argparse
from contextlib import chdir
from itertools import product
from pathlib import Path
from shutil import copytree
from tempfile import TemporaryDirectory
from time import perf_counter

import casatasks
from casatools import calibrater as casa_calibrater
import numpy as np

from calibration_smoothcal_oracle import opened
from calibrater_apply_tool_oracle import result, same_record
from calibrater_state_oracle import casa_log, native_log


SOLUTION_COLUMNS = ('ANTENNA1', 'FIELD_ID', 'TIME', 'INTERVAL', 'GAIN', 'FLAG',
                    'SOLUTION_OK', 'FIT', 'FIT_WEIGHT', 'SNR', 'CAL_DESC_ID')


def solve(factory, capture, dataset, model, cache, product, interval, **controls):
    tool = factory()
    assert tool.setvi(old=True, quiet=True) and tool.open(str(dataset))
    try:
        assert tool.setsolve('EP', interval, str(product),
                             **({'cfcache': str(cache), 'painc': 360.} | controls))
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


def compare_cells(actual, expected):
    for column in SOLUTION_COLUMNS:
        if column == 'GAIN':
            np.testing.assert_allclose(actual[column], expected[column], rtol=1e-3, atol=1e-7)
        elif column == 'TIME':
            np.testing.assert_allclose(actual[column], expected[column], rtol=0., atol=1e-4)
        else:
            np.testing.assert_array_equal(actual[column], expected[column], err_msg=column)


def compare_append(args, products, factories):
    tables = {}
    for engine, factory in factories.items():
        incoming = args.work/f'append-{engine}.EP'
        copytree(products[engine], incoming)
        capture = casa_log(args.work/'append-casa.log') if engine == 'casa' else native_log()
        with capture as logger:
            solve(factory, logger, args.work/f'three-{engine}.ms', args.model,
                  args.work/f'three-{engine}-cache', incoming, '0s', append=True)
        tables[engine] = solution(incoming, 3)
        _, counts = np.unique(tables[engine]['TIME'], return_counts=True)
        np.testing.assert_array_equal(counts, [56, 28, 28])
    compare_cells(tables['native'], tables['casa'])
    np.testing.assert_array_equal(tables['native']['TIME'], tables['casa']['TIME'])

    incompatible = args.work/'incompatible.EP'
    copytree(products['casa'], incompatible)
    with opened(incompatible, True) as table:
        info = table.info()
        info['subType'] = 'GSPLINE'
        table.putinfo(info)
    tool = factories['native']()
    assert tool.setvi(True, True) and tool.open(str(args.work/'three-native.ms'))
    try:
        cache = args.work/'three-native-cache'
        assert tool.setsolve('EP', '0s', str(incompatible), cfcache=str(cache), append=True)
        assert tool.setmodel(str(args.model))
        assert result(tool.solve) == (False, ('RuntimeError', 'Error in Calibrater::solve.'))
    finally:
        assert tool.close()
    with opened(incompatible) as table, opened(products['casa']) as original:
        assert table.info()['subType'] == 'GSPLINE' and table.nrows() == original.nrows()
        np.testing.assert_array_equal(table.getcol('GAIN'), original.getcol('GAIN'))
    print('append epochs and incompatible destination matched', flush=True)


def compare_predictions(args, products):
    from casatools import imager as casa_imager
    from casars.tools import imager as native_imager
    from gaincal_oracle import relative_rms
    from imager_legacy_ep_multislot_oracle import predict

    readers = {'casa': casa_imager, 'native': native_imager}
    with TemporaryDirectory(dir=args.work) as path:
        work = Path(path)
        expected = None
        for writer, reader in product(products, readers):
            cells = predict(work, f'{writer}-{reader}', readers[reader], args.dataset,
                            args.model, args.cache, products[writer], 'pbwproject')
            if expected is None:
                expected = cells
            error = relative_rms(cells, expected)
            assert error < 1.e-3, (writer, reader, error)
            print(writer, reader, cells.size, 'MODEL_DATA cells', 'NRMS', error, flush=True)


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
            missing = args.work/f'without-model-{engine}.EP'
            assert tool.setsolve('EP', '1s', str(missing), cfcache=str(cache))
            assert result(tool.solve) == (False, ('RuntimeError', 'Error in Calibrater::solve.'))
            assert not missing.exists() and list(tool.returndict()['solve_tables']) == []
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


def adjust_input(name, dataset):
    if name not in ('weighted', 'shared-flags', 'skipped-first-buffer', 'parallactic-angles'):
        return
    with opened(dataset, True) as table:
        epoch = table.getcol('TIME')
        moments = np.unique(epoch)
        middle = epoch == moments[1]
        if name == 'weighted':
            weights = table.getcol('WEIGHT')
            weights[:, middle] *= np.array([[2.], [.5]], np.float32)
            weights[:, epoch == moments[2]] *= np.array([[.4], [2.5]], np.float32)
            assert table.putcol('WEIGHT', weights)
        elif name == 'shared-flags':
            flags = table.getcol('FLAG')
            flags[0, ::5, middle] = True
            data = table.getcol('DATA')
            data[1, ::5, middle] *= np.complex64(2. - .4j)
            assert table.putcol('FLAG', flags) and table.putcol('DATA', data)
        elif name == 'skipped-first-buffer':
            scans = table.getcol('SCAN_NUMBER')
            scans[epoch >= moments[1]] = scans[0] + 1
            row_flags = table.getcol('FLAG_ROW') | middle
            assert table.putcol('SCAN_NUMBER', scans) and table.putcol('FLAG_ROW', row_flags)
        else:
            shift = 1800. * np.searchsorted(moments, epoch)
            for name in ('TIME', 'TIME_CENTROID'):
                assert table.putcol(name, table.getcol(name) + shift)


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
        for name, interval, epochs, controls in (
            ('one', '0s', 1, {}), ('three', '1s', 3, {}), ('preaverage', '0s', 1, {'preavg': 10.}),
            ('weighted', '0s', 1, {}), ('shared-flags', '0s', 1, {}),
            ('skipped-first-buffer', '0s', 1, {}), ('parallactic-angles', '1s', 3, {'painc': .5}),
        ):
            if args.only is not None and name != args.only:
                continue
            products = {engine: args.work/f'{name}-{engine}.EP' for engine in ('casa', 'native')}
            results, tables = {}, {}
            for engine, factory in factories.items():
                dataset = args.work/f'{name}-{engine}.ms'
                cache = args.work/f'{name}-{engine}-cache'
                copytree(args.dataset, dataset)
                adjust_input(name, dataset)
                copytree(args.cache, cache)
                capture = casa_log(dataset.with_suffix('.log')) if engine == 'casa' else native_log()
                with capture as logger:
                    results[engine] = solve(factory, logger, dataset, args.model, cache,
                                            products[engine], interval, **controls)
                tables[engine] = solution(products[engine], epochs)
                if name == 'parallactic-angles':
                    angles = int((cache/'aux.dat').read_text().split()[0])
                    assert angles > 1
                    print(engine, angles, 'retained parallactic angles', flush=True)
            if not args.reference_only:
                assert results['native'][:2] == results['casa'][:2]
                same_record(results['native'][2], results['casa'][2])
                compare_cells(tables['native'], tables['casa'])
                for engine, cells in tables.items():
                    compare_reader(products[engine], cells)
                if name == 'three':
                    compare_predictions(args, products)
                    compare_append(args, products, factories)
            print(name, interval, epochs, 'reference' if args.reference_only else 'matched',
                  flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('work', 'dataset', 'model', 'cache'):
        parser.add_argument(f'--{name}', type=lambda path: Path(path).resolve(), required=True)
    parser.add_argument('--reference-only', action='store_true')
    parser.add_argument('--setup-only', action='store_true')
    parser.add_argument('--only', choices=('one', 'three', 'preaverage', 'weighted', 'shared-flags',
                                          'skipped-first-buffer', 'parallactic-angles'))
    run(parser.parse_args())
