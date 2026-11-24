#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Paired ALMA total-power atmospheric correction with CASA 6.7.6.14."""

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
import os
import shutil
import time

import casatasks as casa
from casatasks.private.task_sdatmcor import get_configuration_for_atmcor
from casatools import calibrater, singledishms
import numpy as np

from calibration_smoothcal_oracle import opened

SCALARS = ('DATA_DESC_ID', 'TIME', 'SCAN_NUMBER', 'STATE_ID', 'FIELD_ID',
           'ANTENNA1', 'ANTENNA2', 'FEED1', 'OBSERVATION_ID', 'FLAG_ROW')


def window_data(table, window):
    rows = table.query(f'DATA_DESC_ID == {window}')
    try:
        column = 'FLOAT_DATA' if 'FLOAT_DATA' in rows.colnames() else 'DATA'
        return rows.getcol(column)
    finally:
        rows.close()


def compare_products(reference, result, *, check_real=True):
    with opened(reference) as a, opened(result) as b:
        assert b.nrows() == a.nrows()
        for name in SCALARS + ('WEIGHT', 'SIGMA'):
            np.testing.assert_array_equal(a.getcol(name), b.getcol(name), err_msg=name)
        for window in np.unique(a.getcol('DATA_DESC_ID')):
            left = a.query(f'DATA_DESC_ID == {window}')
            right = b.query(f'DATA_DESC_ID == {window}')
            try:
                for name in ('FLAG', 'WEIGHT_SPECTRUM', 'SIGMA_SPECTRUM'):
                    if name in left.colnames() and left.iscelldefined(name, 0):
                        np.testing.assert_allclose(left.getcol(name), right.getcol(name), atol=1e-6,
                                                   rtol=2e-5, err_msg=name)
                column = 'FLOAT_DATA' if 'FLOAT_DATA' in left.colnames() else 'DATA'
                expected, actual = left.getcol(column), right.getcol(column)
                np.testing.assert_array_equal(np.imag(actual), np.imag(expected))
                difference = float(np.max(np.abs(expected - actual)))
                print(f'{result.name}: SPW {window}, {right.nrows()} rows, max absolute difference {difference:.7g}', flush=True)
                if check_real:
                    np.testing.assert_allclose(actual, expected, atol=8e-4, rtol=1e-4)
            finally:
                left.close()
                right.close()


def originals(source, output):
    spectra = {}
    with opened(source) as main, opened(output) as rows:
        source_keys = zip(*(main.getcol(column) for column in SCALARS[:-1]))
        by_key = {key: index for index, key in enumerate(source_keys)}
        selected_keys = list(zip(*(rows.getcol(column) for column in SCALARS[:-1])))
        for window in np.unique(rows.getcol('DATA_DESC_ID')):
            subset = [by_key[key] for key in selected_keys if key[0] == window]
            original = main.selectrows(subset)
            try:
                spectra[window] = original.getcol('DATA')
            finally:
                original.close()
    return spectra


def check_originals(source, output, *, process=(19, 23)):
    corrections = {}
    with opened(output) as table, opened(source / 'STATE') as state:
        description = table.getcol('DATA_DESC_ID')
        intent = state.getcol('OBS_MODE')
        on_source = np.array([intent[index].startswith('OBSERVE_TARGET#ON_SOURCE')
                              for index in table.getcol('STATE_ID')])
        for window, raw in originals(source, output).items():
            correction = raw - window_data(table, window)
            eligible = on_source[description == window] & (window in process)
            np.testing.assert_array_equal(correction[..., ~eligible], 0.)
            np.testing.assert_array_equal(correction.imag, 0.)
            corrections[window] = correction
    if process:
        assert max(float(np.max(abs(values))) for values in corrections.values()) > 0.1
    return corrections


def pair(root, label, source, tasks, *, check_real=True, **kwargs):
    paths = {}
    for name, task in tasks.items():
        output = root / f'{label}-{name}.ms'
        start = time.perf_counter()
        assert task(str(source), outfile=str(output), **kwargs) is None
        print(f'{label}: {name} {time.perf_counter() - start:.3f}s', flush=True)
        paths[name] = output
    if 'native' in paths:
        compare_products(paths['casa'], paths['native'], check_real=check_real)
    return paths


def erase(paths):
    for path in paths.values():
        shutil.rmtree(path)


def test_windows(root, source, defaults, tasks):
    combinations = [
        ('19', '19,23', (19, 23), (19,)),
        ('19,23', '23', (23,), (23,)),
    ]
    for index, (process, output, windows, apply) in enumerate(combinations):
        paths = pair(root, f'windows-{index}', source, tasks, spw=process, outputspw=output)
        for path in paths.values():
            with opened(path) as table:
                np.testing.assert_array_equal(np.unique(table.getcol('DATA_DESC_ID')), windows)
            check_originals(source, path, process=apply)
        erase(paths)
    disjoint = dict(spw='19', outputspw='23')
    try:
        tasks['casa'](str(source), outfile=str(root / 'disjoint-casa.ms'), **disjoint)
    except RuntimeError as error:
        assert 'processspw' in str(error)
    else:
        raise AssertionError('Expected the CASA 6.7.6.14 empty processspw error')
    if 'native' in tasks:
        output = root / 'disjoint-native.ms'
        assert tasks['native'](str(source), outfile=str(output), **disjoint) is None
        check_originals(source, output, process=())
        shutil.rmtree(output)
    factors = {19: 1.35, 23: .65}
    combined = [
        ('scalar-gain', 1.35, {19: 1.35, 23: 1.35}),
        ('dictionary-gain', factors, factors),
    ]
    caltable = root / 'factors.cal'
    for window, factor in factors.items():
        casa.gencal(str(source), str(caltable), caltype='amp', spw=str(window),
                    parameter=[1./np.sqrt(factor)])
    combined.append(('table-gain', str(caltable), factors))
    for label, parameter, gain in combined:
        paths = pair(root, label, source, tasks, gainfactor=parameter)
        for name, path in paths.items():
            correction = check_originals(source, path)
            for window, values in correction.items():
                np.testing.assert_allclose(values, defaults[name][window] * gain[window],
                                           rtol=3e-4, atol=2e-4)
        erase(paths)


def test_detailed(root, source, default_paths, tasks):
    model = dict(atmtype=3, atmdetail=True, altitude='5.05km', temperature='273K',
                 pressure='0.55bar', humidity=20., pwv='500um', dtem_dh='-5K/km',
                 h0=2.5, dp='8mbar', dpm=1.15, layerboundaries=[5400., 6200.],
                 layertemperature='255K,247K')
    detailed = pair(root, 'explicit-profile', source, tasks, **model)
    for name, result in detailed.items():
        with opened(result) as a, opened(default_paths[name]) as b:
            assert np.max(np.abs(window_data(a, 19) - window_data(b, 19))) > .01
    workers = os.environ.get('OMP_NUM_THREADS')
    os.environ['OMP_NUM_THREADS'] = '1'
    try:
        other = pair(root, 'fixed-pressure-ratio', source, tasks, **(model | {'dpm':1.9}))
    finally:
        if workers is None:
            del os.environ['OMP_NUM_THREADS']
        else:
            os.environ['OMP_NUM_THREADS'] = workers
    for name, first in detailed.items():
        with opened(first) as a, opened(other[name]) as b:
            for window in (19, 23):
                np.testing.assert_array_equal(window_data(a, window), window_data(b, window))
    erase(detailed)
    erase(other)


def test_selection(root, source, tasks):
    with opened(source) as table:
        stamp = float(np.min(table.getcol('TIME')))
    epoch = datetime(1858, 11, 17, tzinfo=timezone.utc)
    lower = (epoch + timedelta(seconds=stamp + 60)).strftime('%Y/%m/%d/%H:%M:%S')
    selected = pair(root, 'selection', source, tasks, outputspw='19', spw='19', antenna='PM02',
                    field='1', scan='4~8', correlation='XX', timerange=f'>{lower}',
                    intent='OBSERVE_TARGET#ON_SOURCE', observation='0', feed='0&&&',
                    msselect='ANTENNA1 == 1')
    for path in selected.values():
        with opened(path) as table:
            assert 0 < table.nrows() < 1648
            assert table.getcell('DATA', 0).shape == (1, 512)
    erase(selected)


def test_cropped_windows(root, source, defaults, tasks):
    paths = {}
    scan = 7
    crop = slice(370, 408, 2)
    for name, task in tasks.items():
        output = root / f'cropped-{name}.ms'
        assert task(str(source), outfile=str(output), spw='19', outputspw='19:370~407^2', scan=str(scan)) is None
        paths[name] = output
        with opened(defaults[name]) as main, opened(output) as product, opened(output / 'SPECTRAL_WINDOW') as spw:
            full = main.query(f'DATA_DESC_ID == 19 and SCAN_NUMBER == {scan}')
            try:
                data = full.getcol('DATA')
                array = product.getcol('DATA')
                assert spw.getcell('NUM_CHAN', 19) == array.shape[1] == 19
                np.testing.assert_array_equal(product.getcol('FLAG'), full.getcol('FLAG')[:, crop, :])
                if name == 'native':
                    np.testing.assert_array_equal(array, data[:, crop, :])
                else:
                    raw = originals(source, output)[19]
                    correction = raw - data
                    np.testing.assert_allclose(raw[:, crop, :] - array, correction[:, :array.shape[1], :],
                                               rtol=0., atol=3e-7)
                    assert np.max(np.abs(array - data[:, crop, :])) > 3e-4
            finally:
                full.close()
    erase(paths)
    if 'native' in tasks:
        task = tasks['native']
        for label, selector, indices, widths in (
            ('out-of-order', '370~372;20~22', [370, 371, 372, 20, 21, 22], [1] * 6),
            ('overlap', '2~9^3;8~9', [2, 5, 8, 8, 9], [3, 3, 2, 1, 1]),
        ):
            output = root / f'{label}-channels.ms'
            assert task(str(source), outfile=str(output), spw='19', outputspw=f'19:{selector}',
                        scan=str(scan)) is None
            with opened(defaults['native']) as main, opened(output) as product, \
                    opened(output / 'SPECTRAL_WINDOW') as spw, opened(source / 'SPECTRAL_WINDOW') as original:
                full = main.query(f'DATA_DESC_ID == 19 and SCAN_NUMBER == {scan}')
                try:
                    np.testing.assert_array_equal(product.getcol('DATA'), full.getcol('DATA')[:, indices, :])
                    np.testing.assert_array_equal(product.getcol('FLAG'), full.getcol('FLAG')[:, indices, :])
                    # The MS writer centers a strided selection's channel
                    # metadata over the represented interval.
                    frequencies = original.getcell('CHAN_FREQ', 19)
                    centers = [np.mean(frequencies[i:i + w]) for i, w in zip(indices, widths)]
                    np.testing.assert_allclose(spw.getcell('CHAN_FREQ', 19), centers, rtol=0., atol=1e-4)
                finally:
                    full.close()
            shutil.rmtree(output)


def test_review_cases(root, source, tasks):
    selectors = dict(spw='19', outputspw='19', scan='7')
    antennas = pair(root, 'antenna-pair', source, tasks, antenna='0&1', **selectors)
    for output in antennas.values():
        with opened(output) as table:
            assert table.nrows() == 232
            np.testing.assert_array_equal(np.unique(table.getcol('ANTENNA1')), [1])
    erase(antennas)
    dry = pair(root, 'dry-sky', source, tasks, atmdetail=True, humidity=0., pwv='0mm', **selectors)
    for path in dry.values():
        with opened(path) as table:
            assert np.isfinite(table.getcol('DATA')).all()
    erase(dry)
    zero_gain = root / 'zero-gain.cal'
    casa.gencal(str(source), str(zero_gain), caltype='amp', spw='19', parameter=[0.])
    if 'native' in tasks:
        output = root / 'finite-output.ms'
        native = tasks['native']
        native(str(source), outfile=str(output), **selectors)
        with opened(output) as table:
            previous = table.getcol('DATA')
        for factor in (str(zero_gain), 1e40):
            try:
                native(str(source), outfile=str(output), overwrite=True, gainfactor=factor, **selectors)
            except RuntimeError as error:
                assert 'finite' in str(error)
            else:
                raise AssertionError(f'Expected refusal of gainfactor={factor}')
            with opened(output) as table:
                np.testing.assert_array_equal(table.getcol('DATA'), previous)
        shutil.rmtree(output)
    for factor, label in ((str(zero_gain), 'zero'), (1e40, 'overflow')):
        output = root / f'{label}-gain-casa.ms'
        tasks['casa'](str(source), outfile=str(output), gainfactor=factor, **selectors)
        with opened(output) as table:
            assert np.any(~np.isfinite(table.getcol('DATA')) & ~table.getcol('FLAG'))
        shutil.rmtree(output)
    shutil.rmtree(zero_gain)


def test_equal_elevation(root, source, tasks):
    fixed = root / 'fixed-elevation-input.ms'
    shutil.copytree(source, fixed)
    with opened(fixed / 'POINTING', True) as pointing:
        directions = pointing.getcol('DIRECTION')
        directions[1, ...] = .9
        pointing.putcol('DIRECTION', directions)
    paths = pair(root, 'equal-elevation', fixed, tasks, spw='19', outputspw='19', scan='7')
    for path in paths.values():
        check_originals(fixed, path, process=())
    erase(paths)
    shutil.rmtree(fixed)


def test_data_columns(root, source, tasks):
    for datacolumn, alternate in (('corrected', 'CORRECTED_DATA'), ('float_data', 'FLOAT_DATA')):
        renamed = root / f'{datacolumn}-input.ms'
        shutil.copytree(source, renamed)
        calibration = calibrater()
        try:
            assert calibration.open(str(renamed), addcorr=True, addmodel=False)
        finally:
            calibration.close()
        with opened(renamed, True) as table:
            if alternate not in table.colnames():
                descriptor = table.getcoldesc('DATA')
                descriptor.update(name=alternate, dataManagerType='StandardStMan',
                                  dataManagerGroup=f'Sdatmcor{alternate}', ndim=2, option=0)
                descriptor.pop('shape', None)
                if alternate == 'FLOAT_DATA':
                    descriptor['valueType'] = 'float'
                table.addcols({alternate:descriptor})
            if alternate == 'FLOAT_DATA':
                for row in range(table.nrows()):
                    values = table.getcell('DATA', row)
                    table.putcell(alternate, row, values.real)
        paths = pair(root, datacolumn, renamed, tasks, datacolumn=datacolumn, outputspw='19')
        erase(paths)
        shutil.rmtree(renamed)


def compare_readers(paths):
    from casars.tools import table as native_table

    for path in paths.values():
        tool = native_table()
        try:
            assert tool.open(str(path))
            with opened(path) as reference:
                assert tool.nrows() == reference.nrows()
                for row in (0, reference.nrows() - 1):
                    for column in ('DATA', 'FLAG', 'WEIGHT', 'DATA_DESC_ID'):
                        np.testing.assert_array_equal(tool.getcell(column, row), reference.getcell(column, row))
        finally:
            assert tool.close()


def test_errors(root, source, tasks):
    for name, task in tasks.items():
        output = root / f'error-{name}.ms'
        cases = [
            ('', {}, Exception, 'infile MUST'),
            (str(root / 'missing.ms'), {}, Exception, 'infile does not exist'),
            (str(source), {'atmtype':0}, Exception, 'should be any one'),
            (str(source), {'humidity':120.}, Exception, 'range 0~100'),
            (str(source), {'datacolumn':'wrong'}, AssertionError, 'unallowed value wrong'),
            (str(source), {'datacolumn':'DATA'}, AssertionError, 'unallowed value DATA'),
            (str(source), {'datacolumn':None}, AssertionError, 'null value not allowed'),
            (str(source), {'datacolumn':2}, AssertionError, 'must be of cStr type'),
            (str(source), {'gainfactor':str(root / 'missing.cal')}, FileNotFoundError, 'should exist'),
            (str(source), {'atmdetail':True,'layerboundaries':[5400, 6300],
                           'layertemperature':[250.]}, RuntimeError, 'should be the same'),
        ]
        for infile, kwargs, error_type, message in cases:
            try:
                task(infile, outfile=str(output), **kwargs)
            except error_type as error:
                assert message in str(error), str(error)
            else:
                raise AssertionError(f'Expected {error_type.__name__} for {kwargs}')
            if output.exists():
                assert name == 'casa'
                shutil.rmtree(output)
        assert task(str(source), outfile=str(output), outputspw='19', scan='7') is None
        try:
            task(str(source), outfile=str(output), outputspw='19', scan='7')
        except Exception as error:
            assert 'Specified outfile already exist.' in str(error)
        else:
            raise AssertionError('A protected output was replaced')
        assert task(str(source), outfile=str(output), outputspw='19', scan='7', overwrite=True) is None
        check_originals(source, output, process=(19,))
        shutil.rmtree(output)


def test_tool(root, source, tasks):
    factories = {'casa': singledishms}
    if 'native' in tasks:
        from casars.tools import singledishms as native_tool
        factories['native'] = native_tool
    defaults = dict(atmtype=2, atmdetail=False, altitude='', temperature='', pressure='',
                    humidity=-1., pwv='', dtem_dh='', h0='', dp='', dpm=-1.,
                    layerboundaries='', layertemperature='')
    config = get_configuration_for_atmcor(str(source), '19', '19,23', 1.0, defaults)
    outputs = {}
    for name, factory in factories.items():
        tool = factory()
        output = root / f'direct-{name}.ms'
        try:
            assert tool.open(str(source))
            assert tool.set_selection(spw='19,23', reindex=False)
            assert tool.atmcor(config, 'data', str(output))
        finally:
            assert tool.close()
        check_originals(source, output, process=(19,))
        outputs[name] = output
    if 'native' in outputs:
        compare_products(outputs['casa'], outputs['native'])
        compare_readers(outputs)
    erase(outputs)



def diagnostic_records(records):
    return [
        (priority, message) for priority, message in records
        if message.startswith(('Setting numThreads_ to ', 'processspw (input) = '))
        or message.endswith(' output but not corrected')
    ]


def test_task_diagnostics(root, source, tasks):
    malformed = root / 'missing-pressure.ms'
    shutil.copytree(source, malformed)
    with opened(malformed / 'ASDM_CALATMOSPHERE', True) as table:
        table.removecols('groundPressure')

    non_science = root / 'non-science-window.ms'
    shutil.copytree(source, non_science)
    with opened(source / 'DATA_DESCRIPTION') as description:
        windows = description.getcol('SPECTRAL_WINDOW_ID')
    with opened(source / 'STATE') as state:
        modes = state.getcol('OBS_MODE')
    calibrator_state = next(index for index, mode in enumerate(modes)
                            if not mode.startswith('OBSERVE_TARGET'))
    with opened(non_science, True) as table:
        states = table.getcol('STATE_ID')
        states[windows[table.getcol('DATA_DESC_ID')] == 19] = calibrator_state
        table.putcol('STATE_ID', states)

    threads = ('INFO', 'Setting numThreads_ to 2')
    requested = ('INFO', 'processspw (input) = [23]')
    warning = ('WARN', 'SPW 19 is output but not corrected')
    ddids = ','.join(map(str, np.flatnonzero(windows == 23)))
    cases = [
        ('repeated', source, {}, [threads, requested, warning]),
        ('filtered', source, {'msselect': f'DATA_DESC_ID IN [{ddids}]'},
         [threads, requested]),
        ('malformed', malformed, {}, [threads, requested, warning]),
        ('non-science', non_science, {'spw': '23,19,23'},
         [threads, ('INFO', 'processspw (input) = [19, 23]'), warning]),
    ]
    previous_log = casa.casalog.logfile()
    env_names = ('OMP_NUM_THREADS', 'CASA_RS_LOG_TABLE', 'CASA_RS_LOG_TABLE_PRIORITY')
    previous_env = {name: os.environ.get(name) for name in env_names}
    os.environ['OMP_NUM_THREADS'] = '2'
    os.environ['CASA_RS_LOG_TABLE_PRIORITY'] = 'INFO'
    try:
        for name, task in tasks.items():
            for label, input_ms, options, expected in cases:
                logfile = root / f'diagnostics-{name}-{label}.log'
                output = root / f'diagnostics-{name}-{label}.ms'
                if name == 'casa':
                    assert casa.casalog.setlogfile(str(logfile))
                else:
                    os.environ['CASA_RS_LOG_TABLE'] = str(logfile)
                arguments = {'spw': '23,23', 'outputspw': '23,19,19', **options}
                try:
                    assert task(str(input_ms), outfile=str(output), **arguments) is None
                except RuntimeError as error:
                    assert label == 'malformed' and 'groundPressure' in str(error), str(error)
                else:
                    assert label != 'malformed'
                if name == 'casa':
                    records = [line.split('\t', 3) for line in logfile.read_text().splitlines()]
                    actual = diagnostic_records((line[1], line[3]) for line in records if len(line) == 4)
                else:
                    with opened(logfile) as table:
                        actual = diagnostic_records(zip(table.getcol('PRIORITY'), table.getcol('MESSAGE')))
                assert actual == expected, (name, label, actual, expected)
                if output.exists():
                    shutil.rmtree(output)
        if 'native' in tasks:
            logfile = root / 'diagnostics-warn-only.log'
            os.environ['CASA_RS_LOG_TABLE'] = str(logfile)
            os.environ['CASA_RS_LOG_TABLE_PRIORITY'] = 'WARN'
            output = root / 'diagnostics-warn-only.ms'
            assert tasks['native'](str(source), outfile=str(output), spw='23') is None
            with opened(logfile) as table:
                assert diagnostic_records(zip(table.getcol('PRIORITY'), table.getcol('MESSAGE'))) == [warning]
            shutil.rmtree(output)
    finally:
        assert casa.casalog.setlogfile(previous_log)
        for name, value in previous_env.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
    shutil.rmtree(malformed)
    shutil.rmtree(non_science)
    print('Atmospheric selection, startup and WARN diagnostics match CASA.', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source', type=Path, help='Upstream measurementset/almasd/X320b_sel2.ms')
    parser.add_argument('work', type=Path)
    parser.add_argument('--reference-only', action='store_true')
    parser.add_argument('--default-only', action='store_true')
    args = parser.parse_args()
    root = args.work.resolve()
    root.mkdir()
    source = args.source.resolve()
    assert casa.version_string() == '6.7.6.14'
    tasks = {'casa': casa.sdatmcor}
    if not args.reference_only:
        from casars.tasks import sdatmcor
        tasks['native'] = sdatmcor
    defaults = pair(root, 'default', source, tasks)
    correction = {name: check_originals(source, path) for name, path in defaults.items()}
    if 'native' in defaults:
        compare_readers(defaults)
        squared_error = sum(np.sum(np.abs(correction['native'][window] - ref)**2)
                            for window, ref in correction['casa'].items())
        squared_signal = sum(np.sum(np.abs(ref)**2) for ref in correction['casa'].values())
        nrms = np.sqrt(squared_error / squared_signal)
        print(f'Normalized RMS of the atmospheric correction {nrms:.7g}', flush=True)
        assert nrms < 1e-3
    if not args.default_only:
        test_windows(root, source, correction, tasks)
        test_detailed(root, source, defaults, tasks)
        test_selection(root, source, tasks)
        test_tool(root, source, tasks)
        test_data_columns(root, source, tasks)
        test_equal_elevation(root, source, tasks)
        test_cropped_windows(root, source, defaults, tasks)
        test_review_cases(root, source, tasks)
        test_errors(root, source, tasks)
        test_task_diagnostics(root, source, tasks)
    print('ALMA atmospheric correction, selection and MeasurementSet interchange passed.', flush=True)


if __name__ == '__main__':
    main()
