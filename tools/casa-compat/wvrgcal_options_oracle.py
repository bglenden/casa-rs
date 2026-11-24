#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare nondefault ALMA WVR task and direct-tool behavior with CASA 6.7.6.14."""

import argparse
from contextlib import redirect_stdout
import inspect
from io import StringIO
import os
import re
import shutil
import signal
import subprocess
import sys
from pathlib import Path
import time
from unittest.mock import patch

import casatasks
from casatools import ctsys, wvr
import numpy as np

from applycal_oracle import copy_ms
from calibration_smoothcal_oracle import opened
from wvrgcal_oracle import fixture


def extra_inputs(ms, offset_table):
    """Add a second source, a valid POINTING series, and impaired antenna DA05."""
    from casacore.tables import makearrcoldesc, makescacoldesc, maketabdesc, table

    for subtable, name in (('FIELD', 'SCIENCE'), ('SOURCE', 'SCIENCE')):
        with opened(ms / subtable, True) as tab:
            values = {col: tab.getcell(col, 0) for col in tab.colnames()
                      if tab.iscelldefined(col, 0)}
            row = tab.nrows()
            tab.addrows(1)
            for column, value in values.items():
                tab.putcell(column, row, value)
            tab.putcell('NAME', row, name)
            tab.putcell('SOURCE_ID', row, 1)
    with opened(ms / 'HISTORY', True) as tab:
        row = tab.nrows()
        tab.addrows(1)
        tab.putcell('APPLICATION', row, 'input-only sentinel')
    with opened(ms / 'POINTING', True) as tab:
        for row in range(tab.nrows()):
            direction = tab.getcell('DIRECTION', row)
            direction[1, :] = 1.4833566897263437
            tab.putcell('DIRECTION', row, direction)
    with opened(ms, True) as tab:
        times, antenna = tab.getcol('TIME'), tab.getcol('ANTENNA1')
        first_time = min(times)
        tab.putcol('FIELD_ID', (times >= first_time + 12).astype('int32'))
        autos = np.flatnonzero((tab.getcol('DATA_DESC_ID') == 1) &
                               (antenna == tab.getcol('ANTENNA2')))
        for row in autos[(antenna[autos] == 4) & (times[autos] < first_time + 8)]:
            tab.putcell('FLAG', int(row), np.ones((1, 4), bool))
    description = maketabdesc([makescacoldesc('TIME', 0.), makescacoldesc('ANTENNA', 0),
                               makearrcoldesc('OFFSETS', 0., ndim=1, shape=[4])])
    with table(str(offset_table), description, nrow=len(autos), ack=False) as offsets:
        offsets.putcol('TIME', times[autos])
        offsets.putcol('ANTENNA', antenna[autos])
        offsets.putcol('OFFSETS', np.tile([.02, .01, -.02, .01], (len(autos), 1)))


def compare_tables(reference, candidate):
    columns = ('TIME', 'FIELD_ID', 'SPECTRAL_WINDOW_ID', 'ANTENNA1', 'ANTENNA2',
               'INTERVAL', 'SCAN_NUMBER', 'OBSERVATION_ID', 'FLAG', 'PARAMERR', 'SNR')
    with opened(reference) as want, opened(candidate) as got:
        assert got.info()['subType'] == want.info()['subType'] == 'T Jones'
        for col in columns:
            np.testing.assert_array_equal(got.getcol(col), want.getcol(col), err_msg=col)
        actual, expected = got.getcol('CPARAM'), want.getcol('CPARAM')
    assert actual.shape == expected.shape and expected.size
    error = np.linalg.norm(actual - expected) / np.linalg.norm(expected)
    assert error <= 1e-3, (reference.name, error)
    with opened(candidate / 'HISTORY') as history:
        assert history.nrows() == 1 and 'WVR' in history.getcell('APPLICATION', 0)
    return float(error)


def compare_reports(reference, candidate):
    assert reference.keys() == candidate.keys()
    for key, expected in reference.items():
        if key in ('RMS_um', 'Disc_um'):
            np.testing.assert_allclose(candidate[key], expected, rtol=1e-3, atol=0.001, err_msg=key)
        else:
            np.testing.assert_array_equal(candidate[key], expected, err_msg=key)


def run_cases(root, vis, native, native_wvr):
    cases = (
        ('segmented', {}, {}),
        ('pointing', {'usefieldtab': False}, {}),
        ('two-fits', {'segsource': False, 'nsol': 2}, {}),
        ('time-offset', {'toffset': -1.}, {}),
        ('source-flag', {'sourceflag': ['FLUX']}, {}),
        ('tied-sources', {'tie': '0,1'}, {}),
        ('unsegmented-source-flag', {'segsource': False, 'sourceflag': ['FLUX']}, {}),
        ('stat-field', {'statfield': '0'}, {}),
        ('stat-source', {'statsource': 'FLUX'}, {}),
        ('stat-final-source', {'statsource': 'SCIENCE'}, {}),
        ('temperature-offsets', {'offsetstable': str(root / 'offsets')}, {}),
        ('dispersed-window', {'disperse': True, 'spw': 0, 'reversespw': '0'}, {}),
        ('smooth-scale', {'smooth': '7s', 'scale': .8}, {}),
        ('antenna-distance', {'wvrflag': ['0', 'DA02'], 'refant': ['DA02', '2'],
                              'maxdistm': 80., 'minnumants': 2, 'mingoodfrac': .5}, {}),
        ('good-fraction', {'mingoodfrac': .5, 'refant': '4'}, {}),
        ('fraction-above-one', {'mingoodfrac': 1.1}, {}),
        ('continuum', {'segsource': False, 'cont': True, 'rseed': 79}, {}),
        ('tool-smooth-reverse', {}, {'reverse': True, 'smooth': 4, 'wvrspw': 1}),
        ('tool-reverse-window', {}, {'reversespw': ['0'], 'spw': 0, 'segsource': True}),
    )
    for name, task_args, tool_args in cases:
        default = dict(wvrspw=[1], usefieldtab=True)
        expected, actual = (root / f'{name}-{engine}.cal' for engine in ('casa', 'native'))
        if tool_args:
            log_casa, log_native = (root / f'{name}-{engine}.log' for engine in ('casa', 'native'))
            assert wvr().gcal(str(vis), str(expected), logfile=str(log_casa), **(default | tool_args)) == 0
            assert native_wvr().gcal(str(vis), str(actual), logfile=str(log_native), **(default | tool_args)) == 0
            for logfile in (log_casa, log_native):
                text = logfile.read_text()
                assert 'Disc (um)' in text and 'Expected performance' in text
            if name == 'tool-reverse-window':
                def estimate(path):
                    return float(re.search(r'Rough estimate.*coefficient error.*: (\S+)', path.read_text())[1])
                expected_error = estimate(log_casa)
                assert expected_error > 0.0
                np.testing.assert_allclose(estimate(log_native), expected_error, rtol=0.002)
        else:
            reference = casatasks.wvrgcal(str(vis), str(expected), **(default | task_args))
            candidate = native.wvrgcal(str(vis), str(actual), **(default | task_args))
            assert reference['rval'] == (-2 if name == 'continuum' else 0)
            compare_reports(reference, candidate)
            if name == 'smooth-scale':
                compare_tables(Path(f'{expected}_unsmoothed'), Path(f'{actual}_unsmoothed'))
        error = compare_tables(expected, actual)
        print(f'{name} gain normalized RMS {error:.6g}', flush=True)
    return len(cases)


def path_failure(task, vis, output):
    stdout = StringIO()
    with redirect_stdout(stdout):
        try:
            task(vis, output)
        except Exception as error:
            return type(error), str(error), stdout.getvalue()
    raise AssertionError(f'wvrgcal accepted {vis!r}, {output!r}')


def path_resolution(root, vis, native):
    runtime = root / 'runtime-paths'
    runtime.mkdir()
    inputs = runtime / 'wvr-input.ms'
    copy_ms(vis, inputs)
    existing = runtime / 'wvr-existing.cal'
    existing.mkdir()
    marker = existing / 'preserved'
    marker.write_text('existing publication')
    paths = ctsys.getpath()
    ctsys.setpath([str(runtime), *paths])
    try:
        with patch.dict(os.environ, {'CASA_RS_MEASURESPATH': str(runtime)}):
            for given, out in ((inputs.name, ''), (str(vis), existing.name), (np.str_(str(vis)), '')):
                assert path_failure(native.wvrgcal, given, out) == path_failure(casatasks.wvrgcal, given, out)
    finally:
        ctsys.setpath(paths)
    assert marker.read_text() == 'existing publication'


def publication_controls(root, vis, native, native_wvr):
    defaults = dict(usefieldtab=True, wvrspw=[1], wvrflag=['0', '1'],
                    refant='0', maxdistm=0.)
    denied = root / 'unusable-ref.cal'
    expected = casatasks.wvrgcal(str(vis), str(denied), **defaults)
    actual = native.wvrgcal(str(vis), str(denied), **defaults)
    compare_reports(expected, actual)
    assert actual['rval'] == -1 and not denied.exists()

    for invalid in (dict(nsol=2), dict(minnumants=4)):
        malformed = root / 'refused-argument.cal'
        for api in (casatasks, native):
            try:
                api.wvrgcal(str(vis), str(malformed), usefieldtab=True, **invalid)
            except Exception:
                assert not malformed.exists()
            else:
                raise AssertionError(f'invalid arguments were accepted: {invalid}')

    defaults = dict(usefieldtab=True, wvrspw=[1], segsource=False)
    reference = root / 'replace-casa.cal'
    candidate = root / 'replace-native.cal'
    for tool, output in ((wvr(), reference), (native_wvr(), candidate)):
        log = output.with_suffix('.log')
        assert tool.gcal(str(vis), str(output), logfile=str(log), **defaults) == 0
        assert tool.gcal(str(vis), str(output), logfile=str(log), scale=.9, **defaults) == 0
    compare_tables(reference, candidate)

    protected = vis / 'table.dat'
    before = protected.read_bytes()
    assert native_wvr().gcal(str(vis), str(denied), logfile=str(protected), **defaults) == -1
    assert protected.read_bytes() == before and not denied.exists()
    before = (candidate / 'table.dat').read_bytes()
    expected = path_failure(casatasks.wvrgcal, str(vis), str(candidate))
    assert expected[:2] == (Exception, '')
    assert path_failure(native.wvrgcal, str(vis), str(candidate)) == expected
    assert (candidate / 'table.dat').read_bytes() == before
    path_resolution(root, vis, native)
    from casacore.tables import table
    offsets = root / 'offsets'
    offset_view = root / 'offset-view'
    with table(str(offsets), ack=False) as backing, backing.selectrows(range(backing.nrows())) as view:
        view.copy(str(offset_view), deep=False).close()
    for scientific_input, options in (
        (root / 'libair-ddefault.csv', dict(disperse=True)),
        (offsets / 'table.dat', dict(offsets=str(offset_view))),
    ):
        original = scientific_input.read_bytes()
        assert native_wvr().gcal(str(vis), str(candidate), logfile=str(scientific_input),
                                 **(defaults | options)) == -1
        assert scientific_input.read_bytes() == original
    compare_tables(reference, candidate)


def reference_retry(root, original, native_wvr):
    vis = root / 'retry.ms'
    copy_ms(original, vis)
    with opened(vis, True) as ms:
        desc, left, right = (ms.getcol(col) for col in ('DATA_DESC_ID', 'ANTENNA1', 'ANTENNA2'))
        for row in np.flatnonzero((desc == 1) & (left == 0) & (right == 0)):
            ms.putcell('DATA', int(row), np.array([[297, 282, 285, 271]], np.complex64))
    paths = [root / 'retry-casa.cal', root / 'retry-native.cal']
    tool_args = dict(usefieldtab=True, wvrspw=[1], refant='0')
    for tool, path in zip((wvr(), native_wvr()), paths):
        assert tool.gcal(str(vis), str(path), logfile=str(path.with_suffix('.log')), **tool_args) == 0
        for selection, code in (({'wvrspw': [0]}, -10), ({'spw': [99]}, -11), ({'reversespw': [99]}, 1)):
            refused = root / f'{path.stem}-{code}.cal'
            assert tool.gcal(str(vis), str(refused), **(tool_args | selection)) == code
            assert not refused.exists()
    error = compare_tables(*paths)
    assert 'Bayesian evidence was zero' in paths[0].with_suffix('.log').read_text()
    print(f'zero-evidence antenna replacement gain normalized RMS {error:.6g}', flush=True)


def metadata_controls(root, vis, native, native_wvr):
    mutated = root / 'metadata.ms'
    copy_ms(vis, mutated)
    with opened(mutated / 'DATA_DESCRIPTION', True) as dd:
        row = dd.nrows()
        dd.addrows(1)
        for column in dd.colnames():
            dd.putcell(column, row, dd.getcell(column, 1))
    with opened(mutated / 'SOURCE', True) as source:
        row = source.nrows() - 1
        assert source.getcell('SOURCE_ID', row) == 1
        source.putcell('NAME', row, 'FLUX')
    reference = root / 'metadata-casa.cal'
    candidate = root / 'metadata-native.cal'
    options = dict(usefieldtab=True, statsource='FLUX')
    expected = casatasks.wvrgcal(str(mutated), str(reference), **options)
    actual = native.wvrgcal(str(mutated), str(candidate), **options)
    assert expected['success']
    compare_reports(expected, actual)
    compare_tables(reference, candidate)

    logfile = str(root / 'sparse-spw.log')
    sparse = root / 'sparse-reverse-native.cal'
    ordinary = root / 'sparse-ordinary-native.cal'
    sparse_casa = root / 'sparse-reverse-casa.cal'
    options = dict(usefieldtab=True, spw=[1], logfile=logfile)
    assert native_wvr().gcal(str(mutated), str(ordinary), **options) == 0
    assert native_wvr().gcal(str(mutated), str(sparse), reverse=True, **options) == 0
    assert wvr().gcal(str(mutated), str(sparse_casa), reverse=True, **options) == 0
    with opened(ordinary) as direct, opened(sparse) as reverse, opened(sparse_casa) as upstream:
        np.testing.assert_allclose(reverse.getcol('CPARAM'), np.conj(direct.getcol('CPARAM')), atol=1e-6)
        np.testing.assert_allclose(upstream.getcol('CPARAM'), direct.getcol('CPARAM'), atol=1e-6)
        assert np.max(np.abs(direct.getcol('CPARAM').imag)) > 0.1

    # Output history is independent when the source lacks its HISTORY subtable.
    no_history = root / 'no-history.ms'
    copy_ms(mutated, no_history)
    with opened(no_history, True) as ms:
        ms.removekeyword('HISTORY')
    shutil.rmtree(no_history / 'HISTORY')
    independent_history = root / 'independent-history.cal'
    assert native_wvr().gcal(str(no_history), str(independent_history), **options) == 0
    with opened(independent_history / 'HISTORY') as history:
        assert history.nrows() == 1

    with opened(mutated / 'SPECTRAL_WINDOW', True) as spw:
        channels = spw.getcell('CHAN_FREQ', 0)
        original_channels = channels.copy()
        channels[len(channels) // 2] = np.nan
        spw.putcell('CHAN_FREQ', 0, channels)
    assert native_wvr().gcal(str(mutated), str(candidate), spw=[0], usefieldtab=True, logfile=logfile) == -1
    compare_tables(reference, candidate)
    with opened(mutated / 'SPECTRAL_WINDOW', True) as spw:
        spw.putcell('CHAN_FREQ', 0, original_channels)

    unavailable = root / 'all-antenna-flags.cal'
    with opened(mutated / 'ANTENNA', True) as ant:
        ant.putcol('FLAG_ROW', np.ones(ant.nrows(), bool))
    for tool in (wvr(), native_wvr()):
        assert tool.gcal(str(mutated), str(unavailable), **options) == -1
        assert not unavailable.exists()
    with opened(mutated / 'ANTENNA', True) as ant:
        ant.putcol('FLAG_ROW', np.zeros(ant.nrows(), bool))
    with opened(mutated, True) as ms:
        for row in np.flatnonzero(ms.getcol('DATA_DESC_ID') == 1):
            ms.putcell('FLAG', int(row), np.ones((1, 4), bool))
    for api in (casatasks, native):
        failed = api.wvrgcal(str(mutated), str(unavailable), usefieldtab=True)
        assert failed['rval'] == 1 and not failed['success'] and not unavailable.exists()
    print('WVR metadata, all-flagged inputs and sparse reversal controls passed.', flush=True)


def missing_pointing_direction(root, source, native):
    """An unreadable POINTING row invokes the FIELD fallback."""
    from casacore.tables import makearrcoldesc, maketabdesc, table

    fixture_path = root / 'missing-pointing-direction.ms'
    copy_ms(source, fixture_path)
    with table(str(fixture_path / 'POINTING'), readonly=False, ack=False) as pointing:
        row = pointing.nrows()
        last = max(pointing.getcol('TIME'))
        directions = pointing.getcol('DIRECTION')
        directions[:, 0, 1] = 0.0
        keywords = pointing.getcolkeywords('DIRECTION')
        # IncrementalStMan extends the previous array when a row is appended.
        pointing.renamecol('DIRECTION', 'SAVED_DIRECTION')
        pointing.addcols(maketabdesc(makearrcoldesc(
            'DIRECTION', 0., ndim=2, datamanagertype='StandardStMan', keywords=keywords,
        )))
        for index, direction in enumerate(directions):
            pointing.putcell('DIRECTION', index, direction)
        pointing.addrows(1)
        pointing.putcell('TIME', row, last + 1000.0)
        assert not pointing.iscelldefined('DIRECTION', row)

    explicit = root / 'missing-pointing-forced-field.cal'
    settings = {'wvrspw': [1]}
    # CASA can terminate while opening the undefined array. Its explicit
    # FIELD path supplies the geometry reference for the native fallback.
    field = casatasks.wvrgcal(str(fixture_path), str(explicit), usefieldtab=True, **settings)
    assert field['success']
    product = root / 'missing-pointing-native.cal'
    report = native.wvrgcal(str(fixture_path), str(product), usefieldtab=False, **settings)
    compare_reports(field, report)
    assert compare_tables(explicit, product) <= 1e-3
    print('unreadable unused pointing direction falls back to FIELD', flush=True)


def check_recorded_statistics(vis, report, casa_report, windows, radiometer_window,
                              antennas, stamps, temperatures, path, terms):
    assert report.keys() == casa_report.keys()
    for name, expected in casa_report.items():
        if name not in ('RMS_um', 'Disc_um'):
            np.testing.assert_array_equal(report[name], expected, err_msg=name)

    rows = np.flatnonzero(windows == radiometer_window)
    times, time_index = np.unique(stamps[rows], return_inverse=True)
    with opened(vis / 'POINTING') as pointing:
        first_at_or_after = np.maximum.accumulate(pointing.getcol('TIME'))
        indices = np.searchsorted(first_at_or_after, times).clip(0, pointing.nrows() - 1)
        elevation = np.array([pointing.getcell('DIRECTION', int(index))[1, 0]
                              for index in indices])
    zenith = path[rows] * np.sin(elevation[time_index])
    discrepancy = temperatures[rows, 1] / terms[rows, 1] - temperatures[rows, 3] / terms[rows, 3]
    for name, values in (('RMS_um', zenith), ('Disc_um', discrepancy)):
        expected = [
            float(f'{np.std(values[antennas[rows] == antenna]) * 1e6:.3g}')
            for antenna in range(len(casa_report[name]))
        ]
        np.testing.assert_allclose(report[name], expected, rtol=1e-3, atol=.001, err_msg=name)
    print('recorded default report agrees with corrected pointing and antenna statistics', flush=True)


def recorded_failed_retrievals(root, vis, native):
    """Keep each successful fit attached to the time segment it measured."""
    raw = root / 'upstream-segmented.cal'
    messages = root / 'upstream-segmented.log'
    start = time.monotonic()
    assert wvr().gcal(str(vis), str(raw), segsource=True, logfile=str(messages)) == 0
    casa_seconds = time.monotonic() - start
    actual = root / 'default-native.cal'
    start = time.monotonic()
    report = native.wvrgcal(str(vis), str(actual))
    native_seconds = time.monotonic() - start
    assert report['success'], report
    upstream_task = root / 'upstream-default-task.cal'
    casa_report = casatasks.wvrgcal(str(vis), str(upstream_task))
    assert casa_report['success'], casa_report

    retrievals, results = messages.read_text().split('Retrieved parameters', 1)
    attempts = retrievals.split('Using random seed')[1:]
    successful = [i for i, result in enumerate(attempts) if 'Bayesian evidence was zero' not in result]
    assert successful == [1, 3], successful
    numerical = results.split('dT4dL', 1)[1].strip().splitlines()[:len(successful)]
    coefficients = np.array([list(map(float, row.split()))[-4:] for row in numerical]) * 1000

    with opened(vis / 'FIELD') as fields:
        source_for_field = fields.getcol('SOURCE_ID')
    with opened(vis / 'DATA_DESCRIPTION') as descriptions:
        window_for_description = descriptions.getcol('SPECTRAL_WINDOW_ID')
    with opened(vis / 'SPECTRAL_WINDOW') as windows:
        channel_count = windows.getcol('NUM_CHAN')
    radiometer = np.flatnonzero(channel_count[window_for_description] == 4)
    assert radiometer.size == 1
    with opened(vis) as main:
        times = main.getcol('TIME')
        order = np.argsort(times, kind='stable')
        sources = source_for_field[main.getcol('FIELD_ID')[order]]
        ordered_time = times[order]
        changes = np.flatnonzero(sources[1:] != sources[:-1]) + 1
        boundaries = np.column_stack((ordered_time[np.r_[0, changes]],
                                      ordered_time[np.r_[changes - 1, len(times) - 1]]))
        assert len(boundaries) == len(attempts)
        selected = main.query(f'DATA_DESC_ID == {radiometer[0]} AND ANTENNA1 == ANTENNA2')
        try:
            temperatures = selected.getcol('DATA').reshape(4, -1).T.real.astype('float64')
            input_flags = selected.getcol('FLAG').reshape(4, -1).T.any(axis=1)
            temperature_row = {
                (stamp, antenna): row
                for row, (stamp, antenna) in enumerate(
                    zip(selected.getcol('TIME'), selected.getcol('ANTENNA1')))
            }
        finally:
            selected.close()

    with opened(raw / 'SPECTRAL_WINDOW') as windows:
        frequencies = windows.getcol('CHAN_FREQ').ravel()
    with opened(raw) as table:
        gain = table.getcol('CPARAM')
        flags = table.getcol('FLAG').ravel()
        stamps = table.getcol('TIME')
        antennas = table.getcol('ANTENNA1')
        windows = table.getcol('SPECTRAL_WINDOW_ID')
    use = np.flatnonzero(~flags)
    rows = np.array([temperature_row[stamps[i], antennas[i]] for i in use])
    assert not input_flags[rows].any()
    noise = np.array([.10, .08, .08, .09])

    def expected(segments):
        knot_times = boundaries[segments].ravel()
        knot_values = np.repeat(coefficients, 2, axis=0)
        terms = np.array([np.interp(stamps[use], knot_times, values)
                          for values in knot_values.T]).T
        weights = (terms / noise**2) / np.sum((terms / noise)**2, axis=1)[:, None]
        path = np.sum(temperatures[rows] * weights, axis=1)
        gains = np.ones(flags.size, dtype=gain.dtype)
        gains[use] = np.exp(-2j * np.pi * frequencies[windows[use]] * path / 3e8)
        return gains.reshape(gain.shape), path, terms

    # libair keeps the first N interval bounds when N fits survive, even
    # when those fits measured later segments. Reproduce that mistake in the
    # reference law, then use the successful segments for science validation.
    legacy, _, _ = expected(list(range(len(successful))))
    corrected, path, terms = expected(successful)
    check_recorded_statistics(
        vis, report, casa_report, windows[use], window_for_description[radiometer[0]],
        antennas[use], stamps[use], temperatures[rows], path, terms,
    )
    legacy_error = np.linalg.norm(gain - legacy) / np.linalg.norm(legacy)
    assignment_error = np.linalg.norm(gain - corrected) / np.linalg.norm(corrected)
    assert legacy_error < 1e-3 and assignment_error > .05, (legacy_error, assignment_error)
    reference = root / 'default-casa.cal'
    copy_ms(raw, reference)
    with opened(reference, True) as table:
        table.putcol('CPARAM', corrected)
    error = compare_tables(reference, actual)
    print(f'default recorded corrected gain normalized RMS {error:.6g}; upstream segment '
          f'assignment error {assignment_error:.6g}. CASA {casa_seconds:.3f}s, '
          f'casa-rs {native_seconds:.3f}s', flush=True)


def recorded_empty_single_retrieval(root, vis, native):
    """CASA dereferences an absent retrieval when the remaining sample is flagged."""
    arguments = [str(vis), str(root / 'empty-casa.cal')]
    program = (
        "import resource,sys;resource.setrlimit(resource.RLIMIT_CORE,(0,0));"
        "from casatasks import wvrgcal;"
        "wvrgcal(sys.argv[1],sys.argv[2],segsource=False,toffset=-1.)"
    )
    child = subprocess.run([sys.executable, '-c', program, *arguments], cwd=root,
                           capture_output=True, text=True, timeout=90)
    assert child.returncode == -signal.SIGSEGV, (child.returncode, child.stderr)
    product = root / 'empty-native.cal'
    report = native.wvrgcal(str(vis), str(product), segsource=False, toffset=-1.)
    assert report['rval'] == 1 and not report['success'] and not product.exists(), report
    print('recorded empty single retrieval returns failure without publishing a table', flush=True)


def recorded_cases(root, vis, native):
    """Compare retrieval and application on a recorded ALMA observation."""
    pinned = vis.name == 'uid___A002_X8ca70c_X5_shortened.ms'
    if pinned:
        recorded_failed_retrievals(root, vis, native)
        cases = (
            ('time-offset', dict(segsource=False, toffset=-1., nsol=2, refant='DA41')),
            ('fixed-fits', dict(segsource=False, nsol=5, rseed=27, refant='DA41')),
        )
    else:
        cases = (
            ('default', {}),
            ('time-offset', dict(segsource=False, toffset=-1.)),
            ('fixed-fits', dict(segsource=False, nsol=5, rseed=27)),
        )
    for name, args in cases:
        results = []
        for name_api, api in (('casa', casatasks), ('native', native)):
            path = root / f'{name}-{name_api}.cal'
            start = time.monotonic()
            report = api.wvrgcal(str(vis), str(path), **args)
            duration = time.monotonic() - start
            assert report['success'], (name, name_api, report)
            results.append((path, report, duration))
        (path_casa, report_casa, casa_seconds), (path_native, report_native, native_seconds) = results
        compare_reports(report_casa, report_native)
        error = compare_tables(path_casa, path_native)
        print(f'{name} recorded gain normalized RMS {error:.6g}, CASA {casa_seconds:.3f}s, '
              f'casa-rs {native_seconds:.3f}s', flush=True)
    if pinned:
        recorded_empty_single_retrieval(root, vis, native)


def corrected_recorded_sample(root, vis, native):
    with opened(vis / 'SPECTRAL_WINDOW') as table:
        channels = table.getcol('NUM_CHAN')
    with opened(vis / 'DATA_DESCRIPTION') as table:
        windows = table.getcol('SPECTRAL_WINDOW_ID')
    with opened(vis) as table:
        present = np.unique(table.getcol('DATA_DESC_ID'))
    science = next(int(windows[dd]) for dd in present if channels[windows[dd]] != 4)
    descriptions = np.flatnonzero(windows == science).tolist()
    samples = []
    for engine, api in (('casa', casatasks), ('native', native)):
        product = root / f'corrected-{engine}.ms'
        copy_ms(vis, product)
        api.applycal(str(product), spw=str(science), gaintable=[str(root / f'default-{engine}.cal')],
                     flagbackup=False, calwt=False)
        with opened(product) as table:
            selected = table.query(f'DATA_DESC_ID IN {descriptions}')
            try:
                rows = np.linspace(0, selected.nrows() - 1, min(64, selected.nrows()), dtype=int)
                data = [selected.getcell('CORRECTED_DATA', int(row)) for row in rows]
                flags = [selected.getcell('FLAG', int(row)) for row in rows]
                samples.append((np.stack(data), np.stack(flags)))
            finally:
                selected.close()
    (reference, flags), (actual, actual_flags) = samples
    np.testing.assert_array_equal(flags, actual_flags)
    usable = ~flags
    denominator = np.linalg.norm(reference[usable])
    assert denominator > 0
    error = np.linalg.norm(actual[usable] - reference[usable]) / denominator
    assert error <= 1e-3, error
    print(f'recorded science SPW {science} corrected visibility normalized RMS {error:.6g}', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', required=True, type=Path)
    parser.add_argument('--recorded-ms', type=Path, help='recorded ALMA MeasurementSet with WVR data')
    args = parser.parse_args()
    from casars import tasks
    from casars.tools import wvr as native_wvr

    assert casatasks.version_string() == '6.7.6.14'
    for oracle, candidate in ((casatasks.wvrgcal, tasks.wvrgcal), (wvr().gcal, native_wvr().gcal)):
        expected = inspect.signature(oracle).parameters
        actual = inspect.signature(candidate).parameters
        assert list(actual)[:len(expected)] == list(expected)
        for key, parameter in expected.items():
            assert (actual[key].kind, actual[key].default) == (parameter.kind, parameter.default), key
    root = args.work.resolve()
    root.mkdir()
    if args.recorded_ms is not None:
        recorded_ms = args.recorded_ms.resolve(strict=True)
        recorded_cases(root, recorded_ms, tasks)
        corrected_recorded_sample(root, recorded_ms, tasks)
        return
    os.chdir(root)
    vis = root / 'radiometer.ms'
    fixture(vis)
    extra_inputs(vis, root / 'offsets')
    (root / 'libair-ddefault.csv').write_text(
        '50000000000 , 0.02\n100000000000 , 0.07\n150000000000 , 0.05\n200000000000 , 0.15\n')
    os.environ['WVRGCAL_DISPDIR'] = str(root)
    count = run_cases(root, vis, tasks, native_wvr)
    publication_controls(root, vis, tasks, native_wvr)
    reference_retry(root, vis, native_wvr)
    metadata_controls(root, vis, tasks, native_wvr)
    missing_pointing_direction(root, vis, tasks)
    print(f'{count} nondefault WVR scenarios and publication controls passed.')


if __name__ == '__main__':
    main()
