#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Fit point, Gaussian and disk visibility models with CASA and casa-rs."""

import argparse
from contextlib import redirect_stderr
import io
import json
import os
from pathlib import Path
import re
import sys
from tempfile import TemporaryFile

import casatasks as casa
from casatools import componentlist, table
import numpy as np

from bandpass_oracle import source_data
from calibration_ft_oracle import coordinates, initial_time_range
from calibration_scratch_oracle import prepare, state


def fixture(root, seed, shape, corrected=True, uneven=False):
    path = root/f'{shape}-source.ms'
    prepare(seed, path, corrected=corrected)
    unit = root/f'{shape}-unit.cl'
    ra, dec = coordinates(seed)
    cl = componentlist()
    geometry = {} if shape == 'Point' else dict(majoraxis='1.4arcsec', minoraxis='0.98arcsec',
                                                positionangle='28deg')
    assert cl.addcomponent(dir=f'J2000 {ra}rad {dec}rad', flux=1., fluxunit='Jy',
                           shape=shape, **geometry)
    assert cl.rename(str(unit))
    cl.done()
    casa.ft(str(path), field='0', complist=str(unit), usescratch=True)
    tb = table()
    assert tb.open(str(path), nomodify=False)
    try:
        data = tb.getcol('MODEL_DATA')
        u, v, _ = tb.getcol('UVW')
        frequencies = 100e9 + np.arange(data.shape[1])*1e6
        uv_phase = (2*np.pi * np.deg2rad(1./3600) / 299792458.) * (0.13*u - 0.12*v)
        phase = np.exp(1j*frequencies[:, None]*uv_phase[None, :])
        data *= 2.7*phase
        data[1:3, ...] = 65
        if uneven:
            data[[0, -1], :, :] *= (1 + np.arange(data.shape[1])**2)[None, :, None]
        column = 'CORRECTED_DATA' if corrected else 'DATA'
        tb.putcol(column, data)
        weights = tb.getcol('WEIGHT')
        weights *= np.arange(1, 5, dtype=np.float32)[:, None]
        weights *= (1+tb.getcol('ANTENNA1')[None, :])
        tb.putcol('WEIGHT', weights)
    finally:
        tb.close()
    return path, unit


def copy_for_case(source, dataset):
    from applycal_oracle import copy_ms
    copy_ms(source, dataset)


def component(path):
    cl = componentlist()
    assert cl.open(str(path))
    try:
        return cl.getcomponent(0)
    finally:
        cl.done()


def numeric_record(record):
    shape = record['shape']
    directions = shape['direction']
    fields = [*record['flux']['value'], directions['m0']['value'], directions['m1']['value']]
    if shape['type'] != 'Point':
        for name in ('majoraxis', 'minoraxis', 'positionangle'):
            fields.append(shape[name]['value'])
    return np.asarray(fields)


def flagged_fixture(root, source, *, all_rows=False):
    path = root/('all-flagged.ms' if all_rows else 'flagged.ms')
    copy_for_case(source, path)
    tb = table()
    assert tb.open(str(path), nomodify=False)
    try:
        rows = tb.getcol('FLAG_ROW')
        step = 1 if all_rows else 4
        rows[::step] = True
        tb.putcol('FLAG_ROW', rows)
        if not all_rows:
            flags = tb.getcol('FLAG')
            flags[1, 2, :] = True
            tb.putcol('FLAG', flags)
            outliers = tb.getcol('CORRECTED_DATA')
            outliers[:, :, rows] = 64
            outliers[:, 2, :] = 64
            tb.putcol('CORRECTED_DATA', outliers)
    finally:
        tb.close()
    return path


def phase_and_weight_fixtures(root, source):
    phase = root/'time-polynomial.ms'
    light = root/'small-weights.ms'
    copy_for_case(source, phase)
    copy_for_case(source, light)
    tb = table()
    assert tb.open(str(phase))
    epoch = tb.getcell('TIME', 0)
    tb.close()
    assert tb.open(str(phase/'FIELD'), nomodify=False)
    try:
        centre = tb.getcell('PHASE_DIR', 0)[:, 0]
        tb.putcell('NUM_POLY', 0, 1)
        tb.putcell('TIME', 0, epoch-20.)
        tb.putcell('PHASE_DIR', 0, np.column_stack((centre, [1e-4, 1e-5])))
    finally:
        tb.close()
    assert tb.open(str(light), nomodify=False)
    try:
        tb.putcol('WEIGHT', np.full_like(tb.getcol('WEIGHT'), 1e-23))
    finally:
        tb.close()
    return phase, light


def fit_report(api, dataset, options):
    if api is casa:
        with TemporaryFile(mode='w+', encoding='utf-8') as captured:
            sys.stdout.flush()
            original = os.dup(1)
            try:
                os.dup2(captured.fileno(), 1)
                result = api.uvmodelfit(str(dataset), **options)
                sys.stdout.flush()
            finally:
                os.dup2(original, 1)
                os.close(original)
            captured.seek(0)
            text = captured.read()
    else:
        captured = io.StringIO()
        with redirect_stderr(captured):
            result = api.uvmodelfit(str(dataset), **options)
        text = captured.getvalue()
    matches = re.findall(r'^([Ixy]) = ([-+.\deE]+) \+/- ([-+.\deE]+)', text, re.MULTILINE)
    assert [match[0] for match in matches] == ['I', 'x', 'y'], text
    return result, np.array([[float(value), float(error)] for _, value, error in matches])


def compare(root, source, native, name, options, *, output=True, error=None, partition=False):
    outcomes = []
    reports = []
    for label, api in [('casa', casa), ('native', native)]:
        dataset = root/f'{name}-{label}.ms'
        table_path = root/f'{name}-{label}.cl'
        if partition:
            casa.partition(vis=str(source), outputvis=str(dataset), separationaxis='baseline',
                           numsubms=2, datacolumn='all', disableparallel=True)
        else:
            copy_for_case(source, dataset)
        before = state(dataset)
        try:
            if output:
                result = api.uvmodelfit(str(dataset), **options, outfile=str(table_path))
            else:
                result, report = fit_report(api, dataset, options)
                reports.append(report)
        except RuntimeError as actual:
            assert error and error in str(actual)
        else:
            assert result is None and error is None
        after = state(dataset)
        np.testing.assert_equal(after[1], before[1])
        for column, values in before[0].items():
            np.testing.assert_array_equal(after[0][column], values)
        outcomes.append((after[0], component(table_path) if output and not error else None))
    np.testing.assert_equal(outcomes[1][0], outcomes[0][0])
    if reports:
        np.testing.assert_allclose(reports[1], reports[0], atol=5e-7, rtol=5e-6)
        return {'reported_values_and_errors': True}
    if error:
        return {'agrees': True}
    expected, actual = [r[1] for r in outcomes]
    assert expected['shape']['type'] == actual['shape']['type']
    np.testing.assert_equal(expected['spectrum'], actual['spectrum'])
    reference, fitted = numeric_record(expected), numeric_record(actual)
    np.testing.assert_allclose(fitted, reference, atol=1e-6, rtol=1e-3, err_msg=name)
    np.testing.assert_allclose(fitted[4:6], reference[4:6], atol=1e-10, rtol=0, err_msg=name)
    return {'max_parameter_difference': float(np.max(np.abs(fitted-reference)))}


def validation_cases(native, source):
    cases = [
        ('default-vis', {}),
        ('missing-vis', {'vis': str(source.with_name('missing.ms'))}),
        ('invalid-shape', {'vis': str(source), 'comptype': 'p'}),
    ]
    for name, options in cases:
        failures = []
        for tool in (casa, native):
            try:
                tool.uvmodelfit(**options)
            except AssertionError as error:
                failures.append(str(error))
            else:
                raise AssertionError(f'{name} did not fail validation')
        assert failures[0] == failures[1], failures
        yield name, {'agrees': True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--native-bin', type=Path, required=True)
    parser.add_argument('--case')
    args = parser.parse_args()
    assert casa.version_string() == '6.7.6.14'
    os.environ['CASARS_CALIBRATE_BIN'] = str(args.native_bin.resolve())
    os.environ['CASARS_REQUIRE_TASK_BINARIES'] = '1'
    from casars import tasks
    root = args.work.resolve()
    root.mkdir(parents=True)
    seed = root/'simulated.ms'
    source_data(seed, stokes='XX XY YX YY')
    point, _ = fixture(root, seed, 'Point')
    gaussian, _ = fixture(root, seed, 'Gaussian')
    disk, _ = fixture(root, seed, 'Disk')
    no_scratch_root = root/'no-corrected'
    no_scratch_root.mkdir()
    bare, _ = fixture(no_scratch_root, seed, 'Point', corrected=False)
    nonuniform_root = root/'nonuniform'
    nonuniform_root.mkdir()
    nonuniform, _ = fixture(nonuniform_root, seed, 'Point', uneven=True)
    flagged = flagged_fixture(root, point)
    all_flagged = flagged_fixture(root, point, all_rows=True)
    polynomial, small_weights = phase_and_weight_fixtures(root, point)
    rows = [
        ('point', point, dict(field='0', sourcepar=[1.5, .01, -.01], niter=10)),
        ('flux-only', point, dict(field='0', sourcepar=[1.2, .13, -.12],
                                  varypar=[True, False, False], niter=5)),
        ('flagged-cells', flagged, dict(field='0', sourcepar=[1.2, .13, -.12],
                                      varypar=[True, False, False], niter=5)),
        ('all-flagged', all_flagged, dict(field='0', niter=0)),
        ('negative-iterations', all_flagged, dict(field='0', sourcepar=[1.2, .13, -.12], niter=-1)),
        ('initial', point, dict(field='0', sourcepar=[1.2, .13, -.12], niter=0)),
        ('boolean-iterations', point, dict(field='0', sourcepar=[1.2, .13, -.12], niter=False)),
        ('boolean-source-parameters', point, dict(field='0', sourcepar=[True, False, False], niter=0)),
        ('phase-polynomial', polynomial, dict(field='0', niter=0)),
        ('small-weights', small_weights, dict(field='0', sourcepar=[1., .13, -.12],
                                             varypar=[True, False, False])),
        ('gaussian', gaussian, dict(field='0', comptype='G',
              sourcepar=[2.1, .1, -.1, 1.27, .72, 30.], niter=12)),
        ('disk', disk, dict(field='0', comptype='D',
              sourcepar=[2.3, .11, -.11, 1.31, .73, 30.], niter=14)),
        ('new-corrected', bare, dict(field='0', sourcepar=[1.5, .01, -.01], niter=10)),
        ('multi-ms-new-corrected', bare, dict(field='0', sourcepar=[1.5, .01, -.01], niter=10)),
        ('channel-range', nonuniform, dict(field='0', spw='0:1~7^2', sourcepar=[1.5, .13, -.12],
                                            varypar=[True, False, False])),
        ('disjoint-channels', nonuniform, dict(field='0', spw='0:0;3;7', sourcepar=[1.5, .13, -.12],
                                               varypar=[True, False, False])),
        ('distance-vector', point, dict(field='0', uvrange=['>1m'], niter=0)),
        ('selection', point, dict(field='0', scan='1', antenna='0~4',
                                  timerange=initial_time_range(seed), uvrange='>1m',
                                  msselect='ANTENNA1!=ANTENNA2', sourcepar=[1.2, .13, -.12])),
        ('selection-disabled', point, dict(field='0', selectdata=False, scan='999', antenna='missing',
                                           msselect='bad column', sourcepar=[1.2, .13, -.12])),
        ('no-output', point, dict(field='0')),
        ('more-than-one-field', point, dict(niter=0)),
        ('wrong-length', point, dict(field='0', comptype='G')),
    ]
    results = {} if args.case else dict(validation_cases(tasks, point))
    for name, source, options in rows:
        if args.case and args.case not in name:
            continue
        failed_cases = ('all-flagged', 'more-than-one-field', 'wrong-length')
        error = 'Error in Calibrater::modelfit.' if name in failed_cases else None
        results[name] = compare(root, source, tasks, name, options, output=name != 'no-output',
                                error=error, partition=name.startswith('multi-ms-'))
        print(name, results[name], flush=True)
    (root/'results.json').write_text(json.dumps(results, indent=2)+'\n')


if __name__ == '__main__':
    main()
