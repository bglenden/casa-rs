#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare BPOLY coefficients and calibrated visibilities with pinned CASA."""

import argparse
import inspect
import json
import os
from pathlib import Path
import time

import casatasks as casa
from casatools import table
import numpy as np

from bandpass_oracle import source_data
from applycal_oracle import copy_ms
from calibration_lifecycle_oracle import contents
from gaincal_delay_oracle import duplicate_rows
from gaincal_oracle import relative_rms


def compare(root, source, native, field='0', apply_options=None, interchange=False, **options):
    tables, records, corrected = {}, {}, {}
    elapsed = {}
    apply_options = (apply_options or {}).copy()
    priors = apply_options.pop('gaintable', [])
    for name, engine in [('casa', casa), ('native', native)]:
        ms, output = root/f'{name}.ms', root/f'{name}.bpoly'
        copy_ms(source, ms)
        start = time.perf_counter()
        records[name] = engine.bandpass(str(ms), str(output), field=field, bandtype='BPOLY',
                                        **options)
        elapsed[name] = time.perf_counter() - start
        np.testing.assert_array_equal(records[name].pop('solve_tables'), [str(output)])
        tables[name] = contents(output)
        engine.applycal(str(ms), field=field, gaintable=[*priors, str(output)], flagbackup=False,
                        **apply_options)
        corrected[name] = contents(ms)
    expected, actual = tables['casa'], tables['native']
    errors = {
        column: relative_rms(actual[column], expected[column])
        for column in ('POLY_COEFF_AMP', 'POLY_COEFF_PHASE')
    }
    for column in ('CORRECTED_DATA', 'WEIGHT'):
        errors[column] = relative_rms(corrected['native'][column], corrected['casa'][column])
    print(root.name, errors, 'solve seconds', elapsed, flush=True)
    for column in ('INTERVAL', 'FIELD_ID', 'ANTENNA1', 'CAL_DESC_ID', 'REF_ANT',
                   'VALID_DOMAIN', 'N_POLY_AMP', 'N_POLY_PHASE', 'OBSERVATION_ID',
                   'SCAN_NUMBER', 'FLAG', 'SNR', 'SOLUTION_OK', 'PHASE_UNITS'):
        np.testing.assert_array_equal(actual[column], expected[column], err_msg=column)
    np.testing.assert_allclose(actual['TIME'], expected['TIME'], atol=1e-4, rtol=0.)
    np.testing.assert_equal(records['native'], records['casa'])
    for column in ('FLAG', 'FLAG_ROW'):
        np.testing.assert_array_equal(corrected['native'][column], corrected['casa'][column])
    if interchange:
        for reader, engine in [('casa', casa), ('native', native)]:
            writer = 'native' if reader == 'casa' else 'casa'
            ms = root/f'{reader}-interop.ms'
            copy_ms(source, ms)
            engine.applycal(str(ms), field=field, flagbackup=False,
                            gaintable=[*priors, str(root/f'{writer}.bpoly')], **apply_options)
            state = contents(ms)
            errors[f'{reader}_interop'] = relative_rms(
                state['CORRECTED_DATA'], corrected[reader]['CORRECTED_DATA'])
            np.testing.assert_array_equal(state['FLAG'], corrected[reader]['FLAG'])
    return errors


def masked_data(source, target):
    copy_ms(source, target)
    tool = table()
    tool.open(str(target), nomodify=False)
    try:
        data = tool.getcol('DATA')
        data[:, [0, 5, 6, 11]] *= 2.5 - 1j
        tool.putcol('DATA', data)
    finally:
        tool.close()


def wrapped_data(source, target):
    copy_ms(source, target)
    tool = table()
    tool.open(str(target), nomodify=False)
    try:
        data = tool.getcol('DATA')
        a, b = (tool.getcol(column) for column in ('ANTENNA1', 'ANTENNA2'))
        channel = np.arange(data.shape[1])[:, None]
        data *= np.exp(1j*(.8 + .18*channel)*(a-b))
        tool.putcol('DATA', data)
    finally:
        tool.close()


def multiwindow_data(source, target):
    copy_ms(source, target)
    tool = table()
    duplicate_rows(tool, target/'SPECTRAL_WINDOW')
    duplicate_rows(tool, target/'DATA_DESCRIPTION')
    count = duplicate_rows(tool, target)
    tool.open(str(target/'DATA_DESCRIPTION'), nomodify=False)
    try:
        tool.putcell('SPECTRAL_WINDOW_ID', 1, 1)
    finally:
        tool.close()
    tool.open(str(target/'SPECTRAL_WINDOW'), nomodify=False)
    try:
        first = tool.getcell('CHAN_FREQ', 0)
        tool.putcell('CHAN_FREQ', 1, first[0] + 30.e6 + 2.*(first-first[0]))
        for column in ('CHAN_WIDTH', 'EFFECTIVE_BW', 'RESOLUTION'):
            tool.putcell(column, 1, 2.*tool.getcell(column, 0))
    finally:
        tool.close()
    tool.open(str(target), nomodify=False)
    try:
        tool.putcol('DATA_DESC_ID', np.ones(count, dtype=np.int32), count, count)
        data = tool.getcol('DATA', count, count)
        tool.putcol('DATA', data*1.3, count, count)
    finally:
        tool.close()


def flag_data(source, target):
    copy_ms(source, target)
    tool = table()
    tool.open(str(target), nomodify=False)
    try:
        flags = tool.getcol('FLAG')
        time = tool.getcol('TIME')
        a = tool.getcol('ANTENNA1')
        flags[0, 3:7, (time < np.unique(time)[3]) & (a < 2)] = True
        flags[1, -1, (time == time.min()) & (a == 0)] = True
        tool.putcol('FLAG', flags)
    finally:
        tool.close()


def model_data(source, target):
    copy_ms(source, target)
    casa.clearcal(str(target), addmodel=True)
    tool = table()
    tool.open(str(target), nomodify=False)
    try:
        model = tool.getcol('MODEL_DATA')
        channel = np.arange(model.shape[1])[:, None]
        model[0] = (1.4 + .03*channel)*np.exp(.01j*channel)
        model[1] = 1.7 + .02*channel
        tool.putcol('MODEL_DATA', model)
    finally:
        tool.close()


def frequency_weight_data(source, target, *, alternate_weight=1., flagged_hands=0):
    copy_ms(source, target)
    tool = table()
    tool.open(str(target), nomodify=False)
    try:
        flags = tool.getcol('FLAG')
        flags[:flagged_hands, ::2] = True
        tool.putcol('FLAG', flags)
        if 'WEIGHT_SPECTRUM' not in tool.colnames():
            tool.addcols({'WEIGHT_SPECTRUM': {'valueType': 'float', 'ndim': 2}})
        weights = np.ones(flags.shape, dtype=np.float32)
        weights[:, ::2] = alternate_weight
        tool.putcol('WEIGHT_SPECTRUM', weights)
    finally:
        tool.close()


def singular_interval(root, source, native):
    flagged = root/'singular-interval.ms'
    copy_ms(source, flagged)
    tool = table()
    tool.open(str(flagged), nomodify=False)
    try:
        flags = tool.getcol('FLAG')
        time = tool.getcol('TIME')
        first = time == time.min()
        flags[..., first] = True
        flags[:, 1, first] = False
        tool.putcol('FLAG', flags)
    finally:
        tool.close()
    products = []
    for name, engine in [('casa', casa), ('native', native)]:
        output = root/f'singular-interval-{name}.bpoly'
        engine.bandpass(str(flagged), str(output), field='0', bandtype='BPOLY', solint='int')
        products.append(contents(output))
    expected, actual = products
    finite = np.isfinite(expected['POLY_COEFF_AMP']).all(axis=(0, 1, 2, 3))
    assert finite.sum() == 25 and not np.any(finite[:5])
    np.testing.assert_allclose(actual['TIME'], expected['TIME'][finite], atol=1e-4, rtol=0.)
    return {key: relative_rms(actual[key], expected[key][..., finite])
            for key in ('POLY_COEFF_AMP', 'POLY_COEFF_PHASE')}


def singleton_windows(root, source, native):
    output = root/'singleton-windows.bpoly'
    native.bandpass(str(source), str(output), field='0', bandtype='BPOLY',
                    spw='0:5,1:5', combine='spw,scan', maskedge=0, degamp=0, degphase=0)
    state = contents(source)
    antennas = np.unique(state['ANTENNA1'].tolist() + state['ANTENNA2'].tolist())
    design, samples = [], []
    for description in (0, 1):
        for left in antennas:
            for right in antennas[antennas > left]:
                rows = ((state['FIELD_ID'] == 0) & (state['DATA_DESC_ID'] == description)
                        & (state['ANTENNA1'] == left) & (state['ANTENNA2'] == right))
                design.append(.5 * ((antennas == left) | (antennas == right)))
                samples.append(np.log(np.abs(state['DATA'][:, 5, rows].mean(axis=1))))
    expected = np.linalg.lstsq(design, samples, rcond=None)[0]
    actual = contents(output)
    frequencies = contents(source/'SPECTRAL_WINDOW')['CHAN_FREQ'][5, :]
    assert actual['VALID_DOMAIN'][0, 0] <= frequencies.min()
    assert actual['VALID_DOMAIN'][1, 0] >= frequencies.max()
    return {'POLY_COEFF_AMP': relative_rms(actual['POLY_COEFF_AMP'].squeeze().T, expected)}


def append_cases(root, source, native):
    results = {}
    engines = [('casa', casa), ('native', native)]
    for writer, engine in engines:
        original = root/f'initial-{writer}.bpoly'
        engine.bandpass(str(source), str(original), field='0', spw='0', bandtype='BPOLY')
        original_rows = contents(original)
        for appender, consumer in engines:
            label = f'append-{appender}-to-{writer}'
            output = root/f'{label}.bpoly'
            copy_ms(original, output)
            record = consumer.bandpass(str(source), str(output), field='0', spw='1',
                                       bandtype='BPOLY', append=True)
            np.testing.assert_array_equal(record['solve_tables'], [str(output)])
            descriptors = contents(output/'CAL_DESC')
            np.testing.assert_array_equal(descriptors['SPECTRAL_WINDOW_ID'], [[0, 1]])
            rows = contents(output)
            np.testing.assert_array_equal(np.bincount(rows['CAL_DESC_ID']), [5, 5])
            first_window = rows['CAL_DESC_ID'] == 0
            for column in ('TIME', 'POLY_COEFF_AMP', 'POLY_COEFF_PHASE'):
                np.testing.assert_array_equal(rows[column][..., first_window],
                                              original_rows[column], err_msg=column)
            assert np.all(np.isfinite(rows['POLY_COEFF_AMP']))
            assert np.all(np.isfinite(rows['POLY_COEFF_PHASE']))
            consumer.bandpass(str(source), str(output), field='0', spw='1',
                              bandtype='BPOLY', append=True)
            np.testing.assert_array_equal(contents(output/'CAL_DESC')['SPECTRAL_WINDOW_ID'],
                                          [[0, 1]])
            np.testing.assert_array_equal(np.bincount(contents(output)['CAL_DESC_ID']), [5, 10])
            consumer.bandpass(str(source), str(output), field='0', spw='1', bandtype='BPOLY')
            np.testing.assert_array_equal(contents(output/'CAL_DESC')['SPECTRAL_WINDOW_ID'], [[1]])
            assert len(contents(output)['TIME']) == 5
            results[label] = {}
    return results


def no_fit_cases(root, source, multi, native):
    results = {}
    for label, dataset, options in [
        ('invalid-selection', source, dict(field='MISSING', refant='MISSING')),
        ('excess-degree', source, dict(degamp=12)),
        ('masked-all-channels', source, dict(maskcenter=12)),
        ('unequal-window-channel-counts', multi, dict(spw='0:0~10,1', combine='spw,scan')),
    ]:
        records = []
        parameters = dict(field='0', bandtype='BPOLY') | options
        for name, engine in [('casa', casa), ('native', native)]:
            output = root/f'{label}-{name}.bpoly'
            record = engine.bandpass(str(dataset), str(output), **parameters)
            if label == 'masked-all-channels' and name == 'casa':
                np.testing.assert_array_equal(record.pop('solve_tables'), [str(output)])
                assert not np.isfinite(contents(output)['POLY_COEFF_AMP']).all()
            else:
                np.testing.assert_array_equal(record.pop('solve_tables'), [])
                assert not output.exists()
            records.append(record)
        np.testing.assert_equal(records[1], records[0])
        results[label] = {}
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--native-bin', type=Path, required=True)
    parser.add_argument('--ngc5921', type=Path)
    parser.add_argument('--only', nargs='*')
    args = parser.parse_args()
    assert casa.version_string() == '6.7.6.14'
    os.environ['CASARS_CALIBRATE_BIN'] = str(args.native_bin.resolve())
    os.environ['CASARS_REQUIRE_TASK_BINARIES'] = '1'
    from casars import tasks as native
    expected = inspect.signature(casa.bandpass).parameters
    actual = {name: param for name, param in inspect.signature(native.bandpass).parameters.items()
              if param.kind != inspect.Parameter.KEYWORD_ONLY}
    assert list(actual) == list(expected)
    for name in expected:
        assert actual[name].kind == expected[name].kind
        np.testing.assert_equal(actual[name].default, expected[name].default)

    root = args.work.resolve()
    root.mkdir(parents=True)
    source = root/'source.ms'
    source_data(source)
    masked = root/'masked.ms'
    masked_data(source, masked)
    multi = root/'multiple-windows.ms'
    multiwindow_data(source, multi)
    flagged = root/'flagged.ms'
    flag_data(source, flagged)
    scratch = root/'scratch-model.ms'
    model_data(source, scratch)
    wrapped = root/'wrapped.ms'
    wrapped_data(source, wrapped)
    virtual = root/'virtual-model.ms'
    copy_ms(scratch, virtual)
    casa.setjy(str(virtual), field='0', standard='manual', fluxdensity=[3., 0., 0., 0.],
               usescratch=False)
    observations = root/'observations.ms'
    copy_ms(source, observations)
    tool = table()
    observation = duplicate_rows(tool, observations/'OBSERVATION')
    count = duplicate_rows(tool, observations)
    tool.open(str(observations), nomodify=False)
    try:
        tool.putcol('OBSERVATION_ID', np.full(count, observation, dtype=np.int32), count, count)
        for column in ('TIME', 'TIME_CENTROID'):
            tool.putcol(column, tool.getcol(column, count, count) + 5., count, count)
    finally:
        tool.close()
    gain = str(root/'prior.G')
    casa.gaincal(str(source), gain, field='0', refant='A0', calmode='p', minsnr=0.)
    callibrary = root/'gain.callib'
    callibrary.write_text(f"caltable='{gain}' tinterp='nearest' calwt=True spwmap=[0]\n")
    linear = root/'full-linear.ms'
    source_data(linear, stokes='XX XY YX YY')
    yy = root/'scalar-yy.ms'
    source_data(yy, stokes='YY')
    cases = [
        ('default', source, dict(interchange=True)),
        ('solution-normalized', source, dict(solnorm=True)),
        ('visibility-normalized', source, dict(visnorm=True)),
        ('both-normalized', source, dict(visnorm=True, solnorm=True)),
        ('center-and-edges', masked, dict(maskcenter=2, maskedge=10, degamp=2, degphase=4)),
        ('time-int', source, dict(solint='int')),
        ('time-subintegration', source, dict(solint='1ms')),
        ('time-seconds', source, dict(solint='25s')),
        ('time-numeric', source, dict(solint=25)),
        ('frequency-suffix', source, dict(solint='inf,2ch')),
        ('frequency-bandwidth', source, dict(solint='inf,2MHz')),
        ('selected-channels', source, dict(spw='0:1~10')),
        ('disjoint-masked-channels', source, dict(spw='0:0~2;8~11', maskcenter=2,
                                                maskedge=0, degamp=2, degphase=2)),
        ('channel-stride', source, dict(spw='0:0~11^2')),
        ('single-channel', source, dict(spw='0:5', maskedge=0, degamp=0, degphase=0)),
        ('named-reference', source, dict(refant='A2')),
        ('ranked-reference', source, dict(refant='A2,A1')),
        ('point-source', source, dict(smodel=[2., .3, .2, .1])),
        ('scratch-model', scratch, {}),
        ('virtual-model', virtual, {}),
        ('uneven-flags', flagged, {}),
        ('corrdepflags', flagged, dict(corrdepflags=True)),
        ('combined-frequency', multi, dict(combine='spw,scan', spw='1,0',
                                          apply_options=dict(spwmap=[0, 0]), interchange=True)),
        ('separate-frequency', multi, dict(combine='scan', spw='0,1')),
        ('combined-observations', observations, dict(combine='obs,scan')),
        ('combined-fields', source, dict(field='0,1', combine='scan,field')),
        ('half-band-mask', source, dict(maskedge=50, degamp=0, degphase=0)),
        ('negative-mask', source, dict(maskedge=-5, maskcenter=-1)),
        ('legacy-cuts', source, dict(minblperant=30, minsnr=10000.)),
        ('negative-cuts', source, dict(minblperant=-1, minsnr=-1.)),
        ('prior-table', source, dict(gaintable=[gain], apply_options=dict(gaintable=[gain]))),
        ('prior-callibrary', source, dict(docallib=True, callib=str(callibrary),
                                         apply_options=dict(gaintable=[gain]))),
        ('parallactic-point', linear, dict(parang=True, smodel=[1.1, .24, .19, .15],
                                         apply_options=dict(parang=True))),
        ('scalar-yy', yy, dict(refant='A0', interchange=True)),
        ('wrapped-phases', wrapped, dict(refant='A2')),
        ('phase-only', source, dict(degamp=-1, refant='A0')),
        ('phase-only-integrations', source, dict(degamp=-1, refant='A0', solint='int')),
        ('amplitude-only', source, dict(degphase=-1, refant='A0')),
    ]
    for name, options in [
        ('flat-spectrum', {}),
        ('weighted-spectrum', dict(alternate_weight=1000.)),
        ('one-hand-flagged-bins', dict(flagged_hands=1)),
        ('both-hands-flagged-bins', dict(flagged_hands=2)),
    ]:
        dataset = root/f'{name}.ms'
        frequency_weight_data(source, dataset, **options)
        cases.append((name, dataset, dict(solint='inf,2ch', maskedge=0, degamp=2, degphase=2)))
    if args.ngc5921 is not None:
        ngc = args.ngc5921.resolve()
        ngc_prior = str(root/'ngc5921.G')
        casa.gaincal(str(ngc), ngc_prior, field='0', spw='0', refant='VA15', calmode='p',
                     combine='', minsnr=0.)
        cases.append(('vla-ngc5921', ngc, dict(refant='VA15', spw='0', combine='',
                      gaintable=[ngc_prior], apply_options=dict(spw='0', gaintable=[ngc_prior]),
                      interchange=True)))
    results = {}
    for name, dataset, options in cases:
        if args.only is not None and name not in args.only:
            continue
        folder = root/name
        folder.mkdir()
        results[name] = compare(folder, dataset, native, **options)
    if args.only is None or 'append' in args.only:
        results.update(append_cases(root, multi, native))
    if args.only is None or 'no-fit' in args.only:
        results.update(no_fit_cases(root, source, multi, native))
    if args.only is None or 'singular-interval' in args.only:
        results['singular-interval'] = singular_interval(root, source, native)
    if args.only is None or 'singleton-windows' in args.only:
        results['singleton-windows'] = singleton_windows(root, multi, native)
    for left, right in [('flat-spectrum', 'weighted-spectrum'),
                        ('one-hand-flagged-bins', 'both-hands-flagged-bins')]:
        if left in results and right in results:
            left_coefficients, right_coefficients = (
                contents(root/label/'casa.bpoly')['POLY_COEFF_AMP'] for label in (left, right))
            assert relative_rms(left_coefficients, right_coefficients) > .01
    (root/'results.json').write_text(json.dumps(results, indent=2)+'\n')
    for name, errors in results.items():
        assert max(errors.values(), default=0.) < 1e-3, (name, errors)


if __name__ == '__main__':
    main()
