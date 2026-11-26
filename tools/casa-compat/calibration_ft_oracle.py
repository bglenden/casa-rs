#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare the ft model records, visibility storage and consumers with CASA."""

import argparse
from datetime import UTC, datetime, timedelta
import json
import os
from pathlib import Path
import shutil
import tarfile

import casatasks as casa
from casatools import componentlist, image, imager, table
import numpy as np

from applycal_mms_oracle import referenced_tables
from applycal_oracle import copy_ms
from calibration_scratch_oracle import MAIN, prepare, state
from bandpass_oracle import source_data
from gaincal_oracle import relative_rms, solve


def coordinates(ms):
    tb = table()
    assert tb.open(str(ms/'FIELD'))
    try:
        return tb.getcell('PHASE_DIR', 0)[:, 0]
    finally:
        tb.close()


def component(path, direction, flux, *, resolved=False):
    cl = componentlist()
    ra, dec = direction
    shape = (dict(shape='Gaussian', majoraxis='23arcsec', minoraxis='9arcsec',
                  positionangle='28deg', freq='100GHz', spectrumtype='spectral index', index=-0.5)
             if resolved else {})
    assert cl.addcomponent(dir=f'J2000 {ra}rad {dec}rad', flux=flux, fluxunit='Jy', **shape)
    assert cl.rename(str(path))
    cl.done()


def compare_outputs(actual, expected):
    a, native_model = actual
    b, casa_model = expected
    assert a.keys() == b.keys()
    for col in a:
        if col in MAIN:
            np.testing.assert_array_equal(a[col], b[col], err_msg=col)
        else:
            scale = max(1., float(np.max(np.abs(b[col]))))
            np.testing.assert_allclose(a[col], b[col], atol=3e-5*scale, rtol=0, err_msg=col)
    np.testing.assert_equal(native_model, casa_model)


def history_trace(path):
    tb = table()
    tb.open(str(path/'HISTORY'))
    try:
        messages = tb.getcol('MESSAGE')
        origin = tb.getcol('ORIGIN')
    finally:
        tb.close()
    indices = np.flatnonzero((messages == 'taskname = ft') & (origin == 'ft'))
    if not len(indices):
        return []
    return [line for line in messages[indices[-1]:] if not line.startswith('vis ')][:12]


def validation_paths(root, source, native, point):
    failures = [
        ('no-model', {'usescratch': True}, ValueError, True, 'specify a model image or component list'),
        ('unknown-image', {'model': str(root/'missing.image')}, ValueError, False, 'cannot be found'),
        ('restriction', {'scan': '1', 'complist': str(point)}, ValueError, False, 'usescratch must be set to True'),
        ('invalid-nterms', {'nterms': 0, 'complist': str(point)}, ValueError, False, 'greater than or equal to 1'),
        ('missing-terms', {'nterms': 2, 'complist': str(point)}, ValueError, False, 'nterms model-image names'),
        ('uvrange-type', {'uvrange': ['>1m'], 'complist': str(point), 'usescratch': True},
         AssertionError, False, "{'uvrange': ['must be of cStr type']}"),
        ('observation-type', {'observation': 0, 'complist': str(point), 'usescratch': True},
         AssertionError, False, "{'observation': ['must be of cStr type']}"),
    ]
    outcomes = {}
    for label, arguments, exception, added, text in failures:
        outputs = []
        for tool, engine in [(casa, 'casa'), (native, 'native')]:
            path = root/f'refused-{label}-{engine}.ms'
            prepare(source, path)
            try:
                tool.ft(str(path), **arguments)
            except exception as error:
                assert text in str(error)
            else:
                raise AssertionError(f'ft accepted {label}')
            state_after = state(path)
            assert ('MODEL_DATA' in state_after[0]) == added
            outputs.append(state_after)
        compare_outputs(outputs[1], outputs[0])
        outcomes[f'refused-{label}'] = {'scratch_initialized': added}
    return outcomes



def scratch_attachment(root, source, native, point):
    """Scratch attachment side effects precede model validation and retry."""
    missing = str(root / "missing-attachment.image")
    for name in ("open", "selectvis"):
        path = root / f"scratch-attachment-{name}.ms"
        prepare(source, path, model=True, virtual=True)
        before, (_, source_keywords, _) = state(path)
        assert len(source_keywords) == 4
        tool = imager()
        try:
            if name == "open":
                assert tool.open(str(path), usescratch=True) is True
            else:
                assert tool.selectvis(vis=str(path), usescratch=True) is True
            attached, virtual = state(path)
            assert virtual[:2] == ({}, {})
            np.testing.assert_equal(attached, before)
            assert tool.ft(model=[missing]) is False
            compare_outputs(state(path), (attached, virtual))
            assert tool.ft(complist=str(point)) is True
            assert np.any(state(path)[0]["MODEL_DATA"] != before["MODEL_DATA"])
        finally:
            tool.done()
        print(f"scratch-attachment-{name}: cleared at attachment and retried on the same object",
              flush=True)

    for existing in (False, True):
        outputs = []
        for engine, api in (("casa", casa), ("native", native)):
            path = root / f"scratch-attachment-{existing}-{engine}.ms"
            prepare(source, path, model=existing, virtual=True)
            before, (_, source_keywords, _) = state(path)
            assert len(source_keywords) == 4
            try:
                api.ft(str(path), usescratch=True, model=missing)
            except ValueError as error:
                message = getattr(getattr(error, "completion", None), "message", str(error))
                assert message == f"Model image {missing} cannot be found", message
            else:
                raise AssertionError("ft accepted a missing image")
            attached, virtual = state(path)
            assert virtual[:2] == ({}, {}), (engine, existing, "virtual recipes survived attachment")
            if existing:
                np.testing.assert_equal(attached, before)
            else:
                np.testing.assert_equal({key: value for key, value in attached.items()
                                         if key != "MODEL_DATA"}, before)
                assert "MODEL_DATA" in attached
            assert api.ft(str(path), usescratch=True, complist=str(point)) is None
            output = state(path)
            assert np.any(output[0]["MODEL_DATA"] != attached["MODEL_DATA"])
            outputs.append(((attached, virtual), output))
        for expected, actual in zip(*outputs, strict=True):
            compare_outputs(actual, expected)
        print(f"scratch-attachment-existing-{existing}: paired failure, storage and retry", flush=True)
    return {"scratch-attachment": {"public_tool_sequences": 2, "paired_task_sequences": 2}}

def make_images(root, source):
    phase = coordinates(source)
    tb = table()
    assert tb.open(str(source/'SPECTRAL_WINDOW'))
    try:
        frequencies = tb.getcell('CHAN_FREQ', 0)
    finally:
        tb.close()
    outputs = {}
    for name, count, flux in [('point', 1, 2.), ('term1', 1, .5),
                               ('cube', len(frequencies), 1.), ('empty', 1, 0.),
                               ('fractional', 1, 2.), ('outside', 1, 2.)]:
        path = root/f'{name}.im'
        tool = image()
        data = np.zeros((32, 32, 1, count), np.float32)
        data[16, 16, 0, :] = flux
        if count > 1:
            data[16, 16, 0, :] += np.arange(count)*.2
            data[17, 18, 0, :] = .1
        assert tool.fromarray(outfile=str(path), pixels=data)
        cs = tool.coordsys()
        cs.setunits(['rad', 'rad', '', 'Hz'])
        cs.setreferencevalue(value=f'{phase[0]}rad {phase[1]}rad', type='direction')
        cs.setincrement(value='-1arcsec 1arcsec', type='direction')
        ref_pixel = {'fractional': [15.5, 15.5], 'outside': [-4., 12.]}.get(name, [16., 16.])
        cs.setreferencepixel(value=ref_pixel, type='direction')
        cs.setreferencecode('TOPO', type='spectral', adjust=False)
        cs.setreferencepixel(value=[0.], type='spectral')
        cs.setreferencevalue(value=f'{frequencies[0]}Hz', type='spectral')
        cs.setincrement(value='1000000Hz' if count > 1 else '30000000Hz', type='spectral')
        assert tool.setcoordsys(cs.torecord())
        assert tool.setbrightnessunit('Jy/pixel')
        cs.done()
        tool.done()
        outputs[name] = str(path)
    return outputs


def record_layout(path):
    pointers, source, records = state(path)[1]
    layout = []
    for record in records or []:
        if not record:
            continue
        entries = {}
        for key, entry in record.items():
            if key.startswith(('ft_', 'cl_')):
                container = entry['container']
                entries[key] = (entry['type'], entry['indexcombination'],
                                entry['channelselection'], container.get('name'),
                                container.get('thisterm'), container.get('reffreq'))
            else:
                entries[key] = entry
        layout.append(entries)
    return pointers, source, layout


def image_workflows(root, source, native, point, case=None):
    images = make_images(root, source)
    cases = [
        ('point-scratch', 'point', True, {}),
        ('point-virtual', 'point', False, {}),
        ('fractional-scratch', 'fractional', True, {}),
        ('fractional-virtual', 'fractional', False, {}),
        ('outside-reference', 'outside', True, {}),
        ('cube-scratch', 'cube', True, {}),
        ('cube-virtual', 'cube', False, {}),
        ('cube-selected', 'cube', True, {'spw': '0:2~4'}),
        ('cube-strided-scratch', 'cube', True, {'spw': '0:1~8^2'}),
        ('cube-disjoint-ranges', 'cube', True, {'spw': '0:1~2;5~8^2'}),
        ('cube-disjoint-component', 'cube', True,
         {'spw': '0:1~2;5~8^2', 'complist': str(point)}),
        ('cube-virtual-selected', 'cube', False, {'spw': '0:2~4'}),
        ('taylor-scratch', ('point', 'term1'), True, {'nterms': 2, 'reffreq': '99GHz'}),
        ('taylor-virtual', ('point', 'term1'), False, {'nterms': 2, 'reffreq': '99GHz'}),
        ('taylor-default-frequency', ('point', 'term1'), False, {'nterms': 2}),
        ('empty-scratch', 'empty', True, {}),
        ('empty-virtual', 'empty', False, {}),
        ('add-to-scratch', 'point', True, {'incremental': True}),
        ('with-component', 'point', True, {'complist': str(point)}),
        ('with-virtual-component', 'point', False, {'complist': str(point)}),
    ]
    outcomes = {}
    for label, model, scratch, kwargs in cases:
        if case and case not in label:
            continue
        model = images[model] if isinstance(model, str) else [images[name] for name in model]
        results = []
        for writer, reader in ((casa, casa), (native, casa), (casa, native), (native, native)):
            ms = root/f'image-{label}-{writer.__name__.split(".")[-1]}-{reader.__name__.split(".")[-1]}.ms'
            prepare(source, ms, model=scratch)
            assert writer.ft(str(ms), field='0', model=model, usescratch=scratch, **kwargs) is None
            record = record_layout(ms)
            if not scratch:
                assert reader.uvsub(str(ms)) is None
            actual, _ = state(ms)
            column = 'MODEL_DATA' if scratch else 'CORRECTED_DATA'
            if results:
                expectation, reference, ref_record = results[0]
                np.testing.assert_equal(record, ref_record)
                for name in MAIN:
                    if name in expectation:
                        np.testing.assert_array_equal(actual[name], expectation[name])
                scale = max(1., float(np.max(np.abs(reference))))
                np.testing.assert_allclose(actual[column], reference, atol=3e-5*scale, rtol=0,
                                           err_msg=label)
            results.append((actual, actual[column], record))
        outcomes[label] = {'paired_writes_and_cross_reads': True}
        print(label, flush=True)
    if case is None or case in 'fits-model':
        outcomes['fits-model'] = fits_model(root, source, native, images)
    if case is None or case in 'image-lifetime':
        outcomes['image-lifetime'] = independent_image_lifetimes(root, source, native, images)
    return outcomes


def mms_workflows(root, source, native, point):
    result = {}
    for scratch in (False, True):
        engine_states = []
        for label, api in [('casa', casa), ('native', native)]:
            dataset = root/f'mms-{scratch}-{label}.ms'
            path = root/f'mms-{scratch}-{label}.mms'
            prepare(source, dataset)
            casa.partition(str(dataset), str(path), separationaxis='scan', numsubms=2,
                           disableparallel=True, flagbackup=False, datacolumn='all')
            assert api.ft(str(path), field='0', complist=str(point), usescratch=scratch) is None
            parts = referenced_tables(path)
            assert len(parts) == 2
            engine_states.append([state(part) for part in parts])
        for native_state, casa_state in zip(engine_states[1], engine_states[0]):
            compare_outputs(native_state, casa_state)
        result[f'mms-scratch-{scratch}'] = {'two_partitions_match': True}
    for test in ('empty-SOURCE', 'missing-SOURCE-ID'):
        engine_states = []
        for label, api in [('casa', casa), ('native', native)]:
            dataset = root/f'mms-{test}-{label}.ms'
            path = root/f'mms-{test}-{label}.mms'
            prepare(source, dataset)
            if test == 'missing-SOURCE-ID':
                tb = table()
                tb.open(str(dataset/'FIELD'), nomodify=False)
                tb.putcell('SOURCE_ID', 0, -1)
                tb.close()
            casa.partition(str(dataset), str(path), separationaxis='baseline', numsubms=2,
                           disableparallel=True, flagbackup=False, datacolumn='all')
            parts = referenced_tables(path)
            if test == 'empty-SOURCE':
                for part in parts:
                    tb = table()
                    tb.open(str(part/'SOURCE'), nomodify=False)
                    tb.removerows(list(range(tb.nrows())))
                    tb.close()
            assert api.ft(str(path), field='0', complist=str(point)) is None
            states = [state(part) for part in parts]
            assert all('model_0' in model[0] for _, model in states)
            engine_states.append(states)
        for native_state, casa_state in zip(engine_states[1], engine_states[0]):
            compare_outputs(native_state, casa_state)
        result[test] = {'models_in_both_physical_main_tables': True}
    return result


def independent_image_lifetimes(root, source, native, images):
    path = root/'lifetime.mms'
    dataset = root/'lifetime.ms'
    prepare(source, dataset)
    casa.partition(str(dataset), str(path), separationaxis='baseline', numsubms=2,
                   disableparallel=True, flagbackup=False, datacolumn='all')
    first, second = referenced_tables(path)
    assert native.ft(str(path), field='0', model=images['point']) is None
    def stored_grid(part):
        records = state(part)[1][2]
        grid = Path(next(record['ft_0']['container']['diskimage'] for record in records if record))
        assert grid.resolve().parent == (part/'SOURCE').resolve()
        return grid.resolve()

    original_first, original_second = [stored_grid(part) for part in (first, second)]
    assert original_first != original_second
    assert native.ft(str(first), field='0', model=images['cube']) is None
    assert stored_grid(first) != original_first
    assert stored_grid(second) == original_second
    assert original_second.exists()
    # A SOURCE table supplies no complete list of MAIN tables that can
    # retain the retired recipe. Replacement leaves its grid available.
    assert original_first.exists()
    expected = root/'lifetime-reference.ms'
    copy_ms(second, expected)
    assert casa.uvsub(str(expected)) is None
    assert native.uvsub(str(second)) is None
    np.testing.assert_allclose(state(second)[0]['CORRECTED_DATA'],
                               state(expected)[0]['CORRECTED_DATA'], atol=1e-5, rtol=0)
    return {'the_remaining_partition_retains_its_grid': True}


def fits_model(root, source, native, images):
    path = root/'existing-model.fits'
    tool = image()
    assert tool.open(images['point'])
    assert tool.tofits(outfile=str(path))
    tool.done()
    for name, scratch, models in (
        ('virtual', False, str(path)),
        ('scratch', True, str(path)),
        ('taylor', True, [str(path), images['term1']]),
    ):
        states = []
        for label, api in [('casa', casa), ('native', native)]:
            dataset = root/f'fits-model-{name}-{label}.ms'
            prepare(source, dataset)
            assert api.ft(str(dataset), usescratch=scratch, model=models,
                          nterms=2 if name == 'taylor' else 1) is None
            states.append(state(dataset))
        compare_outputs(states[1], states[0])
    return {'same_model_and_scratch_state': True}


def calibration_chain(root, source, native, component_path):
    """Fit against the other engine's persisted virtual component recipe."""
    cases = [(casa, casa, casa), (casa, native, native), (native, casa, casa),
             (native, native, native)]
    expected = None
    errors = []
    for index, (writer, solver, subtractor) in enumerate(cases):
        path = root/f'chain-{index}.ms'
        prepare(source, path)
        writer.ft(str(path), field='0', complist=str(component_path))
        before = state(path)
        result = solve(solver, root, f'chain-{index}', path, field='0', gaintype='G',
                       calmode='ap', solint='inf', minblperant=2)
        subtractor.uvsub(str(path))
        arrays, virtual = state(path)
        np.testing.assert_equal(before[1], virtual)
        for name in MAIN:
            if name in before[0]:
                np.testing.assert_equal(arrays[name], before[0][name])
        if expected is None:
            expected = result, arrays['CORRECTED_DATA']
            assert not np.all(result['FLAG'])
            continue
        for name in ('FIELD_ID', 'ANTENNA1', 'ANTENNA2', 'SPECTRAL_WINDOW_ID', 'FLAG'):
            np.testing.assert_array_equal(result[name], expected[0][name])
        good = ~expected[0]['FLAG']
        errors.append({
            'gain_nrms': relative_rms(result['CPARAM'][good], expected[0]['CPARAM'][good]),
            'corrected_nrms': relative_rms(arrays['CORRECTED_DATA'], expected[1]),
        })
        assert errors[-1]['gain_nrms'] < 1e-3
        assert errors[-1]['corrected_nrms'] < 1e-5
    return errors


def initial_time_range(source):
    tb = table()
    tb.open(str(source))
    try:
        start = datetime(1858, 11, 17, tzinfo=UTC) + timedelta(seconds=float(tb.getcell('TIME', 0)))
    finally:
        tb.close()
    fmt = '%Y/%m/%d/%H:%M:%S'
    return f'{(start-timedelta(seconds=1)).strftime(fmt)}~{(start+timedelta(seconds=1)).strftime(fmt)}'



def publication_after_cleanup_error(root, failed, native):
    """Read the exact record and grids left by the native MAIN-cleanup test."""
    source = failed / "mssel_test_small.ms"
    archive = (Path(__file__).resolve().parents[2]
               / "libs/casacore/casa-ms/tests/fixtures/mssel_test_small.ms.tgz")
    with tarfile.open(archive) as fixture:
        fixture.extractall(root, filter="data")
    reference = root / source.name
    tb = table()
    assert tb.open(str(reference / "SPECTRAL_WINDOW"))
    try:
        reference_hz = tb.getcell("CHAN_FREQ", 1)[0] * 0.99
    finally:
        tb.close()

    terms = []
    for term in range(2):
        assert tb.open(str(failed / f"term-{term}.im"))
        try:
            pixels = tb.getcell("map", 0)
            coords = tb.getkeyword("coords")
        finally:
            tb.close()
        path = root / f"casa-term-{term}.im"
        tool = image()
        assert tool.fromarray(outfile=str(path), pixels=np.asarray(pixels, np.float32))
        assert tool.setcoordsys(coords)
        assert tool.setbrightnessunit("Jy/pixel")
        tool.done()
        terms.append(str(path))
    assert casa.ft(str(reference), field="0", model=terms, nterms=2,
                   reffreq=f"{reference_hz}Hz") is None
    assert tb.open(str(reference / "SOURCE"))
    try:
        assert tb.getkeyword("definedmodel_field_0") == "model_0"
    finally:
        tb.close()

    def sample_rows():
        selected = ((tb.getcol("FIELD_ID") == 0)
                    & (tb.getcol("DATA_DESC_ID") == 1))
        rows = np.flatnonzero(selected)[:12]
        assert len(rows) == 12
        return [int(row) for row in rows]

    def unflag_samples(path):
        # All rows of this fixture's field 0 are originally flagged.
        assert tb.open(str(path), nomodify=False)
        try:
            for row in sample_rows():
                tb.putcell("FLAG_ROW", row, False)
                tb.putcell("FLAG", row, np.zeros_like(tb.getcell("FLAG", row)))
        finally:
            tb.close()

    def sampled(path):
        assert tb.open(str(path))
        try:
            return np.stack([
                tb.getcell("DATA", row) - tb.getcell("CORRECTED_DATA", row)
                for row in sample_rows()
            ])
        finally:
            tb.close()

    unflag_samples(reference)
    assert casa.uvsub(str(reference)) is None
    expected = sampled(reference)
    assert np.max(np.abs(expected.real)) > 1.5
    evidence = {}
    for name, reader in [("CASA", casa), ("Rust", native)]:
        destination = root / f"after-error-{name}.ms"
        shutil.copytree(source, destination)
        unflag_samples(destination)
        assert reader.uvsub(str(destination)) is None
        actual = sampled(destination)
        np.testing.assert_allclose(actual, expected, atol=3e-5, rtol=0)
        max_error = float(np.max(np.abs(actual - expected)))
        evidence[name] = {"complex_samples": actual.size, "max_abs_error": max_error}
        print(f"{name}: {actual.size} complex samples after MAIN cleanup error, "
              f"max abs error {max_error:g}", flush=True)
    return {"virtual-publication": evidence}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--native-bin', type=Path, required=True)
    parser.add_argument('--images', action='store_true')
    parser.add_argument('--chain', action='store_true')
    parser.add_argument('--mms', action='store_true')
    parser.add_argument('--case', help='Run cases containing this text')
    parser.add_argument('--fault-work', type=Path,
                        help='Read the output of the native FT publication fault test')
    args = parser.parse_args()
    assert casa.version_string() == '6.7.6.14'
    os.environ['CASARS_CALIBRATE_BIN'] = str(args.native_bin.resolve())
    os.environ['CASARS_REQUIRE_TASK_BINARIES'] = '1'
    from casars import tasks
    root = args.work.resolve()
    root.mkdir(parents=True)
    if args.fault_work is not None:
        evidence = publication_after_cleanup_error(root, args.fault_work.resolve(), tasks)
        (root / "results.json").write_text(json.dumps(evidence, indent=2) + "\n")
        return
    source = root/'source.ms'
    source_data(source, stokes='XX XY YX YY')
    ra, dec = coordinates(source)
    point = root/'point.cl'
    second = root/'point-second.cl'
    gaussian = root/'gaussian.cl'
    component(point, (ra, dec), 2.)
    component(second, (ra, dec), 3.)
    component(gaussian, (ra+1e-5, dec-2e-5), [3., 0.2, -0.3, 0.1], resolved=True)
    cases = [
        ('virtual-default', point, {}, {}, False),
        ('virtual-field-channel', point, dict(field='0', spw='0:2~4'), {}, False),
        ('virtual-repeat', second, dict(field='0', spw='0:1~8^2'), {}, True),
        ('virtual-incremental', second, dict(field='0', incremental=True), {}, True),
        ('virtual-offaxis', gaussian, dict(field='0'), {}, False),
        ('scratch-default', point, dict(field='0', usescratch=True), {}, False),
        ('scratch-existing', point, dict(field='0', usescratch=True), {'model':True}, True),
        ('scratch-incremental', point, dict(field='0', usescratch=True, incremental=True), {'model':True}, False),
        ('scratch-selected', point, dict(field='0', usescratch=True, spw='0:2~4'), {'model':True}, False),
        ('scratch-strided', point, dict(field='0', usescratch=True, spw='0:1~8^2'), {'model':True}, False),
        ('scratch-multiple-ranges', point, dict(field='0', usescratch=True, spw='0:1~2;5~8^2'), {}, False),
        ('scratch-offaxis', gaussian, dict(field='0', usescratch=True), {}, False),
        ('scratch-selection-intent', point, dict(intent='CALIBRATE_FLUX*', usescratch=True), {}, False),
        ('scratch-selection-scan', point, dict(scan='1', usescratch=True), {}, False),
        ('scratch-selection-antenna', point, dict(antenna='0&1', usescratch=True), {}, False),
        ('scratch-selection-distance', point, dict(uvrange='>1m', usescratch=True), {}, False),
        ('scratch-selection-time', point, dict(timerange=initial_time_range(source), usescratch=True), {}, False),
        ('scratch-selection-observation', point, dict(observation='0', usescratch=True), {}, False),
        ('scratch-boolean-terms', point, dict(nterms=True, usescratch=True), {}, False),
        ('scratch-empty-selection', point, dict(scan='999', usescratch=True), {}, True),
    ]
    evidence = {}
    for label, cl, options, fixture, initial in cases:
        if args.case and args.case not in label:
            continue
        states = []
        for engine, api in [('casa', casa), ('native', tasks)]:
            ms = root/f'{label}-{engine}.ms'
            prepare(source, ms, **fixture)
            if initial:
                casa.ft(str(ms), field='0', complist=str(point))
            assert api.ft(str(ms), complist=str(cl), **options) is None
            states.append((state(ms), history_trace(ms)))
        compare_outputs(states[1][0], states[0][0])
        assert len(states[1][1]) == 12
        np.testing.assert_equal(states[1][1], states[0][1])
        evidence[label] = {'paired': True}
        print(label, flush=True)
    if args.images:
        evidence.update(image_workflows(root, source, tasks, point, args.case))
    if args.mms:
        evidence.update(mms_workflows(root, source, tasks, point))
    if args.case is None:
        evidence.update(validation_paths(root, source, tasks, point))
    if args.case is None or args.case in "scratch-attachment":
        evidence.update(scratch_attachment(root, source, tasks, point))
    if args.chain:
        spectral = root/'spectral-point.cl'
        cl = componentlist()
        assert cl.addcomponent(dir=f'J2000 {ra}rad {dec}rad', flux=2., fluxunit='Jy',
                               freq='100GHz', spectrumtype='spectral index', index=-0.5)
        assert cl.rename(str(spectral))
        cl.done()
        evidence['chain'] = calibration_chain(root, source, tasks, spectral)
    (root/'results.json').write_text(json.dumps(evidence, indent=2)+'\n')


if __name__ == '__main__':
    main()
