#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare statwt previews and persisted scalar, spectral, and flag columns."""

import argparse
import inspect
import os
from pathlib import Path

import casatasks as casa
import casatools
import numpy as np

from applycal_oracle import copy_ms
from calibration_initweights_oracle import opened
from gaincal_oracle import amplitude_data, gain_and_apply
from record_casa_returns import make_cal_ms


COLUMNS = ("DATA", "CORRECTED_DATA", "MODEL_DATA", "WEIGHT", "SIGMA", "FLAG", "FLAG_ROW")
SPECTRA = ("WEIGHT_SPECTRUM", "SIGMA_SPECTRUM")


def fixture(path):
    make_cal_ms(str(path))
    with opened(path, True) as table:
        rows = table.nrows()
        values = np.array([[-3, -2, -1, 0, 1, 2, 3, 4], [0, 4, -1, 2, 3, 1, -5, 0]], dtype=np.complex64)
        values += 1j * np.array([[0, 0, 1, 1, -1, 0, 2, -3], [1, 0, 6, 2, 0, -2, -2, 4]])
        cube = values[:, :, None] + np.reshape(np.arange(rows) // 10, (1, 1, rows))
        flags = np.zeros(cube.shape, dtype=bool)
        flags[:, 0, 1] = True
        flags[0, :, 2] = True
        flags[1, 1:, 2] = True
        flags[:, :, 3] = True
        table.putcol("DATA", cube)
        table.putcol("FLAG", flags)
        table.putcol("FLAG_ROW", np.all(flags, axis=(0, 1)))
        table.putcell("FLAG_ROW", 4, True)
        table.putcol("CORRECTED_DATA", cube * 2)
        table.putcol("MODEL_DATA", cube * .25)
        table.putcol("SIGMA", np.broadcast_to([[2.], [4.]], (2, rows)).astype(np.float32).copy())
        for column in ("WEIGHT_SPECTRUM", "SIGMA_SPECTRUM"):
            if column in table.colnames():
                table.removecols(column)


def read(path):
    with opened(path) as table:
        return {name: table.getcol(name) for name in (*COLUMNS, *SPECTRA)
                if name in table.colnames()}


def compare_case(root, base, native, name, options, *, through_tool=False, field_rows=None, multi_ms=False):
    products = []
    reports = []
    for api, engine in ((casa, "casa"), (native, "native")):
        vis = root / f"{engine}-{name}.ms"
        if multi_ms:
            casa.partition(str(base), str(vis), separationaxis='scan', numsubms=2,
                           disableparallel=True, flagbackup=False, datacolumn='all')
        else:
            copy_ms(base, vis)
        before = read(vis)
        if through_tool:
            if engine == 'casa':
                factory = casatools.ms
            else:
                from casars.tools import ms as factory

            def run(preview):
                tool = factory()
                assert tool.statwt() == {}
                assert tool.open(str(vis), nomodify=preview, lock=True)
                try:
                    supplied = dict(options)
                    assert tool.msselect({'field': 'FLUX'}, onlyparse=True)
                    if 'field' in supplied:
                        assert tool.msselect({'field': supplied.pop('field')})
                        assert tool.msselect({'spw': '0:2~5'})
                        assert not tool.msselect({'unknown': 'ignored'})
                    return tool.statwt(preview=preview, **supplied)
                finally:
                    tool.close()
        else:
            def run(preview):
                supplied = dict(options)
                supplied.setdefault('flagbackup', preview)
                return api.statwt(str(vis), preview=preview, **supplied)
        preview = run(True)
        assert list(preview) == ["mean", "variance"]
        assert not Path(str(vis) + ".flagversions").exists()
        after = read(vis)
        assert after.keys() == before.keys()
        for column, values in after.items():
            np.testing.assert_array_equal(values, before[column], err_msg=column)
        report = run(False)
        for key in report:
            np.testing.assert_allclose(report[key], preview[key], rtol=2e-7, atol=0, err_msg=(engine, name, key))
        reports.append(report)
        result = read(vis)
        if options.get('flagbackup'):
            snapshot = Path(str(vis) + '.flagversions') / 'flags.statwt_1'
            with opened(snapshot) as backup:
                for column in ('FLAG', 'FLAG_ROW'):
                    np.testing.assert_array_equal(backup.getcol(column), before[column])
        for column in (name for name in ("DATA", "CORRECTED_DATA", "MODEL_DATA") if name in before):
            np.testing.assert_array_equal(result[column], before[column], err_msg=column)
        if (field_rows is not None or options.get("field")) and options.get("selectdata", True):
            with opened(vis) as table:
                excluded = table.getcol("FIELD_ID") != (field_rows if field_rows is not None else 0)
            for column in ("WEIGHT", "SIGMA", "FLAG", "FLAG_ROW", *SPECTRA):
                if column in before:
                    np.testing.assert_array_equal(result[column][..., excluded], before[column][..., excluded])
        products.append(result)
    for key in reports[0]:
        np.testing.assert_allclose(reports[1][key], reports[0][key], rtol=2e-7, atol=0, err_msg=key)
    assert products[1].keys() == products[0].keys()
    for column in products[0]:
        np.testing.assert_allclose(products[1][column], products[0][column], rtol=2e-7, atol=0,
                                   err_msg=column)


def compare_downstream(root, base, native):
    amplitude = amplitude_data(root, base)
    prepared = {}
    for spectrum in (False, True):
        for name, api in (('casa', casa), ('native', native)):
            path = root / f'downstream-{spectrum}-{name}.ms'
            copy_ms(amplitude, path)
            api.initweights(str(path), wtmode='nyq', dowtsp=spectrum)
            api.statwt(str(path), datacolumn='data', chanbin=4 if spectrum else 'spw',
                       timebin=3, flagbackup=False)
            prepared[spectrum, name] = path
        for calwt in (False, True):
            case = root / f'downstream-{spectrum}-calwt-{calwt}'
            case.mkdir()
            result = gain_and_apply(
                case, prepared[spectrum, 'casa'], native, 'G', 'inf', calmode='ap', minblperant=2,
                apply_options={'calwt': [calwt]}, native_source=prepared[spectrum, 'native'],
            )
            expected = read(next(case.glob('*-casa.ms')))
            actual = read(next(case.glob('*-native.ms')))
            for column in ('SIGMA', *SPECTRA):
                if column in expected:
                    np.testing.assert_allclose(actual[column], expected[column], rtol=1e-3, atol=0,
                                               err_msg=column)
            print(f'{case.name} {result}')


def compare_new_selected_spectrum(root, base, native):
    products = []
    for engine, api in (('casa', casa), ('native', native)):
        vis = root / f'{engine}-new-selected-spectrum.ms'
        copy_ms(base, vis)
        original = read(vis)
        args = {'field': 'FLUX', 'chanbin': 3, 'flagbackup': False}
        if engine == 'casa':
            try:
                api.statwt(str(vis), **args)
            except RuntimeError as error:
                assert 'known issue' in str(error)
            else:
                raise AssertionError('CASA did not expose its selected-spectrum creation issue')
            seeded = read(vis)
            for column in original:
                np.testing.assert_array_equal(seeded[column], original[column])
        api.statwt(str(vis), **args)
        result = read(vis)
        with opened(vis) as table:
            excluded = table.getcol('FIELD_ID') != 0
        expected = np.broadcast_to(original['WEIGHT'][:, None, :], result['WEIGHT_SPECTRUM'].shape)
        np.testing.assert_array_equal(result['WEIGHT_SPECTRUM'][..., excluded], expected[..., excluded])
        products.append(result)
    for column, expected in products[0].items():
        np.testing.assert_allclose(products[1][column], expected, rtol=2e-7, atol=0, err_msg=column)


def selector_fixture(path, base):
    copy_ms(base, path)
    for subtable, overrides in [
        ('OBSERVATION', {}),
        ('STATE', {'OBS_MODE': 'CALIBRATE_PHASE#ON_SOURCE'}),
        ('SPECTRAL_WINDOW', {'NAME': 'DECIMATED'}),
        ('DATA_DESCRIPTION', {'SPECTRAL_WINDOW_ID': 1}),
    ]:
        with opened(path / subtable, True) as table:
            assert table.nrows() == 1, subtable
            table.addrows(1)
            for column in table.colnames():
                if table.iscelldefined(column, 0):
                    table.putcell(column, 1, overrides.get(column, table.getcell(column, 0)))
    with opened(path, True) as table:
        fields = table.getcol('FIELD_ID')
        for column in ('OBSERVATION_ID', 'STATE_ID', 'ARRAY_ID', 'DATA_DESC_ID'):
            table.putcol(column, fields)
        scans = np.unique(table.getcol('SCAN_NUMBER')[fields == 1])
        assert len(scans) == 1
        return str(scans[0])


def compare_undefined_spectra(root, base, native):
    for spectrum, rows in [('weight-first-undefined', [1]), ('weight-later-undefined', [0])]:
        source = root / f'{spectrum}.ms'
        copy_ms(base, source)
        with opened(source, True) as table:
            descriptor = table.getcoldesc('DATA')
            descriptor.update(valueType='float', dataManagerType='StandardStMan',
                              dataManagerGroup='StandardStMan', option=0)
            descriptor.pop('shape', None)
            table.addcols({'WEIGHT_SPECTRUM': descriptor})
            for row in rows:
                table.putcell('WEIGHT_SPECTRUM', row, np.full((2, 8), 4.0, dtype=np.float32))
        previews = []
        for engine, api in [('casa', casa), ('native', native)]:
            vis = root / f'{engine}-{spectrum}.ms'
            copy_ms(source, vis)
            if spectrum == 'weight-later-undefined':
                try:
                    api.statwt(str(vis), flagbackup=False, preview=True)
                except RuntimeError as error:
                    assert 'WEIGHT_SPECTRUM' in str(error)
                else:
                    raise AssertionError(f'{engine} accepted a partially initialized weight spectrum')
            else:
                previews.append(api.statwt(str(vis), flagbackup=False, preview=True))
                api.statwt(str(vis), flagbackup=False)
                with opened(vis) as table:
                    assert not table.iscelldefined('WEIGHT_SPECTRUM', 0)
                    np.testing.assert_array_equal(table.getcell('WEIGHT_SPECTRUM', 1), 4.0)
        if previews:
            np.testing.assert_allclose(list(previews[0].values()), list(previews[1].values()), rtol=2e-7)


def compare_write_batches(root, base, native):
    expanded = root / 'multiple-write-batches.ms'
    copy_ms(base, expanded)
    with opened(expanded, True) as table:
        while table.nrows() < 70_000:
            rows = table.nrows()
            table.copyrows(str(expanded), startrowout=rows, nrow=rows)
            for name in ('TIME', 'TIME_CENTROID'):
                values = table.getcol(name, startrow=rows, nrow=rows)
                table.putcol(name, values + np.ptp(values) + 10, startrow=rows, nrow=rows)
    compare_case(root, expanded, native, 'spectral-write-batches', {'chanbin': 3})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", required=True, type=Path)
    parser.add_argument("--native-bin", required=True, type=Path)
    parser.add_argument("--large", action="store_true", help="include the 122,880-row spectral write")
    args = parser.parse_args()
    root = args.work.resolve()
    root.mkdir()
    os.environ["CASARS_CALIBRATE_BIN"] = str(args.native_bin.resolve())
    os.environ["CASARS_REQUIRE_TASK_BINARIES"] = "1"
    from casars import tasks as native

    assert casa.version_string() == "6.7.6.14"
    source = inspect.signature(casa.statwt)
    candidate = inspect.signature(native.statwt)
    assert [(p.name, p.kind, p.default) for p in source.parameters.values()] == [
        (p.name, p.kind, p.default) for p in candidate.parameters.values()
    ]
    base = root / "base.ms"
    fixture(base)
    cases = [
        ("corrected", {}),
        ("data", {"datacolumn": "DATA"}),
        ("correlations", {"datacolumn": "data", "combine": "corr"}),
        ("selected-field", {"field": "FLUX"}),
        ("selectdata-disabled", {"field": "unused", "selectdata": False}),
        ("minimum-and-range", {"minsamp": 8, "wtrange": [0.04, 0.08]}),
        ("residual-data", {"datacolumn": "residual_data"}),
        ("residual-corrected", {"datacolumn": "residual"}),
        ("flag-backup", {"flagbackup": True, "minsamp": 9}),
    ]
    for name, options in cases:
        compare_case(root, base, native, name, options)
    selected = root / 'observing-selectors.ms'
    scan = selector_fixture(selected, base)
    selectors = [('scan', scan), ('observation', '1'), ('array', '1'),
                 ('intent', '*CALIBRATE_PHASE*'), ('spw', 'DECIMATED:1~5^2')]
    for argument, value in selectors:
        compare_case(root, selected, native, f'only-{argument}', {argument: value}, field_rows=1)
    compare_case(root, selected, native, 'different-data-descriptions',
                 {'combine': 'field,scan,state', 'timebin': '200s'})
    auto = root / 'autocorrelations.ms'
    copy_ms(base, auto)
    with opened(auto, True) as table:
        antenna1, antenna2 = table.getcol('ANTENNA1'), table.getcol('ANTENNA2')
        antenna2[::10] = antenna1[::10]
        table.putcol('ANTENNA2', antenna2)
    compare_case(root, auto, native, 'autocorrelations', {'timebin': 2, 'chanbin': 3})
    gapped = root / "gapped.ms"
    copy_ms(base, gapped)
    with opened(gapped, True) as table:
        times = table.getcol("TIME")
        timestamps = np.unique(times)
        times[(times >= timestamps[1]) & (times <= timestamps[5])] += 25
        table.putcol("TIME", times)
    timed = [
        ("timestamp-blocks", {"timebin": 2}),
        ("seconds-blocks", {"timebin": "20s"}),
        ("timestamp-sliding", {"timebin": 2, "slidetimebin": True}),
        ("seconds-sliding", {"timebin": "20s", "slidetimebin": True}),
        ("combine-boundaries", {"timebin": "120s", "combine": "field,scan,state"}),
    ]
    for name, options in timed:
        compare_case(root, gapped, native, name, {"datacolumn": "data", **options})

    weighted = root / 'unequal-exposures.ms'
    copy_ms(gapped, weighted)
    with opened(weighted, True) as table:
        times = table.getcol('TIME')
        indices = np.searchsorted(np.unique(times), times)
        table.putcol('EXPOSURE', 2.0 + indices)
    channels = [
        ('uneven-exposures-sliding', weighted, {'timebin': 3, 'slidetimebin': True}),
        ('uneven-exposures-blocks', weighted, {'timebin': '25s', 'combine': 'field,scan,state'}),
        ('channel-count', base, {'chanbin': 3}),
        ('channel-frequency', base, {'chanbin': '1MHz', 'combine': 'corr'}),
        ('data-channel-count', base, {'chanbin': 2, 'datacolumn': 'data', 'wtrange': [3.0, .01]}),
        ('fit-selected', base, {'fitspw': '0:1~6^2;2~3'}),
        ('fit-excluded', base, {'chanbin': 4, 'fitspw': '0:0~1;6~7', 'excludechans': True}),
        ('spw-channel-selector', base, {'spw': '0:2~5'}),
        ('classical-empty-bin', base, {'timebin': '1s', 'minsamp': 30}),
    ]
    raw = root / 'raw-only.ms'
    copy_ms(base, raw)
    with opened(raw, True) as table:
        table.removecols('CORRECTED_DATA')
    channels.extend([
        ('raw-only', raw, {'datacolumn': 'data'}),
        ('raw-only-spectra', raw, {'datacolumn': 'data', 'chanbin': 3}),
    ])
    spectra = root / 'spectra.ms'
    copy_ms(base, spectra)
    casa.statwt(str(spectra), chanbin=3, flagbackup=False)
    casa.statwt(str(spectra), chanbin=3, datacolumn='data', flagbackup=False)
    channels.extend([
        ('existing-corrected-spectrum', spectra, {'field': 'FLUX', 'chanbin': '1MHz', 'combine': 'corr'}),
        ('existing-raw-spectrum', spectra, {'datacolumn': 'data', 'timebin': 2, 'slidetimebin': True}),
    ])
    for name, source_ms, options in channels:
        compare_case(root, source_ms, native, name, options)
    no_model = root / 'no-model.ms'
    copy_ms(base, no_model)
    with opened(no_model, True) as table:
        table.removecols('MODEL_DATA')
    compare_case(root, no_model, native, 'residual-without-model', {'datacolumn': 'residual_data'})
    virtual = root / 'virtual-model.ms'
    copy_ms(no_model, virtual)
    casa.setjy(str(virtual), field='FLUX', standard='manual', reffreq='1GHz',
               fluxdensity=[2., 0., 0., 0.], spix=-0.7, usescratch=False)
    compare_case(root, virtual, native, 'residual-virtual-model', {'datacolumn': 'residual', 'field': 'FLUX'})
    compare_new_selected_spectrum(root, base, native)
    compare_undefined_spectra(root, base, native)
    outliers = root / 'outliers.ms'
    copy_ms(weighted, outliers)
    with opened(outliers, True) as table:
        values = table.getcol('CORRECTED_DATA')
        values[0, 3, ::14] += 200 + 80j
        values[1, 6, ::13] -= 50 + 300j
        table.putcol('CORRECTED_DATA', values)
    robust = [
        ('hinges-default', {'statalg': 'hinges-fences'}),
        ('hinges-fences', {'statalg': 'hinges-fences', 'fence': 1.5, 'chanbin': 4}),
        ('chauvenet-default', {'statalg': 'chauvenet'}),
        ('chauvenet-limited', {'statalg': 'chauvenet', 'zscore': 1.75, 'maxiter': 2}),
        ('fit-half-mean', {'statalg': 'fit-half'}),
        ('fit-half-median', {'statalg': 'fit-half', 'center': 'median', 'lside': False}),
        ('fit-half-zero', {'statalg': 'fit-half', 'center': 'zero', 'combine': 'corr'}),
    ]
    for name, options in robust:
        compare_case(root, outliers, native, name, {'timebin': 3, 'slidetimebin': True, **options})
    tools = [
        ('tool-default', base, {}),
        ('tool-selection', spectra, {'field': 'FLUX', 'statalg': 'hinges-fences', 'fence': 1.5, 'chanbin': 4}),
        ('tool-variants', weighted, {'chanbin': '1MHz', 'timebin': '25s', 'slidetimebin': True, 'datacolumn': 'data'}),
    ]
    for name, source_ms, options in tools:
        compare_case(root, source_ms, native, name, options, through_tool=True)
    partitions = [
        ('multi-ms-scalar', {}, False),
        ('multi-ms-spectral', {'chanbin': 3}, False),
        ('multi-ms-cross-scan', {'timebin': '200s', 'combine': 'field,scan,state'}, False),
        ('multi-ms-tool', {'chanbin': 4, 'datacolumn': 'data'}, True),
    ]
    for name, options, through_tool in partitions:
        compare_case(root, base, native, name, options, through_tool=through_tool, multi_ms=True)
    compare_downstream(root, base, native)
    if args.large:
        compare_write_batches(root, base, native)
    total = len(cases) + len(selectors) + len(timed) + len(channels) + len(robust) + len(tools)
    print(f"{total + len(partitions) + 11 + args.large} matched CASA statwt workflows")


if __name__ == "__main__":
    main()
