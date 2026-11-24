#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare position-switch sky and system-temperature tables with pinned CASA."""

import argparse
import select
import shutil
import subprocess
import sys
from pathlib import Path

import casatasks as casa
from casatools import table
import numpy as np

from applycal_oracle import copy_ms
from calibration_smoothcal_oracle import columns, compare_mapping, opened

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tools' / 'singledish'))
from tsdimaging_oracle import make_map


def make_position_switch(root):
    shutil.copyfile(ROOT / 'crates/casa-simulation/tests/data/readantenna/aca.tp.cfg',
                    root / 'aca.tp.cfg')
    vis = Path(make_map(str(root)))
    with opened(vis / 'STATE', True) as state:
        start = state.nrows()
        state.addrows(2)
        for index, intent in enumerate(('ON_SOURCE', 'OFF_SOURCE')):
            row = start + index
            state.putcell('OBS_MODE', row, f'OBSERVE_TARGET#{intent}')
            state.putcell('SIG', row, index == 0)
            state.putcell('REF', row, index == 1)
    with opened(vis, True) as main:
        time = main.getcol('TIME')
        ends = np.array([0, 1, time.size - 2, time.size - 1])
        time[0] -= 5
        exposure = np.ones(time.size)
        exposure[ends] = [1., 4., 3., 2.]
        scan = np.full(time.size, 2, dtype=np.int32)
        scan[ends] = [1, 1, 3, 3]
        state = np.full(time.size, start, dtype=np.int32)
        state[ends] = start + 1
        flags = np.zeros_like(main.getcol('FLAG'))
        flags[:, :, ends[-2:]] = True
        flags[0, 0, ends[0]] = True
        data = main.getcol('DATA')
        data.real[:] = (2. + np.arange(data.shape[0])[:, None, None] * .3
                        + np.arange(data.shape[1])[None, :, None] * .13
                        + np.arange(time.size)[None, None, :] * .006)
        data.imag[:] = 0.25
        for name, value in [('TIME', time), ('TIME_CENTROID', time), ('SCAN_NUMBER', scan),
                            ('FIELD_ID', np.zeros_like(scan)), ('STATE_ID', state),
                            ('EXPOSURE', exposure), ('FLAG', flags), ('DATA', data)]:
            main.putcol(name, value)
        main.putcol('FLAG_ROW', np.zeros(time.size, dtype=bool))
    with opened(vis / 'SPECTRAL_WINDOW', True) as spectral:
        assert spectral.nrows() == 1
        spectral.addrows(1)
        for name in spectral.colnames():
            if spectral.iscelldefined(name, 0):
                spectral.putcell(name, 1, spectral.getcell(name, 0))
        spectral.putcol('TOTAL_BANDWIDTH', np.array([3., 7.]))
    return vis, time[ends], data.real[:, :, ends[:2]]


def assert_scientific_table(path, time, off):
    cells = columns(path)
    want = (off[:, :, 0] + 4 * off[:, :, 1]) / 5.
    want[0, 0] = off[0, 0, 1]
    np.testing.assert_allclose(cells['FPARAM'][:, :, 0], want, rtol=2e-7)
    np.testing.assert_allclose(cells['TIME'], [time[:2].mean(), time[2:].mean()], rtol=0., atol=1e-6)
    np.testing.assert_array_equal(cells['SCAN_NUMBER'], [1, 3])
    np.testing.assert_array_equal(cells['INTERVAL'], [5., 5.])
    np.testing.assert_array_equal(cells['ANTENNA1'], cells['ANTENNA2'])
    np.testing.assert_array_equal(cells['FLAG'][:, :, 0], False)
    np.testing.assert_array_equal(cells['FLAG'][:, :, 1], True)
    np.testing.assert_array_equal(cells['FPARAM'][:, :, 1], 0.)
    with opened(path) as table:
        assert table.info()['subType'] == table.getkeyword('VisCal') == 'SDSKY_PS'
    with opened(path / 'SPECTRAL_WINDOW') as spectral:
        np.testing.assert_array_equal(spectral.getcol('FLAG_ROW'), [False, True])
        widths = spectral.getcol('CHAN_WIDTH')
        np.testing.assert_allclose(spectral.getcol('TOTAL_BANDWIDTH'), np.abs(widths.sum(axis=0)))
    return cells


def compare_sky(root, engines):
    seed, time, off = make_position_switch(root)
    float_ms = root / 'float.ms'
    copy_ms(seed, float_ms)
    with opened(float_ms, True) as main:
        desc = main.getcoldesc('DATA')
        desc.update(name='FLOAT_DATA', valueType='float', dataManagerType='StandardStMan',
                    dataManagerGroup='FloatSkyInput')
        data = main.getcol('DATA')
        main.addcols({'FLOAT_DATA': desc})
        main.putcol('FLOAT_DATA', data.real.astype(np.float32))
        main.putcol('DATA', 2 * data)
    for kind, vis in (('data', seed), ('float', float_ms)):
        actual = {}
        for engine, task in engines.items():
            output = root / f'{engine}-{kind}.cal'
            assert task(str(vis), outfile=str(output), field='0', spw='0', scan='1,3') is None
            actual[engine] = assert_scientific_table(output, time, off)
        if 'native' in actual:
            compare_mapping(actual['native'], actual['casa'], False)
        print(f'{kind} position-switch spectra and flagged group agree', flush=True)
    return seed


def compare_tsys(root, seed, engines):
    from casaconfig import config
    from casatools import ms as ms_tool

    vis = root / 'temperatures.ms'
    copy_ms(seed, vis)
    table_path = vis / 'SYSCAL'
    if table_path.exists():
        shutil.rmtree(table_path)
    with opened(vis) as main:
        times = np.unique(main.getcol('TIME'))[[0, 8, -1]]
    with opened(vis / 'SPECTRAL_WINDOW') as spectral:
        channels = spectral.getcell('NUM_CHAN', 0)
    kelvin = 95. + np.arange(6 * channels).reshape(3, 2 * channels)
    ascii_path = root / 'tsys.txt'
    ascii_path.write_text(''.join(
        ' '.join(map(str, [0, 0, 0, time + .5, 1, *row, 'false'])) + '\n'
        for time, row in zip(times, kelvin)
    ))
    tool = table()
    try:
        assert tool.fromascii(
            str(table_path), str(ascii_path), sep=' ',
            columnnames=['ANTENNA_ID', 'FEED_ID', 'SPECTRAL_WINDOW_ID', 'TIME', 'INTERVAL',
                         'TSYS_SPECTRUM', 'TSYS_FLAG'],
            datatypes=['I', 'I', 'I', 'D', 'D', f'R2,{channels}', 'B'],
        )
    finally:
        tool.close()
    with opened(table_path, True) as syscal:
        syscal.putcolkeywords('TIME', {'QuantumUnits': ['s'], 'MEASINFO': {
            'type': 'epoch', 'Ref': 'UTC'}})
        syscal.putcolkeywords('INTERVAL', {'QuantumUnits': ['s']})
        syscal.putcolkeywords('TSYS_SPECTRUM', {'QuantumUnits': ['K']})
    with opened(vis, True) as main:
        main.putkeyword('SYSCAL', f'Table: {table_path}')
    with opened(vis / 'HISTORY', True) as history:
        row = history.nrows()
        history.addrows(1)
        history.putcell('MESSAGE', row, 'Input MS processing history')
    with opened(vis / 'FIELD', True) as field:
        field.putcell('TIME', 0, times[0])
        for name in ('DELAY_DIR', 'PHASE_DIR', 'REFERENCE_DIR'):
            field.putcell(name, 0, np.array([[1.e-6], [-2.e-6]]))
    ephemeris = (Path(config.measurespath) / 'ephemerides' / 'JPL-Horizons'
                 / 'Uranus_55197-59214dUTC_J2000.tab')
    tool = ms_tool()
    assert tool.open(str(vis), nomodify=False)
    try:
        assert tool.addephemeris(0, str(ephemeris), 'Uranus', [0])
    finally:
        tool.done()
    actual = {}
    directions = {}
    for engine, task in engines.items():
        product = root / f'{engine}-tsys.cal'
        assert task(str(vis), 'tsys', outfile=str(product)) is None
        actual[engine] = columns(product)
        with opened(product / 'HISTORY') as history:
            assert history.nrows() == 0
        with opened(product / 'FIELD') as fields:
            directions[engine] = fields.getcell('PHASE_DIR', 0)
            assert np.linalg.norm(directions[engine]) > 0.1
        np.testing.assert_allclose(actual[engine]['TIME'], times, rtol=0., atol=1e-6)
        np.testing.assert_array_equal(actual[engine]['FPARAM'],
                                      kelvin.T.reshape(2, channels, 3, order='F'))
        assert task(str(vis), 'tsys', outfile=str(product), overwrite=True) is None
        compare_mapping(columns(product), actual[engine], False)
        try:
            task(str(vis), 'tsys', outfile=str(product))
        except RuntimeError as error:
            assert 'output file exists' in str(error)
        else:
            raise AssertionError('An existing outfile must require overwrite=True')
        compare_mapping(columns(product), actual[engine], False)
    if 'native' in actual:
        compare_mapping(actual['native'], actual['casa'], False)
        np.testing.assert_allclose(directions['native'], directions['casa'], rtol=2e-7)
    print('Spectral Tsys generation, overwrite and product protection agree', flush=True)
    return vis


def check_selectors_and_errors(root, seed, temperatures, engines):
    with opened(seed / 'SPECTRAL_WINDOW') as spectral:
        channels = spectral.getcell('NUM_CHAN', 0)
    for engine, task in engines.items():
        cal = root / f'{engine}-one-scan.cal'
        assert task(str(seed), outfile=str(cal), spw='0:1', scan='1') is None
        assert columns(cal)['FPARAM'].shape[1:] == (channels, 1)
        result = root / f'{engine}-refused.cal'
        fraction = root / f'{engine}-whitespace-fraction.cal'
        assert task(str(seed), outfile=str(fraction), fraction=' 50%') is None
        assert fraction.is_dir()
        edges = root / f'{engine}-explicit-edge-count.cal'
        assert task(str(seed), outfile=str(edges), fraction='75%', noff=1) is None
        compare_mapping(columns(edges), columns(fraction), False)
        tests = [
            (seed, {'scan': '2'}, RuntimeError, 'Error in Calibrater::solve.'),
            (seed, {'fraction': 'oops'}, RuntimeError, "could not convert string to float: 'oops'"),
            (seed, {'fraction': '75%'}, ValueError, 'Too many edge points'),
            (seed, {'fraction': []}, RuntimeError, 'Invalid fraction value'),
            (seed, {'fraction': {}}, RuntimeError, 'Invalid fraction value'),
            (temperatures, {'calmode': 'tsys', 'field': 'MISSING_FIELD'}, RuntimeError,
             'Error in data selection specification'),
            (root / 'missing.ms', {}, AssertionError, 'the infile parameter must be a path that exists'),
        ]
        for vis, selectors, exception, message in tests:
            try:
                task(str(vis), outfile=str(result), **selectors)
            except exception as error:
                assert message in str(error), (engine, selectors, error)
            else:
                raise AssertionError((engine, selectors, exception))
            assert not result.exists()
        assert task(str(temperatures), 'tsys', outfile=str(result), spw='0') is None
        assert not result.exists()
    print('Channel selections and invalid position-switch and Tsys inputs agree', flush=True)


def check_parameter_planes(root, seed, engines):
    single = root / 'single-correlation.ms'
    casa.split(str(seed), outputvis=str(single), datacolumn='data', correlation='XX')
    with opened(single) as main:
        assert main.getcol('DATA').shape[0] == 1
    nonfinite = root / 'nonfinite.ms'
    copy_ms(seed, nonfinite)
    with opened(nonfinite, True) as main:
        data = main.getcell('DATA', 0)
        data[0, 1] = np.nan + 0j
        main.putcell('DATA', 0, data)
    for case, vis in (('single', single), ('nonfinite', nonfinite)):
        products = {}
        for engine, task in engines.items():
            out = root / f'{engine}-{case}.cal'
            assert task(str(vis), outfile=str(out), scan='1') is None
            products[engine] = values = columns(out)
            if case == 'single':
                assert values['FPARAM'].shape[0] == 2
                np.testing.assert_array_equal(values['FPARAM'][1], 0.)
                np.testing.assert_array_equal(values['FLAG'][1], True)
                np.testing.assert_array_equal(values['PARAMERR'][1], 0.)
                np.testing.assert_array_equal(values['SNR'][1], 0.)
            else:
                assert np.isnan(values['FPARAM'][0, 1, 0])
                assert not values['FLAG'][0, 1, 0]
        if 'native' in products:
            compare_mapping(products['native'], products['casa'], False)
    print('Single-correlation padding and unflagged nonfinite cells agree', flush=True)


def check_busy_replacement(root, seed, temperatures, native):
    hold = """
import sys
from casatools import table
tool = table()
assert tool.open(sys.argv[1], nomodify=False, lockoptions={'option': 'user'})
tool.lock(write=True, nattempts=1)
assert tool.haslock(write=True)
print('locked', flush=True)
sys.stdin.readline()
assert tool.haslock(write=True)
tool.close()
"""
    separate = root / 'busy-output.ms'
    copy_ms(seed, separate)
    sky = root / 'native-data.cal'
    tsys = root / 'native-tsys.cal'
    for mode, vis, product, locked in (
        ('ps', seed, sky, sky), ('tsys', temperatures, tsys, tsys),
        ('ps', seed, separate, separate / 'FIELD'),
    ):
        before = columns(product)
        with subprocess.Popen([sys.executable, '-c', hold, str(locked)], text=True,
                              stdin=subprocess.PIPE, stdout=subprocess.PIPE) as peer:
            try:
                assert select.select([peer.stdout], [], [], 15.)[0]
                assert peer.stdout.readline().strip() == 'locked'
                try:
                    native(str(vis), mode, outfile=str(product), overwrite=True)
                except RuntimeError as error:
                    assert 'lock' in str(error).lower()
                else:
                    raise AssertionError('Overwriting a busy table tree must fail')
            finally:
                peer.communicate('\n', timeout=15)
            assert peer.returncode == 0
        compare_mapping(columns(product), before, False)
    into_ms = temperatures / 'ANTENNA' / 'unsafe.cal'
    try:
        native(str(temperatures), 'tsys', outfile=str(into_ms))
    except RuntimeError as error:
        assert 'overlap' in str(error)
    else:
        raise AssertionError('A calibration table cannot replace input MS storage')
    assert not into_ms.exists()
    print('CASA-held root and subtable locks protect existing outputs', flush=True)


def check_intent(root, seed, temperatures, tasks):
    vis = root / 'selected-intent.ms'
    copy_ms(seed, vis)
    with opened(vis / 'STATE', True) as state:
        tagged = state.nrows()
        state.addrows(1)
        state.putcell('OBS_MODE', tagged, 'OBSERVE_TARGET#OFF_SOURCE,CALIBRATE_FOCUS#ON_SOURCE')
        state.putcell('SIG', tagged, False)
        state.putcell('REF', tagged, True)
    with opened(vis, True) as main:
        states = main.getcol('STATE_ID')
        states[main.getcol('SCAN_NUMBER') == 1] = tagged
        assert main.putcol('STATE_ID', states)

    selected = {}
    for engine, task in tasks.items():
        default = root / f'{engine}-all-intents.cal'
        subset = root / f'{engine}-selected-intent.cal'
        task(str(vis), calmode='ps', outfile=str(default))
        task(str(vis), calmode='ps', outfile=str(subset), intent='CALIBRATE_FOCUS#ON_SOURCE')
        np.testing.assert_array_equal(columns(default)['SCAN_NUMBER'], [1, 3])
        selected[engine] = columns(subset)
        np.testing.assert_array_equal(selected[engine]['SCAN_NUMBER'], [1])

        default_tsys = root / f'{engine}-tsys-default-intent.cal'
        inactive_tsys = root / f'{engine}-tsys-unused-intent.cal'
        task(str(temperatures), calmode='tsys', outfile=str(default_tsys))
        task(str(temperatures), calmode='tsys', outfile=str(inactive_tsys), intent='UNUSED_INTENT')
        compare_mapping(columns(inactive_tsys), columns(default_tsys), False)

        applications = []
        for name, extra in (('default', {}), ('unused', {'intent': 'UNUSED_INTENT'})):
            apply_vis = root / f'{engine}-apply-{name}-intent.ms'
            copy_ms(vis, apply_vis)
            task(str(apply_vis), calmode='apply', applytable=str(default), **extra)
            with opened(apply_vis) as main:
                applications.append({key: main.getcol(key) for key in ('CORRECTED_DATA', 'FLAG')})
        compare_mapping(applications[1], applications[0], False)
    if 'native' in selected:
        compare_mapping(selected['native'], selected['casa'], False)
    print('Position-switch intent selection and inactive apply/Tsys intent matched', flush=True)
    check_composite_intent(root, vis, temperatures, tasks)


def check_composite_intent(root, vis, temperatures, tasks):
    from sdcal_apply_oracle import compare_products

    with_tsys = root / 'composite-intent-with-tsys.ms'
    copy_ms(vis, with_tsys)
    copy_ms(temperatures / 'SYSCAL', with_tsys / 'SYSCAL')
    with opened(with_tsys, True) as main:
        assert main.putkeyword('SYSCAL', f'Table: {with_tsys / "SYSCAL"}')
    intent = 'CALIBRATE_FOCUS#ON_SOURCE'
    for label, source in (('ps,apply', vis), ('ps,tsys,apply', with_tsys)):
        expected = None
        for engine, task in tasks.items():
            priors = [str(root / f'{engine}-selected-intent.cal')]
            if source == with_tsys:
                tsys = root / f'{engine}-composite-intent-tsys.cal'
                assert task(str(source), 'tsys', outfile=str(tsys)) is None
                priors.append(str(tsys))
            for implementation in ('direct', 'composite'):
                work = root / f'{label}-{implementation}-{engine}.ms'
                copy_ms(source, work)
                if implementation == 'direct':
                    assert task(str(work), 'apply', applytable=priors, interp='linear') is None
                else:
                    assert task(str(work), label, intent=intent, interp='linear') is None
                with opened(work) as output:
                    got = {column: output.getcol(column) for column in ('CORRECTED_DATA', 'FLAG', 'WEIGHT')}
                if expected is None:
                    expected = got
                else:
                    compare_products(got, expected, corrected_rtol=4e-6 if source == with_tsys else 1e-7)
        print(f'{label} with active intent agrees with explicit sky selection', flush=True)

    for task in tasks.values():
        for mode in ('ps', 'ps,apply'):
            try:
                task(str(vis), mode, outfile=str(root / 'invalid-intent.cal'), intent='UNKNOWN_INTENT')
            except RuntimeError as error:
                assert type(error) is RuntimeError
                assert str(error) == (
                    'Error in data selection specification: State Expression: No match found for "UNKNOWN_INTENT"\n'
                    'State Expression: Partial or no match for State ID list []'
                )
            else:
                raise AssertionError(f'{mode} accepted an unmatched intent')


def check_measurement_set_replacement(root, seed, tasks):
    products = {}
    for writer, task in tasks.items():
        product = root / f'{writer}-replaced.ms'
        copy_ms(seed, product)
        assert task(str(seed), 'ps', outfile=str(product), overwrite=True) is None
        with opened(product) as replaced:
            assert replaced.getkeyword('VisCal') == 'SDSKY_PS'
            assert replaced.nrows() == 2
        compare_mapping(columns(product), columns(root / f'{writer}-data.cal'), False)
        products[writer] = product
    expected = None
    for writer, product in products.items():
        for reader, task in tasks.items():
            vis = root / f'{writer}-replaced-applied-{reader}.ms'
            copy_ms(seed, vis)
            assert task(str(vis), 'apply', applytable=str(product)) is None
            with opened(vis) as main:
                result = {key: main.getcol(key) for key in ('CORRECTED_DATA', 'FLAG', 'WEIGHT')}
            if expected is None:
                expected = result
            else:
                compare_mapping(result, expected, False)
    print(f'Separate MeasurementSet replacement passed {len(tasks)} sky-table writers '
          f'and {len(tasks)} application readers', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=Path)
    parser.add_argument('--reference-only', action='store_true')
    args = parser.parse_args()
    assert casa.version_string() == '6.7.6.14'
    root = args.work.resolve()
    root.mkdir()
    tasks = {'casa': casa.sdcal}
    if not args.reference_only:
        from casars.tasks import sdcal
        tasks['native'] = sdcal
    seed = compare_sky(root, tasks)
    temperatures = compare_tsys(root, seed, tasks)
    check_selectors_and_errors(root, seed, temperatures, tasks)
    check_parameter_planes(root, seed, tasks)
    check_intent(root, seed, temperatures, tasks)
    check_measurement_set_replacement(root, seed, tasks)
    if 'native' in tasks:
        check_busy_replacement(root, seed, temperatures, tasks['native'])


if __name__ == '__main__':
    main()
