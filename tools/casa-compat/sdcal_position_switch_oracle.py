#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare position-switch sky and system-temperature tables with pinned CASA."""

import argparse
import shutil
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
    actual = {}
    for engine, task in engines.items():
        product = root / f'{engine}-tsys.cal'
        assert task(str(vis), 'tsys', outfile=str(product)) is None
        actual[engine] = columns(product)
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
        tests = [
            (seed, {'scan': '2'}, RuntimeError, 'Error in Calibrater::solve.'),
            (seed, {'fraction': 'oops'}, RuntimeError, "could not convert string to float: 'oops'"),
            (seed, {'fraction': '75%'}, ValueError, 'Too many edge points'),
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


if __name__ == '__main__':
    main()
