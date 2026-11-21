#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare CASA switched-power priors and their weighted application."""

from pathlib import Path
import argparse
import shutil
import signal
import subprocess
import sys

from casacore.tables import makearrcoldesc, makescacoldesc, maketabdesc, table
import casatasks as casa
import numpy as np

from applycal_oracle import copy_ms
from calibration_smoothcal_oracle import opened
from record_casa_returns import make_cal_ms

MODES = ('evlagain', 'swpow', 'swpwts', 'swp/rq', 'rq')


def create_table(path, row_specs, arrays, ndim=1):
    columns = [
        makescacoldesc('ANTENNA_ID', 0), makescacoldesc('SPECTRAL_WINDOW_ID', 0),
        makescacoldesc('TIME', 0.0), makescacoldesc('INTERVAL', 10.0),
    ]
    columns.extend(makearrcoldesc(name, 0., ndim=ndim, valuetype='float') for name in arrays)
    if 'NOISE_CAL' in arrays:
        columns.append(makescacoldesc('NUM_CAL_LOAD', 1))
    with table(str(path), maketabdesc(columns), nrow=len(row_specs), ack=False) as result:
        for row, values in enumerate(row_specs):
            for name, value in values.items():
                result.putcell(name, row, value)


def seed(vis):
    make_cal_ms(str(vis), telescope='VLA', frequency='6GHz', stokes='RR RL LR LL')
    with opened(vis, True) as ms:
        times = np.unique(ms.getcol('TIME'))
        ms.putcol('FLAG', np.zeros_like(ms.getcol('FLAG')))
        ms.putcol('FLAG_ROW', np.zeros(ms.nrows(), dtype=bool))
    def metadata(antenna, when):
        return dict(ANTENNA_ID=antenna, SPECTRAL_WINDOW_ID=0, TIME=when, INTERVAL=10.)
    power_rows = []
    for slot, when in enumerate(times):
        for antenna in range(5):
            if slot == 2 and antenna == 2:
                continue
            k = 1 + antenna + slot
            power_rows.append(dict(metadata(antenna, when),
                                   SWITCHED_DIFF=[2+k*.2, 3+k*.3],
                                   SWITCHED_SUM=[50+k, 70+k],
                                   REQUANTIZER_GAIN=[.8+k*.01, 1.2-k*.01]))
    power_rows[0]['SWITCHED_DIFF'][0] = 0.
    power_rows[1]['REQUANTIZER_GAIN'][1] = 0.
    power_rows[2]['SWITCHED_SUM'][0] = -4.
    noise_rows = [dict(metadata(antenna, times[0]), NOISE_CAL=[3. + antenna*.1, 4.5 + antenna*.1],
                       NUM_CAL_LOAD=1) for antenna in range(5)]
    create_table(vis/'SYSPOWER', power_rows[::-1], ['SWITCHED_DIFF', 'SWITCHED_SUM', 'REQUANTIZER_GAIN'])
    create_table(vis/'CALDEVICE', noise_rows, ['NOISE_CAL'])
    rebind_extensions(vis)


def read_calibration(path):
    main, spw = {}, {}
    with opened(path) as source:
        assert source.info()['subType'] == 'G EVLASWPOW'
        assert source.getkeyword('VisCal') == 'G EVLASWPOW'
        assert source.getkeyword('PolBasis') == 'unknown'
        assert source.getkeyword('ParType') == 'Float'
        assert not any(source.iscelldefined('WEIGHT', row) for row in range(source.nrows()))
        for name in ('TIME', 'FIELD_ID', 'SPECTRAL_WINDOW_ID', 'SCAN_NUMBER',
                     'OBSERVATION_ID', 'INTERVAL', 'ANTENNA1', 'ANTENNA2',
                     'FPARAM', 'FLAG', 'PARAMERR', 'SNR'):
            main[name] = source.getcol(name)
    with opened(path/'SPECTRAL_WINDOW') as windows:
        for name in ('NUM_CHAN', 'CHAN_FREQ', 'TOTAL_BANDWIDTH', 'CHAN_WIDTH', 'RESOLUTION',
                     'EFFECTIVE_BW', 'REF_FREQUENCY', 'FLAG_ROW'):
            spw[name] = windows.getcol(name)
    return main | spw


def compare_columns(actual, expected):
    assert actual.keys() == expected.keys()
    for name, values in expected.items():
        if name in ('FPARAM', 'CORRECTED_DATA', 'WEIGHT', 'WEIGHT_SPECTRUM'):
            np.testing.assert_allclose(actual[name], values, rtol=2e-6, atol=2e-7, err_msg=name)
        else:
            np.testing.assert_array_equal(actual[name], values, err_msg=name)


def check_parameter_laws(vis, calibration, mode):
    power = {}
    with opened(vis/'SYSPOWER') as table:
        for row in range(table.nrows()):
            key = tuple(table.getcell(name, row) for name in ('TIME', 'ANTENNA_ID'))
            power[key] = [table.getcell(name, row) for name in
                          ('SWITCHED_SUM', 'SWITCHED_DIFF', 'REQUANTIZER_GAIN')]
    with opened(vis/'CALDEVICE') as table:
        temperatures = dict(zip(table.getcol('ANTENNA_ID'), table.getcol('NOISE_CAL').T))
    efficiency = np.float32(.932)
    digital = np.sqrt(efficiency * np.float32(1.176))
    for row, key in enumerate(zip(calibration['TIME'], calibration['ANTENNA1'])):
        valid = key in power
        if valid:
            total, diff, rq = power[key]
            tcal = temperatures[key[1]]
            required = (rq,) if mode == 'rq' else (total, rq)
            if mode in ('swpow', 'evlagain', 'swp/rq'):
                required += (diff, tcal)
            valid = all(np.all(value > np.finfo(np.float32).eps) for value in required)
        expected = np.ones((2, 2), dtype=np.float32)
        if valid:
            if mode in ('rq', 'swpwts'):
                expected[:, 0] = rq * digital
                expected[:, 1] = (1.0 if mode == 'rq' else total / 2) / rq**2
            else:
                expected[:, 0] = np.sqrt(diff / tcal) / (rq if mode == 'swp/rq' else 1) * digital
                expected[:, 1] = tcal * total / diff / 2
        np.testing.assert_allclose(calibration['FPARAM'][:, 0, row], expected.ravel(),
                                   rtol=2e-6, atol=2e-7)
        np.testing.assert_array_equal(calibration['FLAG'][:, 0, row], not valid)


def rebind_extensions(path):
    # Calibration constructors follow table-valued keywords. Keep applications
    # independent of the generator's MeasurementSet and its table refs.
    with table(str(path), readonly=False, ack=False) as root:
        for extension in ('SYSPOWER', 'CALDEVICE'):
            with table(str(path/extension), ack=False) as subtable:
                root.putkeyword(extension, subtable)


def apply(root, vis, engines, tables, weighted, interp):
    expected = None
    combinations = 0
    for reader, api in engines.items():
        for writer, product in tables.items():
            work = root/f'apply-{reader}-{writer}.ms'
            copy_ms(vis, work)
            rebind_extensions(work)
            assert api.applycal(str(work), gaintable=[str(product)], interp=[interp],
                                calwt=weighted, flagbackup=False) is None
            with opened(work) as output:
                names = ['CORRECTED_DATA', 'WEIGHT', 'FLAG', 'FLAG_ROW', 'SIGMA']
                if 'WEIGHT_SPECTRUM' in output.colnames() and output.iscelldefined('WEIGHT_SPECTRUM', 0):
                    names.append('WEIGHT_SPECTRUM')
                columns = {name: output.getcol(name) for name in names}
            if expected is None:
                expected = columns
            else:
                compare_columns(columns, expected)
            shutil.rmtree(work)
            combinations += 1
    return combinations


def parameter_and_failure_controls(root, vis, engines):
    work = root/'controls.ms'
    copy_ms(vis, work)
    rebind_extensions(work)
    with table(str(work/'SYSPOWER'), readonly=False, ack=False) as power:
        times = power.getcol('TIME')
        power.putcol('TIME', times + np.where(times < times.max(), .2, 0.))
    with opened(work/'CALDEVICE') as source:
        noise_rows = [{name: source.getcell(name, row) for name in source.colnames()}
                      for row in range(source.nrows() - 1)]
    noise_rows[0]['NOISE_CAL'] = np.array([3.125], np.float32)
    # The casacore Python array has reversed axes relative to CASA's binding.
    noise_rows[1]['NOISE_CAL'] = np.array([[3.5, 10.5], [4.5, 11.5]], np.float32)
    noise_rows[1]['NUM_CAL_LOAD'] = 2
    replacement = work/'CALDEVICE-shapes'
    create_table(replacement, noise_rows, ['NOISE_CAL'], ndim=0)
    with table(str(work), readonly=False, ack=False) as main, table(str(replacement), ack=False) as noise:
        main.putkeyword('CALDEVICE', noise)
    expected = None
    for name, api in engines.items():
        output = root/f'mixed-noise-{name}.cal'
        assert api.gencal(str(work), str(output), 'swpow') is None
        actual = read_calibration(output)
        assert np.any(actual['FPARAM'][[1, 3], 0] > 20.)
        assert np.all(actual['FLAG'][:, :, actual['ANTENNA1'] == 4])
        # The sixth power sample is after the first scan's final visibility
        # but before the next scan. CASA dereferences an empty scan selection.
        gap = actual['TIME'] == np.unique(actual['TIME'])[5]
        assert np.count_nonzero(gap) == 5
        np.testing.assert_array_equal(actual['SCAN_NUMBER'][gap], 0 if name == 'casa' else -1)
        actual['SCAN_NUMBER'] = actual['SCAN_NUMBER'][~gap]
        if expected is None:
            expected = actual
        else:
            compare_columns(actual, expected)
        refused = root/f'missing-{name}.cal'
        try:
            api.gencal(str(work), str(refused), 'swpow', spw='999')
        except RuntimeError as error:
            assert 'Error in Calibrater::specifycal.' in str(error)
        else:
            raise AssertionError('invalid SPW accepted')
        assert not refused.exists()
    with table(str(work), readonly=False, ack=False) as main:
        main.removekeyword('CALDEVICE')
    for name, api in engines.items():
        refused = root/f'missing-{name}.cal'
        try:
            api.gencal(str(work), str(refused), 'swpow')
        except RuntimeError as error:
            assert 'Error in Calibrater::specifycal.' in str(error)
        else:
            raise AssertionError('missing CALDEVICE accepted')
        assert not refused.exists()
    rebind_extensions(work)
    with table(str(work/'SYSPOWER'), readonly=False, ack=False) as power:
        for name in ('SWITCHED_SUM', 'SWITCHED_DIFF', 'REQUANTIZER_GAIN'):
            for row in range(power.nrows()):
                power.putcell(name, row, power.getcell(name, row)[:1])
    if 'native' in engines:
        refused = root/'malformed-native.cal'
        try:
            engines['native'].gencal(str(work), str(refused), 'rq')
        except RuntimeError as error:
            assert 'must have two receptors' in str(error)
        else:
            raise AssertionError('malformed SYSPOWER accepted')
        assert not refused.exists()
    script = 'from casatasks import gencal; import sys; gencal(sys.argv[1], sys.argv[2], "rq")'
    refused = root/'malformed-casa.cal'
    completed = subprocess.run([sys.executable, '-c', script, str(work), str(refused)],
                               capture_output=True, timeout=30)
    assert completed.returncode == -signal.SIGABRT.value, completed.stderr.decode()
    assert not refused.exists()


def solver_preapply(root, vis, engines, prior):
    source = root/'preapply-input.ms'
    copy_ms(vis, source)
    rebind_extensions(source)
    rows = read_calibration(prior)
    gains = {(time, antenna): solution[[0, 2], 0]
             for time, antenna, solution in zip(rows['TIME'], rows['ANTENNA1'],
                                                rows['FPARAM'].transpose(2, 0, 1))}
    with opened(source, True) as target:
        data = target.getcol('DATA')
        data.fill(0.)
        for row, (time, left, right) in enumerate(zip(target.getcol('TIME'),
                                                     target.getcol('ANTENNA1'),
                                                     target.getcol('ANTENNA2'))):
            # A nonseparable residual keeps CASA's chi-squared positive.
            perturbation = 1.0 + 2e-4 * np.cos(2 * left + right) + 1e-4j * np.sin(left * right)
            for correlation, receptor in ((0, 0), (3, 1)):
                data[correlation, :, row] = (gains[time, left][receptor]
                                             * gains[time, right][receptor] * perturbation)
        target.putcol('DATA', data)
    expected = None
    for name, api in engines.items():
        work = root/f'preapply-{name}.ms'
        copy_ms(source, work)
        rebind_extensions(work)
        output = root/f'preapply-{name}.cal'
        result = api.gaincal(str(work), str(output), field='PHASE', solint='int',
                             refant='A3', minsnr=0., minblperant=2,
                             gaintable=[str(prior)], interp=['nearest'])
        assert isinstance(result, dict)
        with opened(output) as solution:
            values = solution.getcol('CPARAM')
            flags = solution.getcol('FLAG')
            assert values.size == 60 and not flags.any()
            np.testing.assert_allclose(values, 1.0, atol=4e-4)
            antennas = solution.getcol('ANTENNA1')
        if expected is None:
            expected = antennas, values
        else:
            np.testing.assert_array_equal(antennas, expected[0])
            np.testing.assert_allclose(values, expected[1], atol=1e-4)


def protect_external_input_tables(root, vis, native):
    work = root/'external-inputs.ms'
    copy_ms(vis, work)
    external = []
    with table(str(work), readonly=False, ack=False) as main:
        for name in ('SYSPOWER', 'CALDEVICE'):
            path = root/f'external-{name}'
            shutil.copytree(vis/name, path)
            with table(str(path), ack=False) as linked:
                main.putkeyword(name, linked)
            external.append(path)
    output = root/'report-overlap.cal'
    for path in external:
        header = path/'table.dat'
        before = header.read_bytes()
        try:
            native.gencal(str(work), str(output), 'rq', output=str(header), overwrite=True)
        except (RuntimeError, ValueError) as error:
            assert 'report path overlaps a scientific product' in str(error)
        else:
            raise AssertionError('report overwrote a linked input table')
        assert header.read_bytes() == before
        assert not output.exists()


def refused_products(root, vis, engines, prior):
    work = root/'missing-column.ms'
    copy_ms(vis, work)
    rebind_extensions(work)
    with table(str(work/'CALDEVICE'), readonly=False, ack=False) as noise:
        noise.removecols('NUM_CAL_LOAD')
    for name, api in engines.items():
        output = root/f'preserve-{name}.cal'
        copy_ms(prior, output)
        original = read_calibration(output)
        try:
            api.gencal(str(work), str(output), 'rq')
        except RuntimeError as error:
            assert 'Error in Calibrater::specifycal.' in str(error)
        else:
            raise AssertionError('generation replaced a table with malformed CALDEVICE')
        compare_columns(read_calibration(output), original)

    for name, api in engines.items():
        work = root/f'dangling-{name}.ms'
        copy_ms(vis, work)
        rebind_extensions(work)
        (work/'CALDEVICE').rename(work/'unlinked-CALDEVICE')
        with opened(work) as main:
            original = {key: main.getcol(key) for key in ('DATA', 'FLAG', 'WEIGHT')}
        try:
            api.applycal(str(work), gaintable=[str(prior)], calwt=True, flagbackup=False)
        except RuntimeError:
            pass
        else:
            raise AssertionError('application accepted a dangling CALDEVICE reference')
        with opened(work) as main:
            compare_columns({key: main.getcol(key) for key in original}, original)


def selected_correlation(root, vis, correlation):
    output = root/f'sole-{correlation}.ms'
    assert casa.mstransform(str(vis), str(output), correlation=correlation, datacolumn='data') is None
    for name in ('SYSPOWER', 'CALDEVICE'):
        if not (output/name).exists():
            shutil.copytree(vis/name, output/name)
    rebind_extensions(output)
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', required=True, type=Path)
    parser.add_argument('--reference-only', action='store_true')
    args = parser.parse_args()
    assert casa.version_string() == '6.7.6.14'
    root = args.work.resolve()
    root.mkdir()
    vis = root/'vla.ms'
    seed(vis)
    engines = {'casa': casa}
    if not args.reference_only:
        from casars import tasks as native
        engines['native'] = native
    applications = 0
    for mode in MODES:
        name = mode.replace('/', '-')
        expected = None
        tables = {}
        for writer, api in engines.items():
            path = root/f'{writer}-{name}.cal'
            tables[writer] = path
            assert api.gencal(str(vis), str(path), mode) is None
            result = read_calibration(path)
            assert result['TIME'].size == 60
            check_parameter_laws(vis, result, mode)
            if expected is None:
                expected = result
            else:
                compare_columns(result, expected)
            assert api.gencal(str(vis), str(path), mode, antenna='A1', pol='R',
                              spw='0:0~2', parameter=[100.], uniform=False) is None
            compare_columns(read_calibration(path), result)
        for interpolation in ('nearest', 'linear'):
            for weighted in (False, True):
                applications += apply(root, vis, engines, tables, weighted, interpolation)
        if mode == 'swpow':
            solver_preapply(root, vis, engines, tables['casa'])
            spectral = root/'spectral-weights.ms'
            copy_ms(vis, spectral)
            rebind_extensions(spectral)
            assert casa.initweights(str(spectral), wtmode='ones', dowtsp=True) is None
            applications += apply(root, spectral, engines, tables, True, 'nearest')
            with opened(spectral, True) as main:
                main.putcol('TIME', main.getcol('TIME') + 1.7)
            applications += apply(root, spectral, engines, tables, True, 'linear')
            for correlation in ('LL', 'LR'):
                selected = selected_correlation(root, vis, correlation)
                applications += apply(root, selected, engines, tables, True, 'nearest')
            refused_products(root, vis, engines, tables['casa'])
        print(mode, 'table and apply/weights passed', flush=True)
    parameter_and_failure_controls(root, vis, engines)
    if 'native' in engines:
        protect_external_input_tables(root, vis, engines['native'])
    print(f'{len(MODES)} switched-power modes and {applications} applications passed, '
          'plus solver preapply, mixed noise calibration, asynchronous times and refusal controls.')


if __name__ == '__main__':
    main()
