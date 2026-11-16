#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Installed CASA C++ reads native time-calibrated MSs and consumes their tables."""
import argparse
import gzip
import json
from pathlib import Path
import numpy as np
import casatasks
from casatools import simulator, table
from sm_apply_time_oracle import FACTORS, OFFSETS
from sm_apply_parameters_oracle import initialize, receptor_values
from sm_apply_parameters_readback import compare
from sm_multiple_models_oracle import make_session
from sm_apply_time_modes_oracle import apply_partitioned, observation_rows


def check_table(path, case):
    tb = table()
    tb.open(str(path))
    try:
        calibration = case['calibration']
        n = calibration['rows']
        assert tb.nrows() == n
        kind = case['kind']
        receptors, channels = calibration['parameters'], calibration['channels']
        assert tb.getkeyword('VisCal') == tb.info()['subType'] == kind + ' Jones'
        values, flags = tb.getcol('CPARAM'), tb.getcol('FLAG')
        assert values.shape == flags.shape == (receptors, channels, n)
        times, antennas = tb.getcol('TIME'), tb.getcol('ANTENNA1')
        assert set(times) == set(calibration['times_s']) and set(antennas) == set(range(8))
        for column in ('INTERVAL', 'FIELD_ID', 'SPECTRAL_WINDOW_ID'):
            np.testing.assert_array_equal(tb.getcol(column), np.zeros(n))
        for row, (time, antenna) in enumerate(zip(times, antennas)):
            original = time + (0 if case['probe'] == 'short2_exact' else 5)
            index = calibration['original_times_s'].index(original)
            leakage = kind in ('D', 'Df')
            base = receptor_values('D' if leakage else 'G', int(antenna), 0)
            expected = np.empty((receptors, channels), dtype=np.complex64)
            for receptor in range(receptors):
                for channel in range(channels):
                    if case['probe'] == 'analytic':
                        value = (1, 2, .5, 1.25)[index]
                    elif leakage:
                        value = base[receptor] + OFFSETS[index] + .01 * channel
                    else:
                        value = base[receptor] * FACTORS[index] * (1 + .05 * channel) * np.exp(.03j * channel)
                    expected[receptor, channel] = value
            np.testing.assert_array_equal(values[:, :, row], expected)
            expected_flags = np.zeros((receptors, channels), dtype=bool)
            if case['probe'] == 'flag' and antenna == 0 and index == 0:
                expected_flags[0, 1 if channels > 1 else 0] = True
            np.testing.assert_array_equal(flags[:, :, row], expected_flags)
    finally:
        tb.close()


def consume(path, case, out):
    out.mkdir(parents=True)
    output = out / 'casa.ms'
    sm = make_session(output, stokes='XX XY YX YY')
    sm.close()
    initialize(output)
    if case.get('segment') == 'split' and 'perobs' in case['mode']:
        observation_rows(output / 'OBSERVATION', 2)
        tb = table(); tb.open(str(output), nomodify=False)
        tb.putcol('OBSERVATION_ID', tb.getcol('FIELD_ID')); tb.close()
    sm = simulator()
    assert sm.openfromms(str(output))
    # CASA crashes on spline setup. Verify the original native knot table
    # through its separately retained nearest-CASA control, never the sampled
    # spline control used to judge native spline calculations.
    interp = 'nearest,nearest' if case['mode'] == 'spline' else case['interp']
    if 'perscan' in case['mode'] and case.get('segment') not in ('unset', None):
        apply_partitioned(sm, output, path.resolve(), case, out)
    else:
        assert sm.setapply(table=str(path.resolve()), interp=interp, calwt=False)
        assert sm.corrupt()
    sm.close()
    reference = {**case, 'rows': case.get('casa_nearest_rows', case['rows'])}
    return compare(output, reference)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--fixture', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    assert casatasks.version_string() == '6.7.6.14'
    fixture = json.load(gzip.open(args.fixture, 'rt'))
    cases = {case['name']: case for case in fixture['cases'] if not case.get('refusal')}
    assert len(cases) in (34, 21)
    expected = {(name, workers) for name in cases for workers in (1, 3)}
    seen, reports = set(), []
    args.out.mkdir(parents=True, exist_ok=True)
    for manifest in sorted(args.root.glob('*/case.json')):
        run = json.loads(manifest.read_text())
        key = run['name'], run['workers']
        assert key in expected and key not in seen, key
        seen.add(key)
        case = cases[run['name']]
        nrms = compare(manifest.parent / 'applied.ms', case)
        check_table(manifest.parent / 'terms.cal', case)
        report = {**run, 'native_ms_cpp_nrms': nrms}
        if run['workers'] == 1:
            report['casa_consumes_native_table_nrms'] = consume(manifest.parent / 'terms.cal', case, args.out / case['name'])
        reports.append(report)
    assert seen == expected, ('missing', sorted(expected - seen))
    (args.out / 'report.json').write_text(json.dumps({'casa_version':casatasks.version_string(), 'runs':reports}, indent=1, allow_nan=False) + '\n')
    print(f'C++ readback: {len(reports)} native MS/table pairs; {len(cases)} native tables consumed', flush=True)


if __name__ == '__main__':
    main()
