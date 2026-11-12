#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA reads native simulated DATA and consumes supplied physical error tables."""
import argparse
import gzip
import json
from pathlib import Path
import numpy as np
from casatools import table, simulator
from casatasks import version_string
from sm_apply_parameters_oracle import initialize
from sm_apply_parameters_readback import compare
from sm_multiple_models_oracle import make_session


def check_table(path, case):
    tb = table(); tb.open(str(path))
    try:
        calibration = case['calibration']
        assert tb.nrows() == calibration['rows']
        assert tb.info()['subType'] == tb.getkeyword('VisCal') == calibration['kind']
        values, flags = tb.getcol('FPARAM'), tb.getcol('FLAG')
        assert values.shape == flags.shape == tuple(calibration['shape']) + (calibration['rows'],)
        assert np.isfinite(values).all() and not flags.any()
        for row, antenna in enumerate(tb.getcol('ANTENNA1')):
            if case['caltype'] == 'sbd':
                endpoint = int(tb.getcol('FIELD_ID')[row])
                expected = case.get('time_values', [case['parameter']])[endpoint] if antenna == 0 else [0., 0.]
            elif case['caltype'] == 'antpos':
                expected = case['parameter'] if antenna == 0 else [0., 0., 0.]
            else:
                expected = case.get('values', case['parameter'])
            np.testing.assert_array_equal(values[:, 0, row], np.array(expected, dtype=np.float32))
    finally:
        tb.close()


def consume(directory, case, output):
    sm = make_session(output, stokes='XX XY YX YY'); sm.close(); initialize(output)
    sm = simulator(); assert sm.openfromms(str(output))
    if case.get('leakage'):
        assert sm.setapply(table=str((directory / 'leakage.cal').resolve()), interp='linear', calwt=False)
    assert sm.setapply(table=str((directory / 'terms.cal').resolve()), interp='linear', calwt=False, opacity=case.get('opacity', 0.))
    assert sm.corrupt(); sm.close()
    return compare(output, case)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--fixture', type=Path, required=True)
    args = parser.parse_args(); assert version_string() == '6.7.6.14'
    cases = {case['name']: case for case in json.load(gzip.open(args.fixture, 'rt'))['cases']}
    expected = {(name, workers) for name in cases for workers in (1, 3)}
    args.out.mkdir(parents=True, exist_ok=True); seen = set(); reports = []
    for manifest in sorted(args.root.glob('*/case.json')):
        run = json.loads(manifest.read_text()); key = run['name'], run['workers']
        assert key in expected and key not in seen; seen.add(key)
        case = cases[run['name']]; directory = manifest.parent
        check_table(directory / 'terms.cal', case)
        report = {**run, 'native_ms_cpp_nrms': compare(directory / 'data.ms', case)}
        if run['workers'] == 1:
            report['native_table_consumer_nrms'] = consume(directory, case, args.out / (run['name'] + '.ms'))
        reports.append(report)
    assert seen == expected, ('missing', sorted(expected - seen))
    (args.out / 'report.json').write_text(json.dumps(dict(casa_version=version_string(), runs=reports), indent=1, allow_nan=False) + '\n')
    print('CASA physical-table interoperability passed', len(reports), flush=True)


if __name__ == '__main__':
    main()
