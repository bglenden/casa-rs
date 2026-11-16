#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Custom test-ramp intervals: actual CASA iterator times and source formula.

CASA6.7.6.14 initializes the test corruptor before its slot times exist.
Evaluate its explicit formula through CASA ATM after obtaining sizeUpSim's
chunks from the installed CASA VisIterator, preserving scan/field/SPW boundaries.
"""
import argparse
import json
from pathlib import Path
import tempfile

import numpy as np
from sm_multiple_models_oracle import make_session
from troposphere_stats import at_tool_wet_delays


COLUMNS = ['ARRAY_ID', 'OBSERVATION_ID', 'SCAN_NUMBER', 'FIELD_ID', 'DATA_DESC_ID', 'TIME']


def chunks(path, interval):
    from casatools import ms
    tool = ms(); assert tool.open(str(path))
    assert tool.iterinit(columns=COLUMNS, interval=interval, adddefaultsortcolumns=False)
    assert tool.iterorigin()
    result = []
    while True:
        values = tool.getdata(['time', 'field_id', 'data_desc_id'])
        times = values['time']
        result.append(dict(start=float(times.min()), time_s=float((times.min() + times.max()) / 2),
                           field=int(values['field_id'][0]), window=int(values['data_desc_id'][0])))
        if not tool.iternext():
            break
    tool.iterend(); tool.close()
    return result


def record(out):
    frequencies = 230e9 + np.arange(4) * 15.625e6
    dispersive, continuum = at_tool_wet_delays(0.01, frequencies)
    cases = []
    with tempfile.TemporaryDirectory() as work:
        path = Path(work) / 'data.ms'
        sm = make_session(path, 'XX XY YX YY')
        # make_session's CASA hour-angle offset is 37 s; native times are 40..140 s.
        assert sm.observe(sourcename='target', spwname='band6', starttime='3s', stoptime='103s')
        original = bool(sm.settrop(mode='test', pwv=0.01, simint=25.0))
        sm.close()
        assert original is False
        for interval in [0.05, 5., 10., 15., 20., 25., 1e6]:
            solutions = chunks(path, interval)
            slot_times = np.array([s['start'] for s in solutions])
            count = len(solutions)
            for solution in solutions:
                slot = int(np.argmin(np.abs(slot_times - solution['time_s'])))
                excess = (np.float32(slot / count) + np.arange(8, dtype=np.float32) / np.float32(8))
                excess *= np.float32(0.01) * np.float32(10.)
                gains = np.exp(1j * excess[:, None].astype(float) * (dispersive + continuum))
                solution['gains'] = np.stack((gains.real, gains.imag), axis=-1).tolist()
            cases.append(dict(simint_s=interval, solutions=solutions))
    Path(out).write_text(json.dumps(dict(casa_version='6.7.6.14', original=original, cases=cases), indent=2) + '\n')
    print('CASA iterator/source-ATM reference:', [(c['simint_s'], len(c['solutions'])) for c in cases], flush=True)


def verify_native(root):
    from casatools import simulator, table
    specifications = sorted(Path(root).glob('*/case.json'))
    assert len(specifications) == 7, len(specifications)
    for specification in specifications:
        path, calibration = specification.parent / 'data.ms', specification.parent / 'trop.cal'
        tb = table(); tb.open(str(calibration))
        gains = {(float(tb.getcell('TIME', r)), int(tb.getcell('ANTENNA1', r))):
                 tb.getcell('CPARAM', r)[0] for r in range(tb.nrows())}
        tb.close()
        times = sorted({t for t, _ in gains})
        tb.open(str(path)); expected = []
        for row in range(tb.nrows()):
            time = float(tb.getcell('TIME', row))
            nearest = min(times, key=lambda t: abs(t - time))
            p, q = int(tb.getcell('ANTENNA1', row)), int(tb.getcell('ANTENNA2', row))
            expected.append(tb.getcell('DATA', row) * gains[nearest, p] * gains[nearest, q].conj())
        tb.close()
        sm = simulator(); assert sm.openfromms(str(path))
        assert sm.setapply(table=str(calibration), interp='nearest')
        assert sm.corrupt(); sm.close()
        tb.open(str(path)); actual = np.asarray([tb.getcell('DATA', r) for r in range(tb.nrows())]); tb.close()
        expected = np.asarray(expected)
        residual = float(np.linalg.norm(actual - expected) / np.linalg.norm(expected))
        assert residual < 1e-3, (specification, residual)
        print('CASA consumed native custom test-ramp table:', specification.read_text().strip(), 'NRMS', residual, flush=True)


def main():
    import casatasks
    assert casatasks.version_string() == '6.7.6.14'
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--out')
    group.add_argument('--verify-native')
    args = parser.parse_args()
    if args.out:
        record(args.out)
    else:
        verify_native(args.verify_native)


if __name__ == '__main__':
    main()
