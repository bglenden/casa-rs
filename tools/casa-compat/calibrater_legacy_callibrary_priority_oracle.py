#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare the legacy calibrater's competing calibration-library slices."""

import argparse
from contextlib import chdir
from pathlib import Path

import casatasks
from casatools import calibrater as casa_calibrater
import numpy as np

from blcal_oracle import copy_ms, source
from calibration_smoothcal_oracle import opened
from calibrater_apply_tool_oracle import same_record


def fixture(work):
    vis = source(work)
    cal = work/'field-gains.cal'
    casatasks.gencal(vis=str(vis), caltable=str(cal), caltype='amp', parameter=[1.2])
    with opened(cal, True) as table:
        count = table.nrows()
        columns = ('TIME', 'INTERVAL', 'FIELD_ID', 'SPECTRAL_WINDOW_ID', 'ANTENNA1',
                   'ANTENNA2', 'SCAN_NUMBER', 'OBSERVATION_ID', 'CPARAM', 'FLAG', 'PARAMERR', 'SNR')
        rows = {key: table.getcol(key) for key in columns}
        table.addrows(count)
        for field, gain in enumerate((1.2, 1.5)):
            for name, values in rows.items():
                if name == 'FIELD_ID':
                    values = np.full_like(values, field)
                elif name == 'CPARAM':
                    values = np.full_like(values, gain)
                assert table.putcol(name, values, startrow=field*count, nrow=count)
    with opened(vis, True) as table:
        field, time = (table.getcol(k) for k in ('FIELD_ID', 'TIME'))
        scan = np.where((field == 0) & (time < np.median(time[field == 0])), 1, 2)
        assert table.putcol('SCAN_NUMBER', scan.astype('int32'))
    return vis, cal


def run_case(factory, vis, library, legacy, operation, selection):
    with opened(vis, True) as table:
        data = table.getcol('DATA')
        flags, flag_row, weights = (table.getcol(k) for k in ('FLAG', 'FLAG_ROW', 'WEIGHT'))
        assert table.putcol('CORRECTED_DATA', data)
        assert table.putcol('MODEL_DATA', np.ones_like(data))
    tool = factory()
    assert tool.setvi(legacy, True) and tool.open(str(vis), addmodel=True)
    assert tool.selectvis(**selection) and tool.setcallib(library)
    assert (tool.corrupt() if operation == 'corrupt' else tool.correct('calflag'))
    activity = tool.activityrec()
    assert tool.close()
    column = 'MODEL_DATA' if operation == 'corrupt' else 'CORRECTED_DATA'
    with opened(vis) as table:
        field, scan = (table.getcol(k) for k in ('FIELD_ID', 'SCAN_NUMBER'))
        actual = table.getcol(column)
        np.testing.assert_array_equal(table.getcol('FLAG'), flags)
        np.testing.assert_array_equal(table.getcol('FLAG_ROW'), flag_row)
        np.testing.assert_array_equal(table.getcol('WEIGHT'), weights)
    return activity, actual, data, field, scan


def experiment(work, reference_only):
    assert casatasks.version_string() == '6.7.6.14'
    work.mkdir()
    factories = {'casa': casa_calibrater}
    if not reference_only:
        from casars.tools import calibrater as native_calibrater

        factories['native'] = native_calibrater
    with chdir(work):
        vis, cal = fixture(work)
        datasets = {'casa': vis}
        if not reference_only:
            datasets['native'] = work/'native.ms'
            copy_ms(vis, datasets['native'])
        for legacy in (True, False):
            for operation, selection, explicit_field in (
                ('corrupt', {'field': 'FLUX'}, False),
                ('corrupt', {'scan': '2'}, False),
                ('corrupt', {'field': 'PHASE'}, False),
                ('corrupt', {'field': 'FLUX'}, True),
                ('corrupt', {'field': 'PHASE'}, True),
                ('correct', {}, False),
                ('correct', {'scan': '2'}, False),
            ):
                slices = [(1, 0, 1.2), (2, 0, 1.5)]
                if explicit_field:
                    slices.append((2, 1, 1.5))
                for ordered in (slices, list(reversed(slices))):
                    mixed_reference = (legacy and operation == 'corrupt' and explicit_field
                                       and selection.get('field') == 'FLUX')
                    table = {}
                    for index, (scan, field, gain) in enumerate(ordered):
                        options = {'tinterp': 'linear', 'scan': str(scan), 'calwt': False,
                                   'fldmap': [0, 0] if gain == 1.2 else [1, 1]}
                        if explicit_field:
                            options['field'] = str(field)
                        table[str(index)] = options
                    outputs = {}
                    for engine, factory in factories.items():
                        activity, actual, data, field, scan = run_case(
                            factory, datasets[engine], {str(cal): table}, legacy, operation, selection)
                        observed = np.ones_like(data) if operation == 'corrupt' else data.copy()
                        chosen = ((field == 0) if selection.get('field') == 'FLUX' else
                                  (field == 1) if selection.get('field') == 'PHASE' else
                                  (scan == 2) if selection.get('scan') == '2' else
                                  np.ones_like(field, dtype=bool))
                        gain = np.where(scan == 1, 1.2, 1.5)
                        if legacy and operation == 'corrupt' and np.any(chosen & (scan == 1)):
                            gain[field == 0] = 1.2
                        scale = gain[chosen]**2
                        observed[..., chosen] *= scale if operation == 'corrupt' else 1./scale
                        if mixed_reference and engine == 'casa':
                            np.testing.assert_allclose(np.unique(np.round(actual[..., chosen].real, 4)),
                                                       [1., 1.2, 1.44], atol=1e-6, rtol=0.)
                        else:
                            np.testing.assert_allclose(actual, observed, atol=4e-7, rtol=4e-7,
                                                       err_msg=str((legacy, operation, selection, ordered, engine)))
                        outputs[engine] = activity, actual
                    if 'native' in outputs:
                        same_record(outputs['native'][0], outputs['casa'][0])
                        if not mixed_reference:
                            np.testing.assert_allclose(outputs['native'][1], outputs['casa'][1],
                                                       atol=4e-7, rtol=4e-7)
                    print(legacy, operation, selection, explicit_field, ordered[0][:2],
                          'reference' if reference_only else 'matched', flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=lambda path: Path(path).resolve())
    parser.add_argument('--reference-only', action='store_true')
    args = parser.parse_args()
    experiment(args.work, args.reference_only)
