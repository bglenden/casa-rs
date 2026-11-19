#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Exercise reference selection, continuity, and table readback against CASA."""

import argparse
import inspect
import os
import subprocess
import sys
from pathlib import Path

import casatasks as casa
import numpy as np

from calibration_smoothcal_oracle import (
    apply_round_trip,
    columns,
    compare_mapping,
    copy_ms,
    fixture,
    opened,
    synthetic_table,
)


def reference_fixture(vis, path, subtype, *, real=False, width=2, channels=1,
                      single_polarization=False):
    synthetic_table(vis, path, subtype, real, width)
    with opened(path, True) as t:
        times = t.getcol('TIME')
        t.addrows(24)
        antenna = np.tile(np.arange(5, dtype=np.int32), times.size)
        slot = np.repeat(np.arange(times.size), 5)
        shape = (width, channels, antenna.size)
        t.putcol('TIME', times[slot])
        t.putcol('ANTENNA1', antenna)
        t.putcol('ANTENNA2', np.full(antenna.size, 4, dtype=np.int32))
        t.putcol('INTERVAL', np.full(antenna.size, 512.))
        for name in ('FIELD_ID', 'SPECTRAL_WINDOW_ID', 'SCAN_NUMBER', 'OBSERVATION_ID'):
            t.putcol(name, np.zeros(antenna.size, dtype=np.int32))
        pol, chan, row = np.indices(shape)
        flags = (slot == 3) | ((antenna == 0) & np.isin(slot, [1, 2]))
        flags |= (antenna == 1) & (slot == 1)
        flags = np.broadcast_to(flags, shape).copy()
        if channels > 1:
            flags |= (chan == 1) & (antenna == 3) & (slot == 1)
        if single_polarization:
            flags[width // 2:] = True
        data = .15 * pol + .3 * chan + .4 * antenna + .16 * slot
        if subtype == 'Fringe Jones':
            data = np.where(pol % 4 == 2, data * 1e-14, data).astype(np.float32)
        elif subtype == 'D Jones':
            data = (.05 * data + .15j * data).astype(np.complex64)
        else:
            data = ((1. + .1 * antenna + .03 * pol) * np.exp(1j * data)).astype(np.complex64)
            if channels > 1:
                data[:, 1, 0] = 0.
        t.putcol('FLAG', flags)
        t.putcol('SNR', (100. + pol + row).astype(np.float32))
        t.putcol('PARAMERR', (.1 + .01 * pol + .001 * row).astype(np.float32))
        t.putcol('FPARAM' if real else 'CPARAM', data)


def compare(root, native, vis, source, name, reference, mode=None, *, alias=False,
            positional=False, expected_references=None):
    products = []
    for engine, api in (('casa', casa), ('native', native)):
        target = root/f'{engine}-{name}.cal'
        if alias:
            copy_ms(source, target)
            source_name, target_name = str(target), ''
        else:
            source_name, target_name = str(source), str(target)
        if positional:
            result = api.rerefant(str(vis), source_name, target_name, mode, reference)
        else:
            options = {} if mode is None else {'refantmode': mode}
            result = api.rerefant(vis=str(vis), tablein=source_name, caltable=target_name,
                                  refant=reference, **options)
        assert result is None
        products.append(columns(target))
    compare_mapping(products[1], products[0], False)
    if expected_references is not None:
        expected = np.repeat(expected_references, 5)
        np.testing.assert_array_equal(products[1]['ANTENNA2'], expected)
        np.testing.assert_array_equal(products[1]['FLAG'][..., expected == 4], True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--native-bin', type=Path, required=True)
    args = parser.parse_args()
    root = args.work.resolve()
    root.mkdir()
    os.environ['CASARS_CALIBRATE_BIN'] = str(args.native_bin.resolve())
    os.environ['CASARS_REQUIRE_TASK_BINARIES'] = '1'
    from casars import tasks as native
    assert casa.version_string() == '6.7.6.14'
    assert list(inspect.signature(casa.rerefant).parameters.values()) == [
        p.replace(annotation=inspect.Parameter.empty)
        for p in inspect.signature(native.rerefant).parameters.values()
    ]
    vis, gains = root/'source.ms', root/'gains.cal'
    fixture(vis, root/'solved.cal')
    reference_fixture(vis, gains, 'G Jones')
    compare(root, native, vis, gains, 'ranked-flex', 'A0,A1,A3',
            expected_references=[0, 3, 3, 4, 3, 0])
    compare(root, native, vis, gains, 'strict-recovery', '0,1,3', 'strict', positional=True,
            expected_references=[0, 4, 4, 4, 0, 0])
    compare(root, native, vis, gains, 'nearest-fallback', 'A0', 'flex')
    compare(root, native, vis, gains, 'alias', 'A0,A3', alias=True,
            expected_references=[0, 3, 3, 4, 3, 0])
    for name, subtype, real, width, channels, single in [
        ('bandpass', 'B Jones', False, 2, 8, False),
        ('single-pol', 'B Jones', False, 2, 8, True),
        ('leakage', 'D Jones', False, 2, 1, False),
        ('tscalar', 'T Jones', False, 1, 1, False),
        ('fringe', 'Fringe Jones', True, 8, 1, False),
        ('fringe-single-pol', 'Fringe Jones', True, 8, 1, True),
    ]:
        source = root/f'{name}.cal'
        reference_fixture(vis, source, subtype, real=real, width=width, channels=channels,
                          single_polarization=single)
        compare(root, native, vis, source, name, 'A0,A1,A3', 'flex',
                expected_references=[0, 3, 3, 4, 3, 0])
    for name, ref, mode in [('empty', '', 'flex'), ('missing', 'missing-antenna', 'strict'),
                            ('bad-mode', 'A0', 'bad')]:
        for api, label in ((casa, 'casa'), (native, 'native')):
            output = root/f'{label}-invalid-{name}.cal'
            try:
                api.rerefant(str(vis), str(gains), str(output), mode, ref)
            except (AssertionError, RuntimeError, ValueError):
                assert not output.exists()
            else:
                raise AssertionError(f'{label} accepted {name} reference configuration')
    delay = root/'delay.cal'
    synthetic_table(vis, delay, 'K Jones', True, 2)
    output = root/'unsupported-delay.cal'
    probe = subprocess.run([
        sys.executable, '-c',
        'import resource,sys; resource.setrlimit(resource.RLIMIT_CORE,(0,0)); '
        'from casatasks import rerefant; rerefant(*sys.argv[1:])',
        str(vis), str(delay), str(output), 'strict', 'A0',
    ], capture_output=True, timeout=45)
    assert probe.returncode in (-6, -11), probe.stderr.decode()
    try:
        native.rerefant(str(vis), str(delay), str(output), 'strict', 'A0')
    except RuntimeError:
        assert not output.exists()
    else:
        raise AssertionError('native rerefant accepted an unsupported delay table')
    compare(root, native, vis, root/'solved.cal', 'apply-pair', 'A0,A1')
    apply_round_trip(root, native, vis, 'apply-pair')
    print('15 paired CASA rerefant case groups and four downstream applications')


if __name__ == '__main__':
    main()
