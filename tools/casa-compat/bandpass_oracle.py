#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare ordinary B solutions and their formal errors with pinned CASA."""

import argparse
import json
import os
from pathlib import Path

import casatasks as casa
from casatools import table
import numpy as np

from applycal_oracle import copy_ms
from calibration_lifecycle_oracle import contents
from gaincal_oracle import relative_rms
from record_casa_returns import make_cal_ms


def source_data(path):
    make_cal_ms(str(path), channels=12)
    rng = np.random.default_rng(719)
    tool = table()
    tool.open(str(path), nomodify=False)
    try:
        data = tool.getcol('DATA')
        a, b = (tool.getcol(key) for key in ('ANTENNA1', 'ANTENNA2'))
        ch = np.arange(data.shape[1])[:, None]
        for receptor in range(data.shape[0]):
            gain_a = (1 + .07*a + .03*ch) * np.exp(1j * (.03+.012*ch) * a)
            gain_b = (1 + .07*b + .03*ch) * np.exp(1j * (.03+.012*ch) * b)
            noise = rng.normal(size=data.shape[1:]) + 1j*rng.normal(size=data.shape[1:])
            data[receptor] = gain_a * gain_b.conj() * (1.+.12*receptor) + .035*noise
        tool.putcol('DATA', data)
        tool.putcol('FLAG', np.zeros(data.shape, dtype=bool))
        tool.putcol('FLAG_ROW', np.zeros(len(a), dtype=bool))
    finally:
        tool.close()


def gapped_data(source, target):
    copy_ms(source, target)
    tool = table()
    tool.open(str(target), nomodify=False)
    try:
        data = tool.getcol('DATA')
        a, b = (tool.getcol(key) for key in ('ANTENNA1', 'ANTENNA2'))
        # The bounded single-channel gap crosses the phase wrap for A4.
        data *= np.exp(.24j * np.arange(data.shape[1])[:, None] * (a-b))
        tool.putcol('DATA', data)
        flags = tool.getcol('FLAG')
        flags[:, [0, 3, 6, 7, 11]] = True
        tool.putcol('FLAG', flags)
    finally:
        tool.close()


def compare(root, source, native, normalize, fillgaps=0, remaining_flags=None):
    products = []
    for name, engine in [('casa', casa), ('native', native)]:
        ms, output = root/f'{name}.ms', root/f'{name}.B'
        copy_ms(source, ms)
        engine.bandpass(str(ms), str(output), field='0', refant='A0',
                        smodel=[1.], minsnr=0., solnorm=normalize, fillgaps=fillgaps)
        products.append(contents(output))
    expected, actual = products
    for column in ('FLAG', 'ANTENNA1', 'ANTENNA2', 'FIELD_ID', 'SPECTRAL_WINDOW_ID'):
        np.testing.assert_array_equal(actual[column], expected[column], err_msg=column)
    np.testing.assert_allclose(actual['TIME'], expected['TIME'], rtol=0., atol=1e-4)
    if remaining_flags is None:
        assert np.all(expected['PARAMERR'][~expected['FLAG']] > 0.)
    else:
        np.testing.assert_array_equal(np.flatnonzero(actual['FLAG'].all(axis=(0, 2))), remaining_flags)
    return {key: relative_rms(actual[key], expected[key]) for key in ('CPARAM', 'PARAMERR', 'SNR')}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--native-bin', type=Path, required=True)
    args = parser.parse_args()
    assert casa.version_string() == '6.7.6.14'
    os.environ['CASARS_CALIBRATE_BIN'] = str(args.native_bin.resolve())
    os.environ['CASARS_REQUIRE_TASK_BINARIES'] = '1'
    from casars import tasks as native

    root = args.work.resolve()
    root.mkdir(parents=True)
    source = root/'source.ms'
    source_data(source)
    gapped = root/'gapped.ms'
    gapped_data(source, gapped)
    results = {}
    for label, normalize in [('standard', False), ('normalized', True)]:
        folder = root/label
        folder.mkdir()
        results[label] = compare(folder, source, native, normalize)
    for gaps, flags in [(-1, [0, 3, 6, 7, 11]), (1, [0, 6, 7, 11]), (2, [0, 11])]:
        folder = root/f'fill-{gaps}'
        folder.mkdir()
        results[folder.name] = compare(folder, gapped, native, False, gaps, flags)
    (root/'results.json').write_text(json.dumps(results, indent=2)+'\n')
    for name, report in results.items():
        assert max(report.values()) < 1e-3, (name, report)


if __name__ == '__main__':
    main()
