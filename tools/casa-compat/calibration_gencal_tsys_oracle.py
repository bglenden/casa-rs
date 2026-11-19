#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare scalar and spectral SYSCAL import and application with pinned CASA."""

import argparse
import json
import os
from pathlib import Path
import shutil

import casatasks as casa
from casatools import table
import numpy as np

from applycal_oracle import copy_ms
from calibration_gencal_gaincurve_oracle import protects_prior_input, refuses, retune
from calibration_gencal_specify_oracle import apply_both, columns, compare_tables, generate


def attach_syscal(vis, raw, *, spectrum, channel_override=None, destination=None):
    times = np.unique(columns(vis, ('TIME',))['TIME'])
    channels = int(columns(vis / 'SPECTRAL_WINDOW', ('NUM_CHAN',))['NUM_CHAN'][0])
    sample_times = (times[0], times[1], times[-1])
    antennas = (1, 3, 7, 27)
    kind = 'TSYS_SPECTRUM' if spectrum else 'TSYS'
    count = (channels if channel_override is None else channel_override) if spectrum else 1
    records = []
    for i, time in enumerate(sample_times):
        for antenna in antennas:
            values = [100 + antenna + i + j / 8 for j in range(2 * count)]
            if spectrum:
                for j in range(2 * count // 3):
                    values[j] *= 5
            if i == 1:
                values[0] = -1.
            record = [antenna, 0, 0, float(time) + 2.5, 5, *values, 'true']
            records.append(' '.join(map(str, record)) + '\n')
    raw.write_text(''.join(records))
    path = vis / 'SYSCAL' if destination is None else destination
    if path.exists():
        shutil.rmtree(path)
    tool = table()
    assert tool.fromascii(
        str(path), str(raw), sep=' ',
        columnnames=['ANTENNA_ID', 'FEED_ID', 'SPECTRAL_WINDOW_ID', 'TIME', 'INTERVAL',
                     kind, 'TSYS_FLAG'],
        datatypes=['I', 'I', 'I', 'D', 'D', f'R2,{count}' if spectrum else 'R2', 'B'],
    )
    tool.close()
    assert tool.open(str(path), nomodify=False)
    try:
        tool.putcolkeywords('TIME', {'QuantumUnits': ['s'], 'MEASINFO': {'type': 'epoch', 'Ref': 'UTC'}})
        tool.putcolkeywords('INTERVAL', {'QuantumUnits': ['s']})
        tool.putcolkeywords(kind, {'QuantumUnits': ['K']})
    finally:
        tool.close()
    assert tool.open(str(vis), nomodify=False)
    try:
        if 'SYSCAL' in tool.keywordnames():
            tool.removekeyword('SYSCAL')
        tool.putkeyword('SYSCAL', f'Table: {path}')
    finally:
        tool.close()


def check_apply(root, vis, reference, candidate, native, *, spectral_weights):
    root.mkdir()
    return {'apply_nrms': apply_both(root, vis, reference, candidate, native,
                                   spectral_weights=spectral_weights, weight_rtol=2.e-6)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', required=True, type=Path, help='One-window NGC 5921 VLA MS')
    parser.add_argument('--work', required=True, type=Path)
    parser.add_argument('--native-bin', required=True, type=Path)
    args = parser.parse_args()
    assert casa.version_string() == '6.7.6.14'
    os.environ['CASARS_CALIBRATE_BIN'] = str(args.native_bin.resolve())
    os.environ['CASARS_REQUIRE_TASK_BINARIES'] = '1'
    from casars import tasks as native

    source = args.source.resolve()
    root = args.work.resolve()
    root.mkdir(parents=True, exist_ok=True)
    assert not any(root.iterdir()), 'work directory must be empty'
    results = {}
    for form in ('scalar', 'spectrum'):
        vis = root / f'{form}.ms'
        copy_ms(source, vis)
        linked = root / 'scalar-syscal.tbl' if form == 'scalar' else None
        attach_syscal(vis, root / f'{form}.txt', spectrum=form == 'spectrum', destination=linked)
        if linked is not None:
            assert b'./scalar-syscal.tbl' in (vis / 'table.dat').read_bytes()
            results['external-input-protection'] = protects_prior_input(root, vis, linked, native,
                                                                       'tsys')
        for uniform in (True, False):
            case = f'{form}-{uniform}'
            reference, candidate = (root / f'{case}-{engine}.cal' for engine in ('casa', 'native'))
            options = {} if uniform else {
                'uniform': False, 'spw': '0', 'antenna': '1', 'pol': 'L',
                'parameter': [42], 'infile': str(root / 'unused'),
            }
            for module, output in ((casa, reference), (native, candidate)):
                generate(module, vis, output, [('tsys', options)])
            results[case] = {'table_nrms': compare_tables(candidate, reference)}
            if uniform:
                result = check_apply(root / f'{form}-spectrum-weights', vis, reference, candidate,
                                     native, spectral_weights=True)
                results[f'{form}-spectrum-weights'] = result
                if form == 'spectrum':
                    plain = root / 'spectrum-scalar-weights.ms'
                    copy_ms(vis, plain)
                    tool = table()
                    assert tool.open(str(plain), nomodify=False)
                    try:
                        tool.removecols([name for name in ('WEIGHT_SPECTRUM', 'SIGMA_SPECTRUM')
                                         if name in tool.colnames()])
                    finally:
                        tool.close()
                    results['spectrum-scalar-weights'] = check_apply(
                        root / 'spectrum-scalar-weights', plain, reference, candidate,
                        native, spectral_weights=False)
                    shifted = root / 'shifted-spectral-window.ms'
                    copy_ms(plain, shifted)
                    frequencies = columns(plain / 'SPECTRAL_WINDOW', ('CHAN_FREQ',))['CHAN_FREQ'][:, 0]
                    frequency = frequencies[len(frequencies) // 2] + np.ptp(frequencies) / 4
                    retune(shifted, 0, frequency)
                    results['shifted-spectral-window'] = check_apply(
                        root / 'shifted-spectral-window', shifted, reference, candidate,
                        native, spectral_weights=False)
            else:
                for module, engine in ((casa, 'casa'), (native, 'native')):
                    replacement = root / f'{form}-{engine}-replacement.cal'
                    generate(module, vis, replacement, [('tsys', {}), ('tsys', options)])
                    compare_tables(replacement, reference)
                results[f'{form}-replacement'] = {'table_nrms': 0.0}

    results['absent-syscal'] = refuses(root, 'absent-syscal', source, 'tsys', {}, native)
    invalid = root / 'bad-channels.ms'
    copy_ms(source, invalid)
    attach_syscal(invalid, root / 'bad-channels.txt', spectrum=True, channel_override=3)
    results['bad-channels'] = refuses(root, 'bad-channels', invalid, 'tsys', {}, native)
    (root / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
    print(json.dumps(results, indent=2))


if __name__ == '__main__':
    main()
