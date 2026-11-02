#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Freeze CASA's MSSelection `spw` selection (#980).

Run with CASA 6.7.6.14's casatools (6.7.6-14) under `python -I`. It builds
two small synthetic MeasurementSets with the simulator, rewrites their
SPECTRAL_WINDOW and DATA_DESCRIPTION rows to the windows recorded in the
output, and records, for each expression, what `ms.msselect({'spw': ...})`
and `ms.msselectedindices()` report: the spectral windows (`getSpwList`),
their data descriptions (`getSPWDDIDList`), the channel list
(`getChanList`) and the data descriptions whose MAIN rows are selected, or
that CASA refused the expression. casa-ms's `selection::spw_expr` tests
(`crates/casa-ms/tests/spw_selection_oracle.rs`) replay every case.

  python -I tools/casa-compat/spw_selection_oracle.py --work DIR --out FILE

Each data description `k` the simulator writes holds `2**k` MAIN rows, so the
number of selected rows names the selected data descriptions exactly.

Datasets:
  bands  ten windows with assorted names (numeric, duplicate, empty, unit-like,
         with `#` and `-`), ascending and descending channels, negative and
         positive widths on descending channels, non-uniform widths, one
         channel, a flagged window, a window without a data description, a
         window with two, and a flagged data description
  vla    three 64-channel windows named as the VLA names them, the last
         descending with negative widths
"""

import argparse
import hashlib
import json
import random
import re
import shutil
from pathlib import Path

import numpy as np


def window(name, freqs, widths, ref=None, total=None, flag=False):
    freqs = [float(f) for f in freqs]
    widths = [float(w) for w in widths]
    return dict(name=name, chan_freq_hz=freqs, chan_width_hz=widths,
                ref_frequency_hz=float(freqs[0] if ref is None else ref),
                total_bandwidth_hz=float(sum(abs(w) for w in widths) if total is None else total),
                flag_row=flag)


def linear(start, step, n):
    return [start + step * k for k in range(n)]


def bands():
    nonuniform = [1e6] * 4 + [2e6] * 4 + [4e6] * 4 + [8e6] * 4
    centres, edge = [], 20.0e9
    for width in nonuniform:
        centres.append(edge + width / 2)
        edge += width
    windows = [
        window('ALMA_RB_03#BB_1#SW-01#FULL_RES', linear(100.0e9, 15.625e6, 16), [15.625e6] * 16),
        window('spw1', linear(101.0e9, -7.8125e6, 32), [-7.8125e6] * 32, ref=101.0e9),
        window('SPW_2', linear(1.0e9, 128e6, 8), [128e6] * 8, ref=1.448e9),
        window('7', linear(230.0e9, -0.5e6, 64), [0.5e6] * 64, ref=230.0e9),
        window('12', [50.0e9], [2.0e9], ref=50.0e9),
        window('', centres, nonuniform, ref=centres[0]),
        window('spw1', linear(100.0e9, 15.625e6, 16), [15.625e6] * 16, ref=100.1e9),
        window('Hz', linear(345.0e9, 1.0e9, 4), [1.0e9] * 4, flag=True),
        window('orphan', linear(5.0e9, 1.0e6, 8), [1.0e6] * 8),
        window('LBAND-A', linear(1.4e9, 1.0e6, 8), [1.0e6] * 8, ref=1.4e9, total=8.0e6),
    ]
    # Data description 8 is re-pointed at window 9, so window 8 has none and
    # window 9 two; 10 is a flagged second one for window 2 and 11 an
    # unflagged second one for window 0, neither with MAIN rows.
    descriptions = [dict(spw=k, flag_row=False) for k in range(10)]
    descriptions[8]['spw'] = 9
    descriptions += [dict(spw=2, flag_row=True), dict(spw=0, flag_row=False)]
    return dict(windows=windows, data_descriptions=descriptions)


def vla():
    windows = [
        window('EVLA_C#A0C0#0', linear(4.488e9, 2.0e6, 64), [2.0e6] * 64),
        window('EVLA_C#A0C0#1', linear(4.616e9, 2.0e6, 64), [2.0e6] * 64),
        window('EVLA_C#B0D0#2', linear(7.5e9, -2.0e6, 64), [-2.0e6] * 64),
    ]
    return dict(windows=windows, data_descriptions=[dict(spw=k, flag_row=False) for k in range(3)])


def build(path, spec):
    import casatools
    sm = casatools.simulator()
    me = casatools.measures()
    shutil.rmtree(path, ignore_errors=True)
    sm.open(str(path))
    sm.setconfig(telescopename='VLA', x=[0.0, 100.0], y=[0.0, 0.0], z=[0.0, 0.0],
                 dishdiameter=[25.0, 25.0], mount=['alt-az', 'alt-az'],
                 antname=['A0', 'A1'], coordsystem='local', referencelocation=me.observatory('VLA'))
    windows = spec['windows']
    simulated = len(windows)
    for k in range(simulated):
        sm.setspwindow(spwname='w%d' % k, freq='1GHz', deltafreq='1MHz', freqresolution='1MHz',
                       nchannels=len(windows[k]['chan_freq_hz']), stokes='RR')
    sm.setfeed(mode='perfect R L')
    sm.setfield(sourcename='F0', sourcedirection=me.direction('J2000', '12h00m00s', '40d00m00s'))
    sm.setauto(autocorrwt=0.0)
    sm.settimes(integrationtime='1s', usehourangle=False,
                referencetime=me.epoch('UTC', '2017/03/01/09:00:00'))
    for k in range(simulated):
        sm.observe('F0', 'w%d' % k, starttime='0s', stoptime='%ds' % (2 ** k))
    sm.close()
    tb = casatools.table()
    tb.open(str(path) + '/SPECTRAL_WINDOW', nomodify=False)
    assert tb.nrows() == simulated, tb.nrows()
    for row, w in enumerate(windows):
        n = len(w['chan_freq_hz'])
        assert tb.getcell('NUM_CHAN', row) == n
        tb.putcell('NAME', row, w['name'])
        tb.putcell('CHAN_FREQ', row, np.asarray(w['chan_freq_hz']))
        tb.putcell('CHAN_WIDTH', row, np.asarray(w['chan_width_hz']))
        tb.putcell('EFFECTIVE_BW', row, np.abs(np.asarray(w['chan_width_hz'])))
        tb.putcell('RESOLUTION', row, np.abs(np.asarray(w['chan_width_hz'])))
        tb.putcell('REF_FREQUENCY', row, w['ref_frequency_hz'])
        tb.putcell('TOTAL_BANDWIDTH', row, w['total_bandwidth_hz'])
        tb.putcell('FLAG_ROW', row, w['flag_row'])
    tb.close()
    tb.open(str(path) + '/DATA_DESCRIPTION', nomodify=False)
    extra = len(spec['data_descriptions']) - tb.nrows()
    if extra > 0:
        tb.addrows(extra)
    for row, d in enumerate(spec['data_descriptions']):
        tb.putcell('SPECTRAL_WINDOW_ID', row, d['spw'])
        tb.putcell('POLARIZATION_ID', row, 0)
        tb.putcell('FLAG_ROW', row, d['flag_row'])
    tb.close()
    tb.open(str(path))
    ddids = tb.getcol('DATA_DESC_ID').tolist()
    tb.close()
    for k in range(simulated):
        assert ddids.count(k) == 2 ** k, (k, ddids.count(k))


VLA_CURATED = [
    '', '*', '0', '1', '2', '3', '0,1', '0~2', '0:2~5;7~9', '*:3~6', '0~1:10~20^2', '0:0~63^4',
    'EVLA_C#A0C0#0', 'EVLA_C#A0C0#1:5~9', '"EVLA_C#A0C0#*"', 'EVLA_C*', '"EVLA_C#B0D0#2":0~3',
    '4.5GHz~4.6GHz', '4.5GHz~4.6GHz:0~3', '4.55GHz', '4.551GHz', '7.437GHz', '~4.55GHz', '~7.43GHz',
    '0:4.5GHz~4.55GHz', '0:4500MHz~4550MHz', '0:4.5GHz~4.55GHz^4MHz', '0:4.5GHz~4.55GHz^1MHz',
    '2:7.4GHz~7.45GHz', '2:7.45GHz~7.4GHz', '2:7.4GHz~7.45GHz^6MHz', '0~2:4.5GHz~4.55GHz',
    '0~2:4.5GHz~4.55GHz;0~1', '0~2:7.4GHz~7.45GHz;0~1', '*:4.6GHz~4.65GHz', '0:4.49GHz', '0:4.4890GHz',
    '0:4.4895GHz', '2:7.4385GHz', '0:4GHz~5GHz', '0:5GHz~6GHz', '0:3GHz~4GHz', '2:8GHz~9GHz',
    '2:1GHz~2GHz', '0:4.5GHz~4.55GHz;10~12', '0:10~12;4.5GHz~4.55GHz', '>4.6GHz', '<4.6GHz', '>1',
    '<1', '0<>2', '4GHz<>5GHz', '>0:0~3', '<2:5', '0:0', '0:63', '0:64', '0:100', '0:60~70',
    '0:70~80', '0:-1', '0:-1~10', '0:-5~-2', '0:-3', '0:5~5', '0:^3', '0:^3^5', '0:0~10^0',
    '0:0~10^-2', '0:0~10^2.9', '0:1.9~4.1', '0:4.1~1.9', '2~1', '0~0', '0:3~1', '0;2', '0;2:1~3',
    '0~2^2', '^2', '3', '-1', '0,3', '0,0', '1,0', '2,0:5', '0:1~3,0:5~7', '0:1~3;1~3', '(0)',
    '(0,1:2~4)', '((0))', '(0', '0)', '(0),1', '0:', ':0', '0::1', '0,', ',0', '0 1', '0&1', '!0',
    ' ', '0 : 1 ~ 3', '0\t,\n1', '0\r', 'evla_c#a0c0#0', '0:4.5ghz~4.55GHZ', '0:4500000kHz~4550000khz',
    '0:4.5e9Hz~4.55e9Hz', '0:4.5e9hz~4.55E9Hz', '0:4500000000~4550000000Hz', '0:4.5~4.55GHz',
    '0:4500mHz~4550mHz', '0:4.5GHz~4550MHz', '0:4.55GHz~4.5GHz', '0:4.5GHz~4.55', '0:4.5~4.55',
    '0:0.0045THz~0.00455THz', '0:-10km/s~10km/s', '0:1~2m/s', '0:10kms', '4.5GHz', '0:^4MHz',
    '4.5GHz~4.6GHz^20MHz', '4.5GHz~4.6GHz^1GHz', '4GHz~5GHz^0Hz', '4.5GHz~4.5GHz',
    '1 GHz~ 5 GHz', '1GHz~5GHz;0', '0;1GHz~5GHz', '4.5GHz;4.62GHz', '4.62GHz;4.5GHz', '4.5GHz~4.7GHz;2',
    '0é', 'é0', '0"abc', '0/abc', '"EVLA_C#A0C0#0', '/EVLA', '"EVLA_C#A0C0#0"x',
]

BANDS_CURATED = [
    '', ' ', '*', '0', '9', '10', '-1', '0,1', '1,0', '0,0', '0~3', '3~0', '0~9', '0~20', '0~12',
    '^2', '0;2;4', '0~9^3', '0~9^0', '0~9^-1', '1.7', '0.2~2.9', '+1', '1e0', '1E0', '1d1', '1.',
    '12', '13', '12,13', '4~13', '"12"', '"7"', '7', 'spw1', 'SPW_2', 'Hz', '"Hz"', '"Hz",0', '6,7',
    '7:0~1', 'orphan', 'orphan,0', '8', '8:0~3', '9', '9:2~4', 'LBAND-A', '"LBAND-A"', 'LBAND*',
    '"ALMA*"', 'ALMA*', '*SW-01*', '"*SW-01*"', '/spw./', '/spw?/', '/SPW_[0-9]/', '"spw[1]"',
    'spw{1}', '"spw{1,2}"', '"SPW_{1,2}"', '""', '"', '/', '"abc', '0"abc', '0/abc', '"spw\\1"',
    '"sp\nw1"', 'ALMA_RB_03#BB_1#SW', "'spw1'", 'spw1\'', '{a}', '#', '+a', '?', '*:0', '*:3~6',
    'spw1:0~3', 'spw1:1~2;5~7', '0:-1', '0:-1~5', '0:-3', '0:100', '0:10~100', '0:20~30', '0:5~3',
    '0:3~3', '0:2~10^3', '0:^2', '0:^2^3', '0:0~10^2.9', '0:1~2;5~7', '0:1~2;1~2', '0:1~2,0:5~7',
    '0~2:1~3', '0;2:1~3', '0:0~15^0', '4:0', '4:1', '4:0~3', '0:100.0GHz~100.1GHz',
    '0:100000~100100MHz', '0:100GHz~100.1GHz^31.25MHz', '0:100.05GHz', '0:100.0546875GHz',
    '1:100.9GHz~100.95GHz', '1:100.95GHz~100.9GHz', '1:100.9GHz~100.95GHz^20MHz',
    '3:229.99GHz~229.995GHz', '3:229.98GHz', '3:229GHz~229.98GHz', '3:229.99GHz~231GHz',
    '0:99GHz~99.5GHz', '0:200GHz~300GHz', '0~1:100.0GHz~100.1GHz', '0,1:100.9GHz~101GHz',
    '5:20.001GHz~20.01GHz', '5:20.001GHz~20.06GHz^4MHz', '0:100GHz~100.1GHz;3~5',
    '0:3~5;100GHz~100.1GHz', '1~2:1.0GHz~1.5GHz;5~6', '1~3:1.0GHz~1.5GHz;5~6;100.9GHz~101GHz',
    '2~3:1.0GHz~1.5GHz;5~6', '0:300GHz~400GHz;1~2', '0~1:300GHz~400GHz;1~2', '4:49GHz~51GHz',
    '4:50GHz', '4:52GHz~53GHz', '100GHz~101GHz', '100.1GHz', '100.11GHz', '100.117GHz', '1.4GHz',
    '1.448GHz', '1GHz~2GHz', '1GHz~2GHz^10MHz', '1GHz~2GHz^100MHz', '1GHz~2GHz^-0Hz', '~1.4GHz',
    '~1.448GHz', '~100GHz', '~50GHz', '~1', '>50GHz', '<50GHz', '>5', '<2', '>9', '<0', '2<>5',
    '1GHz<>60GHz', '5<>2', '1GHz<>3', '3<>1GHz', '>1GHz:0~3', '1GHz~2GHz:0~3', '100GHz~101GHz;0',
    '0;100GHz~101GHz', '100GHz~101GHz;1GHz~2GHz', '1.4GHz~1.4GHz', '1 GHz~2 GHz', '1GHz ~ 2GHz',
    '1000MHz~2GHz', '1~2GHz', '1~2.5GHz', '2GHz~1GHz', '1GHz~2', '1THz', '0:1e11Hz~1.001e11Hz',
    '0:1e5kHz~1.001e5khz', '0:100gHz~100.1ghz', '0:100000mhz~100100mhz', '0:-10km/s~10km/s',
    '(0)', '(0,1)', '(0:1~3)', '((0))', '(0', '0)', '(0),1', '0:', ':0', '0,', ',0', '0 1', '0&1',
    '!0', '0\t,\n1', '0\r', 'spw1 ', ' spw1', 'spw 1', '0é', 'é0', '5', '5:0~3', '"":0~3',
    '229.98GHz~230GHz', '230GHz', '~230GHz', '345GHz', '>300GHz', '7,8,9', '7~9', '0~9:0',
    '0~9:2~4', '*:2~4', '*:1.0GHz~1.5GHz', '*:100GHz~100.1GHz', '"*"', '"*":0',
]

# Tokens `sscanf("%f")` cannot read, whose value CASA leaves uninitialised;
# recorded as evidence, refused by the port.
UNDEFINED = ['.', '+', '-', 'e1', 'd5', '0:.', '0:+', '0:-', '0:e1', '0:1~.', '0:^.']

# A frequency range of windows stepped by a negative step steps by the
# window's least CHAN_WIDTH (`MSSpwIndex::matchFrequencyRange`); a window whose
# least width is negative then never leaves the loop, so CASA does not return.
# Not run; the port refuses them.
HANGS = [('bands', '1GHz~2GHz^-1MHz'), ('vla', '4GHz~5GHz^-1MHz')]


def random_expression(rng, names):
    def pick(options):
        return rng.choice(options)

    def number():
        return pick(['0', '1', '2', '3', '5', '7', '9', '10', '12', '15', '20', '31', '63', '64',
                     '100', '-1', '-3', '1.5', '2.9', '0.5', '+2', '1e1', str(rng.randint(0, 70))])

    def freq():
        base = pick(['100.0', '100.05', '100.1', '100.9', '101', '1.0', '1.4', '1.448', '1.5',
                     '20.001', '20.01', '229.99', '230', '50', '4.5', '4.55', '4.6', '7.4', '7.45'])
        unit = pick(['GHz'] * 6 + ['MHz', 'ghz', 'Hz', 'kHz', 'THz'])
        if unit == 'MHz':
            base = str(float(base) * 1000)
        elif unit == 'Hz':
            base = '%.6e' % (float(base) * 1e9)
        elif unit == 'kHz':
            base = str(float(base) * 1e6)
        elif unit == 'THz':
            base = str(float(base) / 1000)
        return base + unit

    def element(channel):
        form = rng.random()
        if form < 0.3:
            return number()
        if form < 0.6:
            top = 70 if channel else 12
            a, b = sorted([rng.randint(-2, top), rng.randint(-2, top)])
            text = '%d~%d' % (a, b) if rng.random() < 0.9 else '%d~%d' % (b, a)
            if rng.random() < 0.25:
                text += '^' + pick(['2', '3', '0', '-1', '4.5'])
            return text
        if form < 0.65:
            return '^' + pick(['2', '3', '5'])
        if form < 0.8 or not channel:
            return freq()
        text = '%s~%s' % (freq(), freq())
        if rng.random() < 0.2:
            text += '^' + pick(['10MHz', '2MHz', '0.5MHz', '31.25MHz'])
        return text

    def flist(channel):
        return ';'.join(element(channel) for _ in range(pick([1, 1, 1, 2, 2, 3])))

    def spw():
        form = rng.random()
        if form < 0.45:
            return flist(False)
        if form < 0.6:
            name = pick(names)
            return name if re.fullmatch(r"[a-zA-Z_'{}+#][a-zA-Z0-9_'{}+#]*", name or '-') else '"%s"' % name
        if form < 0.7:
            return pick(['*', '"*"', '"spw*"', 'spw*', '"EVLA*"', 'EVLA_C*', '"*2"', '/spw?/'])
        if form < 0.8:
            return pick(['>', '<', '~']) + pick([number(), freq()])
        if form < 0.85:
            return '%s<>%s' % (number(), number())
        return number()

    def spec():
        text = spw()
        if rng.random() < 0.55:
            text += ':' + flist(True)
        return text

    text = pick([',', ',', ', ']).join(spec() for _ in range(pick([1, 1, 1, 2, 2, 3])))
    if rng.random() < 0.05:
        text = '(' + text + ')'
    if rng.random() < 0.06:
        at = rng.randint(0, len(text))
        text = text[:at] + pick(list('-.&!%\'"aT ;:~,^<>()/*')) + text[at:]
    return text


def query(ms_tool, path, nrow, text, logfile):
    import casatools
    before = logfile.stat().st_size if logfile.exists() else 0
    ms_tool.open(str(path))
    try:
        ok = ms_tool.msselect({'spw': text}, onlyparse=False)
        selected = ms_tool.nrow(True)
        indices = ms_tool.msselectedindices() if ok else None
    finally:
        ms_tool.close()
    warnings = []
    if logfile.exists():
        with logfile.open('rb') as handle:
            handle.seek(before)
            for line in handle.read().decode('utf-8', 'replace').splitlines():
                parts = line.split('\t')
                if len(parts) >= 4 and parts[1] == 'WARN':
                    warnings.append(parts[3].strip())
    if not ok:
        try:
            indices = casatools.ms().msseltoindex(vis=str(path), spw=text)
        except Exception as exc:  # noqa: BLE001 - CASA raises bare RuntimeErrors
            return {'error': ' '.join(str(exc).split())[:300]}
        rows = 'all'
    else:
        assert selected <= nrow
        rows = [k for k in range(nrow.bit_length()) if selected >> k & 1]
    return {
        'spw': np.atleast_1d(indices['spw']).astype(int).tolist(),
        'spwdd': np.atleast_1d(indices['spwdd']).astype(int).tolist(),
        'channel': np.asarray(indices['channel']).astype(int).reshape(-1, 4).tolist(),
        'rows': rows,
        'warnings': warnings,
    }


def main():
    import casatools
    assert casatools.version_string() == '6.7.6-14', casatools.version_string()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--random-cases', type=int, default=1200)
    parser.add_argument('--seed', type=int, default=980)
    args, _ = parser.parse_known_args()
    args.work.mkdir(parents=True, exist_ok=True)
    logfile = args.work / 'spw-oracle.log'
    sink = casatools.logsink()
    sink.setglobal(True)
    sink.setlogfile(str(logfile))
    sets = {'bands': bands(), 'vla': vla()}
    paths, nrows = {}, {}
    for name, spec in sets.items():
        paths[name] = args.work / (name + '.ms')
        build(paths[name], spec)
        simulated = len(spec['windows'])
        nrows[name] = 2 ** simulated - 1
    ms_tool = casatools.ms()
    rng = random.Random(args.seed)
    cases = [('bands', text) for text in BANDS_CURATED] + [('vla', text) for text in VLA_CURATED]
    for k in range(args.random_cases):
        name = 'bands' if k % 2 == 0 else 'vla'
        names = [w['name'] for w in sets[name]['windows']]
        cases.append((name, random_expression(rng, names)))
    records = []
    for name, text in cases:
        result = query(ms_tool, paths[name], nrows[name], text, logfile)
        records.append(dict(dataset=name, expression=text, **result))
    undefined = []
    for text in UNDEFINED:
        result = query(ms_tool, paths['bands'], nrows['bands'], text, logfile)
        undefined.append(dict(dataset='bands', expression=text, casa=result))
    script = Path(__file__).read_bytes()
    header = {
        'casa_tools': casatools.version_string(),
        'casacore_commit': 'aaf72eb7edd5a5fecefa78ca04713233b7b6ffd6',
        'oracle_script': 'tools/casa-compat/spw_selection_oracle.py',
        'oracle_script_sha256': hashlib.sha256(script).hexdigest(),
        'seed': args.seed,
        'scope': ('MSSelection spw expressions via ms.msselect and ms.msselectedindices: spw '
                  '(getSpwList), spwdd (getSPWDDIDList), channel (getChanList: spw, start, stop, '
                  'step), rows (the data descriptions whose MAIN rows are selected, or "all" when '
                  'msselect builds no row condition), warnings (WARN lines CASA logged)'),
        'datasets': sets,
        'undefined_tokens': undefined,
        'not_returning': [dict(dataset=name, expression=text) for name, text in HANGS],
    }
    lines = [json.dumps(header, indent=1)[:-2] + ',\n "cases": [']
    lines += [' ' + json.dumps(record) + ',' for record in records]
    lines[-1] = lines[-1][:-1]
    lines.append(' ]\n}')
    args.out.write_text('\n'.join(lines) + '\n')
    print('wrote %d cases to %s' % (len(records), args.out))


if __name__ == '__main__':
    main()
