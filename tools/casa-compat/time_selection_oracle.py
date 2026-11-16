#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Freeze CASA's MSSelection `timerange` row selection (#980).

Run with CASA 6.7.6.14's casatools (6.7.6-14) under `python -I`. It builds
three small synthetic MeasurementSets with the simulator, rewrites their MAIN
TIME, EXPOSURE, INTERVAL, FIELD_ID and FLAG_ROW to the segments recorded in
the output, and records the physical MAIN rows `ms.msselect` keeps for each
case, or that CASA refused the expression. casa-ms's
`selection::time_expr` tests replay every case.

  python -I tools/casa-compat/time_selection_oracle.py --work DIR --out FILE

Datasets:
  uniform    1800 rows every 2 s from 2017/03/01/09:00:00.5 UTC, EXPOSURE 2 s
  fields     rows 0-9 flagged; rows 900-1799 FIELD 1, a day and an hour later,
             EXPOSURE 6 s: probes the first logical row and later terms
  fractions  one row per FIELD, with assorted sub-second times: probes the
             defaults `MSTimeParse::getDefaults` rebuilds from a row's TIME
"""

import argparse
import hashlib
import json
import random
import shutil
from pathlib import Path

import numpy as np

T0 = 57813 * 86400.0 + 9 * 3600 + 0.5
DAY = 86400.0


def segment(rows, time0, step, exposure, field, flag_row=False):
    return dict(rows=rows, time0_s=time0, step_s=step, exposure_s=exposure,
                field=field, flag_row=flag_row)


def fraction_times():
    """Sub-second times of day on assorted dates, none within 20 us of midnight."""
    days = [57813, 58849, 50000, 51544, 60000, 45000, 61000]
    tods = [32400.5, 32400.25, 32400.1234, 32400.0001, 32400.0005, 32400.0009999,
            32400.9995, 32400.999999, 32400.4995, 32400.5005, 35999.9999, 35999.999,
            3599.0004, 86399.999, 86399.9, 0.001, 0.5, 43200.123456, 59.999, 60.0015,
            1.001, 2.501, 12.345, 45296.789]
    times = [d * DAY + tod for d in days[:2] for tod in tods]
    rng = random.Random(980)
    for _ in range(24):
        day = rng.choice(days)
        tod = round(rng.uniform(0.01, 86399.9), rng.choice([1, 3, 4, 6]))
        times.append(day * DAY + tod)
    return times


def datasets():
    fractions = [segment(1, t, 0.0, 1.0 + 0.25 * (k % 5), k) for k, t in enumerate(fraction_times())]
    return {
        'uniform': [segment(1800, T0, 2.0, 2.0, 0)],
        'fields': [segment(10, T0, 2.0, 2.0, 0, True),
                   segment(890, T0 + 20.0, 2.0, 2.0, 0),
                   segment(900, T0 + 1800.0 + DAY + 3600.0, 2.0, 6.0, 1)],
        'fractions': fractions,
    }


def expand(segments):
    cols = {key: [] for key in ('TIME', 'EXPOSURE', 'FIELD_ID', 'FLAG_ROW')}
    for seg in segments:
        for i in range(seg['rows']):
            cols['TIME'].append(seg['time0_s'] + seg['step_s'] * i)
            cols['EXPOSURE'].append(seg['exposure_s'])
            cols['FIELD_ID'].append(seg['field'])
            cols['FLAG_ROW'].append(seg['flag_row'])
    return cols


def build(path, cols, nfields):
    import casatools
    sm = casatools.simulator()
    me = casatools.measures()
    shutil.rmtree(path, ignore_errors=True)
    sm.open(str(path))
    sm.setconfig(telescopename='VLA', x=[0.0, 100.0], y=[0.0, 0.0], z=[0.0, 0.0],
                 dishdiameter=[25.0, 25.0], mount=['alt-az', 'alt-az'],
                 antname=['A0', 'A1'], coordsystem='local', referencelocation=me.observatory('VLA'))
    sm.setspwindow(spwname='spw0', freq='1.4GHz', deltafreq='1MHz', freqresolution='1MHz',
                   nchannels=1, stokes='RR')
    sm.setfeed(mode='perfect R L')
    for k in range(nfields):
        sm.setfield(sourcename='F%d' % k, sourcedirection=me.direction('J2000', '12h00m00s', '40d00m00s'))
    sm.setauto(autocorrwt=0.0)
    sm.settimes(integrationtime='2s', usehourangle=False,
                referencetime=me.epoch('UTC', '2017/03/01/09:00:00'))
    nrow = len(cols['TIME'])
    sm.observe('F0', 'spw0', starttime='0s', stoptime='%ds' % (2 * nrow))
    sm.close()
    tb = casatools.table()
    tb.open(str(path), nomodify=False)
    assert tb.nrows() == nrow, tb.nrows()
    tb.putcol('TIME', np.asarray(cols['TIME'], dtype=float))
    tb.putcol('EXPOSURE', np.asarray(cols['EXPOSURE'], dtype=float))
    tb.putcol('INTERVAL', np.asarray(cols['EXPOSURE'], dtype=float))
    tb.putcol('FIELD_ID', np.asarray(cols['FIELD_ID'], dtype=np.int32))
    tb.putcol('FLAG_ROW', np.asarray(cols['FLAG_ROW'], dtype=bool))
    tb.close()
    tb.open(str(path))
    assert tb.getcol('TIME').tolist() == cols['TIME']
    assert tb.getcol('EXPOSURE').tolist() == cols['EXPOSURE']
    tb.close()


CURATED = [
    '', '2017/03/01/09:00:00.5', '2017/03/01/09:00:01.5', '2017/03/01/09:00:00', '09:00:00',
    '09:10', '09:10:', '9', '2017/03/01/9', '09:00:00~09:01:00', '[09:00:00~09:01:00]',
    '3.0[09:00:00~09:01:00]', '3[09:00:00~09:01:00]', '0.0[09:00:02.5~09:00:06.5]',
    '09:00:02.5~09:00:06.5', '>09:59:00', '<09:00:10', '09:00:00+00:00:10', '09:00:00+10',
    '09:00:00+1:00', '09:00:00~08:00:00', '2017/03/01', '2017/03/01~2017/03/01', '*',
    '2017/03/01/09:00:00.5,2017/03/01/09:30:00.5', '09:00:00.5~09:00:04.5,09:30:00.5',
    '2017-03-01', 'abc', ' ', '2017/13/01/09:00:00', '2017/02/31/09:00:00.5',
    '1800/01/01/00:00:00', '2017/02/29/09:00:00.5', '2017/03/01/08:59:60.5',
    '2017/03/01/08:60:00.5', '09:00:00.5009~09:00:10', '09:00:02.5004~09:00:10',
    '09:00:02.501~09:00:10', '09:00:01.001', '09:00:12.345', '*/*/*/*:*:*',
    '2017/03/01/09:00:00.5~10:00:00', '2017/03/01/09:30', '09:30:00', '10:00:00',
    '2017/03/01/09:00:00.5~2017/03/01/09:00:00.5',
    '[2017/03/01/09:00:00.5~2017/03/01/09:00:00.5]', '2.0[09:00:00.5~09:00:02.5]',
    '09:00:00.5+00:00:02', '[09:00:00.5+00:00:02]', '>2017/03/01/09:59:58.5',
    '<2017/03/01/09:00:00.5', '09:00:', '::', '::5', '/03/01/09:00:00', '03/01//09:00:00',
    '03/01/09:00:00', '2017/03/01 09:00:00', '  09:00:00.5  ', '09:00:00 ~ 09:00:04',
    '09:00:00.5~09:00:04.5,>09:59:58', '2017/03/01/09:00:00.5~2017/03/02', '2017/3/1/9:0:0.5',
    '0.5[09:00:00.5~09:00:02.5]', '2017/03/01/09:00:00.5~2017/03/01/09:00:00.4', '09:00:00~',
    '~09:00:00', '09:00:00,', ',09:00:00', '[09:00:00]', '[>09:00:00]',
    '2017/03/01/09:00:00.5 + 2', '09:00:00.5+2.5', '09:00:00.5+:2.5', '09:00:00.5+1/00:00:00',
    '09:00:00.5+48:00:00', '2017/03/*/09:00:00.5', '*/*/*', '*:*:*', '*:*:30', '09:*:30',
    '2017/03/01/09:00:00.5~*', '09:00:00.5~09:00:02.5~09:00:04.5', '57813.375', '09:00:00+10s',
    '09:00:00.5-09:00:02.5', '.5[09:00:00.5~09:00:02.5]', '5.[09:00:00.5~09:00:02.5]',
    '09:00:00.', '09:00:00.5~09:00:02.5,',
    '2017/03/01/09:00:00.5~2017/03/01/09:00:02.5,[09:30:00.5~09:30:02.5]',
    '<09:00:00.5,>09:59:58.5', '2017/03/01/09', '2017/03/01/09:', '2017/03/01/09:00',
    '2017/03/01/09:00:', '09', '09:00', '08:60:00.5', '2017//01/09:00:00', '2017/03//09:00:00',
    '2017/03/01//09:00:00', '09:00:00.5 , 09:00:04.5', '09:00:00.5&&09:00:04.5',
    "'09:00:00'", '>=09:00:00', '2017/03/01T09:00:00', '99999999999999999999',
    '2017/02/28/33:00:00.5', '0.1[09:00:00.4~09:00:00.6]', '[09:00:00.5~09:00:00.5]',
]

FIELD_CURATED = [
    '*', '*,*', '09:00:00.5', '10:30:00.5', '10:30:00.5,10:40:00.5',
    '2017/03/02/10:30:03.5,2017/03/02/10:40:03.5',
    '[2017/03/02/10:30:00.5~2017/03/02/10:30:10.5]',
    '2017/03/02/10:30:00.5~2017/03/02/10:30:10.5,[2017/03/02/10:40:00.5~2017/03/02/10:40:10.5]',
    '>10:30:00.5', '2017/03/02/10:30:03.5', '09:00:20.5~09:00:30.5,10:30:00.5~10:31:00.5',
    '*,>10:30:00.5', '[*~*],[*~*]', '<*,>*',
]


def random_expression(rng):
    def pick(options):
        return rng.choice(options)

    def seconds():
        return pick(['0', '00', '00.5', '30', '59.999', '60.5', '*', '', '.5', '5.',
                     str(rng.randint(0, 59)),
                     '%0.*f' % (rng.choice([1, 2, 3, 4]), rng.uniform(0, 60))])

    def clock():
        h = pick(['9', '09', '09', '09', '10', '08', '*', '', '23', '24'])
        m = pick(['0', '00', '30', '59', '60', '*', '', str(rng.randint(0, 59))])
        form = rng.random()
        if form < 0.55:
            return '%s:%s:%s' % (h, m, seconds())
        if form < 0.65:
            return '%s:%s:' % (h, m)
        if form < 0.8:
            return '%s:%s' % (m, seconds())
        if form < 0.88:
            return '%s:' % pick(['9', '30', '*', ''])
        return pick(['9', '30', '*', '0'])

    def date():
        return '%s/%s/%s' % (pick(['2017'] * 8 + ['*', '', '1858', '1800', '2016']),
                             pick(['3', '03'] * 5 + ['2', '*', '', '13', '0']),
                             pick(['1', '01'] * 5 + ['2', '28', '29', '31', '*', '32', '0']))

    def year_time():
        form = rng.random()
        if form < 0.4:
            return '%s/%s' % (date(), clock())
        if form < 0.5:
            return date()
        return clock()

    def duration():
        return pick(['10', '1:00', '00:00:10', ':2.5', '2.5', '0:0:30', '1:0:0', '48:00:00',
                     '0:10:', '*', '00:01:00'])

    def range_text():
        if rng.random() < 0.7:
            return '%s~%s' % (year_time(), year_time())
        return '%s+%s' % (year_time(), duration())

    def term():
        form = rng.random()
        if form < 0.3:
            return year_time()
        if form < 0.4:
            return '>' + year_time()
        if form < 0.5:
            return '<' + year_time()
        if form < 0.8:
            return range_text()
        if form < 0.9:
            return '[%s]' % range_text()
        return '%s[%s]' % (pick(['0.0', '0.5', '1.0', '2.', '.5', '3', '10.25']), range_text())

    sep = pick([',', ',', ', ', ' , '])
    text = sep.join(term() for _ in range(pick([1, 1, 1, 2, 2, 3])))
    if rng.random() < 0.06:
        at = rng.randint(0, len(text))
        text = text[:at] + pick(list("-.&|!%'\"aT ;/:~[],")) + text[at:]
    return text


def cases(rng, random_cases):
    out = [('uniform', {}, text) for text in CURATED]
    for select in ({}, {'field': '0'}, {'field': '1'}, {'field': '0,1'}):
        out += [('fields', select, text) for text in FIELD_CURATED]
    nfractions = len(fraction_times())
    for k in range(nfractions):
        for text in ('>*', '<*', '*~*', '*', '>*,<*'):
            out.append(('fractions', {'field': str(k)}, text))
    for _ in range(random_cases):
        out.append(('uniform', {}, random_expression(rng)))
    for _ in range(random_cases // 4):
        select = rng.choice([{}, {'field': '0'}, {'field': '1'}])
        out.append(('fields', select, random_expression(rng)))
    return out


def runs(rows):
    out = []
    for r in rows:
        if out and out[-1][1] == r - 1:
            out[-1][1] = r
        else:
            out.append([r, r])
    return out


# MSTimeGram.yy keeps a bracketed range's edge state in process globals, set
# when `[` is read and cleared only when a range completes. A refused bracketed
# range leaves them set, so CASA's next unbracketed range in the process is
# edge-inclusive. A range that completes clears them; one runs before each case.
RESET = '2017/03/01/09:00:00.5~2017/03/01/09:00:00.6'

# (refused expression, expression run next without a reset)
LEAKS = [
    ('10.25[09:00:00~08:00:00]', '09:00:00~09:01:00'),
    ('[09:00:00]', '09:00:00~09:01:00'),
    ('0.0[09:00:10~09:00:00]', '09:00:00.5+00:00:30'),
]


def reset(ms_tool, path):
    # Its rows do not matter (a leaked `0.0[` makes it select none); completing
    # the range clears the globals.
    ms_tool.open(str(path))
    try:
        ms_tool.msselect({'time': RESET}, onlyparse=False)
    finally:
        ms_tool.close()


def query(ms_tool, path, nrow, row_of_time, select, text, clean=True):
    import casatools
    if clean:
        reset(ms_tool, path)
    ms_tool.open(str(path))
    try:
        items = dict(select)
        items['time'] = text
        ok = ms_tool.msselect(items, onlyparse=False)
        selected = ms_tool.nrow(True)
        if ok:
            times = np.atleast_1d(ms_tool.getdata(['time'])['time'])
            return {'rows': runs(sorted(row_of_time[t] for t in times.tolist()))}
        if selected == 0:
            return {'rows': []}
        if text == '' and not select:
            return {'rows': runs(range(nrow))}
        assert selected == nrow, (text, select, selected)
    finally:
        ms_tool.close()
    message = 'refused'
    try:
        casatools.ms().msseltoindex(vis=str(path), time=text, **select)
    except Exception as exc:  # noqa: BLE001 - CASA raises bare RuntimeErrors
        message = ' '.join(str(exc).split())[:200]
    return {'error': message}


def main():
    import casatools
    assert casatools.version_string() == '6.7.6-14', casatools.version_string()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--random-cases', type=int, default=800)
    parser.add_argument('--seed', type=int, default=980)
    args, _ = parser.parse_known_args()
    args.work.mkdir(parents=True, exist_ok=True)
    sets = datasets()
    paths, row_index, nrows = {}, {}, {}
    for name, segments in sets.items():
        cols = expand(segments)
        paths[name] = args.work / (name + '.ms')
        build(paths[name], cols, max(cols['FIELD_ID']) + 1)
        assert len(set(cols['TIME'])) == len(cols['TIME'])
        row_index[name] = {t: i for i, t in enumerate(cols['TIME'])}
        nrows[name] = len(cols['TIME'])
    ms_tool = casatools.ms()
    records = []
    for name, select, text in cases(random.Random(args.seed), args.random_cases):
        result = query(ms_tool, paths[name], nrows[name], row_index[name], select, text)
        records.append(dict(dataset=name, select=select, timerange=text, **result))
    leaks = []
    path, nrow, index = paths['uniform'], nrows['uniform'], row_index['uniform']
    for refused, text in LEAKS:
        first = query(ms_tool, path, nrow, index, {}, refused)
        assert 'error' in first, (refused, first)
        leaked = query(ms_tool, path, nrow, index, {}, text, clean=False)
        clean = query(ms_tool, path, nrow, index, {}, text)
        leaks.append(dict(dataset='uniform', after_refused=refused, timerange=text,
                          casa_rows_after_refusal=leaked['rows'], rows=clean['rows']))
    script = Path(__file__).read_bytes()
    header = {
        'casa_tools': casatools.version_string(),
        'casacore_commit': 'aaf72eb7edd5a5fecefa78ca04713233b7b6ffd6',
        'oracle_script': 'tools/casa-compat/time_selection_oracle.py',
        'oracle_script_sha256': hashlib.sha256(script).hexdigest(),
        'seed': args.seed,
        'row_identity': 'physical zero-based MAIN rows, as [first, last] runs',
        'scope': 'MSSelection timerange rows via ms.msselect; other selectors are FIELD ids only',
        'state': 'each case runs after a completed range (%s) clears the grammar globals' % RESET,
        'datasets': sets,
        'bracket_state_leaks': leaks,
    }
    lines = [json.dumps(header, indent=1)[:-2] + ',\n "cases": [']
    lines += [' ' + json.dumps(record) + ',' for record in records]
    lines[-1] = lines[-1][:-1]
    lines.append(' ]\n}')
    args.out.write_text('\n'.join(lines) + '\n')
    print('wrote %d cases to %s' % (len(records), args.out))


if __name__ == '__main__':
    main()
