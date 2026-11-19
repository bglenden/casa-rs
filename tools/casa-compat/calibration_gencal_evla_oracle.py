#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare CASA and native VLA corrections through a frozen or live archive."""

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
from threading import Thread
from urllib.parse import parse_qs, urlsplit

import casatasks as casa
from casatools import table
import numpy as np

from applycal_oracle import copy_ms
from calibration_gencal_gaincurve_oracle import compare_case
from calibration_gencal_specify_oracle import apply_both, generate
from record_casa_returns import make_cal_ms


ARCHIVE_2010 = b'''<pre>
; frozen rows from the baseline archive format
JUL16 JUL16 JUL16 11:00 1 W09 1 1 1
JUL17 JUL18 21:44 1 W09 0 .003 0
JUL17 JUL18 21:44 1 W08 1 1 1
JUL17 JUL19 21:44 1 W09 .005 0 0
JUL20 JUL20 JUL20 21:44 1 W09 1 1 1
JUL17 JUL21 21:44 1 W09 1 1 1
</pre>
'''


@contextmanager
def archive_proxy(fail_at=None):
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            url = urlsplit(self.path)
            year = int(parse_qs(url.query)['Year'][0])
            calls.append(year)
            if len(calls) == fail_at:
                self.send_error(503)
                return
            body = ARCHIVE_2010 if year == 2010 else b'<pre>\n</pre>\n'
            self.send_response(200)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    variables = ('http_proxy', 'HTTP_PROXY', 'no_proxy', 'NO_PROXY')
    old = {name: os.environ.get(name) for name in variables}
    with ThreadingHTTPServer(('127.0.0.1', 0), Handler) as server:
        proxy = f'http://127.0.0.1:{server.server_port}'
        os.environ.update(dict(zip(variables, (proxy, proxy, '', ''), strict=True)))
        worker = Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            yield calls
        finally:
            server.shutdown()
            worker.join()
            for name, value in old.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value


def configure_ms(root, name, base, stamp, telescope='EVLA'):
    vis = root / f'{name}.ms'
    copy_ms(base, vis)
    tool = table()
    assert tool.open(str(vis), nomodify=False)
    times = tool.getcol('TIME')
    shift = (stamp.timestamp() + 40587 * 86400) - min(times)
    for column in ('TIME', 'TIME_CENTROID'):
        tool.putcol(column, tool.getcol(column) + shift)
    tool.close()
    assert tool.open(str(vis / 'OBSERVATION'), nomodify=False)
    tool.putcol('TELESCOPE_NAME', [telescope] * tool.nrows())
    tool.putcol('TIME_RANGE', tool.getcol('TIME_RANGE') + shift)
    tool.close()
    assert tool.open(str(vis / 'ANTENNA'), nomodify=False)
    tool.putcol('STATION', ['W09', 'EVLA:W10', 'E01', 'VLA:N02', 'N30'])
    tool.close()
    return vis


def freeze_results(root, fixture, native):
    results = {}
    with archive_proxy() as calls:
        results['archive-offsets'], expected = compare_case(root, 'archive-offsets', fixture,
            'antpos', {}, native, check_apply=True)
        np.testing.assert_array_equal(expected['FPARAM'][:, 0, 0], np.float32([.005, .003, 0.]))
        assert calls.count(2010) == 4, calls
        results['day-cutoff'], expected = compare_case(root, 'day-cutoff', fixture,
            'antpos', {'ant_pos_time_limit': 2}, native)
        np.testing.assert_array_equal(expected['FPARAM'][:, 0, 0], np.float32([0., .003, 0.]))
    for name, engine in (('casa', casa), ('native', native)):
        prior = root / f'protected-{name}.cal'
        copy_ms(root / 'archive-offsets' / 'casa.cal', prior)
        before = (prior / 'table.dat').read_bytes()
        for limit, fail_at in ((1, None), (0, 1)):
            absent = root / f'absent-{name}-{limit}.cal'
            with archive_proxy(fail_at):
                generate(engine, fixture, absent, [('antpos', {'ant_pos_time_limit': limit})])
                assert not absent.exists()
            with archive_proxy(fail_at):
                generate(engine, fixture, prior, [('antpos', {'ant_pos_time_limit': limit})])
                assert (prior / 'table.dat').read_bytes() == before
        with archive_proxy(fail_at=2):
            try:
                generate(engine, fixture, prior, [('antpos', {})])
            except (RuntimeError, OSError):
                assert (prior / 'table.dat').read_bytes() == before
            else:
                raise AssertionError(f'{name} accepted an archive transfer failure')
    results.update({'initial-connection-failure': {'preserved': True},
                    'empty-cutoff': {'preserved': True},
                    'later-transfer-failure': {'preserved': True}})
    return results


def affected_epoch(root, fixture, native):
    with archive_proxy():
        result, values = compare_case(root, 'tropo-marker', fixture, 'antpos', {}, native)
    assert not np.any(values['FPARAM'])
    case = root / 'tropo-marker'
    for scale in (1.0, 0.0, -2.0):
        for writer in ('casa', 'native'):
            tool = table()
            assert tool.open(str(case / f'{writer}.cal'), nomodify=False)
            if scale == 1.0:
                assert tool.getkeyword('VLATrDelCorr') == 1.0
            else:
                tool.putkeyword('VLATrDelCorr', scale)
            tool.close()
        output = case / f'apply-{scale}'
        output.mkdir()
        result[f'tropo_scale_{scale}_nrms'] = apply_both(
            output, fixture, case / 'casa.cal', case / 'native.cal', native, calwt=False)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', required=True, type=Path)
    parser.add_argument('--native-bin', required=True, type=Path)
    parser.add_argument('--live', action='store_true')
    args = parser.parse_args()
    assert casa.version_string() == '6.7.6.14'
    os.environ['CASARS_CALIBRATE_BIN'] = str(args.native_bin.resolve())
    os.environ['CASARS_REQUIRE_TASK_BINARIES'] = '1'
    from casars import tasks as native

    root = args.work.resolve()
    root.mkdir(parents=True)
    base = root / 'base.ms'
    make_cal_ms(str(base), telescope='VLA', frequency='1400MHz', stokes='RR LL',
                antenna_names=[f'ea{index:02}' for index in range(1, 6)])
    fixture = configure_ms(root, 'archive', base, datetime(2010, 7, 17, 12, tzinfo=timezone.utc))
    results = freeze_results(root, fixture, native)
    affected = configure_ms(root, '2016B', base, datetime(2016, 9, 1, 9, tzinfo=timezone.utc))
    results['tropo-marker'] = affected_epoch(root, affected, native)
    if args.live:
        results['live-archive'], values = compare_case(root, 'live-archive', fixture, 'antpos',
            {'ant_pos_time_limit': 2}, native)
        np.testing.assert_array_equal(values['FPARAM'][:, 0, 0], np.float32([0., .003, 0.]))
    (root / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
    print(json.dumps(results, indent=2))


if __name__ == '__main__':
    main()
