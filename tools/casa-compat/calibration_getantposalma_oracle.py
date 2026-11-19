#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Exercise the CASA ALMA archive helper against frozen and live services."""

import argparse
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import inspect
import json
import os
from pathlib import Path
from threading import Thread
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

import casatasks as casa
from casatools import table
import numpy as np

from calibration_gencal_specify_oracle import apply_both, compare_tables, generate
from record_casa_returns import make_cal_ms


ASDM = 'uid://A002/X10ac6bc/X896d'
WINDOW = '2023-03-14T00:40:20,2023-03-20T17:58:20'


@contextmanager
def frozen_service(positions):
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            url = urlsplit(self.path)
            path = url.path.strip('/')
            calls.append((path, parse_qs(url.query)))
            if path == 'failure' or (path == 'retry' and len(calls) == 1):
                self.send_error(503)
                return
            body = {'empty': '{}', 'invalid': '['}.get(path, json.dumps(positions)).encode()
            self.send_response(200)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    with ThreadingHTTPServer(('127.0.0.1', 0), Handler) as server:
        worker = Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            endpoint = f'http://127.0.0.1:{server.server_port}'
            yield endpoint, calls
        finally:
            server.shutdown()
            worker.join()


def output(module, path, **options):
    assert module.getantposalma(str(path), **options) is None
    result = json.loads(path.read_text())
    assert result['metadata'].pop('outfile') == str(path)
    assert result['metadata'].pop('timestamp')
    return result


def positions_for(vis):
    tool = table()
    assert tool.open(str(vis / 'ANTENNA'))
    try:
        return {tool.getcell('NAME', index):
                (tool.getcell('POSITION', index) + np.array([.003, -.007, .002]) * (index + 1)).tolist()
                for index in range(tool.nrows())}
    finally:
        tool.close()


def successful_requests(root, endpoint, calls, native):
    results = {}
    for label, options, expected_paths in (
        ('defaults', {'hosts': endpoint + '/positions'}, ['positions']),
        ('fallback', {'hosts': [endpoint + '/failure', endpoint + '/positions', 'unused'],
                      'tw': WINDOW, 'snr': 5.25, 'search': 'both_closest', 'firstintegration': False},
                     ['failure', 'positions']),
        ('retry', {'hosts': [endpoint + '/retry'], 'nretry': 1, 'rdelay': .01, 'snr': 0, 'search': ''},
                  ['retry', 'retry']),
    ):
        values, sequences = [], []
        for name, engine in (('casa', casa), ('native', native)):
            calls.clear()
            path = root / f'{label}-{name}.json'
            values.append(output(engine, path, asdm=ASDM, **options))
            sequences.append(calls[:])
        assert values[0] == values[1], (label, values)
        assert sequences[0] == sequences[1], (label, sequences)
        assert [name for name, _ in sequences[0]] == expected_paths
        results[label] = {'antennas': len(values[0]['data']), 'requests': len(expected_paths)}
    return results


def errors_and_replacement(root, endpoint, calls, native):
    results = {}
    for name, engine in (('casa', casa), ('native', native)):
        destination = root / f'preserve-{name}.json'
        destination.write_text('prior output')
        valid = {'asdm': ASDM, 'hosts': [endpoint + '/positions'], 'overwrite': True}
        for label, changed, expected in (
            ('no-overwrite', {'overwrite': False}, RuntimeError),
            ('no-hosts', {'hosts': []}, ValueError),
            ('missing-first-host', {'hosts': ['', endpoint]}, ValueError),
            ('bad-host', {'hosts': ['invalid host']}, ValueError),
            ('missing-asdm', {'asdm': ''}, ValueError),
            ('bad-window-count', {'tw': '2023-03-14T00:40:20'}, ValueError),
            ('bad-window-time', {'tw': 'today-ish,2023-03-14T00:40:20'}, ValueError),
            ('reversed-window', {'tw': ','.join(reversed(WINDOW.split(',')))}, ValueError),
            ('negative-snr', {'snr': -1}, ValueError),
            ('bad-snr', {'snr': '5'}, ValueError),
            ('bad-snr-kind', {'snr': [1]}, TypeError),
            ('negative-retries', {'nretry': -1}, ValueError),
            ('invalid-retry-delay', {'nretry': 1, 'rdelay': 0.}, ValueError),
            ('no-position-service', {'hosts': [endpoint + '/failure']}, RuntimeError),
            ('invalid-service-response', {'hosts': [endpoint + '/invalid']}, json.JSONDecodeError),
            ('empty-service-response', {'hosts': [endpoint + '/empty', endpoint + '/positions']}, RuntimeError),
        ):
            try:
                engine.getantposalma(str(destination), **{**valid, **changed})
            except expected:
                assert destination.read_text() == 'prior output'
            else:
                raise AssertionError(f'{name} accepted {label}')
            results[label] = {'preserved': True}
        output(engine, destination, **valid)
        directory = root / f'replace-{name}.json'
        directory.mkdir()
        (directory / 'prior').write_text('prior directory output')
        output(engine, directory, **valid)
        results['overwrite'] = {'replaced_file_and_directory': True}
        calls.clear()
        try:
            engine.getantposalma(asdm=ASDM, hosts=valid['hosts'])
        except ValueError:
            assert not calls
        else:
            raise AssertionError(f'{name} accepted an omitted outfile')
        results['missing-outfile'] = {'refused': True}
    return results


def calibrated_positions(root, vis, native):
    for label, engine in (('casa', casa), ('native', native)):
        source = root / f'fallback-{label}.json'
        for owner, writer in (('casa', casa), ('native', native)):
            product = root / f'offsets-{label}-{owner}.cal'
            generate(writer, vis, product, [('antpos', {'infile': str(source)})])
    reference = root / 'offsets-casa-casa.cal'
    return {
        'table_nrms': max(compare_tables(product, reference) for product in root.glob('offsets-*.cal')),
        'apply_nrms': apply_both(root, vis, reference, root / 'offsets-native-native.cal', native),
    }


def routine_parameters(root, vis, native):
    reference = inspect.signature(casa.gencal).parameters
    candidate = inspect.signature(native.gencal).parameters
    assert list(reference) == list(candidate)[:len(reference)]
    assert all(candidate[name].default == parameter.default for name, parameter in reference.items())
    outputs = []
    for name, engine in (('casa', casa), ('native', native)):
        path = root / f'inactive-service-options-{name}.cal'
        generate(engine, vis, path, [('amp', {'parameter': 1.01, 'endpoint': 'ignored',
                                              'timeout': -1, 'retry': -2, 'retry_wait_time': -3})])
        outputs.append(path)
    return {'casa_parameters': len(reference), 'table_nrms': compare_tables(*outputs)}


def live_service(root, native):
    from casatasks.private import task_getantposalma

    certificate_bundle = os.getenv('SSL_CERT_FILE', task_getantposalma.certifi.where())
    with patch.object(task_getantposalma.certifi, 'where', return_value=certificate_bundle):
        reference = output(casa, root / 'live-casa.json', asdm=ASDM)
    candidate = output(native, root / 'live-native.json', asdm=ASDM)
    assert reference == candidate
    assert reference['data']
    return {'antennas': len(reference['data']), 'identical': True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', required=True, type=Path)
    parser.add_argument('--native-bin', required=True, type=Path)
    parser.add_argument('--live', action='store_true')
    args = parser.parse_args()
    assert casa.version_string() == '6.7.6.14'
    os.environ['CASARS_CALIBRATE_BIN'] = str(args.native_bin.resolve())
    os.environ['CASARS_REQUIRE_TASK_BINARIES'] = '1'
    for name in ('no_proxy', 'NO_PROXY'):
        os.environ[name] = ','.join(filter(None, [os.environ.get(name), '127.0.0.1']))
    from casars import tasks as native

    reference = inspect.signature(casa.getantposalma).parameters
    candidate = inspect.signature(native.getantposalma).parameters
    assert list(candidate) == list(reference)
    assert all(candidate[name].default == parameter.default for name, parameter in reference.items())
    root = args.work.resolve()
    root.mkdir(parents=True)
    vis = root / 'alma.ms'
    make_cal_ms(str(vis))
    with frozen_service(positions_for(vis)) as (endpoint, calls):
        results = successful_requests(root, endpoint, calls, native)
        results.update(errors_and_replacement(root, endpoint, calls, native))
    results['gencal'] = calibrated_positions(root, vis, native)
    results['routine-service-options'] = routine_parameters(root, vis, native)
    if args.live:
        results['live-service'] = live_service(root, native)
    (root / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
    print(json.dumps(results, indent=2))


if __name__ == '__main__':
    main()
