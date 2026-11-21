#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare VLA calibrator model retrieval and table interchange with CASA."""

import argparse
import inspect
import json
import os
from pathlib import Path
import subprocess
from unittest.mock import patch

import casatasks as casa
from casatasks.private import task_getcalmodvla
from casatools import componentlist, table
from casars import tasks as rust
from casars.tools import componentlist as native_componentlist
import numpy as np

from calibration_getantposalma_oracle import frozen_service


LIVE_SERVICE = 'https://obs.vla.nrao.edu/calmodvla'


def frozen_model():
    def component(ra, dec, value, index):
        rad = lambda angle: {'unit': 'rad', 'value': angle}
        return {
            'flux': {'error': [0., 0., 0., 0.], 'polarisation': 'Stokes',
                     'unit': 'Jy', 'value': value},
            'label': 'VLA reference',
            'shape': {'direction': {'error': {'latitude': rad(0.), 'longitude': rad(0.)},
                                    'm0': rad(ra), 'm1': rad(dec),
                                    'refer': 'J2000', 'type': 'direction'},
                      'type': 'Point'},
            'spectrum': {'error': 0.,
                         'frequency': {'m0': {'unit': 'GHz', 'value': 1.5},
                                       'refer': 'LSRK', 'type': 'frequency'},
                         'index': index, 'stokesindex': [index], 'type': 'Spectral Index'},
        }

    return {'nelements': 2,
            'component0': component(.1, .4, [2.5, .14, -.05, 0.], -.71),
            'component1': component(.11, .39, [.7, 0., .08, -.012], -.55)}


def compare_records(actual, expected):
    assert actual.keys() == expected.keys(), (actual.keys(), expected.keys())
    for name, baseline in expected.items():
        if isinstance(baseline, dict):
            compare_records(actual[name], baseline)
        else:
            np.testing.assert_equal(actual[name], baseline, err_msg=name)


def compare_tables(actual, expected):
    contents = []
    for path in (expected, actual):
        reader = table()
        assert reader.open(str(path))
        assert reader.info()['type'] == 'Component List'
        metadata = reader.getkeyword('web_service')
        assert metadata['response']['nelements'] == reader.nrows() and reader.nrows() > 0
        assert reader.done()
        results = []
        for factory in (componentlist, native_componentlist):
            tool = factory()
            assert tool.open(str(path))
            results.append(tool.torecord())
            assert tool.done()
        contents.append((metadata, results))
    reference, received = contents
    compare_records(received[0], reference[0])
    for expected_record, actual_record in zip(reference[1], received[1]):
        compare_records(actual_record, expected_record)


def valid_calls(root, endpoint, calls):
    patterns = (
        ('name', {'source': '3c286', 'obsdate': np.float64(60000.75), 'band': 'l',
                  'refdate': np.int64(60123), 'hosts': endpoint + '/positions'},
         ['positions'], {'source': ['3C286'], 'band': ['l'], 'obsdate': ['60000'],
                         'refdate': ['60123']}),
        ('direction', {'direction': 'J2000 01:37:41.1 33.09.32', 'band': 'Q',
                       'obsdate': '2023-02-25', 'refdate': .75,
                       'hosts': [endpoint + '/failure', endpoint + '/invalid',
                                 endpoint + '/empty', endpoint + '/positions', 'bad-host']},
         ['failure', 'invalid', 'empty', 'positions'],
         {'position': ['J2000 01:37:41.1 33.09.32'], 'band': ['Q'], 'obsdate': ['60000'],
          'refdate': ['0']}),
    )
    for name, options, paths, query in patterns:
        results = []
        for engine, module in (('casa', casa), ('rust', rust)):
            calls.clear()
            path = root / f'{name}-{engine}.cl'
            assert module.getcalmodvla(str(path), **options) is None
            assert [tag for tag, _ in calls] == paths
            assert all(args == (query | {'type': ['setjy']}) for _, args in calls)
            results.append(path)
        compare_tables(results[1], results[0])
        print(name, paths, 'two cross-opened components', flush=True)

    options = dict(source='3C286', band='L', obsdate=60000, hosts=[endpoint + '/positions'])
    calls.clear()
    date = root / 'calendar-reference.cl'
    try:
        casa.getcalmodvla(str(date), refdate='2023-02-25', **options)
    except TypeError:
        assert not calls and not date.exists()
    else:
        raise AssertionError('expected the pinned refdate string comparison to fail')
    assert rust.getcalmodvla(str(date), refdate='2023-02-25', **options) is None
    assert calls[0][1]['refdate'] == ['60000']
    reader = table()
    assert reader.open(str(date))
    assert reader.getkeyword('web_service')['refdate'] == '2023-02-25'
    assert reader.done()


def invalid_calls(root, endpoint, calls):
    for label, module in (('casa', casa), ('rust', rust)):
        path = root / f'prior-{label}.cl'
        path.write_text('prior calibration model')
        defaults = dict(overwrite=True, source='3C286', band='L', obsdate=60000,
                        hosts=[endpoint + '/positions'])
        cases = (
            ({'overwrite': False}, RuntimeError), ({'source': ''}, ValueError),
            ({'source': '', 'direction': 'invalid'}, ValueError),
            ({'direction': 'J2000 0deg 0deg'}, ValueError),
            ({'band': ''}, ValueError), ({'band': 'BAD'}, ValueError),
            ({'obsdate': '20230225'}, ValueError), ({'hosts': []}, ValueError),
            ({'hosts': ['bad-host']}, ValueError),
            ({'hosts': [endpoint + '/failure', endpoint + '/empty']}, RuntimeError),
        )
        for changes, error_type in cases:
            try:
                module.getcalmodvla(str(path), **(defaults | changes))
            except error_type:
                assert path.read_text() == 'prior calibration model', changes
            else:
                raise AssertionError((label, changes))
        assert module.getcalmodvla(str(path), **defaults) is None
        assert path.is_dir()
        assert module.getcalmodvla(str(path), **defaults) is None
    compare_tables(root / 'prior-rust.cl', root / 'prior-casa.cl')


def cli_call(root, endpoint, calls):
    path = root / 'cli.cl'
    calls.clear()
    subprocess.run([os.environ['CASARS_CALIBRATE_BIN'], 'getcalmodvla', '--outfile', str(path),
                    '--source', '3C286', '--band', 'L', '--obsdate', '60000',
                    '--refdate', '2023-02-25', '--hosts', endpoint + '/positions',
                    '--no-save-last'], check=True)
    assert calls == [('positions', {'type': ['setjy'], 'source': ['3C286'], 'band': ['L'],
                                    'obsdate': ['60000'], 'refdate': ['60000']})]
    tool = componentlist()
    assert tool.open(str(path)) and tool.length() == 2 and tool.done()


def live(root):
    default = {'source': '3C286', 'band': 'L', 'obsdate': 60000, 'hosts': [LIVE_SERVICE]}
    pinned = root / 'live-casa.cl'
    native = root / 'live-rust.cl'
    ca_bundle = os.environ.get('SSL_CERT_FILE', task_getcalmodvla.certifi.where())
    with patch.object(task_getcalmodvla.certifi, 'where', return_value=ca_bundle):
        assert casa.getcalmodvla(str(pinned), **default) is None
    assert rust.getcalmodvla(str(native), **default) is None
    compare_tables(native, pinned)
    reader = table()
    assert reader.open(str(native))
    print('live NRAO model components', reader.nrows(), flush=True)
    assert reader.nrows() > 10 and reader.done()


def recorded_response(root, document):
    with frozen_service(document) as (endpoint, calls):
        options = {'source': '3C286', 'band': 'L', 'obsdate': 60000,
                   'hosts': [endpoint + '/positions']}
        paths = [root / f'captured-{name}.cl' for name in ('casa', 'rust')]
        for module, path in zip((casa, rust), paths):
            assert module.getcalmodvla(str(path), **options) is None
        assert len(calls) == 2
    compare_tables(paths[1], paths[0])
    print('captured NRAO model components', document['nelements'], flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    parser.add_argument('--live', action='store_true')
    parser.add_argument('--captured', type=Path, help='recorded JSON from the official NRAO service')
    args = parser.parse_args()
    args.root.mkdir()
    actual = inspect.signature(rust.getcalmodvla).parameters
    expected = inspect.signature(casa.getcalmodvla).parameters
    assert list(actual) == list(expected)
    for name, parameter in expected.items():
        assert (actual[name].kind, actual[name].default) == (parameter.kind, parameter.default)
    with frozen_service(frozen_model()) as (endpoint, calls):
        valid_calls(args.root, endpoint, calls)
        invalid_calls(args.root, endpoint, calls)
        cli_call(args.root, endpoint, calls)
    if args.captured is not None:
        recorded_response(args.root, json.loads(args.captured.read_text()))
    if args.live:
        live(args.root)


if __name__ == '__main__':
    main()
