#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare Jy/K files, service queries, and mixed CASA/native workflows."""

import argparse
from contextlib import contextmanager
import inspect
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import threading
import time
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

import casatasks as casa
from casacore.tables import complete_ms_desc, makescacoldesc, maketabdesc, table
import certifi
import numpy as np

from applycal_oracle import copy_ms
from calibration_smoothcal_oracle import columns, compare_mapping, opened
from record_casa_returns import make_cal_ms


def gains(path):
    with opened(path) as source:
        assert source.info()['subType'] == 'G Jones'
        assert source.getkeyword('VisCal') == 'G Jones'
        return source.getcol('CPARAM')[:, 0, :]


def refusal(call, output):
    try:
        call()
    except Exception as exc:
        assert not output.exists()
        return type(exc), str(exc)
    raise AssertionError(f'Unexpected success: {output}')


def compare_file(root, vis, engines):
    factor_file = root/'factors.csv'
    name = vis.name
    factor_file.write_text(f'MS,Antenna,SPW,Pol,Factor\nother.ms,A0,0,I,invalid\n'
                           f'{name},A0,0,XX,4\n#ignore\nbad,row\n'
                           f'{name},A0,0,YY,9\n"{name}","A1",0,I,16\n'
                           f'"{name}",A0,0,XX,25\n{name},A2,0,unexpected,6.25')
    expected = np.array([[.1, .25, .4, 1., 1.], [1./3, .25, .4, 1., 1.]])
    products = {}
    for label, api in engines.items():
        gencal = root/f'{label}-gencal.cal'
        result = api.gencal(str(vis), str(gencal), 'jyperk', str(factor_file), 'model-fit',
                            0, 0, 0, -1, 'unused', 'unused', 'unused', [123.], False)
        assert result is None
        task = root/f'{label}-getjyperkalma.cal'
        tilde_path = '~/' + os.path.relpath(vis, Path.home())
        result = api.getjyperkalma(tilde_path, str(task), str(factor_file), spw='unused', pol='unused')
        assert result is None
        for path in (gencal, task):
            np.testing.assert_allclose(gains(path), expected, rtol=2e-7)
        compare_mapping(columns(gencal), columns(task), False)
        products[label] = gencal
    compare_mapping(columns(products['native']), columns(products['casa']), False)

    single = root/'single.csv'
    single.write_text(f'{name},A0,0,XX,4\n')
    for label, api in engines.items():
        updated = root/f'{label}-update.cal'
        api.gencal(str(vis), str(updated), 'jyperk', str(single))
        np.testing.assert_allclose(gains(updated)[:, 0], [.5, 1])
        with opened(updated) as cal:
            np.testing.assert_array_equal(cal.getcol('OBSERVATION_ID'), 0)
        api.gencal(str(vis), str(updated), 'jyperk', str(single))
        np.testing.assert_allclose(gains(updated)[:, 0], [.25, 1])
        with opened(updated) as cal:
            np.testing.assert_array_equal(cal.getcol('OBSERVATION_ID'), -1)

    for label, content in [('empty', ''), ('unmatched', 'other.ms,A0,0,XX,4\n'),
                           ('bad-number', f'{name},A0,0,XX,words\n'), ('missing', None),
                           ('endpoint', f'{name},A0,0,I,4\n')]:
        source = root/f'{label}.csv'
        if content is not None:
            source.write_text(content)
        for task_name in ('gencal', 'getjyperkalma'):
            results = []
            for engine, api in engines.items():
                output = root/f'{engine}-{task_name}-{label}.cal'
                options = {'caltype': 'jyperk'} if task_name == 'gencal' else {}
                options.update(endpoint='bogus' if label == 'endpoint' else 'asdm')
                task = getattr(api, task_name)
                results.append(refusal(lambda: task(str(vis), str(output), infile=str(source), **options), output))
            assert results[0] == results[1], (task_name, label, results)

    for task_name in ('gencal', 'getjyperkalma'):
        for vis_arg in ({}, {'vis': str(root/'missing.ms')}):
            results = []
            for label, api in engines.items():
                output = root/f'{label}-{task_name}-missing-vis.cal'
                task = getattr(api, task_name)
                options = {'caltype': 'jyperk'} if task_name == 'gencal' else {}
                results.append(refusal(lambda: task(caltable=str(output), **vis_arg, **options), output))
            assert results[0] == results[1] and results[0][0] is AssertionError, results

    # CASA publishes an unflagged nonfinite gain for these factors. Check the
    # native refusal before changing an existing table or creating a new one.
    invalid = root/'invalid-factor.csv'
    invalid.write_text(f'{name},A0,0,XX,4\n{name},A1,0,XX,0\n{name},A2,0,YY,-9\n')
    output = root/'casa-invalid.cal'
    casa.gencal(str(vis), str(output), 'jyperk', str(invalid))
    assert not np.isfinite(gains(output)[0, 1]) and not np.isfinite(gains(output)[1, 2])
    with opened(output) as cal:
        assert not cal.getcol('FLAG').any()
    current = columns(products['native'])
    try:
        engines['native'].getjyperkalma(str(vis), str(products['native']), str(invalid))
    except ValueError as error:
        assert 'finite positive gain' in str(error)
    else:
        raise AssertionError('Invalid Jy/K factor changed an existing table')
    compare_mapping(columns(products['native']), current, False)
    malformed = root/'bad-encoding.csv'
    malformed.write_bytes(f'{name},A0,0,'.encode() + b'\xc2,\xa2\n')
    for label, api in engines.items():
        output = root/f'{label}-encoding.cal'
        error_type, message = refusal(
            lambda: api.getjyperkalma(str(vis), str(output), str(malformed)), output
        )
        assert error_type is (UnicodeDecodeError if label == 'casa' else RuntimeError)
        assert 'utf-8' in message.lower()
    for factor in ('1e-40', '1e60'):
        invalid.write_text(2 * f'{name},A0,0,XX,{factor}\n')
        output = root/f'cumulative-{factor}.cal'
        error_type, message = refusal(
            lambda: engines['native'].getjyperkalma(str(vis), str(output), str(invalid)), output
        )
        assert error_type is RuntimeError and 'Accumulated Jy/K gain' in message
        try:
            engines['native'].getjyperkalma(str(vis), str(products['native']), str(invalid))
        except RuntimeError as error:
            assert 'Accumulated Jy/K gain' in str(error)
        else:
            raise AssertionError('Extreme Jy/K factors changed an existing table')
        compare_mapping(columns(products['native']), current, False)
    return products, expected


def apply_and_solve(root, vis, engines, products, expected):
    source = root/'preapply.ms'
    copy_ms(vis, source)
    with opened(source, True) as main:
        left, right = main.getcol('ANTENNA1'), main.getcol('ANTENNA2')
        factor = (expected[:, left] * expected[:, right])[:, None, :]
        original = main.getcol('DATA')
        weights = main.getcol('WEIGHT')
        perturbation = 1. + 2e-4 * np.cos(2 * left + right) + 1e-4j * np.sin(left * right)
        main.putcol('DATA', (factor * perturbation).astype('complex64') * np.ones_like(original))
    assert np.count_nonzero(factor != 1) > 0
    auto = (left == 0) & (right == 0)
    assert auto.any()
    reference = None
    solutions = None
    for writer, cal in products.items():
        for reader, api in engines.items():
            work = root/f'applied-{writer}-{reader}.ms'
            copy_ms(vis, work)
            api.applycal(str(work), gaintable=[str(cal)], calwt=[True], flagbackup=False)
            with opened(work) as main:
                flags = main.getcol('FLAG')
                corrected = main.getcol('CORRECTED_DATA')
                weight = main.getcol('WEIGHT')
            want = original / factor
            assert np.any(~flags[:, :, auto])
            np.testing.assert_allclose(corrected[~flags], want[~flags], rtol=2e-6)
            np.testing.assert_allclose(weight, weights * factor[:, 0]**2, rtol=2e-6)
            if reference is None:
                reference = flags, weight, corrected
            else:
                np.testing.assert_array_equal(flags, reference[0])
                np.testing.assert_allclose(weight, reference[1], rtol=2e-6)
                np.testing.assert_allclose(corrected[~flags], reference[2][~flags], rtol=2e-6)

            solved_ms = root/f'solved-{writer}-{reader}.ms'
            copy_ms(source, solved_ms)
            solved = root/f'solved-{writer}-{reader}.cal'
            api.gaincal(str(solved_ms), str(solved), field='PHASE', solint='int', refant='A3',
                        minsnr=0., minblperant=2, gaintable=[str(cal)], interp=['nearest'])
            result = gains(solved)
            with opened(solved) as table_tool:
                assert not table_tool.getcol('FLAG').any()
            np.testing.assert_allclose(result, 1., atol=4e-4)
            if solutions is None:
                solutions = result
            else:
                np.testing.assert_allclose(result, solutions, atol=1e-4)


@contextmanager
def database(responder):
    recorded = []

    class Request(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_GET(self):
            path = urlsplit(self.path)
            query = {key: values[0] for key, values in parse_qs(path.query).items()}
            recorded.append((path.path, query, time.monotonic()))
            status, body = responder(path.path, query, len(recorded))
            progressing = path.path.startswith('/progress/')
            if progressing:
                time.sleep(.65)
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            if progressing:
                self.wfile.flush()
                time.sleep(.65)
            try:
                self.wfile.write(body if isinstance(body, bytes) else json.dumps(body).encode())
            except BrokenPipeError:
                pass

    with ThreadingHTTPServer(('127.0.0.1', 0), Request) as server:
        worker = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': .02})
        worker.start()
        try:
            yield f'http://127.0.0.1:{server.server_port}', recorded
        finally:
            server.shutdown()
            worker.join()


def service_fixture(vis):
    with opened(vis/'SPECTRAL_WINDOW', True) as spw:
        spw.putcell('NAME', 0, 'TEST#ALMA_RB_03#BB_2#SW-01#FULL_RES')
        prototype = {key: spw.getcell(key, 0) for key in spw.colnames() if spw.iscelldefined(key, 0)}
        spw.addrows(1)
        for key, value in prototype.items():
            spw.putcell(key, 1, value)
        spw.putcell('NAME', 1, 'UNUSED#ALMA_RB_08#BB_4#SW-02#FULL_RES')
    with table(str(vis/'SPECTRAL_WINDOW'), readonly=False, ack=False) as spw:
        spw.addcols(maketabdesc(makescacoldesc('BBC_NO', 0)))
        spw.putcell('BBC_NO', 0, 2)
    with table(str(vis/'WEATHER'), complete_ms_desc('WEATHER'), nrow=3, ack=False) as weather:
        weather.putcol('TEMPERATURE', np.array([280., 920., 284.], dtype='float32'))
        weather.putcol('TEMPERATURE_FLAG', [False, True, False])
        with table(str(vis), readonly=False, ack=False) as main:
            main.putkeyword('WEATHER', weather)
    with opened(vis/'STATE', True) as state:
        if not state.nrows():
            state.addrows(1)
        state.putcell('OBS_MODE', 0, 'OBSERVE_TARGET#ON_SOURCE')
    with opened(vis, True) as main:
        rows = np.flatnonzero(main.getcol('ANTENNA1') == 0)
        row = int(rows[0])
        when = main.getcell('TIME', row)
        main.putcell('ANTENNA2', row, 0)
        main.putcol('STATE_ID', np.zeros(main.nrows(), dtype='int32'))
    with opened(vis/'POINTING', True) as pointing:
        if pointing.nrows():
            pointing.removerows(list(range(pointing.nrows())))
        pointing.putcolkeyword('DIRECTION', 'MEASINFO', {'type': 'direction', 'Ref': 'AZEL'})
        pointing.addrows(2)
        for row, angle in enumerate((30., 50.)):
            pointing.putcell('ANTENNA_ID', row, 0)
            pointing.putcell('TIME', row, when + 10. + row * 20.)
            pointing.putcell('INTERVAL', row, 20.)
            pointing.putcell('NUM_POLY', row, 0)
            pointing.putcell('DIRECTION', row, np.deg2rad([[150.], [angle]]))


def compare_services(root, vis, engines):
    service_outputs = {}
    interpolation_queries = {}
    asdm_factors = [{'MS': vis.name, 'Antenna': 'A0', 'Spwid': 0, 'factor': 4.},
                    {'MS': vis.name, 'Antenna': 'A0', 'Spwid': '0', 'factor': 9.},
                    {'MS': vis.name, 'Antenna': 'A1', 'Spwid': 1, 'factor': 64.},
                    {'MS': 'other.ms', 'Antenna': 'A2', 'Spwid': 0, 'factor': 81.}]
    fallback = root/'file-fallback.csv'
    fallback.write_text(f'{vis.name},A0,0,I,25\n')

    def send(path, query, index):
        if path.startswith('/timeout/'):
            time.sleep(1.2)
            return 503, b''
        if path.startswith('/syntax/'):
            return 200, b'{'
        if path.startswith('/empty/'):
            return 200, {'success': True, 'data': {'factors': []}}
        if path.startswith('/rejected/'):
            return 200, {'success': False, 'error': 'not found'}
        if path.startswith('/retry/') and index == 1:
            return 503, b''
        if path.endswith('/asdm/'):
            return 200, {'success': True, 'data': {'factors': asdm_factors}}
        factor = {'mean': 4.} if path.endswith('/interpolation/') else 16.
        return 200, {'success': True, 'query': query, 'data': {'factor': factor}}

    for label, api in engines.items():
        from casatasks.private.jyperk import JyPerKDatabaseClient
        with database(send) as (url, records), patch.object(JyPerKDatabaseClient, 'DEFAULT_BASE_URL', url):
            os.environ['JYPERKDB_URL'] = url
            via_file = root/f'{label}-gencal-precedence.cal'
            api.gencal(str(vis), str(via_file), 'jyperk', str(fallback))
            assert not records
            np.testing.assert_allclose(gains(via_file)[:, 0], .2, rtol=2e-7)
            via_web = root/f'{label}-gencal-service.cal'
            api.gencal(str(vis), str(via_web), 'jyperk', retry=1)
            assert len(records) == 1 and records[0][:2] == ('/asdm/', {'uid': 'uid://A002/X85c183/X36f'})
            np.testing.assert_allclose(gains(via_web)[:, 0], 1./6., rtol=2e-7)

        with database(send) as (url, records), patch.object(JyPerKDatabaseClient, 'DEFAULT_BASE_URL', f'{url}/rejected'):
            os.environ['JYPERKDB_URL'] = f'{url}/rejected'
            rejected = root/f'{label}-gencal-service-rejected.cal'
            try:
                api.gencal(str(vis), str(rejected), 'jyperk', retry=1)
            except RuntimeError as error:
                assert str(error) == 'Failed to get Jy/K factors from DB: not found'
            else:
                raise AssertionError('expected the Jy/K service rejection')
            assert len(records) == 1 and records[0][0] == '/rejected/asdm/'
            assert not rejected.exists()

        for case, host_path, backups, use_file, gain, paths in (
            ('retry', 'retry', [], False, 1./6., ['retry/asdm']*2),
            ('progress', 'progress', [], True, 1./6., ['progress/asdm']),
            ('timeout', 'timeout', [], True, .2, ['timeout/asdm']*2),
            ('backup', 'syntax', ['rejected', 'empty', 'success'], True, 1./6.,
             ['syntax/asdm', 'rejected/asdm', 'empty/asdm', 'success/asdm']),
            ('file-fallback', 'empty', ['rejected'], True, .2, ['empty/asdm', 'rejected/asdm']),
        ):
            with database(send) as (url, records):
                os.environ['JYPERKDB_URL'] = f'{url}/{host_path}'
                output = root/f'{label}-{case}.cal'
                api.getjyperkalma(str(vis), str(output), str(fallback) if use_file else '',
                                 'asdm', 1, 2, int(case == 'retry'), '', 'ignored',
                                 [f'{url}/{host}' for host in backups])
            assert [path.strip('/') for path, _, _ in records] == paths, records
            assert all(query == {'uid': 'uid://A002/X85c183/X36f'} for _, query, _ in records)
            if case == 'retry':
                assert .95 <= records[1][2] - records[0][2] <= 3
            np.testing.assert_allclose(gains(output)[:, 0], gain, rtol=2e-7)
            np.testing.assert_array_equal(gains(output)[:, 5:], 1.)
            service_outputs[label, case] = output

        for endpoint in ('interpolation', 'model-fit'):
            with database(send) as (url, records):
                os.environ['JYPERKDB_URL'] = url
                output = root/f'{label}-{endpoint}.cal'
                api.getjyperkalma(str(vis), str(output), endpoint=endpoint, spw='0', retry_wait_time=0)
            assert len(records) == 5 and all(path == f'/{endpoint}/' for path, _, _ in records)
            queries = [query for _, query, _ in records]
            for antenna, (_, query, _) in enumerate(records):
                assert query['antenna'] == f'A{antenna}' and query['baseband'] == '2' and query['band'] == '3'
                assert float(query['temperature']) == 282.
                assert float(query['frequency']) == 100003500000.
                assert query.get('delta_days') == ('1000' if endpoint == 'interpolation' else None)
                assert query['elevation'] == 'nan' if antenna else np.isclose(float(query['elevation']), 30.)
            for query in queries:
                for key in ('temperature', 'frequency', 'elevation'):
                    query[key] = 'nan' if query[key] == 'nan' else round(float(query[key]), 6)
            if endpoint in interpolation_queries:
                assert queries == interpolation_queries[endpoint], (queries, interpolation_queries[endpoint])
            else:
                interpolation_queries[endpoint] = queries
            expected = [.5, 1., 1., 1., 1.] if endpoint == 'interpolation' else [.25]*5
            wanted = np.broadcast_to([*expected, *([1.] * 5)], (2, 10))
            np.testing.assert_allclose(gains(output), wanted, rtol=2e-7)
            service_outputs[label, endpoint] = output
    os.environ.pop('JYPERKDB_URL', None)
    for case in ('retry', 'progress', 'timeout', 'backup', 'file-fallback', 'interpolation', 'model-fit'):
        compare_mapping(columns(service_outputs['native', case]), columns(service_outputs['casa', case]), False)


def compare_live(root, engines):
    vis = root/'uid___A002_X85c183_X36f.ms'
    make_cal_ms(str(vis), antenna_names=['DA61', 'PM03', 'PM04', 'A3', 'A4'])
    with opened(vis/'SPECTRAL_WINDOW', True) as spw:
        prototype = {key: spw.getcell(key, 0) for key in spw.colnames() if spw.iscelldefined(key, 0)}
        spw.addrows(17)
        for row in range(1, 18):
            for key, value in prototype.items():
                spw.putcell(key, row, value)
            spw.putcell('NAME', row, f'SPW{row}')
    with opened(vis/'DATA_DESCRIPTION', True) as description:
        description.putcell('SPECTRAL_WINDOW_ID', 0, 17)
    host = 'https://asa.alma.cl/science/jy-kelvins'
    trust = os.environ.get('SSL_CERT_FILE') or certifi.where()
    with patch.dict(os.environ, {'JYPERKDB_URL': host}), patch.object(certifi, 'where', return_value=trust):
        results = {}
        for engine, api in engines.items():
            output = root/f'{engine}-live.cal'
            then = time.monotonic()
            api.getjyperkalma(str(vis), str(output), spw='17', timeout=25, retry=1)
            print(f'{engine} live ASDM call returned a table in {time.monotonic()-then:.3f} s')
            with opened(output) as cal:
                selected = cal.getcol('SPECTRAL_WINDOW_ID') == 17
                data = cal.getcol('CPARAM')[:, 0, selected]
                assert selected.sum() == 5
                assert np.all(data[:, :3] != 1.) and np.all(data[:, 3:] == 1.)
                results[engine] = columns(output)
            print(f'{engine} live ASDM gains for SPW 17: {data[0].real.tolist()}')
    compare_mapping(results['native'], results['casa'], False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work', type=Path)
    parser.add_argument('--live', action='store_true', help='query the public ALMA ASDM endpoint')
    args = parser.parse_args()
    from casars import tasks as native
    public = [param.replace(annotation=inspect.Parameter.empty)
              for param in inspect.signature(native.getjyperkalma).parameters.values()
              if param.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD]
    assert inspect.Signature(public) == inspect.signature(casa.getjyperkalma)
    assert casa.version_string() == '6.7.6.14'
    os.environ.pop('JYPERKDB_URL', None)
    root = args.work.resolve()
    root.mkdir()
    engines = {'casa': casa, 'native': native}
    if args.live:
        compare_live(root, engines)
        return
    vis = root/'uid___A002_X85c183_X36f.ms'
    make_cal_ms(str(vis))
    with opened(vis, True) as main_table:
        auto = int(np.flatnonzero(main_table.getcol('ANTENNA1') == 0)[0])
        main_table.putcell('ANTENNA2', auto, 0)
        main_table.putcol('FLAG', np.zeros_like(main_table.getcol('FLAG')))
        main_table.putcol('FLAG_ROW', np.zeros_like(main_table.getcol('FLAG_ROW')))
    products, expected = compare_file(root, vis, engines)
    print('Both file tasks, table update, validation and atomic refusal agree')
    apply_and_solve(root, vis, engines, products, expected)
    print('Four writer/reader pairs agree through weighted apply and gain preapply')
    service_fixture(vis)
    compare_services(root, vis, engines)
    print('Three endpoints, retry wait, timeout, ordered fallback and query metadata agree')


if __name__ == '__main__':
    main()
