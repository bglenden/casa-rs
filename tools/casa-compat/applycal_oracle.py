#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Same-input CASA/public-Python applycal comparisons, with hard failures.

The initial small corpus establishes numerical, mutation and API/state gaps.
Its recorded latencies are not dataset-scale performance acceptance.
"""
from __future__ import annotations

import argparse
import contextlib
import inspect
import io
import json
import os
from pathlib import Path
import resource
import time

import numpy as np
import casatasks as casa
from casatools import table

from calibration_lifecycle_oracle import contents, equal, snapshot
from record_casa_returns import make_cal_ms


def plain(value):
    if isinstance(value, dict):
        return {str(key): plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [plain(item) for item in value]
    return value.tolist() if hasattr(value, 'tolist') else value


def history(vis):
    return contents(vis/'HISTORY')


def copy_ms(source, destination):
    tool = table()
    tool.open(str(source))
    copied = tool.copy(str(destination), deep=True, valuecopy=True)
    copied.close()
    tool.close()


def run(root, native_bin):
    from casars import tasks as native
    assert casa.version_string() == '6.7.6.14'
    reference = inspect.signature(casa.applycal)
    actual = {name: parameter for name, parameter in inspect.signature(native.applycal).parameters.items()
              if parameter.kind != inspect.Parameter.KEYWORD_ONLY}
    assert list(actual) == list(reference.parameters)
    for name, parameter in reference.parameters.items():
        assert actual[name].kind == parameter.kind, name
        assert actual[name].default == parameter.default, name
    os.environ['CASARS_CALIBRATE_BIN'] = str(native_bin.resolve())
    os.environ['CASARS_REQUIRE_TASK_BINARIES'] = '1'
    os.environ['CASA_RS_CALIBRATION_PERF'] = '1'
    os.environ['CASA_RS_CALIBRATION_PERF_DIR'] = str(root/'profiles')
    os.environ['CASA_RS_CALIBRATION_TIMING'] = '1'
    (root/'profiles').mkdir()
    source, constant, solved = root/'source.ms', root/'constant.gcal', root/'solved.gcal'
    channelized = root/'channelized.bcal'
    make_cal_ms(str(source))
    casa.gencal(vis=str(source), caltable=str(constant), caltype='amp', parameter=[2.])
    casa.gaincal(vis=str(source), caltable=str(solved), refant='A0', solint='int')
    casa.bandpass(vis=str(source), caltable=str(channelized), refant='A0', solint='inf', minsnr=0.)
    tool = table()
    tool.open(str(channelized), nomodify=False)
    gains = tool.getcol('CPARAM')
    gains[:] = np.arange(1, gains.shape[1]+1)[None, :, None]
    tool.putcol('CPARAM', gains)
    flags = tool.getcol('FLAG')
    flags[:] = False
    flags[:, 4, :] = True
    tool.putcol('FLAG', flags)
    tool.close()
    tool.open(str(source), nomodify=False)
    if 'CORRECTED_DATA' in tool.colnames():
        tool.removecols(['CORRECTED_DATA'])
    tool.close()
    result = dict(casa_version=casa.version_string(), signature=str(reference), cases={}, failures=[],
                  timing_scope='Small-fixture public-task latency and CPU only; not dataset-scale acceptance')

    def case(name, extra=None, gain=constant, precorrected=False, repetitions=1, weight_seed=None, spectral_sigma=False):
        records, paths = {}, {}
        before_gain = snapshot(gain)
        for engine, module in [('casa', casa), ('native', native)]:
            path = root/f'{name}.{engine}.ms'
            paths[engine] = path
            copy_ms(source, path)
            if precorrected:
                tool.open(str(path), nomodify=False)
                descriptor = tool.getcoldesc('DATA')
                descriptor['name'] = 'CORRECTED_DATA'
                descriptor['dataManagerType'] = 'StandardStMan'
                descriptor['dataManagerGroup'] = 'CorrectedDataTest'
                tool.addcols({'CORRECTED_DATA': descriptor})
                tool.putcol('CORRECTED_DATA', tool.getcol('DATA')*(7.+2.j))
                tool.close()
            if weight_seed is not None:
                tool.open(str(path), nomodify=False)
                for column, value in zip(['WEIGHT', 'SIGMA'], weight_seed):
                    tool.putcol(column, np.full_like(tool.getcol(column), value))
                tool.close()
            if spectral_sigma:
                tool.open(str(path), nomodify=False)
                data = tool.getcol('DATA')
                for column in ['WEIGHT_SPECTRUM', 'SIGMA_SPECTRUM']:
                    descriptor = tool.getcoldesc('DATA')
                    descriptor.update(name=column, valueType='float', dataManagerType='StandardStMan',
                                      dataManagerGroup=column+'Test')
                    tool.addcols({column: descriptor})
                    values = np.full(data.shape, 99., dtype=np.float32) if column == 'WEIGHT_SPECTRUM' else np.broadcast_to(np.linspace(1., 3., data.shape[1], dtype=np.float32)[None, :, None], data.shape).copy()
                    tool.putcol(column, values)
                tool.close()
            before = history(path)['TIME'].size
            arguments = dict(vis=str(path), gaintable=[str(gain)], flagbackup=False)
            arguments.update(extra or {})
            captured = io.StringIO()
            preload = os.environ.pop('LD_PRELOAD', None) if engine == 'native' else None
            cpu_start = sum(resource.getrusage(kind).ru_utime+resource.getrusage(kind).ru_stime
                            for kind in [resource.RUSAGE_SELF, resource.RUSAGE_CHILDREN])
            start = time.perf_counter()
            try:
                with contextlib.redirect_stderr(captured):
                    for _ in range(repetitions):
                        value = module.applycal(**arguments)
                record = dict(value=plain(value))
            except (ValueError, TypeError, RuntimeError, OSError) as error:
                record = dict(error=type(error).__name__, message=str(error))
            finally:
                if preload is not None:
                    os.environ['LD_PRELOAD'] = preload
            record['wall_seconds'] = time.perf_counter()-start
            record['cpu_seconds'] = sum(resource.getrusage(kind).ru_utime+resource.getrusage(kind).ru_stime
                                        for kind in [resource.RUSAGE_SELF, resource.RUSAGE_CHILDREN])-cpu_start
            record['stderr'] = captured.getvalue()
            record['history'] = {column: plain(values[..., before:]) for column, values in history(path).items()}
            backup = Path(str(path)+'.flagversions')
            record['flag_versions'] = sorted(item.name for item in backup.glob('flags.*')) if backup.exists() else []
            records[engine] = record
        assert before_gain == snapshot(gain), f'{name}: input gain table mutated'
        def check(component, operation):
            try:
                operation()
            except (AssertionError, KeyError) as error:
                result['failures'].append(dict(case=name, component=component, message=str(error)))
        def compare_main():
            a, b = contents(paths['casa']), contents(paths['native'])
            assert a.keys() == b.keys(), (a.keys(), b.keys())
            equal(paths['casa'], paths['native'])
            for column in ['TIME', 'INTERVAL', 'TIME_CENTROID']:
                np.testing.assert_allclose(a[column], b[column], rtol=0, atol=1e-4, err_msg=column)
        def compare_return():
            assert records['casa'].get('value') == records['native'].get('value')
            assert ('error' in records['casa']) == ('error' in records['native']), records
        def compare_history():
            left, right = records['casa']['history'], records['native']['history']
            assert len(left['TIME']) == len(right['TIME']), (len(left['TIME']), len(right['TIME']))
        def compare_backup():
            assert len(records['casa']['flag_versions']) == len(records['native']['flag_versions'])
        for component, operation in [('return', compare_return), ('MAIN', compare_main),
                                     ('HISTORY', compare_history), ('flagbackup', compare_backup)]:
            check(component, operation)
        result['cases'][name] = dict(arguments=plain(extra or {}), repetitions=repetitions,
                                    weight_seed=weight_seed, spectral_sigma=spectral_sigma, records=records)
        (root/'results.json').write_text(json.dumps(result, indent=2)+'\n')
        print(name, 'FAILED' if any(item['case'] == name for item in result['failures']) else 'passed', flush=True)

    case('default')
    case('no-calwt', dict(calwt=False))
    case('repeat-apply', repetitions=2)
    case('weight-from-sigma', weight_seed=(3., 2.))
    case('spectral-sigma', spectral_sigma=True)
    case('calonly', dict(applymode='calonly'))
    case('trial', dict(applymode='trial'))
    case('field', dict(field='PHASE'))
    case('preserve-unselected', dict(field='PHASE'), precorrected=True)
    case('channels', dict(spw='0:2~5'))
    case('channels-field', dict(field='PHASE', spw='0:2~5'), precorrected=True)
    case('channels-overlap', dict(spw='0:0~2;2~4'), precorrected=True, spectral_sigma=True)
    case('channels-existing', dict(spw='0:0~2'), precorrected=True)
    case('channels-spectral', dict(spw='0:0~2'), precorrected=True, spectral_sigma=True)
    case('channels-stride', dict(spw='0:0~6^2'), precorrected=True, spectral_sigma=True)
    case('channels-calonly', dict(spw='0:0~6^2', applymode='calonly'), precorrected=True)
    case('channels-no-calwt', dict(spw='0:0~6^2', calwt=False), precorrected=True, spectral_sigma=True)
    case('channels-trial', dict(spw='0:0~6^2', applymode='trial'), precorrected=True, spectral_sigma=True)
    case('channels-bandpass', dict(spw='0:2~6^2'), gain=channelized, precorrected=True)
    case('channels-bandpass-no-calwt', dict(spw='0:2~6^2', calwt=False), gain=channelized, precorrected=True)
    case('flagbackup', dict(flagbackup=True))
    case('solved-gains', gain=solved)
    for mode in ['calflagstrict', 'flagonly', 'flagonlystrict']:
        case(mode, dict(applymode=mode), gain=solved)
    case('parang-alone', dict(gaintable=[], parang=True))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--native-bin', type=Path, required=True)
    args = parser.parse_args()
    root = args.work.resolve()
    root.mkdir(parents=True, exist_ok=True)
    assert not any(root.iterdir()), 'work directory must be empty'
    native_bin = args.native_bin.resolve()
    os.chdir(root)
    result = run(root, native_bin)
    assert not result['failures'], [(item['case'], item['component'], item['message'][:180]) for item in result['failures']]


if __name__ == '__main__':
    main()
