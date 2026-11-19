#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare supplied antenna positions, legacy VLA offsets and ALMA JSON."""

import argparse
import json
from json import JSONDecodeError
import os
from pathlib import Path
import subprocess
import sys

import casatasks as casa
from casatools import table
import numpy as np

from applycal_oracle import copy_ms
from calibration_gencal_gaincurve_oracle import compare_case, refuses
from calibration_gencal_specify_oracle import apply_both, compare_tables, generate, with_second_window
from gaincal_oracle import ephemeris_observation
from record_casa_returns import make_cal_ms


def pinned_crash(vis, output, kind, options):
    command = (
        'import json,resource,sys; resource.setrlimit(resource.RLIMIT_CORE,(0,0)); '
        'from casatasks import gencal; gencal(**json.load(sys.stdin))'
    )
    arguments = dict(vis=str(vis), caltable=str(output), caltype=kind, **options)
    reference = subprocess.run([sys.executable, '-c', command], input=json.dumps(arguments),
                               text=True, capture_output=True, timeout=30)
    assert reference.returncode < 0, reference.stderr
    assert not output.exists()


def file_inputs(root, source, native):
    alma = root / 'alma.ms'
    copy_ms(source, alma)
    tool = table()
    assert tool.open(str(alma / 'OBSERVATION'), nomodify=False)
    tool.putcell('TELESCOPE_NAME', 0, 'ALMA')
    tool.close()
    assert tool.open(str(alma / 'ANTENNA'))
    positions = {}
    for antenna, offset in ((1, [.04, -.01, .02]), (27, [.005, .003, -.007])):
        name = tool.getcell('NAME', antenna)
        positions[name] = (tool.getcell('POSITION', antenna) + np.array(offset)).tolist()
    tool.close()
    path = root / 'positions.json'
    path.write_text(json.dumps({'metadata': {'product_code': 'antposalma'}, 'data': positions}))
    result, _ = compare_case(root, 'alma-json', alma, 'antpos', {'infile': str(path)}, native)
    results = {'alma-json': result}
    name, original = next(iter(positions.items()))
    extended = root / 'extended-position.json'
    extended.write_text(json.dumps({'metadata': {'product_code': 'antposalma'},
                                    'data': {name: original + ['unused']}}))
    results['unused-coordinate'], _ = compare_case(
        root, 'unused-coordinate', alma, 'antpos', {'infile': str(extended)}, native)
    for label, content, expected_error in (
        ('missing-file', None, FileNotFoundError),
        ('invalid-json', '{', JSONDecodeError),
        ('no-metadata', '{}', KeyError),
        ('short-position', json.dumps({'metadata': {'product_code': 'antposalma'},
                                      'data': {name: original[:2]}}), IndexError),
    ):
        infile = root / f'{label}.json'
        if content is not None:
            infile.write_text(content)
        for engine, module in (('casa', casa), ('native', native)):
            output = root / f'{label}-{engine}.cal'
            try:
                generate(module, alma, output, [('antpos', {'infile': str(infile)})])
            except expected_error:
                assert not output.exists()
            else:
                raise AssertionError(f'{engine} accepted {label}')
        results[label] = {'refused': True}
    for label, vis, options in (
        ('unrelated-telescope', source, {}),
        ('mixed-antenna', alma, {'antenna': '1'}),
        ('mixed-polarization', alma, {'pol': 'X'}),
        ('mixed-position', alma, {'parameter': [.1, .2, .3]}),
    ):
        for engine, module in (('casa', casa), ('native', native)):
            output = root / f'{label}-{engine}.cal'
            try:
                generate(module, vis, output, [('antpos', {'infile': str(path), **options})])
            except ValueError:
                assert not output.exists()
            else:
                raise AssertionError(f'{engine} accepted {label}')
        results[label] = {'refused': True}
    expected = path.read_bytes()
    refused = root / 'protected-source.cal'
    try:
        generate(native, alma, refused, [('antpos', {
            'infile': str(path), 'output': str(path), 'overwrite': True,
        })])
    except ValueError as error:
        assert 'overlap' in str(error)
    else:
        raise AssertionError('gencal accepted a report over the source JSON')
    assert not refused.exists()
    assert path.read_bytes() == expected
    results['protected-source'] = {'refused': True}
    return results


def moving_position(root, native):
    case = root / 'moving-position'
    case.mkdir()
    fixed = case / 'fixed.ms'
    make_cal_ms(str(fixed), stokes='XX XY YX YY')
    moving, _ = ephemeris_observation(case, fixed, offset=(.002, -.004))
    options = {'antenna': '0,1', 'parameter': [17, 28, 34, 40, 59, 63]}
    result, _ = compare_case(case, 'calibration', fixed, 'antpos', options, native)
    tables = case / 'calibration'
    result['apply_nrms'] = apply_both(case, moving, tables / 'casa.cal', tables / 'native.cal', native)
    return result


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

    root = args.work.resolve()
    root.mkdir(parents=True, exist_ok=True)
    assert not any(root.iterdir()), 'work directory must be empty'
    vis = root / 'positions.ms'
    copy_ms(args.source.resolve(), vis)
    with_second_window(vis)
    tool = table()
    assert tool.open(str(vis))
    assert tool.getcolkeywords('TIME')['MEASINFO']['Ref'] == 'TAI'
    tool.close()
    cases = {
        'manual': ('antpos', {'antenna': '27,1', 'parameter': [.1, .2, .3, .4, .5, .6]}),
        'cycle-triple': ('antpos', {'antenna': '27,1', 'parameter': [.1, .2, .3]}),
        'extra-triple': ('antpos', {'antenna': '1', 'parameter': [.1, .2, .3, .4, .5, .6]}),
        'range': ('antpos', {'antenna': '1~3', 'parameter': [.1, .2, .3]}),
        'empty-antenna-ids': ('antpos', {'antenna': '/VA01&VA02/', 'parameter': [.1, .2, .3]}),
        'legacy-vla': ('antposvla', {'antenna': '1', 'parameter': [1, 0, 0],
                                   'spw': '1', 'pol': 'ignored'}),
        'legacy-repeated': ('antposvla', {'antenna': '1~3', 'parameter': [1, 0, 0]}),
    }
    results = {}
    for label, (kind, options) in cases.items():
        results[label], _ = compare_case(root, label, vis, kind, options, native,
                                          check_apply=label == 'manual')
    mapped = root / 'forced-window-map'
    mapped.mkdir()
    results['forced-window-map'] = {'apply_nrms': apply_both(
        mapped, vis, root / 'manual' / 'casa.cal', root / 'manual' / 'native.cal', native,
        spwmap=[1, 1])}
    results['incomplete-triple'] = refuses(root, 'incomplete-triple', vis, 'antpos',
                                          {'antenna': '1', 'parameter': [.1]}, native)
    for label, kind, options in (
        ('wildcard', 'antpos', {'antenna': '*', 'parameter': [.1, .2, .3]}),
        ('legacy-all', 'antposvla', {'parameter': [1, 0, 0]}),
    ):
        pinned_crash(vis, root / f'{label}-crash.cal', kind, options)
        reference, candidate = (root / f'{label}-{engine}.cal' for engine in ('casa', 'native'))
        comparison = {**options, 'antenna': '0~27'}
        if kind == 'antposvla':
            comparison['parameter'] = options['parameter'] * 28
        generate(casa, vis, reference, [(kind, comparison)])
        generate(native, vis, candidate, [(kind, options)])
        results[label] = {'table_nrms': compare_tables(candidate, reference)}
    results.update(file_inputs(root, vis, native))
    results['moving-position'] = moving_position(root, native)

    initial = [('antpos', {'antenna': '1', 'parameter': [.03, .01, -.02]})]
    additional = [('antposvla', {'antenna': '1', 'parameter': [.01, -.03, .04]})]
    reference = root / 'accumulated-casa.cal'
    generate(casa, vis, reference, initial + additional)
    errors = []
    for first_name, first in (('casa', casa), ('native', native)):
        for second_name, second in (('casa', casa), ('native', native)):
            output = root / f'append-{first_name}-{second_name}.cal'
            generate(first, vis, output, initial)
            generate(second, vis, output, additional)
            errors.append(compare_tables(output, reference))
    results['accumulation'] = {'table_nrms': max(errors)}
    (root / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
    print(json.dumps(results, indent=2))


if __name__ == '__main__':
    main()
