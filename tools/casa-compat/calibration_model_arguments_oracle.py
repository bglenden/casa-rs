#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare public model task argument coercion, errors and scratch mutations."""

import argparse
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory

import casatasks as casa
import numpy as np

from calibration_ft_oracle import compare_outputs, component, coordinates
from calibration_scratch_oracle import prepare, source_data, state


def cases(point):
    return (
        ('clearcal-str', 'clearcal', {'field': np.str_('0'), 'addmodel': True}, None, True),
        ('clearcal-null', 'clearcal', {'intent': None},
         (AssertionError, "{'intent': ['null value not allowed']}"), False),
        ('clearcal-bool-schema', 'clearcal', {'addmodel': 0, 'spw': 0},
         (AssertionError, "{'addmodel': ['must be of cBool type'], 'spw': ['must be of cStr type']}"), False),
        ('clearcal-bool-tool', 'clearcal', {'addmodel': np.bool_(True)},
         (TypeError, "in method 'calibrater_open', argument 5 of type 'bool'"), False),
        ('delmod-field-schema', 'delmod', {'field': ['0']},
         (AssertionError, "{'field': ['must be of cStr type']}"), False),
        ('delmod-otf-tool', 'delmod', {'otf': np.bool_(False)},
         (TypeError, "in method 'calibrater_delmod', argument 2 of type 'bool'"), False),
        ('delmod-scr-tool', 'delmod', {'scr': np.bool_(True)},
         (TypeError, "in method 'calibrater_delmod', argument 5 of type 'bool'"), False),
        ('ft-int32', 'ft', {'nterms': np.int32(1), 'complist': str(point), 'usescratch': True},
         None, True),
        ('ft-int64', 'ft', {'nterms': np.int64(1), 'complist': str(point), 'usescratch': True},
         None, True),
        ('ft-bool-open', 'ft', {'usescratch': np.bool_(True), 'complist': str(point)},
         (TypeError, "in method 'imager_open', argument 4 of type 'bool'"), False),
        ('ft-bool-predict', 'ft', {'incremental': np.bool_(True), 'complist': str(point), 'usescratch': True},
         (TypeError, "in method 'imager_ft', argument 4 of type 'bool'"), True),
        ('ft-tuple', 'ft', {'model': (), 'complist': str(point), 'usescratch': True},
         (AssertionError, "{'model': ['must be of cVariant type']}"), False),
        ('ft-array', 'ft', {'model': np.array(['missing.image']), 'usescratch': True},
         (ValueError, "The model image must be a string or a list of strings (or '' or [])"), True),
        ('ft-mixed-model', 'ft', {'model': ['', 1], 'usescratch': True},
         (ValueError, 'Model image names must be strings'), True),
        ('ft-model-order', 'ft', {'model': ['missing.image', 1], 'usescratch': True,
                                  'incremental': np.bool_(True)},
         (ValueError, 'Model image missing.image cannot be found'), True),
        ('ft-taylor-reference-order', 'ft', {'nterms': 2, 'model': ['', str(point.parent)],
                                            'usescratch': True, 'incremental': np.bool_(True)},
         (RuntimeError, 'File name is empty'), True),
        ('ft-taylor-reference-quantity', 'ft', {'nterms': 2, 'model': ['', str(point.parent)],
                                               'reffreq': 'not a frequency', 'usescratch': True},
         (RuntimeError, str(("*** Error *** In conversion of reffreq='", 'not a frequency',
                             "' to a numerical value"))), True),
        ('uvsub-bool-schema', 'uvsub', {'reverse': 0},
         (AssertionError, "{'reverse': ['must be of cBool type']}"), False),
        ('uvsub-bool-tool', 'uvsub', {'reverse': np.bool_(True)},
         (TypeError, "in method 'ms_uvsub', argument 2 of type 'bool'"), False),
        ('uvmodelfit-vary-scalar', 'uvmodelfit', {'varypar': np.bool_(True)},
         (AssertionError, "{'varypar': ['must be of cBoolVec type']}"), False),
        ('uvmodelfit-inactive-variant', 'uvmodelfit', {'selectdata': False, 'uvrange': ()},
         (AssertionError, "{'uvrange': ['must be of cVariant type']}"), False),
        ('uvmodelfit-null-shape', 'uvmodelfit', {'comptype': None},
         (AssertionError, "{'comptype': ['unallowed value None', 'null value not allowed']}"), False),
        ('uvmodelfit-empty-selection', 'uvmodelfit', {'field': '0', 'scan': '999'},
         (RuntimeError, 'Error in data selection specification: MSSelectionNullSelection : The selected table has zero rows.'),
         True),
    )


def compare(root, source, native, point):
    results = {}
    for label, name, arguments, failure, model_added in cases(point):
        outcomes = []
        for engine, api in (('casa', casa), ('native', native)):
            path = root / f'{label}-{engine}.ms'
            prepare(source, path, virtual=True)
            try:
                assert getattr(api, name)(str(path), **arguments) is None
            except (AssertionError, TypeError, ValueError, RuntimeError) as error:
                assert failure is not None, (label, engine, error)
                expected_type, message = failure
                assert type(error) is expected_type, (label, engine, type(error), str(error))
                if label == 'ft-taylor-reference-order':
                    # The C++ image factory also prints its source location.
                    assert message in str(error), (label, engine, str(error))
                else:
                    assert str(error) == message, (label, engine, str(error))
            else:
                assert failure is None, (label, engine)
            outcome = state(path)
            assert ('MODEL_DATA' in outcome[0]) == model_added, (label, engine)
            outcomes.append(outcome)
        compare_outputs(outcomes[1], outcomes[0])
        results[label] = {'paired': True, 'model_initialized': model_added}
        print(label, results[label], flush=True)
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--native-bin', type=Path, required=True)
    args = parser.parse_args()
    assert casa.version_string() == '6.7.6.14'
    os.environ['CASARS_CALIBRATE_BIN'] = str(args.native_bin.resolve())
    os.environ['CASARS_REQUIRE_TASK_BINARIES'] = '1'
    from casars import tasks

    args.work.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=args.work) as scratch:
        root = Path(scratch)
        source = root / 'source.ms'
        source_data(source, stokes='XX XY YX YY')
        point = root / 'point.cl'
        component(point, coordinates(source), 2.)
        results = compare(root, source, tasks, point)
    (args.work / 'results.json').write_text(json.dumps(results, indent=2) + '\n')


if __name__ == '__main__':
    main()
