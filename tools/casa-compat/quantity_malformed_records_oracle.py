#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA quantity handling for malformed and floating-array records."""
import json
from casaconfig import config
config.data_auto_update = False
config.measures_auto_update = False
from casatools import quanta, version_string  # noqa: E402

VALUES = ['bad', None, True, False, [], {}, [1.0], [1.0, 2.0], 1, 1.5]
q = quanta()
results = []
for value in VALUES:
    for method in ('quantity', 'getunit', 'add'):
        args = [{'value': value, 'unit': 'm'}] + ([1.0] if method == 'add' else [])
        case = {'method': method, 'args': args}
        try:
            result = getattr(q, method)(*args)
            if isinstance(result, dict) and hasattr(result.get('value'), 'tolist'):
                result['value'] = result['value'].tolist()
            case['result'] = result
        except Exception as error:
            case['error'] = {'type': type(error).__name__, 'message': str(error)}
        results.append(case)
print('CASA_RESULT=' + json.dumps({'casatools': version_string(), 'cases': results}))
