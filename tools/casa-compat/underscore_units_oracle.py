#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA quantity parsing of the special underscore unit."""
import json
from casaconfig import config
config.data_auto_update = False
config.measures_auto_update = False
from casatools import quanta, version_string  # noqa: E402

UNITS = ['_', '_0', '_1', '_2', '_4', '_+2', '_-0', '_-1', '_-2', '(_)2',
         '(_)-2', '_._', '_/s2', 'm_2', 'm._2', 'm/_2']
q = quanta()
results = []
for unit in UNITS:
    for args in ([4, unit], ['4' + unit], [{'value': 4.0, 'unit': unit}]):
        item = {'args': args}
        try: item['result'] = q.quantity(*args)
        except Exception as error:
            item['error'] = {'type': type(error).__name__, 'message': str(error)}
        results.append(item)
print('CASA_RESULT=' + json.dumps({'casatools': version_string(), 'cases': results}))
