#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA quanta.atan2 values, units and errors."""
import json
import math
from casaconfig import config
config.data_auto_update = False
config.measures_auto_update = False
from casatools import quanta, version_string  # noqa: E402

VALUES = [0.0, -0.0, 1.0, -1.0, 2.0, '50%', '2_', '1rad', '1m', None, 'nonsense', {'value': [1.0, 2.0], 'unit': ''}]
q = quanta()
results = []
for x in VALUES:
    for y in VALUES:
        item = {'x': x, 'y': y}
        try:
            result = q.atan2(x, y)
            if math.isnan(result['value']):
                result['value'] = 'NaN'
            item['result'] = result
        except Exception as error:
            item['error'] = {'type': type(error).__name__, 'message': str(error)}
        results.append(item)
print('CASA_RESULT=' + json.dumps({'casatools': version_string(), 'cases': results}))
