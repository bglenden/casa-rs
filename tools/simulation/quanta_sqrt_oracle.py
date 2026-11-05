#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA's quanta.sqrt quantities and errors."""
import json
import math
from casaconfig import config
config.data_auto_update = False
config.measures_auto_update = False
from casatools import quanta, version_string  # noqa: E402

VALUES = [0.0, -0.0, 1.0, -1.0, 4.0, 2.0, None, '4m2', '4km2',
          '4cm2', '4m', '4m/s', '4Hz2', '4s-2', '4kg2.m2/s4',
          '50%', '4rad', '4deg2', '4arcsec2', '4sr2', '4_2',
          {'value': 9.0, 'unit': 'm2'}, {'value': [1.0, 4.0], 'unit': 'm2'},
          [1.0, 4.0], 'bad', '', 'NaN', 'inf']
q = quanta()
results = []
for value in VALUES:
    item = {'input': value}
    try:
        result = q.sqrt(value)
        if math.isnan(result['value']): result['value'] = 'NaN'
        elif math.isinf(result['value']): result['value'] = 'Infinity' if result['value'] > 0 else '-Infinity'
        item['result'] = result
    except Exception as error:
        item['error'] = {'type': type(error).__name__, 'message': str(error)}
    results.append(item)
print('CASA_RESULT=' + json.dumps({'casatools': version_string(), 'cases': results}))
