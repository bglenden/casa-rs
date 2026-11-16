#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA quantity-record errors for invalid units and malformed records."""
import json
from casaconfig import config
config.data_auto_update = False
config.measures_auto_update = False
from casatools import quanta, version_string  # noqa: E402

RECORDS = [{'value': 4.0, 'unit': 'not-a-unit'},
           {'value': [1.0, 2.0], 'unit': 'not-a-unit'},
           {'value': 4.0}, {'unit': 'm'}]
METHODS = [('quantity', []), ('getunit', []), ('convert', ['m']), ('add', [1.0])]
q = quanta()
results = []
for record in RECORDS:
    for method, rest in METHODS:
        item = {'method': method, 'args': [record, *rest]}
        try: item['result'] = getattr(q, method)(*item['args'])
        except Exception as error:
            item['error'] = {'type': type(error).__name__, 'message': str(error)}
        results.append(item)
print('CASA_RESULT=' + json.dumps({'casatools': version_string(), 'cases': results}))
