#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA quanta.constants names, minimax lookup, and argument errors."""
import json
from casaconfig import config
config.data_auto_update = config.measures_auto_update = False
from casatools import quanta, version_string  # noqa: E402

NAMES = ['pi', 'ee', 'c', 'G', 'h', 'HI', 'R', 'NA', 'e', 'mp', 'mp_me',
         'mu0', 'epsilon0', 'k', 'F', 'me', 're', 'a0', 'R0', 'k2']
VALUES = NAMES + ['', 'p', 'PI', 'pii', 'E', 'ep', 'eps', 'eps0', 'epsilon',
                  'epsilon1', 'eeextra', 'h', 'hy', 'hi', 'm', 'mp_', 'mp_meX',
                  'mpp', 'mu', 'muu', 'k3', 'R0foo', 'fTurn', 'qTurn', 'foo',
                  ' π', 'unknown', None, 1, [], {'x': 1}]
q = quanta()
results = []
for value in VALUES:
    item = {'input': value}
    try: item['result'] = q.constants(value)
    except Exception as error:
        item['error'] = {'type': type(error).__name__, 'message': str(error)}
    results.append(item)
print('CASA_RESULT=' + json.dumps({'casatools': version_string(), 'cases': results}))
