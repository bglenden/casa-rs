#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Write a CASA 6.7.6-14 component list containing a custom table keyword.

Run under CASA's Python with a nonexistent output path. The resulting table
is the `keywords.cl` fixture in casa-components' CASA interoperability tests.
"""
import sys
from pathlib import Path

from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import componentlist  # noqa: E402

path = Path(sys.argv[1])
cl = componentlist()
cl.addcomponent(freq="1.4GHz")
cl.rename(str(path))
cl.putkeyword("marker", {"answer": 42})
assert cl.getkeyword("marker") == {"answer": 42}
cl.done()
cl = componentlist()
cl.open(str(path))
assert cl.getkeyword("marker") == {"answer": 42}
cl.done()
(path / "table.lock").unlink(missing_ok=True)
# Preserve CASA's empty subtype while avoiding a trailing-space-only line.
info = path / "table.info"
info.write_text(info.read_text().replace("SubType = \n", "SubType =\n"))
