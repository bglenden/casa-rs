"""Deterministic CASA runtime configuration for frozen VLASS oracles."""

from __future__ import annotations

import os


measurespath = os.environ.get(
    "CASA_RS_VLASS_MEASURES_DIR",
    os.path.expanduser("~/.casa/data"),
)
datapath = [measurespath]
data_auto_update = False
measures_auto_update = False
skipnetworkcheck = True
logfile = os.environ.get(
    "CASA_RS_VLASS_CASA_LOGFILE",
    os.path.abspath("casa-vlass-oracle.log"),
)
