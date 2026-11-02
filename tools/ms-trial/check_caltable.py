# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA's applycal of a gain table casa-rs wrote through an MSv4's namespace.

Usage, with a CASA install's Python (casatasks is an oracle here only):
    CASA_RS_TRIAL_KEEP=DIR cargo test -p casa-calibration --lib \
        a_caltable_rebinds_to_a_store_numbered_otherwise
    python check_caltable.py DIR

DIR holds what that law kept: `batch.G`, solved from batches of an MSv4
(the trial fixture without antenna 0) and written through its namespace;
`production.G`, production gaincal's on `without.ms`, the MSv2 that MSv4
was converted from; and `without_v4.ms`, the MSv4 converted back, whose
subtables are the namespace `batch.G` persists. CASA applies each table to
its own MeasurementSet by CASA's own association (rows as indices), and the
corrected visibilities and flags must agree sample for sample, matched on
(TIME, station, station): the namespace the writer persisted is the one
CASA reads. See docs/measurement-set/trial/README.md, "Caltable round
trip".
"""
import shutil
import sys
import tempfile

import numpy as np
from casatasks import applycal
from casatools import table

keep = sys.argv[1]
tb = table()


def applied(ms, gaintable, work):
    copy = f"{work}/{ms}"
    shutil.copytree(f"{keep}/{ms}", copy)
    applycal(vis=copy, gaintable=[f"{keep}/{gaintable}"], interp=["linear"],
             calwt=[False], applymode="calflag", flagbackup=False)
    tb.open(f"{copy}/ANTENNA")
    stations = list(tb.getcol("STATION"))
    tb.close()
    tb.open(copy)
    columns = {c: tb.getcol(c) for c in ["TIME", "ANTENNA1", "ANTENNA2", "CORRECTED_DATA", "FLAG"]}
    tb.close()
    return {
        (columns["TIME"][r], stations[columns["ANTENNA1"][r]], stations[columns["ANTENNA2"][r]]):
            (columns["CORRECTED_DATA"][..., r], columns["FLAG"][..., r])
        for r in range(len(columns["TIME"]))
    }


with tempfile.TemporaryDirectory() as work:
    ours = applied("without_v4.ms", "batch.G", work)
    theirs = applied("without.ms", "production.G", work)

assert ours.keys() == theirs.keys(), "the same samples"
worst = 0.0
flagged = 0
for key, (values, flags) in ours.items():
    their_values, their_flags = theirs[key]
    assert (flags == their_flags).all(), key
    flagged += int(flags.all())
    good = ~flags
    if good.any():
        scale = np.abs(their_values[good]).max()
        worst = max(worst, float(np.abs(values[good] - their_values[good]).max() / scale))
print(f"compared {len(ours)} samples, {flagged} flagged throughout; "
      f"largest relative difference {worst:.3g}")
assert worst < 1e-5
