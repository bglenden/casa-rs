# SPDX-License-Identifier: LGPL-3.0-or-later
"""A CASA `sm` script: the VLA in D configuration observing one field.

Written as a CASA user writes it, with CASA's imports. To run it with
casa-rs, only the import block changes (`run_sm_scripts.py` swaps it):

    from casars import simutil
    from casars.tools import measures, quanta, simulator

It writes `vla_d_rr.ms` (one spectral window, one correlation) in the
current directory.
"""

# --- imports: the only lines that differ between CASA and casa-rs ---
from casatasks.private import simutil
from casatools import measures, quanta, simulator
# --- end imports ---

import os

me = measures()
qa = quanta()
sm = simulator()
util = simutil.simutil()

configdir = os.path.expanduser("~/.casa/data/alma/simmos")
x, y, z, d, padnames, antnames, telescope, posobs = util.readantenna(
    os.path.join(configdir, "vla.d.cfg")
)

sm.open("vla_d_rr.ms")
sm.setconfig(
    telescopename=telescope,
    x=x,
    y=y,
    z=z,
    dishdiameter=d.tolist(),
    mount=["alt-az"],
    antname=antnames,
    padname=padnames,
    coordsystem="global",
    referencelocation=me.observatory("VLA"),
)
sm.setspwindow(
    spwname="LBAND",
    freq="1.4GHz",
    deltafreq="1MHz",
    freqresolution="1MHz",
    refcode="TOPO",
    nchannels=64,
    stokes="RR",
)
sm.setfeed(mode="perfect R L")
sm.settimes(
    integrationtime="10s",
    usehourangle=True,
    referencetime=me.epoch("UTC", "2026/03/21/00:00:00"),
)
sm.setfield(
    sourcename="src",
    sourcedirection=me.direction("J2000", "13h00m00.0", "+34d00m00.0"),
)
sm.observe("src", "LBAND", starttime="-600s", stoptime="600s")
sm.close()
