# SPDX-License-Identifier: LGPL-3.0-or-later
"""A CASA `sm` script: ALMA's Cycle 10 configuration 3 observing one field,
with two and with four correlations.

Written as a CASA user writes it, with CASA's imports. To run it with
casa-rs, only the import block changes (`run_sm_scripts.py` swaps it):

    from casars import simutil
    from casars.tools import measures, quanta, simulator

It writes `alma_xx_yy.ms` and `alma_full_pol.ms` in the current directory.
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
    os.path.join(configdir, "alma.cycle10.3.cfg")
)
print("observatory:", me.show(posobs), "at", qa.angle(posobs["m0"], prec=9)[0], qa.angle(posobs["m1"], prec=9)[0])

for msname, stokes in (("alma_xx_yy.ms", "XX YY"), ("alma_full_pol.ms", "XX XY YX YY")):
    sm.open(msname)
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
        referencelocation=posobs,
    )
    sm.setspwindow(
        spwname="spw0",
        freq=qa.quantity(230.0, "GHz"),
        deltafreq=qa.quantity("15.625MHz"),
        freqresolution=qa.quantity("15.625MHz"),
        refcode="LSRK",
        nchannels=32,
        stokes=stokes,
    )
    sm.setfeed(mode="perfect X Y")
    sm.setauto(autocorrwt=0.0)
    reference = me.epoch("UTC", "2026/03/21/00:00:00")
    sm.settimes(integrationtime=qa.quantity(6, "s"), usehourangle=True, referencetime=reference)
    target = me.direction("J2000", "10h00m00.0", "-30d00m00.0")
    sm.setfield(sourcename="target", sourcedirection=target)
    sm.observe("target", "spw0", starttime="-300s", stoptime="300s")
    sm.close()
