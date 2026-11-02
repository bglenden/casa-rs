# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA's side of the 12 m + 7 m evidence (#937 PR B): concat of simalma's
m51c pair with simalma's weights, a one-input concat for casa-rs to append
to, and CASA appending the 7 m to casa-rs's 12 m.

Usage: python3.12 m51c_casa.py STAGE WORK
  STAGE refs:   WORK/ref.ms = concat([12m, 7m], visweightscale=[1, 0.34]);
                WORK/b.ms  = concat([12m])
  STAGE append: WORK/a.ms (a copy of casa-rs's WORK/ours12.ms) gains the 7 m
                at 0.34
"""

import os
import shutil
import sys

from casatasks import concat

STAGE, WORK = sys.argv[1], sys.argv[2]
TWELVE = os.path.join(WORK, "m51c.alma_0.5arcsec.ms")
SEVEN = os.path.join(WORK, "m51c.aca.i.ms")

if STAGE == "refs":
    for name in ["ref.ms", "b.ms"]:
        shutil.rmtree(os.path.join(WORK, name), ignore_errors=True)
    concat(vis=[TWELVE, SEVEN], concatvis=os.path.join(WORK, "ref.ms"), visweightscale=[1.0, 0.34])
    concat(vis=[TWELVE], concatvis=os.path.join(WORK, "b.ms"))
elif STAGE == "append":
    target = os.path.join(WORK, "a.ms")
    shutil.rmtree(target, ignore_errors=True)
    shutil.copytree(os.path.join(WORK, "ours12.ms"), target)
    concat(vis=[SEVEN], concatvis=target, visweightscale=[0.34])
else:
    raise SystemExit(f"unknown stage {STAGE}")
print(f"{STAGE} done")
