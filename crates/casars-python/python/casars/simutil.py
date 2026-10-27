"""CASA's `simutil` helper class for casa-rs.

A CASA script changes only its import::

    from casars import simutil

    u = simutil.simutil()
    x, y, z, d, padnames, antnames, telescope, posobs = u.readantenna("vla.d.cfg")
    pointings = u.calc_pointings2("10arcsec", ["60arcsec", "40arcsec"], maptype="ALMA",
                                  direction="J2000 18h00m00 -23d00m00")

In CASA, `simutil` is a Python helper class in the private `simutil` module of
CASA's task package, not a CASA tool, so it is not in `casars.tools` or in the
object-surface bundle. casa-rs implements, in Rust (the class lives in the
`casars._core` extension, ADR-0018), the parts `sm` scripts and `simobserve`
use:
- `readantenna`;
- the pointings `calc_pointings2`, `read_pointings` and `write_pointings`;
- the direction helpers `direction_splitter`, `average_direction`,
  `median_direction`, `dir_m2s`, `wrapang`, `isquantity` and `isdirection`;
- `noisetemp`, `bandname` and `polsettings`;
- `itrf2loc`, `baselineLengths` and `approxBeam`.

The sky-model helpers come with `simobserve`. `casars.simutil` is the class's
public import: CASA's `private` path would mislabel a supported entry point.
"""

# CASA's import is `from casatasks.private import simutil`.

from __future__ import annotations

from ._core import simutil

__all__ = ["simutil"]
