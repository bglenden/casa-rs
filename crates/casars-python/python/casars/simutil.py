"""CASA's `simutil` helper class for casa-rs.

A CASA script changes only its import::

    from casars import simutil

    u = simutil.simutil()
    x, y, z, d, padnames, antnames, telescope, posobs = u.readantenna("vla.d.cfg")

In CASA, `simutil` is a Python helper class in the private `simutil` module of
CASA's task package, not a CASA tool, so it is not in `casars.tools` or in the
object-surface bundle. casa-rs implements its `readantenna` in Rust (the class
lives in the `casars._core` extension, ADR-0018); the rest of `simutil` comes
with `simobserve`. This module path is provisional: where CASA's private task
helpers belong in casa-rs is an open question.
"""

# CASA's import is `from casatasks.private import simutil`.

from __future__ import annotations

from ._core import simutil

__all__ = ["simutil"]
