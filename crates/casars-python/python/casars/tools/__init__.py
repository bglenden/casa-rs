"""CASA tools for casa-rs, under CASA's class names (ADR-0019).

A CASA script changes only its imports::

    from casars.tools import measures, quanta, simulator

    me = measures()
    sm = simulator()
    sm.open("sim.ms")
    sm.setconfig(referencelocation=me.observatory("VLA"), ...)

Each tool is a class in the `casars._core` extension, with CASA's method
names, parameter order and defaults declared in Rust. This module adds only
the module path.
"""

from __future__ import annotations

from .._core import measures, quanta, simulator

__all__ = ["measures", "quanta", "simulator"]
