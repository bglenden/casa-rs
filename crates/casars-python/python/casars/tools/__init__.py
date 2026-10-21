"""CASA tools for casa-rs, under CASA's class names (ADR-0019).

A CASA script changes only its imports::

    from casars.tools import simulator

    sm = simulator()
    sm.open("sim.ms")

Each tool is a class in the `casars._core` extension, with CASA's method
names, parameter order and defaults declared in Rust. This module adds only
the module path.
"""

from __future__ import annotations

from .._core import simulator

__all__ = ["simulator"]
