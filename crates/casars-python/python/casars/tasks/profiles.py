"""Profile-aware CASA-named wrappers for every catalog task.

This is the idiomatic task-specific namespace for sparse TOML profiles,
typed :class:`~casars.parameters.TaskParameters`, source selection, safety
authorization, and managed Last lifecycle. The implementation is generated
in :mod:`casars.tasks.catalog` from the authoritative Rust surface catalog.

Specialized sibling modules such as :mod:`casars.tasks.imager` expose the
distinct provider request/result object protocols and intentionally do not
participate in profile persistence.
"""

from .catalog import *  # noqa: F403
from .catalog import __all__
