# SPDX-License-Identifier: LGPL-3.0-or-later
"""Shared user-facing errors for imaging performance harness workflows."""


class HarnessError(Exception):
    """An evidence workflow error that should be shown without a traceback."""
