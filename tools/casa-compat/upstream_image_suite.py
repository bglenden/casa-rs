#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Run the imported CASA image task suites with CASA or casa-rs."""

from upstream_casa_suite import main


if __name__ == "__main__":
    raise SystemExit(main("image", __doc__))
