#!/usr/bin/env python3
"""Execute a checked JSON batch of already allowlist-built argv vectors."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: command_batch.py REQUEST", file=sys.stderr)
        return 2
    request = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    commands = request.get("commands")
    if request.get("schema_version") != 1 or not isinstance(commands, list):
        raise ValueError("unsupported command batch request")
    for command in commands:
        if not isinstance(command, list) or not command or not all(isinstance(item, str) for item in command):
            raise ValueError("invalid command vector")
        completed = subprocess.run(command, check=False)
        if completed.returncode:
            return completed.returncode
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
