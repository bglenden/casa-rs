#!/usr/bin/env python3
"""Run allowlist-planned TUI commands through separate GhosttyKit surfaces."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: tui_batch.py REQUEST", file=sys.stderr)
        return 2
    request = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    if request.get("schema_version") != 1:
        raise ValueError("unsupported TUI batch request")
    for command in request["commands"]:
        argv = [
            request["capture"],
            "--cwd", request["cwd"],
            "--output", request["output"],
            "--width", "2200",
            "--height", "1400",
            "--font-size", "12",
            "--settle-seconds", "60",
        ]
        for event in request["input_events"]:
            argv.extend(["--input-event", f"{event['after_ms']}:{event['text']}"])
        argv.extend(["--", *command])
        completed = subprocess.run(argv, check=False)
        if completed.returncode:
            return completed.returncode
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
