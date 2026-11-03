#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Summarize an `sm_parity_campaign.sh` run into a receipt (#1053).

    python3 tools/simulation/sm_parity_receipt.py OUT \\
        --casa-version "casatools 6.7.6-14" \\
        --out resources/casa-compat/evidence/sm-parity-20261103.json

Reads `OUT/<set>/report.json` (compare_sm_ms.py's) for every set that has
one and records, per scenario, whether it agrees with CASA (no differences
beyond the known ones), its DATA comparison and its largest UVW difference,
with the revision, the host and the command, so the reviewed reference can
cite a `passed` comparison.
"""

from __future__ import annotations

import argparse
import datetime
import json
import platform
import subprocess
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("run", type=Path, help="the campaign's OUT directory")
    parser.add_argument("--casa-version", required=True, help="the CASA the oracle ran, as it reports itself")
    parser.add_argument("--revision", help="the casa-rs revision that ran (default: this checkout's HEAD)")
    parser.add_argument("--host", help="where it ran (default: this host)")
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    revision = args.revision or subprocess.run(
        ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True).stdout.strip()
    sets, totals = {}, {"scenarios": 0, "agree": 0}
    for report_path in sorted(args.run.glob("*/report.json")):
        reports = json.loads(report_path.read_text(encoding="utf-8"))
        scenarios = {}
        for report in reports:
            agree = not report["differences"]
            scenarios[report["name"]] = {
                "agrees": agree,
                "differences": report["differences"],
                "known_differences": len(report["known"]),
                "data": report.get("data", {}),
                "largest_uvw_m": report.get("largest", {}).get("MAIN.UVW"),
            }
            totals["scenarios"] += 1
            totals["agree"] += agree
        sets[report_path.parent.name] = {
            "scenario_file": f"tools/simulation/sm_{report_path.parent.name}_scenarios.json",
            "agree": sum(item["agrees"] for item in scenarios.values()),
            "total": len(scenarios),
            "scenarios": scenarios,
        }
    if not sets:
        print(f"sm-parity-receipt: no */report.json under {args.run}", file=sys.stderr)
        return 1
    receipt = {
        "schema": "sm-parity-receipt/1",
        "work_issue": 1053,
        "date": datetime.date.today().isoformat(),
        "oracle": args.casa_version,
        "native_revision": revision,
        "host": args.host or platform.platform(),
        "command": "tools/simulation/sm_parity_campaign.sh OUT " + " ".join(sets),
        "comparator": "tools/simulation/compare_sm_ms.py (tables, descriptors, cells; DATA NRMS <= 1e-3 or the scenario's statistics)",
        "summary": totals,
        "sets": sets,
    }
    args.out.write_text(json.dumps(receipt, indent=1, default=str) + "\n", encoding="utf-8")
    print(f"sm-parity-receipt: {totals['agree']}/{totals['scenarios']} agree -> {args.out}", file=sys.stderr)
    return 0 if totals["agree"] == totals["scenarios"] else 1


if __name__ == "__main__":
    sys.exit(main())
