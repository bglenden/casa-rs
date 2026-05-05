#!/usr/bin/env bash
set -euo pipefail

outdir="${1:-target/wave6-issue169-3c391}"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

export CASA_RS_WAVE6_DATASET=vla
"$repo_root/scripts/run-wave6-issue53-mosaic-panels.sh" "$outdir"

python3 - "$outdir" <<'PY'
import json
import sys
from pathlib import Path

outdir = Path(sys.argv[1])
summary_path = outdir / "wave6-issue53-mosaic-panel-summary.json"
summary = json.loads(summary_path.read_text(encoding="utf-8"))
issue_summary = {
    "issue": 169,
    "dataset": "vla/3c391",
    "source_page": "https://casaguides.nrao.edu/index.php?title=VLA_Continuum_Tutorial_3C391-CASA6.4.1",
    "input_registry_keys": [
        "vla/3c391/final-calibrated-mosaic-ms",
        "vla/3c391/raw-10s-spw0",
    ],
    "products": {
        key: value
        for key, value in summary.items()
        if key.startswith("vla_")
    },
    "deferred_products": {
        "raw_calibration_replay": (
            "The raw 10s SPW0 archive is staged and registered as slow parity. "
            "This issue uses the official final-calibrated mosaic MS for the "
            "same-input imaging comparison; raw setjy/gencal/bandpass/"
            "fluxscale/gaincal/applycal/statwt replay remains separate "
            "calibration breadth work."
        ),
        "interactive_clean_screenshots": (
            "CASA Guide clean-mask and polygon screenshots are inventoried as "
            "interactive display artifacts. The runner generates noninteractive "
            "image products, colorbar panels, and numeric residual metrics."
        ),
    },
}
(outdir / "wave6-issue169-summary.json").write_text(
    json.dumps(issue_summary, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
print(json.dumps(issue_summary, indent=2, sort_keys=True))
PY

echo "Wrote Wave 6 #169 3C391 evidence under $outdir"
