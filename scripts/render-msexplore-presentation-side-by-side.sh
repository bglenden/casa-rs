#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

usage() {
  cat <<'EOF'
Usage:
  scripts/render-msexplore-presentation-side-by-side.sh \
    --ms /path/to.ms \
    --output /path/to/side-by-side.png \
    [--field 0] [--spw 0] [--scan 1]
EOF
}

ms_path=""
output_path=""
field="0"
spw="0"
scan="1"
rust_label="casa-rs overplot with headers and exterior legend"
casa_label="CASA plotms overplot with headers and exterior legend"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ms)
      ms_path="$2"
      shift 2
      ;;
    --output)
      output_path="$2"
      shift 2
      ;;
    --field)
      field="$2"
      shift 2
      ;;
    --spw)
      spw="$2"
      shift 2
      ;;
    --scan)
      scan="$2"
      shift 2
      ;;
    --rust-label)
      rust_label="$2"
      shift 2
      ;;
    --casa-label)
      casa_label="$2"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "unrecognized option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$ms_path" || -z "$output_path" ]]; then
  usage >&2
  exit 1
fi

if [[ -z "${CASA_RS_CASA_PYTHON:-}" && -x "$HOME/SoftwareProjects/casa-build/venv/bin/python" ]]; then
  export CASA_RS_CASA_PYTHON="$HOME/SoftwareProjects/casa-build/venv/bin/python"
fi

if [[ -z "${CASA_RS_CASA_PYTHON:-}" ]]; then
  echo "CASA_RS_CASA_PYTHON is not set and no default CASA python was found" >&2
  exit 1
fi

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

rust_png="$tmpdir/rust.png"
casa_png="$tmpdir/casa.png"
casa_log="$tmpdir/casa.log"
page_spec="$tmpdir/page-spec.json"

mkdir -p "$(dirname "$output_path")"
rust_copy="${output_path%.png}.rust.png"
casa_copy="${output_path%.png}.casa.png"
log_copy="${output_path%.png}.casa.log"

cat >"$page_spec" <<EOF
{
  "page_title": "Amplitude Overplot Presentation",
  "exprange": "all",
  "gridrows": 1,
  "gridcols": 1,
  "plots": [
    {
      "preset": "amplitude_vs_time",
      "plotindex": 0,
      "rowindex": 0,
      "colindex": 0,
      "title": "Amplitude:vector",
      "showlegend": true,
      "legendposition": "exteriorRight"
    },
    {
      "preset": "amplitude_vs_time",
      "scalar": true,
      "plotindex": 1,
      "rowindex": 0,
      "colindex": 0,
      "title": "Amplitude:scalar",
      "showlegend": true,
      "legendposition": "exteriorRight"
    }
  ]
}
EOF

echo "Rendering casa-rs presentation page..."
cargo run -q -p casacore-ms --bin msexplore -- \
  --page-spec "$page_spec" \
  --field "$field" \
  --spw "$spw" \
  --scan "$scan" \
  --headeritems filename,observer,projid \
  --plot-output "$rust_png" \
  --plot-format png \
  --plot-width 1400 \
  --plot-height 900 \
  "$ms_path"

echo "Rendering CASA presentation page..."
(cd "$tmpdir" && \
CASA_VIS="$ms_path" \
CASA_OUT="$casa_png" \
CASA_FIELD="$field" \
CASA_SPW="$spw" \
CASA_SCAN="$scan" \
DISPLAY="${DISPLAY:-:0}" \
"$CASA_RS_CASA_PYTHON" - <<'PY'
import os

try:
    from casatasks import plotms
except Exception:
    from casaplotms import plotms

common = {
    "vis": os.environ["CASA_VIS"],
    "field": os.environ["CASA_FIELD"],
    "spw": os.environ["CASA_SPW"],
    "scan": os.environ["CASA_SCAN"],
    "showgui": False,
    "verbose": True,
    "gridrows": 1,
    "gridcols": 1,
    "showlegend": True,
    "legendposition": "exteriorRight",
    "headeritems": "filename,observer,projid",
    "title": "Amplitude Overplot Presentation",
}

plotms(
    xaxis="time",
    yaxis="amp",
    rowindex=0,
    colindex=0,
    plotindex=0,
    clearplots=True,
    **common,
)
plotms(
    xaxis="time",
    yaxis="amp",
    scalar=True,
    rowindex=0,
    colindex=0,
    plotindex=1,
    clearplots=False,
    plotfile=os.environ["CASA_OUT"],
    expformat="png",
    overwrite=True,
    width=1400,
    height=900,
    **common,
)
PY
)

latest_casa_log="$(ls -1t "$tmpdir"/casa-*.log 2>/dev/null | head -n 1 || true)"
if [[ -z "$latest_casa_log" ]]; then
  echo "CASA export did not produce a log file" >&2
  exit 1
fi
cp "$latest_casa_log" "$casa_log"

echo "Building side-by-side image..."
python3 - "$rust_png" "$casa_png" "$output_path" "$rust_label" "$casa_label" <<'PY'
import sys
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

rust_path = Path(sys.argv[1])
casa_path = Path(sys.argv[2])
output_path = Path(sys.argv[3])
rust_label = sys.argv[4]
casa_label = sys.argv[5]

rust = Image.open(rust_path).convert("RGB")
casa = Image.open(casa_path).convert("RGB")
font = ImageFont.load_default()
banner_height = 36
canvas = Image.new(
    "RGB",
    (rust.width + casa.width, max(rust.height, casa.height) + banner_height),
    "white",
)
canvas.paste(rust, (0, banner_height))
canvas.paste(casa, (rust.width, banner_height))

draw = ImageDraw.Draw(canvas)
draw.rectangle((0, 0, rust.width, banner_height), fill="#f2f6ff")
draw.rectangle((rust.width, 0, rust.width + casa.width, banner_height), fill="#fff4e8")
draw.text((12, 10), rust_label, fill="black", font=font)
draw.text((rust.width + 12, 10), casa_label, fill="black", font=font)

canvas.save(output_path)
PY

cp "$rust_png" "$rust_copy"
cp "$casa_png" "$casa_copy"
cp "$casa_log" "$log_copy"

echo "Wrote:"
echo "  $output_path"
echo "  $rust_copy"
echo "  $casa_copy"
echo "  $log_copy"
