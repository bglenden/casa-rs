#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

usage() {
  cat <<'EOF'
Usage:
  scripts/render-msexplore-side-by-side.sh \
    --ms /path/to.ms \
    --output /path/to/side-by-side.png \
    --casa-xaxis velocity \
    --casa-yaxis amp \
    --casa-kw field=0 \
    --casa-kw spw=0 \
    --casa-kw scan=1 \
    -- --xaxis velocity --yaxis amplitude --field 0 --spw 0 --scan 1

Arguments before `--` configure the CASA half. Arguments after `--` are passed
directly to the Rust `msexplore` CLI.
EOF
}

ms_path=""
output_path=""
casa_xaxis=""
casa_yaxis=""
rust_label="casa-rs"
casa_label="CASA"
declare -a casa_kwargs=()
declare -a rust_args=()

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
    --casa-xaxis)
      casa_xaxis="$2"
      shift 2
      ;;
    --casa-yaxis)
      casa_yaxis="$2"
      shift 2
      ;;
    --casa-kw)
      casa_kwargs+=("$2")
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
    --)
      shift
      rust_args=("$@")
      break
      ;;
    *)
      echo "unrecognized option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$ms_path" || -z "$output_path" || -z "$casa_xaxis" || -z "$casa_yaxis" || ${#rust_args[@]} -eq 0 ]]; then
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

mkdir -p "$(dirname "$output_path")"
rust_copy="${output_path%.png}.rust.png"
casa_copy="${output_path%.png}.casa.png"
log_copy="${output_path%.png}.casa.log"

echo "Rendering casa-rs plot..."
cargo run -q -p casacore-ms --bin msexplore -- \
  --plot-output "$rust_png" \
  --plot-format png \
  --plot-width 1600 \
  --plot-height 900 \
  "${rust_args[@]}" \
  "$ms_path"

echo "Rendering CASA plot..."
(cd "$tmpdir" && \
CASA_VIS="$ms_path" \
CASA_OUT="$casa_png" \
CASA_XAXIS="$casa_xaxis" \
CASA_YAXIS="$casa_yaxis" \
CASA_EXTRA_KWARGS="$(printf '%s\n' "${casa_kwargs[@]}")" \
DISPLAY="${DISPLAY:-:0}" \
"$CASA_RS_CASA_PYTHON" - <<'PY'
import os

try:
    from casatasks import plotms
except Exception:
    from casaplotms import plotms

kwargs = {
    "vis": os.environ["CASA_VIS"],
    "xaxis": os.environ["CASA_XAXIS"],
    "yaxis": os.environ["CASA_YAXIS"],
    "plotfile": os.environ["CASA_OUT"],
    "expformat": "png",
    "overwrite": True,
    "showgui": False,
    "verbose": True,
    "width": 1600,
    "height": 900,
}
for item in os.environ.get("CASA_EXTRA_KWARGS", "").splitlines():
    if not item.strip():
        continue
    key, value = item.split("=", 1)
    kwargs[key] = value
plotms(**kwargs)
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
canvas = Image.new("RGB", (rust.width + casa.width, rust.height + banner_height), "white")
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

echo "casa-rs PNG: $rust_copy"
echo "CASA PNG:    $casa_copy"
echo "CASA log:    $log_copy"
echo "Combined:    $output_path"
