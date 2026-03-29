#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if [[ -z "${CASA_RS_TESTDATA_ROOT:-}" && -d "$HOME/SoftwareProjects/casatestdata" ]]; then
  export CASA_RS_TESTDATA_ROOT="$HOME/SoftwareProjects/casatestdata"
fi

if [[ -z "${CASA_RS_CASA_PYTHON:-}" && -x "$HOME/SoftwareProjects/casa-build/venv/bin/python" ]]; then
  export CASA_RS_CASA_PYTHON="$HOME/SoftwareProjects/casa-build/venv/bin/python"
fi

ms_path="${1:-${CASA_RS_TESTDATA_ROOT:-$HOME/SoftwareProjects/casatestdata}/measurementset/vla/ngc5921.ms}"
output_dir="${2:-$repo_root/target/msexplore-comparisons/gallery}"

if [[ ! -d "$ms_path" ]]; then
  echo "MeasurementSet not found: $ms_path" >&2
  exit 1
fi

mkdir -p "$output_dir"

render_case() {
  local slug="$1"
  local casa_x="$2"
  local casa_y="$3"
  local rust_label="$4"
  local casa_label="$5"
  shift 5

  local -a casa_kwargs=()
  local -a casa_expr_kwargs=()
  local -a rust_args=()
  local mode="casa"
  for token in "$@"; do
    case "$token" in
      --RUST--)
        mode="rust"
        ;;
      *)
        if [[ "$mode" == "casa" ]]; then
          if [[ "$token" == expr:* ]]; then
            casa_expr_kwargs+=("${token#expr:}")
          else
            casa_kwargs+=("$token")
          fi
        else
          rust_args+=("$token")
        fi
        ;;
    esac
  done

  local output_png="$output_dir/$slug.png"
  local cmd=(
    "$repo_root/scripts/render-msexplore-side-by-side.sh"
    --ms "$ms_path"
    --output "$output_png"
    --casa-xaxis "$casa_x"
    --casa-yaxis "$casa_y"
    --rust-label "$rust_label"
    --casa-label "$casa_label"
  )
  local kw
  for kw in "${casa_kwargs[@]}"; do
    cmd+=(--casa-kw "$kw")
  done
  if ((${#casa_expr_kwargs[@]})); then
    for kw in "${casa_expr_kwargs[@]}"; do
      cmd+=(--casa-expr-kw "$kw")
    done
  fi
  cmd+=(--)
  cmd+=("${rust_args[@]}")

  echo "==> $slug"
  "${cmd[@]}"
}

common_selection=(
  "field=0"
  "spw=0"
  "scan=1"
)

render_case "amplitude-vs-time" "time" "amp" "casa-rs amplitude vs time" "CASA plotms amplitude vs time" \
  "${common_selection[@]}" --RUST-- --preset amplitude_vs_time --field 0 --spw 0 --scan 1
render_case "amplitude-phase-vs-time" "time" "amp" "casa-rs amplitude and phase vs time" "CASA plotms amplitude and phase vs time" \
  "${common_selection[@]}" "expr:yaxis=['amp','phase']" "expr:yaxislocation=['left','right']" "expr:showlegend=True" \
  --RUST-- --xaxis time --yaxis amplitude --yaxis2 phase --showlegend --field 0 --spw 0 --scan 1
echo "==> amplitude-phase-vs-time-stacked"
"$repo_root/scripts/render-msexplore-stacked-side-by-side.sh" \
  --ms "$ms_path" \
  --output "$output_dir/amplitude-phase-vs-time-stacked.png" \
  --field 0 \
  --spw 0 \
  --scan 1
echo "==> amplitude-phase-vs-time-page"
"$repo_root/scripts/render-msexplore-page-side-by-side.sh" \
  --ms "$ms_path" \
  --output "$output_dir/amplitude-phase-vs-time-page.png" \
  --field 0 \
  --spw 0 \
  --scan 1
echo "==> amplitude-overplot"
"$repo_root/scripts/render-msexplore-overplot-side-by-side.sh" \
  --ms "$ms_path" \
  --output "$output_dir/amplitude-overplot.png" \
  --field 0 \
  --spw 0 \
  --scan 1
render_case "phase-vs-time" "time" "phase" "casa-rs phase vs time" "CASA plotms phase vs time" \
  "${common_selection[@]}" --RUST-- --preset phase_vs_time --field 0 --spw 0 --scan 1
render_case "amplitude-vs-uvdist" "uvdist" "amp" "casa-rs amplitude vs uvdist" "CASA plotms amplitude vs uvdist" \
  "${common_selection[@]}" --RUST-- --preset amplitude_vs_uv_distance --field 0 --spw 0 --scan 1
render_case "weight-vs-time" "time" "wt" "casa-rs weight vs time" "CASA plotms weight vs time" \
  "${common_selection[@]}" --RUST-- --preset weight_vs_time --field 0 --spw 0 --scan 1
render_case "sigma-vs-time" "time" "sigma" "casa-rs sigma vs time" "CASA plotms sigma vs time" \
  "${common_selection[@]}" --RUST-- --preset sigma_vs_time --field 0 --spw 0 --scan 1
render_case "flag-vs-time" "time" "flag" "casa-rs flag vs time" "CASA plotms flag vs time" \
  "${common_selection[@]}" --RUST-- --preset flag_vs_time --field 0 --spw 0 --scan 1
render_case "weight-spectrum-vs-time" "time" "wtsp" "casa-rs weight spectrum vs time" "CASA plotms weight spectrum vs time" \
  "${common_selection[@]}" --RUST-- --preset weight_spectrum_vs_time --field 0 --spw 0 --scan 1
render_case "sigma-spectrum-vs-time" "time" "sigmasp" "casa-rs sigma spectrum vs time" "CASA plotms sigma spectrum vs time" \
  "${common_selection[@]}" --RUST-- --preset sigma_spectrum_vs_time --field 0 --spw 0 --scan 1
render_case "flagrow-vs-time" "time" "flagrow" "casa-rs flagrow vs time" "CASA plotms flagrow vs time" \
  "${common_selection[@]}" --RUST-- --preset flagrow_vs_time --field 0 --spw 0 --scan 1
render_case "elevation-vs-time" "time" "elevation" "casa-rs elevation vs time" "CASA plotms elevation vs time" \
  "${common_selection[@]}" --RUST-- --preset elevation_vs_time --field 0 --spw 0 --scan 1
render_case "azimuth-vs-time" "time" "azimuth" "casa-rs azimuth vs time" "CASA plotms azimuth vs time" \
  "${common_selection[@]}" --RUST-- --preset azimuth_vs_time --field 0 --spw 0 --scan 1
render_case "hour-angle-vs-time" "time" "hourang" "casa-rs hour angle vs time" "CASA plotms hour angle vs time" \
  "${common_selection[@]}" --RUST-- --preset hour_angle_vs_time --field 0 --spw 0 --scan 1
render_case "parallactic-angle-vs-time" "time" "parang" "casa-rs parallactic angle vs time" "CASA plotms parallactic angle vs time" \
  "${common_selection[@]}" --RUST-- --preset parallactic_angle_vs_time --field 0 --spw 0 --scan 1
render_case "azimuth-vs-elevation" "elevation" "azimuth" "casa-rs azimuth vs elevation" "CASA plotms azimuth vs elevation" \
  "${common_selection[@]}" --RUST-- --preset azimuth_vs_elevation --field 0 --spw 0 --scan 1
render_case "amplitude-vs-channel" "chan" "amp" "casa-rs amplitude vs channel" "CASA plotms amplitude vs channel" \
  "${common_selection[@]}" --RUST-- --preset amplitude_vs_channel --field 0 --spw 0 --scan 1
render_case "phase-vs-channel" "chan" "phase" "casa-rs phase vs channel" "CASA plotms phase vs channel" \
  "${common_selection[@]}" --RUST-- --preset phase_vs_channel --field 0 --spw 0 --scan 1
render_case "u-v" "u" "v" "casa-rs u vs v" "CASA plotms u vs v" \
  "${common_selection[@]}" --RUST-- --xaxis u --yaxis v --field 0 --spw 0 --scan 1
render_case "amplitude-vs-u" "u" "amp" "casa-rs amplitude vs u" "CASA plotms amplitude vs u" \
  "${common_selection[@]}" --RUST-- --xaxis u --yaxis amplitude --field 0 --spw 0 --scan 1
render_case "amplitude-vs-v" "v" "amp" "casa-rs amplitude vs v" "CASA plotms amplitude vs v" \
  "${common_selection[@]}" --RUST-- --xaxis v --yaxis amplitude --field 0 --spw 0 --scan 1
render_case "amplitude-vs-w" "w" "amp" "casa-rs amplitude vs w" "CASA plotms amplitude vs w" \
  "${common_selection[@]}" --RUST-- --xaxis w --yaxis amplitude --field 0 --spw 0 --scan 1
render_case "amplitude-vs-frequency" "frequency" "amp" "casa-rs amplitude vs frequency" "CASA plotms amplitude vs frequency" \
  "${common_selection[@]}" --RUST-- --preset amplitude_vs_frequency --field 0 --spw 0 --scan 1
render_case "phase-vs-frequency" "frequency" "phase" "casa-rs phase vs frequency" "CASA plotms phase vs frequency" \
  "${common_selection[@]}" --RUST-- --preset phase_vs_frequency --field 0 --spw 0 --scan 1
render_case "amplitude-vs-velocity" "velocity" "amp" "casa-rs amplitude vs velocity" "CASA plotms amplitude vs velocity" \
  "${common_selection[@]}" "freqframe=LSRK" "restfreq=1.420405752GHz" "veldef=RADIO" \
  --RUST-- --xaxis velocity --yaxis amplitude --field 0 --spw 0 --scan 1 --freqframe LSRK --restfreq 1.420405752GHz --veldef RADIO
render_case "phase-vs-velocity" "velocity" "phase" "casa-rs phase vs velocity" "CASA plotms phase vs velocity" \
  "${common_selection[@]}" "freqframe=LSRK" "restfreq=1.420405752GHz" "veldef=RADIO" \
  --RUST-- --xaxis velocity --yaxis phase --field 0 --spw 0 --scan 1 --freqframe LSRK --restfreq 1.420405752GHz --veldef RADIO
render_case "real-vs-imaginary" "real" "imag" "casa-rs real vs imaginary" "CASA plotms real vs imaginary" \
  "${common_selection[@]}" --RUST-- --preset real_vs_imaginary --field 0 --spw 0 --scan 1

index_path="$output_dir/index.html"
python3 - "$output_dir" "$index_path" <<'PY'
import html
import os
import sys
from pathlib import Path

gallery = Path(sys.argv[1])
index = Path(sys.argv[2])
images = sorted(
    path for path in gallery.glob("*.png")
    if not path.name.endswith(".casa.png") and not path.name.endswith(".rust.png")
)

rows = []
for image in images:
    title = image.stem.replace("-", " ")
    rows.append(
        f"<section><h2>{html.escape(title)}</h2>"
        f"<p><a href='{html.escape(image.name)}'>{html.escape(image.name)}</a> | "
        f"<a href='{html.escape(image.stem + '.rust.png')}'>rust</a> | "
        f"<a href='{html.escape(image.stem + '.casa.png')}'>casa</a> | "
        f"<a href='{html.escape(image.stem + '.casa.log')}'>casa log</a></p>"
        f"<img src='{html.escape(image.name)}' style='width: 100%; max-width: 1800px; border: 1px solid #ccc' />"
        f"</section>"
    )

index.write_text(
    "<!doctype html><html><head><meta charset='utf-8'><title>msexplore gallery</title>"
    "<style>body{font-family:system-ui,sans-serif;margin:24px}section{margin:24px 0}</style>"
    "</head><body><h1>casa-rs vs CASA plotms</h1>"
    + "".join(rows)
    + "</body></html>",
    encoding="utf-8",
)
PY

echo "Gallery written to $index_path"
