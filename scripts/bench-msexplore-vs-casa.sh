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

if [[ $# -gt 1 ]]; then
  echo "usage: $0 [measurementset-path]" >&2
  exit 2
fi

if [[ $# -eq 1 ]]; then
  ms_path="$1"
elif [[ -n "${CASA_RS_TESTDATA_ROOT:-}" ]]; then
  ms_path="$CASA_RS_TESTDATA_ROOT/measurementset/vla/ngc5921.ms"
else
  echo "error: pass a MeasurementSet path or set CASA_RS_TESTDATA_ROOT" >&2
  exit 2
fi

if [[ ! -d "$ms_path" ]]; then
  echo "error: MeasurementSet not found: $ms_path" >&2
  exit 2
fi

if [[ -z "${CASA_RS_CASA_PYTHON:-}" ]]; then
  echo "error: CASA_RS_CASA_PYTHON is not set and no default CASA python was found" >&2
  exit 2
fi

repeats="${BENCH_REPEATS:-5}"
plot_width="${BENCH_PLOT_WIDTH:-1600}"
plot_height="${BENCH_PLOT_HEIGHT:-900}"
display_value="${DISPLAY:-:0}"

echo "ms_path=$ms_path"
echo "CASA_RS_CASA_PYTHON=$CASA_RS_CASA_PYTHON"
echo "repeats=$repeats"
echo "scenario=preset=amplitude_vs_time spw=0 iteraxis=scan gridcols=2 xselfscale color_by=correlation"
echo

cargo build --release -p casacore-ms --bin msexplore --example profile_msexplore >/dev/null

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

median_from_file() {
  python3 - "$1" <<'PY'
import statistics
import sys

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    values = [float(line.strip()) for line in handle if line.strip()]
if not values:
    raise SystemExit("no benchmark values provided")
print(f"{statistics.median(values):.6f}")
PY
}

echo "Rust release CLI timings (seconds):"
rust_cli_file="$tmpdir/rust-cli.txt"
for run in $(seq 1 "$repeats"); do
  out="$tmpdir/rust-$run.png"
  /usr/bin/time -lp target/release/msexplore \
    "$ms_path" \
    --preset amplitude_vs_time \
    --spw 0 \
    --iteraxis scan \
    --gridcols 2 \
    --xselfscale \
    --color-by correlation \
    --plot-width "$plot_width" \
    --plot-height "$plot_height" \
    --plot-output "$out" \
    >/dev/null 2>"$tmpdir/rust-$run.stderr"
  real_seconds="$(awk '/^real / {print $2}' "$tmpdir/rust-$run.stderr")"
  printf "  run=%s real=%s\n" "$run" "$real_seconds"
  printf "%s\n" "$real_seconds" >>"$rust_cli_file"
done
rust_cli_median="$(median_from_file "$rust_cli_file")"
echo "  median=$rust_cli_median"
echo

echo "Rust in-memory profiler medians (milliseconds):"
target/release/examples/profile_msexplore \
  "$ms_path" \
  --preset amplitude_vs_time \
  --spw 0 \
  --iteraxis scan \
  --gridcols 2 \
  --xselfscale \
  --color-by correlation \
  --plot-width "$plot_width" \
  --plot-height "$plot_height" \
  --repeats "$repeats" \
  --warmups 1 \
  | sed 's/^/  /'
echo

cat >"$tmpdir/casa-bench.py" <<'PY'
import os
import statistics
import tempfile
import time

try:
    from casatasks import plotms
except Exception:
    from casaplotms import plotms

vis = os.environ["CASA_RS_BENCH_MS_PATH"]
repeats = int(os.environ["CASA_RS_BENCH_REPEATS"])
plot_width = int(os.environ["CASA_RS_BENCH_PLOT_WIDTH"])
plot_height = int(os.environ["CASA_RS_BENCH_PLOT_HEIGHT"])
times = []

with tempfile.TemporaryDirectory() as td:
    warm = os.path.join(td, "warm.png")
    plotms(
        vis=vis,
        xaxis="time",
        yaxis="amp",
        spw="0",
        iteraxis="scan",
        gridcols=2,
        xselfscale=True,
        coloraxis="corr",
        plotfile=warm,
        expformat="png",
        customsymbol=True,
        symbolshape="circle",
        symbolsize=3,
        title="Amplitude vs Time",
        showgui=False,
        clearplots=True,
        width=plot_width,
        height=plot_height,
        dpi=72,
        overwrite=True,
    )
    for run in range(repeats):
        out = os.path.join(td, f"run-{run}.png")
        start = time.perf_counter()
        plotms(
            vis=vis,
            xaxis="time",
            yaxis="amp",
            spw="0",
            iteraxis="scan",
            gridcols=2,
            xselfscale=True,
            coloraxis="corr",
            plotfile=out,
            expformat="png",
            customsymbol=True,
            symbolshape="circle",
            symbolsize=3,
            title="Amplitude vs Time",
            showgui=False,
            clearplots=True,
            width=plot_width,
            height=plot_height,
            dpi=72,
            overwrite=True,
        )
        elapsed = time.perf_counter() - start
        times.append(elapsed)
        print(f"run={run + 1} real={elapsed:.6f}")

print(f"median={statistics.median(times):.6f}")
PY

echo "Warm CASA plotms timings (seconds):"
CASA_RS_BENCH_MS_PATH="$ms_path" \
CASA_RS_BENCH_REPEATS="$repeats" \
CASA_RS_BENCH_PLOT_WIDTH="$plot_width" \
CASA_RS_BENCH_PLOT_HEIGHT="$plot_height" \
DISPLAY="$display_value" \
  "$CASA_RS_CASA_PYTHON" "$tmpdir/casa-bench.py" | sed 's/^/  /'
