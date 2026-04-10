#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if [[ -z "${CASA_RS_TESTDATA_ROOT:-}" && -d "/Volumes/home/casatestdata" ]]; then
  export CASA_RS_TESTDATA_ROOT="/Volumes/home/casatestdata"
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
field="${IMAGER_BENCH_FIELD:-0}"
spw="${IMAGER_BENCH_SPW:-0}"
channel_start="${IMAGER_BENCH_CHANNEL_START:-0}"
channel_count="${IMAGER_BENCH_CHANNEL_COUNT:-1}"
specmode="${IMAGER_BENCH_SPECMODE:-mfs}"
imsize="${IMAGER_BENCH_IMSIZE:-128}"
cell_arcsec="${IMAGER_BENCH_CELL_ARCSEC:-30}"
minor_cycle_length="${IMAGER_BENCH_MINOR_CYCLE_LENGTH:-2}"
cyclefactor="${IMAGER_BENCH_CYCLEFACTOR:-1.0}"
min_psf_fraction="${IMAGER_BENCH_MIN_PSFFRACTION:-0.05}"
max_psf_fraction="${IMAGER_BENCH_MAX_PSFFRACTION:-0.8}"
weighting="${IMAGER_BENCH_WEIGHTING:-natural}"
robust="${IMAGER_BENCH_ROBUST:-0.5}"
deconvolver="${IMAGER_BENCH_DECONVOLVER:-hogbom}"
scales="${IMAGER_BENCH_SCALES:-}"
wterm="${IMAGER_BENCH_WTERM:-none}"
mode="${IMAGER_BENCH_MODE:-dirty}"
niter="${IMAGER_BENCH_NITER:-4}"
gain="${IMAGER_BENCH_GAIN:-0.1}"
threshold_jy="${IMAGER_BENCH_THRESHOLD_JY:-0}"
psfcutoff="${IMAGER_BENCH_PSFCUTOFF:-0.35}"

if [[ "$wterm" != "none" ]]; then
  echo "error: scripts/bench-imager-vs-casa.sh only supports IMAGER_BENCH_WTERM=none for Rust-vs-CASA comparisons" >&2
  exit 2
fi

if [[ "$specmode" != "mfs" && "$specmode" != "cube" ]]; then
  echo "error: IMAGER_BENCH_SPECMODE must be mfs or cube" >&2
  exit 2
fi

if [[ "$specmode" == "cube" && "$mode" != "dirty" ]]; then
  echo "error: cube benchmarking currently only supports IMAGER_BENCH_MODE=dirty" >&2
  exit 2
fi

if [[ "$mode" == "dirty" ]]; then
  dirty_flag="--dirty-only"
  casa_niter="0"
else
  dirty_flag=""
  casa_niter="$niter"
fi

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

echo "ms_path=$ms_path"
echo "CASA_RS_CASA_PYTHON=$CASA_RS_CASA_PYTHON"
echo "mode=$mode specmode=$specmode field=$field spw=$spw channel_start=$channel_start channel_count=$channel_count weighting=$weighting robust=$robust deconvolver=$deconvolver scales=$scales wterm=$wterm imsize=$imsize cell_arcsec=$cell_arcsec repeats=$repeats cycleniter=$minor_cycle_length cyclefactor=$cyclefactor minpsffraction=$min_psf_fraction maxpsffraction=$max_psf_fraction"
echo

cargo build --release -p casars-imager --bin casars-imager --example profile_imager >/dev/null

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT
staged_ms_path="$tmpdir/benchmark.ms"
cp -R "$ms_path" "$staged_ms_path"
ms_path="$staged_ms_path"

echo "Rust release CLI timings (seconds):"
rust_cli_file="$tmpdir/rust-cli.txt"
for run in $(seq 1 "$repeats"); do
  prefix="$tmpdir/rust-run-$run"
  if [[ -n "$scales" ]]; then
    /usr/bin/time -lp target/release/casars-imager \
      --ms "$ms_path" \
      --imagename "$prefix" \
      --imsize "$imsize" \
      --cell-arcsec "$cell_arcsec" \
      --field "$field" \
      --spw "$spw" \
      --channel-start "$channel_start" \
      --channel-count "$channel_count" \
      --specmode "$specmode" \
      --datacolumn DATA \
      --weighting "$weighting" \
      --robust "$robust" \
      --deconvolver "$deconvolver" \
      --scales "$scales" \
      --niter "$niter" \
      --gain "$gain" \
      --threshold-jy "$threshold_jy" \
      --psfcutoff "$psfcutoff" \
      --minor-cycle-length "$minor_cycle_length" \
      --cyclefactor "$cyclefactor" \
      --minpsffraction "$min_psf_fraction" \
      --maxpsffraction "$max_psf_fraction" \
      --wterm "$wterm" \
      --no-preview-pngs \
      $dirty_flag \
      >/dev/null 2>"$tmpdir/rust-$run.stderr"
  else
    /usr/bin/time -lp target/release/casars-imager \
      --ms "$ms_path" \
      --imagename "$prefix" \
      --imsize "$imsize" \
      --cell-arcsec "$cell_arcsec" \
      --field "$field" \
      --spw "$spw" \
      --channel-start "$channel_start" \
      --channel-count "$channel_count" \
      --specmode "$specmode" \
      --datacolumn DATA \
      --weighting "$weighting" \
      --robust "$robust" \
      --deconvolver "$deconvolver" \
      --niter "$niter" \
      --gain "$gain" \
      --threshold-jy "$threshold_jy" \
      --psfcutoff "$psfcutoff" \
      --minor-cycle-length "$minor_cycle_length" \
      --cyclefactor "$cyclefactor" \
      --minpsffraction "$min_psf_fraction" \
      --maxpsffraction "$max_psf_fraction" \
      --wterm "$wterm" \
      --no-preview-pngs \
      $dirty_flag \
      >/dev/null 2>"$tmpdir/rust-$run.stderr"
  fi
  real_seconds="$(awk '/^real / {print $2}' "$tmpdir/rust-$run.stderr")"
  printf "  run=%s real=%s\n" "$run" "$real_seconds"
  printf "%s\n" "$real_seconds" >>"$rust_cli_file"
done
echo "  median=$(median_from_file "$rust_cli_file")"
echo

echo "Rust stage medians (milliseconds):"
if [[ -n "$scales" ]]; then
  target/release/examples/profile_imager \
    "$ms_path" \
    --field "$field" \
    --spw "$spw" \
    --channel-start "$channel_start" \
    --channel-count "$channel_count" \
    --specmode "$specmode" \
    --datacolumn DATA \
    --weighting "$weighting" \
    --robust "$robust" \
    --deconvolver "$deconvolver" \
    --scales "$scales" \
    --imsize "$imsize" \
    --cell-arcsec "$cell_arcsec" \
    --niter "$niter" \
    --gain "$gain" \
    --threshold-jy "$threshold_jy" \
    --psfcutoff "$psfcutoff" \
    --minor-cycle-length "$minor_cycle_length" \
    --cyclefactor "$cyclefactor" \
    --minpsffraction "$min_psf_fraction" \
    --maxpsffraction "$max_psf_fraction" \
    --wterm "$wterm" \
    $dirty_flag \
    --repeats "$repeats" \
    --warmups 1 \
    | sed 's/^/  /'
else
  target/release/examples/profile_imager \
    "$ms_path" \
    --field "$field" \
    --spw "$spw" \
    --channel-start "$channel_start" \
    --channel-count "$channel_count" \
    --specmode "$specmode" \
    --datacolumn DATA \
    --weighting "$weighting" \
    --robust "$robust" \
    --deconvolver "$deconvolver" \
    --imsize "$imsize" \
    --cell-arcsec "$cell_arcsec" \
    --niter "$niter" \
    --gain "$gain" \
    --threshold-jy "$threshold_jy" \
    --psfcutoff "$psfcutoff" \
    --minor-cycle-length "$minor_cycle_length" \
    --cyclefactor "$cyclefactor" \
    --minpsffraction "$min_psf_fraction" \
    --maxpsffraction "$max_psf_fraction" \
    --wterm "$wterm" \
    $dirty_flag \
    --repeats "$repeats" \
    --warmups 1 \
    | sed 's/^/  /'
fi
echo

cat >"$tmpdir/casa-imager-bench.py" <<'PY'
import os
import statistics
import tempfile
import time
from casatasks import tclean

vis = os.environ["CASA_RS_BENCH_MS_PATH"]
repeats = int(os.environ["CASA_RS_BENCH_REPEATS"])
field = os.environ["CASA_RS_BENCH_FIELD"]
spw = os.environ["CASA_RS_BENCH_SPW"]
chan_start = int(os.environ["CASA_RS_BENCH_CHANNEL_START"])
chan_count = int(os.environ["CASA_RS_BENCH_CHANNEL_COUNT"])
imsize = int(os.environ["CASA_RS_BENCH_IMSIZE"])
cell_arcsec = os.environ["CASA_RS_BENCH_CELL_ARCSEC"]
niter = int(os.environ["CASA_RS_BENCH_NITER"])
gain = float(os.environ["CASA_RS_BENCH_GAIN"])
threshold_jy = os.environ["CASA_RS_BENCH_THRESHOLD_JY"]
psfcutoff = float(os.environ["CASA_RS_BENCH_PSFCUTOFF"])
cycleniter = int(os.environ["CASA_RS_BENCH_MINOR_CYCLE_LENGTH"])
cyclefactor = float(os.environ["CASA_RS_BENCH_CYCLEFACTOR"])
minpsffraction = float(os.environ["CASA_RS_BENCH_MIN_PSFFRACTION"])
maxpsffraction = float(os.environ["CASA_RS_BENCH_MAX_PSFFRACTION"])
weighting = os.environ["CASA_RS_BENCH_WEIGHTING"]
robust = float(os.environ["CASA_RS_BENCH_ROBUST"])
deconvolver = os.environ["CASA_RS_BENCH_DECONVOLVER"]
scales = [] if os.environ["CASA_RS_BENCH_SCALES"] == "" else [int(float(v)) for v in os.environ["CASA_RS_BENCH_SCALES"].split(",")]
specmode = os.environ["CASA_RS_BENCH_SPECMODE"]
spw_selector = f"{spw}:{chan_start}" if chan_count == 1 else f"{spw}:{chan_start}~{chan_start + chan_count - 1}"
times = []

with tempfile.TemporaryDirectory() as td:
    for run in range(repeats):
        prefix = os.path.join(td, f"run-{run}")
        start = time.perf_counter()
        kwargs = dict(
            vis=vis,
            imagename=prefix,
            datacolumn="data",
            field=field,
            stokes="I",
            specmode=specmode,
            gridder="standard",
            weighting=weighting,
            deconvolver=deconvolver,
            scales=scales,
            imsize=imsize,
            cell=f"{cell_arcsec}arcsec",
            niter=niter,
            cycleniter=cycleniter,
            robust=robust,
            gain=gain,
            threshold=f"{threshold_jy}Jy",
            cyclefactor=cyclefactor,
            minpsffraction=minpsffraction,
            maxpsffraction=maxpsffraction,
            restoration=True,
            calcpsf=True,
            calcres=True,
            restart=True,
            interactive=False,
            parallel=False,
            pbcor=False,
            usemask="user",
            mask="",
            savemodel="none",
            psfcutoff=psfcutoff,
        )
        if specmode == "cube":
            kwargs.update(
                spw=str(spw),
                interpolation="nearest",
                nchan=chan_count,
                start=chan_start,
                width=1,
            )
        else:
            kwargs.update(spw=spw_selector)
        tclean(**kwargs)
        elapsed = time.perf_counter() - start
        times.append(elapsed)
        print(f"run={run + 1} real={elapsed:.6f}")

print(f"median={statistics.median(times):.6f}")
PY

echo "CASA tclean timings (seconds):"
CASA_RS_BENCH_MS_PATH="$ms_path" \
CASA_RS_BENCH_REPEATS="$repeats" \
CASA_RS_BENCH_FIELD="$field" \
CASA_RS_BENCH_SPW="$spw" \
CASA_RS_BENCH_CHANNEL_START="$channel_start" \
CASA_RS_BENCH_CHANNEL_COUNT="$channel_count" \
CASA_RS_BENCH_SPECMODE="$specmode" \
CASA_RS_BENCH_IMSIZE="$imsize" \
CASA_RS_BENCH_CELL_ARCSEC="$cell_arcsec" \
CASA_RS_BENCH_WEIGHTING="$weighting" \
CASA_RS_BENCH_ROBUST="$robust" \
CASA_RS_BENCH_DECONVOLVER="$deconvolver" \
CASA_RS_BENCH_SCALES="$scales" \
CASA_RS_BENCH_NITER="$casa_niter" \
CASA_RS_BENCH_GAIN="$gain" \
CASA_RS_BENCH_THRESHOLD_JY="$threshold_jy" \
CASA_RS_BENCH_PSFCUTOFF="$psfcutoff" \
CASA_RS_BENCH_MINOR_CYCLE_LENGTH="$minor_cycle_length" \
CASA_RS_BENCH_CYCLEFACTOR="$cyclefactor" \
CASA_RS_BENCH_MIN_PSFFRACTION="$min_psf_fraction" \
CASA_RS_BENCH_MAX_PSFFRACTION="$max_psf_fraction" \
  "$CASA_RS_CASA_PYTHON" "$tmpdir/casa-imager-bench.py" | sed 's/^/  /'
echo

echo "CASA PySynthesisImager stage medians (milliseconds):"
CASA_RS_BENCH_MS_PATH="$ms_path" \
CASA_RS_BENCH_REPEATS="$repeats" \
CASA_RS_BENCH_FIELD="$field" \
CASA_RS_BENCH_SPW="$spw" \
CASA_RS_BENCH_CHANNEL_START="$channel_start" \
CASA_RS_BENCH_CHANNEL_COUNT="$channel_count" \
CASA_RS_BENCH_SPECMODE="$specmode" \
CASA_RS_BENCH_IMSIZE="$imsize" \
CASA_RS_BENCH_CELL_ARCSEC="$cell_arcsec" \
CASA_RS_BENCH_WEIGHTING="$weighting" \
CASA_RS_BENCH_ROBUST="$robust" \
CASA_RS_BENCH_DECONVOLVER="$deconvolver" \
CASA_RS_BENCH_SCALES="$scales" \
CASA_RS_BENCH_NITER="$casa_niter" \
CASA_RS_BENCH_GAIN="$gain" \
CASA_RS_BENCH_THRESHOLD_JY="$threshold_jy" \
CASA_RS_BENCH_PSFCUTOFF="$psfcutoff" \
CASA_RS_BENCH_MINOR_CYCLE_LENGTH="$minor_cycle_length" \
CASA_RS_BENCH_CYCLEFACTOR="$cyclefactor" \
CASA_RS_BENCH_MIN_PSFFRACTION="$min_psf_fraction" \
CASA_RS_BENCH_MAX_PSFFRACTION="$max_psf_fraction" \
  "$CASA_RS_CASA_PYTHON" "$repo_root/tools/perf/imager/casa_phase_bench.py" | sed 's/^/  /'
