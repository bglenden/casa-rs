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

repeats="${CAL_BENCH_REPEATS:-5}"
field="${CAL_BENCH_FIELD:-0}"
spw="${CAL_BENCH_SPW:-0}"
refant="${CAL_BENCH_REFANT:-VA15}"
apply_mode="${CAL_BENCH_APPLY_MODE:-calflag}"
json_out="${CAL_BENCH_JSON_OUT:-}"
casa_profile_dir="${CAL_BENCH_CASA_PROFILE_DIR:-}"

echo "ms_path=$ms_path"
echo "CASA_RS_CASA_PYTHON=$CASA_RS_CASA_PYTHON"
echo "repeats=$repeats"
echo "field=$field spw=$spw refant=$refant apply_mode=$apply_mode"
echo "timing_excludes=measurement-set copy and caltable generation"
if [[ -n "$json_out" ]]; then
  echo "json_out=$json_out"
fi
if [[ -n "$casa_profile_dir" ]]; then
  echo "casa_profile_dir=$casa_profile_dir"
fi
echo

cargo build --release -p casa-calibration --bin calibrate >/dev/null

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

median_from_json_reports() {
  python3 - "$1" "$2" <<'PY'
import json
import statistics
import sys

path_list = sys.argv[1]
field = sys.argv[2]
values = []
with open(path_list, "r", encoding="utf-8") as handle:
    for line in handle:
        report_path = line.strip()
        if not report_path:
            continue
        with open(report_path, "r", encoding="utf-8") as report_handle:
            obj = json.load(report_handle)
        current = obj
        for part in field.split("."):
            current = current[part]
        values.append(float(current) / 1_000_000_000.0)
if not values:
    raise SystemExit(f"no values provided for {field}")
print(f"{statistics.median(values):.6f}")
PY
}

median_from_json_traces() {
  python3 - "$1" "$2" <<'PY'
import json
import statistics
import sys

path_list = sys.argv[1]
field = sys.argv[2]
values = []
with open(path_list, "r", encoding="utf-8") as handle:
    for line in handle:
        trace_path = line.strip()
        if not trace_path:
            continue
        with open(trace_path, "r", encoding="utf-8") as trace_handle:
            for raw in trace_handle:
                raw = raw.strip()
                if not raw:
                    continue
                event = json.loads(raw)
                if event.get("kind") != "apply_completed":
                    continue
                value = event.get(field)
                if isinstance(value, int):
                    values.append(float(value) / 1_000_000_000.0)
if not values:
    raise SystemExit(f"no trace values provided for {field}")
print(f"{statistics.median(values):.6f}")
PY
}

copy_ms() {
  local source="$1"
  local destination="$2"
  python3 - "$source" "$destination" <<'PY'
import pathlib
import shutil
import sys

source = pathlib.Path(sys.argv[1])
destination = pathlib.Path(sys.argv[2])
if destination.exists():
    shutil.rmtree(destination)
shutil.copytree(source, destination, symlinks=True)
PY
}

echo "Generating CASA phase.gcal benchmark input..."
phase_gcal="$tmpdir/phase.gcal"
CASA_RS_CAL_MS="$ms_path" \
CASA_RS_CALTABLE="$phase_gcal" \
CASA_RS_CAL_FIELD="$field" \
CASA_RS_CAL_SPW="$spw" \
CASA_RS_CAL_REFANT="$refant" \
  "$CASA_RS_CASA_PYTHON" - <<'PY'
import os
from casatasks import gaincal

gaincal(
    vis=os.environ["CASA_RS_CAL_MS"],
    caltable=os.environ["CASA_RS_CALTABLE"],
    field=os.environ["CASA_RS_CAL_FIELD"],
    spw=os.environ["CASA_RS_CAL_SPW"],
    solint="inf",
    refant=os.environ["CASA_RS_CAL_REFANT"],
    calmode="p",
    minsnr=0.0,
)
PY
echo

warm_rust_ms="$tmpdir/warm-rust.ms"
copy_ms "$ms_path" "$warm_rust_ms"
target/release/calibrate \
  --ms "$warm_rust_ms" \
  --gaintables "$phase_gcal" \
  --field "$field" \
  --spw "$spw" \
  --apply-mode "$apply_mode" \
  --format json \
  >/dev/null

warm_casa_ms="$tmpdir/warm-casa.ms"
copy_ms "$ms_path" "$warm_casa_ms"
CASA_RS_APPLY_MS="$warm_casa_ms" \
CASA_RS_APPLY_CALTABLE="$phase_gcal" \
CASA_RS_APPLY_FIELD="$field" \
CASA_RS_APPLY_SPW="$spw" \
CASA_RS_APPLY_MODE="$apply_mode" \
  "$CASA_RS_CASA_PYTHON" - <<'PY'
import os
from casatasks import applycal

applycal(
    vis=os.environ["CASA_RS_APPLY_MS"],
    field=os.environ["CASA_RS_APPLY_FIELD"],
    spw=os.environ["CASA_RS_APPLY_SPW"],
    gaintable=[os.environ["CASA_RS_APPLY_CALTABLE"]],
    interp=["nearest"],
    calwt=False,
    applymode=os.environ["CASA_RS_APPLY_MODE"],
    flagbackup=False,
)
PY

echo "Rust calibrate timings (seconds):"
rust_times="$tmpdir/rust-times.txt"
rust_reports="$tmpdir/rust-report-paths.txt"
rust_trace_paths="$tmpdir/rust-trace-paths.txt"
for run in $(seq 1 "$repeats"); do
  run_ms="$tmpdir/rust-run-$run.ms"
  copy_ms "$ms_path" "$run_ms"
  rust_report="$tmpdir/rust-run-$run.json"
  run_perf_dir="$tmpdir/rust-run-$run-perf"
  mkdir -p "$run_perf_dir"
  /usr/bin/time -lp \
    env CASA_RS_CALIBRATION_PERF=1 CASA_RS_CALIBRATION_PERF_DIR="$run_perf_dir" \
    target/release/calibrate \
      --ms "$run_ms" \
      --gaintables "$phase_gcal" \
      --field "$field" \
      --spw "$spw" \
      --apply-mode "$apply_mode" \
      --format json \
      >"$rust_report" 2>"$tmpdir/rust-run-$run.stderr"
  real_seconds="$(awk '/^real / {print $2}' "$tmpdir/rust-run-$run.stderr")"
  printf "  run=%s real=%s\n" "$run" "$real_seconds"
  printf "%s\n" "$real_seconds" >>"$rust_times"
  printf "%s\n" "$rust_report" >>"$rust_reports"
  find "$run_perf_dir" -name '*.jsonl' -print | sort | head -n1 >>"$rust_trace_paths"
done
rust_median="$(median_from_file "$rust_times")"
rust_total_median="$(median_from_json_reports "$rust_reports" "timings.total_ns")"
rust_planning_median="$(median_from_json_reports "$rust_reports" "timings.planning_ns")"
rust_planning_selection_median="$(median_from_json_reports "$rust_reports" "timings.planning_selection_ns")"
rust_planning_selected_rows_median="$(median_from_json_reports "$rust_reports" "timings.planning_selected_rows_ns")"
rust_planning_ms_spws_median="$(median_from_json_reports "$rust_reports" "timings.planning_measurement_set_spectral_windows_ns")"
rust_planning_table_plans_median="$(median_from_json_reports "$rust_reports" "timings.planning_calibration_table_plans_ns")"
rust_open_median="$(median_from_json_reports "$rust_reports" "timings.open_measurement_set_ns")"
rust_row_field_index_median="$(median_from_json_reports "$rust_reports" "timings.row_field_index_lookup_ns")"
rust_ensure_median="$(median_from_json_reports "$rust_reports" "timings.ensure_corrected_data_ns")"
rust_corr_lookup_median="$(median_from_json_reports "$rust_reports" "timings.correlation_lookup_ns")"
rust_cal_load_median="$(median_from_json_reports "$rust_reports" "timings.calibration_load_ns")"
rust_row_read_median="$(median_from_json_traces "$rust_trace_paths" "row_read_total_ns")"
rust_row_fetch_median="$(median_from_json_traces "$rust_trace_paths" "row_fetch_ns")"
rust_row_read_overhead_median="$(median_from_json_traces "$rust_trace_paths" "row_read_overhead_ns")"
rust_compute_median="$(median_from_json_reports "$rust_reports" "timings.row_compute_ns")"
rust_writeback_median="$(median_from_json_reports "$rust_reports" "timings.row_writeback_ns")"
rust_save_median="$(median_from_json_reports "$rust_reports" "timings.save_ns")"
echo "  median=$rust_median"
echo "  report_total_median=$rust_total_median"
echo "  planning_median=$rust_planning_median"
echo "  planning_selection_median=$rust_planning_selection_median"
echo "  planning_selected_rows_median=$rust_planning_selected_rows_median"
echo "  planning_ms_spws_median=$rust_planning_ms_spws_median"
echo "  planning_table_plans_median=$rust_planning_table_plans_median"
echo "  open_ms_median=$rust_open_median"
echo "  row_field_index_lookup_median=$rust_row_field_index_median"
echo "  ensure_corrected_data_median=$rust_ensure_median"
echo "  correlation_lookup_median=$rust_corr_lookup_median"
echo "  calibration_load_median=$rust_cal_load_median"
echo "  row_read_median=$rust_row_read_median"
echo "  row_fetch_median=$rust_row_fetch_median"
echo "  row_read_overhead_median=$rust_row_read_overhead_median"
echo "  row_compute_median=$rust_compute_median"
echo "  row_writeback_median=$rust_writeback_median"
echo "  save_median=$rust_save_median"
echo

cat >"$tmpdir/casa-apply-bench.py" <<'PY'
import cProfile
import os
import pathlib
import pstats
import statistics
import time
from casatasks import applycal

vis = os.environ["CASA_RS_APPLY_MS"]
field = os.environ["CASA_RS_APPLY_FIELD"]
spw = os.environ["CASA_RS_APPLY_SPW"]
caltable = os.environ["CASA_RS_APPLY_CALTABLE"]
applymode = os.environ["CASA_RS_APPLY_MODE"]
repeats = int(os.environ["CASA_RS_APPLY_REPEATS"])
profile_dir = os.environ.get("CASA_RS_APPLY_PROFILE_DIR", "").strip()
if profile_dir:
    pathlib.Path(profile_dir).mkdir(parents=True, exist_ok=True)
times = []

for run in range(repeats):
    run_vis = os.path.join(os.environ["CASA_RS_APPLY_RUN_ROOT"], f"casa-run-{run + 1}.ms")
    start = time.perf_counter()
    if profile_dir:
        profiler = cProfile.Profile()
        profiler.runcall(
            applycal,
            vis=run_vis,
            field=field,
            spw=spw,
            gaintable=[caltable],
            interp=["nearest"],
            calwt=False,
            applymode=applymode,
            flagbackup=False,
        )
        profile_path = pathlib.Path(profile_dir) / f"casa-applycal-run-{run + 1}.pstats"
        profiler.dump_stats(profile_path)
        summary_path = pathlib.Path(profile_dir) / f"casa-applycal-run-{run + 1}.txt"
        with summary_path.open("w", encoding="utf-8") as handle:
            stats = pstats.Stats(profiler, stream=handle)
            stats.sort_stats("cumulative")
            stats.print_stats(40)
    else:
        applycal(
            vis=run_vis,
            field=field,
            spw=spw,
            gaintable=[caltable],
            interp=["nearest"],
            calwt=False,
            applymode=applymode,
            flagbackup=False,
        )
    elapsed = time.perf_counter() - start
    times.append(elapsed)
    print(f"run={run + 1} real={elapsed:.6f}")

print(f"median={statistics.median(times):.6f}")
PY

for run in $(seq 1 "$repeats"); do
  copy_ms "$ms_path" "$tmpdir/casa-run-$run.ms"
done

echo "CASA applycal timings (seconds):"
CASA_RS_APPLY_MS="$ms_path" \
CASA_RS_APPLY_RUN_ROOT="$tmpdir" \
CASA_RS_APPLY_REPEATS="$repeats" \
CASA_RS_APPLY_CALTABLE="$phase_gcal" \
CASA_RS_APPLY_FIELD="$field" \
CASA_RS_APPLY_SPW="$spw" \
CASA_RS_APPLY_MODE="$apply_mode" \
CASA_RS_APPLY_PROFILE_DIR="$casa_profile_dir" \
  "$CASA_RS_CASA_PYTHON" "$tmpdir/casa-apply-bench.py" | tee "$tmpdir/casa-output.txt" | sed 's/^/  /'

casa_median="$(awk -F= '/^median=/ {print $2}' "$tmpdir/casa-output.txt" | tail -n1)"
echo
python3 - "$rust_median" "$casa_median" <<'PY'
import sys

rust = float(sys.argv[1])
casa = float(sys.argv[2])
ratio = rust / casa if casa else float("inf")
winner = "Rust" if rust < casa else "CASA"
print(f"Summary: rust_median={rust:.6f}s casa_median={casa:.6f}s ratio={ratio:.3f} winner={winner}")
PY

if [[ -n "$json_out" ]]; then
  mkdir -p "$(dirname "$json_out")"
  python3 - "$json_out" "$ms_path" "$repeats" "$field" "$spw" "$refant" "$apply_mode" "$rust_times" "$rust_reports" "$rust_trace_paths" "$tmpdir/casa-output.txt" <<'PY'
import json
import statistics
import sys
from pathlib import Path

json_out = Path(sys.argv[1])
ms_path = sys.argv[2]
repeats = int(sys.argv[3])
field = sys.argv[4]
spw = sys.argv[5]
refant = sys.argv[6]
apply_mode = sys.argv[7]
rust_times_path = Path(sys.argv[8])
rust_reports_path = Path(sys.argv[9])
rust_traces_path = Path(sys.argv[10])
casa_output_path = Path(sys.argv[11])


def read_lines(path: Path) -> list[str]:
    return [line.strip() for line in path.read_text().splitlines() if line.strip()]


def read_float_lines(path: Path) -> list[float]:
    return [float(value) for value in read_lines(path)]


def median(values: list[float]) -> float:
    return statistics.median(values) if values else 0.0


def report_field(report_path: Path, dotted: str) -> float:
    current = json.loads(report_path.read_text())
    for part in dotted.split("."):
        current = current[part]
    return float(current) / 1_000_000_000.0


def trace_completed_event(path: Path) -> dict | None:
    events = [
        json.loads(line)
        for line in path.read_text().splitlines()
        if line.strip()
    ]
    completed = [event for event in events if event.get("kind") == "apply_completed"]
    return completed[-1] if completed else None


rust_times = read_float_lines(rust_times_path)
rust_report_paths = [Path(value) for value in read_lines(rust_reports_path)]
rust_trace_paths = [Path(value) for value in read_lines(rust_traces_path)]
trace_events = [event for path in rust_trace_paths if path.exists() for event in [trace_completed_event(path)] if event is not None]

report_fields = [
    "timings.total_ns",
    "timings.planning_ns",
    "timings.planning_selection_ns",
    "timings.planning_selected_rows_ns",
    "timings.planning_measurement_set_spectral_windows_ns",
    "timings.planning_calibration_table_plans_ns",
    "timings.open_measurement_set_ns",
    "timings.row_field_index_lookup_ns",
    "timings.ensure_corrected_data_ns",
    "timings.correlation_lookup_ns",
    "timings.calibration_load_ns",
    "timings.row_compute_ns",
    "timings.row_writeback_ns",
    "timings.save_ns",
]

trace_fields = [
    "execute_apply_plan_ns",
    "execute_apply_plan_unattributed_ns",
    "row_field_index_lookup_ns",
    "row_loop_ns",
    "row_read_total_ns",
    "row_fetch_ns",
    "row_compute_ns",
    "row_read_overhead_ns",
    "row_writeback_ns",
    "save_ns",
    "drop_ns",
    "total_ns",
]

report_medians = {
    field_name: median([report_field(path, field_name) for path in rust_report_paths])
    for field_name in report_fields
}
trace_medians = {
    field_name: median([float(event.get(field_name, 0.0)) / 1_000_000_000.0 for event in trace_events])
    for field_name in trace_fields
}

casa_runs: list[float] = []
casa_median = 0.0
for line in read_lines(casa_output_path):
    if line.startswith("run="):
        parts = dict(part.split("=", 1) for part in line.split())
        casa_runs.append(float(parts["real"]))
    elif line.startswith("median="):
        casa_median = float(line.split("=", 1)[1])

winner = "Rust" if median(rust_times) < casa_median else "CASA"
ratio = (median(rust_times) / casa_median) if casa_median else float("inf")

payload = {
    "benchmark": "calibrate_vs_casa",
    "measurement_set": ms_path,
    "repeats": repeats,
    "scenario": {
        "field": field,
        "spw": spw,
        "refant": refant,
        "apply_mode": apply_mode,
    },
    "rust": {
        "real_seconds": {
            "samples": rust_times,
            "median": median(rust_times),
        },
        "report_median_seconds": report_medians,
        "trace_median_seconds": trace_medians,
    },
    "casa": {
        "real_seconds": {
            "samples": casa_runs,
            "median": casa_median,
        }
    },
    "comparison": {
        "ratio_rust_over_casa": ratio,
        "winner": winner,
    },
}

json_out.write_text(json.dumps(payload, indent=2) + "\n")
print(f"Wrote benchmark JSON: {json_out}")
PY
fi
