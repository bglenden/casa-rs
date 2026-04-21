# `calibrate` Apply Performance Tracing

`calibrate` can emit structured apply-performance traces for post-run analysis.

## Files

When `CASA_RS_CALIBRATION_PERF=1` is set, `calibrate` writes:

- JSONL: `/tmp/casa-calibration-perf-<pid>.jsonl`
- text summary: `/tmp/casa-calibration-perf-<pid>.log`

Override the directory with:

```bash
CASA_RS_CALIBRATION_PERF_DIR=/path/to/output
```

## Runtime Controls

- `CASA_RS_CALIBRATION_PERF=1`
  Enable structured apply tracing.
- `CASA_RS_CALIBRATION_PERF_DIR=/path`
  Override the output directory.
- `CASA_RS_CALIBRATION_PROFILE=1`
  Keep the human-readable stderr timing summaries enabled.

Example:

```bash
CASA_RS_CALIBRATION_PERF=1 \
CASA_RS_CALIBRATION_PERF_DIR=/tmp/calibrate-perf \
target/release/calibrate \
  --ms /path/to.ms \
  --gaintables /path/to.phase.gcal \
  --field 0 \
  --spw 0 \
  --apply-mode calflag \
  --format json >/tmp/calibrate-report.json
```

## Event Model

The JSONL trace currently records:

- `apply_plan_summary`
- `apply_completed`

Each event includes:

- selected row count and calibration table count
- row-field index lookup timing before the per-row apply loop
- planning/open/apply/save/drop timings
- row-read totals plus the split between fetch, compute, and read-overhead
- execute-plan unattributed time after the bucketed phases are subtracted

## Analysis Pipeline

1. Run `calibrate` with `CASA_RS_CALIBRATION_PERF=1`.
2. If you want the CLI report too, keep `--format json` and save stdout.
3. If you want CASA-side comparison artifacts too, set `CAL_BENCH_CASA_PROFILE_DIR=/path`
   when running `scripts/bench-calibrate-vs-casa.sh`; each CASA run writes a
   `.pstats` file plus a cumulative-time text summary.
4. Run the report script:

```bash
python3 tools/perf/calibrate/report.py /tmp/casa-calibration-perf-<pid>.jsonl
```

The script prints:

- event counts
- median / p95 timings for the main phase fields
- the most recent apply-completed summary line

## Scope

This trace is for batch MeasurementSet apply work. It does not instrument CASA
internals directly; cross-language comparisons should still be made with the
benchmark scripts that run Rust and CASA on the same staged dataset.
