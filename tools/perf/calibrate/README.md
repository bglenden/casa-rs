# `calibrate` Apply Performance Tracing

With `CASA_RS_CALIBRATION_PERF=1`, `calibrate` writes an apply-performance
trace, `casa-calibration-perf-<pid>.jsonl`, and a text summary,
`casa-calibration-perf-<pid>.log`, to `/tmp` or to
`CASA_RS_CALIBRATION_PERF_DIR`. `CASA_RS_CALIBRATION_PROFILE=1` keeps the
human-readable stderr timing summaries.

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

python3 tools/perf/calibrate/report.py /tmp/calibrate-perf/casa-calibration-perf-<pid>.jsonl
```

The trace has one `apply_plan_summary` and one `apply_completed` event per
apply, with the selected row and table counts and the planning, open, apply,
save and row-read timings. The report prints event counts, median and p95
phase timings, and the last completed apply.

`scripts/bench-calibrate-vs-casa.sh` times the same apply in Rust and CASA on
one staged dataset and collects these traces; with
`CAL_BENCH_CASA_PROFILE_DIR=/path` each CASA run also writes a `.pstats`
profile and a cumulative-time summary. The trace does not instrument CASA.
