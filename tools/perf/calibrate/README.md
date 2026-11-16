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

## Calibration product lifecycle

`lifecycle.py` compares replacement/append gain and B-bandpass solves,
opacity accumulation, flux-table append and corrected-data exports through
both public Python APIs. Run it with the pinned CASA Python and an installed
`casars`, on an idle host, after building release `calibrate`:

```bash
OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 RAYON_NUM_THREADS=1 \
  "$CASA_RS_CASA_PYTHON" tools/perf/calibrate/lifecycle.py \
  --vis target/calibration-perf.ms --native-bin target/release/calibrate \
  --out target/calibration-perf.json --repeats 5
```

The input is the FLUX/PHASE fixture from
`tools/casa-compat/record_casa_returns.py::make_cal_ms`; its keyword controls
`channels`, `scan_seconds` and `integration_seconds` let a run exceed tiny
startup-dominated inputs. Stage it before timing. Output preparation is
excluded equally; public task launch stays included. The output records
alternating samples, CPU/wall time, imports separately, input size, memory,
workers, a local warm-file throughput baseline and native phase timings.
The native solver phases use `CASA_RS_CALIBRATION_TIMING=1`; the input phase
includes selection, read/decode and averaging, so it alone cannot establish
an I/O bound. Profile it if low throughput or CPU time warrants investigation.
Formal bandpass error estimates remain CAL-02's numerical work; the lifecycle
oracle checks their preservation separately from the cross-CASA gain/flag
comparison. `calibration_lifecycle_oracle.py` performs bidirectional table
exchange and full spectral-subtable checks outside benchmark timing.
