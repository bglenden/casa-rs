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

python3 suites/perf/calibrate/report.py /tmp/calibrate-perf/casa-calibration-perf-<pid>.jsonl
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

`lifecycle.py` compares replacement/append gain and B-bandpass solves
(explicit point sources, plus gaincal's default `MODEL_DATA` input),
opacity accumulation, flux-table append and corrected-data exports through
both public Python APIs. Run it with the pinned CASA Python and an installed
`casars`, on an idle host, after building release `calibrate`:

```bash
OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 RAYON_NUM_THREADS=1 \
  "$CASA_RS_CASA_PYTHON" suites/perf/calibrate/lifecycle.py \
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
Use repeatable `--case` labels to measure only changed workflows. On Linux,
`--cache cold` flushes and evicts only the staged input and product file pages
before each call, outside timing. Block-I/O counters verify physical reads;
user and system CPU are recorded separately. The local cold-read baseline
records its own physical-read count too.

The native solver phases use `CASA_RS_CALIBRATION_TIMING=1`; the input phase
includes selection, read/decode and averaging. `solve_read_decode` and
`solve_accumulate` split batch input from accumulation. Read/decode includes
format conversion, so its duration alone cannot establish a disk I/O limit.
Profile it if low throughput or CPU time warrants investigation.

With at least two available/requested workers and more than 256 selected rows,
one reader overlaps input with ordered accumulation on the calling thread.
The channel holds one 256-row batch; including reader and consumer, at most
three batches are live. A one-worker budget keeps the serial path. The output
records the actual native worker count. Overlapping phase durations must not
be summed to infer wall time; compare matched worker budgets for both engines.

Fresh-process gaincal calls record import, first-call and process times and
verify their products too. Linux per-call native and fresh-Python peaks use
`/proc/self/status`'s `VmHWM`: `getrusage` can retain a parent's pre-exec peak.
Process peaks are reported separately and are not simultaneous totals.
Formal bandpass error estimates remain CAL-02's numerical work; the lifecycle
oracle checks their preservation separately from the cross-CASA gain/flag
comparison. `calibration_lifecycle_oracle.py` performs bidirectional table
exchange and full spectral-subtable checks outside benchmark timing.
