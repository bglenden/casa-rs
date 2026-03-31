# `imexplore` Movie Performance Tracing

`imexplore` can emit structured movie-performance traces for post-run analysis.

## Files

When `CASARS_IMEXPLORE_PERF=1` is set, `casars` writes:

- JSONL: `/tmp/casars-imexplore-perf-<pid>.jsonl`
- text summary: `/tmp/casars-imexplore-perf-<pid>.log`

Override the directory with:

```bash
CASARS_IMEXPLORE_PERF_DIR=/path/to/output
```

## Runtime Controls

- `CASARS_IMEXPLORE_PERF=1`
  Enable tracing.
- `CASARS_IMEXPLORE_PERF_DIR=/path`
  Override the output directory.
- `SIGUSR1`
  Force an immediate summary flush without stopping playback.

Example:

```bash
CASARS_IMEXPLORE_PERF=1 \
CASARS_IMEXPLORE_PERF_DIR=/tmp/imexplore-perf \
cargo run --release -p casars -- \
  imexplore /Volumes/home/casatestdata/unittest/imval/n4826_bima.im
```

While the app is running:

```bash
kill -USR1 <pid>
```

## Event Model

Each movie frame gets a monotonic `frame_seq`. The JSONL file records events such
as:

- `movie_started`
- `movie_stopped`
- `fps_changed`
- `frame_requested`
- `browser_command_sent`
- `browser_snapshot_received`
- `plane_render_requested`
- `plane_render_completed`
- `plane_presented`
- `frame_dropped`
- `summary`

The trace includes:

- movie axis/index metadata
- render request key hash
- canvas cell and pixel sizes
- raster vs spreadsheet mode
- backend timing breakdown
- cache outcome classification

## Analysis Pipeline

1. Run `imexplore` with `CASARS_IMEXPLORE_PERF=1`.
2. Exercise the movie path you care about.
3. Send `SIGUSR1` if you want an immediate summary before exit.
4. Run the report script:

```bash
python3 tools/perf/imexplore/report.py /tmp/casars-imexplore-perf-<pid>.jsonl
```

The script prints:

- event counts
- per-kind outcome counts
- recent summary lines from the trace
- per-frame latency breakdowns when available

## Scope

Wave 1 focuses on the plane movie pipeline. The linked spectrum is not yet
timed in the same per-frame detail.
