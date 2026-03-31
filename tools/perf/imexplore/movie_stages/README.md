# `imexplore` Movie Stages

This directory holds the staged movie-performance harness for `imexplore`.

## Progression Rule

Work stays in the current stage until that stage sustains at least `30 FPS` on:

`/Volumes/home/casatestdata/unittest/imval/n4826_bima.im`

The currently implemented stages are:

1. `stage1`: no GUI / no terminal
2. `stage2`: Ghostty producer, no `ratatui`
3. `stage3`: minimal `ratatui` UI

Progression is still strict: stay in the first stage whose performance path does not
meet the `30 FPS` target. At the moment, that means:

- Stage 1 passes
- Stage 2 `preload-then-place` passes
- Stage 3 is the current active optimization stage

## Stage 1 Example

Example:

```bash
cargo run --release -p casars --example imexplore_movie_stage1 -- --mode render-only
cargo run --release -p casars --example imexplore_movie_stage1 -- --mode preview-render
```

Useful options:

```bash
--image /Volumes/home/casatestdata/unittest/imval/n4826_bima.im
--target-fps 30
--warmup-loops 1
--measure-loops 3
--preview-workers 2
--render-workers 4
--ready-buffer 32
--bitmap-width 1320
--bitmap-height 588
--output-dir /tmp/imexplore-movie-stages
```

Outputs:

- JSONL trace: `/tmp/imexplore-movie-stages/imexplore-movie-stage1-<mode>-<timestamp>.jsonl`
- Text log: `/tmp/imexplore-movie-stages/imexplore-movie-stage1-<mode>-<timestamp>.log`

## Telemetry Schema

The JSONL stream uses two record kinds:

- `frame`
- `summary`

Common fields:

- `stage`
- `mode`
- `phase`

Frame records include:

- `sequence`
- `loop_index`
- `occurrence_index`
- `preview_ns`
- `render_ns`
- `present_ns`
- `ready_buffer_size`
- `preview_active_workers`
- `render_active_workers`
- `preview_queue_depth`
- `render_queue_depth`
- `cache_result`
- `achieved_fps`
- `stale_count`
- `dropped_count`
- `late`

Summary records include:

- `frame_count`
- `target_fps`
- `achieved_fps`
- `preview_p50_ns`
- `preview_p95_ns`
- `render_p50_ns`
- `render_p95_ns`
- `present_p50_ns`
- `present_p95_ns`
- `ready_buffer_max`
- `preview_queue_max`
- `render_queue_max`
- `preview_max_active`
- `render_max_active`
- `stale_count`
- `dropped_count`
- `late_count`
- `gate_pass`

## Reporting

Summarize one or more traces with:

```bash
python3 tools/perf/imexplore/movie_stages/report.py /tmp/imexplore-movie-stages/*.jsonl
```

The report groups by `stage`, `mode`, and `phase`, then prints:

- frame count
- achieved FPS
- p50/p95 preview/render/present latency
- max queue occupancy
- max worker concurrency
- stale/dropped/late counts
- gate pass/fail

## Stage 2 Example

Stage 2 reuses the exact same real-data preview/render pipeline, but presents the
already-rendered plane-pane bitmaps directly in Ghostty without `ratatui`.

Example:

```bash
cargo run --release -p casars --example imexplore_movie_stage2 -- --mode upload-each-frame
cargo run --release -p casars --example imexplore_movie_stage2 -- --mode preload-then-place
```

Useful options are the same as Stage 1:

```bash
--image /Volumes/home/casatestdata/unittest/imval/n4826_bima.im
--target-fps 30
--warmup-loops 1
--measure-loops 3
--preview-workers 2
--render-workers 4
--ready-buffer 32
--bitmap-width 1320
--bitmap-height 588
--output-dir /tmp/imexplore-stage2
```

Outputs:

- JSONL trace: `/tmp/imexplore-stage2/imexplore-movie-stage2-<mode>-<timestamp>.jsonl`
- Text log: `/tmp/imexplore-stage2/imexplore-movie-stage2-<mode>-<timestamp>.log`

Mode intent:

- `upload-each-frame`: diagnostic lower-bound path that pays Ghostty upload cost every frame
- `preload-then-place`: performance path that preloads one cycle, then re-places cached terminal images

Stage progression for Stage 2 is based on the performance path. The upload-each-frame
mode is still required and measured, but it is expected to run slower because it does
fresh terminal uploads on every frame.

## Stage 3 Example

Stage 3 keeps the Stage 1/2 preview and render pipeline, but presents frames through
one minimal `ratatui` plane panel plus a small status footer.

Example:

```bash
cargo run --release -p casars --example imexplore_movie_stage3
```

Useful options are the same as earlier stages:

```bash
--image /Volumes/home/casatestdata/unittest/imval/n4826_bima.im
--target-fps 30
--warmup-loops 1
--measure-loops 3
--preview-workers 2
--render-workers 4
--ready-buffer 32
--bitmap-width 1320
--bitmap-height 588
--output-dir /tmp/imexplore-stage3
```

Outputs:

- JSONL trace: `/tmp/imexplore-stage3/imexplore-movie-stage3-ratatui-panel-<timestamp>.jsonl`
- Text log: `/tmp/imexplore-stage3/imexplore-movie-stage3-ratatui-panel-<timestamp>.log`
