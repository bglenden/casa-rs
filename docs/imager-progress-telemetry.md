# Imager Progress Telemetry

`casars-imager` can emit coarse running progress for GUI clients without
changing the final stdout result JSON. The normal CLI path enables it with
hidden launcher flags:

```sh
casars-imager --ms example.ms --imagename out/example --imsize 512 \
  --cell-arcsec 0.25 --managed-output true \
  --progress true --progress-max-uv-points 64 --progress-min-interval-ms 250
```

The legacy JSON task protocol can still opt in through the run request:

```json
{
  "kind": "run",
  "request": {
    "measurement_set": "example.ms",
    "image_name": "out/example",
    "image_size": 512,
    "cell_arcsec": 0.25,
    "progress": {
      "enabled": true,
      "max_uv_points": 64,
      "min_interval_ms": 250
    }
  }
}
```

Each progress record is one stderr line beginning with:

```text
CASARS_IMAGER_PROGRESS 
```

The rest of the line is a JSON `ImagerProgressEvent`. Consumers must ignore
ordinary stderr lines and must treat malformed prefixed lines as diagnostics,
not as task failure. Stdout remains reserved for the final managed-output JSON
or JSON task result.

## Payload

The event schema is included in `casars-imager --json-schema` as
`progress_event_schema`. Fields are optional except `schema_version`,
`sequence`, `elapsed_ms`, `phase`, and `summary`.

- `ms_read`: current MeasurementSet row/channel block, reported as
  `[row_start,row_end)` and `[channel_start,channel_end)`.
- `output_cube`: output `X,Y,Z` shape and active whole-plane Z/frequency range,
  reported as `[active_plane_start,active_plane_end)`.
- `uv_coverage`: bounded approximate measured UV points plus derived
  Hermitian/conjugate points. This is display telemetry, not a science product.
- `deconvolution`: major/minor cycle counts, component count, and coarse
  residual values when available.
- `runtime`: active/available thread counts and backend/GPU status.
- `work`: calculable coarse units for progress bars. Current units are output
  planes plus configured CLEAN iteration budget.

Progress event precision is intentionally lower than the final task result.
The interface is designed for a live status screen and must stay below 1%
runtime overhead on representative workloads.

## Throttling

The imager clamps `max_uv_points` to an internal bound and clamps very small
`min_interval_ms` values upward. Forced lifecycle events can still be emitted
at task start, finish, and failure. Hot loops only update bounded state or emit
at existing row-block and slab boundaries.

## Overhead Check

Use `tools/perf/imager/measure_progress_overhead.py` with the same JSON request
for disabled and enabled telemetry:

```sh
cargo build -p casars-imager

python3 tools/perf/imager/measure_progress_overhead.py \
  --binary target/debug/casars-imager \
  --request /path/to/request.json \
  --repeats 5 \
  --output target/imager-progress-overhead.json
```

The report includes disabled/enabled median wall time, overhead percent, event
count, and progress payload bytes. If overhead exceeds 1%, reduce event
frequency or UV sample limits before using the stream as a default GUI path.
