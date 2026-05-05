# Wave 7 Ticket Closeout

Truth class: current evidence
Last reality check: 2026-05-05
Verification: `bash -n scripts/run-wave7-ticket-closeout.sh`; `WAVE7_TICKET_OUTDIR=target/wave7-ticket-closeout/current WAVE7_RUN_ISSUE197=1 WAVE7_RUN_ISSUE198=0 WAVE7_RUN_ISSUE199=0 WAVE7_TICKET_REPEATS=1 scripts/run-wave7-ticket-closeout.sh`; `WAVE7_TICKET_OUTDIR=target/wave7-ticket-closeout/current WAVE7_RUN_TARGETED_BENCHES=1 WAVE7_TICKET_REPEATS=1 scripts/run-wave7-ticket-closeout.sh`; `just quick`; `just verify`

Wave issue: #144
Wave child: #130
Ticket closeouts: #197, #198, #199
Follow-up implementation tickets: #204, #205

This document records the Wave 7 ticket-level performance triage after the
initial closeout split. The ticket work stayed inside measurement, evidence,
and follow-up shaping; it did not change imaging, calibration, plotting, table,
or runtime algorithms.

The current rerun artifacts are under:

```text
target/wave7-ticket-closeout/current/
```

## #197 Imaging Throughput

Existing same-input tutorial wall-clock evidence already showed the serial
mosaic/CLEAN gap:

| Workload | CASA C++ | casa-rs | Prior status |
|---|---:|---:|---|
| Wave 6 #161 Antennae Band 7 continuum runner | `42.026 s` | `193.242 s` | `4.60x CASA` |
| Wave 6 #169 VLA 3C391 multiscale mosaic runner | `75.489 s` | `257.494 s` | `3.41x CASA` |
| Wave 6 #163 M100 70-channel raw combine | comparable CASA wall-clock not preserved | `552.409 s` | casa-rs `run_imaging=479.390 s`, `prepare_plane_input=67.155 s` |

The #163 blocker is now explicit: the same-input CASA products and comparison
panels exist in the Wave 6 evidence trail, but a comparable 70-channel CASA
wall-clock timing sidecar was not preserved in the current branch. The raw M100
archives are staged under the explicit external tutorial root
`/Volumes/GLENDENNING/casa-rs/tutorial-data`; `/Volumes/home/casatestdata` is
not needed for this tutorial data.

Current casa-rs managed-output profiles localize the confirmed #161/#169
bottleneck to serial gridding/degridding:

| Current profile | Wall | Frontend total | prepare | run_imaging | Dominant core stages |
|---|---:|---:|---:|---:|---|
| Antennae North continuum clean | `134.19 s` | `132.804 s` | `7.299 s` | `125.471 s` | `psf_grid=80.775 s`, `residual_degrid_grid=43.334 s` |
| VLA 3C391 multiscale mosaic | `547.93 s` | `547.390 s` | `46.314 s` | `501.041 s` | `psf_grid=184.120 s`, `residual_degrid_grid=307.836 s`, `weighting=6.465 s` |

The first implementation follow-up is #204. It targets the serial mosaic
PSF/residual grid-degrid hot path and explicitly excludes parallel/chanchunks
work, which remains owned by #56.

## #198 Calibration Apply/Export

The targeted TW Hydra applycal benchmark used the staged `twhya_selfcal.ms`
archive extracted under the run directory, with `field=5`, `spw=0`,
`refant=DV22`, and `applymode=calflag`.

| Runtime | Wall median |
|---|---:|
| CASA `applycal` | `0.893 s` |
| casa-rs `calibrate apply` | `2.510 s` |
| Ratio | `2.81x CASA` |

casa-rs internal timing:

| Stage | Time |
|---|---:|
| report total | `2.460 s` |
| save | `1.733 s` |
| ensure corrected data | `0.337 s` |
| row loop | `0.281 s` |
| row compute | `0.167 s` |
| row read/fetch | `0.098 s` |
| planning | `0.064 s` |
| calibration load | `0.001 s` |

The dominant bottleneck is save/persistence after apply, not caltable loading
or calibration row math. The implementation follow-up is #205.

## #199 Plot Export

Direct `plotms` timing now works on this local CASA path. The repeated run in
`issue199-plotms-msexplore-bench.log` used the same `ngc5921.ms`, amplitude vs
time, `spw=0`, `iteraxis=scan`, `gridcols=2`, correlation coloring, and
`1600x900` PNG export.

| Runtime | Median |
|---|---:|
| CASA `plotms` PNG export | `4.141 s` |
| casa-rs `msexplore` CLI PNG export | `3.200 s` |
| casa-rs in-process fresh pipeline | `3.107 s` |
| casa-rs in-process reused-open pipeline | `2.330 s` |

The Rust path is not slower than direct `plotms` in this measured scenario.
The dominant Rust in-process cost is rendering (`2.809 s` median), followed by
fresh payload build (`1.891 s` median). Since no direct plotms regression is
confirmed, #199 does not need a performance implementation follow-up from this
wave.

## Rerun Harness

Use:

```bash
scripts/run-wave7-ticket-closeout.sh
```

By default the script runs data preflight only. To rerun one slice:

```bash
WAVE7_RUN_ISSUE197=1 scripts/run-wave7-ticket-closeout.sh
WAVE7_RUN_ISSUE198=1 scripts/run-wave7-ticket-closeout.sh
WAVE7_RUN_ISSUE199=1 scripts/run-wave7-ticket-closeout.sh
```

Set `WAVE7_RUN_TARGETED_BENCHES=1` to run all three targeted slices.
