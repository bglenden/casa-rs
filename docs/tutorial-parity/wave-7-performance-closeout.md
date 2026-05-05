# Wave 7 Performance Closeout

Truth class: current descriptive
Last reality check: 2026-05-05
Verification: `bash -n scripts/run-wave7-performance-closeout.sh`; `just docs-check`

Wave issue: #144
Child issue: #130

Wave 7 consolidates tutorial-program performance evidence and splits remaining
performance work into subsystem tickets. It does not change imaging,
calibration, plotting, table, or simulation algorithms.

## Measurement Policy

- Compare CASA 6.7.5-9 / CASA C++ and casa-rs on the same staged tutorial
  inputs before claiming match/exceed status.
- Record whether a timing is cold path, fresh-open, reused-open, or warm
  in-process. If that distinction is missing, treat the number as evidence for
  follow-up shaping rather than a final performance claim.
- Use the shared tutorial-data registry or shared fixture resolver for new
  timing runs. Do not use personal workstation paths as implicit script
  fallbacks.
- Severe regressions block closeout only when the workload and outputs are
  already equivalent enough for the comparison to be meaningful.
- Non-severe gaps become follow-up tickets with reproduction commands, dataset
  keys, suspected subsystem, and acceptance criteria.

## Current Baseline

| Vertical / capability | Evidence | CASA timing | casa-rs timing | Status |
|---|---|---:|---:|---|
| Wave 3 #117 TW Hydra dirty MFS imaging | `docs/tutorial-parity/wave-3-issue-117-mfs-foundation.md` | `5.246 s` wall | `4.777 s` internal after #95 | Meets current target for this standard-MFS dirty case. |
| Wave 3 #118 TW Hydra self-cal loop | `docs/tutorial-parity/wave-3-issue-118-selfcal-parity.md` | `175.962 s` total | `176.736 s` total | End-to-end runtime is within roughly 2%, but calibration/apply/export is still slower. |
| Wave 3 #120 image analysis | `docs/tutorial-parity/wave-3-issue-120-image-analysis.md` | `0.001977-0.020663 s` warm medians | `0.000206-0.006461 s` warm medians | casa-rs is faster on the measured warm in-process operations. |
| Wave 5 #125 VLA protoplanetary disk imaging / plotting | `docs/tutorial-parity/wave-5-simulation-parity.md` | `2.146 s` `tclean`; `0.209 s` headless plot fallback | `2.897 s` imager; `6.284 s` `msexplore` | Imaging is close after the MFS frequency-cache fix; plot timing needs a fair `plotms` comparison. |
| Wave 5 #126 simulation corruptions | `docs/tutorial-parity/wave-5-simulation-parity.md` | `0.114 s` noise+gain corruption | native corruption timings recorded in JSON artifacts | Direct CASA comparison is limited by simulator feature availability. |
| Wave 6 #161 Antennae Band 7 mosaic | `docs/tutorial-parity/wave-6-issue-161-antennae-band7.md` | `42.026 s` continuum runner; `3.124 s` two-channel cube probe | `193.242 s` continuum runner; `2.395 s` two-channel cube probe | Continuum mosaic/CLEAN is 4.60x CASA and needs imaging-performance follow-up. |
| Wave 6 #163 M100 data combination | `docs/tutorial-parity/wave-6-issue-163-m100-band3-combine.md` | Same-input CASA products recorded; full comparable CASA timing not yet recorded for the 70-channel scale-up | `33.757 s` two-channel release probe; `552.409 s` 70-channel release scale-up | Needs a same-shape CASA timing and cube-scaling breakdown before optimization claims. |
| Wave 6 #169 VLA 3C391 mosaic | `docs/tutorial-parity/wave-6-issue-169-3c391.md` | `75.489 s` | `257.494 s` | Mosaic/CLEAN is 3.41x CASA and belongs with the imaging-performance follow-up. |

## Follow-Up Split

The current evidence supports these subsystem splits:

| Area | Reason | Follow-up |
|---|---|---|
| Imaging mosaic / CLEAN throughput | Wave 6 #161 and #169 are 3.41x-4.60x CASA on same-input mosaic products, and #163 shows large cube scale-up cost. | #197 serial mosaic/PB/CLEAN throughput triage before broader runtime-model changes. |
| Calibration/apply/export overhead | Wave 3 #118 matches total runtime, but calibration/apply/export is `37.347 s` in casa-rs versus `9.513 s` in CASA. | #198 isolate calibration apply, split/export, and caltable I/O overhead in the self-cal loop. |
| Plot export / `msexplore` timing | Wave 5 #125 shows `msexplore` plot output at `6.284 s`, but the CASA side was a matplotlib fallback because `plotms` was unavailable without `DISPLAY`. | #199 run fair `plotms` versus `msexplore` export timing with a display-capable CASA path or equivalent exported-data oracle. |
| Large cube runtime controls | Existing #56 already covers CASA-like `parallel` and `chanchunks` controls. | Keep #56 separate from the serial-throughput triage unless profiling shows the fix requires runtime/concurrency changes. |

## Rerun Harness

Use:

```bash
scripts/run-wave7-performance-closeout.sh
```

By default the script runs shared dataset preflight checks and writes a closeout
run directory under `target/wave7-performance-closeout/` without running heavy
benchmarks. M100-heavy reruns from this workstation should set the tutorial
root explicitly because the full M100 archives are staged on the external
mirror, not under the default home tutorial root:

```bash
CASA_RS_TUTORIAL_DATA_ROOT=/Volumes/GLENDENNING/casa-rs/tutorial-data \
  scripts/run-wave7-performance-closeout.sh
```

Use:

```bash
WAVE7_RUN_BENCHES=1 scripts/run-wave7-performance-closeout.sh
```

to also run the existing shared benchmark scripts and capture their logs.

## Closeout Decision

Wave 7 closeout created #197, #198, and #199 as follow-up tickets. The wave
should not attempt to fix imaging throughput directly; any algorithm,
concurrency, or runtime-model change must happen under the shaped follow-up
issue and the stop-and-ask rules in `AGENTS.md`.
