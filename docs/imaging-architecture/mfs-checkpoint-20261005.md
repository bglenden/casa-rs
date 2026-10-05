# MFS engineering checkpoint — 2026-10-05

Truth class: evidence / restart handoff, not scientific acceptance
Work issue: #541 (T55)

Owner requested a break, commit/push of all current code, and preservation
before deleting worktree `5d43`. No further benchmark was started. No merge,
release, issue closure or worktree deletion is part of this checkpoint.

## Source and verified change

Branch: `codex/t55-full-size-validation`; parent:
`b9c738aaf715ed3a67a2fe86507ff097f77c89e6`.
The checkpoint commit contains all nine modified tracked files, the Wave3
timing driver and this handoff. The issue comment records its exact commit ID.

Ordinary scalar Stokes-I MFS already combines correlations and prediction /
accumulation into one normal-operator replay record per row/channel/chart.
The planner counted correlations and processing roles separately. This was a
fourfold storage over-reservation for the dual-correlation Wave3 input.
Reconstruction now supplies the finite record capacity; runtime prices the
shared-role records. Other layouts retain bounded capacity and typed exhaustion.
No numerical operator, ownership, spill I/O or admission check is bypassed.

Full-input W1 and W4 admission both pass: temporary-storage reservation
**345,089,170,385 → 86,303,890,384 bytes**. These are scratch-disk ceilings,
not measured cache size or RAM. The MS is streamed, not loaded into RAM or
copied as a visibility replay. The computed normal-operator cache stores
gridding geometry/coefficients/weights for later CLEAN major cycles. The new
bound does not establish that this cache is economically justified at scale;
actual size, traffic and end-to-end time remain to be measured.

Verification saved with the frozen candidate:

- Storage regression red before correction; green for one/two/four correlations.
- Runtime library: 352 passed, 13 opt-in ignored.
- Major-cycle integration: 34 passed, including actual dual-correlation emission.
- Connected multi-worker CPU publication and CPU/resident/streaming Metal pass.
- Full-input W1/W4 production-admission probes pass, about 1.27 seconds each.
- Architecture, format, diff and Python syntax checks pass. Mechanical source
  binding revision 93 preserves all scientific and issue-scope ratchets.
- Scoped Clippy is not green: three pre-existing findings in
  `GriddedNormalReplayState`, `managed_spill::load_retained_block_source` and the resident
  candidate closure in `spectral_cycle_plan.rs`. The unchanged parent contains
  the same code. No lint waiver or unrelated production repair was added.

## Workload and completed timing

Existing Wave3 VLA B-array input: 4,212,000 rows, one 512-channel SPW,
two correlations, DATA only, 1.500–2.011 GHz, approximately 35.82 GB on disk.
All rows/channels form one 4096² MFS image with 0.8-arcsec pixels; standard
gridder, uniform weighting, single-term Stokes I, Clark, gain 0.1,
niter 10,000 / cycleniter 1,000 / requested threshold 5 mJy. Full recipe saved.

Matching CASA serial: **7,268.199736 seconds** complete tclean; guard wall
7,272.066 seconds; peak sampled aggregate RSS 2,637,905,920 bytes. It stopped
at 10,000 components / 13 major cycles with residual peak 22.823 mJy, not 5 mJy.
Original native W1 failed admission; its unchanged reproduction is preserved.
**No native end-to-end timing or CASA product comparison exists for this
workload yet.** W1/W4/Metal reruns were not launched before the owner's break.
The system was non-quiescent; this is one CASA observation, not a statistical
claim. Standard imaging of a widefield simulation is not w-term/intrinsic-sky
acceptance. This is not full T55 acceptance or goal completion.

Earlier matching representative four-SPW A+C pilot remains useful, not the same
workload: CASA 52.8241 s, CPU W1 29.8016 s, CPU W4 15.8242 s, Metal W4
14.7979 s, with seven products/nine checks and panels passing. The planned
full A+C observation remains **24/32 SPWs complete**, not fully qualified.

## Durable location and restart

External checkpoint root:
`/Volumes/GLENDENNING/casa-rs-evidence/checkpoints/5d43-20261005/`.
The needed internal evidence is relocated to its `internal-evidence/` subtree;
the original `/Users/brianglendenning/SoftwareProjects/casa-rs-evidence` path
remains a symlink, preserving embedded historical paths without rewriting
controllers. Relocation is complete only when the verified manifest/receipt
exists. The issue comment records the verification outcome.

The single current summary is `internal-evidence/t55/tranche5-20260916/CURRENT.md`.
The latest repair report, logs, source and exact frozen binaries are in
`internal-evidence/t55/wave3-standard-mfs-4096-20261005-v1/`.
Original input, the owner-initialized COW input, failed metadata clone and saved
CASA products remain under the existing external Wave3 directories. The
checkpoint separately preserves ignored worktree evidence; Rust/Swift build
caches are recreatable and do not need preservation. Check the manifest before
deleting anything. No independent off-device backup is claimed.

On a future authorized resumption:

1. Mount GLENDENNING, fetch the checkpoint branch into a new checkout, and read
   CURRENT plus `STORAGE-REPAIR.md`. Do not recreate old controller state or
   reuse the expired overnight qualification deadline.
2. Set `CASA_RS_MFS_SOURCE_ROOT` to that checkout. The committed timing driver
   accepts this override rather than requiring deleted worktree `5d43`.
3. Reuse the completed matching CASA reference and same input/settings. Resume
   only native W1, CPU W4 and Metal W4 with a fresh attempt label:

   ```sh
   export CASA_RS_MFS_SOURCE_ROOT=/absolute/path/to/new/casa-rs
   python3 "$CASA_RS_MFS_SOURCE_ROOT/tools/perf/imager/wave3_mfs_timing.py" \
     resume-native \
     --binary /Users/brianglendenning/SoftwareProjects/casa-rs-evidence/t55/wave3-standard-mfs-4096-20261005-v1/continuum_application-storage-v3 \
     --attempt resumed-after-checkpoint-v1
   ```

   Detach using the previously established supervised mechanism if required;
   verify real progress rather than assuming a launch succeeded. Do not use
   `run` or the old `resume` action: those repeat CASA / input staging.
4. Retain native planning/sampled aggregate RSS 16 GiB, builds/checks/CASA 8 GiB,
   two Cargo jobs, incremental off and the exact retained static SIMD-v2 FFTW.
   Measure actual cache bytes/I/O and complete application time before judging
   the cache's benefit. Run unchanged seven-product/nine-check CASA comparison,
   inspect panels, and diagnose demonstrated failures. Do not claim parity from
   admission or small tests. CI/full verification, final review and broader/full
   qualification remain pending. No automatic push/merge/release/cleanup.

The earlier native keyword initializer damaged CASA update access on this
legacy MAIN format. Its failed clone is preserved; CASA wrote the same private
owner keyword into a fresh COW clone. Scientific input stayed unchanged.
Underlying native legacy-keyword-write interoperability remains an adjacent
unresolved risk, not repaired by this storage-accounting change.
