# T55 travel recovery — 2026-09-14

Work issue: #541. Related: #543, draft PR #630.

## 2026-09-15: plan-identity CI repair and serial restart

The previous run `22745ec0e448480f9534d180d1012b11` is archived at
`autoresearch-results/archive/20260915-050733`. Its retained trial
`ef3cf12fd9` caches leading bounded-record sort keys: matched Rust/CASA ratio
1.09758232221312, paired candidate/parent 0.9909536096386621 with 95% interval
[0.9830882437329485, 0.9988819037517114]. This remains short of serial parity.

Hosted CI run `34927655662` failed the exact plan-ID golden in
`plan_seals_physical_work_and_every_required_binding`. The logical, geometry,
numerics, product-graph and physical-work identities matched in the local
macOS/Linux diagnostic; the numerical publication artifact identity differed.
The fixture now uses a single phase-centre pixel and passes that same compiled
problem to the existing transaction-staging helper. All binding assertions,
repeatability and one platform-independent version-12 golden remain. Production
hashing, scientific tests and the serial performance workload are unchanged.

The new digest matched on macOS release and Linux ARM64 debug before replacing
the golden. After replacement, all 28 `compile_plan_run` tests selected by
`plan_` pass on both platforms (macOS 3.32 seconds, Linux 91.84 seconds, excluding
build). macOS build/test used 143.05 seconds and 2,418,327,552 bytes sampled peak
under 600-second/8-GiB caps; Linux used two CPUs, 8 GiB without swap and a
600-second timeout. `cargo fmt --all -- --check` and `git diff --check` pass.
This is local CI-failure repair evidence, not a green hosted x86 CI claim.
The remote PR still points to `0454aefec6`; no push or merge is included.

On resumption, the earlier `/private/tmp` evidence and input directories were
absent. New evidence is under `/private/tmp/casa-t55-restart.ENFucS`, including
the red/green macOS logs and retained Linux container logs. Snapshot 19 and all
664 manifest entries were verified again. Restore the exact frozen inputs and
original reference executable before initializing a fresh baseline; never use
the current candidate as its own reference. The approved remaining allowance
is 19 trials, preserving all retired hypotheses and the 0.90 target. Full-size
acceptance still requires GLENDENNING; the sections below record the earlier
recovery rather than asserting its temporary files still exist.

## Current source and result

Local branch `codex/t55-serial-autoresearch` was fast-forwarded to the published
PR tip `5f5335d7539898bf58ceb22a0c30298ae3481738`. No new commit, push, merge,
release, or optimization trial was performed in this recovery pass.

The pre-existing `spectral_cycle_plan.rs` edit is preserved byte-for-byte. It
selects bounded replay when whole-spill retention exceeds the explicit memory
ceiling. Added assertions in `spectral_cycle_plan/admission_tests.rs` establish
that the bounded physical candidate fits while whole-spill retention does not.

The CI repair changes only the test mutation helper in `managed_spill.rs`:
flush injected writes before replay. On Linux, unsynced dirty pages prevented
`POSIX_FADV_DONTNEED` eviction, so `PageCacheRetention` masked the intended
checksum/framing/seal rejection. The original Rust corruption test reproduced
that exact error in Linux; the fix preserves every production integrity check.
The assertion now prints the actual unexpected error.

## Verification

- Runtime unit suite: macOS 255 passed / 8 ignored; Linux 255 passed / 7 ignored.
  All 32 spill tests pass on both platforms. Linux used arm64 Rust 1.98.1 in an
  isolated container, not the hosted x86 GitHub runner. Its first full-suite run
  lacked measures tables; installing the same bundled runtime as CI resolved
  those environment failures. No test was weakened or skipped to obtain green.
- Reconstruction unit suite: 224 passed / 17 ignored.
- Taylor/joint integration suite: 17 passed.
- Synthetic production T55 cube suite: 5 passed, including exact worker-count
  and channel-window equivalence.
- Whole-phase admission regression and T59 explicit-memory adaptation pass.
- Focused runtime Clippy with warnings denied, rustfmt, and diff whitespace pass.
- Restored real small-fixture smoke: both native builds complete 3 major cycles
  and 19 actual minor iterations. All seven current-native products have zero
  RMS difference from the preserved native reference. Worst CASA product nRMS
  is `1.4303151613360715e-6`, below the unchanged `0.001` ceiling. WCS, topology,
  and beam checks pass. All seven CASA review panels were visually inspected.

Native checks used 600-second / 8-GiB sampled-process guards. Linux used two CPUs,
an 8-GiB hard container memory limit, no swap allowance, and 600-second commands.
Docker Desktop was stopped after testing; its rebuildable images/cache remain.
This is setup and focused correctness evidence, not a paired speed result or
full-input T55/T57 acceptance. Hosted CI has not been rerun with the local fix.

## Recovered setup and important restoration corrections

Evidence root: `/private/tmp/casa-t55-travel.OWyd8m`.
The verified snapshot, commands/scripts, successful and failed logs, resource
receipts, input materialization, binaries, product comparisons, and panels are
there. `setup-smoke.json` identifies the actual dirty-tree executable. Its SHA
is `1b2ec41ee93add9c7d895248bd726d48d0e7d674546d22e929e670d36ca6e613`.

Snapshot 19 was recovered from `origin/codex/t55-autoresearch-checkpoint` and its
archive and manifest verified. The exact upstream fixture was materialized from
`casatestdata` revision `f661170943a0c6eafd6cd0b51ed09ddcb7b36f78`, path
`measurementset/vla/refim_point_withline.ms`, using only its LFS pointers. It is
63 files / 83,431,604 bytes excluding `table.lock`. Both configured input copies
match the frozen SHA `d62a5398f825f6a96510bd64516ab0ad731ce73a75201230d995b4df965be8f0`.

The old restore recipe omitted required lock synchronization state:

1. For the native copy, stage upstream bytes without locks, run the captured
   `initialize_imaging_owner` executable, then replace the root `table.dat` with
   the captured owner metadata. This reproduces the original write/unlock and
   its MAIN counter of 1 while restoring the exact original scientific IDs.
   Simply replacing `table.dat` without initialization produces counter 0 and
   correctly fails owner validation. Verify the full frozen tree afterward.
2. For the separate CASA copy, stage without locks and replace `table.dat`, but
   let CASA create its own lock files. Copying native-created sync files into
   this copy triggered a C++ data-manager counter-vector assertion. The failed
   locks were preserved, not deleted. Frozen non-lock bytes remained unchanged.

Immutable experiment files are restored at
`/private/tmp/casa-t55-serial-ratio-t2.UrhEd2`; the original native product prefix
is `/private/tmp/casa-t55-serial.d5AeGJ/measured/candidate-1/natural-w1/image`.
All frozen harness/control hashes match. Recovery scripts are in the evidence
root; successful native runs were reused after repairing CASA-only setup.

## Next action and boundaries

After the recovery pass, the user authorized committing and pushing these
repairs and this record to the existing draft PR #630, without merging. This
checkpoint includes the preserved bounded planner edit and its focused coverage;
it is not an autoresearch trial or a performance keep. Hosted CI must evaluate
the pushed checkpoint; the local green evidence above does not establish its
result.

Establish a clean, fresh baseline before restarting the controlled loop.
Controller status is `not_initialized`:
snapshot controller history is preserved separately, not installed over this
newer source tree. Do not forge continuity with the older snapshot HEAD or
reset retired hypotheses and retry allowances. No new trial is running.

GLENDENNING is unavailable. Full-size cube/VLASS numerical, visual, memory,
timing, worker/Metal, 10x, and independent-final-review obligations remain open;
none were waived or reclassified as complete by this small-fixture recovery.
