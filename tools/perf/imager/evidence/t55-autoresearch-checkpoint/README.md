# T55 serial autoresearch restart checkpoint

This branch is an evidence carrier, not the active implementation branch and
not acceptance, merge, or release. Do not run the controller on this branch.
The canonical current work summary is [draft PR #630](https://github.com/bglenden/casa-rs/pull/630).
Tied tickets: [#541](https://github.com/bglenden/casa-rs/issues/541),
[#543](https://github.com/bglenden/casa-rs/issues/543), and programme
[#486](https://github.com/bglenden/casa-rs/issues/486).

## Snapshot 19 — post-ADR-0013 restart checkpoint, 2026-09-11

- Source: `1c1f54f7777dcb90611586b5ffceb2c7efafdcc9`, verified on
  `origin/codex/t55-serial-autoresearch` and `origin/codex/t55-cube-pipeline`.
- Run: `b13c5d09af904c0e94263a2cb86c92af`, iteration 0, clean/consistent;
  bounded-fixture baseline 1.1833692208217712. Target 0.90 remains unmet.
- Retained since snapshot 18: reader-verified replay digest binding
  (`930e919e`, paired 0.9533445209 [0.9109397678, 0.9977232387]); ADR-0013
  private spill v3 with CRC32C (`b9db62bd`, bounded ratio 1.2399 -> 1.1955,
  paired 0.936815 [0.895951, 0.979542], guard 165.3 s / 3.12 GiB sampled peak);
  `just arch-check` repair (`190718f6`); medium-VLA probe reframe (`e0b8f390`);
  ADR-0013 note alignment (`1c1f54f7`).
- Large 32 GiB serial gate revalidation (user-authorized; provisional single
  pairs): `wave3-standard-mfs-single-term-heavy-wave2-serial` completed in
  600.15 s versus the frozen official CASA anchor 688.996833 s (-12.9%), all
  products RMS-identical and inside the unchanged 0.001 ceiling. Same-session
  control at `b2bc3f7cd` ran 741.01 s, so its historical 486.35 s record does
  not reproduce on today's host; paired same-session comparisons are
  authoritative and absolute anchors are not comparable across host states.
- `just arch-check` and `just docs-check` are green. The ignored medium-VLA
  discriminators record dataset identities, block packing, residency and
  proof-byte totals as JSON evidence and keep structural invariants; all three
  pass on the mounted dataset.
- `snapshot-19.tar.gz` contains the controller state/history (current run and
  all archives), fixed experiment configuration for the current experiment
  root, measurement/comparison/resource receipts and test logs for every
  captured commit, the retained baseline executable, the original
  native-reference executable and reproduction receipt, exact controller
  implementation, the current work record and the current PR #630 body.
  Binaries are macOS arm64 project test executables, not portable.
- No MeasurementSet payloads, generated image arrays, credentials or account
  identifiers are redistributed. `reference-reproduction/input-owner-table.dat`
  is table-description metadata plus the generated casa-rs owner manifest; it
  carries no visibility payload.
- Snapshot 18 remains historical evidence, not the restart tip.

## Snapshot 18 — quota pause, 2026-09-09/10 (historical)

- Source: `bb47e918348b7dd6ddcc070b7ca0a7d1f3cca3b9`, verified on
  `origin/codex/t55-serial-autoresearch` and `origin/codex/t55-cube-pipeline`.
- Run: `5643a1a0493c4c7d8f1cd2f3ae30c28d`, iteration 18, clean/consistent.
- Retained native/CASA ratio: 1.1790668889333646; target: 0.90.
  Initial ratio: 1.7628787405420967. The target and full-input acceptance remain open.
- Last retained implementation: `bb47e918348b7dd6ddcc070b7ca0a7d1f3cca3b9`
  (trial 18, contiguous row-slice tile commit). C/P 0.9770404627, paired 95%
  interval [0.9590116944, 0.9954081597]; native/CASA medians 8.143475042 /
  6.924885834 s. All seven products versus both references, metadata and focused
  guards passed. Trial 15 private spill v2 and trial 17 single release codegen
  unit were also retained. Trials 13/14/16 were visibly reverted.
- Quota checkpoint: at **15% weekly remaining or below**, start no experiments,
  reach a safe recorded boundary, preserve source/evidence remotely, and pause
  for explicit resumption. Most recent reading when preparing this snapshot:
  15% remaining (85% used); 10080-minute window; reset Unix 1789435318. Account-wide usage
  is not task billing. No new unattended overnight checkpoint is approved.
- Current optional design consultation, not final contract review:
  [GPT-6 Pro, Power 5](https://chatgpt.com/c/6aa1e814-97cc-83e8-b963-e8ee5b9c7432).
  It completed and supported the trial-15 private spill v2 experiment, subject
  to original-seal integrity and adversarial tests. Its preserved summary and
  local verification limits are in `experiment/oracle-spill-v2-review.md`.

No trial 19 was started or selected. This is an execution pause, not goal
completion. The immutable foreground controller still validates `active` because
the target is unmet. Available Goal tools cannot set `paused`; app control was
denied and must not be bypassed. Brian must pause the Goal in the app if needed.
Neither automatic Goal continuation nor quota reset authorizes new experiments.

`snapshot-18.tar.gz` contains the unchanged controller run/events/logs (including
the earlier archived five-trial campaign), fixed experiment configuration,
measurement/comparison/resource receipts and test logs, profiles, the retained
native executable, the original native-reference executable and reproduction
receipt, exact controller implementation, and current work record. Binaries
are macOS arm64 project test executables, not portable to other platforms.
The older snapshot 14 remains as historical evidence and is not the restart tip.
See `SHA256SUMS` for the compressed archive and `MANIFEST.sha256` inside it for
each file. No credentials or account identifier are included.

No MeasurementSet payloads, generated image arrays, or cached CASA installation
are redistributed. The 13,630-byte `reference-reproduction/input-owner-table.dat`
is table-description metadata plus the generated casa-rs owner manifest; it
contains no visibility payload. It is the only non-lock file that differs from
the upstream regression fixture. It is required because owner initialization
mints random scientific identities; rerunning initialization produces a different
frozen input hash. Those scientific consistency identifiers are not credentials.

## Preserve the acceptance and rejected experiments

The numeric target uses five fresh alternating parent/candidate/CASA task pairs,
one worker, fixed 256x256/16-channel/168480-row Natural/Clark input, three major
cycles and 19 actual minor iterations. A keep requires improvement in the fixed
CASA ratio and C/P 95% log-ratio upper bound below 1, then seven-product nRMS
<=0.001 against both CASA and the original native reference, scientific metadata,
WCS/topology/beam guards, and affected focused tests/interoperability. Limits:
4 GiB native, 8 GiB sampled process group, 600-second experimental pipelines.
The controller wrapper allows 700 seconds around the internal 600-second guard.

All original T55/T57 scope, full-input correctness/numerical/visual/timing and
bounded-memory acceptance, and the final independent contract review remain.
No serial parity claim, full-input speedup, merge, or approved deferral exists.
The unchanged parent reproduces 88 `compile_plan_run` failures / 91 passes;
these are recorded, not passing evidence. The trial-12 library and focused plan
tests and all retained scientific guards passed as detailed in the work record.
The ignored medium-VLA discriminators now record dataset identities, block
packing, residency and proof-byte totals as JSON evidence and assert only
structural invariants and stable selection-derived totals; all three pass on the
mounted dataset. The former 7-versus-6 captured-block drift was traced to
`42a1a4b53a` per-row multi-domain projection charging, not the selected-row
metadata size.

Do not restart retired hypotheses: pending-take-only resampling; identical-pol
prediction range or atom reuse; selected-metadata borrowing (including the
mistaken repeat in current trials 8/9); indexed/on-demand spectral axes; direct
MS getter; canonical hash batching; shaped-Zip tile commit; flat-row degridding;
compiler-to-writer payload-checksum reuse; sealed-product bulk-read candidate
(byte-wise tiled bool unpack, contiguous standard-layout window copies, bulk
f32/validity encoding); tile-accumulator resize-then-fill clear. The
sealed-product candidate consumed a user-authorized retry-rule waiver and a final
authorized warm-build rerun; those allowances are spent. Trial 18 used the one
evidence-producing retry of tile indexing with a different row-slice
implementation and was retained; that allowance is consumed, not reset by the
keep.
The exact-axis reservation's one evidence-producing retry was retained in
trial 3. Routine build/fixture repairs did not renew scientific retry budgets.
Use the complete controller histories and `current-work-record.md`, not this
short list alone. No failed hypothesis may be renamed to reset its allowance.

## Restore after explicit resumption

1. Read the latest PR summary and any newer snapshot on this evidence branch.
   Refresh weekly usage. Respect the explicit pause until Brian resumes the work;
   neither a quota reset nor restoring files is permission to restart experiments.
2. Fetch the source branch and check out the exact SHA in the original repository
   path below, on `codex/t55-serial-autoresearch`. Preserve any existing dirty work;
   do not overwrite another checkout or active run. Verify the archive checksum,
   inspect its relative path inventory, extract into a new temporary directory,
   then verify `MANIFEST.sha256` from the extracted snapshot root.
3. For an exact controller continuation, restore `controller/` as
   `autoresearch-results/` in the source checkout, and `experiment/` at the exact
   experiment-root path below. Restore the captured controller implementation
   only if the original compatible installation is unavailable; do not overwrite
   an unrelated skill installation. The configuration and event stream are
   immutable: do not edit paths, metrics, event history, frozen controls or HEAD
   to make a mismatched restoration pass.
4. Obtain the regression dataset from its upstream repository, not this snapshot:
   `https://open-bitbucket.nrao.edu/scm/CASA/casatestdata.git`, inspected revision
   `f661170943a0c6eafd6cd0b51ed09ddcb7b36f78`, path
   `measurementset/vla/refim_point_withline.ms`. Acquire its actual Git LFS objects.
   The uninitialized local upstream tree was verified as SHA-256
   `b470f8ef785bdbcaa9432f27e907d640f09f911032ced77c75d10f49aba80b6e`,
   63 files / 83,431,604 bytes, excluding `table.lock`.
   Stage **new copies** at both configured native and CASA input paths, excluding
   `table.lock`. Replace only each new copy's root `table.dat` with the captured
   owner metadata. Do not modify upstream data. This reconstruction was tested:
   it exactly restores the frozen tree SHA-256
   `d62a5398f825f6a96510bd64516ab0ad731ce73a75201230d995b4df965be8f0`,
   63 files / 83,434,343 bytes. Verify that identity using the captured source's
   `perf_harness.tree_identity`; a mismatch is a real blocker, not permission to
   overwrite the frozen hash.
5. If the preserved native reference arrays are absent, regenerate them from
   `reference-reproduction/candidate-application` (SHA-256
   `acd3f2b76734fe657f67a964317c7b002b6ecd0c7d9406f67a78ac6fc3ff8cd3`)
   using `image_case` in the committed `t55_serial_autoresearch.py`, the restored
   config, and the configured reference run directory below. Use the existing
   600-second / 8-GiB process guard. This executes one reference case, not the
   six-case historical `measure.py` campaign. Require three major cycles,
   19 actual minor iterations, one worker, 4-GiB native request and accepted
   publication. The original binary, request, results and exact parent comparison
   are preserved; do not substitute the latest candidate as its own reference.
6. Confirm CASA 6.7.6.14 and the configured interpreter, compiler/runtime
   prerequisites and disk headroom. Rust/cargo versions are in the snapshot.
   Verify both preserved executable hashes, all frozen-control hashes, input
   identities, source HEAD and clean Git state. Run the controller's `status`
   command below; it must report matching source, branch and consistent history.
7. After Brian explicitly resumes work, resume the existing foreground Goal
   through its app controls, then inspect the completed Pro review, current
   evidence and rejected-candidate history before selecting a new candidate.
   Never call `init` over restored state or `resume` on an already-active
   foreground event stream. Check quota before the next trial. Only `finish`
   may commit/revert an experiment in the active repository.

If the same absolute paths/platform cannot be restored, preserve this snapshot
as immutable history and explicitly establish a new approved tranche with fresh
baseline/configuration/guards. Do not forge continuity or reset rejected
hypotheses, acceptance requirements, resource caps or pause conditions.

```text
Repository: /Users/brianglendenning/.codex/worktrees/5d43/casa-rs
Experiment root: /private/tmp/casa-t55-serial-ratio-t2.UrhEd2
Native MS: /private/tmp/casa-t55-real-density-accounted.p5Medq/input/refim_point_withline.ms
CASA MS: /private/tmp/casa-t55-serial-ratio-t2.UrhEd2/refim_point_withline.ms
Reference run directory: /private/tmp/casa-t55-serial.d5AeGJ/measured/candidate-1
Reference product prefix: /private/tmp/casa-t55-serial.d5AeGJ/measured/candidate-1/natural-w1/image
Controller scripts: /Users/brianglendenning/.agents/skills/codex-autoresearch/scripts
```

```sh
python3 /Users/brianglendenning/.agents/skills/codex-autoresearch/scripts/autoresearch.py status --repo /Users/brianglendenning/.codex/worktrees/5d43/casa-rs
CASA_RS_T55_AUTORESEARCH_ROOT=/private/tmp/casa-t55-serial-ratio-t2.UrhEd2 PYTHONDONTWRITEBYTECODE=1 python3 tools/perf/imager/t55_serial_autoresearch.py verify
# The guard is the same command with final argument "guard".
# Do not run verify manually into an existing trial directory; the controller owns trials.
```
