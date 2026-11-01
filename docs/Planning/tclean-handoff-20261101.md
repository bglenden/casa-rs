# tclean handoff — 2026-11-01

**Status: work in progress. `tclean` is not finished or equivalent to CASA yet.**
Work issues: #980, #54, #944, #823 and #991. The owner wants `tclean`
completed and verified first; other existing conversions follow. Do not expand
simulation tasks except for a dependency needed to finish an existing conversion.

## Resume here

Use the main checkout `/Users/brianglendenning/SoftwareProjects/casa-rs` and
branch `codex/tclean-equivalence`. Both this branch and the separate unfinished
`codex/python-table-create` checkpoint are pushed to `origin`. The table branch
is at `68187b052ea4a8079f1497ed529acc7dff93b4f9`; its own
[`python-table-checkpoint-20261031.md`](python-table-checkpoint-20261031.md)
records 184 passes, 5 skips, two reproduced failures and remaining work. Table
completion is not a prerequisite for resuming `tclean`.

The full in-progress state was first preserved and pushed in commit
`50d565e372ef8af6db65d21b564ef499ab9bf4aa`. A following cleanup removes an
already-rejected raw selector-provenance experiment from the active tree while
preserving it in that commit. Do not treat that experiment or its fixture
bypasses as the accepted design. The active tree keeps the bounded FIELD and
VLA primary-beam fixes described below; neither is a claim of whole-task parity.

Use CASA **casatasks 6.7.6.14 / casatools 6.7.6-14** as the oracle. Rust remains
the runtime; CASA is an oracle only. Follow `AGENTS.md`, `TESTING.md` and the
imaging skill. The main checkout's `target` points to GLENDENNING; use
`CARGO_INCREMENTAL=0`. Large run data and durable evidence belong on the NAS;
the external disk is the fast working copy.

## Immediate unfinished work

1. **VLA cube PB.** The pre-fix T1.5 `point-cube-briggs` and
   `withline-cube-wproject-hogbom` PB products differ from CASA by NRMS
   `0.0135702511`. The original c64 binary and corrected-residual binary
   produce bit-identical PBs, so this predates the residual fix. CASA chooses
   the VLA aperture at spectral pixel zero and scales that aperture using each
   channel's frequency. Native code previously chose a new aperture per channel.
   The retained candidate uses channel zero for model choice; its regression
   fails on the parent and passes on the candidate, including both spectral
   directions, and touched clippy passes. It still needs a fresh paired CASA
   run of all affected T1.5 rows, full applicable gates, and independent review.
   The existing kernel harness originally emitted an obsolete JSON envelope;
   the NAS archive includes its corrected copy. The saved CASA requests specify
   `datacolumn=data`; one old kernel run did not, and its MS has
   `CORRECTED_DATA`. Use the request-aligned recipe in the archive for new
   comparisons and retain that old result only as a diagnostic.
2. **FIELD selection.** Commit `7b95dde55e` bounded numeric ranges, but the
   subsequent full gate found four regressions: three incomplete fixtures and a
   real flagdata no-match behavior regression. The current repair distinguishes
   all fields from an explicit empty match in the generic compiled selection,
   preserves imaging's no-match refusal, and completes the fixtures. Focused
   selection tests (83), integration tests (13, one helper ignored), and
   touched clippy pass. The last full workspace run, before this repair, was
   **4,732 passed, 4 failed, 45 skipped**; it has not been rerun after the
   repair. The repair still needs full relevant gates and independent review.
3. **Scan/observation activation.** Provider 32 still refuses nondefault values.
   The compact native selection engine has scoped CASA evidence, but the public
   provider 32→33 candidate is **proposed, not applied**. The owner has not
   answered the pending activation question. `AGENTS.md` requires asking before
   provider-contract bundle changes. See
   [`tclean-numeric-selection-contract-proposal-20261101.md`](tclean-numeric-selection-contract-proposal-20261101.md)
   and the [unapplied candidate patch](tclean-provider33-candidate-20261101.patch.gz).
   Its complete context is also in the preserved archive below. Candidate SHA256:
   `fa7f7af6aee69c6b5928f3fd044b847b523d57806787dbee351ffa31dddaf084`.
   It was prepared against the raw experiment preserved in commit `50d565e372`;
   it no longer applies directly to the cleaned active tree. Rebase/review its
   intent after any necessary approval, rather than promoting the rejected raw
   restoration path. Then rebuild the Python extension and native task together,
   exercise kwargs, profiles, Mapping, save/reload and refusals, and rerun the
   final-source CASA matrix.
4. **Other gaps.** Restart/outlier and orphan cases, masks and auto-masking,
   selection grammar and multi-MS, additional gridders/modes, metadata, return
   values, persistence, representative acceptance and interoperability remain
   incomplete. A source-level edge around CASA zeroing planes with very small
   positive `sumwt` needs a paired probe; it is not an approved deviation. The
   extra native `.summary.json` sidecar proposal is also awaiting an owner
   decision. Update the structured compatibility reference and work records as
   each behavior is verified.

The original dense-cell f32 accumulation defect has an owner-approved native
correction. Representative and runtime-prediction acceptance are still open.
A later single-term residual-unit fix is committed at
`2851c6dcdcac828d2129ef66ef04dffc73273a6c`. Its red/green regression and
three scoped CPU64/CPU32/Metal32 CASA probes pass the original limits; the two
older f32 cycle-threshold failures remain in their raw receipts. This does not
create a general numerical waiver. Preserve the owner's scoped weak Taylor
weight rule and exact-output approvals in
[`tclean-owner-decisions-20261101.json`](../../resources/casa-compat/evidence/tclean-owner-decisions-20261101.json).
Headless interactive operation is an accepted temporary exception; explicit
uvtaper units, matching Stokes with a warning, correct absolute numbered paths,
`write_pb=True`, and preservation of previous numbered-image histories were
separately agreed. New-image history remains required.

## Evidence and verification

- The final pre-repair full gate and its four failures are retained in the NAS
  handoff archive. Focused FIELD and PB tests are recorded in that archive.
  The cleaned state passes `just arch-check`, all 64 imaging-product tests,
  and all 64 imager T1 tests. Their raw logs and SHA256 manifest are in the
  sibling `tclean-handoff-checks-20261101` directory on the NAS.
- `/Volumes/home/casa-rs/tclean/20261031/tclean-handoff-20261101.tar` contains
  the normalization experiment, red/green logs, reviewed T1.5 CASA/native
  products, request recipes, three input MeasurementSets, PB diagnostics,
  and focused FIELD logs. It was member-verified and reread on the NAS;
  SHA256 `6721d4c96b7ed31876dc4f1b5bbc4937a9c02babb8635f8f89cac5a3530d6416`.
- `field-numeric-bounds-20261101.tar` SHA256
  `17354bfd7a7a0884e2d7ab83fed82d55d43f08213e4fedb0a160ed00bb385459` and
  `scan-observation-engine-20261101.tar` SHA256
  `7b35ca58b0ebee3cc6d8fa2f503a45a617084cec9d76a95103484d175d14accf`
  are in the same NAS directory. The latter preserves all 39 original engine
  product comparisons and their two original scalar failures.
- `scan-observation-contract-proposal-20261101.tar` SHA256
  `1a581aea060369e0d45cce4d1e8cfac16d12906b991f4e69703c1e5351a78fc6`
  contains the unapproved candidate, warning probes and source context.
- The version-verified working products and logs also remain under
  `/Volumes/GLENDENNING/casa-rs-evidence/tclean-20261031/` and
  `/Volumes/GLENDENNING/casa-rs-evidence/if/IF-8/` for fast reuse. Do not
  count a test name, skipped run or raw failed comparison as passing evidence.

The [Oracle review](https://chatgpt.com/c/6ae7afa9-d6bc-83e8-829d-a65bfdeb9a01)
used the local `~/.agents/skills/oracle/SKILL.md` skill and displayed GPT-6 Pro; Astra identity was not exposed. The handoff agent was
GPT-6.1 Terra; effort was not exposed. Separate-agent review model identities
were not exposed. Treat review as evidence, not as owner approval.

The attached `f027` worktree is this chat's active workspace; it cannot safely
be removed while the chat is attached. The main checkout also had pre-existing
untracked `apps/casars-mac/.gui-test` data and `casa`, `data`, `ms` symlinks;
these are unrelated to this work and were preserved. Recheck attachments and
processes before later worktree cleanup.
