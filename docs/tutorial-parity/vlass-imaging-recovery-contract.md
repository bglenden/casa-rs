# VLASS Imaging Merge-Recovery Contract

Truth class: approved execution contract

Approved: 2026-08-02 by Brian Glendenning

Verification: `just docs-check` plus the focused recovery-contract and imaging
harness tests

This contract turns the existing VLASS imaging wave into a finite,
merge-oriented recovery. It does not reduce the scientific scope, the
full-geometry rows, the 19-product clean contract, the 32 GiB laptop
acceptance host, or the independent 10x performance requirement.

The machine-readable controlling record is
`tools/perf/imager/vlass_recovery_contract.json`. Launches are recorded in
`tools/perf/imager/vlass_recovery_launch_ledger.json`; the bounded audit of
already-created work is recorded in
`tools/perf/imager/vlass_recovery_salvage_catalog.json` and summarized in
`vlass-imaging-recovery-salvage-audit.md`. The longer historical
optimization plan remains available at archive commit `4c3cf8cc9`; current
`main` intentionally removed that 10,000-line experimental ledger, and this
PR does not resurrect it. Where the archived plan's older scheduling language
conflicts with this document, this recovery contract controls scheduling,
launch budgets, and closeout.

## Recovery outcome

The wave terminates in exactly one of two states:

1. A reviewable four-PR train whose selected production code passes the
   applicable repository gates, full CASA science comparisons, bounded-memory
   laptop execution, and the independent 10x rows.
2. A documented blocker after the bounded attempts are exhausted, with the
   exact failed gate, retained evidence, and one specific decision requested
   from Brian.

“Continue optimizing” is not a terminal state. A likely 100x deconvolution
speedup remains interesting but is not a fixed acceptance target. Once the
scientific floor and 10x requirement make the train merge-ready, stop and ask
Brian whether to merge or spend a separately agreed budget pursuing more
performance.

## Preservation boundary

The unique dirty worktree was preserved before cleanup or formatting:

| Item | Identity |
| --- | --- |
| Trusted pre-experiment checkpoint | `aea444b5e40fde5486e2ea421e5f8e2cf32d6174` |
| Archive branch | `codex/vlass-recovery-archive-20260802` |
| Archive commit | `4c3cf8cc916656f1ece6efc9bbd3fb674e401132` |
| Archive tree | `d46765f2795d3b4c31edef891f523ddae2433b7b` |
| Verified Git bundle SHA-256 | `4ff641f50a5d0248288ba480255765a8870b7bbab6ef9842b3cacc14e75dc519` |
| Binary patch SHA-256 | `55286ec6390dde0552df38abe80e1de2ea84891906d6549675ab0133a57b1aad` |

The bundle and patch are under
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/recovery-archive/2026-08-02/`.
The remote archive SHA matches the local commit. The archive commit is
deliberately non-promotable: its normal commit hook found a missing SPDX
header in one experimental file, and the exact evidence was committed with
hooks bypassed rather than edited before preservation.

Draft PR #451 remains an evidence and handoff record. It is not merged,
undrafted, force-pushed, or treated as the final review unit. Audited pieces
move into the recovery PR train; superseded experiments remain reachable from
the archive.

## CASA reference rows

The old `8,183.264 s` number is the connected-63-field **dirty** CASA time. It
is not a matched clean baseline and must never be used to claim a clean
speedup.

Two clean references are now required:

| ID | Selection and cap | Manifest | Wall ceiling |
| --- | --- | --- | ---: |
| `CASA-B-FRAGMENT63-CLEAN-CAP20000-v1` | All 63 fields, SPWs 2–17, `niter=20000` | `vlass-fragment-all-fields-clean-cap20000-casa.json` | 7 days |
| `CASA-A-SINGLE-CLEAN-N2000-v1` | Field 1525, SPWs 2–17, `niter=2000` | `vlass-fragment-single-field-clean-casa.json` | 72 hours |

The 63-field CAP20000 row changes only the scientific `niter` ceiling from
the checksum-bound `niter=2000` all-fields manifest. Geometry, data selection,
mask and mask hash, `nsigma`, gain, major/minor-cycle controls, AWProject,
POINTING, W planes, A/WB/conjugate beams, Briggs weighting, MT-MFS, scales,
restoration, and all 19 products remain unchanged. Identity, description, run
label, and evidence-role strings distinguish the new reference.

`niter=20000` is a ceiling, not a claim that CASA will select exactly 20,000
components. Each CASA and casa-rs receipt records the configured cap, actual
iterations/components, cycle boundaries, and stop reason.

The reference launch order is B then A. There is one normal launch for each
row and one shared retry only for an external invalidation such as power,
mount, storage, or host failure. CASA parameter changes, scientific failure,
timeout, or an avoidable harness defect do not silently earn more retries.
No unchanged dirty or reduced CASA reference is rerun.

### CASA-B v1 negative evidence

`CASA-B-FRAGMENT63-CLEAN-CAP20000-v1` completed its CASA imaging call on
2026-08-02 in `6,902.788125 s`, but it is not a valid clean reference. The
configured mask covered pixels `[6243, 6003]` through `[6306, 6066]`, while
the measured `0.431877822 Jy` residual peak was at `[4633, 6183]`. The
largest residual inside the mask was only `0.000659208 Jy`, below CASA's
`0.00169344 Jy` `nsigma` threshold. CASA therefore selected zero minor
iterations, produced an empty model, and performed only the dirty and
restoration work. The elapsed time must not be reported as an all-fields
clean baseline.

The v1 receipt is
`b62daf1de4549d4f5ca186c1b5c02ce501089d97ba72cfc61cefe9fb35084bb5`;
its CASA log is
`535248c9c1910d94666df59a5d45dd5b66e25b759d1587acfa862e1f064671c0`.
The comparator correctly rejected the empty model, but the harness then
decorated the operational failure with a numerical tolerance result and
obscured it as a schema error. The regression is covered by the focused
comparison-protocol test.

The launch ledger records a proposed v2 amendment using the already existing
source mask at pixels `[4602, 6152]` through `[4665, 6215]`, whose stable tree
SHA-256 is
`fabf361e6609a4d66c251458c2ed31bc80978d936e78a39a8f449bd1a63dc322`.
The proposed base and CAP20000 manifest SHA-256 values are respectively
`0cabbe5fdc2f687a10fce3653d018e09db7349a0a1299d16a4f6772557f9f5d9`
and
`63948fe140d5c06c00b924eea407e5afe8ccb2f99e2c927290d9de4644002053`.
The original v1 manifests remain unchanged. Because this was an avoidable
configuration defect rather than an external invalidation, the corrected v2
launch remains blocked on Brian's explicit approval; CASA-A has not been
launched.

## Candidate budget and promotion

The salvage audit receives at most eight engineer-hours. It may select one
primary candidate and one reserve from code and evidence that already existed
at archive commit `4c3cf8cc9`. It does not invent another architecture.
Retired families stay retired. Positive diagnostic-only work is not promoted
without an already executable, bounded, scientifically testable route.

At most two immutable casa-rs candidate freezes may enter the promotion
ladder. Their full-size budget is four planned rows—A and B for each
freeze—plus one shared external-invalidation retry. A candidate reaches full
geometry only after:

1. The real 4096-square, four-SPW clean row passes.
2. The real 4096-square, full-16-SPW clean row passes.
3. Its memory plan and stage projection are credible for the 32 GiB laptop.

Reduced rows are development gates, never final evidence. Smaller diagnostics
are permitted only for an isolated semantic regression in the selected
candidate; they cannot reopen an architecture tournament.

## Correctness and performance

CASA is the scientific reference, not an implementation transcript. Bitwise
identity, identical component order, and identical major-cycle history remain
diagnostics rather than gates. Promotion still requires:

- the exact data, field, SPW, POINTING, AWProject, weighting, MT-MFS, mask,
  scale, and restoration semantics;
- the exact 19-product inventory with correct coordinates and metadata;
- all frozen numerical, topology, beam-aware, source, morphology, flux,
  residual/noise, dynamic-range, and stable-domain alpha gates;
- stable convergence without divergence or conspicuous coherent artifacts;
- an actual-iteration, component-count, cycle, and stopping receipt; and
- bounded memory, compression, swap, I/O, progress, and end-to-end stage
  evidence on the 32 GiB laptop.

Each final row independently requires at least 10x against its corresponding
matched CASA reference. No speedup is inferred across dirty versus clean,
single-field versus 63-field, reduced versus full geometry, or different
product sets.

## Finite delivery train

The merge train has four review boundaries:

1. Contract, evidence harness, CAP20000 manifest, launch ledger, and CASA
   references.
2. Audited shared execution substrate: multi-SPW, POINTING, AWProject,
   MT-MFS, product parity, resource planner/telemetry, and canonical UI/task
   controls only.
3. One selected performance candidate, with the reserve used only after a
   declared primary failure.
4. Final acceptance receipts, bounded refactor, issue #454 experimental
   cleanup, sidecar reviews, and closeout.

The recovery budget is eight engineer-hours for salvage and 48 engineer-hours
total inside a 72-active-hour window. Valid long-running CASA reference wall
time is excluded from those active-work clocks. CI is run at coherent PR
boundaries and once for final verification, not after every scientific
measurement. Existing green evidence is reused whenever intervening changes
cannot affect it.

No PR is merged automatically. When the train is merge-ready—or when the
bounded attempts end in a blocker—the agent presents the evidence and waits
for Brian’s decision.
