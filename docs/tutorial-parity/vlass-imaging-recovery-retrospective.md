# VLASS Imaging Recovery Retrospective

Truth class: historical closeout

Closeout date: 2026-08-13

Verification: `just docs-check`

This retrospective records process lessons from the VLASS AWProject and
MT-MFS recovery campaign. It is not a current execution contract, a new
performance target, or an authorization to rerun the full-resolution
workload. The approved recovery rules remain in
`vlass-imaging-recovery-contract.md`; the source and evidence selection is in
`vlass-imaging-recovery-salvage-audit.md`.

## Intended outcome

The campaign intended to recover the scientifically valid VLASS work, produce
a coherent reviewable implementation, demonstrate full 12150-square,
16-SPW, all-63-field CLEAN correctness on the 32 GiB laptop, and land a
substantial performance improvement over matched CASA.

## Actual outcome

The campaign achieved a scientifically accepted full-resolution result and
landed important correctness and bounded-residency work. It did not establish
the intended substantial end-to-end VLASS CLEAN speedup. The accepted
full-resolution result used role-segmented Metal initial gridding and CPU
residual refresh. All 19 products passed the frozen CASA-B-v2 scientific
contract, with worst normalized RMS `2.2086646e-4` against the unchanged
`1e-3` limit.

The result did not satisfy the original no-new-swap guard. Global Swapouts
increased by 344,504 16,384-byte pages, or 5,644,353,536 bytes. Brian
explicitly accepted that exception. The required status is therefore:

> 12150² science pass; no-new-swap guard waived by user.

The outcome was scientifically valuable, but the engineering campaign took
too long, generated too many full-scale attempts, and accumulated excessive
experimental and cleanup work before reaching closeout.

## What worked

- The all-product scientific contract prevented performance work from hiding
  correctness regressions. Product inventory, normalized RMS, topology, flux,
  peak, centroid, beam, nonfinite, sample-count, and actual Metal-execution
  checks made the accepted result credible.
- Instrumenting both CASA and casa-rs eventually localized real semantic
  boundaries instead of relying on visual similarity or blind parameter
  changes.
- The POINTING epoch correction fixed a genuine source-block invariance
  problem and became durable production behavior with regression coverage.
- A known-correct CPU residual backend provided the decisive subsystem A/B.
  It reproduced CASA's 4096-square minor-cycle trajectory and produced a
  scientifically accepted result after compressed and Metal-residual
  candidates failed.
- Role-segmented Metal initial gridding, cold-plane staging, streamed product
  formation, reusable CPU residual grids, and early lifetime releases made the
  full geometry executable within the machine's physical-memory envelope.
- Failed representation candidates were ultimately removed rather than
  becoming silent fallbacks or competing production modes.

## What made the campaign ineffective

### Full-resolution execution became the diagnostic loop

Successive 12150-square runs discovered different resource failures during
initial gridding, residual setup, model FFT, later major cycles, and finish.
Some failures arrived only after hours. Those runs were useful evidence, but
they were an extremely expensive way to discover stage-local ownership bugs.

Full-resolution execution should have been reserved for final evidence. After
the first failure at a stage, the next work should have been a bounded
full-geometry stage probe that reproduced the same ownership and allocator
shape without completing CLEAN.

### Correctness, feasibility, and optimization stayed coupled

Once Metal initial gridding plus CPU residual refresh passed the 4096-square
scientific contract, it was a viable slow-correct baseline. That baseline
should have been frozen and landed before further Metal residual,
representation, and memory optimization. Keeping every concern in one wave
made cleanup, verification, and branch recovery harder and risked losing the
scientifically correct path.

### Several discriminators did not predict the terminal result

Compressed-kernel changes materially improved local numerical error without
materially improving the final CLEAN products or trajectory. Minor-cycle
length changes also failed to improve science. Work continued too long after
the terminal metrics had falsified those hypotheses.

For a representation or precision hypothesis, a material improvement in the
local metric must move the first divergent operator, trajectory, or product
metric. If it does not, close the hypothesis rather than adding another rung
to a precision ladder.

### Fixture identity and path execution were not fail-closed enough

Some focused probes initially used mismatched FFT provenance, unreachable
source-block ordinals, changed row blocking, or incomplete Metal role
dispatch. These were corrected, but each could produce persuasive output from
the wrong experiment.

Every discriminator should emit and verify dataset selection, CASA/reference
identity, convolution-function cache identity, source ordering and blocking,
candidate revision and binary, backend selection, and executed device
partitions before doing substantive work.

### Static memory accounting was treated as stronger evidence than it was

The planner captured logical ownership but initially missed allocator
retention, temporary compiler/group ownership, and phase-specific runtime
overlap. A passing static ledger is necessary but not sufficient for a
full-resolution launch. The campaign needed measured probes at initial-grid,
residual-refresh, model-transform, and finish boundaries with conservative
headroom.

### Failed experiments accumulated in the production diff

Late cleanup found alternate compressed representations, environment-driven
selectors, temporary sidecars and comparators, shadow paths, dump code, stale
tests, and a large dead helper island. Deferring cleanup made every subsequent
change and verification more difficult.

One hypothesis should be active at a time. Rejected diagnostic code should be
reverted, moved to maintained acceptance tooling, or preserved as an immutable
historical commit before the next candidate begins.

### Authorization and reporting did not support efficient steering

The campaign sometimes asked again for permission already granted within the
approved experimental envelope. Conversely, long-running subwork could move
from one candidate to another without a concise decision checkpoint. Frequent
block counters documented activity but did not always expose the decision,
budget, or falsifier.

A campaign should record its authority envelope once. Updates should emphasize
stage transitions, hypothesis status, elapsed budget, resource headroom, and
the next decision. A new architecture candidate or long-run launch remains a
distinct cost boundary even when bounded implementation work is pre-approved.

## Decisions we would make differently

1. Freeze and land the first slow-correct 4096-square implementation before
   beginning a separate performance wave.
2. Use a mandatory ladder: fixture identity and execution receipt; one
   operator or source segment; cumulative checkpoint; niter-zero products;
   one residual refresh; offline minor-cycle trajectory; medium CLEAN; final
   full-resolution evidence. A red stage vetoes later stages.
3. Permit only one active hypothesis. Record its causal prediction, terminal
   metric, automatic falsifier, maximum runtime, resource ceiling, fallback,
   and cleanup disposition before implementation.
4. Require a same-revision, mode-faithful turnaround receipt before any run
   projected over 30 minutes. Runs projected over 90 minutes are final
   evidence only.
5. After a long-run failure, reproduce the failing stage with a bounded probe
   before another long run. After two full-resolution failures, stop for an
   explicit continuation or waiver decision.
6. Validate full-resolution resource phases independently and require measured
   headroom in addition to the static planner ledger.
7. Generate standard visual review panels early: two random and two
   bright-source 400-by-400 CASA, casa-rs, and difference panels using shared
   color scales.
8. Preserve accepted results and the first unresolved failure in full. Retain
   compact receipts and logs for rejected or diagnosed runs, then remove their
   rebuildable products, spill files, and workspaces.

## Durable policy changes

The reusable campaign controls from this retrospective belong in two active
documents:

- `.agents/skills/casa-rs-imaging-performance/SKILL.md` owns imaging campaign
  sequencing, candidate discipline, and scientific/performance workflow.
- `TESTING.md` owns normative identity, launch, terminal-receipt, and artifact
  requirements for long performance runs.

`AGENTS.md` remains intentionally short. This historical account does not
replace issue and PR closeout evidence, and it does not add a new source of
scientific semantics.
