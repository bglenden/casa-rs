# Issue 581 batched-dispatch candidate: rejected

Date: 2026-08-29

This records the terminal evidence for the batched bounded-replay dispatch
candidate described in issue #581. The candidate commits are preserved in Git
history, but the implementation was reverted after its compute-bearing
discriminator failed. No full all-channel run was performed.

## Candidate

- Parent: `909a924733185c24cfed75d295a9bcf5a388ac63`
- Batched dispatch: `813bdf147`
- Admitted replay window: `c091fa8a2`
- Cross-lane fixture: `8c06cb583`
- Dataset: directly mounted 32 GiB-class VLA MeasurementSet,
  `wave1-vla-single-medium.ms`
- Frozen CASA oracle: reused; CASA was not rerun.

The candidate replaced one Rayon dispatch per four-partition wave with one
dispatch per source block, retained the single execution path for one or many
workers, admitted its bounded memory window, and preserved deterministic
ordinal commit.

## Structural evidence

The focused bounded-stream suite, complete-data MFS integration tests, strict
Clippy, formatting, and architecture checks passed. A real six-partition,
three-record-group fixture split the middle record group across two worker
lanes and produced bit-exact serial and parallel products and execution/commit
identities.

The one-channel scheduler probe reduced parallel replay wall time from the
previous fixed-pool result of 0.726072 s to 0.273980 s (62.3%), with 2,000
dispatch batches for 2,000 source blocks instead of 32,000 dispatch waves.
That passed the scheduler-local discriminator but did not establish useful
compute scaling.

## Compute-bearing discriminator

Both runs selected 16 channels and processed 31,985 source blocks and
1,978,026 logical partitions. The serial and four-worker products were sampled
with the canonical image comparator; `.image`, `.residual`, `.psf`, and
`.model` all had zero RMS and zero peak difference.

| Measurement | Serial | Four workers | Change |
| --- | ---: | ---: | ---: |
| Full Rust wall | 90.614444 s | 95.666174 s | 5.57% slower |
| Gridded replay wall | 4.086159 s | 5.339140 s | 30.66% slower |
| Source fill | 1.463156 s | 1.758373 s | 20.18% slower |
| Prepare | 0.720712 s | 0.843343 s | 17.01% slower |
| Execute | 0.872256 s | 1.307322 s | 49.88% slower |
| Deterministic commit | 2.204062 s | 2.871370 s | 30.28% slower |
| Dispatch batches | 31,985 | 31,985 | equal |
| Started worker threads | 0 | 4 | as planned |
| Peak partial capacity | 32,768 B | 32,768 B | equal |
| Peak kernel window | 51,280 B | 8,440,128 B | within plan |

Execution and commit identity digests matched exactly. The four worker lanes
were balanced (495,294 / 494,244 / 494,244 / 494,244 partitions), so imbalance
does not explain the failure. Every measured replay substage regressed, and
worker wait time dominated active time.

## Decision

The pre-recorded falsifier required the compute-bearing serial/multi-worker
discriminator to improve, and provisional retention required at least 5% of
the frozen 486.349 s full-run wall (24.317 s). The candidate instead regressed
both replay and total wall time. It is therefore rejected and reverted. The
full all-channel gate would add cost without changing that decision and was
not run.

The result rules out dispatch frequency as the next dominant optimization
target for this replay shape. Any next candidate must begin from the restored
parent and explain the measured execute/commit cost rather than stacking
another change on this failed mechanism.
