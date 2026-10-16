# T51 Paired-AW VLASS Acceptance Gate

Truth class: historical acceptance record; its runner no longer exists

## Approved representative closeout

On 2026-09-07, Brian explicitly approved closing T51 with representative
correctness while deferring full-dataset correctness/scalability and performance
to [#625](https://github.com/bglenden/casa-rs/issues/625). The
[canonical T51 record](https://github.com/bglenden/casa-rs/issues/537#issuecomment-5553500652)
contains the approval and current results. This is a selection exception, not a
numerical, product, persistence, or memory waiver. It does not close the broader
requirements of #448, #445, or #52.

`perf_harness.t51_representative_acceptance` uses the production application
through the test-only `t51_aw_subset_full_products` seam. The five baselines
`5&22;4&5;13&14;12&13;12&23` retain all 63 fields, 16 SPWs, 64 channels per SPW,
four correlations, 4096-square images, and AW/POINTING geometry: 10,080 rows and
2,580,480 correlation-channel samples. DIRTY populates the cold private store;
actual deterministic CLEAN reuses it unchanged with the frozen mask, original
2000-iteration budget, and original stopping criteria. Both roles retain the
full 18/19-product comparison contracts below.

The catalog-scoped preparation route records two preparation receipts when cold
(one expected rejected reuse probe plus one successful ordered import), or one
when fully warm. The 1,024 cells remain individually identified and checked
inside those phase receipts; receipt count is not artifact count. The
[mechanism map](t51-aw-operator-mechanism-map.md#catalog-scoped-preparation)
defines partial failure, cooperative cancellation and restart behavior.

The earlier six-product, Rust-start-model residual-refresh diagnostic is not
this gate's reference. The representative gate produces independent CASA DIRTY
and CLEAN references once using the installed 6.7.6.14 task/library code and an
explicitly identified debugger-compatible Python interpreter. It copies the
frozen 6.7.5.9 raw CFS/WTCFS payloads into an isolated cache, verifies their
identity, and guards eight native CF-generation entries before imaging. The
reference starts with an empty model. It does not rerun or replace the frozen
full-dataset reference products described below.

The gate binds source, binary, interpreter, runtime, input, and mask identities;
uses one worker, 16 GiB admission, and the 384 MiB decoded AW pool; and supervises
the process group plus observed descendants under a 32 GiB sampled RSS ceiling
and a 30-minute checkpoint. This is not a kernel-enforced instantaneous RSS
guarantee or a performance acceptance target. A missing terminal receipt is not
a pass. Same-scale visual review and the single programme contract review
remain required after numerical completion.

## Original full-dataset protocol retained for #625

This gate runs the production casa-rs compile/plan/run route for dirty imaging
and deterministic CLEAN. It does not invoke CASA. Both runs compare against the
already-frozen CASA 6.7.5.9 products for the 4,096-square, 63-field, full-16-SPW
VLASS fragment.

The gate fails closed unless each receipt proves:

- `awproject`, 32 W planes, EVLA A term, wideband conjugate beams, POINTING,
  Briggs weighting, MT-MFS with two Taylor terms, and flat-noise normalization;
- the complete validated paired-CF inventory: exactly 1,024 cells across all 16
  selected SPWs, with positive W, Mueller, and parallactic-angle axes;
- one immutable private-artifact catalog identity and logical size across every
  attempt-local reader session, with nonzero validated reads, bytes, operations,
  decoded copies, and matching load/read counts;
- scheduler-owned reader closure and exact measured decoded, pinned,
  decoder-workspace, and combined residency within the 384 MiB decoded-cell
  ceiling, explicit maximum-cell decoder-workspace ceiling, and plan-owned
  total ceiling;
- at least 1,000,000 selected correlation-channel samples with internally
  consistent row/channel/correlation telemetry;
- an exact 18-product dirty or 19-product clean inventory, matching metadata,
  and matching finite/mask/non-finite validity topology;
- full-array normalized RMS no greater than `1e-3` for every product; and
- measured production peak RSS strictly below 32 GiB.

The dirty run owns cold prepared-artifact population. The clean run reuses the
same cache root and exercises the warm boundary. The runner is forced to skip
CASA and to execute casa-rs, so an unavailable or aliased Rust AW route cannot
produce a passing receipt.

## Frozen inputs

```text
Dataset root used for the retained T51 evidence (the immutable source MS
remains unchanged):
/Volumes/GLENDENNING/casa-rs-vlass/issue-446/t51-ticket-537-20260903.thHAWF/data

Frozen dirty CASA prefix:
/Volumes/GLENDENNING/casa-rs-vlass/issue-446/recovery-references/casa-f-all63-clean-4096-full-16-spw/cache-construction-artifacts/vlass-fragment-all-fields-dirty-4096-full-16-spw-casa/reduced_turnaround_only/20260806T112128Z-vlass-fragment-all-fields-dirty-4096-full-16-spw-casa-9f8ed92d/casa/measured-001/casa

Frozen clean CASA prefix:
/Volumes/GLENDENNING/casa-rs-vlass/issue-446/recovery-references/casa-g-all63-clean-4096-full-16-spw/artifacts/vlass-fragment-all-fields-clean-4096-full-16-spw-casa/reduced_turnaround_only/20260810T221526Z-vlass-fragment-all-fields-clean-4096-full-16-spw-casa-baseline/casa/measured-001/casa

Deterministic clean mask:
/Volumes/GLENDENNING/casa-rs-vlass/issue-446/masks/vlass-source-box-4096-spectral.mask

CASA Python used only by the image-product comparator:
/Applications/CASA.app/Contents/Frameworks/Python.framework/Versions/3.12/bin/python3.12
```

## Runner

The runner, `tools/perf/imager/t51_aw_vlass_acceptance.py`, was deleted after
the imaging foundation performance pass (IF-10, #659). It required the spill-rate
variables plan section 8.2 removed and validated the prepared-AW private
manifests and reader-session receipts that IF-3 deleted (D3); the production
route keeps neither. The gate cannot run as written above. The full-size VLASS
obligations stay open on #445 and #448.
