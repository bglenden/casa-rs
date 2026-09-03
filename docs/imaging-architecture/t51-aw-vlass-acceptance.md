# T51 Paired-AW VLASS Acceptance Gate

Truth class: executable acceptance recipe

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

## Required external inputs

```text
Explicitly owner-initialized dataset root used for the retained T51 evidence
(the immutable source MS remains unchanged):
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

## Exact command

```bash
CASA_RS_VLASS_DATA_ROOT=/Volumes/GLENDENNING/casa-rs-vlass/issue-446/t51-ticket-537-20260903.thHAWF/data \
CASA_RS_CASA_PYTHON=/Applications/CASA.app/Contents/Frameworks/Python.framework/Versions/3.12/bin/python3.12 \
CASA_RS_IMAGING_SPILL_READ_BYTES_PER_SECOND=3000000000 \
CASA_RS_IMAGING_SPILL_WRITE_BYTES_PER_SECOND=3000000000 \
python3 tools/perf/imager/t51_aw_vlass_acceptance.py \
  --dirty-casa-prefix /Volumes/GLENDENNING/casa-rs-vlass/issue-446/recovery-references/casa-f-all63-clean-4096-full-16-spw/cache-construction-artifacts/vlass-fragment-all-fields-dirty-4096-full-16-spw-casa/reduced_turnaround_only/20260806T112128Z-vlass-fragment-all-fields-dirty-4096-full-16-spw-casa-9f8ed92d/casa/measured-001/casa \
  --clean-casa-prefix /Volumes/GLENDENNING/casa-rs-vlass/issue-446/recovery-references/casa-g-all63-clean-4096-full-16-spw/artifacts/vlass-fragment-all-fields-clean-4096-full-16-spw-casa/reduced_turnaround_only/20260810T221526Z-vlass-fragment-all-fields-clean-4096-full-16-spw-casa-baseline/casa/measured-001/casa \
  --output-dir /Volumes/GLENDENNING/casa-rs-vlass/issue-446/t51-ticket-537/receipts \
  --artifact-root /Volumes/GLENDENNING/casa-rs-vlass/issue-446/t51-ticket-537/artifacts \
  --cf-cache-root /Volumes/GLENDENNING/casa-rs-vlass/issue-446/t51-ticket-537/casa-oracle-cache \
  --prepared-aw-casa-cache /Volumes/GLENDENNING/casa-rs-vlass/issue-446/cf-cache/6.7.5.9/c03a1fab375d7f1747bad8cfb3fad38cf4620fea401570ed779d3def3fad1c36 \
  --prepared-aw-shared-parent /Volumes/GLENDENNING/casa-rs-vlass/issue-446/t51-ticket-537/native-aw-cold-warm
```

The terminal artifact is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-446/t51-ticket-537/receipts/t51-aw-vlass-acceptance.json`.
Use a fresh run-specific replacement for the `t51-ticket-537` directory on
each actual execution.
The gate binds a conservative 3.0 GB/s read and write storage profile, below
the cache-bypassing 1 GiB samples measured on the acceptance filesystem
(3.16 GB/s read and 3.25 GB/s write on 2026-09-03).
It contains the two immutable workload-receipt paths, their required
`["cold", "warm"]` sequence, the immutable CASA-oracle cache root, the distinct
validated paired CFS/WTCFS source, the single shared native prepared-store
parent, the exact cache inventory, and every reader-session transfer and
residency receipt. The shared parent must not exist before an actual gate run.
The gate rejects a preexisting private store, requires the dirty run to
materialize exactly 1,024 private manifests, and requires the clean run to
reuse the exact unchanged manifest set.

The direct MeasurementSet must already contain a valid
`CASA_RS_IMAGING_OWNER_MANIFEST`; the gate never mutates or silently migrates
the source dataset. The output directory, artifact root, CASA-oracle cache root,
and shared native prepared-store parent must all be fresh safe paths on
`/Volumes/GLENDENNING`. Native production storage validation requires the
MeasurementSet and outputs to share one filesystem; do not make an unrecorded
copy merely to bypass this constraint. `--cf-cache-root` is only the
harness/CASA-oracle cache workspace and is never forwarded as native
`--cfcache`.
Append `--dry-run` to validate both production plans and all path bindings
without executing imaging or the comparator.
