# T51 Paired-AW VLASS Acceptance Gate

Truth class: executable acceptance recipe

This gate runs the production casa-rs compile/plan/run route for dirty imaging
and deterministic CLEAN. It does not invoke CASA. Both runs compare against the
already-frozen CASA 6.7.5.9 products for the 4,096-square, 63-field, full-16-SPW
VLASS fragment.

The gate fails closed unless each receipt proves:

- `awproject`, 32 W planes, EVLA A term, wideband conjugate beams, POINTING,
  Briggs weighting, MT-MFS with two Taylor terms, and flat-noise normalization;
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
Dataset root:
/Volumes/GLENDENNING/casa-rs-vlass/issue-446/data/b80d5e87487ab8ab01faa064c4cd48db6d93446fd0add208c051dd574e0d353a

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
CASA_RS_VLASS_DATA_ROOT=/Volumes/GLENDENNING/casa-rs-vlass/issue-446/data/b80d5e87487ab8ab01faa064c4cd48db6d93446fd0add208c051dd574e0d353a \
CASA_RS_CASA_PYTHON=/Applications/CASA.app/Contents/Frameworks/Python.framework/Versions/3.12/bin/python3.12 \
python3 tools/perf/imager/t51_aw_vlass_acceptance.py \
  --dirty-casa-prefix /Volumes/GLENDENNING/casa-rs-vlass/issue-446/recovery-references/casa-f-all63-clean-4096-full-16-spw/cache-construction-artifacts/vlass-fragment-all-fields-dirty-4096-full-16-spw-casa/reduced_turnaround_only/20260806T112128Z-vlass-fragment-all-fields-dirty-4096-full-16-spw-casa-9f8ed92d/casa/measured-001/casa \
  --clean-casa-prefix /Volumes/GLENDENNING/casa-rs-vlass/issue-446/recovery-references/casa-g-all63-clean-4096-full-16-spw/artifacts/vlass-fragment-all-fields-clean-4096-full-16-spw-casa/reduced_turnaround_only/20260810T221526Z-vlass-fragment-all-fields-clean-4096-full-16-spw-casa-baseline/casa/measured-001/casa \
  --output-dir /Volumes/GLENDENNING/casa-rs-vlass/issue-537/receipts \
  --artifact-root /Volumes/GLENDENNING/casa-rs-vlass/issue-537/artifacts \
  --cf-cache-root /Volumes/GLENDENNING/casa-rs-vlass/issue-537/casa-oracle-cache \
  --prepared-aw-casa-cache /Volumes/GLENDENNING/casa-rs-vlass/issue-446/cf-cache/6.7.5.9/3f8343a6717f48d89286e440be1fd59ba542a88324b98061540d6d4aa79e0e1c \
  --prepared-aw-shared-parent /Volumes/GLENDENNING/casa-rs-vlass/issue-537/native-aw-cold-warm
```

The terminal artifact is
`/Volumes/GLENDENNING/casa-rs-vlass/issue-537/receipts/t51-aw-vlass-acceptance.json`.
It contains the two immutable workload-receipt paths, their required
`["cold", "warm"]` sequence, the immutable CASA-oracle cache root, the distinct
validated paired CFS/WTCFS source, the single shared native prepared-store
parent, and the measured summary. The shared parent must not exist before an
actual gate run. The gate rejects a preexisting private store, requires the
dirty run to materialize private manifests, and requires the clean run to reuse
the exact unchanged manifest set.

The MeasurementSet, output directory, artifact root, CASA-oracle cache root,
and shared native prepared-store parent must all be fresh safe paths on
`/Volumes/GLENDENNING`. Native production storage validation requires the
MeasurementSet and outputs to share one filesystem; do not copy or materialize
the MeasurementSet to satisfy this constraint. `--cf-cache-root` is only the
harness/CASA-oracle cache workspace and is never forwarded as native
`--cfcache`.
Append `--dry-run` to validate both production plans and all path bindings
without executing imaging or the comparator.
