# Proposal: retain the native run-summary sidecar

Status: proposed; owner decision pending. Work issue: #980.

Recommend retaining `<imagename>.summary.json` as an explicitly documented
additional casa-rs output. CASA 6.7.6.14 does not create this file. This
proposal changes no CASA parameter, signature/default, image value or return
value. It asks whether the existing additional file is an acceptable deviation.

The native task always sets a summary target once output preflight succeeds
(`crates/casars-imager/src/lib.rs`, `run_with_statistics`). Python launches
that same task executable. There is currently no public tclean opt-out.
The internal Rust execution context can omit its summary target; that is
not a user-facing CASA parameter.

The sidecar records status, resolved request, products, backend/precision,
phase timing and resource information. It is separate from the MeasurementSet
and scientific image tables. It supports the existing performance/admission
evidence and failure diagnostics. The imaging foundation's accepted design
already documents this report and its contents, but it did not explicitly
state a CASA output-file exception. No accepted ADR is changed by this proposal.

The pinned nmajor oracle's `initial_only` directory contains seven image
product directories. Native produces those seven plus `image.summary.json`.
The harness's request/stdout/stderr files are test artifacts, not task outputs.
The [nmajor receipt](../../resources/casa-compat/evidence/tclean-nmajor-zero-20261101.json)
binds native revision `c64f13cb09054b080fe8b228a5f2c02c99bfd486`, the exact
binary and the CASA build. Existing comparisons verify image-product inventory;
they do not silently count the extra summary as CASA-identical output.

User impact if accepted: scripts/globs see one extra JSON file beside image
products. Fresh numbered runs place the report beside the chosen numbered
prefix; ordinary continuation updates its own report. Scientific files and
CASA's task parameters retain their existing contracts. Correct history and
all other unfinished output/metadata obligations remain required.

The alternative is to make ordinary CASA-shaped calls omit this sidecar,
while retaining diagnostics through another explicit mechanism. That would
need a separate concrete implementation and contract review. The recommendation
here is to preserve the existing report and record this exact deviation.
