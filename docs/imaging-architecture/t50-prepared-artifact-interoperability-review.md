# T50 prepared-artifact persistent-format interoperability review

Truth class: implementation boundary review
Status: accepted for T50/#536
Reviewed: 2026-08-22
Implementation base: `252095de4d43791bf0c21acb5257169f80aa8e85`

## Scope reviewed

T50 needs persistent, content-addressed implementation-preparation artifacts
without creating a second scientific-product authority or silently extending a
CASA-visible format. This review covers the existing CASA AWProject cache
reader, casacore-compatible MeasurementSet and image persistence, provider
contracts, and the proposed private cache.

The existing `casa-imaging::AwConvolutionFunctionCache` is the CASA
interoperability owner. It opens an existing directory of paired CASA PagedImage
tables named `CFS_*.im` and `WTCFS_*.im`, validates the complete
frequency-by-W-by-Mueller-by-parallactic-angle inventory, and loads pixels
read-only. A pair is asymmetric: imaging and weight planes may have different
pixel shapes, support extents, and affine UU/VV coordinate definitions while
covering the same UV world window. The current adapter validates those named
roles, their shared scientific key, sampling, metadata, and finite complex
pixels. A generic byte reader is therefore not a CASA-cache validator.

## Decision

T50 will persist only a private casa-rs prepared-artifact format beneath an
explicitly configured cache root. Its directory names, manifest, and payload
are not CASA image tables and will never use the `CFS_`, `WTCFS_`, or `.im`
naming conventions. The store will neither discover nor mutate a CASA cache,
and it will not place locks, indexes, manifests, packed files, or other sidecars
inside one.

The private format may change under its own versioned schema. It must retain
separate named imaging and weight layout records, including their independently
sized shapes, supports, affine coordinate identities, and byte ranges. The
format is accepted only because it is not CASA-visible and does not change any
casacore-compatible MeasurementSet, table, lattice, coordinate, or image
structure.

Existing CASA caches remain read-only inputs to the existing validated legacy
adapter. T50 does not add a `load_from_casa_cache` entrypoint and does not allow
an arbitrary reader or caller-authored provenance label to claim CASA
validation. A later native backend may translate an
`AwConvolutionFunctionCache` cell into the private artifact representation only
after it maps the adapter's exact paired metadata into the T50 descriptor; that
integration is outside T50 and must appear as explicit plan-listed work.

Generation and cold load therefore accept only bounded absolute regular-file
source descriptors. The runtime opens one source at a time under the cache
node's lease, rejects CASA image, MeasurementSet, and table ancestry, and folds
source-descriptor residency and source-read traffic into the same physical
buffer reservation used for private-store streaming. This execution-local path
contract is not a CASA cache reader and is never persisted as provenance.

T50 also does not add or expand a provider-contract bundle. The private cache is
an execution implementation detail. It uses ADR-0010's canonical
`ArtifactIdentity`, `CacheIdentity`, plan nodes, resource/storage claims,
artifact measurements, and `ExecutionReceipt` projection. Its lookup,
generation, load, validation, residency, eviction, and failure cannot select
Product Graph members or acquire product-generation/publication authority.

## Stop conditions

Stop for a separate review before any implementation:

- writes, renames, or sidecars a CASA `CFS_`/`WTCFS_` cache;
- changes a casacore-compatible MeasurementSet, table, lattice, coordinate, or
  image format;
- exposes the private schema through a provider contract;
- lets prepared-artifact cache state choose Product Graph topology, product
  generation, or scientific publication; or
- bypasses the canonical ADR-0010 `ExecutionPlan` or `ExecutionReceipt`.
