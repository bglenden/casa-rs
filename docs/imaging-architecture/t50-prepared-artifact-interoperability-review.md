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

At the reviewed T50 base, `casa-imaging::AwConvolutionFunctionCache` was the CASA
interoperability owner. It opened an existing directory of paired CASA PagedImage
tables named `CFS_*.im` and `WTCFS_*.im`, validates the complete
frequency-by-W-by-Mueller-by-parallactic-angle inventory, and loads pixels
read-only. A pair is asymmetric: imaging and weight planes may have different
pixel shapes, support extents, and affine UU/VV coordinate definitions while
covering the same UV world window. The current adapter validates those named
roles, their shared scientific key, sampling, metadata, and finite complex
pixels. A generic byte reader is therefore not a CASA-cache validator.

Post-T23/#574, that displaced reader has been deleted with the old imaging
package. No production AWProject cache reader remains. A later AWProject ticket
must introduce its CASA-compatible reader at the final prepared-artifact owner
before claiming the corresponding capability Native; it may not recover the
deleted package or add a fallback route.

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

Generation executes through the implementation adapter selected by the
canonical registry and fills only the store-owned bounded buffer; it cannot
copy caller-selected source paths. Cold load accepts content-committed regular
files only through a source artifact listed in the canonical plan, owned by an
exact predecessor/import node, and retained in that predecessor's receipt.
The load node verifies the source identity and digests while its lease is live,
opens one source at a time, rejects CASA image, MeasurementSet, and table
ancestry, and accounts source-descriptor residency and source-read traffic in
the same physical reservation used for private-store streaming. Execution-local
locators are neither identity-bearing provenance nor persisted in the cache.

Prepared-artifact identity commits only to the registry/catalog owner,
implementation version, compiled scientific commitments, artifact kind, and
canonical named layout. Cache-root identity, byte and entry budgets, streaming
policy, and eviction policy belong only to `CacheIdentity`, so relocating the
same immutable bytes or changing cache policy cannot change content identity.
Provider/catalog metadata is obtained through the exact implementation
registry record; callers cannot pass an owner record directly to a descriptor.

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
