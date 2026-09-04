// SPDX-License-Identifier: LGPL-3.0-or-later

//! Plan-bound private persistence for immutable implementation preparation.

mod accounting;
mod codec;
mod filesystem;
mod planning;
mod reader;
mod transaction;

use accounting::*;
use codec::*;
use filesystem::*;
pub use planning::{PreparedArtifactPlanError, PreparedArtifactPlanFragment};
pub use reader::{
    PreparedArtifactExecutionBinding, PreparedArtifactReader, PreparedArtifactReaderFactory,
    PreparedArtifactReaderPlan, PreparedArtifactReaderResidency,
    PreparedArtifactResidencyMeasurements,
};

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, BufReader, Read, Write},
    mem::{size_of, size_of_val},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, Weak},
};

use casa_imaging_model::{
    CompiledProblem, PreparedArtifactScientificIdentity, PreparedArtifactScientificKind,
};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tempfile::Builder;

use crate::{
    ArtifactDisposition, ArtifactIdentity, ArtifactMeasurement, ArtifactRole, CacheIdentity,
    ImplementationRegistry, ImplementationRegistryId, IoBufferKind, IoMeasurement, LeaseResource,
    PlannedArtifact, RateResourceId, RedactedPath, ResourceMeasurement, StorageDomain,
    StorageDomainId, StorageUseKind, WorkDependency, WorkExecutionContext, WorkImplementationId,
    WorkKind, WorkMeasurements, WorkNodeId,
};

const ARTIFACT_IDENTITY_DOMAIN: &[u8] = b"casa-rs/private-prepared-artifact/identity\0";
const CONTENT_IDENTITY_DOMAIN: &[u8] = b"casa-rs/private-prepared-artifact/content\0";
const CACHE_IDENTITY_DOMAIN: &[u8] = b"casa-rs/private-prepared-artifact/cache\0";
const CACHE_ROOT_IDENTITY_DOMAIN: &[u8] = b"casa-rs/private-prepared-artifact/root\0";
const LOAD_SOURCE_IDENTITY_DOMAIN: &[u8] = b"casa-rs/private-prepared-artifact/load-source\0";
const IMPORT_SOURCE_IDENTITY_DOMAIN: &[u8] = b"casa-rs/private-prepared-artifact/import-source\0";
const WORK_NODE_IDENTITY_DOMAIN: &[u8] = b"casa-rs/private-prepared-artifact/work-node\0";
const WORK_IMPLEMENTATION_ID_DOMAIN: &[u8] =
    b"casa-rs/private-prepared-artifact/work-implementation\0";
const REJECTION_EVIDENCE_DOMAIN: &[u8] = b"casa-rs/private-prepared-artifact/rejection\0";
const EVICTION_LEDGER_DOMAIN: &[u8] = b"casa-rs/private-prepared-artifact/eviction-ledger\0";
const EVICTION_OBSERVED_DOMAIN: &[u8] = b"casa-rs/private-prepared-artifact/eviction-observed\0";
const ORPHAN_STAGING_EVIDENCE_DOMAIN: &[u8] =
    b"casa-rs/private-prepared-artifact/orphan-staging-evidence\0";
const IDENTITY_VERSION: u32 = 6;
const CACHE_SCHEMA: &str = "casa-rs-private-prepared-artifact";
const CACHE_SCHEMA_VERSION: u32 = 6;
const EVICTION_POLICY: &str = "lexicographic-existing-artifact-identity-v1";
const CACHE_DIRECTORY: &str = "objects-v3";
const LOCK_FILE: &str = ".casa-rs-prepared-artifact.lock";
const MANIFEST_FILE: &str = "manifest.json";
const PAYLOAD_FILE: &str = "payload.bin";
const STAGING_PREFIX: &str = ".staging-";
const MANIFEST_RESERVATION_BYTES: u64 = 16 * 1024;
// The manifest is parsed into owned Rust values before payload validation.  The
// resident allowance deliberately exceeds the serialized ceiling so strings,
// vectors, and serde bookkeeping remain inside the plan-bound reservation.
const MANIFEST_RESIDENT_BYTES: u64 = 64 * 1024;
const MAX_MANIFEST_SEGMENTS: usize = 64;
const MAX_ENTRY_FILES: usize = 2;
const STREAMING_BUFFER_CEILING: usize = 64 * 1024;
const MAX_CACHE_COMPONENT_BYTES: usize = 255;
const MAX_IDENTIFIER_BYTES: usize = 256;
// Source descriptors are execution-local paths, not persisted names. Bounding
// them makes their complete owned residency plan-visible without relying on a
// platform-specific PATH_MAX value.
const MAX_SOURCE_PATH_BYTES: usize = 4 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CacheInventoryEntry {
    identity: ArtifactIdentity,
    bytes: u64,
}

/// Implementation family of a private prepared artifact.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PreparedArtifactKind {
    /// Paired imaging and weight convolution-function planes.
    ConvolutionFunction,
    /// Immutable spectral-coordinate or channel-routing map.
    SpectralMap,
    /// Other immutable numerical implementation kernel.
    Kernel,
}

/// Scalar or complex representation in one named payload segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PreparedArtifactPrecision {
    /// IEEE-754 binary32 scalar.
    F32,
    /// IEEE-754 binary64 scalar.
    F64,
    /// Interleaved real/imaginary IEEE-754 binary32 complex value.
    ComplexF32,
    /// Interleaved real/imaginary IEEE-754 binary64 complex value.
    ComplexF64,
    /// Signed little-endian 32-bit integer.
    I32,
    /// Unsigned little-endian 32-bit integer.
    U32,
    /// Unsigned byte.
    U8,
}

impl PreparedArtifactPrecision {
    const fn element_bytes(self) -> u64 {
        match self {
            Self::F32 | Self::I32 | Self::U32 => 4,
            Self::F64 | Self::ComplexF32 => 8,
            Self::ComplexF64 => 16,
            Self::U8 => 1,
        }
    }

    const fn scalar_bytes(self) -> usize {
        match self {
            Self::F32 | Self::ComplexF32 | Self::I32 | Self::U32 => 4,
            Self::F64 | Self::ComplexF64 => 8,
            Self::U8 => 1,
        }
    }

    const fn is_complex(self) -> bool {
        matches!(self, Self::ComplexF32 | Self::ComplexF64)
    }
}

/// Exact axis and byte order of one named payload segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PreparedArtifactOrder {
    /// Axis zero is contiguous and scalars use little-endian byte order.
    Axis0ContiguousLittleEndian,
    /// The final axis is contiguous and scalars use little-endian byte order.
    LastAxisContiguousLittleEndian,
}

/// Bit-exact affine UU/VV coordinate identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedArtifactUvAffine {
    reference_value_bits: [u64; 2],
    reference_pixel_bits: [u64; 2],
    increment_bits: [u64; 2],
    pc_matrix_bits: [[u64; 2]; 2],
}

impl PreparedArtifactUvAffine {
    /// Validate and retain the exact affine coordinate bit patterns.
    pub fn new(
        reference_value: [f64; 2],
        reference_pixel: [f64; 2],
        increment: [f64; 2],
        pc_matrix: [[f64; 2]; 2],
    ) -> Result<Self, PreparedArtifactError> {
        let values = [
            reference_value[0],
            reference_value[1],
            reference_pixel[0],
            reference_pixel[1],
            increment[0],
            increment[1],
            pc_matrix[0][0],
            pc_matrix[0][1],
            pc_matrix[1][0],
            pc_matrix[1][1],
        ];
        let determinant = pc_matrix[0][0] * pc_matrix[1][1] - pc_matrix[0][1] * pc_matrix[1][0];
        if values.iter().any(|value| !value.is_finite())
            || increment.contains(&0.0)
            || !determinant.is_finite()
            || determinant == 0.0
        {
            return Err(PreparedArtifactError::InvalidUvAffine);
        }
        Ok(Self {
            reference_value_bits: reference_value.map(f64::to_bits),
            reference_pixel_bits: reference_pixel.map(f64::to_bits),
            increment_bits: increment.map(f64::to_bits),
            pc_matrix_bits: pc_matrix.map(|row| row.map(f64::to_bits)),
        })
    }

    /// Return the exact UU/VV reference-value bits.
    #[must_use]
    pub const fn reference_value_bits(&self) -> [u64; 2] {
        self.reference_value_bits
    }

    /// Return the exact UU/VV reference-pixel bits.
    #[must_use]
    pub const fn reference_pixel_bits(&self) -> [u64; 2] {
        self.reference_pixel_bits
    }

    /// Return the exact UU/VV increment bits.
    #[must_use]
    pub const fn increment_bits(&self) -> [u64; 2] {
        self.increment_bits
    }

    /// Return the row-major exact 2x2 PC-matrix bits.
    #[must_use]
    pub const fn pc_matrix_bits(&self) -> [[u64; 2]; 2] {
        self.pc_matrix_bits
    }

    fn validate_persisted(&self) -> Result<(), PreparedArtifactError> {
        Self::new(
            self.reference_value_bits.map(f64::from_bits),
            self.reference_pixel_bits.map(f64::from_bits),
            self.increment_bits.map(f64::from_bits),
            self.pc_matrix_bits.map(|row| row.map(f64::from_bits)),
        )
        .map(|_| ())
    }
}

/// Independently shaped and interpreted convolution-function plane.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedArtifactPlaneDescriptor {
    shape: [u64; 2],
    support: [u64; 2],
    sampling: u64,
    uv_affine: PreparedArtifactUvAffine,
    precision: PreparedArtifactPrecision,
    order: PreparedArtifactOrder,
}

impl PreparedArtifactPlaneDescriptor {
    /// Validate every layout and coordinate semantic of one CF plane.
    pub fn new(
        shape: [u64; 2],
        support: [u64; 2],
        sampling: u64,
        uv_affine: PreparedArtifactUvAffine,
        precision: PreparedArtifactPrecision,
        order: PreparedArtifactOrder,
    ) -> Result<Self, PreparedArtifactError> {
        if shape.contains(&0) || support.contains(&0) || sampling == 0 || !precision.is_complex() {
            return Err(PreparedArtifactError::InvalidLayout);
        }
        for axis in 0..2 {
            let required = support[axis]
                .checked_mul(sampling)
                .and_then(|half| half.checked_mul(2))
                .and_then(|diameter| diameter.checked_add(1))
                .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
            if required > shape[axis] {
                return Err(PreparedArtifactError::InvalidLayout);
            }
        }
        Ok(Self {
            shape,
            support,
            sampling,
            uv_affine,
            precision,
            order,
        })
    }

    /// Return the exact two-dimensional pixel shape.
    #[must_use]
    pub const fn shape(&self) -> [u64; 2] {
        self.shape
    }

    /// Return independent X/Y support extents.
    #[must_use]
    pub const fn support(&self) -> [u64; 2] {
        self.support
    }

    /// Return the oversampling factor.
    #[must_use]
    pub const fn sampling(&self) -> u64 {
        self.sampling
    }

    /// Return the exact UU/VV affine coordinate identity.
    #[must_use]
    pub const fn uv_affine(&self) -> &PreparedArtifactUvAffine {
        &self.uv_affine
    }

    /// Return the element precision.
    #[must_use]
    pub const fn precision(&self) -> PreparedArtifactPrecision {
        self.precision
    }

    /// Return the axis and byte order.
    #[must_use]
    pub const fn order(&self) -> PreparedArtifactOrder {
        self.order
    }

    fn into_segment(self, name: &str) -> PreparedArtifactSegmentDescriptor {
        PreparedArtifactSegmentDescriptor {
            name: name.to_string(),
            shape: self.shape.to_vec(),
            support: self.support.to_vec(),
            sampling: vec![self.sampling; 2],
            uv_affine: Some(self.uv_affine),
            precision: self.precision,
            order: self.order,
        }
    }
}

/// Exact layout of one private, named payload segment.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedArtifactSegmentDescriptor {
    name: String,
    shape: Vec<u64>,
    support: Vec<u64>,
    sampling: Vec<u64>,
    uv_affine: Option<PreparedArtifactUvAffine>,
    precision: PreparedArtifactPrecision,
    order: PreparedArtifactOrder,
}

impl PreparedArtifactSegmentDescriptor {
    /// Validate a generic named segment used by a spectral map or kernel.
    pub fn new(
        name: impl Into<String>,
        shape: Vec<u64>,
        support: Vec<u64>,
        sampling: Vec<u64>,
        uv_affine: Option<PreparedArtifactUvAffine>,
        precision: PreparedArtifactPrecision,
        order: PreparedArtifactOrder,
    ) -> Result<Self, PreparedArtifactError> {
        let segment = Self {
            name: name.into(),
            shape,
            support,
            sampling,
            uv_affine,
            precision,
            order,
        };
        segment.validate()?;
        Ok(segment)
    }

    /// Return the private segment name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the logical element shape.
    #[must_use]
    pub fn shape(&self) -> &[u64] {
        &self.shape
    }

    /// Return per-axis support semantics.
    #[must_use]
    pub fn support(&self) -> &[u64] {
        &self.support
    }

    /// Return per-axis sampling semantics.
    #[must_use]
    pub fn sampling(&self) -> &[u64] {
        &self.sampling
    }

    /// Return a UU/VV affine identity when this is a CF plane.
    #[must_use]
    pub const fn uv_affine(&self) -> Option<&PreparedArtifactUvAffine> {
        self.uv_affine.as_ref()
    }

    /// Return the exact element precision.
    #[must_use]
    pub const fn precision(&self) -> PreparedArtifactPrecision {
        self.precision
    }

    /// Return the exact axis and byte order.
    #[must_use]
    pub const fn order(&self) -> PreparedArtifactOrder {
        self.order
    }

    fn validate(&self) -> Result<(), PreparedArtifactError> {
        if !valid_segment_name(&self.name)
            || self.shape.is_empty()
            || self.shape.len() > 16
            || self.shape.contains(&0)
            || self.support.len() != self.shape.len()
            || self.sampling.len() != self.shape.len()
            || self.sampling.contains(&0)
            || self.uv_affine.is_some() && self.shape.len() != 2
        {
            return Err(PreparedArtifactError::InvalidLayout);
        }
        if let Some(affine) = &self.uv_affine {
            affine.validate_persisted()?;
        }
        for axis in 0..self.shape.len() {
            if self.support[axis] == 0 {
                continue;
            }
            let required = self.support[axis]
                .checked_mul(self.sampling[axis])
                .and_then(|half| half.checked_mul(2))
                .and_then(|diameter| diameter.checked_add(1))
                .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
            if required > self.shape[axis] {
                return Err(PreparedArtifactError::InvalidLayout);
            }
        }
        self.byte_len()?;
        Ok(())
    }

    fn byte_len(&self) -> Result<u64, PreparedArtifactError> {
        checked_product(&self.shape)
            .and_then(|elements| elements.checked_mul(self.precision.element_bytes()))
            .ok_or(PreparedArtifactError::ArtifactTooLarge)
    }
}

/// Canonical provider/catalog record stored by an immutable implementation registry.
///
/// Callers may describe registrations while assembling a registry, but cannot
/// pass one directly to a prepared descriptor. The descriptor asks the exact
/// registry snapshot for the record keyed by the owning implementation and
/// mints its closed owner internally.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedArtifactRegistration {
    provider_catalog: String,
    provider: String,
    provider_version: String,
    implementation: WorkImplementationId,
}

impl PreparedArtifactRegistration {
    /// Declare one provider/catalog-owned preparation implementation.
    pub fn new(
        provider_catalog: impl Into<String>,
        provider: impl Into<String>,
        provider_version: impl Into<String>,
        implementation: WorkImplementationId,
    ) -> Result<Self, PreparedArtifactError> {
        let provider_catalog = provider_catalog.into();
        let provider = provider.into();
        let provider_version = provider_version.into();
        if !valid_identifier(&provider_catalog)
            || !valid_identifier(&provider)
            || !valid_identifier(&provider_version)
            || !valid_identifier(implementation.as_str())
        {
            return Err(PreparedArtifactError::InvalidOwner);
        }
        Ok(Self {
            provider_catalog,
            provider,
            provider_version,
            implementation,
        })
    }

    /// Return the canonical provider-catalog identity.
    #[must_use]
    pub fn provider_catalog(&self) -> &str {
        &self.provider_catalog
    }

    /// Return the provider identity.
    #[must_use]
    pub fn provider(&self) -> &str {
        &self.provider
    }

    /// Return the exact provider version.
    #[must_use]
    pub fn provider_version(&self) -> &str {
        &self.provider_version
    }

    /// Return the selected implementation identity.
    #[must_use]
    pub const fn implementation(&self) -> &WorkImplementationId {
        &self.implementation
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PreparedArtifactOwner {
    implementation_registry: ImplementationRegistryId,
    registration: PreparedArtifactRegistration,
}

impl PreparedArtifactOwner {
    fn from_registry<R: ImplementationRegistry>(
        registry: &R,
        implementation: &WorkImplementationId,
    ) -> Result<Self, PreparedArtifactError> {
        let registration = registry
            .prepared_artifact_registration(implementation)
            .filter(|registration| registration.implementation() == implementation)
            .ok_or(PreparedArtifactError::InvalidOwner)?;
        Ok(Self {
            implementation_registry: registry.registry_id(),
            registration: registration.clone(),
        })
    }

    fn from_manifest(
        implementation_registry: ImplementationRegistryId,
        registration: PreparedArtifactRegistration,
    ) -> Self {
        Self {
            implementation_registry,
            registration,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScientificCommitments {
    compiled_problem: String,
    observation_snapshot: String,
    compiled_geometry: String,
    numerics_contract: String,
    owner_scientific_identity: String,
}

impl ScientificCommitments {
    fn from_problem(
        problem: &CompiledProblem,
        owner_scientific_identity: PreparedArtifactScientificIdentity,
    ) -> Self {
        Self {
            compiled_problem: encode_hex(&problem.problem_id().as_bytes()),
            observation_snapshot: encode_hex(&problem.inputs().observation().as_bytes()),
            compiled_geometry: encode_hex(&problem.geometry().geometry_id().as_bytes()),
            numerics_contract: encode_hex(&problem.numerics_id().as_bytes()),
            owner_scientific_identity: encode_hex(&owner_scientific_identity.as_bytes()),
        }
    }

    fn validate(&self) -> Result<(), PreparedArtifactError> {
        for identity in [
            &self.compiled_problem,
            &self.observation_snapshot,
            &self.compiled_geometry,
            &self.numerics_contract,
        ] {
            if decode_digest(identity).is_none_or(|digest| digest == [0; 32]) {
                return Err(PreparedArtifactError::InvalidDescriptor);
            }
        }
        if decode_digest(&self.owner_scientific_identity).is_none_or(|digest| digest == [0; 32]) {
            return Err(PreparedArtifactError::InvalidScientificKey);
        }
        Ok(())
    }

    fn matches_context(&self, context: WorkExecutionContext<'_>) -> bool {
        self.compiled_problem == encode_hex(&context.compiled().problem_id().as_bytes())
            && self.observation_snapshot
                == encode_hex(&context.compiled().observation_snapshot_id().as_bytes())
            && self.compiled_geometry
                == encode_hex(&context.compiled().geometry().geometry_id().as_bytes())
            && self.numerics_contract == encode_hex(&context.compiled().numerics_id().as_bytes())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CacheScope {
    root_identity: String,
    storage_domain: String,
    cache_bytes: u64,
    entries: u64,
    streaming_buffer_bytes: u64,
    eviction_policy: String,
}

impl CacheScope {
    fn new(
        root: &Path,
        storage_domain: &StorageDomainId,
        budget: PreparedArtifactBudget,
    ) -> Result<Self, PreparedArtifactError> {
        Ok(Self {
            root_identity: encode_hex(&derive_cache_root_identity(root)),
            storage_domain: storage_domain.as_str().to_string(),
            cache_bytes: budget.cache_bytes,
            entries: u64::try_from(budget.entries)
                .map_err(|_| PreparedArtifactError::InvalidBudget)?,
            streaming_buffer_bytes: budget.streaming_buffer_bytes,
            eviction_policy: EVICTION_POLICY.to_string(),
        })
    }

    fn validate(&self) -> Result<(), PreparedArtifactError> {
        if decode_digest(&self.root_identity).is_none()
            || !valid_identifier(&self.storage_domain)
            || self.cache_bytes == 0
            || self.entries == 0
            || usize::try_from(self.entries).is_err()
            || self.streaming_buffer_bytes == 0
            || self.eviction_policy != EVICTION_POLICY
        {
            return Err(PreparedArtifactError::InvalidDescriptor);
        }
        Ok(())
    }
}

/// Owner-derived immutable compatibility descriptor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedArtifactDescriptor {
    identity: ArtifactIdentity,
    cache_identity: CacheIdentity,
    owner: PreparedArtifactOwner,
    kind: PreparedArtifactKind,
    scientific: ScientificCommitments,
    cache_scope: CacheScope,
    segments: Vec<PreparedArtifactSegmentDescriptor>,
}

impl PreparedArtifactDescriptor {
    fn storage_demand_id(&self, operations_rate: Option<&RateResourceId>) -> String {
        let base = format!("private-prepared-cache-{}", self.cache_identity);
        match operations_rate {
            Some(rate) => format!("{base}-operations-{}", rate.as_str()),
            None => base,
        }
    }

    fn storage_domain_id(&self) -> StorageDomainId {
        StorageDomainId::new(self.cache_scope.storage_domain.clone())
    }

    /// Describe an asymmetric named imaging/weight CF pair.
    pub fn convolution_function<R: ImplementationRegistry>(
        store: &PreparedArtifactStore,
        registry: &R,
        implementation: &WorkImplementationId,
        problem: &CompiledProblem,
        scientific_identity: PreparedArtifactScientificIdentity,
        imaging: PreparedArtifactPlaneDescriptor,
        weight: PreparedArtifactPlaneDescriptor,
    ) -> Result<Self, PreparedArtifactError> {
        if scientific_identity.kind() != PreparedArtifactScientificKind::ConvolutionFunction
            || imaging.sampling() != weight.sampling()
        {
            return Err(PreparedArtifactError::InvalidLayout);
        }
        let owner = PreparedArtifactOwner::from_registry(registry, implementation)?;
        let scientific = ScientificCommitments::from_problem(problem, scientific_identity);
        Self::from_commitments(
            owner,
            PreparedArtifactKind::ConvolutionFunction,
            scientific,
            store.scope.clone(),
            vec![
                imaging.into_segment("imaging"),
                weight.into_segment("weight"),
            ],
        )
    }

    /// Describe a spectral map or other kernel through private named segments.
    pub fn from_owner_identity<R: ImplementationRegistry>(
        store: &PreparedArtifactStore,
        registry: &R,
        implementation: &WorkImplementationId,
        problem: &CompiledProblem,
        scientific_identity: PreparedArtifactScientificIdentity,
        segments: Vec<PreparedArtifactSegmentDescriptor>,
    ) -> Result<Self, PreparedArtifactError> {
        let owner = PreparedArtifactOwner::from_registry(registry, implementation)?;
        let kind = match scientific_identity.kind() {
            PreparedArtifactScientificKind::ConvolutionFunction => {
                PreparedArtifactKind::ConvolutionFunction
            }
            PreparedArtifactScientificKind::SpectralMap => PreparedArtifactKind::SpectralMap,
            PreparedArtifactScientificKind::Kernel => PreparedArtifactKind::Kernel,
        };
        let scientific = ScientificCommitments::from_problem(problem, scientific_identity);
        Self::from_commitments(owner, kind, scientific, store.scope.clone(), segments)
    }

    fn from_commitments(
        owner: PreparedArtifactOwner,
        kind: PreparedArtifactKind,
        scientific: ScientificCommitments,
        cache_scope: CacheScope,
        mut segments: Vec<PreparedArtifactSegmentDescriptor>,
    ) -> Result<Self, PreparedArtifactError> {
        if segments.is_empty() || segments.len() > MAX_MANIFEST_SEGMENTS {
            return Err(PreparedArtifactError::InvalidDescriptor);
        }
        scientific.validate()?;
        cache_scope.validate()?;
        for segment in &segments {
            segment.validate()?;
        }
        segments.sort_unstable_by(|left, right| left.name.cmp(&right.name));
        if segments.windows(2).any(|pair| pair[0].name == pair[1].name) {
            return Err(PreparedArtifactError::InvalidDescriptor);
        }
        if kind == PreparedArtifactKind::ConvolutionFunction {
            validate_cf_segments(&segments)?;
        }
        let cache_identity = derive_cache_identity(&owner, &cache_scope)?;
        let mut descriptor = Self {
            identity: ArtifactIdentity::from_owner_digest([0; 32]),
            cache_identity,
            owner,
            kind,
            scientific,
            cache_scope,
            segments,
        };
        descriptor.identity = derive_artifact_identity(&descriptor)?;
        descriptor.payload_bytes()?;
        Ok(descriptor)
    }

    /// Return the domain-separated canonical artifact identity.
    #[must_use]
    pub const fn identity(&self) -> ArtifactIdentity {
        self.identity
    }

    /// Return the owner-derived canonical cache identity.
    #[must_use]
    pub const fn cache_identity(&self) -> CacheIdentity {
        self.cache_identity
    }

    /// Return the implementation-preparation family.
    #[must_use]
    pub const fn kind(&self) -> PreparedArtifactKind {
        self.kind
    }

    /// Return every private named segment in canonical name order.
    #[must_use]
    pub fn segments(&self) -> &[PreparedArtifactSegmentDescriptor] {
        &self.segments
    }

    /// Return the separately described imaging plane for a CF artifact.
    #[must_use]
    pub fn imaging_plane(&self) -> Option<&PreparedArtifactSegmentDescriptor> {
        self.segment("imaging")
    }

    /// Return the separately described weight plane for a CF artifact.
    #[must_use]
    pub fn weight_plane(&self) -> Option<&PreparedArtifactSegmentDescriptor> {
        self.segment("weight")
    }

    /// Return one exact private segment by name.
    #[must_use]
    pub fn segment(&self, name: &str) -> Option<&PreparedArtifactSegmentDescriptor> {
        self.segments
            .binary_search_by(|segment| segment.name.as_str().cmp(name))
            .ok()
            .map(|index| &self.segments[index])
    }

    /// Bind this descriptor to one canonical plan node and artifact role.
    #[must_use]
    pub fn planned_artifact(&self, operation: PreparedArtifactOperation) -> PlannedArtifact {
        let role = match operation {
            PreparedArtifactOperation::Generate => ArtifactRole::Prepared,
            PreparedArtifactOperation::Load => ArtifactRole::Prepared,
            PreparedArtifactOperation::Reuse => ArtifactRole::Cache,
            PreparedArtifactOperation::Consume => ArtifactRole::Cache,
        };
        PlannedArtifact::new(
            self.identity,
            self.work_node_id(operation),
            role,
            Some(self.cache_identity),
        )
    }

    /// Bind the plan-listed cache-inventory evidence artifact for one operation.
    ///
    /// The ledger uses the existing ADR-0010 artifact-measurement fields to
    /// retain a deterministic identity of the entries considered for
    /// eviction. It is an execution input, never a Product Graph artifact.
    #[must_use]
    pub fn eviction_artifact(&self, operation: PreparedArtifactOperation) -> PlannedArtifact {
        PlannedArtifact::new(
            derive_eviction_ledger_identity(self, operation),
            self.work_node_id(operation),
            ArtifactRole::Input,
            None,
        )
    }

    /// Return the owner-derived exact node identity for one cache operation.
    #[must_use]
    pub fn work_node_id(&self, operation: PreparedArtifactOperation) -> WorkNodeId {
        WorkNodeId::new(format!(
            "prepared-artifact-{}-{}",
            operation.name(),
            encode_hex(&derive_work_node_identity(self, operation))
        ))
    }

    /// Return the exact operation adapter identity that the plan must select.
    ///
    /// The selected registry entry identity includes every owner field, so a
    /// caller cannot change provider metadata without changing the plan-bound
    /// implementation key resolved by the execution registry.
    #[must_use]
    pub fn work_implementation_id(
        &self,
        operation: PreparedArtifactOperation,
    ) -> WorkImplementationId {
        let mut hasher = Sha256::new();
        hasher.update(WORK_IMPLEMENTATION_ID_DOMAIN);
        hasher.update(IDENTITY_VERSION.to_le_bytes());
        hasher.update(self.owner.implementation_registry.as_bytes());
        hash_bytes(
            &mut hasher,
            self.owner.registration.provider_catalog.as_bytes(),
        )
        .expect("validated provider catalog identity length");
        hash_bytes(&mut hasher, self.owner.registration.provider.as_bytes())
            .expect("validated provider identity length");
        hash_bytes(
            &mut hasher,
            self.owner.registration.provider_version.as_bytes(),
        )
        .expect("validated provider version identity length");
        hash_bytes(
            &mut hasher,
            self.owner.registration.implementation.as_str().as_bytes(),
        )
        .expect("validated implementation identity length");
        hasher.update([operation_tag(operation)]);
        let digest: [u8; 32] = hasher.finalize().into();
        WorkImplementationId::new(format!(
            "prepared-artifact-{}-{}",
            operation.name(),
            encode_hex(&digest)
        ))
    }

    fn payload_bytes(&self) -> Result<u64, PreparedArtifactError> {
        self.segments.iter().try_fold(0_u64, |total, segment| {
            total
                .checked_add(segment.byte_len()?)
                .ok_or(PreparedArtifactError::ArtifactTooLarge)
        })
    }
}

/// Exact explicit cache operation selected into a `WorkKind::Cache` node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreparedArtifactOperation {
    /// Generate deterministic bytes for an absent artifact.
    Generate,
    /// Load deterministic bytes from a separately validated private source.
    Load,
    /// Revalidate and reuse an exact private-cache hit.
    Reuse,
    /// Revalidate and stream an exact private-cache hit to its prepared operator.
    Consume,
}

impl PreparedArtifactOperation {
    const fn name(self) -> &'static str {
        match self {
            Self::Generate => "cold-generation",
            Self::Load => "cold-load",
            Self::Reuse => "warm-reuse",
            Self::Consume => "consume",
        }
    }
}

/// Fail-closed reason that a plan-listed warm candidate was not reusable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreparedArtifactRejection {
    /// No entry exists for the exact owner-derived identity.
    Missing,
    /// The entry was incomplete or had unknown inventory.
    Incomplete,
    /// The schema, manifest, identity, or layout was incompatible.
    Incompatible,
    /// Payload bytes or their integrity digests were corrupt.
    Corrupt,
    /// A floating-point payload contained NaN or infinity.
    NonFinite,
}

impl PreparedArtifactRejection {
    #[must_use]
    pub(crate) fn evidence_identity(self, planned: ArtifactIdentity) -> ArtifactIdentity {
        let mut hasher = Sha256::new();
        hasher.update(REJECTION_EVIDENCE_DOMAIN);
        hasher.update(IDENTITY_VERSION.to_le_bytes());
        hasher.update(planned.as_bytes());
        hasher.update([self.tag()]);
        ArtifactIdentity::from_owner_digest(hasher.finalize().into())
    }

    /// Recover a typed rejection from durable receipt evidence.
    #[must_use]
    pub fn from_evidence_identity(
        planned: ArtifactIdentity,
        evidence: ArtifactIdentity,
    ) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|rejection| rejection.evidence_identity(planned) == evidence)
    }

    const ALL: [Self; 5] = [
        Self::Missing,
        Self::Incomplete,
        Self::Incompatible,
        Self::Corrupt,
        Self::NonFinite,
    ];

    const fn tag(self) -> u8 {
        match self {
            Self::Missing => 0,
            Self::Incomplete => 1,
            Self::Incompatible => 2,
            Self::Corrupt => 3,
            Self::NonFinite => 4,
        }
    }
}

/// Plan-executed result of exact warm-cache inspection and reuse.
#[derive(Debug)]
pub enum PreparedArtifactReuseOutcome {
    /// A complete exact hit passed integrity validation.
    Reused(PreparedArtifact),
    /// The candidate failed closed and was not exposed for use.
    Rejected(PreparedArtifactRejection),
}

/// Hard private-cache and streaming-buffer bounds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PreparedArtifactBudget {
    cache_bytes: u64,
    entries: usize,
    streaming_buffer_bytes: u64,
}

impl PreparedArtifactBudget {
    /// Require positive cache, entry-count, and buffer ceilings.
    pub fn new(
        cache_bytes: u64,
        entries: usize,
        streaming_buffer_bytes: u64,
    ) -> Result<Self, PreparedArtifactError> {
        if cache_bytes == 0 || entries == 0 || streaming_buffer_bytes == 0 {
            return Err(PreparedArtifactError::InvalidBudget);
        }
        Ok(Self {
            cache_bytes,
            entries,
            streaming_buffer_bytes,
        })
    }

    /// Return the persistent private-cache byte ceiling.
    #[must_use]
    pub const fn cache_bytes(self) -> u64 {
        self.cache_bytes
    }

    /// Return the persistent entry-count ceiling.
    #[must_use]
    pub const fn entries(self) -> usize {
        self.entries
    }

    /// Return the resident streaming-buffer ceiling.
    #[must_use]
    pub const fn streaming_buffer_bytes(self) -> u64 {
        self.streaming_buffer_bytes
    }
}

/// Exact resource and storage bounds that planning must reserve.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PreparedArtifactReservation {
    persistent_cache_bytes: u64,
    entry_bytes: u64,
    temporary_staging_bytes: u64,
    source_read_bytes: u64,
    file_descriptors: u64,
    source_descriptor_bytes: u64,
    streaming_buffer_bytes: u64,
    resident_buffer_bytes: u64,
}

impl PreparedArtifactReservation {
    /// Return the persistent-cache storage claim.
    #[must_use]
    pub const fn persistent_cache_bytes(self) -> u64 {
        self.persistent_cache_bytes
    }

    /// Return the exact payload-plus-manifest entry reservation.
    #[must_use]
    pub const fn entry_bytes(self) -> u64 {
        self.entry_bytes
    }

    /// Return the same-filesystem private staging claim.
    #[must_use]
    pub const fn temporary_staging_bytes(self) -> u64 {
        self.temporary_staging_bytes
    }

    /// Return the hard source-file byte-read ceiling.
    ///
    /// Load reserves the exact payload bytes plus one bounded oversize probe
    /// per named segment. Generation and reuse have no external source read.
    #[must_use]
    pub const fn source_read_bytes(self) -> u64 {
        self.source_read_bytes
    }

    /// Return the peak file-descriptor claim for the complete operation.
    ///
    /// Load holds the private-store lock, staged payload, and one sequential
    /// source file. Generation and reuse need only the lock and staged payload.
    #[must_use]
    pub const fn file_descriptors(self) -> u64 {
        self.file_descriptors
    }

    /// Return the source-descriptor component of the resident-memory claim.
    #[must_use]
    pub const fn source_descriptor_bytes(self) -> u64 {
        self.source_descriptor_bytes
    }

    /// Return the streaming-buffer component of the resident-buffer claim.
    #[must_use]
    pub const fn streaming_buffer_bytes(self) -> u64 {
        self.streaming_buffer_bytes
    }

    /// Return the bounded resident validation/generation buffer claim.
    #[must_use]
    pub const fn resident_buffer_bytes(self) -> u64 {
        self.resident_buffer_bytes
    }
}

/// One content-committed private regular-file segment of a cold-load source.
///
/// The path is only an execution-local locator. The segment digest contributes
/// to a source identity that must be owned by a predecessor plan node, and the
/// store verifies the bytes while the load node's lease is live.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedArtifactSourceSegment {
    name: Box<str>,
    source: Box<Path>,
    sha256: [u8; 32],
    storage_domain: StorageDomainId,
    storage_operations_rate: Option<RateResourceId>,
    storage_root: Box<Path>,
    storage_root_identity: [u8; 32],
}

impl PreparedArtifactSourceSegment {
    /// Maximum encoded bytes in one execution-local source path.
    ///
    /// This capability bound is independent of the host's path limit and lets
    /// planning reserve source-descriptor residency before the file exists.
    pub const MAX_PATH_BYTES: usize = MAX_SOURCE_PATH_BYTES;

    /// Bind one absolute, bounded locator and expected immutable content digest.
    pub fn new(
        name: impl Into<String>,
        source: impl Into<PathBuf>,
        sha256: [u8; 32],
        storage_domain: &StorageDomain,
    ) -> Result<Self, PreparedArtifactError> {
        let name = name.into();
        let source = source.into();
        let domain_root = storage_domain
            .root
            .canonicalize()
            .map_err(|_| PreparedArtifactError::InvalidSource)?;
        let source = match source.canonicalize() {
            Ok(source) => source,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let parent = source
                    .parent()
                    .ok_or(PreparedArtifactError::InvalidSource)?
                    .canonicalize()
                    .map_err(|_| PreparedArtifactError::InvalidSource)?;
                let file_name = source
                    .file_name()
                    .ok_or(PreparedArtifactError::InvalidSource)?;
                parent.join(file_name)
            }
            Err(_) => return Err(PreparedArtifactError::InvalidSource),
        };
        if !valid_segment_name(&name)
            || !source.is_absolute()
            || !source.starts_with(&domain_root)
            || !valid_identifier(storage_domain.id.as_str())
            || domain_root.as_os_str().as_encoded_bytes().len() > Self::MAX_PATH_BYTES
            || source.as_os_str().as_encoded_bytes().len() > Self::MAX_PATH_BYTES
        {
            return Err(PreparedArtifactError::InvalidSource);
        }
        Ok(Self {
            name: name.into_boxed_str(),
            source: source.into_boxed_path(),
            sha256,
            storage_domain: storage_domain.id.clone(),
            storage_operations_rate: storage_domain.operations_rate.clone(),
            storage_root: domain_root.clone().into_boxed_path(),
            storage_root_identity: derive_cache_root_identity(&domain_root),
        })
    }

    fn storage_demand_id(&self, source_identity: ArtifactIdentity) -> String {
        let base = format!(
            "private-prepared-source-{source_identity}-{}-{}",
            self.storage_domain.as_str(),
            encode_hex(&self.storage_root_identity)
        );
        self.storage_operations_rate
            .as_ref()
            .map_or(base.clone(), |rate| {
                format!("{base}-operations-{}", rate.as_str())
            })
    }
}

/// Exact content source owned by a declared predecessor node for cold load.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedArtifactLoadSource {
    identity: ArtifactIdentity,
    producer: WorkNodeId,
    segments: Vec<PreparedArtifactSourceSegment>,
}

impl PreparedArtifactLoadSource {
    /// Bind canonical segment commitments and their execution-local locators
    /// to the node that accounts for producing or importing those bytes.
    pub fn new(
        descriptor: &PreparedArtifactDescriptor,
        producer: WorkNodeId,
        segments: Vec<PreparedArtifactSourceSegment>,
    ) -> Result<Self, PreparedArtifactError> {
        validate_source_segments(descriptor, &segments)?;
        let identity = derive_load_source_identity(descriptor, &segments)?;
        Ok(Self {
            identity,
            producer,
            segments,
        })
    }

    /// Return the content identity that planning and receipts must retain.
    #[must_use]
    pub const fn identity(&self) -> ArtifactIdentity {
        self.identity
    }

    /// Return the exact node that owns the source artifact evidence.
    #[must_use]
    pub const fn producer(&self) -> &WorkNodeId {
        &self.producer
    }

    /// Declare this immutable source as an input owned by its producer/import node.
    #[must_use]
    pub fn planned_artifact(&self) -> PlannedArtifact {
        PlannedArtifact::new(
            self.identity,
            self.producer.clone(),
            ArtifactRole::Input,
            None,
        )
    }

    fn storage_demands(&self) -> BTreeMap<String, StorageDomainId> {
        self.segments
            .iter()
            .map(|segment| {
                (
                    segment.storage_demand_id(self.identity),
                    segment.storage_domain.clone(),
                )
            })
            .collect()
    }
}

/// One validated implementation-owned source segment translated during a cold load.
///
/// Unlike [`PreparedArtifactSourceSegment`], this locator may name a structured
/// directory such as a CASA image table. The generic store never opens it. It
/// binds the adapter-owned source to a storage domain and exact logical I/O
/// ceilings so translation cannot hide source reads inside generation work.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedArtifactImportSegment {
    name: Box<str>,
    source: Box<Path>,
    source_identity: ArtifactIdentity,
    source_bytes: u64,
    source_operations: u64,
    storage_domain: StorageDomainId,
    storage_operations_rate: Option<crate::RateResourceId>,
    storage_root: Box<Path>,
    storage_root_identity: [u8; 32],
    source_device: u64,
    source_inode: u64,
}

impl PreparedArtifactImportSegment {
    /// Bind one canonical structured source and its exact logical read ceiling.
    pub fn new(
        name: impl Into<String>,
        source: impl Into<PathBuf>,
        source_identity: ArtifactIdentity,
        source_bytes: u64,
        source_operations: u64,
        storage_domain: &StorageDomain,
    ) -> Result<Self, PreparedArtifactError> {
        let name = name.into();
        let source = source
            .into()
            .canonicalize()
            .map_err(|_| PreparedArtifactError::InvalidSource)?;
        let storage_root = storage_domain
            .root
            .canonicalize()
            .map_err(|_| PreparedArtifactError::InvalidSource)?;
        let metadata = source
            .metadata()
            .map_err(|_| PreparedArtifactError::InvalidSource)?;
        if !valid_segment_name(&name)
            || !source.is_absolute()
            || !source.starts_with(&storage_root)
            || !metadata.file_type().is_dir()
            || source_bytes == 0
            || source_operations == 0
            || !valid_identifier(storage_domain.id.as_str())
            || storage_root.as_os_str().as_encoded_bytes().len() > MAX_SOURCE_PATH_BYTES
            || source.as_os_str().as_encoded_bytes().len() > MAX_SOURCE_PATH_BYTES
        {
            return Err(PreparedArtifactError::InvalidSource);
        }
        Ok(Self {
            name: name.into_boxed_str(),
            source: source.into_boxed_path(),
            source_identity,
            source_bytes,
            source_operations,
            storage_domain: storage_domain.id.clone(),
            storage_operations_rate: storage_domain.operations_rate.clone(),
            storage_root_identity: derive_cache_root_identity(&storage_root),
            storage_root: storage_root.into_boxed_path(),
            source_device: metadata.dev(),
            source_inode: metadata.ino(),
        })
    }

    fn storage_demand_id(&self, source_identity: ArtifactIdentity) -> String {
        let base = format!(
            "private-prepared-import-{source_identity}-{}-{}",
            self.storage_domain.as_str(),
            encode_hex(&self.storage_root_identity)
        );
        self.storage_operations_rate
            .as_ref()
            .map_or(base.clone(), |rate| {
                format!("{base}-operations-{}", rate.as_str())
            })
    }
}

/// Exact structured source owned by a declared predecessor node for cold import.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedArtifactImportSource {
    identity: ArtifactIdentity,
    producer: WorkNodeId,
    segments: Vec<PreparedArtifactImportSegment>,
}

impl PreparedArtifactImportSource {
    /// Bind validated adapter sources to one prepared descriptor and producer.
    pub fn new(
        descriptor: &PreparedArtifactDescriptor,
        producer: WorkNodeId,
        segments: Vec<PreparedArtifactImportSegment>,
    ) -> Result<Self, PreparedArtifactError> {
        validate_import_segments(descriptor, &segments)?;
        let identity = derive_import_source_identity(descriptor, &segments)?;
        Ok(Self {
            identity,
            producer,
            segments,
        })
    }

    /// Return the source identity retained by planning and receipts.
    #[must_use]
    pub const fn identity(&self) -> ArtifactIdentity {
        self.identity
    }

    /// Return the node that owns the validated source evidence.
    #[must_use]
    pub const fn producer(&self) -> &WorkNodeId {
        &self.producer
    }

    /// Return the exact logical source bytes translated by this import.
    #[must_use]
    pub fn source_read_bytes(&self) -> u64 {
        self.segments
            .iter()
            .map(|segment| segment.source_bytes)
            .sum()
    }

    /// Declare this validated structured source as a predecessor-owned input.
    #[must_use]
    pub fn planned_artifact(&self) -> PlannedArtifact {
        PlannedArtifact::new(
            self.identity,
            self.producer.clone(),
            ArtifactRole::Input,
            None,
        )
    }

    fn source_operations(&self) -> Result<u64, PreparedArtifactError> {
        self.segments.iter().try_fold(0_u64, |total, segment| {
            total
                .checked_add(segment.source_operations)
                .ok_or(PreparedArtifactError::ArtifactTooLarge)
        })
    }

    fn storage_demands(&self) -> BTreeMap<String, StorageDomainId> {
        self.segments
            .iter()
            .map(|segment| {
                (
                    segment.storage_demand_id(self.identity),
                    segment.storage_domain.clone(),
                )
            })
            .collect()
    }
}

#[derive(Clone, Copy)]
enum PreparedArtifactSourceBinding<'a> {
    Files(&'a PreparedArtifactLoadSource),
    Import(&'a PreparedArtifactImportSource),
}

impl<'a> PreparedArtifactSourceBinding<'a> {
    const fn identity(self) -> ArtifactIdentity {
        match self {
            Self::Files(source) => source.identity,
            Self::Import(source) => source.identity,
        }
    }

    fn producer(self) -> &'a WorkNodeId {
        match self {
            Self::Files(source) => &source.producer,
            Self::Import(source) => &source.producer,
        }
    }

    fn planned_artifact(self) -> PlannedArtifact {
        match self {
            Self::Files(source) => source.planned_artifact(),
            Self::Import(source) => source.planned_artifact(),
        }
    }

    fn storage_demands(self) -> BTreeMap<String, StorageDomainId> {
        match self {
            Self::Files(source) => source.storage_demands(),
            Self::Import(source) => source.storage_demands(),
        }
    }

    fn import_operations(self) -> Result<u64, PreparedArtifactError> {
        match self {
            Self::Files(_) => Ok(0),
            Self::Import(source) => source.source_operations(),
        }
    }
}

/// Plan-selected generator for one immutable prepared artifact.
///
/// The store owns the only bounded output buffer and calls this interface from
/// the declared generation node. Implementations fill exact byte ranges and do
/// not supply path-based source authority.
pub trait PreparedArtifactGenerator {
    /// Fill one exact canonical segment byte range.
    fn fill_segment(
        &mut self,
        segment: &PreparedArtifactSegmentDescriptor,
        byte_offset: u64,
        output: &mut [u8],
    ) -> Result<(), PreparedArtifactError>;
}

/// Plan-bound translator for one validated structured cold-load source.
///
/// Implementations fill the store-owned bounded output buffer and return the
/// exact number of logical source-storage operations performed for that call.
/// The translated source byte count is exactly the output slice length.
pub trait PreparedArtifactImporter {
    /// Fill one canonical segment range and report source operations performed.
    fn fill_segment(
        &mut self,
        segment: &PreparedArtifactSegmentDescriptor,
        byte_offset: u64,
        output: &mut [u8],
    ) -> Result<u64, PreparedArtifactError>;
}

/// Plan-bound streaming consumer for one validated private prepared artifact.
///
/// Chunks are delivered in canonical segment order and never exceed the
/// store's configured streaming-buffer ceiling. Implementations may retain a
/// bounded decoded cell, but receive no cache path or persistence authority.
pub trait PreparedArtifactConsumer {
    /// Consume one exact byte chunk from a named prepared segment.
    fn consume_segment(
        &mut self,
        segment: &PreparedArtifactSegmentDescriptor,
        byte_offset: u64,
        input: &[u8],
    ) -> Result<(), PreparedArtifactError>;
}

impl<F> PreparedArtifactConsumer for F
where
    F: FnMut(&PreparedArtifactSegmentDescriptor, u64, &[u8]) -> Result<(), PreparedArtifactError>,
{
    fn consume_segment(
        &mut self,
        segment: &PreparedArtifactSegmentDescriptor,
        byte_offset: u64,
        input: &[u8],
    ) -> Result<(), PreparedArtifactError> {
        self(segment, byte_offset, input)
    }
}

impl<F> PreparedArtifactGenerator for F
where
    F: FnMut(
        &PreparedArtifactSegmentDescriptor,
        u64,
        &mut [u8],
    ) -> Result<(), PreparedArtifactError>,
{
    fn fill_segment(
        &mut self,
        segment: &PreparedArtifactSegmentDescriptor,
        byte_offset: u64,
        output: &mut [u8],
    ) -> Result<(), PreparedArtifactError> {
        self(segment, byte_offset, output)
    }
}

enum PreparedArtifactMaterialization<'a> {
    Generate(&'a mut dyn PreparedArtifactGenerator),
    Load(&'a PreparedArtifactLoadSource),
    Import {
        source: &'a PreparedArtifactImportSource,
        importer: &'a mut dyn PreparedArtifactImporter,
    },
}

/// Validated immutable identity of one private prepared artifact.
///
/// This value deliberately exposes no payload reader. T50 validates and
/// persists prepared bytes, but a later consumer must be a separate
/// plan-listed operation with its own execution context and receipt evidence.
/// Keeping the payload inaccessible prevents consumer I/O from escaping the
/// cache node's lease and finalized measurements.
pub struct PreparedArtifact {
    identity: ArtifactIdentity,
    integrity_identity: ArtifactIdentity,
    cache_identity: CacheIdentity,
}

impl fmt::Debug for PreparedArtifact {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedArtifact")
            .field("identity", &self.identity)
            .field("cache_identity", &self.cache_identity)
            .finish_non_exhaustive()
    }
}

impl PreparedArtifact {
    /// Return the exact canonical artifact identity.
    #[must_use]
    pub const fn identity(&self) -> ArtifactIdentity {
        self.identity
    }

    /// Return the domain-separated identity of the validated payload bytes.
    #[must_use]
    pub const fn integrity_identity(&self) -> ArtifactIdentity {
        self.integrity_identity
    }

    /// Return the exact owner-derived cache identity.
    #[must_use]
    pub const fn cache_identity(&self) -> CacheIdentity {
        self.cache_identity
    }
}

/// Cross-process locked private cache for immutable prepared artifacts.
#[derive(Debug)]
pub struct PreparedArtifactStore {
    root: PathBuf,
    cache: PathBuf,
    lock_path: PathBuf,
    budget: PreparedArtifactBudget,
    scope: CacheScope,
    storage_domain: StorageDomainId,
    storage_operations_rate: Option<RateResourceId>,
    state: Arc<RootState>,
    #[cfg(test)]
    fail_after_evictions: Option<usize>,
    #[cfg(test)]
    fail_after_publication_rename: bool,
}

#[derive(Debug)]
struct RootState {
    access: Mutex<RootAccessState>,
    readers_released: Condvar,
}

#[derive(Debug, Default)]
struct RootAccessState {
    active_readers: usize,
}

static ROOT_STATES: OnceLock<Mutex<BTreeMap<PathBuf, Weak<RootState>>>> = OnceLock::new();

struct StoreLock<'a> {
    _in_process: MutexGuard<'a, RootAccessState>,
    file: File,
    locked: bool,
}

struct ReaderStoreLock {
    state: Arc<RootState>,
    file: File,
    locked: bool,
}

impl StoreLock<'_> {
    fn release(&mut self, evidence: &mut ValidationEvidence) -> Result<(), PreparedArtifactError> {
        evidence.store_control_operation();
        FileExt::unlock(&self.file)?;
        self.locked = false;
        Ok(())
    }
}

impl Drop for StoreLock<'_> {
    fn drop(&mut self) {
        if self.locked {
            let _ = FileExt::unlock(&self.file);
        }
    }
}

impl ReaderStoreLock {
    fn release(&mut self, evidence: &mut ValidationEvidence) -> Result<(), PreparedArtifactError> {
        if !self.locked {
            return Ok(());
        }
        let mut access = self
            .state
            .access
            .lock()
            .map_err(|_| PreparedArtifactError::PoisonedStore)?;
        evidence.store_control_operation();
        FileExt::unlock(&self.file)?;
        access.active_readers = access
            .active_readers
            .checked_sub(1)
            .ok_or(PreparedArtifactError::PoisonedStore)?;
        self.locked = false;
        if access.active_readers == 0 {
            self.state.readers_released.notify_all();
        }
        Ok(())
    }
}

impl Drop for ReaderStoreLock {
    fn drop(&mut self) {
        if !self.locked {
            return;
        }
        let _ = FileExt::unlock(&self.file);
        if let Ok(mut access) = self.state.access.lock() {
            access.active_readers = access.active_readers.saturating_sub(1);
            if access.active_readers == 0 {
                self.state.readers_released.notify_all();
            }
        }
        self.locked = false;
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactManifest {
    schema: String,
    schema_version: u32,
    identity: String,
    cache_identity: String,
    descriptor: ManifestDescriptor,
    payload_sha256: String,
    payload_bytes: u64,
    segments: Vec<ManifestSegment>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestDescriptor {
    implementation_registry: String,
    provider_catalog: String,
    provider: String,
    provider_version: String,
    implementation: String,
    kind: PreparedArtifactKind,
    scientific: ScientificCommitments,
    cache_scope: CacheScope,
}

impl ManifestDescriptor {
    fn from_descriptor(descriptor: &PreparedArtifactDescriptor) -> Self {
        Self {
            implementation_registry: descriptor.owner.implementation_registry.to_string(),
            provider_catalog: descriptor.owner.registration.provider_catalog.clone(),
            provider: descriptor.owner.registration.provider.clone(),
            provider_version: descriptor.owner.registration.provider_version.clone(),
            implementation: descriptor
                .owner
                .registration
                .implementation
                .as_str()
                .to_string(),
            kind: descriptor.kind,
            scientific: descriptor.scientific.clone(),
            cache_scope: descriptor.cache_scope.clone(),
        }
    }

    fn into_descriptor(
        self,
        segments: Vec<PreparedArtifactSegmentDescriptor>,
    ) -> Result<PreparedArtifactDescriptor, PreparedArtifactError> {
        let registry = decode_digest(&self.implementation_registry)
            .map(ImplementationRegistryId::from_sha256)
            .ok_or(PreparedArtifactError::InvalidManifest)?;
        let registration = PreparedArtifactRegistration::new(
            self.provider_catalog,
            self.provider,
            self.provider_version,
            WorkImplementationId::new(self.implementation),
        )?;
        let owner = PreparedArtifactOwner::from_manifest(registry, registration);
        PreparedArtifactDescriptor::from_commitments(
            owner,
            self.kind,
            self.scientific,
            self.cache_scope,
            segments,
        )
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestSegment {
    descriptor: PreparedArtifactSegmentDescriptor,
    offset: u64,
    bytes: u64,
    sha256: String,
}

struct ManifestSegmentIntegrity {
    offset: u64,
    bytes: u64,
    sha256: String,
}

struct ValidatedArtifact {
    payload_sha256: [u8; 32],
    payload_bytes: u64,
    disk_bytes: u64,
    path: PathBuf,
}

#[derive(Clone, Debug)]
struct MaterializedArtifactEvidence {
    payload_sha256: [u8; 32],
    payload_bytes: u64,
    path: PathBuf,
    disposition: ArtifactDisposition,
}

enum ReuseEvaluation {
    Reused {
        validated: ValidatedArtifact,
        cache_bytes: u64,
    },
    Rejected {
        rejection: PreparedArtifactRejection,
        path: PathBuf,
        cache_bytes: u64,
    },
}

impl ValidatedArtifact {
    fn into_handle(self, descriptor: &PreparedArtifactDescriptor) -> PreparedArtifact {
        PreparedArtifact {
            identity: descriptor.identity,
            integrity_identity: derive_content_identity(descriptor, self.payload_sha256),
            cache_identity: descriptor.cache_identity(),
        }
    }
}

/// Typed fail-closed prepared-artifact error.
#[derive(Debug)]
pub enum PreparedArtifactError {
    /// A plan-bound store operation failed after producing receipt evidence.
    Execution {
        /// Underlying typed store failure.
        source: Box<PreparedArtifactError>,
        /// Completed resource, I/O, and mutation evidence.
        measurements: WorkMeasurements,
    },
    /// Private-cache or payload I/O failed.
    Io(io::Error),
    /// The private manifest could not be encoded or decoded.
    Json(serde_json::Error),
    /// One hard cache bound was zero.
    InvalidBudget,
    /// Owner identifiers were absent or invalid.
    InvalidOwner,
    /// A CF-cell scientific coordinate or normalization key was invalid.
    InvalidCellKey,
    /// The scientific key did not match the prepared-artifact kind.
    InvalidScientificKey,
    /// A descriptor omitted or duplicated an identity-bearing semantic.
    InvalidDescriptor,
    /// Shape, support, sampling, precision, order, or segment name was invalid.
    InvalidLayout,
    /// A UU/VV coordinate was nonfinite, singular, or degenerate.
    InvalidUvAffine,
    /// Shape or byte accounting overflowed.
    ArtifactTooLarge,
    /// The exact entry cannot fit the configured persistent budget.
    CacheBudgetExceeded {
        /// Required bytes.
        required: u64,
        /// Configured hard ceiling.
        budget: u64,
    },
    /// Published entries exceed the policy committed into the cache identity.
    CacheEntryBudgetExceeded {
        /// Observed complete entry count.
        required: usize,
        /// Configured hard entry-count ceiling.
        budget: usize,
    },
    /// The exact streaming buffer cannot hold one scalar.
    StreamingBufferTooSmall {
        /// Required bytes.
        required: u64,
        /// Configured hard ceiling.
        budget: u64,
    },
    /// Actual concurrent private-store residency exceeded the planned claim.
    ResidentBudgetExceeded {
        /// Uncensored observed concurrent peak.
        required: u64,
        /// Planned resident-buffer ceiling.
        budget: u64,
    },
    /// Private manifest metadata exceeded its conservative reservation.
    ManifestReservationExceeded {
        /// Actual staged bytes.
        actual: u64,
        /// Planned staged bytes.
        reserved: u64,
    },
    /// The node or canonical planned artifact did not bind this operation.
    UnplannedOperation,
    /// The descriptor did not come from the compiled problem executing the node.
    ScientificBindingMismatch,
    /// The descriptor and store have different root or hard policy commitments.
    CachePolicyMismatch,
    /// The descriptor owner was not selected by the running plan registry.
    ImplementationRegistryMismatch,
    /// The cache node omitted an exact resource, storage, lock, or I/O reservation.
    MissingReservation(&'static str),
    /// The node attempted to combine prepared-cache work with product publication.
    ProductAuthorityViolation,
    /// Named sources did not exactly match the canonical descriptor order.
    SegmentMismatch,
    /// Cold-load source identity was absent from the complete plan.
    UnplannedSource,
    /// Cold-load source ownership was not an exact predecessor of the load node.
    SourceProducerMismatch,
    /// Cold-load source bytes disagreed with their plan-listed content commitment.
    SourceIdentityMismatch,
    /// A structured importer disagreed with its plan-listed source I/O.
    InvalidSourceMeasurement,
    /// A source descriptor was relative, unbounded, non-regular, or cache-owned.
    InvalidSource,
    /// A stream or published entry ended before its exact size.
    IncompleteArtifact,
    /// A stream or published entry exceeded its exact size.
    OversizedArtifact,
    /// Floating-point payload contained NaN or infinity.
    NonFiniteValue {
        /// Segment containing the value.
        segment: String,
        /// Scalar index within that segment.
        scalar: u64,
    },
    /// The private schema name or version is unsupported.
    UnknownSchema {
        /// Observed schema name.
        schema: String,
        /// Observed schema version.
        version: u32,
    },
    /// Private manifest structure was invalid.
    InvalidManifest,
    /// Directory, manifest, cache, or re-derived artifact identity differed.
    IdentityMismatch,
    /// A published artifact no longer matches the requested descriptor.
    StaleArtifact,
    /// Manifest segment offsets, sizes, or descriptors disagreed.
    SegmentLayoutMismatch,
    /// Payload byte counts or digests failed integrity validation.
    CorruptArtifact,
    /// The same owner-derived identity produced different bytes.
    PublicationConflict,
    /// A non-staging path did not belong to the private cache schema.
    UnknownCacheEntry(PathBuf),
    /// A configured cache or source path belongs to CASA-visible persistence.
    CasaVisiblePath(PathBuf),
    /// A reader was used before its plan-bound Cache node activated it.
    ReaderInactive,
    /// A reader session was activated more than once.
    ReaderAlreadyActivated,
    /// A reader was used after close, abort, or terminal release.
    ReaderClosed,
    /// A reader call did not match its exact plan, attempt, or lease.
    ReaderBindingMismatch,
    /// A requested artifact is absent from the reader's sealed catalog.
    ReaderArtifactMissing,
    /// Decoded-cell pins or loads were still live at the terminal fence.
    ReaderStillInUse,
    /// An in-process cache lock was poisoned by a prior panic.
    PoisonedStore,
}

impl fmt::Display for PreparedArtifactError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Execution { source, .. } => write!(formatter, "{source}"),
            Self::Io(error) => write!(formatter, "prepared-artifact I/O failed: {error}"),
            Self::Json(error) => write!(formatter, "prepared-artifact manifest failed: {error}"),
            Self::InvalidBudget => formatter.write_str("prepared-artifact budget must be positive"),
            Self::InvalidOwner => formatter.write_str("prepared-artifact owner is invalid"),
            Self::InvalidCellKey => formatter.write_str("prepared-artifact cell key is invalid"),
            Self::InvalidScientificKey => {
                formatter.write_str("prepared-artifact scientific key is invalid")
            }
            Self::InvalidDescriptor => {
                formatter.write_str("prepared-artifact descriptor is invalid")
            }
            Self::InvalidLayout => {
                formatter.write_str("prepared-artifact segment layout is invalid")
            }
            Self::InvalidUvAffine => formatter.write_str("prepared-artifact UV affine is invalid"),
            Self::ArtifactTooLarge => formatter.write_str("prepared-artifact size overflowed"),
            Self::CacheBudgetExceeded { required, budget } => write!(
                formatter,
                "prepared artifact requires {required} cache bytes but budget is {budget}"
            ),
            Self::CacheEntryBudgetExceeded { required, budget } => write!(
                formatter,
                "prepared artifact cache contains {required} entries but budget is {budget}"
            ),
            Self::StreamingBufferTooSmall { required, budget } => write!(
                formatter,
                "prepared artifact requires a {required}-byte scalar buffer but budget is {budget}"
            ),
            Self::ResidentBudgetExceeded { required, budget } => write!(
                formatter,
                "prepared artifact reached {required} resident bytes but planned {budget}"
            ),
            Self::ManifestReservationExceeded { actual, reserved } => write!(
                formatter,
                "prepared artifact staged {actual} bytes but reserved {reserved}"
            ),
            Self::UnplannedOperation => formatter.write_str(
                "prepared-artifact operation is not bound by the canonical execution plan",
            ),
            Self::ScientificBindingMismatch => formatter.write_str(
                "prepared-artifact scientific commitments do not match the compiled problem",
            ),
            Self::CachePolicyMismatch => formatter.write_str(
                "prepared-artifact descriptor does not match the store root and cache policy",
            ),
            Self::ImplementationRegistryMismatch => formatter.write_str(
                "prepared-artifact owner does not match the running implementation registry",
            ),
            Self::MissingReservation(kind) => {
                write!(
                    formatter,
                    "prepared-artifact node lacks its {kind} reservation"
                )
            }
            Self::ProductAuthorityViolation => formatter
                .write_str("prepared-artifact cache work cannot own Product Graph publication"),
            Self::SegmentMismatch => formatter
                .write_str("prepared-artifact sources do not match the canonical named segments"),
            Self::UnplannedSource => formatter.write_str(
                "prepared-artifact cold-load source is not listed in the execution plan",
            ),
            Self::SourceProducerMismatch => formatter.write_str(
                "prepared-artifact cold-load source is not owned by an exact predecessor node",
            ),
            Self::SourceIdentityMismatch => formatter.write_str(
                "prepared-artifact cold-load source bytes do not match the planned identity",
            ),
            Self::InvalidSourceMeasurement => formatter.write_str(
                "prepared-artifact importer disagrees with its plan-listed source I/O",
            ),
            Self::InvalidSource => formatter.write_str(
                "prepared-artifact source must be a bounded absolute validated path outside the private cache",
            ),
            Self::IncompleteArtifact => formatter.write_str("prepared artifact is incomplete"),
            Self::OversizedArtifact => {
                formatter.write_str("prepared artifact exceeds its exact planned size")
            }
            Self::NonFiniteValue { segment, scalar } => write!(
                formatter,
                "prepared-artifact segment {segment:?} contains a nonfinite scalar at {scalar}"
            ),
            Self::UnknownSchema { schema, version } => write!(
                formatter,
                "unknown prepared-artifact schema {schema:?} version {version}"
            ),
            Self::InvalidManifest => formatter.write_str("prepared-artifact manifest is invalid"),
            Self::IdentityMismatch => formatter.write_str(
                "prepared-artifact identity does not match its owner-derived descriptor",
            ),
            Self::StaleArtifact => formatter
                .write_str("prepared-artifact compatibility descriptor is stale or mismatched"),
            Self::SegmentLayoutMismatch => formatter
                .write_str("prepared-artifact named segment layout does not match its descriptor"),
            Self::CorruptArtifact => {
                formatter.write_str("prepared-artifact integrity validation failed")
            }
            Self::PublicationConflict => formatter
                .write_str("owner-derived prepared-artifact identity produced conflicting bytes"),
            Self::UnknownCacheEntry(path) => {
                write!(
                    formatter,
                    "unknown prepared-artifact cache entry {}",
                    path.display()
                )
            }
            Self::CasaVisiblePath(path) => write!(
                formatter,
                "prepared-artifact path is CASA-visible: {}",
                path.display()
            ),
            Self::ReaderInactive => {
                formatter.write_str("prepared-artifact reader has not been activated")
            }
            Self::ReaderAlreadyActivated => {
                formatter.write_str("prepared-artifact reader was already activated")
            }
            Self::ReaderClosed => {
                formatter.write_str("prepared-artifact reader is closed")
            }
            Self::ReaderBindingMismatch => formatter
                .write_str("prepared-artifact reader does not match the executing plan binding"),
            Self::ReaderArtifactMissing => formatter
                .write_str("prepared-artifact reader catalog does not contain the requested artifact"),
            Self::ReaderStillInUse => formatter
                .write_str("prepared-artifact reader still has live decoded loads or pins"),
            Self::PoisonedStore => formatter.write_str("prepared-artifact cache lock is poisoned"),
        }
    }
}

impl Error for PreparedArtifactError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Execution { source, .. } => Some(source.as_ref()),
            Self::Io(error) => Some(error),
            Self::Json(error) => Some(error),
            _ => None,
        }
    }
}

impl PreparedArtifactError {
    /// Return completed plan-bound measurements retained by a failed operation.
    #[must_use]
    pub const fn work_measurements(&self) -> Option<&WorkMeasurements> {
        match self {
            Self::Execution { measurements, .. } => Some(measurements),
            _ => None,
        }
    }

    fn with_measurements(self, measurements: WorkMeasurements) -> Self {
        Self::Execution {
            source: Box::new(self),
            measurements,
        }
    }
}

impl From<io::Error> for PreparedArtifactError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<serde_json::Error> for PreparedArtifactError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

fn root_state(root: &Path) -> Result<Arc<RootState>, PreparedArtifactError> {
    let states = ROOT_STATES.get_or_init(|| Mutex::new(BTreeMap::new()));
    let mut states = states
        .lock()
        .map_err(|_| PreparedArtifactError::PoisonedStore)?;
    states.retain(|_, state| state.strong_count() != 0);
    if let Some(state) = states.get(root).and_then(Weak::upgrade) {
        return Ok(state);
    }
    let state = Arc::new(RootState {
        access: Mutex::new(RootAccessState::default()),
        readers_released: Condvar::new(),
    });
    states.insert(root.to_path_buf(), Arc::downgrade(&state));
    Ok(state)
}

fn validate_cf_segments(
    segments: &[PreparedArtifactSegmentDescriptor],
) -> Result<(), PreparedArtifactError> {
    if segments.len() != 2 || segments[0].name != "imaging" || segments[1].name != "weight" {
        return Err(PreparedArtifactError::InvalidDescriptor);
    }
    for segment in segments {
        if segment.shape.len() != 2
            || segment.support.contains(&0)
            || segment.uv_affine.is_none()
            || !segment.precision.is_complex()
        {
            return Err(PreparedArtifactError::InvalidLayout);
        }
    }
    let imaging = &segments[0];
    let weight = &segments[1];
    if imaging.sampling != weight.sampling
        || imaging.uv_affine.as_ref().map(|uv| uv.reference_value_bits)
            != weight.uv_affine.as_ref().map(|uv| uv.reference_value_bits)
        || imaging.uv_affine.as_ref().map(|uv| uv.pc_matrix_bits)
            != weight.uv_affine.as_ref().map(|uv| uv.pc_matrix_bits)
        || !same_uv_world_window(imaging, weight)
    {
        return Err(PreparedArtifactError::InvalidLayout);
    }
    Ok(())
}

fn same_uv_world_window(
    imaging: &PreparedArtifactSegmentDescriptor,
    weight: &PreparedArtifactSegmentDescriptor,
) -> bool {
    let imaging_uv = imaging.uv_affine.as_ref().expect("validated imaging UV");
    let weight_uv = weight.uv_affine.as_ref().expect("validated weight UV");
    (0..2).all(|axis| {
        let imaging_size = imaging.shape[axis] as f64;
        let weight_size = weight.shape[axis] as f64;
        nearly_equal(
            f64::from_bits(imaging_uv.reference_pixel_bits[axis]) / imaging_size,
            f64::from_bits(weight_uv.reference_pixel_bits[axis]) / weight_size,
        ) && nearly_equal(
            f64::from_bits(imaging_uv.increment_bits[axis]) * imaging_size,
            f64::from_bits(weight_uv.increment_bits[axis]) * weight_size,
        )
    })
}

fn nearly_equal(left: f64, right: f64) -> bool {
    let scale = left.abs().max(right.abs()).max(1.0);
    (left - right).abs() <= scale * 32.0 * f64::EPSILON
}

fn derive_cache_identity(
    owner: &PreparedArtifactOwner,
    scope: &CacheScope,
) -> Result<CacheIdentity, PreparedArtifactError> {
    let mut hasher = Sha256::new();
    hasher.update(CACHE_IDENTITY_DOMAIN);
    hasher.update(IDENTITY_VERSION.to_le_bytes());
    hasher.update(CACHE_SCHEMA_VERSION.to_le_bytes());
    hasher.update(owner.implementation_registry.as_bytes());
    hash_bytes(&mut hasher, owner.registration.provider_catalog.as_bytes())?;
    hash_bytes(&mut hasher, owner.registration.provider.as_bytes())?;
    hash_bytes(&mut hasher, owner.registration.provider_version.as_bytes())?;
    hash_bytes(
        &mut hasher,
        owner.registration.implementation.as_str().as_bytes(),
    )?;
    hasher.update(
        decode_digest(&scope.root_identity).ok_or(PreparedArtifactError::InvalidDescriptor)?,
    );
    hash_bytes(&mut hasher, scope.storage_domain.as_bytes())?;
    hasher.update(scope.cache_bytes.to_le_bytes());
    hasher.update(scope.entries.to_le_bytes());
    hasher.update(scope.streaming_buffer_bytes.to_le_bytes());
    hash_bytes(&mut hasher, scope.eviction_policy.as_bytes())?;
    Ok(CacheIdentity::from_owner_digest(hasher.finalize().into()))
}

fn derive_artifact_identity(
    descriptor: &PreparedArtifactDescriptor,
) -> Result<ArtifactIdentity, PreparedArtifactError> {
    let mut hasher = Sha256::new();
    hasher.update(ARTIFACT_IDENTITY_DOMAIN);
    hasher.update(IDENTITY_VERSION.to_le_bytes());
    hasher.update(descriptor.owner.implementation_registry.as_bytes());
    hash_bytes(
        &mut hasher,
        descriptor.owner.registration.provider_catalog.as_bytes(),
    )?;
    hash_bytes(
        &mut hasher,
        descriptor.owner.registration.provider.as_bytes(),
    )?;
    hash_bytes(
        &mut hasher,
        descriptor.owner.registration.provider_version.as_bytes(),
    )?;
    hash_bytes(
        &mut hasher,
        descriptor
            .owner
            .registration
            .implementation
            .as_str()
            .as_bytes(),
    )?;
    hasher.update([kind_tag(descriptor.kind)]);
    for identity in [
        &descriptor.scientific.compiled_problem,
        &descriptor.scientific.observation_snapshot,
        &descriptor.scientific.compiled_geometry,
        &descriptor.scientific.numerics_contract,
    ] {
        hasher.update(decode_digest(identity).ok_or(PreparedArtifactError::InvalidDescriptor)?);
    }
    hasher.update(
        decode_digest(&descriptor.scientific.owner_scientific_identity)
            .ok_or(PreparedArtifactError::InvalidDescriptor)?,
    );
    hash_len(&mut hasher, descriptor.segments.len())?;
    for segment in &descriptor.segments {
        hash_bytes(&mut hasher, segment.name.as_bytes())?;
        hash_u64s(&mut hasher, &segment.shape)?;
        hash_u64s(&mut hasher, &segment.support)?;
        hash_u64s(&mut hasher, &segment.sampling)?;
        match &segment.uv_affine {
            Some(uv) => {
                hasher.update([1]);
                for value in uv.reference_value_bits {
                    hasher.update(value.to_le_bytes());
                }
                for value in uv.reference_pixel_bits {
                    hasher.update(value.to_le_bytes());
                }
                for value in uv.increment_bits {
                    hasher.update(value.to_le_bytes());
                }
                for row in uv.pc_matrix_bits {
                    for value in row {
                        hasher.update(value.to_le_bytes());
                    }
                }
            }
            None => hasher.update([0]),
        }
        hasher.update([precision_tag(segment.precision), order_tag(segment.order)]);
    }
    Ok(ArtifactIdentity::from_owner_digest(
        hasher.finalize().into(),
    ))
}

fn derive_cache_root_identity(root: &Path) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(CACHE_ROOT_IDENTITY_DOMAIN);
    hasher.update(IDENTITY_VERSION.to_le_bytes());
    hasher.update(root.as_os_str().as_encoded_bytes());
    hasher.finalize().into()
}

fn derive_work_node_identity(
    descriptor: &PreparedArtifactDescriptor,
    operation: PreparedArtifactOperation,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(WORK_NODE_IDENTITY_DOMAIN);
    hasher.update(IDENTITY_VERSION.to_le_bytes());
    hasher.update(descriptor.identity.as_bytes());
    hasher.update(descriptor.cache_identity.as_bytes());
    hasher.update([operation_tag(operation)]);
    hasher.finalize().into()
}

fn derive_eviction_ledger_identity(
    descriptor: &PreparedArtifactDescriptor,
    operation: PreparedArtifactOperation,
) -> ArtifactIdentity {
    let mut hasher = Sha256::new();
    hasher.update(EVICTION_LEDGER_DOMAIN);
    hasher.update(IDENTITY_VERSION.to_le_bytes());
    hasher.update(descriptor.identity.as_bytes());
    hasher.update(descriptor.cache_identity.as_bytes());
    hasher.update([operation_tag(operation)]);
    ArtifactIdentity::from_owner_digest(hasher.finalize().into())
}

fn derive_eviction_observed_identity(
    ledger: ArtifactIdentity,
    evictions: &[(ArtifactIdentity, u64)],
) -> ArtifactIdentity {
    let mut hasher = Sha256::new();
    hasher.update(EVICTION_OBSERVED_DOMAIN);
    hasher.update(IDENTITY_VERSION.to_le_bytes());
    hasher.update(ledger.as_bytes());
    hasher.update((evictions.len() as u64).to_le_bytes());
    for (identity, bytes) in evictions {
        hasher.update(identity.as_bytes());
        hasher.update(bytes.to_le_bytes());
    }
    ArtifactIdentity::from_owner_digest(hasher.finalize().into())
}

fn derive_orphan_staging_evidence_identity(
    path: &Path,
    bytes: u64,
) -> Result<ArtifactIdentity, PreparedArtifactError> {
    let name = path
        .file_name()
        .ok_or_else(|| PreparedArtifactError::UnknownCacheEntry(path.to_path_buf()))?;
    let mut hasher = Sha256::new();
    hasher.update(ORPHAN_STAGING_EVIDENCE_DOMAIN);
    hasher.update(IDENTITY_VERSION.to_le_bytes());
    hash_bytes(&mut hasher, name.as_encoded_bytes())?;
    hasher.update(bytes.to_le_bytes());
    Ok(ArtifactIdentity::from_owner_digest(
        hasher.finalize().into(),
    ))
}

fn derive_content_identity(
    descriptor: &PreparedArtifactDescriptor,
    payload_sha256: [u8; 32],
) -> ArtifactIdentity {
    let mut hasher = Sha256::new();
    hasher.update(CONTENT_IDENTITY_DOMAIN);
    hasher.update(IDENTITY_VERSION.to_le_bytes());
    hasher.update(descriptor.identity.as_bytes());
    hasher.update(payload_sha256);
    ArtifactIdentity::from_owner_digest(hasher.finalize().into())
}

fn derive_load_source_identity(
    descriptor: &PreparedArtifactDescriptor,
    segments: &[PreparedArtifactSourceSegment],
) -> Result<ArtifactIdentity, PreparedArtifactError> {
    let mut hasher = Sha256::new();
    hasher.update(LOAD_SOURCE_IDENTITY_DOMAIN);
    hasher.update(IDENTITY_VERSION.to_le_bytes());
    hasher.update(descriptor.identity.as_bytes());
    hash_len(&mut hasher, segments.len())?;
    for segment in segments {
        hash_bytes(&mut hasher, segment.name.as_bytes())?;
        hasher.update(segment.sha256);
        hash_bytes(&mut hasher, segment.storage_domain.as_str().as_bytes())?;
        hasher.update(segment.storage_root_identity);
        if let Some(rate) = &segment.storage_operations_rate {
            hash_bytes(&mut hasher, rate.as_str().as_bytes())?;
        }
    }
    Ok(ArtifactIdentity::from_owner_digest(
        hasher.finalize().into(),
    ))
}

fn derive_import_source_identity(
    descriptor: &PreparedArtifactDescriptor,
    segments: &[PreparedArtifactImportSegment],
) -> Result<ArtifactIdentity, PreparedArtifactError> {
    let mut hasher = Sha256::new();
    hasher.update(IMPORT_SOURCE_IDENTITY_DOMAIN);
    hasher.update(IDENTITY_VERSION.to_le_bytes());
    hasher.update(descriptor.identity.as_bytes());
    hash_len(&mut hasher, segments.len())?;
    for segment in segments {
        hash_bytes(&mut hasher, segment.name.as_bytes())?;
        hasher.update(segment.source_identity.as_bytes());
        hasher.update(segment.source_bytes.to_le_bytes());
        hasher.update(segment.source_operations.to_le_bytes());
        hash_bytes(&mut hasher, segment.storage_domain.as_str().as_bytes())?;
        hasher.update(segment.storage_root_identity);
        if let Some(rate) = &segment.storage_operations_rate {
            hash_bytes(&mut hasher, rate.as_str().as_bytes())?;
        }
        hasher.update(segment.source_device.to_le_bytes());
        hasher.update(segment.source_inode.to_le_bytes());
    }
    Ok(ArtifactIdentity::from_owner_digest(
        hasher.finalize().into(),
    ))
}

struct MeasurementInput {
    operation: PreparedArtifactOperation,
    cache_bytes: u64,
    evidence: ValidationEvidence,
}

fn validate_source_segments(
    descriptor: &PreparedArtifactDescriptor,
    inputs: &[PreparedArtifactSourceSegment],
) -> Result<(), PreparedArtifactError> {
    if inputs.len() != descriptor.segments.len()
        || descriptor
            .segments
            .iter()
            .zip(inputs)
            .any(|(segment, input)| segment.name != input.name.as_ref())
    {
        Err(PreparedArtifactError::SegmentMismatch)
    } else {
        Ok(())
    }
}

fn validate_import_segments(
    descriptor: &PreparedArtifactDescriptor,
    inputs: &[PreparedArtifactImportSegment],
) -> Result<(), PreparedArtifactError> {
    if inputs.len() != descriptor.segments.len() {
        return Err(PreparedArtifactError::SegmentMismatch);
    }
    for (segment, input) in descriptor.segments.iter().zip(inputs) {
        if segment.name != input.name.as_ref() || segment.byte_len()? != input.source_bytes {
            return Err(PreparedArtifactError::SegmentMismatch);
        }
    }
    Ok(())
}

fn generate_segment(
    generator: &mut dyn PreparedArtifactGenerator,
    output: &mut dyn Write,
    payload_hasher: &mut Sha256,
    buffer: &mut [u8],
    segment: &PreparedArtifactSegmentDescriptor,
    evidence: &mut ValidationEvidence,
) -> Result<[u8; 32], PreparedArtifactError> {
    let mut remaining = segment.byte_len()?;
    let scalar_bytes = segment.precision.scalar_bytes();
    let mut byte_offset = 0_u64;
    let mut scalar = 0_u64;
    let mut segment_hasher = Sha256::new();
    while remaining > 0 {
        let mut limit = usize::try_from(remaining.min(buffer.len() as u64))
            .map_err(|_| PreparedArtifactError::ArtifactTooLarge)?;
        limit -= limit % scalar_bytes;
        generator.fill_segment(segment, byte_offset, &mut buffer[..limit])?;
        validate_finite(&buffer[..limit], segment.precision, &segment.name, scalar)?;
        write_all_counted(output, &buffer[..limit], evidence, CacheIoClass::Write)?;
        payload_hasher.update(&buffer[..limit]);
        segment_hasher.update(&buffer[..limit]);
        remaining -= limit as u64;
        byte_offset += limit as u64;
        scalar += (limit / scalar_bytes) as u64;
    }
    Ok(segment_hasher.finalize().into())
}

struct ImportStream<'a> {
    output: &'a mut dyn Write,
    payload_hasher: &'a mut Sha256,
    buffer: &'a mut [u8],
    evidence: &'a mut ValidationEvidence,
}

fn import_segment(
    importer: &mut dyn PreparedArtifactImporter,
    input: &PreparedArtifactImportSegment,
    source_identity: ArtifactIdentity,
    segment: &PreparedArtifactSegmentDescriptor,
    stream: ImportStream<'_>,
) -> Result<[u8; 32], PreparedArtifactError> {
    let ImportStream {
        output,
        payload_hasher,
        buffer,
        evidence,
    } = stream;
    let mut remaining = segment.byte_len()?;
    let scalar_bytes = segment.precision.scalar_bytes();
    let mut byte_offset = 0_u64;
    let mut scalar = 0_u64;
    let mut source_operations = 0_u64;
    let source_demand_id = input.storage_demand_id(source_identity);
    let mut segment_hasher = Sha256::new();
    while remaining > 0 {
        let mut limit = usize::try_from(remaining.min(buffer.len() as u64))
            .map_err(|_| PreparedArtifactError::ArtifactTooLarge)?;
        limit -= limit % scalar_bytes;
        let operations = importer.fill_segment(segment, byte_offset, &mut buffer[..limit])?;
        source_operations = source_operations
            .checked_add(operations)
            .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
        evidence.record_source_bytes(&source_demand_id, limit as u64);
        evidence.record_source_operations(&source_demand_id, operations);
        validate_finite(&buffer[..limit], segment.precision, &segment.name, scalar)?;
        write_all_counted(output, &buffer[..limit], evidence, CacheIoClass::Write)?;
        payload_hasher.update(&buffer[..limit]);
        segment_hasher.update(&buffer[..limit]);
        remaining -= limit as u64;
        byte_offset += limit as u64;
        scalar += (limit / scalar_bytes) as u64;
    }
    if source_operations != input.source_operations {
        return Err(PreparedArtifactError::InvalidSourceMeasurement);
    }
    Ok(segment_hasher.finalize().into())
}

fn streaming_buffer_len(
    budget: PreparedArtifactBudget,
    descriptor: &PreparedArtifactDescriptor,
) -> Result<usize, PreparedArtifactError> {
    let scalar = descriptor
        .segments
        .iter()
        .map(|segment| segment.precision.scalar_bytes())
        .max()
        .unwrap_or(1);
    let ceiling = usize::try_from(
        budget
            .streaming_buffer_bytes
            .min(STREAMING_BUFFER_CEILING as u64),
    )
    .map_err(|_| PreparedArtifactError::ArtifactTooLarge)?;
    let length = ceiling - ceiling % scalar;
    if length == 0 {
        Err(PreparedArtifactError::StreamingBufferTooSmall {
            required: scalar as u64,
            budget: budget.streaming_buffer_bytes,
        })
    } else {
        Ok(length)
    }
}

fn stream_segment(
    input: &mut File,
    output: &mut dyn Write,
    payload_hasher: &mut Sha256,
    buffer: &mut [u8],
    segment: &PreparedArtifactSegmentDescriptor,
    source_demand_id: &str,
    evidence: &mut ValidationEvidence,
) -> Result<[u8; 32], PreparedArtifactError> {
    let mut remaining = segment.byte_len()?;
    let scalar_bytes = segment.precision.scalar_bytes();
    let mut scalar = 0_u64;
    let mut segment_hasher = Sha256::new();
    while remaining > 0 {
        let mut limit = usize::try_from(remaining.min(buffer.len() as u64))
            .map_err(|_| PreparedArtifactError::ArtifactTooLarge)?;
        limit -= limit % scalar_bytes;
        read_exact_source_counted(input, &mut buffer[..limit], evidence, source_demand_id)?;
        validate_finite(&buffer[..limit], segment.precision, &segment.name, scalar)?;
        evidence.source_read_operation(source_demand_id);
        write_all_counted(output, &buffer[..limit], evidence, CacheIoClass::Write)?;
        payload_hasher.update(&buffer[..limit]);
        segment_hasher.update(&buffer[..limit]);
        remaining -= limit as u64;
        scalar += (limit / scalar_bytes) as u64;
    }
    let mut extra = [0_u8; 1];
    if read_source_counted(input, &mut extra, evidence, source_demand_id)? != 0 {
        return Err(PreparedArtifactError::OversizedArtifact);
    }
    Ok(segment_hasher.finalize().into())
}

fn validate_payload(
    payload: &File,
    segments: &[PreparedArtifactSegmentDescriptor],
    integrity: &[ManifestSegmentIntegrity],
    buffer_len: usize,
    evidence: &mut ValidationEvidence,
) -> Result<([u8; 32], u64), PreparedArtifactError> {
    let mut payload = payload;
    let mut payload_hasher = Sha256::new();
    let mut total = 0_u64;
    let mut buffer = vec![0_u8; buffer_len];
    evidence.with_resident(observed_vec_resident_bytes(&buffer), |evidence| {
        for (segment, integrity) in segments.iter().zip(integrity) {
            let expected_segment_digest =
                decode_digest(&integrity.sha256).ok_or(PreparedArtifactError::InvalidManifest)?;
            let mut segment_hasher = Sha256::new();
            let mut remaining = integrity.bytes;
            let scalar_bytes = segment.precision.scalar_bytes();
            let mut scalar = 0_u64;
            while remaining > 0 {
                let mut limit = usize::try_from(remaining.min(buffer.len() as u64))
                    .map_err(|_| PreparedArtifactError::ArtifactTooLarge)?;
                limit -= limit % scalar_bytes;
                read_exact_counted(
                    &mut payload,
                    &mut buffer[..limit],
                    evidence,
                    CacheIoClass::Read,
                )?;
                validate_finite(&buffer[..limit], segment.precision, &segment.name, scalar)?;
                evidence.store_validation();
                payload_hasher.update(&buffer[..limit]);
                segment_hasher.update(&buffer[..limit]);
                remaining -= limit as u64;
                scalar += (limit / scalar_bytes) as u64;
            }
            if <[u8; 32]>::from(segment_hasher.finalize()) != expected_segment_digest {
                return Err(PreparedArtifactError::CorruptArtifact);
            }
            evidence.store_validation();
            total = total
                .checked_add(integrity.bytes)
                .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
        }
        let mut extra = [0_u8; 1];
        if read_counted(&mut payload, &mut extra, evidence, CacheIoClass::Read)? != 0 {
            return Err(PreparedArtifactError::OversizedArtifact);
        }
        Ok((payload_hasher.finalize().into(), total))
    })
}

#[derive(Clone, Copy, Debug, Default)]
struct IoCounter {
    bytes: u64,
    operations: u64,
}

#[derive(Clone, Debug)]
struct SourceIoCounter {
    demand_id: String,
    counter: IoCounter,
}

#[derive(Clone, Copy, Debug)]
enum CacheIoClass {
    Read,
    Control,
    Write,
}

#[derive(Clone, Debug, Default)]
struct ValidationEvidence {
    source_reads: Vec<SourceIoCounter>,
    cache_read: IoCounter,
    cache_control: IoCounter,
    cache_write: IoCounter,
    resident_current_bytes: u64,
    resident_buffer_bytes: u64,
    resident_limit_bytes: Option<u64>,
    cache_bytes_peak: u64,
    temporary_storage_peak: u64,
    locks_peak: u64,
    file_descriptors_peak: u64,
    evictions: Vec<(ArtifactIdentity, u64)>,
    materialized: Option<MaterializedArtifactEvidence>,
    accounting_overflowed: bool,
}

impl ValidationEvidence {
    #[cfg(test)]
    fn with_resident_limit(limit: u64) -> Self {
        Self {
            resident_limit_bytes: Some(limit),
            ..Self::default()
        }
    }

    fn new(budget: PreparedArtifactBudget) -> Self {
        let evictions = Vec::with_capacity(budget.entries);
        let mut evidence = Self {
            evictions,
            ..Self::default()
        };
        evidence.acquire_resident(observed_vec_resident_bytes(&evidence.evictions));
        evidence
    }

    fn for_operation(budget: PreparedArtifactBudget, resident_limit: u64) -> Self {
        let mut evidence = Self::new(budget);
        evidence.resident_limit_bytes = Some(resident_limit);
        evidence
    }

    fn source_read_operation(&mut self, demand_id: &str) {
        self.record_source_operations(demand_id, 1);
    }

    fn store_read_operation(&mut self) {
        self.record(CacheIoClass::Read, 0);
    }

    fn store_control_operation(&mut self) {
        self.record(CacheIoClass::Control, 0);
    }

    fn store_write_operation(&mut self) {
        self.record(CacheIoClass::Write, 0);
    }

    fn store_validation(&mut self) {
        self.record(CacheIoClass::Read, 0);
    }

    fn acquire_resident(&mut self, bytes: u64) {
        self.resident_current_bytes = match self.resident_current_bytes.checked_add(bytes) {
            Some(current) => current,
            None => {
                self.accounting_overflowed = true;
                u64::MAX
            }
        };
        self.resident_buffer_bytes = self.resident_buffer_bytes.max(self.resident_current_bytes);
    }

    fn release_resident(&mut self, bytes: u64) {
        if self.accounting_overflowed {
            return;
        }
        self.resident_current_bytes = self
            .resident_current_bytes
            .checked_sub(bytes)
            .expect("resident releases must match live allocations");
    }

    fn ensure_resident_budget(&self) -> Result<(), PreparedArtifactError> {
        if self.accounting_overflowed {
            return Err(PreparedArtifactError::ArtifactTooLarge);
        }
        if self
            .resident_limit_bytes
            .is_some_and(|budget| self.resident_buffer_bytes > budget)
        {
            return Err(PreparedArtifactError::ResidentBudgetExceeded {
                required: self.resident_buffer_bytes,
                budget: self.resident_limit_bytes.expect("checked resident limit"),
            });
        }
        Ok(())
    }

    fn with_resident<T>(
        &mut self,
        bytes: u64,
        use_allocation: impl FnOnce(&mut Self) -> Result<T, PreparedArtifactError>,
    ) -> Result<T, PreparedArtifactError> {
        self.acquire_resident(bytes);
        let result = self
            .ensure_resident_budget()
            .and_then(|()| use_allocation(self));
        self.release_resident(bytes);
        result
    }

    fn resize_resident(&mut self, current: &mut u64, next: u64) {
        if next >= *current {
            self.acquire_resident(next - *current);
        } else {
            self.release_resident(*current - next);
        }
        *current = next;
    }

    fn observe_source_inputs(&mut self, source: PreparedArtifactSourceBinding<'_>) {
        match source {
            PreparedArtifactSourceBinding::Files(source) => {
                self.observe_source_descriptors(&source.segments);
            }
            PreparedArtifactSourceBinding::Import(source) => {
                self.acquire_resident(observed_import_descriptor_bytes(&source.segments));
            }
        }
        self.source_reads = source
            .storage_demands()
            .into_keys()
            .map(|demand_id| SourceIoCounter {
                demand_id,
                counter: IoCounter::default(),
            })
            .collect();
        self.acquire_resident(observed_source_counter_bytes(
            &self.source_reads,
            self.source_reads.capacity(),
        ));
    }

    fn observe_source_descriptors(&mut self, inputs: &[PreparedArtifactSourceSegment]) {
        self.acquire_resident(observed_source_descriptor_bytes(inputs));
    }

    fn observe_cache_bytes(&mut self, bytes: u64) {
        self.cache_bytes_peak = self.cache_bytes_peak.max(bytes);
    }

    fn observe_temporary_storage(&mut self, bytes: u64) {
        self.temporary_storage_peak = self.temporary_storage_peak.max(bytes);
    }

    fn observe_locks(&mut self, locks: u64) {
        self.locks_peak = self.locks_peak.max(locks);
    }

    fn observe_file_descriptors(&mut self, file_descriptors: u64) {
        self.file_descriptors_peak = self.file_descriptors_peak.max(file_descriptors);
    }

    fn record(&mut self, class: CacheIoClass, bytes: u64) {
        let counter = match class {
            CacheIoClass::Read => &mut self.cache_read,
            CacheIoClass::Control => &mut self.cache_control,
            CacheIoClass::Write => &mut self.cache_write,
        };
        let overflowed = checked_accumulate(&mut counter.bytes, bytes)
            | checked_accumulate(&mut counter.operations, 1);
        self.accounting_overflowed |= overflowed;
    }

    fn record_source(&mut self, demand_id: &str, bytes: u64) {
        let counter = &mut self
            .source_reads
            .iter_mut()
            .find(|counter| counter.demand_id == demand_id)
            .expect("source counter initialized from the bound load source")
            .counter;
        let overflowed = checked_accumulate(&mut counter.bytes, bytes)
            | checked_accumulate(&mut counter.operations, 1);
        self.accounting_overflowed |= overflowed;
    }

    fn record_source_bytes(&mut self, demand_id: &str, bytes: u64) {
        let counter = &mut self
            .source_reads
            .iter_mut()
            .find(|counter| counter.demand_id == demand_id)
            .expect("source counter initialized from the bound load source")
            .counter;
        self.accounting_overflowed |= checked_accumulate(&mut counter.bytes, bytes);
    }

    fn record_source_operations(&mut self, demand_id: &str, operations: u64) {
        let counter = &mut self
            .source_reads
            .iter_mut()
            .find(|counter| counter.demand_id == demand_id)
            .expect("source counter initialized from the bound load source")
            .counter;
        self.accounting_overflowed |= checked_accumulate(&mut counter.operations, operations);
    }

    fn source_counter(&self, demand_id: &str) -> IoCounter {
        self.source_reads
            .iter()
            .find(|counter| counter.demand_id == demand_id)
            .map(|counter| counter.counter)
            .unwrap_or_default()
    }

    fn aggregate_source_counter(&self) -> IoCounter {
        self.source_reads
            .iter()
            .fold(IoCounter::default(), |total, source| IoCounter {
                bytes: total.bytes.saturating_add(source.counter.bytes),
                operations: total.operations.saturating_add(source.counter.operations),
            })
    }

    fn exact_counter(&self, kind: IoBufferKind) -> Result<IoCounter, PreparedArtifactError> {
        if self.accounting_overflowed {
            return Err(PreparedArtifactError::ArtifactTooLarge);
        }
        let source = self.source_reads.iter().try_fold(
            IoCounter::default(),
            |total, source| -> Result<_, PreparedArtifactError> {
                Ok(IoCounter {
                    bytes: total
                        .bytes
                        .checked_add(source.counter.bytes)
                        .ok_or(PreparedArtifactError::ArtifactTooLarge)?,
                    operations: total
                        .operations
                        .checked_add(source.counter.operations)
                        .ok_or(PreparedArtifactError::ArtifactTooLarge)?,
                })
            },
        )?;
        match kind {
            IoBufferKind::SourceReadAhead => Ok(source),
            IoBufferKind::Writeback => Ok(self.cache_write),
            IoBufferKind::StorageManager => Ok(IoCounter {
                bytes: source
                    .bytes
                    .checked_add(self.cache_read.bytes)
                    .and_then(|bytes| bytes.checked_add(self.cache_write.bytes))
                    .ok_or(PreparedArtifactError::ArtifactTooLarge)?,
                operations: source
                    .operations
                    .checked_add(self.cache_read.operations)
                    .and_then(|operations| operations.checked_add(self.cache_write.operations))
                    .and_then(|operations| operations.checked_add(self.cache_control.operations))
                    .ok_or(PreparedArtifactError::ArtifactTooLarge)?,
            }),
            _ => Ok(IoCounter::default()),
        }
    }

    fn counter(&self, kind: IoBufferKind) -> IoCounter {
        match kind {
            IoBufferKind::SourceReadAhead => self.aggregate_source_counter(),
            IoBufferKind::Writeback => self.cache_write,
            IoBufferKind::StorageManager => IoCounter {
                bytes: self
                    .aggregate_source_counter()
                    .bytes
                    .saturating_add(self.cache_read.bytes)
                    .saturating_add(self.cache_write.bytes),
                operations: self
                    .aggregate_source_counter()
                    .operations
                    .saturating_add(self.cache_read.operations)
                    .saturating_add(self.cache_write.operations)
                    .saturating_add(self.cache_control.operations),
            },
            _ => IoCounter::default(),
        }
    }

    fn measurement(&self, kind: IoBufferKind) -> IoMeasurement {
        let counter = self.counter(kind);
        IoMeasurement::new(kind, counter.bytes, counter.operations)
    }

    fn inspected_bytes(&self) -> u64 {
        self.cache_read.bytes
    }

    fn record_eviction(&mut self, entry: CacheInventoryEntry) {
        let prior = observed_vec_resident_bytes(&self.evictions);
        self.evictions.push((entry.identity, entry.bytes));
        let current = observed_vec_resident_bytes(&self.evictions);
        if current > prior {
            self.acquire_resident(current - prior);
        }
    }
}

fn checked_accumulate(total: &mut u64, value: u64) -> bool {
    match total.checked_add(value) {
        Some(next) => {
            *total = next;
            false
        }
        None => {
            *total = u64::MAX;
            true
        }
    }
}

struct BoundedFileWriter<'a> {
    file: &'a mut File,
    evidence: &'a mut ValidationEvidence,
    limit: u64,
    written: u64,
    exceeded: bool,
}

impl<'a> BoundedFileWriter<'a> {
    fn new(file: &'a mut File, limit: u64, evidence: &'a mut ValidationEvidence) -> Self {
        Self {
            file,
            evidence,
            limit,
            written: 0,
            exceeded: false,
        }
    }

    const fn exceeded(&self) -> bool {
        self.exceeded
    }
}

impl Write for BoundedFileWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let remaining = self.limit.saturating_sub(self.written);
        if bytes.len() as u64 > remaining {
            self.exceeded = true;
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "prepared-artifact manifest exceeds its reservation",
            ));
        }
        let written = self.file.write(bytes)?;
        self.written = self.written.saturating_add(written as u64);
        self.evidence.record(CacheIoClass::Write, written as u64);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

struct BoundedFileReader<'a> {
    file: File,
    evidence: &'a mut ValidationEvidence,
    remaining: u64,
    exceeded: bool,
}

impl BoundedFileReader<'_> {
    fn exceeded(&self) -> bool {
        self.exceeded
    }
}

impl Read for BoundedFileReader<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        if self.remaining == 0 {
            let mut probe = [0_u8; 1];
            let bytes = self.file.read(&mut probe)?;
            self.evidence.record(CacheIoClass::Read, bytes as u64);
            if bytes != 0 {
                self.exceeded = true;
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "prepared-artifact manifest exceeds its reservation",
                ));
            }
            return Ok(0);
        }
        let limit = output.len().min(self.remaining as usize);
        let bytes = self.file.read(&mut output[..limit])?;
        self.remaining = self.remaining.saturating_sub(bytes as u64);
        self.evidence.record(CacheIoClass::Read, bytes as u64);
        Ok(bytes)
    }
}

fn checked_product(values: &[u64]) -> Option<u64> {
    values
        .iter()
        .try_fold(1_u64, |product, value| product.checked_mul(*value))
}

fn observed_vec_resident_bytes<T>(values: &Vec<T>) -> u64 {
    let bytes = values
        .capacity()
        .saturating_mul(size_of::<T>())
        .saturating_add(size_of::<Vec<T>>());
    u64::try_from(bytes).unwrap_or(u64::MAX)
}

fn observed_manifest_resident_bytes(manifest: &ArtifactManifest) -> u64 {
    let bytes = size_of::<ArtifactManifest>()
        .saturating_add(manifest.schema.capacity())
        .saturating_add(manifest.identity.capacity())
        .saturating_add(manifest.cache_identity.capacity())
        .saturating_add(manifest.payload_sha256.capacity())
        .saturating_add(observed_manifest_descriptor_heap_bytes(
            &manifest.descriptor,
        ))
        .saturating_add(observed_manifest_segments_heap_bytes(&manifest.segments));
    u64::try_from(bytes).unwrap_or(u64::MAX)
}

fn observed_validation_state_resident_bytes(
    descriptor: &PreparedArtifactDescriptor,
    integrity: &Vec<ManifestSegmentIntegrity>,
    manifest_identities: [&String; 3],
) -> u64 {
    let bytes = size_of::<PreparedArtifactDescriptor>()
        .saturating_add(3_usize.saturating_mul(size_of::<String>()))
        .saturating_add(descriptor.owner.registration.provider_catalog.capacity())
        .saturating_add(descriptor.owner.registration.provider.capacity())
        .saturating_add(descriptor.owner.registration.provider_version.capacity())
        .saturating_add(descriptor.owner.registration.implementation.as_str().len())
        .saturating_add(descriptor.scientific.compiled_problem.capacity())
        .saturating_add(descriptor.scientific.observation_snapshot.capacity())
        .saturating_add(descriptor.scientific.compiled_geometry.capacity())
        .saturating_add(descriptor.scientific.numerics_contract.capacity())
        .saturating_add(descriptor.scientific.owner_scientific_identity.capacity())
        .saturating_add(descriptor.cache_scope.root_identity.capacity())
        .saturating_add(descriptor.cache_scope.storage_domain.capacity())
        .saturating_add(descriptor.cache_scope.eviction_policy.capacity())
        .saturating_add(
            descriptor
                .segments
                .capacity()
                .saturating_mul(size_of::<PreparedArtifactSegmentDescriptor>()),
        )
        .saturating_add(descriptor.segments.iter().fold(0_usize, |total, segment| {
            total.saturating_add(observed_segment_descriptor_heap_bytes(segment))
        }))
        .saturating_add(
            usize::try_from(observed_segment_integrity_resident_bytes(integrity))
                .unwrap_or(usize::MAX),
        )
        .saturating_add(
            manifest_identities
                .into_iter()
                .fold(0_usize, |total, identity| {
                    total.saturating_add(identity.capacity())
                }),
        );
    u64::try_from(bytes).unwrap_or(u64::MAX)
}

fn observed_segment_integrity_resident_bytes(integrity: &Vec<ManifestSegmentIntegrity>) -> u64 {
    let bytes = size_of::<Vec<ManifestSegmentIntegrity>>()
        .saturating_add(
            integrity
                .capacity()
                .saturating_mul(size_of::<ManifestSegmentIntegrity>()),
        )
        .saturating_add(integrity.iter().fold(0_usize, |total, segment| {
            total.saturating_add(segment.sha256.capacity())
        }));
    u64::try_from(bytes).unwrap_or(u64::MAX)
}

fn observed_manifest_segments_resident_bytes(segments: &Vec<ManifestSegment>) -> u64 {
    let bytes = size_of::<Vec<ManifestSegment>>()
        .saturating_add(observed_manifest_segments_heap_bytes(segments));
    u64::try_from(bytes).unwrap_or(u64::MAX)
}

fn observed_manifest_segments_heap_bytes(segments: &Vec<ManifestSegment>) -> usize {
    segments
        .capacity()
        .saturating_mul(size_of::<ManifestSegment>())
        .saturating_add(segments.iter().fold(0_usize, |total, segment| {
            total
                .saturating_add(observed_segment_descriptor_heap_bytes(&segment.descriptor))
                .saturating_add(segment.sha256.capacity())
        }))
}

fn observed_manifest_descriptor_heap_bytes(descriptor: &ManifestDescriptor) -> usize {
    descriptor
        .implementation_registry
        .capacity()
        .saturating_add(descriptor.provider_catalog.capacity())
        .saturating_add(descriptor.provider.capacity())
        .saturating_add(descriptor.provider_version.capacity())
        .saturating_add(descriptor.implementation.capacity())
        .saturating_add(descriptor.scientific.compiled_problem.capacity())
        .saturating_add(descriptor.scientific.observation_snapshot.capacity())
        .saturating_add(descriptor.scientific.compiled_geometry.capacity())
        .saturating_add(descriptor.scientific.numerics_contract.capacity())
        .saturating_add(descriptor.scientific.owner_scientific_identity.capacity())
        .saturating_add(descriptor.cache_scope.root_identity.capacity())
        .saturating_add(descriptor.cache_scope.storage_domain.capacity())
        .saturating_add(descriptor.cache_scope.eviction_policy.capacity())
}

fn observed_segment_descriptor_heap_bytes(descriptor: &PreparedArtifactSegmentDescriptor) -> usize {
    descriptor
        .name
        .capacity()
        .saturating_add(descriptor.shape.capacity().saturating_mul(size_of::<u64>()))
        .saturating_add(
            descriptor
                .support
                .capacity()
                .saturating_mul(size_of::<u64>()),
        )
        .saturating_add(
            descriptor
                .sampling
                .capacity()
                .saturating_mul(size_of::<u64>()),
        )
}

fn observed_source_descriptor_bytes(inputs: &[PreparedArtifactSourceSegment]) -> u64 {
    let bytes = size_of_val(inputs).saturating_add(inputs.iter().fold(0_usize, |total, input| {
        total
            .saturating_add(input.name.len())
            .saturating_add(input.source.as_os_str().as_encoded_bytes().len())
            .saturating_add(input.storage_domain.as_str().len())
            .saturating_add(
                input
                    .storage_operations_rate
                    .as_ref()
                    .map_or(0, |rate| rate.as_str().len()),
            )
            .saturating_add(input.storage_root.as_os_str().as_encoded_bytes().len())
    }));
    u64::try_from(bytes).unwrap_or(u64::MAX)
}

fn observed_import_descriptor_bytes(inputs: &[PreparedArtifactImportSegment]) -> u64 {
    let bytes = size_of_val(inputs).saturating_add(inputs.iter().fold(0_usize, |total, input| {
        total
            .saturating_add(input.name.len())
            .saturating_add(input.source.as_os_str().as_encoded_bytes().len())
            .saturating_add(input.storage_domain.as_str().len())
            .saturating_add(
                input
                    .storage_operations_rate
                    .as_ref()
                    .map_or(0, |rate| rate.as_str().len()),
            )
            .saturating_add(input.storage_root.as_os_str().as_encoded_bytes().len())
    }));
    u64::try_from(bytes).unwrap_or(u64::MAX)
}

fn observed_source_counter_bytes(counters: &[SourceIoCounter], capacity: usize) -> u64 {
    let bytes = capacity
        .saturating_mul(size_of::<SourceIoCounter>())
        .saturating_add(
            counters
                .iter()
                .map(|counter| counter.demand_id.capacity())
                .sum::<usize>(),
        );
    u64::try_from(bytes).unwrap_or(u64::MAX)
}

fn observed_owned_path_bytes(path: &Path) -> u64 {
    u64::try_from(size_of::<PathBuf>().saturating_add(path.as_os_str().as_encoded_bytes().len()))
        .unwrap_or(u64::MAX)
}

fn observed_path_inventory_bytes(paths: &[Box<Path>], capacity: usize) -> u64 {
    let allocation = capacity
        .saturating_mul(size_of::<Box<Path>>())
        .saturating_add(size_of::<Vec<Box<Path>>>());
    paths.iter().fold(
        u64::try_from(allocation).unwrap_or(u64::MAX),
        |total, path| {
            total.saturating_add(
                u64::try_from(path.as_os_str().as_encoded_bytes().len()).unwrap_or(u64::MAX),
            )
        },
    )
}

fn hash_len(hasher: &mut Sha256, length: usize) -> Result<(), PreparedArtifactError> {
    let length = u64::try_from(length).map_err(|_| PreparedArtifactError::ArtifactTooLarge)?;
    hasher.update(length.to_le_bytes());
    Ok(())
}

fn hash_bytes(hasher: &mut Sha256, bytes: &[u8]) -> Result<(), PreparedArtifactError> {
    hash_len(hasher, bytes.len())?;
    hasher.update(bytes);
    Ok(())
}

fn hash_u64s(hasher: &mut Sha256, values: &[u64]) -> Result<(), PreparedArtifactError> {
    hash_len(hasher, values.len())?;
    for value in values {
        hasher.update(value.to_le_bytes());
    }
    Ok(())
}

const fn kind_tag(kind: PreparedArtifactKind) -> u8 {
    match kind {
        PreparedArtifactKind::ConvolutionFunction => 1,
        PreparedArtifactKind::SpectralMap => 2,
        PreparedArtifactKind::Kernel => 3,
    }
}

const fn operation_tag(operation: PreparedArtifactOperation) -> u8 {
    match operation {
        PreparedArtifactOperation::Generate => 1,
        PreparedArtifactOperation::Load => 2,
        PreparedArtifactOperation::Reuse => 3,
        PreparedArtifactOperation::Consume => 4,
    }
}

const fn precision_tag(precision: PreparedArtifactPrecision) -> u8 {
    match precision {
        PreparedArtifactPrecision::F32 => 1,
        PreparedArtifactPrecision::F64 => 2,
        PreparedArtifactPrecision::ComplexF32 => 3,
        PreparedArtifactPrecision::ComplexF64 => 4,
        PreparedArtifactPrecision::I32 => 5,
        PreparedArtifactPrecision::U32 => 6,
        PreparedArtifactPrecision::U8 => 7,
    }
}

const fn order_tag(order: PreparedArtifactOrder) -> u8 {
    match order {
        PreparedArtifactOrder::Axis0ContiguousLittleEndian => 1,
        PreparedArtifactOrder::LastAxisContiguousLittleEndian => 2,
    }
}

fn encode_hex(bytes: &[u8; 32]) -> String {
    let mut result = String::with_capacity(64);
    for byte in bytes {
        use fmt::Write as _;
        write!(&mut result, "{byte:02x}").expect("writing into String cannot fail");
    }
    result
}

fn decode_digest(value: &str) -> Option<[u8; 32]> {
    if value.len() != 64 {
        return None;
    }
    let mut digest = [0_u8; 32];
    for (index, byte) in digest.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).ok()?;
    }
    Some(digest)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_storage_domain(root: &Path) -> StorageDomain {
        StorageDomain {
            id: StorageDomainId::new("atomic-output"),
            root: root.to_path_buf(),
            capacity_bytes: u64::MAX,
            read_rate: crate::RateResourceId::new("test-read"),
            write_rate: crate::RateResourceId::new("test-write"),
            operations_rate: Some(crate::RateResourceId::new("test-operations")),
            queue: crate::QueueResourceId::new("test-queue"),
        }
    }

    fn open_test_store(
        root: &Path,
        budget: PreparedArtifactBudget,
    ) -> Result<PreparedArtifactStore, PreparedArtifactError> {
        PreparedArtifactStore::open(root, &test_storage_domain(root), budget)
    }

    #[test]
    fn private_store_control_is_storage_manager_work_not_publication() {
        let mut evidence = ValidationEvidence::default();
        evidence.store_control_operation();
        evidence.store_control_operation();

        let storage = evidence.measurement(IoBufferKind::StorageManager);
        assert_eq!(storage.bytes(), 0);
        assert_eq!(storage.operations(), 2);
        assert_eq!(evidence.cache_read.operations, 0);
        assert_eq!(evidence.cache_control.operations, 2);
        assert_eq!(
            evidence.measurement(IoBufferKind::Writeback).operations(),
            0
        );
        assert_eq!(
            evidence.measurement(IoBufferKind::Publication).operations(),
            0
        );
    }

    #[test]
    fn residency_evidence_counts_overlapping_bounded_inventories_and_never_caps_actuals() {
        let directory = tempfile::tempdir().expect("resident inventory");
        fs::write(directory.path().join(MANIFEST_FILE), [0_u8; 1])
            .expect("resident manifest entry");
        fs::write(directory.path().join(PAYLOAD_FILE), [0_u8; 1]).expect("resident payload entry");
        let budget = PreparedArtifactBudget::new(200, 3, 64).expect("resident budget");
        let mut evidence = ValidationEvidence::new(budget);
        let baseline = evidence.resident_current_bytes;
        let mut expected_peak = 0;
        with_directory_paths_counted(
            directory.path(),
            &mut evidence,
            root_inventory_limit(budget).expect("root inventory limit"),
            |evidence, root_paths| {
                let root_bytes = observed_path_inventory_bytes(
                    root_paths,
                    root_inventory_limit(budget).expect("root inventory capacity"),
                );
                assert_eq!(evidence.resident_current_bytes, baseline + root_bytes);
                with_directory_paths_counted(
                    directory.path(),
                    evidence,
                    MAX_ENTRY_FILES,
                    |evidence, entry_paths| {
                        let entry_bytes =
                            observed_path_inventory_bytes(entry_paths, MAX_ENTRY_FILES);
                        expected_peak = baseline + root_bytes + entry_bytes;
                        assert_eq!(evidence.resident_current_bytes, expected_peak);
                        Ok(())
                    },
                )
            },
        )
        .expect("overlapping path inventories");
        assert_eq!(evidence.resident_current_bytes, baseline);
        assert_eq!(evidence.resident_buffer_bytes, expected_peak);
        assert_eq!(
            observed_resource_peak(
                &LeaseResource::IoBuffer(IoBufferKind::StorageManager),
                "private-cache",
                PreparedArtifactOperation::Generate,
                0,
                0,
                &evidence,
            ),
            expected_peak,
            "resident evidence must retain an observed overrun instead of clamping it"
        );
        let failed_temporary_bytes = 201;
        assert_eq!(
            observed_resource_peak(
                &LeaseResource::Storage {
                    demand_id: "private-cache".to_string(),
                    use_kind: StorageUseKind::Temporary,
                },
                "private-cache",
                PreparedArtifactOperation::Generate,
                0,
                failed_temporary_bytes,
                &evidence,
            ),
            failed_temporary_bytes,
            "failed staging evidence must retain bytes above a stale reservation"
        );
    }

    #[test]
    fn residency_evidence_measures_concurrent_peak_not_historical_sum() {
        let mut evidence = ValidationEvidence::with_resident_limit(25);

        evidence.acquire_resident(10);
        evidence.release_resident(10);
        evidence.acquire_resident(20);
        assert_eq!(evidence.resident_buffer_bytes, 20);
        evidence.acquire_resident(5);
        assert_eq!(evidence.resident_buffer_bytes, 25);
        evidence.release_resident(5);
        evidence.release_resident(20);

        evidence.acquire_resident(26);
        assert!(matches!(
            evidence.ensure_resident_budget(),
            Err(PreparedArtifactError::ResidentBudgetExceeded {
                required: 26,
                budget: 25,
            })
        ));
        evidence.release_resident(26);
        assert_eq!(evidence.resident_current_bytes, 0);
    }

    #[test]
    fn private_store_lock_creation_is_counted_once_as_writeback() {
        let directory = tempfile::tempdir().expect("private lock cache");
        let budget = PreparedArtifactBudget::new(200, 3, 1).expect("bounded cache");
        let store = open_test_store(directory.path(), budget).expect("store");
        assert!(!store.lock_path.exists());

        let mut created = ValidationEvidence::default();
        store
            .lock(&mut created)
            .expect("new private lock")
            .release(&mut created)
            .expect("release new private lock");
        assert_eq!(created.cache_write.operations, 1);
        assert_eq!(created.cache_control.operations, 3);
        assert_eq!(created.locks_peak, 1);
        assert_eq!(created.file_descriptors_peak, 1);

        let mut reopened = ValidationEvidence::default();
        store
            .lock(&mut reopened)
            .expect("existing private lock")
            .release(&mut reopened)
            .expect("release existing private lock");
        assert_eq!(reopened.cache_write.operations, 0);
        assert_eq!(reopened.cache_control.operations, 4);
        assert_eq!(reopened.locks_peak, 1);
        assert_eq!(reopened.file_descriptors_peak, 1);
    }

    #[test]
    fn partial_eviction_failure_retains_every_completed_mutation() {
        let directory = tempfile::tempdir().expect("eviction failure cache");
        let budget = PreparedArtifactBudget::new(200, 3, 1).expect("bounded cache");
        let mut store = open_test_store(directory.path(), budget).expect("store");
        let identities = [
            ArtifactIdentity::from_owner_digest([1; 32]),
            ArtifactIdentity::from_owner_digest([2; 32]),
        ];
        for identity in identities {
            let entry = store.entry_path(identity);
            fs::create_dir(&entry).expect("entry directory");
            fs::write(entry.join(MANIFEST_FILE), [0_u8; 64]).expect("manifest bytes");
            fs::write(entry.join(PAYLOAD_FILE), [0_u8; 64]).expect("payload bytes");
        }
        store.fail_after_evictions = Some(1);
        let mut evidence = ValidationEvidence::default();

        let error = store
            .evict_for(
                ArtifactIdentity::from_owner_digest([3; 32]),
                100,
                &mut evidence,
            )
            .expect_err("the injected second eviction step fails");

        assert!(error.to_string().contains("injected post-eviction failure"));
        assert_eq!(evidence.evictions, vec![(identities[0], 128)]);
        assert!(!store.entry_path(identities[0]).exists());
        assert!(store.entry_path(identities[1]).exists());
        assert!(
            store
                .cache
                .join(format!("{STAGING_PREFIX}evicted-{}", identities[0]))
                .exists(),
            "the recoverable staging entry proves the eviction rename completed"
        );
    }

    #[test]
    fn warm_reuse_budget_counts_orphan_staging_bytes_entries_and_residency() {
        let bytes_directory = tempfile::tempdir().expect("staging byte-budget cache");
        let bytes_budget = PreparedArtifactBudget::new(100, 3, 1).expect("staging byte budget");
        let bytes_store =
            open_test_store(bytes_directory.path(), bytes_budget).expect("byte store");
        let staging = bytes_store.cache.join(format!("{STAGING_PREFIX}bytes"));
        fs::create_dir(&staging).expect("staging byte directory");
        fs::write(staging.join(MANIFEST_FILE), [0_u8; 64]).expect("staging manifest");
        fs::write(staging.join(PAYLOAD_FILE), [0_u8; 64]).expect("staging payload");
        let mut bytes_evidence = ValidationEvidence::new(bytes_budget);

        assert!(matches!(
            bytes_store.validate_raw_budget(
                ArtifactIdentity::from_owner_digest([1; 32]),
                &mut bytes_evidence,
            ),
            Err(PreparedArtifactError::CacheBudgetExceeded {
                required: 128,
                budget: 100,
            })
        ));
        assert_eq!(bytes_evidence.cache_bytes_peak, 128);
        assert!(bytes_evidence.resident_buffer_bytes > 0);
        assert_eq!(
            bytes_evidence.resident_current_bytes,
            observed_vec_resident_bytes(&bytes_evidence.evictions),
            "failed inventory storage must be released after inspection"
        );

        let entries_directory = tempfile::tempdir().expect("staging entry-budget cache");
        let entries_budget =
            PreparedArtifactBudget::new(1_000, 1, 1).expect("staging entry budget");
        let entries_store =
            open_test_store(entries_directory.path(), entries_budget).expect("entry store");
        for suffix in ["first", "second"] {
            fs::create_dir(
                entries_store
                    .cache
                    .join(format!("{STAGING_PREFIX}{suffix}")),
            )
            .expect("staging entry directory");
        }
        let mut entries_evidence = ValidationEvidence::new(entries_budget);

        assert!(matches!(
            entries_store.validate_raw_budget(
                ArtifactIdentity::from_owner_digest([2; 32]),
                &mut entries_evidence,
            ),
            Err(PreparedArtifactError::CacheEntryBudgetExceeded {
                required: 2,
                budget: 1,
            })
        ));
        assert!(entries_evidence.resident_buffer_bytes > 0);
        assert_eq!(
            entries_evidence.resident_current_bytes,
            observed_vec_resident_bytes(&entries_evidence.evictions),
            "failed root inventory storage must be released after inspection"
        );
    }

    #[test]
    fn source_descriptor_residency_is_bounded_and_observed() {
        let directory = tempfile::tempdir().expect("source descriptor residency");
        let source = directory.path().join("imaging.bin");
        fs::write(&source, []).expect("source descriptor file");
        let inputs = [PreparedArtifactSourceSegment::new(
            "imaging",
            source.clone(),
            [0; 32],
            &test_storage_domain(directory.path()),
        )
        .expect("bounded source descriptor")];
        let budget = PreparedArtifactBudget::new(1, 1, 1).expect("source descriptor budget");
        let mut evidence = ValidationEvidence::new(budget);
        let baseline = evidence.resident_current_bytes;
        let descriptor_bytes = observed_source_descriptor_bytes(&inputs);
        let canonical_bytes = observed_owned_path_bytes(&source);

        evidence.observe_source_descriptors(&inputs);
        assert_eq!(evidence.resident_current_bytes, baseline + descriptor_bytes);
        evidence
            .with_resident(canonical_bytes, |evidence| {
                assert_eq!(
                    evidence.resident_current_bytes,
                    baseline + descriptor_bytes + canonical_bytes
                );
                Ok(())
            })
            .expect("canonical source path residency");
        assert_eq!(evidence.resident_current_bytes, baseline + descriptor_bytes);
        assert!(
            source_descriptor_reservation(inputs.len()).expect("source reservation")
                >= descriptor_bytes.saturating_add(canonical_bytes)
        );
        assert_eq!(
            evidence.resident_buffer_bytes,
            baseline + descriptor_bytes + canonical_bytes
        );
    }

    #[test]
    fn post_rename_failure_removes_visibility_and_retains_mutation_evidence() {
        let directory = tempfile::tempdir().expect("publication failure cache");
        let budget = PreparedArtifactBudget::new(200, 3, 1).expect("bounded cache");
        let mut store = open_test_store(directory.path(), budget).expect("store");
        store.fail_after_publication_rename = true;
        let staging = store.cache.join(format!("{STAGING_PREFIX}publication"));
        let target = store.entry_path(ArtifactIdentity::from_owner_digest([3; 32]));
        fs::create_dir(&staging).expect("publication staging directory");
        fs::write(staging.join(MANIFEST_FILE), [0_u8; 64]).expect("manifest bytes");
        fs::write(staging.join(PAYLOAD_FILE), [0_u8; 64]).expect("payload bytes");
        let mut evidence = ValidationEvidence::default();
        let evicted = CacheInventoryEntry {
            identity: ArtifactIdentity::from_owner_digest([2; 32]),
            bytes: 128,
        };
        evidence.record_eviction(evicted);

        let error = store
            .rename_staging_for_publication(
                &staging,
                &target,
                MaterializedArtifactEvidence {
                    payload_sha256: [4; 32],
                    payload_bytes: 64,
                    path: target.clone(),
                    disposition: ArtifactDisposition::Built,
                },
                &mut evidence,
            )
            .expect_err("the injected post-rename step fails");

        assert!(
            error
                .to_string()
                .contains("injected post-publication-rename failure")
        );
        assert!(
            !target.exists(),
            "a failed publication must not leave the final artifact visible"
        );
        let materialized = evidence
            .materialized
            .expect("completed materialization evidence");
        assert_eq!(materialized.payload_bytes, 64);
        assert_eq!(materialized.path, target);
        assert_eq!(materialized.disposition, ArtifactDisposition::Built);
        assert_eq!(evidence.evictions, vec![(evicted.identity, evicted.bytes)]);
    }
}
