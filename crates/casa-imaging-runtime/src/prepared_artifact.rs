// SPDX-License-Identifier: LGPL-3.0-or-later

//! Plan-bound private persistence for immutable implementation preparation.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, BufWriter, Read, Seek, Write},
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard, OnceLock, Weak},
};

use casa_imaging_model::CompiledProblem;
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tempfile::Builder;

use crate::{
    ArtifactDisposition, ArtifactIdentity, ArtifactMeasurement, ArtifactRole, CacheIdentity,
    ImplementationRegistryId, IoBufferKind, IoMeasurement, LeaseResource, PlannedArtifact,
    RedactedPath, ResourceMeasurement, StorageUseKind, WorkExecutionContext, WorkImplementationId,
    WorkKind, WorkMeasurements, WorkNodeId,
};

const ARTIFACT_IDENTITY_DOMAIN: &[u8] = b"casa-rs/private-prepared-artifact/identity\0";
const CONTENT_IDENTITY_DOMAIN: &[u8] = b"casa-rs/private-prepared-artifact/content\0";
const CACHE_IDENTITY_DOMAIN: &[u8] = b"casa-rs/private-prepared-artifact/cache\0";
const CACHE_ROOT_IDENTITY_DOMAIN: &[u8] = b"casa-rs/private-prepared-artifact/root\0";
const WORK_NODE_IDENTITY_DOMAIN: &[u8] = b"casa-rs/private-prepared-artifact/work-node\0";
const REJECTION_EVIDENCE_DOMAIN: &[u8] = b"casa-rs/private-prepared-artifact/rejection\0";
const IDENTITY_VERSION: u32 = 2;
const CACHE_SCHEMA: &str = "casa-rs-private-prepared-artifact";
const CACHE_SCHEMA_VERSION: u32 = 2;
const EVICTION_POLICY: &str = "lexicographic-existing-artifact-identity-v1";
const CACHE_DIRECTORY: &str = "objects-v1";
const LOCK_FILE: &str = ".casa-rs-prepared-artifact.lock";
const MANIFEST_FILE: &str = "manifest.json";
const PAYLOAD_FILE: &str = "payload.bin";
const STAGING_PREFIX: &str = ".staging-";
const MANIFEST_RESERVATION_BYTES: u64 = 16 * 1024;
const COPY_BUFFER_CEILING: usize = 64 * 1024;

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

/// Versioned implementation owner that alone derives cache and artifact identities.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedArtifactOwner {
    implementation_registry: ImplementationRegistryId,
    provider: String,
    provider_version: String,
    implementation: WorkImplementationId,
}

impl PreparedArtifactOwner {
    /// Bind an immutable registry, provider version, and implementation identity.
    pub fn new(
        implementation_registry: ImplementationRegistryId,
        provider: impl Into<String>,
        provider_version: impl Into<String>,
        implementation: WorkImplementationId,
    ) -> Result<Self, PreparedArtifactError> {
        let provider = provider.into();
        let provider_version = provider_version.into();
        if !valid_identifier(&provider)
            || !valid_identifier(&provider_version)
            || !valid_identifier(implementation.as_str())
        {
            return Err(PreparedArtifactError::InvalidOwner);
        }
        Ok(Self {
            implementation_registry,
            provider,
            provider_version,
            implementation,
        })
    }

    /// Return the immutable implementation-registry identity.
    #[must_use]
    pub const fn implementation_registry_id(&self) -> ImplementationRegistryId {
        self.implementation_registry
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScientificCommitments {
    compiled_problem: String,
    observation_snapshot: String,
    compiled_geometry: String,
    numerics_contract: String,
}

impl ScientificCommitments {
    fn from_problem(problem: &CompiledProblem) -> Self {
        Self {
            compiled_problem: encode_hex(&problem.problem_id().as_bytes()),
            observation_snapshot: encode_hex(&problem.inputs().observation().as_bytes()),
            compiled_geometry: encode_hex(&problem.geometry().geometry_id().as_bytes()),
            numerics_contract: encode_hex(&problem.numerics_id().as_bytes()),
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
    cache_bytes: u64,
    entries: u64,
    streaming_buffer_bytes: u64,
    eviction_policy: String,
}

impl CacheScope {
    fn new(root: &Path, budget: PreparedArtifactBudget) -> Result<Self, PreparedArtifactError> {
        Ok(Self {
            root_identity: encode_hex(&derive_cache_root_identity(root)),
            cache_bytes: budget.cache_bytes,
            entries: u64::try_from(budget.entries)
                .map_err(|_| PreparedArtifactError::InvalidBudget)?,
            streaming_buffer_bytes: budget.streaming_buffer_bytes,
            eviction_policy: EVICTION_POLICY.to_string(),
        })
    }

    fn validate(&self) -> Result<(), PreparedArtifactError> {
        if decode_digest(&self.root_identity).is_none()
            || self.cache_bytes == 0
            || self.entries == 0
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
    /// Describe an asymmetric named imaging/weight CF pair.
    pub fn convolution_function(
        store: &PreparedArtifactStore,
        owner: PreparedArtifactOwner,
        problem: &CompiledProblem,
        imaging: PreparedArtifactPlaneDescriptor,
        weight: PreparedArtifactPlaneDescriptor,
    ) -> Result<Self, PreparedArtifactError> {
        Self::new(
            store,
            owner,
            PreparedArtifactKind::ConvolutionFunction,
            problem,
            vec![
                imaging.into_segment("imaging"),
                weight.into_segment("weight"),
            ],
        )
    }

    /// Describe a spectral map or other kernel through private named segments.
    pub fn new(
        store: &PreparedArtifactStore,
        owner: PreparedArtifactOwner,
        kind: PreparedArtifactKind,
        problem: &CompiledProblem,
        segments: Vec<PreparedArtifactSegmentDescriptor>,
    ) -> Result<Self, PreparedArtifactError> {
        let scientific = ScientificCommitments::from_problem(problem);
        Self::from_commitments(owner, kind, scientific, store.scope.clone(), segments)
    }

    fn from_commitments(
        owner: PreparedArtifactOwner,
        kind: PreparedArtifactKind,
        scientific: ScientificCommitments,
        cache_scope: CacheScope,
        mut segments: Vec<PreparedArtifactSegmentDescriptor>,
    ) -> Result<Self, PreparedArtifactError> {
        if segments.is_empty() {
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
            PreparedArtifactOperation::Load => ArtifactRole::Input,
            PreparedArtifactOperation::Reuse => ArtifactRole::Cache,
        };
        PlannedArtifact::new(
            self.identity,
            self.work_node_id(operation),
            role,
            Some(self.cache_identity),
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
        WorkImplementationId::new(format!(
            "prepared-artifact-{}-registry-{}-provider-{}-version-{}-implementation-{}",
            operation.name(),
            self.owner.implementation_registry,
            self.owner.provider,
            self.owner.provider_version,
            self.owner.implementation.as_str(),
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
}

impl PreparedArtifactOperation {
    const fn name(self) -> &'static str {
        match self {
            Self::Generate => "cold-generation",
            Self::Load => "cold-load",
            Self::Reuse => "warm-reuse",
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
    /// Return the durable typed evidence identity for this rejection.
    ///
    /// Rejection evidence is stored in the existing receipt
    /// `observed_identity` field. It is deliberately domain-separated from
    /// both artifact content identities and owner-derived artifact identities.
    #[must_use]
    pub fn evidence_identity(self, planned: ArtifactIdentity) -> ArtifactIdentity {
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
pub enum PreparedArtifactReuseOutcome<'lease> {
    /// A complete exact hit passed integrity validation.
    Reused(PreparedArtifact<'lease>),
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
    resident_buffer_bytes: u64,
}

impl PreparedArtifactReservation {
    /// Return the persistent-cache storage claim.
    #[must_use]
    pub const fn persistent_cache_bytes(self) -> u64 {
        self.persistent_cache_bytes
    }

    /// Return the same-filesystem private staging claim.
    #[must_use]
    pub const fn temporary_staging_bytes(self) -> u64 {
        self.temporary_staging_bytes
    }

    /// Return the bounded resident validation/generation buffer claim.
    #[must_use]
    pub const fn resident_buffer_bytes(self) -> u64 {
        self.resident_buffer_bytes
    }
}

/// One private named byte stream supplied to generation or load.
pub struct PreparedArtifactSegmentInput<'a> {
    name: &'a str,
    source: &'a mut dyn Read,
}

impl<'a> PreparedArtifactSegmentInput<'a> {
    /// Bind a byte stream to one descriptor segment name.
    #[must_use]
    pub fn new(name: &'a str, source: &'a mut dyn Read) -> Self {
        Self { name, source }
    }
}

/// Validated immutable handle to one private prepared artifact.
///
/// The lifetime is borrowed from the node's [`WorkExecutionContext`]. This
/// prevents a caller from retaining the payload file after the node returns
/// and its `FileDescriptors` work claim is released.
pub struct PreparedArtifact<'lease> {
    identity: ArtifactIdentity,
    integrity_identity: ArtifactIdentity,
    cache_identity: CacheIdentity,
    payload: File,
    segments: BTreeMap<String, (u64, u64)>,
    _lease: PhantomData<&'lease ()>,
}

impl fmt::Debug for PreparedArtifact<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedArtifact")
            .field("identity", &self.identity)
            .field("cache_identity", &self.cache_identity)
            .field("segments", &self.segments)
            .finish_non_exhaustive()
    }
}

impl PreparedArtifact<'_> {
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

    /// Return private segment names in canonical order.
    pub fn segment_names(&self) -> impl Iterator<Item = &str> {
        self.segments.keys().map(String::as_str)
    }

    /// Copy one immutable named segment to a caller-owned sink.
    pub fn copy_segment_to(
        &self,
        name: &str,
        output: &mut dyn Write,
    ) -> Result<u64, PreparedArtifactError> {
        let &(offset, bytes) = self
            .segments
            .get(name)
            .ok_or_else(|| PreparedArtifactError::UnknownSegment(name.to_string()))?;
        let mut payload = self.payload.try_clone()?;
        payload.seek(io::SeekFrom::Start(offset))?;
        let copied = io::copy(&mut payload.take(bytes), output)?;
        if copied != bytes {
            return Err(PreparedArtifactError::IncompleteArtifact);
        }
        Ok(copied)
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
    state: Arc<RootState>,
}

#[derive(Debug)]
struct RootState {
    mutation: Mutex<()>,
}

static ROOT_STATES: OnceLock<Mutex<BTreeMap<PathBuf, Weak<RootState>>>> = OnceLock::new();

impl PreparedArtifactStore {
    /// Open an explicitly configured private casa-rs cache root.
    pub fn open(
        root: impl AsRef<Path>,
        budget: PreparedArtifactBudget,
    ) -> Result<Self, PreparedArtifactError> {
        reject_casa_visible_root(root.as_ref())?;
        fs::create_dir_all(root.as_ref())?;
        let root = fs::canonicalize(root)?;
        reject_casa_visible_root(&root)?;
        reject_casa_cache_contents(&root)?;
        let cache = root.join(CACHE_DIRECTORY);
        fs::create_dir_all(&cache)?;
        if !cache.symlink_metadata()?.file_type().is_dir() {
            return Err(PreparedArtifactError::UnknownCacheEntry(cache));
        }
        let lock_path = root.join(LOCK_FILE);
        match lock_path.symlink_metadata() {
            Ok(metadata) if !metadata.file_type().is_file() => {
                return Err(PreparedArtifactError::UnknownCacheEntry(lock_path));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
            Ok(_) => {}
        }
        let state = root_state(&root)?;
        let scope = CacheScope::new(&root, budget)?;
        Ok(Self {
            root,
            cache,
            lock_path,
            budget,
            scope,
            state,
        })
    }

    /// Return the explicit private root, which is never a CASA cache path.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Return the complete policy committed into owner-derived cache identities.
    #[must_use]
    pub const fn budget(&self) -> PreparedArtifactBudget {
        self.budget
    }

    /// Derive exact resource/storage bounds for one explicit cache operation.
    pub fn reservation(
        &self,
        descriptor: &PreparedArtifactDescriptor,
        operation: PreparedArtifactOperation,
    ) -> Result<PreparedArtifactReservation, PreparedArtifactError> {
        if descriptor.cache_scope != self.scope {
            return Err(PreparedArtifactError::CachePolicyMismatch);
        }
        let payload_bytes = descriptor.payload_bytes()?;
        let entry_bytes = payload_bytes
            .checked_add(MANIFEST_RESERVATION_BYTES)
            .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
        if entry_bytes > self.budget.cache_bytes {
            return Err(PreparedArtifactError::CacheBudgetExceeded {
                required: entry_bytes,
                budget: self.budget.cache_bytes,
            });
        }
        let minimum_scalar = descriptor
            .segments
            .iter()
            .map(|segment| segment.precision.scalar_bytes() as u64)
            .max()
            .unwrap_or(1);
        if self.budget.streaming_buffer_bytes < minimum_scalar {
            return Err(PreparedArtifactError::StreamingBufferTooSmall {
                required: minimum_scalar,
                budget: self.budget.streaming_buffer_bytes,
            });
        }
        Ok(PreparedArtifactReservation {
            persistent_cache_bytes: self.budget.cache_bytes,
            entry_bytes,
            temporary_staging_bytes: if operation == PreparedArtifactOperation::Reuse {
                0
            } else {
                entry_bytes
            },
            resident_buffer_bytes: payload_bytes.min(self.budget.streaming_buffer_bytes),
        })
    }

    /// Generate, validate, and atomically publish exact cold bytes.
    ///
    /// The returned handle is scoped to the borrowed execution context.
    pub fn generate<'lease>(
        &self,
        context: &'lease WorkExecutionContext<'_>,
        descriptor: &PreparedArtifactDescriptor,
        segments: &mut [PreparedArtifactSegmentInput<'_>],
    ) -> Result<(PreparedArtifact<'lease>, WorkMeasurements), PreparedArtifactError> {
        self.publish(
            context,
            descriptor,
            PreparedArtifactOperation::Generate,
            ArtifactDisposition::Built,
            segments,
        )
    }

    /// Load, validate, and atomically publish bytes from a separately validated source.
    ///
    /// This API conveys no CASA-cache provenance and never opens a CASA path.
    pub fn load<'lease>(
        &self,
        context: &'lease WorkExecutionContext<'_>,
        descriptor: &PreparedArtifactDescriptor,
        segments: &mut [PreparedArtifactSegmentInput<'_>],
    ) -> Result<(PreparedArtifact<'lease>, WorkMeasurements), PreparedArtifactError> {
        self.publish(
            context,
            descriptor,
            PreparedArtifactOperation::Load,
            ArtifactDisposition::Loaded,
            segments,
        )
    }

    /// Revalidate and reuse the exact warm artifact selected by planning.
    ///
    /// A successful handle is scoped to the borrowed execution context; a
    /// rejection returns only durable evidence and no open payload handle.
    pub fn reuse<'lease>(
        &self,
        context: &'lease WorkExecutionContext<'_>,
        descriptor: &PreparedArtifactDescriptor,
    ) -> Result<(PreparedArtifactReuseOutcome<'lease>, WorkMeasurements), PreparedArtifactError>
    {
        let reservation = self.reservation(descriptor, PreparedArtifactOperation::Reuse)?;
        validate_plan_binding(
            *context,
            descriptor,
            PreparedArtifactOperation::Reuse,
            reservation,
        )?;
        let _lock = self.lock()?;
        let cache_bytes = self.validate_raw_budget(descriptor.identity)?;
        let path = self.entry_path(descriptor.identity);
        let mut evidence = ValidationEvidence::metadata_probe();
        let validated = match path.symlink_metadata() {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let measurements = rejected_measurements(
                    *context,
                    descriptor,
                    reservation,
                    PreparedArtifactRejection::Missing,
                    &path,
                    cache_bytes,
                    evidence,
                );
                return Ok((
                    PreparedArtifactReuseOutcome::Rejected(PreparedArtifactRejection::Missing),
                    measurements,
                ));
            }
            Err(error) => return Err(error.into()),
            Ok(_) => match self.validate_entry_with_evidence(
                descriptor.identity,
                Some(descriptor),
                &mut evidence,
            ) {
                Ok(validated) => validated,
                Err(error) => {
                    let Some(rejection) = rejection_for(&error) else {
                        return Err(error);
                    };
                    let measurements = rejected_measurements(
                        *context,
                        descriptor,
                        reservation,
                        rejection,
                        &path,
                        cache_bytes,
                        evidence,
                    );
                    return Ok((
                        PreparedArtifactReuseOutcome::Rejected(rejection),
                        measurements,
                    ));
                }
            },
        };
        let measurements = measurements(
            *context,
            descriptor,
            ArtifactDisposition::Reused,
            &validated,
            reservation,
            PreparedArtifactOperation::Reuse,
            cache_bytes,
        );
        Ok((
            PreparedArtifactReuseOutcome::Reused(validated.into_handle(descriptor, context)),
            measurements,
        ))
    }

    fn publish<'lease>(
        &self,
        context: &'lease WorkExecutionContext<'_>,
        descriptor: &PreparedArtifactDescriptor,
        operation: PreparedArtifactOperation,
        disposition: ArtifactDisposition,
        segments: &mut [PreparedArtifactSegmentInput<'_>],
    ) -> Result<(PreparedArtifact<'lease>, WorkMeasurements), PreparedArtifactError> {
        let reservation = self.reservation(descriptor, operation)?;
        validate_plan_binding(*context, descriptor, operation, reservation)?;
        let (validated, final_disposition, cache_bytes) =
            self.publish_bytes(descriptor, disposition, segments, reservation)?;
        let measurements = measurements(
            *context,
            descriptor,
            final_disposition,
            &validated,
            reservation,
            operation,
            cache_bytes,
        );
        Ok((validated.into_handle(descriptor, context), measurements))
    }

    fn publish_bytes(
        &self,
        descriptor: &PreparedArtifactDescriptor,
        disposition: ArtifactDisposition,
        segments: &mut [PreparedArtifactSegmentInput<'_>],
        reservation: PreparedArtifactReservation,
    ) -> Result<(ValidatedArtifact, ArtifactDisposition, u64), PreparedArtifactError> {
        validate_segment_inputs(descriptor, segments)?;
        let _lock = self.lock()?;
        self.remove_orphan_staging()?;
        let staging = Builder::new()
            .prefix(STAGING_PREFIX)
            .tempdir_in(&self.cache)?;
        let payload_path = staging.path().join(PAYLOAD_FILE);
        let payload_file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&payload_path)?;
        let mut payload = BufWriter::new(payload_file);
        let buffer_len = streaming_buffer_len(self.budget, descriptor)?;
        let mut buffer = vec![0_u8; buffer_len];
        let mut payload_hasher = Sha256::new();
        let mut offset = 0_u64;
        let mut manifest_segments = Vec::with_capacity(descriptor.segments.len());
        for (segment, input) in descriptor.segments.iter().zip(segments.iter_mut()) {
            let bytes = segment.byte_len()?;
            let digest = stream_segment(
                input.source,
                &mut payload,
                &mut payload_hasher,
                &mut buffer,
                segment,
            )?;
            manifest_segments.push(ManifestSegment {
                descriptor: segment.clone(),
                offset,
                bytes,
                sha256: encode_hex(&digest),
            });
            offset = offset
                .checked_add(bytes)
                .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
        }
        payload.flush()?;
        payload.get_ref().sync_all()?;
        drop(payload);
        let payload_sha256: [u8; 32] = payload_hasher.finalize().into();
        let manifest = ArtifactManifest {
            schema: CACHE_SCHEMA.to_string(),
            schema_version: CACHE_SCHEMA_VERSION,
            identity: descriptor.identity.to_string(),
            cache_identity: descriptor.cache_identity.to_string(),
            descriptor: ManifestDescriptor::from_descriptor(descriptor),
            payload_sha256: encode_hex(&payload_sha256),
            payload_bytes: offset,
            segments: manifest_segments,
        };
        let manifest_path = staging.path().join(MANIFEST_FILE);
        let manifest_file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&manifest_path)?;
        let mut manifest_output = BufWriter::new(manifest_file);
        serde_json::to_writer_pretty(&mut manifest_output, &manifest)?;
        manifest_output.write_all(b"\n")?;
        manifest_output.flush()?;
        manifest_output.get_ref().sync_all()?;
        drop(manifest_output);
        sync_directory(staging.path())?;

        let incoming_bytes = directory_size(staging.path())?;
        if incoming_bytes > reservation.entry_bytes {
            return Err(PreparedArtifactError::ManifestReservationExceeded {
                actual: incoming_bytes,
                reserved: reservation.entry_bytes,
            });
        }
        let target = self.entry_path(descriptor.identity);
        let final_disposition = match target.symlink_metadata() {
            Ok(_) => {
                let existing = self.validate_entry(descriptor.identity, Some(descriptor))?;
                if existing.payload_sha256 != payload_sha256 {
                    return Err(PreparedArtifactError::PublicationConflict);
                }
                disposition
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                self.evict_for(descriptor.identity, incoming_bytes)?;
                let staging_path = staging.keep();
                if let Err(error) = fs::rename(&staging_path, &target) {
                    let _ = fs::remove_dir_all(staging_path);
                    return Err(error.into());
                }
                sync_directory(&self.cache)?;
                disposition
            }
            Err(error) => return Err(error.into()),
        };
        let validated = self.validate_entry(descriptor.identity, Some(descriptor))?;
        let cache_bytes = self.validate_budget_without_eviction()?;
        Ok((validated, final_disposition, cache_bytes))
    }

    fn evict_for(
        &self,
        incoming: ArtifactIdentity,
        incoming_bytes: u64,
    ) -> Result<Vec<ArtifactIdentity>, PreparedArtifactError> {
        let mut entries = self.entries()?;
        entries.remove(&incoming);
        let mut total = incoming_bytes;
        for bytes in entries.values() {
            total = total
                .checked_add(*bytes)
                .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
        }
        let mut evicted = Vec::new();
        while total > self.budget.cache_bytes || entries.len() + 1 > self.budget.entries {
            let Some((&identity, &bytes)) = entries.iter().next() else {
                return Err(PreparedArtifactError::CacheBudgetExceeded {
                    required: total,
                    budget: self.budget.cache_bytes,
                });
            };
            fs::remove_dir_all(self.entry_path(identity))?;
            entries.remove(&identity);
            total = total.saturating_sub(bytes);
            evicted.push(identity);
        }
        Ok(evicted)
    }

    fn validate_budget_without_eviction(&self) -> Result<u64, PreparedArtifactError> {
        let entries = self.entries()?;
        let total = entries.values().try_fold(0_u64, |total, bytes| {
            total
                .checked_add(*bytes)
                .ok_or(PreparedArtifactError::ArtifactTooLarge)
        })?;
        if total > self.budget.cache_bytes {
            return Err(PreparedArtifactError::CacheBudgetExceeded {
                required: total,
                budget: self.budget.cache_bytes,
            });
        }
        if entries.len() > self.budget.entries {
            return Err(PreparedArtifactError::CacheEntryBudgetExceeded {
                required: entries.len(),
                budget: self.budget.entries,
            });
        }
        Ok(total)
    }

    fn validate_raw_budget(&self, planned: ArtifactIdentity) -> Result<u64, PreparedArtifactError> {
        let mut total = 0_u64;
        let mut count = 0_usize;
        for path in directory_paths(&self.cache)? {
            let name = path
                .file_name()
                .ok_or_else(|| PreparedArtifactError::UnknownCacheEntry(path.clone()))?
                .to_string_lossy();
            let metadata = path.symlink_metadata()?;
            if name.starts_with(STAGING_PREFIX) {
                if !metadata.file_type().is_dir() {
                    return Err(PreparedArtifactError::UnknownCacheEntry(path));
                }
                continue;
            }
            let digest = decode_digest(&name)
                .filter(|digest| name == encode_hex(digest))
                .ok_or_else(|| PreparedArtifactError::UnknownCacheEntry(path.clone()))?;
            let identity = ArtifactIdentity::from_owner_digest(digest);
            let bytes = if metadata.file_type().is_dir() {
                raw_directory_size(&path)?
            } else if identity == planned {
                metadata.len()
            } else {
                return Err(PreparedArtifactError::UnknownCacheEntry(path));
            };
            total = total
                .checked_add(bytes)
                .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
            count = count
                .checked_add(1)
                .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
        }
        if total > self.budget.cache_bytes {
            return Err(PreparedArtifactError::CacheBudgetExceeded {
                required: total,
                budget: self.budget.cache_bytes,
            });
        }
        if count > self.budget.entries {
            return Err(PreparedArtifactError::CacheEntryBudgetExceeded {
                required: count,
                budget: self.budget.entries,
            });
        }
        Ok(total)
    }

    fn entries(&self) -> Result<BTreeMap<ArtifactIdentity, u64>, PreparedArtifactError> {
        let mut entries = BTreeMap::new();
        for path in directory_paths(&self.cache)? {
            let name = path
                .file_name()
                .ok_or_else(|| PreparedArtifactError::UnknownCacheEntry(path.clone()))?;
            let name = name.to_string_lossy();
            if !path.symlink_metadata()?.file_type().is_dir() {
                return Err(PreparedArtifactError::UnknownCacheEntry(path));
            }
            if name.starts_with(STAGING_PREFIX) {
                continue;
            }
            let digest = decode_digest(&name)
                .filter(|digest| name == encode_hex(digest))
                .ok_or_else(|| PreparedArtifactError::UnknownCacheEntry(path.clone()))?;
            let identity = ArtifactIdentity::from_owner_digest(digest);
            entries.insert(identity, raw_directory_size(&path)?);
        }
        Ok(entries)
    }

    fn remove_orphan_staging(&self) -> Result<(), PreparedArtifactError> {
        let mut removed = false;
        for path in directory_paths(&self.cache)? {
            if path
                .file_name()
                .ok_or_else(|| PreparedArtifactError::UnknownCacheEntry(path.clone()))?
                .to_string_lossy()
                .starts_with(STAGING_PREFIX)
            {
                if !path.symlink_metadata()?.file_type().is_dir() {
                    return Err(PreparedArtifactError::UnknownCacheEntry(path));
                }
                raw_directory_size(&path)?;
                fs::remove_dir_all(path)?;
                removed = true;
            }
        }
        if removed {
            sync_directory(&self.cache)?;
        }
        Ok(())
    }

    fn validate_entry(
        &self,
        identity: ArtifactIdentity,
        expected: Option<&PreparedArtifactDescriptor>,
    ) -> Result<ValidatedArtifact, PreparedArtifactError> {
        let mut evidence = ValidationEvidence::default();
        self.validate_entry_with_evidence(identity, expected, &mut evidence)
    }

    fn validate_entry_with_evidence(
        &self,
        identity: ArtifactIdentity,
        expected: Option<&PreparedArtifactDescriptor>,
        evidence: &mut ValidationEvidence,
    ) -> Result<ValidatedArtifact, PreparedArtifactError> {
        let directory = self.entry_path(identity);
        let directory_type = directory
            .symlink_metadata()
            .map_err(map_incomplete)?
            .file_type();
        if !directory_type.is_dir() {
            return Err(PreparedArtifactError::UnknownCacheEntry(directory));
        }
        validate_entry_inventory(&directory)?;
        let manifest_path = directory.join(MANIFEST_FILE);
        if manifest_path.symlink_metadata()?.len() > MANIFEST_RESERVATION_BYTES {
            return Err(PreparedArtifactError::InvalidManifest);
        }
        let manifest_bytes = read_file_counted(&manifest_path, evidence)?;
        let manifest: ArtifactManifest = serde_json::from_slice(&manifest_bytes)?;
        if manifest.schema != CACHE_SCHEMA || manifest.schema_version != CACHE_SCHEMA_VERSION {
            return Err(PreparedArtifactError::UnknownSchema {
                schema: manifest.schema,
                version: manifest.schema_version,
            });
        }
        let descriptor = manifest.descriptor.clone().into_descriptor()?;
        if descriptor.identity != identity
            || manifest.identity != identity.to_string()
            || manifest.cache_identity != descriptor.cache_identity().to_string()
            || descriptor.cache_scope.root_identity != self.scope.root_identity
        {
            return Err(PreparedArtifactError::IdentityMismatch);
        }
        if expected.is_some_and(|expected| expected != &descriptor) {
            return Err(PreparedArtifactError::StaleArtifact);
        }
        validate_manifest_segments(&descriptor, &manifest)?;
        let expected_payload_digest = decode_digest(&manifest.payload_sha256)
            .ok_or(PreparedArtifactError::InvalidManifest)?;
        let payload_path = directory.join(PAYLOAD_FILE);
        let disk_bytes = directory_size(&directory)?;
        let payload = File::open(&payload_path).map_err(map_incomplete)?;
        let buffer_len = streaming_buffer_len(self.budget, &descriptor)?;
        evidence.resident_buffer_bytes = buffer_len as u64;
        let (payload_sha256, payload_bytes) =
            validate_payload(&payload, &manifest.segments, buffer_len, evidence)?;
        if payload_bytes != manifest.payload_bytes || payload_sha256 != expected_payload_digest {
            return Err(PreparedArtifactError::CorruptArtifact);
        }
        Ok(ValidatedArtifact {
            manifest,
            payload,
            payload_sha256,
            disk_bytes,
            path: directory,
        })
    }

    fn entry_path(&self, identity: ArtifactIdentity) -> PathBuf {
        self.cache.join(identity.to_string())
    }

    fn lock(&self) -> Result<StoreLock<'_>, PreparedArtifactError> {
        let in_process = self
            .state
            .mutation
            .lock()
            .map_err(|_| PreparedArtifactError::PoisonedStore)?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&self.lock_path)?;
        FileExt::lock_exclusive(&file)?;
        Ok(StoreLock {
            _in_process: in_process,
            file,
        })
    }
}

struct StoreLock<'a> {
    _in_process: MutexGuard<'a, ()>,
    file: File,
}

impl Drop for StoreLock<'_> {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
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

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestDescriptor {
    implementation_registry: String,
    provider: String,
    provider_version: String,
    implementation: String,
    kind: PreparedArtifactKind,
    scientific: ScientificCommitments,
    cache_scope: CacheScope,
    segments: Vec<PreparedArtifactSegmentDescriptor>,
}

impl ManifestDescriptor {
    fn from_descriptor(descriptor: &PreparedArtifactDescriptor) -> Self {
        Self {
            implementation_registry: descriptor.owner.implementation_registry.to_string(),
            provider: descriptor.owner.provider.clone(),
            provider_version: descriptor.owner.provider_version.clone(),
            implementation: descriptor.owner.implementation.as_str().to_string(),
            kind: descriptor.kind,
            scientific: descriptor.scientific.clone(),
            cache_scope: descriptor.cache_scope.clone(),
            segments: descriptor.segments.clone(),
        }
    }

    fn into_descriptor(self) -> Result<PreparedArtifactDescriptor, PreparedArtifactError> {
        let registry = decode_digest(&self.implementation_registry)
            .map(ImplementationRegistryId::from_sha256)
            .ok_or(PreparedArtifactError::InvalidManifest)?;
        let owner = PreparedArtifactOwner::new(
            registry,
            self.provider,
            self.provider_version,
            WorkImplementationId::new(self.implementation),
        )?;
        PreparedArtifactDescriptor::from_commitments(
            owner,
            self.kind,
            self.scientific,
            self.cache_scope,
            self.segments,
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestSegment {
    descriptor: PreparedArtifactSegmentDescriptor,
    offset: u64,
    bytes: u64,
    sha256: String,
}

struct ValidatedArtifact {
    manifest: ArtifactManifest,
    payload: File,
    payload_sha256: [u8; 32],
    disk_bytes: u64,
    path: PathBuf,
}

impl ValidatedArtifact {
    fn into_handle<'lease>(
        self,
        descriptor: &PreparedArtifactDescriptor,
        _lease: &'lease WorkExecutionContext<'_>,
    ) -> PreparedArtifact<'lease> {
        PreparedArtifact {
            identity: descriptor.identity,
            integrity_identity: derive_content_identity(descriptor, self.payload_sha256),
            cache_identity: descriptor.cache_identity(),
            payload: self.payload,
            segments: self
                .manifest
                .segments
                .into_iter()
                .map(|segment| (segment.descriptor.name, (segment.offset, segment.bytes)))
                .collect(),
            _lease: PhantomData,
        }
    }
}

/// Typed fail-closed prepared-artifact error.
#[derive(Debug)]
pub enum PreparedArtifactError {
    /// Private-cache or payload I/O failed.
    Io(io::Error),
    /// The private manifest could not be encoded or decoded.
    Json(serde_json::Error),
    /// One hard cache bound was zero.
    InvalidBudget,
    /// Owner identifiers were absent or invalid.
    InvalidOwner,
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
    /// The cache node omitted an exact resource, storage, lock, or I/O reservation.
    MissingReservation(&'static str),
    /// The node attempted to combine prepared-cache work with product publication.
    ProductAuthorityViolation,
    /// Named byte streams did not exactly match the canonical descriptor order.
    SegmentMismatch,
    /// A requested immutable segment does not exist.
    UnknownSegment(String),
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
    /// The configured path is or contains a CASA-visible cache name.
    CasaVisiblePath(PathBuf),
    /// An in-process cache lock was poisoned by a prior panic.
    PoisonedStore,
}

impl fmt::Display for PreparedArtifactError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "prepared-artifact I/O failed: {error}"),
            Self::Json(error) => write!(formatter, "prepared-artifact manifest failed: {error}"),
            Self::InvalidBudget => formatter.write_str("prepared-artifact budget must be positive"),
            Self::InvalidOwner => formatter.write_str("prepared-artifact owner is invalid"),
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
            Self::MissingReservation(kind) => {
                write!(
                    formatter,
                    "prepared-artifact node lacks its {kind} reservation"
                )
            }
            Self::ProductAuthorityViolation => formatter
                .write_str("prepared-artifact cache work cannot own Product Graph publication"),
            Self::SegmentMismatch => formatter
                .write_str("prepared-artifact streams do not match the canonical named segments"),
            Self::UnknownSegment(segment) => {
                write!(formatter, "unknown prepared-artifact segment {segment:?}")
            }
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
                "prepared-artifact cache path is CASA-visible: {}",
                path.display()
            ),
            Self::PoisonedStore => formatter.write_str("prepared-artifact cache lock is poisoned"),
        }
    }
}

impl Error for PreparedArtifactError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Json(error) => Some(error),
            _ => None,
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
        mutation: Mutex::new(()),
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
    if imaging.uv_affine.as_ref().map(|uv| uv.reference_value_bits)
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
    hash_bytes(&mut hasher, owner.provider.as_bytes())?;
    hash_bytes(&mut hasher, owner.provider_version.as_bytes())?;
    hash_bytes(&mut hasher, owner.implementation.as_str().as_bytes())?;
    hasher.update(
        decode_digest(&scope.root_identity).ok_or(PreparedArtifactError::InvalidDescriptor)?,
    );
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
    hasher.update(descriptor.cache_identity.as_bytes());
    hasher.update([kind_tag(descriptor.kind)]);
    for identity in [
        &descriptor.scientific.compiled_problem,
        &descriptor.scientific.observation_snapshot,
        &descriptor.scientific.compiled_geometry,
        &descriptor.scientific.numerics_contract,
    ] {
        hasher.update(decode_digest(identity).ok_or(PreparedArtifactError::InvalidDescriptor)?);
    }
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

fn validate_plan_binding(
    context: WorkExecutionContext<'_>,
    descriptor: &PreparedArtifactDescriptor,
    operation: PreparedArtifactOperation,
    reservation: PreparedArtifactReservation,
) -> Result<(), PreparedArtifactError> {
    if !descriptor.scientific.matches_context(context) {
        return Err(PreparedArtifactError::ScientificBindingMismatch);
    }
    let planned = context.planned_artifacts().cloned().collect::<Vec<_>>();
    validate_plan_declaration(
        context.node(),
        &planned,
        context.stage_prediction(),
        descriptor,
        operation,
        reservation,
    )
}

fn validate_plan_declaration(
    node: &crate::WorkNode,
    planned: &[PlannedArtifact],
    stage: &crate::StagePrediction,
    descriptor: &PreparedArtifactDescriptor,
    operation: PreparedArtifactOperation,
    reservation: PreparedArtifactReservation,
) -> Result<(), PreparedArtifactError> {
    if node.kind != WorkKind::Cache {
        return Err(PreparedArtifactError::UnplannedOperation);
    }
    if node.id != descriptor.work_node_id(operation)
        || node.implementation != descriptor.work_implementation_id(operation)
    {
        return Err(PreparedArtifactError::UnplannedOperation);
    }
    if planned
        .iter()
        .any(|artifact| artifact.role() == ArtifactRole::Output)
    {
        return Err(PreparedArtifactError::ProductAuthorityViolation);
    }
    if planned.len() != 1 {
        return Err(PreparedArtifactError::UnplannedOperation);
    }
    let expected = descriptor.planned_artifact(operation);
    let artifact = &planned[0];
    if artifact.identity() != descriptor.identity
        || artifact.cache_identity() != Some(descriptor.cache_identity())
        || artifact.role() != expected.role()
        || artifact.node() != expected.node()
    {
        return Err(PreparedArtifactError::UnplannedOperation);
    }
    require_claim(
        node,
        |resource| matches!(resource, LeaseResource::Workers),
        1,
        "worker",
    )?;
    require_claim(
        node,
        |resource| matches!(resource, LeaseResource::ResidentCache),
        reservation.resident_buffer_bytes,
        "resident cache",
    )?;
    require_claim(
        node,
        |resource| {
            matches!(
                resource,
                LeaseResource::IoBuffer(IoBufferKind::MappedPageCache)
            )
        },
        reservation.resident_buffer_bytes,
        "mapped-page-cache buffer",
    )?;
    require_claim(
        node,
        |resource| matches!(resource, LeaseResource::Locks),
        1,
        "private-cache lock",
    )?;
    require_claim(
        node,
        |resource| matches!(resource, LeaseResource::FileDescriptors),
        2,
        "file descriptors",
    )?;
    require_claim(
        node,
        |resource| {
            matches!(
                resource,
                LeaseResource::Storage {
                    use_kind: StorageUseKind::PersistentCache,
                    ..
                }
            )
        },
        reservation.persistent_cache_bytes,
        "persistent-cache storage",
    )?;
    if reservation.temporary_staging_bytes > 0 {
        require_claim(
            node,
            |resource| {
                matches!(
                    resource,
                    LeaseResource::Storage {
                        use_kind: StorageUseKind::Temporary,
                        ..
                    }
                )
            },
            reservation.temporary_staging_bytes,
            "temporary staging storage",
        )?;
    }
    let io = stage.io();
    if io.len() != 1
        || io[0].kind() != IoBufferKind::MappedPageCache
        || io[0].bytes() < descriptor.payload_bytes()?
    {
        return Err(PreparedArtifactError::MissingReservation(
            "cache I/O prediction",
        ));
    }
    Ok(())
}

fn require_claim(
    node: &crate::WorkNode,
    predicate: impl Fn(&LeaseResource) -> bool,
    required: u64,
    label: &'static str,
) -> Result<(), PreparedArtifactError> {
    let amount = node
        .claims
        .iter()
        .filter(|claim| predicate(&claim.resource))
        .try_fold(0_u64, |total, claim| total.checked_add(claim.amount))
        .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
    if amount < required {
        Err(PreparedArtifactError::MissingReservation(label))
    } else {
        Ok(())
    }
}

fn measurements(
    context: WorkExecutionContext<'_>,
    descriptor: &PreparedArtifactDescriptor,
    disposition: ArtifactDisposition,
    validated: &ValidatedArtifact,
    reservation: PreparedArtifactReservation,
    operation: PreparedArtifactOperation,
    cache_bytes: u64,
) -> WorkMeasurements {
    let resources = resource_measurements(
        context,
        operation,
        cache_bytes,
        validated.disk_bytes,
        reservation.resident_buffer_bytes,
    );
    let io = context
        .stage_prediction()
        .io()
        .iter()
        .map(|prediction| {
            IoMeasurement::new(
                prediction.kind(),
                validated.manifest.payload_bytes,
                u64::try_from(validated.manifest.segments.len() + 1).unwrap_or(u64::MAX),
            )
        })
        .collect();
    let artifacts = vec![ArtifactMeasurement::new(
        descriptor.identity,
        Some(derive_content_identity(
            descriptor,
            validated.payload_sha256,
        )),
        disposition,
        validated.manifest.payload_bytes,
        Some(RedactedPath::from_path(&validated.path)),
    )];
    WorkMeasurements::new(resources, io, artifacts)
}

fn rejected_measurements(
    context: WorkExecutionContext<'_>,
    descriptor: &PreparedArtifactDescriptor,
    reservation: PreparedArtifactReservation,
    rejection: PreparedArtifactRejection,
    path: &Path,
    cache_bytes: u64,
    evidence: ValidationEvidence,
) -> WorkMeasurements {
    let resources = resource_measurements(
        context,
        PreparedArtifactOperation::Reuse,
        cache_bytes,
        0,
        evidence
            .resident_buffer_bytes
            .min(reservation.resident_buffer_bytes),
    );
    let io = context
        .stage_prediction()
        .io()
        .iter()
        .map(|prediction| {
            IoMeasurement::new(
                prediction.kind(),
                evidence.bytes_read,
                evidence.operations.max(1),
            )
        })
        .collect();
    let artifacts = vec![ArtifactMeasurement::new(
        descriptor.identity,
        Some(rejection.evidence_identity(descriptor.identity)),
        ArtifactDisposition::RejectedStale,
        evidence.bytes_read,
        Some(RedactedPath::from_path(path)),
    )];
    WorkMeasurements::new(resources, io, artifacts)
}

fn resource_measurements(
    context: WorkExecutionContext<'_>,
    operation: PreparedArtifactOperation,
    cache_bytes: u64,
    entry_bytes: u64,
    resident_buffer_bytes: u64,
) -> Vec<ResourceMeasurement> {
    context
        .resources()
        .iter()
        .map(|capability| {
            let peak = match capability.resource() {
                LeaseResource::Workers | LeaseResource::Locks => 1,
                LeaseResource::FileDescriptors => 2,
                LeaseResource::ResidentCache
                | LeaseResource::IoBuffer(IoBufferKind::MappedPageCache) => resident_buffer_bytes,
                LeaseResource::Storage {
                    use_kind: StorageUseKind::PersistentCache,
                    ..
                } => cache_bytes,
                LeaseResource::Storage {
                    use_kind: StorageUseKind::Temporary,
                    ..
                } if operation != PreparedArtifactOperation::Reuse => entry_bytes,
                _ => 0,
            };
            ResourceMeasurement::new(
                capability.resource().clone(),
                capability.lifetime().clone(),
                peak,
            )
        })
        .collect()
}

fn rejection_for(error: &PreparedArtifactError) -> Option<PreparedArtifactRejection> {
    match error {
        PreparedArtifactError::IncompleteArtifact | PreparedArtifactError::UnknownCacheEntry(_) => {
            Some(PreparedArtifactRejection::Incomplete)
        }
        PreparedArtifactError::InvalidOwner
        | PreparedArtifactError::InvalidDescriptor
        | PreparedArtifactError::InvalidLayout
        | PreparedArtifactError::InvalidUvAffine
        | PreparedArtifactError::ArtifactTooLarge
        | PreparedArtifactError::UnknownSchema { .. }
        | PreparedArtifactError::InvalidManifest
        | PreparedArtifactError::IdentityMismatch
        | PreparedArtifactError::StaleArtifact
        | PreparedArtifactError::SegmentLayoutMismatch => {
            Some(PreparedArtifactRejection::Incompatible)
        }
        PreparedArtifactError::Json(error) if !error.is_io() => {
            Some(PreparedArtifactRejection::Incompatible)
        }
        PreparedArtifactError::CorruptArtifact | PreparedArtifactError::OversizedArtifact => {
            Some(PreparedArtifactRejection::Corrupt)
        }
        PreparedArtifactError::NonFiniteValue { .. } => Some(PreparedArtifactRejection::NonFinite),
        _ => None,
    }
}

fn validate_segment_inputs(
    descriptor: &PreparedArtifactDescriptor,
    inputs: &[PreparedArtifactSegmentInput<'_>],
) -> Result<(), PreparedArtifactError> {
    if inputs.len() != descriptor.segments.len()
        || descriptor
            .segments
            .iter()
            .zip(inputs)
            .any(|(segment, input)| segment.name != input.name)
    {
        Err(PreparedArtifactError::SegmentMismatch)
    } else {
        Ok(())
    }
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
            .min(COPY_BUFFER_CEILING as u64),
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
    input: &mut dyn Read,
    output: &mut dyn Write,
    payload_hasher: &mut Sha256,
    buffer: &mut [u8],
    segment: &PreparedArtifactSegmentDescriptor,
) -> Result<[u8; 32], PreparedArtifactError> {
    let mut remaining = segment.byte_len()?;
    let scalar_bytes = segment.precision.scalar_bytes();
    let mut scalar = 0_u64;
    let mut segment_hasher = Sha256::new();
    while remaining > 0 {
        let mut limit = usize::try_from(remaining.min(buffer.len() as u64))
            .map_err(|_| PreparedArtifactError::ArtifactTooLarge)?;
        limit -= limit % scalar_bytes;
        input
            .read_exact(&mut buffer[..limit])
            .map_err(map_incomplete)?;
        validate_finite(&buffer[..limit], segment.precision, &segment.name, scalar)?;
        output.write_all(&buffer[..limit])?;
        payload_hasher.update(&buffer[..limit]);
        segment_hasher.update(&buffer[..limit]);
        remaining -= limit as u64;
        scalar += (limit / scalar_bytes) as u64;
    }
    let mut extra = [0_u8; 1];
    if input.read(&mut extra)? != 0 {
        return Err(PreparedArtifactError::OversizedArtifact);
    }
    Ok(segment_hasher.finalize().into())
}

fn validate_payload(
    payload: &File,
    segments: &[ManifestSegment],
    buffer_len: usize,
    evidence: &mut ValidationEvidence,
) -> Result<([u8; 32], u64), PreparedArtifactError> {
    let mut payload = payload;
    let mut payload_hasher = Sha256::new();
    let mut total = 0_u64;
    let mut buffer = vec![0_u8; buffer_len];
    for segment in segments {
        let expected_segment_digest =
            decode_digest(&segment.sha256).ok_or(PreparedArtifactError::InvalidManifest)?;
        let mut segment_hasher = Sha256::new();
        let mut remaining = segment.bytes;
        let scalar_bytes = segment.descriptor.precision.scalar_bytes();
        let mut scalar = 0_u64;
        while remaining > 0 {
            let mut limit = usize::try_from(remaining.min(buffer.len() as u64))
                .map_err(|_| PreparedArtifactError::ArtifactTooLarge)?;
            limit -= limit % scalar_bytes;
            read_exact_counted(&mut payload, &mut buffer[..limit], evidence)?;
            validate_finite(
                &buffer[..limit],
                segment.descriptor.precision,
                &segment.descriptor.name,
                scalar,
            )?;
            payload_hasher.update(&buffer[..limit]);
            segment_hasher.update(&buffer[..limit]);
            remaining -= limit as u64;
            scalar += (limit / scalar_bytes) as u64;
        }
        if <[u8; 32]>::from(segment_hasher.finalize()) != expected_segment_digest {
            return Err(PreparedArtifactError::CorruptArtifact);
        }
        total = total
            .checked_add(segment.bytes)
            .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
    }
    let mut extra = [0_u8; 1];
    if read_counted(&mut payload, &mut extra, evidence)? != 0 {
        return Err(PreparedArtifactError::OversizedArtifact);
    }
    Ok((payload_hasher.finalize().into(), total))
}

#[derive(Clone, Copy, Debug, Default)]
struct ValidationEvidence {
    bytes_read: u64,
    operations: u64,
    resident_buffer_bytes: u64,
}

impl ValidationEvidence {
    fn metadata_probe() -> Self {
        Self {
            operations: 1,
            ..Self::default()
        }
    }
}

fn read_file_counted(
    path: &Path,
    evidence: &mut ValidationEvidence,
) -> Result<Vec<u8>, PreparedArtifactError> {
    let mut file = File::open(path).map_err(map_incomplete)?;
    let mut contents = Vec::new();
    let mut buffer = [0_u8; 4096];
    loop {
        let bytes = read_counted(&mut file, &mut buffer, evidence)?;
        if bytes == 0 {
            break;
        }
        contents.extend_from_slice(&buffer[..bytes]);
    }
    Ok(contents)
}

fn read_exact_counted<R: Read + ?Sized>(
    input: &mut R,
    output: &mut [u8],
    evidence: &mut ValidationEvidence,
) -> Result<(), PreparedArtifactError> {
    let mut offset = 0;
    while offset < output.len() {
        let bytes = read_counted(input, &mut output[offset..], evidence)?;
        if bytes == 0 {
            return Err(PreparedArtifactError::IncompleteArtifact);
        }
        offset += bytes;
    }
    Ok(())
}

fn read_counted<R: Read + ?Sized>(
    input: &mut R,
    output: &mut [u8],
    evidence: &mut ValidationEvidence,
) -> Result<usize, PreparedArtifactError> {
    let bytes = input.read(output).map_err(map_incomplete)?;
    evidence.operations = evidence.operations.saturating_add(1);
    evidence.bytes_read = evidence.bytes_read.saturating_add(bytes as u64);
    Ok(bytes)
}

fn validate_finite(
    bytes: &[u8],
    precision: PreparedArtifactPrecision,
    segment: &str,
    first_scalar: u64,
) -> Result<(), PreparedArtifactError> {
    match precision {
        PreparedArtifactPrecision::F32 | PreparedArtifactPrecision::ComplexF32 => {
            for (offset, chunk) in bytes.chunks_exact(4).enumerate() {
                if !f32::from_le_bytes(chunk.try_into().expect("exact f32 chunk")).is_finite() {
                    return Err(PreparedArtifactError::NonFiniteValue {
                        segment: segment.to_string(),
                        scalar: first_scalar + offset as u64,
                    });
                }
            }
        }
        PreparedArtifactPrecision::F64 | PreparedArtifactPrecision::ComplexF64 => {
            for (offset, chunk) in bytes.chunks_exact(8).enumerate() {
                if !f64::from_le_bytes(chunk.try_into().expect("exact f64 chunk")).is_finite() {
                    return Err(PreparedArtifactError::NonFiniteValue {
                        segment: segment.to_string(),
                        scalar: first_scalar + offset as u64,
                    });
                }
            }
        }
        PreparedArtifactPrecision::I32
        | PreparedArtifactPrecision::U32
        | PreparedArtifactPrecision::U8 => {}
    }
    Ok(())
}

fn validate_manifest_segments(
    descriptor: &PreparedArtifactDescriptor,
    manifest: &ArtifactManifest,
) -> Result<(), PreparedArtifactError> {
    if manifest.segments.len() != descriptor.segments.len() {
        return Err(PreparedArtifactError::SegmentLayoutMismatch);
    }
    let mut offset = 0_u64;
    for (expected, actual) in descriptor.segments.iter().zip(&manifest.segments) {
        if &actual.descriptor != expected
            || actual.offset != offset
            || actual.bytes != expected.byte_len()?
            || decode_digest(&actual.sha256).is_none()
        {
            return Err(PreparedArtifactError::SegmentLayoutMismatch);
        }
        offset = offset
            .checked_add(actual.bytes)
            .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
    }
    if offset != manifest.payload_bytes || offset != descriptor.payload_bytes()? {
        return Err(PreparedArtifactError::SegmentLayoutMismatch);
    }
    Ok(())
}

fn validate_entry_inventory(directory: &Path) -> Result<(), PreparedArtifactError> {
    let mut names = BTreeSet::new();
    for path in directory_paths(directory).map_err(|error| match error {
        PreparedArtifactError::Io(error) => map_incomplete(error),
        other => other,
    })? {
        if !path.symlink_metadata()?.file_type().is_file() {
            return Err(PreparedArtifactError::UnknownCacheEntry(path));
        }
        names.insert(
            path.file_name()
                .ok_or_else(|| PreparedArtifactError::UnknownCacheEntry(path.clone()))?
                .to_owned(),
        );
    }
    let expected = BTreeSet::from([MANIFEST_FILE.into(), PAYLOAD_FILE.into()]);
    if names != expected {
        return Err(PreparedArtifactError::IncompleteArtifact);
    }
    Ok(())
}

fn reject_casa_visible_root(path: &Path) -> Result<(), PreparedArtifactError> {
    if path.components().any(|component| {
        let name = component.as_os_str().to_string_lossy();
        casa_visible_name(&name)
    }) {
        Err(PreparedArtifactError::CasaVisiblePath(path.to_path_buf()))
    } else {
        Ok(())
    }
}

fn reject_casa_cache_contents(root: &Path) -> Result<(), PreparedArtifactError> {
    for path in directory_paths(root)? {
        let name = path
            .file_name()
            .ok_or_else(|| PreparedArtifactError::UnknownCacheEntry(path.clone()))?;
        if casa_visible_name(&name.to_string_lossy()) {
            return Err(PreparedArtifactError::CasaVisiblePath(path));
        }
    }
    Ok(())
}

fn casa_visible_name(name: &str) -> bool {
    (name.starts_with("CFS_") || name.starts_with("WTCFS_")) && name.contains(".im")
}

fn valid_segment_name(name: &str) -> bool {
    valid_identifier(name)
        && name != MANIFEST_FILE
        && name != PAYLOAD_FILE
        && !name.starts_with("CFS_")
        && !name.starts_with("WTCFS_")
        && !name.ends_with(".im")
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'@'))
}

fn checked_product(values: &[u64]) -> Option<u64> {
    values
        .iter()
        .try_fold(1_u64, |product, value| product.checked_mul(*value))
}

fn directory_size(path: &Path) -> Result<u64, PreparedArtifactError> {
    directory_paths(path)?
        .into_iter()
        .try_fold(0_u64, |total, entry| {
            let metadata = entry.symlink_metadata()?;
            if !metadata.file_type().is_file() {
                return Err(PreparedArtifactError::UnknownCacheEntry(entry));
            }
            total
                .checked_add(metadata.len())
                .ok_or(PreparedArtifactError::ArtifactTooLarge)
        })
}

fn raw_directory_size(path: &Path) -> Result<u64, PreparedArtifactError> {
    let mut pending = vec![path.to_path_buf()];
    let mut total = 0_u64;
    while let Some(directory) = pending.pop() {
        for entry in directory_paths(&directory)? {
            reject_casa_visible_root(&entry)?;
            let metadata = entry.symlink_metadata()?;
            if metadata.file_type().is_dir() {
                pending.push(entry);
            } else {
                total = total
                    .checked_add(metadata.len())
                    .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
            }
        }
    }
    Ok(total)
}

fn directory_paths(path: &Path) -> Result<Vec<PathBuf>, PreparedArtifactError> {
    fs::read_dir(path)?
        .map(|entry| entry.map(|entry| entry.path()).map_err(Into::into))
        .collect()
}

fn sync_directory(path: &Path) -> Result<(), PreparedArtifactError> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn map_incomplete(error: io::Error) -> PreparedArtifactError {
    if error.kind() == io::ErrorKind::UnexpectedEof || error.kind() == io::ErrorKind::NotFound {
        PreparedArtifactError::IncompleteArtifact
    } else {
        error.into()
    }
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
