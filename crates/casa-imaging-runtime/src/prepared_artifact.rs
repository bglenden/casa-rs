// SPDX-License-Identifier: LGPL-3.0-or-later

//! Plan-bound private persistence for immutable implementation preparation.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard, OnceLock, Weak},
};

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
const IDENTITY_VERSION: u32 = 1;
const CACHE_SCHEMA: &str = "casa-rs-private-prepared-artifact";
const CACHE_SCHEMA_VERSION: u32 = 1;
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
    cache_identity: CacheIdentity,
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
        let mut owner = Self {
            implementation_registry,
            provider,
            provider_version,
            implementation,
            cache_identity: CacheIdentity::from_owner_digest([0; 32]),
        };
        owner.cache_identity = derive_cache_identity(&owner)?;
        Ok(owner)
    }

    /// Return the canonical cache namespace identity derived by this owner.
    #[must_use]
    pub const fn cache_identity(&self) -> CacheIdentity {
        self.cache_identity
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

/// Owner-derived immutable compatibility descriptor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedArtifactDescriptor {
    identity: ArtifactIdentity,
    owner: PreparedArtifactOwner,
    kind: PreparedArtifactKind,
    scientific_inputs: Vec<ArtifactIdentity>,
    segments: Vec<PreparedArtifactSegmentDescriptor>,
}

impl PreparedArtifactDescriptor {
    /// Describe an asymmetric named imaging/weight CF pair.
    pub fn convolution_function(
        owner: PreparedArtifactOwner,
        scientific_inputs: impl IntoIterator<Item = ArtifactIdentity>,
        imaging: PreparedArtifactPlaneDescriptor,
        weight: PreparedArtifactPlaneDescriptor,
    ) -> Result<Self, PreparedArtifactError> {
        Self::new(
            owner,
            PreparedArtifactKind::ConvolutionFunction,
            scientific_inputs,
            vec![
                imaging.into_segment("imaging"),
                weight.into_segment("weight"),
            ],
        )
    }

    /// Describe a spectral map or other kernel through private named segments.
    pub fn new(
        owner: PreparedArtifactOwner,
        kind: PreparedArtifactKind,
        scientific_inputs: impl IntoIterator<Item = ArtifactIdentity>,
        mut segments: Vec<PreparedArtifactSegmentDescriptor>,
    ) -> Result<Self, PreparedArtifactError> {
        let mut scientific_inputs = scientific_inputs.into_iter().collect::<Vec<_>>();
        scientific_inputs.sort_unstable();
        if scientific_inputs.is_empty()
            || scientific_inputs
                .iter()
                .any(|identity| identity.as_bytes() == [0; 32])
            || scientific_inputs.windows(2).any(|pair| pair[0] == pair[1])
            || segments.is_empty()
        {
            return Err(PreparedArtifactError::InvalidDescriptor);
        }
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
        let mut descriptor = Self {
            identity: ArtifactIdentity::from_owner_digest([0; 32]),
            owner,
            kind,
            scientific_inputs,
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
        self.owner.cache_identity
    }

    /// Return the implementation-preparation family.
    #[must_use]
    pub const fn kind(&self) -> PreparedArtifactKind {
        self.kind
    }

    /// Return every canonical scientific input commitment.
    #[must_use]
    pub fn scientific_inputs(&self) -> &[ArtifactIdentity] {
        &self.scientific_inputs
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
    pub fn planned_artifact(
        &self,
        node: WorkNodeId,
        operation: PreparedArtifactOperation,
    ) -> PlannedArtifact {
        let role = match operation {
            PreparedArtifactOperation::Generate | PreparedArtifactOperation::Load => {
                ArtifactRole::Prepared
            }
            PreparedArtifactOperation::Reuse => ArtifactRole::Cache,
        };
        PlannedArtifact::new(self.identity, node, role, Some(self.owner.cache_identity))
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

/// Validated presence observed during physical planning.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreparedArtifactAvailability {
    /// No published entry exists for this exact descriptor.
    Cold,
    /// A complete exact entry passed integrity validation.
    Warm,
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
    temporary_staging_bytes: u64,
    resident_buffer_bytes: u64,
    cache_entries: u64,
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

    /// Return the cache entry-count claim.
    #[must_use]
    pub const fn cache_entries(self) -> u64 {
        self.cache_entries
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
pub struct PreparedArtifact {
    identity: ArtifactIdentity,
    integrity_identity: ArtifactIdentity,
    cache_identity: CacheIdentity,
    payload: File,
    segments: BTreeMap<String, (u64, u64)>,
}

impl fmt::Debug for PreparedArtifact {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedArtifact")
            .field("identity", &self.identity)
            .field("cache_identity", &self.cache_identity)
            .field("segments", &self.segments)
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
        let lock_path = root.join(LOCK_FILE);
        let state = root_state(&root)?;
        let store = Self {
            root,
            cache,
            lock_path,
            budget,
            state,
        };
        {
            let _lock = store.lock()?;
            store.remove_orphan_staging()?;
            store.enforce_budget()?;
        }
        Ok(store)
    }

    /// Return the explicit private root, which is never a CASA cache path.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Validate an exact hit or report a cold miss during planning.
    pub fn availability(
        &self,
        descriptor: &PreparedArtifactDescriptor,
    ) -> Result<PreparedArtifactAvailability, PreparedArtifactError> {
        let _lock = self.lock()?;
        let path = self.entry_path(descriptor.identity);
        if path.exists() {
            self.validate_entry(descriptor.identity, Some(descriptor))?;
            Ok(PreparedArtifactAvailability::Warm)
        } else {
            Ok(PreparedArtifactAvailability::Cold)
        }
    }

    /// Derive exact resource/storage bounds for one explicit cache operation.
    pub fn reservation(
        &self,
        descriptor: &PreparedArtifactDescriptor,
        operation: PreparedArtifactOperation,
    ) -> Result<PreparedArtifactReservation, PreparedArtifactError> {
        let payload_bytes = descriptor.payload_bytes()?;
        let persistent_cache_bytes = payload_bytes
            .checked_add(MANIFEST_RESERVATION_BYTES)
            .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
        if persistent_cache_bytes > self.budget.cache_bytes {
            return Err(PreparedArtifactError::CacheBudgetExceeded {
                required: persistent_cache_bytes,
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
            persistent_cache_bytes,
            temporary_staging_bytes: if operation == PreparedArtifactOperation::Reuse {
                0
            } else {
                persistent_cache_bytes
            },
            resident_buffer_bytes: payload_bytes.min(self.budget.streaming_buffer_bytes),
            cache_entries: 1,
        })
    }

    /// Generate, validate, and atomically publish exact cold bytes.
    pub fn generate(
        &self,
        context: WorkExecutionContext<'_>,
        descriptor: &PreparedArtifactDescriptor,
        segments: &mut [PreparedArtifactSegmentInput<'_>],
    ) -> Result<(PreparedArtifact, WorkMeasurements), PreparedArtifactError> {
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
    pub fn load(
        &self,
        context: WorkExecutionContext<'_>,
        descriptor: &PreparedArtifactDescriptor,
        segments: &mut [PreparedArtifactSegmentInput<'_>],
    ) -> Result<(PreparedArtifact, WorkMeasurements), PreparedArtifactError> {
        self.publish(
            context,
            descriptor,
            PreparedArtifactOperation::Load,
            ArtifactDisposition::Loaded,
            segments,
        )
    }

    /// Revalidate and reuse the exact warm artifact selected by planning.
    pub fn reuse(
        &self,
        context: WorkExecutionContext<'_>,
        descriptor: &PreparedArtifactDescriptor,
    ) -> Result<(PreparedArtifact, WorkMeasurements), PreparedArtifactError> {
        let reservation = self.reservation(descriptor, PreparedArtifactOperation::Reuse)?;
        validate_plan_binding(
            context,
            descriptor,
            PreparedArtifactOperation::Reuse,
            reservation,
        )?;
        let _lock = self.lock()?;
        let validated = self.validate_entry(descriptor.identity, Some(descriptor))?;
        let measurements = measurements(
            context,
            descriptor,
            ArtifactDisposition::Reused,
            &validated,
            reservation,
            PreparedArtifactOperation::Reuse,
        );
        Ok((validated.into_handle(descriptor), measurements))
    }

    fn publish(
        &self,
        context: WorkExecutionContext<'_>,
        descriptor: &PreparedArtifactDescriptor,
        operation: PreparedArtifactOperation,
        disposition: ArtifactDisposition,
        segments: &mut [PreparedArtifactSegmentInput<'_>],
    ) -> Result<(PreparedArtifact, WorkMeasurements), PreparedArtifactError> {
        let reservation = self.reservation(descriptor, operation)?;
        validate_plan_binding(context, descriptor, operation, reservation)?;
        let (validated, final_disposition) =
            self.publish_bytes(descriptor, disposition, segments, reservation)?;
        let measurements = measurements(
            context,
            descriptor,
            final_disposition,
            &validated,
            reservation,
            operation,
        );
        Ok((validated.into_handle(descriptor), measurements))
    }

    fn publish_bytes(
        &self,
        descriptor: &PreparedArtifactDescriptor,
        disposition: ArtifactDisposition,
        segments: &mut [PreparedArtifactSegmentInput<'_>],
        reservation: PreparedArtifactReservation,
    ) -> Result<(ValidatedArtifact, ArtifactDisposition), PreparedArtifactError> {
        validate_segment_inputs(descriptor, segments)?;
        let _lock = self.lock()?;
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
            cache_identity: descriptor.owner.cache_identity.to_string(),
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
        if incoming_bytes > reservation.persistent_cache_bytes {
            return Err(PreparedArtifactError::ManifestReservationExceeded {
                actual: incoming_bytes,
                reserved: reservation.persistent_cache_bytes,
            });
        }
        let target = self.entry_path(descriptor.identity);
        let final_disposition = if target.exists() {
            let existing = self.validate_entry(descriptor.identity, Some(descriptor))?;
            if existing.payload_sha256 != payload_sha256 {
                return Err(PreparedArtifactError::PublicationConflict);
            }
            ArtifactDisposition::Reused
        } else {
            self.evict_for(descriptor.identity, incoming_bytes)?;
            let staging_path = staging.keep();
            if let Err(error) = fs::rename(&staging_path, &target) {
                let _ = fs::remove_dir_all(staging_path);
                return Err(error.into());
            }
            sync_directory(&self.cache)?;
            disposition
        };
        let validated = self.validate_entry(descriptor.identity, Some(descriptor))?;
        Ok((validated, final_disposition))
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

    fn enforce_budget(&self) -> Result<Vec<ArtifactIdentity>, PreparedArtifactError> {
        let mut entries = self.entries()?;
        let mut total = entries.values().try_fold(0_u64, |total, bytes| {
            total
                .checked_add(*bytes)
                .ok_or(PreparedArtifactError::ArtifactTooLarge)
        })?;
        let mut evicted = Vec::new();
        while total > self.budget.cache_bytes || entries.len() > self.budget.entries {
            let (&identity, &bytes) =
                entries
                    .iter()
                    .next()
                    .ok_or(PreparedArtifactError::CacheBudgetExceeded {
                        required: total,
                        budget: self.budget.cache_bytes,
                    })?;
            fs::remove_dir_all(self.entry_path(identity))?;
            entries.remove(&identity);
            total = total.saturating_sub(bytes);
            evicted.push(identity);
        }
        if !evicted.is_empty() {
            sync_directory(&self.cache)?;
        }
        Ok(evicted)
    }

    fn entries(&self) -> Result<BTreeMap<ArtifactIdentity, u64>, PreparedArtifactError> {
        let mut entries = BTreeMap::new();
        for entry in fs::read_dir(&self.cache)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with(STAGING_PREFIX) {
                continue;
            }
            if !entry.file_type()?.is_dir() {
                return Err(PreparedArtifactError::UnknownCacheEntry(entry.path()));
            }
            let identity = decode_digest(&name)
                .map(ArtifactIdentity::from_owner_digest)
                .ok_or_else(|| PreparedArtifactError::UnknownCacheEntry(entry.path()))?;
            let validated = self.validate_entry(identity, None)?;
            entries.insert(identity, validated.disk_bytes);
        }
        Ok(entries)
    }

    fn remove_orphan_staging(&self) -> Result<(), PreparedArtifactError> {
        let mut removed = false;
        for entry in fs::read_dir(&self.cache)? {
            let entry = entry?;
            if entry
                .file_name()
                .to_string_lossy()
                .starts_with(STAGING_PREFIX)
            {
                if !entry.file_type()?.is_dir() {
                    return Err(PreparedArtifactError::UnknownCacheEntry(entry.path()));
                }
                fs::remove_dir_all(entry.path())?;
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
        let directory = self.entry_path(identity);
        validate_entry_inventory(&directory)?;
        let manifest_path = directory.join(MANIFEST_FILE);
        if manifest_path.metadata()?.len() > MANIFEST_RESERVATION_BYTES {
            return Err(PreparedArtifactError::InvalidManifest);
        }
        let manifest: ArtifactManifest = serde_json::from_reader(BufReader::new(
            File::open(&manifest_path).map_err(map_incomplete)?,
        ))?;
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
        let payload = File::open(&payload_path).map_err(map_incomplete)?;
        let buffer_len = streaming_buffer_len(self.budget, &descriptor)?;
        let (payload_sha256, payload_bytes) =
            validate_payload(&payload, &manifest.segments, buffer_len)?;
        if payload_bytes != manifest.payload_bytes || payload_sha256 != expected_payload_digest {
            return Err(PreparedArtifactError::CorruptArtifact);
        }
        Ok(ValidatedArtifact {
            manifest,
            payload,
            payload_sha256,
            disk_bytes: directory_size(&directory)?,
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
    scientific_inputs: Vec<String>,
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
            scientific_inputs: descriptor
                .scientific_inputs
                .iter()
                .map(ToString::to_string)
                .collect(),
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
        let inputs = self
            .scientific_inputs
            .iter()
            .map(|value| {
                decode_digest(value)
                    .map(ArtifactIdentity::from_sha256)
                    .ok_or(PreparedArtifactError::InvalidManifest)
            })
            .collect::<Result<Vec<_>, _>>()?;
        PreparedArtifactDescriptor::new(owner, self.kind, inputs, self.segments)
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
    fn into_handle(self, descriptor: &PreparedArtifactDescriptor) -> PreparedArtifact {
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
) -> Result<CacheIdentity, PreparedArtifactError> {
    let mut hasher = Sha256::new();
    hasher.update(CACHE_IDENTITY_DOMAIN);
    hasher.update(IDENTITY_VERSION.to_le_bytes());
    hasher.update(CACHE_SCHEMA_VERSION.to_le_bytes());
    hasher.update(owner.implementation_registry.as_bytes());
    hash_bytes(&mut hasher, owner.provider.as_bytes())?;
    hash_bytes(&mut hasher, owner.provider_version.as_bytes())?;
    hash_bytes(&mut hasher, owner.implementation.as_str().as_bytes())?;
    Ok(CacheIdentity::from_owner_digest(hasher.finalize().into()))
}

fn derive_artifact_identity(
    descriptor: &PreparedArtifactDescriptor,
) -> Result<ArtifactIdentity, PreparedArtifactError> {
    let mut hasher = Sha256::new();
    hasher.update(ARTIFACT_IDENTITY_DOMAIN);
    hasher.update(IDENTITY_VERSION.to_le_bytes());
    hasher.update(descriptor.owner.cache_identity.as_bytes());
    hasher.update([kind_tag(descriptor.kind)]);
    hash_len(&mut hasher, descriptor.scientific_inputs.len())?;
    for identity in &descriptor.scientific_inputs {
        hasher.update(identity.as_bytes());
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
    if planned
        .iter()
        .any(|artifact| artifact.role() == ArtifactRole::Output)
    {
        return Err(PreparedArtifactError::ProductAuthorityViolation);
    }
    if planned.len() != 1 {
        return Err(PreparedArtifactError::UnplannedOperation);
    }
    let expected_role = match operation {
        PreparedArtifactOperation::Generate | PreparedArtifactOperation::Load => {
            ArtifactRole::Prepared
        }
        PreparedArtifactOperation::Reuse => ArtifactRole::Cache,
    };
    let artifact = &planned[0];
    if artifact.identity() != descriptor.identity
        || artifact.cache_identity() != Some(descriptor.cache_identity())
        || artifact.role() != expected_role
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
) -> WorkMeasurements {
    let resources = context
        .resources()
        .iter()
        .map(|capability| {
            let peak = match capability.resource() {
                LeaseResource::Workers | LeaseResource::Locks => 1,
                LeaseResource::FileDescriptors => 2,
                LeaseResource::ResidentCache
                | LeaseResource::IoBuffer(IoBufferKind::MappedPageCache) => {
                    reservation.resident_buffer_bytes
                }
                LeaseResource::Storage {
                    use_kind: StorageUseKind::PersistentCache,
                    ..
                } => validated.disk_bytes,
                LeaseResource::Storage {
                    use_kind: StorageUseKind::Temporary,
                    ..
                } if operation != PreparedArtifactOperation::Reuse => validated.disk_bytes,
                _ => 0,
            }
            .min(capability.amount());
            ResourceMeasurement::new(
                capability.resource().clone(),
                capability.lifetime().clone(),
                peak,
            )
        })
        .collect();
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
            payload
                .read_exact(&mut buffer[..limit])
                .map_err(map_incomplete)?;
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
    if payload.read(&mut extra)? != 0 {
        return Err(PreparedArtifactError::OversizedArtifact);
    }
    Ok((payload_hasher.finalize().into(), total))
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
    for entry in fs::read_dir(directory).map_err(map_incomplete)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            return Err(PreparedArtifactError::UnknownCacheEntry(entry.path()));
        }
        names.insert(entry.file_name());
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
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        if casa_visible_name(&name.to_string_lossy()) {
            return Err(PreparedArtifactError::CasaVisiblePath(entry.path()));
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
    fs::read_dir(path)?.try_fold(0_u64, |total, entry| {
        let entry = entry?;
        total
            .checked_add(entry.metadata()?.len())
            .ok_or(PreparedArtifactError::ArtifactTooLarge)
    })
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
    use std::{collections::BTreeSet, io::Cursor};

    use super::*;
    use crate::{
        ClaimLifetime, IoPrediction, ResourceClaim, StagePrediction, WorkDomain, WorkNode,
    };

    fn owner(version: &str) -> PreparedArtifactOwner {
        PreparedArtifactOwner::new(
            ImplementationRegistryId::from_sha256([3; 32]),
            "native-awproject",
            version,
            WorkImplementationId::new("native-awproject-cpu"),
        )
        .expect("valid prepared-artifact owner")
    }

    fn uv(shape: u64) -> PreparedArtifactUvAffine {
        PreparedArtifactUvAffine::new(
            [0.0, 0.0],
            [shape as f64 / 2.0, shape as f64 / 2.0],
            [8.0 / shape as f64, 8.0 / shape as f64],
            [[1.0, 0.0], [0.0, 1.0]],
        )
        .expect("valid affine")
    }

    fn cf_descriptor(input: u8) -> PreparedArtifactDescriptor {
        PreparedArtifactDescriptor::convolution_function(
            owner("3.1.0"),
            [ArtifactIdentity::from_sha256([input; 32])],
            PreparedArtifactPlaneDescriptor::new(
                [8, 8],
                [1, 1],
                2,
                uv(8),
                PreparedArtifactPrecision::ComplexF32,
                PreparedArtifactOrder::Axis0ContiguousLittleEndian,
            )
            .expect("imaging plane"),
            PreparedArtifactPlaneDescriptor::new(
                [16, 16],
                [3, 3],
                2,
                uv(16),
                PreparedArtifactPrecision::ComplexF32,
                PreparedArtifactOrder::LastAxisContiguousLittleEndian,
            )
            .expect("weight plane"),
        )
        .expect("valid asymmetric CF descriptor")
    }

    fn complex_f32_bytes(shape: [u64; 2], value: f32) -> Vec<u8> {
        let elements = usize::try_from(shape[0] * shape[1]).expect("small test shape");
        (0..elements)
            .flat_map(|_| {
                [value.to_le_bytes(), (-value).to_le_bytes()]
                    .into_iter()
                    .flatten()
            })
            .collect()
    }

    fn payloads() -> (Vec<u8>, Vec<u8>) {
        (
            complex_f32_bytes([8, 8], 1.25),
            complex_f32_bytes([16, 16], -2.5),
        )
    }

    fn artifact_store(root: &Path, entries: usize) -> PreparedArtifactStore {
        PreparedArtifactStore::open(
            root,
            PreparedArtifactBudget::new(1 << 20, entries, 37).expect("bounded budget"),
        )
        .expect("private prepared-artifact store")
    }

    fn publish(
        store: &PreparedArtifactStore,
        descriptor: &PreparedArtifactDescriptor,
    ) -> Result<PreparedArtifact, PreparedArtifactError> {
        let (mut imaging, mut weight) = payloads();
        let mut imaging = Cursor::new(&mut imaging);
        let mut weight = Cursor::new(&mut weight);
        let mut inputs = [
            PreparedArtifactSegmentInput::new("imaging", &mut imaging),
            PreparedArtifactSegmentInput::new("weight", &mut weight),
        ];
        let reservation = store.reservation(descriptor, PreparedArtifactOperation::Generate)?;
        let (validated, _) = store.publish_bytes(
            descriptor,
            ArtifactDisposition::Built,
            &mut inputs,
            reservation,
        )?;
        Ok(validated.into_handle(descriptor))
    }

    fn cache_node(
        descriptor: &PreparedArtifactDescriptor,
        operation: PreparedArtifactOperation,
        reservation: PreparedArtifactReservation,
    ) -> (WorkNode, PlannedArtifact, StagePrediction) {
        let id = WorkNodeId::new(match operation {
            PreparedArtifactOperation::Generate => "prepared-artifact-cold-generation",
            PreparedArtifactOperation::Load => "prepared-artifact-cold-load",
            PreparedArtifactOperation::Reuse => "prepared-artifact-warm-reuse",
        });
        let mut claims = vec![
            ResourceClaim {
                resource: LeaseResource::Workers,
                amount: 1,
                lifetime: ClaimLifetime::Work,
            },
            ResourceClaim {
                resource: LeaseResource::ResidentCache,
                amount: reservation.resident_buffer_bytes(),
                lifetime: ClaimLifetime::Work,
            },
            ResourceClaim {
                resource: LeaseResource::IoBuffer(IoBufferKind::MappedPageCache),
                amount: reservation.resident_buffer_bytes(),
                lifetime: ClaimLifetime::Work,
            },
            ResourceClaim {
                resource: LeaseResource::Locks,
                amount: 1,
                lifetime: ClaimLifetime::Work,
            },
            ResourceClaim {
                resource: LeaseResource::FileDescriptors,
                amount: 2,
                lifetime: ClaimLifetime::Work,
            },
            ResourceClaim {
                resource: LeaseResource::Storage {
                    demand_id: "private-prepared-cache".to_string(),
                    use_kind: StorageUseKind::PersistentCache,
                },
                amount: reservation.persistent_cache_bytes(),
                lifetime: ClaimLifetime::Work,
            },
        ];
        if reservation.temporary_staging_bytes() > 0 {
            claims.push(ResourceClaim {
                resource: LeaseResource::Storage {
                    demand_id: "private-prepared-cache".to_string(),
                    use_kind: StorageUseKind::Temporary,
                },
                amount: reservation.temporary_staging_bytes(),
                lifetime: ClaimLifetime::Work,
            });
        }
        let node = WorkNode {
            id: id.clone(),
            kind: WorkKind::Cache,
            domain: WorkDomain::Cpu,
            implementation: WorkImplementationId::new("native-awproject-cpu"),
            dependencies: BTreeSet::new(),
            claims,
            allocations: Vec::new(),
            fences: BTreeSet::new(),
            quiescence_after: BTreeSet::new(),
        };
        let artifact = descriptor.planned_artifact(id.clone(), operation);
        let stage = StagePrediction::new(id, 1_000).with_io(vec![IoPrediction::new(
            IoBufferKind::MappedPageCache,
            descriptor.payload_bytes().expect("payload bytes"),
            3,
        )]);
        (node, artifact, stage)
    }

    #[test]
    fn owner_derived_identity_commits_inputs_versions_and_asymmetric_named_planes() {
        let baseline = cf_descriptor(7);
        assert_eq!(baseline.segments()[0].name(), "imaging");
        assert_eq!(baseline.segments()[0].shape(), [8, 8]);
        assert_eq!(baseline.segments()[1].name(), "weight");
        assert_eq!(baseline.segments()[1].shape(), [16, 16]);
        assert_ne!(
            baseline.identity().as_bytes(),
            baseline.cache_identity().as_bytes()
        );

        let input_variant = cf_descriptor(8);
        assert_ne!(baseline.identity(), input_variant.identity());
        assert_eq!(baseline.cache_identity(), input_variant.cache_identity());

        let version_variant = PreparedArtifactDescriptor::convolution_function(
            owner("3.2.0"),
            [ArtifactIdentity::from_sha256([7; 32])],
            PreparedArtifactPlaneDescriptor::new(
                [8, 8],
                [1, 1],
                2,
                uv(8),
                PreparedArtifactPrecision::ComplexF32,
                PreparedArtifactOrder::Axis0ContiguousLittleEndian,
            )
            .unwrap(),
            PreparedArtifactPlaneDescriptor::new(
                [16, 16],
                [3, 3],
                2,
                uv(16),
                PreparedArtifactPrecision::ComplexF32,
                PreparedArtifactOrder::LastAxisContiguousLittleEndian,
            )
            .unwrap(),
        )
        .unwrap();
        assert_ne!(baseline.cache_identity(), version_variant.cache_identity());
        assert_ne!(baseline.identity(), version_variant.identity());
    }

    #[test]
    fn canonical_cache_nodes_bind_operation_resources_storage_and_artifact_measurement_inputs() {
        let directory = tempfile::tempdir().unwrap();
        let store = artifact_store(directory.path(), 4);
        let descriptor = cf_descriptor(7);
        for operation in [
            PreparedArtifactOperation::Generate,
            PreparedArtifactOperation::Load,
            PreparedArtifactOperation::Reuse,
        ] {
            let reservation = store.reservation(&descriptor, operation).unwrap();
            let (node, artifact, stage) = cache_node(&descriptor, operation, reservation);
            validate_plan_declaration(
                &node,
                &[artifact],
                &stage,
                &descriptor,
                operation,
                reservation,
            )
            .expect("canonical prepared-artifact node binding");
        }

        let reservation = store
            .reservation(&descriptor, PreparedArtifactOperation::Generate)
            .unwrap();
        let (mut node, artifact, stage) = cache_node(
            &descriptor,
            PreparedArtifactOperation::Generate,
            reservation,
        );
        node.claims.retain(|claim| {
            !matches!(
                claim.resource,
                LeaseResource::Storage {
                    use_kind: StorageUseKind::Temporary,
                    ..
                }
            )
        });
        assert!(matches!(
            validate_plan_declaration(
                &node,
                &[artifact],
                &stage,
                &descriptor,
                PreparedArtifactOperation::Generate,
                reservation,
            ),
            Err(PreparedArtifactError::MissingReservation(
                "temporary staging storage"
            ))
        ));

        let (node, artifact, stage) = cache_node(
            &descriptor,
            PreparedArtifactOperation::Generate,
            reservation,
        );
        let output = PlannedArtifact::new(
            ArtifactIdentity::from_sha256([99; 32]),
            node.id.clone(),
            ArtifactRole::Output,
            None,
        );
        assert!(matches!(
            validate_plan_declaration(
                &node,
                &[artifact, output],
                &stage,
                &descriptor,
                PreparedArtifactOperation::Generate,
                reservation,
            ),
            Err(PreparedArtifactError::ProductAuthorityViolation)
        ));
    }

    #[test]
    fn private_publication_is_atomic_named_and_reusable_without_casa_provenance() {
        let directory = tempfile::tempdir().unwrap();
        let store = artifact_store(directory.path(), 4);
        let descriptor = cf_descriptor(7);
        assert_eq!(
            store.availability(&descriptor).unwrap(),
            PreparedArtifactAvailability::Cold
        );
        let artifact = publish(&store, &descriptor).unwrap();
        assert_eq!(artifact.identity(), descriptor.identity());
        assert_ne!(artifact.integrity_identity(), descriptor.identity());
        assert_eq!(
            artifact.segment_names().collect::<Vec<_>>(),
            ["imaging", "weight"]
        );
        let mut copied = Vec::new();
        artifact.copy_segment_to("weight", &mut copied).unwrap();
        assert_eq!(copied, payloads().1);
        assert_eq!(
            store.availability(&descriptor).unwrap(),
            PreparedArtifactAvailability::Warm
        );

        let entry = directory
            .path()
            .join(CACHE_DIRECTORY)
            .join(descriptor.identity().to_string());
        assert!(entry.join(MANIFEST_FILE).is_file());
        assert!(entry.join(PAYLOAD_FILE).is_file());
        let names = fs::read_dir(&entry)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            names,
            BTreeSet::from([MANIFEST_FILE.into(), PAYLOAD_FILE.into()])
        );
        assert!(
            names
                .iter()
                .all(|name| !name.to_string_lossy().contains("CFS_")
                    && !name.to_string_lossy().contains("WTCFS_"))
        );
    }

    #[test]
    fn corrupt_incomplete_unknown_nonfinite_and_stale_entries_fail_closed() {
        let directory = tempfile::tempdir().unwrap();
        let store = artifact_store(directory.path(), 4);
        let descriptor = cf_descriptor(7);
        publish(&store, &descriptor).unwrap();
        let payload = directory
            .path()
            .join(CACHE_DIRECTORY)
            .join(descriptor.identity().to_string())
            .join(PAYLOAD_FILE);
        let mut bytes = fs::read(&payload).unwrap();
        bytes[0] ^= 1;
        fs::write(&payload, bytes).unwrap();
        assert!(matches!(
            store.availability(&descriptor),
            Err(PreparedArtifactError::CorruptArtifact)
        ));

        let directory = tempfile::tempdir().unwrap();
        let store = artifact_store(directory.path(), 4);
        let descriptor = cf_descriptor(7);
        publish(&store, &descriptor).unwrap();
        let manifest = directory
            .path()
            .join(CACHE_DIRECTORY)
            .join(descriptor.identity().to_string())
            .join(MANIFEST_FILE);
        let mut value: serde_json::Value =
            serde_json::from_slice(&fs::read(&manifest).unwrap()).unwrap();
        value["schema_version"] = 999.into();
        fs::write(&manifest, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
        assert!(matches!(
            store.availability(&descriptor),
            Err(PreparedArtifactError::UnknownSchema { version: 999, .. })
        ));

        let directory = tempfile::tempdir().unwrap();
        let store = artifact_store(directory.path(), 4);
        let descriptor = cf_descriptor(7);
        publish(&store, &descriptor).unwrap();
        let payload = directory
            .path()
            .join(CACHE_DIRECTORY)
            .join(descriptor.identity().to_string())
            .join(PAYLOAD_FILE);
        fs::remove_file(payload).unwrap();
        assert!(matches!(
            store.availability(&descriptor),
            Err(PreparedArtifactError::IncompleteArtifact)
        ));

        let directory = tempfile::tempdir().unwrap();
        let store = artifact_store(directory.path(), 4);
        let descriptor = cf_descriptor(7);
        publish(&store, &descriptor).unwrap();
        let mut stale_descriptor = descriptor.clone();
        stale_descriptor.scientific_inputs[0] = ArtifactIdentity::from_sha256([11; 32]);
        assert!(matches!(
            store.availability(&stale_descriptor),
            Err(PreparedArtifactError::StaleArtifact)
        ));

        let directory = tempfile::tempdir().unwrap();
        let store = artifact_store(directory.path(), 4);
        let descriptor = cf_descriptor(7);
        let (mut imaging, mut weight) = payloads();
        imaging[..4].copy_from_slice(&f32::NAN.to_le_bytes());
        let mut imaging = Cursor::new(&mut imaging);
        let mut weight = Cursor::new(&mut weight);
        let mut inputs = [
            PreparedArtifactSegmentInput::new("imaging", &mut imaging),
            PreparedArtifactSegmentInput::new("weight", &mut weight),
        ];
        let reservation = store
            .reservation(&descriptor, PreparedArtifactOperation::Generate)
            .unwrap();
        assert!(matches!(
            store.publish_bytes(
                &descriptor,
                ArtifactDisposition::Built,
                &mut inputs,
                reservation,
            ),
            Err(PreparedArtifactError::NonFiniteValue { .. })
        ));
        assert_eq!(
            fs::read_dir(directory.path().join(CACHE_DIRECTORY))
                .unwrap()
                .count(),
            0
        );
    }

    #[test]
    fn cache_budget_drives_locked_deterministic_eviction_and_casa_roots_are_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let store = artifact_store(directory.path(), 2);
        let first = cf_descriptor(7);
        let second = cf_descriptor(8);
        let third = cf_descriptor(9);
        publish(&store, &first).unwrap();
        publish(&store, &second).unwrap();
        publish(&store, &third).unwrap();
        let evicted = first.identity().min(second.identity());
        let retained = first.identity().max(second.identity());
        let evicted_descriptor = if evicted == first.identity() {
            &first
        } else {
            &second
        };
        let retained_descriptor = if retained == first.identity() {
            &first
        } else {
            &second
        };
        assert_eq!(
            store.availability(evicted_descriptor).unwrap(),
            PreparedArtifactAvailability::Cold
        );
        assert_eq!(
            store.availability(retained_descriptor).unwrap(),
            PreparedArtifactAvailability::Warm
        );
        assert_eq!(
            store.availability(&third).unwrap(),
            PreparedArtifactAvailability::Warm
        );

        let narrowed = artifact_store(directory.path(), 1);
        let (trimmed_descriptor, survivor_descriptor) = if retained < third.identity() {
            (retained_descriptor, &third)
        } else {
            (&third, retained_descriptor)
        };
        assert_eq!(
            narrowed.availability(trimmed_descriptor).unwrap(),
            PreparedArtifactAvailability::Cold
        );
        assert_eq!(
            narrowed.availability(survivor_descriptor).unwrap(),
            PreparedArtifactAvailability::Warm
        );

        let casa_root = tempfile::tempdir().unwrap();
        fs::create_dir(casa_root.path().join("CFS_0.im")).unwrap();
        assert!(matches!(
            PreparedArtifactStore::open(
                casa_root.path(),
                PreparedArtifactBudget::new(1 << 20, 2, 64).unwrap(),
            ),
            Err(PreparedArtifactError::CasaVisiblePath(_))
        ));
        let private_parent = tempfile::tempdir().unwrap();
        assert!(matches!(
            PreparedArtifactStore::open(
                private_parent.path().join("WTCFS_0.im"),
                PreparedArtifactBudget::new(1 << 20, 2, 64).unwrap(),
            ),
            Err(PreparedArtifactError::CasaVisiblePath(_))
        ));
    }
}
