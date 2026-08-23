// SPDX-License-Identifier: LGPL-3.0-or-later

//! Plan-bound private persistence for immutable implementation preparation.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, BufReader, Read, Write},
    mem::size_of,
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
const WORK_IMPLEMENTATION_ID_DOMAIN: &[u8] =
    b"casa-rs/private-prepared-artifact/work-implementation\0";
const REJECTION_EVIDENCE_DOMAIN: &[u8] = b"casa-rs/private-prepared-artifact/rejection\0";
const CELL_IDENTITY_DOMAIN: &[u8] = b"casa-rs/private-prepared-artifact/cell\0";
const SCIENTIFIC_KEY_DOMAIN: &[u8] = b"casa-rs/private-prepared-artifact/scientific-key\0";
const EVICTION_LEDGER_DOMAIN: &[u8] = b"casa-rs/private-prepared-artifact/eviction-ledger\0";
const EVICTION_OBSERVED_DOMAIN: &[u8] = b"casa-rs/private-prepared-artifact/eviction-observed\0";
const IDENTITY_VERSION: u32 = 4;
const CACHE_SCHEMA: &str = "casa-rs-private-prepared-artifact";
const CACHE_SCHEMA_VERSION: u32 = 4;
const EVICTION_POLICY: &str = "lexicographic-existing-artifact-identity-v1";
const CACHE_DIRECTORY: &str = "objects-v1";
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

/// Interpretation of the exact W coordinate in an AW cell key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PreparedArtifactAwInterpretation {
    /// The coordinate is a Wavelength value in lambda.
    Wavelength,
    /// The coordinate is a baseline W value in metres.
    BaselineMeters,
}

/// Exact scientific key for one AW convolution-function cell.
///
/// This key is intentionally independent of the pixel layout. It retains the
/// exact normal and conjugate frequencies, W coordinate and interpretation,
/// Mueller element, normal and conjugate polarization codes, telescope/band
/// metadata, dish diameter, W increment, rotational interpretation,
/// parallactic angle, and normalization. Two cells with identical shapes but
/// different scientific coordinates therefore cannot alias one owner-derived
/// artifact identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedArtifactCellKey {
    frequency_hz_bits: u64,
    w_coordinate_bits: u64,
    mueller_element: i32,
    polarization: i32,
    parallactic_angle_deg_bits: u64,
    conjugate_frequency_hz_bits: u64,
    conjugate_polarization: i32,
    telescope: String,
    band: String,
    antenna_diameter_m_bits: u64,
    w_increment_bits: u64,
    interpretation: PreparedArtifactAwInterpretation,
    rotationally_symmetric: bool,
    normalization: String,
}

impl PreparedArtifactCellKey {
    /// Validate and retain one exact AW cell's scientific coordinates.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        frequency_hz: f64,
        w_coordinate: f64,
        mueller_element: i32,
        polarization: i32,
        parallactic_angle_deg: f64,
        conjugate_frequency_hz: f64,
        conjugate_polarization: i32,
        telescope: impl Into<String>,
        band: impl Into<String>,
        antenna_diameter_m: f64,
        w_increment: f64,
        interpretation: PreparedArtifactAwInterpretation,
        rotationally_symmetric: bool,
        normalization: impl Into<String>,
    ) -> Result<Self, PreparedArtifactError> {
        let telescope = telescope.into();
        let band = band.into();
        let normalization = normalization.into();
        if !frequency_hz.is_finite()
            || frequency_hz <= 0.0
            || !w_coordinate.is_finite()
            || !parallactic_angle_deg.is_finite()
            || !conjugate_frequency_hz.is_finite()
            || conjugate_frequency_hz <= 0.0
            || !antenna_diameter_m.is_finite()
            || antenna_diameter_m <= 0.0
            || !w_increment.is_finite()
            || w_increment < 0.0
            || !valid_identifier(&telescope)
            || !valid_identifier(&band)
            || !valid_identifier(&normalization)
        {
            return Err(PreparedArtifactError::InvalidCellKey);
        }
        Ok(Self {
            frequency_hz_bits: frequency_hz.to_bits(),
            w_coordinate_bits: w_coordinate.to_bits(),
            mueller_element,
            polarization,
            parallactic_angle_deg_bits: parallactic_angle_deg.to_bits(),
            conjugate_frequency_hz_bits: conjugate_frequency_hz.to_bits(),
            conjugate_polarization,
            telescope,
            band,
            antenna_diameter_m_bits: antenna_diameter_m.to_bits(),
            w_increment_bits: w_increment.to_bits(),
            interpretation,
            rotationally_symmetric,
            normalization,
        })
    }

    /// Return the exact frequency coordinate in Hz.
    #[must_use]
    pub fn frequency_hz(&self) -> f64 {
        f64::from_bits(self.frequency_hz_bits)
    }

    /// Return the exact W coordinate in its declared interpretation.
    #[must_use]
    pub fn w_coordinate(&self) -> f64 {
        f64::from_bits(self.w_coordinate_bits)
    }

    /// Return the exact Mueller element number.
    #[must_use]
    pub const fn mueller_element(&self) -> i32 {
        self.mueller_element
    }

    /// Return the exact correlation-plane code.
    #[must_use]
    pub const fn polarization(&self) -> i32 {
        self.polarization
    }

    /// Return the exact parallactic-angle bin in degrees.
    #[must_use]
    pub fn parallactic_angle_deg(&self) -> f64 {
        f64::from_bits(self.parallactic_angle_deg_bits)
    }

    /// Return the exact paired conjugate-beam frequency in Hz.
    #[must_use]
    pub fn conjugate_frequency_hz(&self) -> f64 {
        f64::from_bits(self.conjugate_frequency_hz_bits)
    }

    /// Return the exact paired conjugate-polarization code.
    #[must_use]
    pub const fn conjugate_polarization(&self) -> i32 {
        self.conjugate_polarization
    }

    /// Return the telescope identity paired with this cell.
    #[must_use]
    pub fn telescope(&self) -> &str {
        &self.telescope
    }

    /// Return the observing band identity paired with this cell.
    #[must_use]
    pub fn band(&self) -> &str {
        &self.band
    }

    /// Return the exact antenna diameter in metres.
    #[must_use]
    pub fn antenna_diameter_m(&self) -> f64 {
        f64::from_bits(self.antenna_diameter_m_bits)
    }

    /// Return the exact W increment in the declared interpretation.
    #[must_use]
    pub fn w_increment(&self) -> f64 {
        f64::from_bits(self.w_increment_bits)
    }

    /// Return the W-coordinate interpretation.
    #[must_use]
    pub const fn interpretation(&self) -> PreparedArtifactAwInterpretation {
        self.interpretation
    }

    /// Return the paired CASA rotational-symmetry interpretation.
    #[must_use]
    pub const fn rotationally_symmetric(&self) -> bool {
        self.rotationally_symmetric
    }

    /// Return the exact normalization identity.
    #[must_use]
    pub fn normalization(&self) -> &str {
        &self.normalization
    }

    fn validate(&self) -> Result<(), PreparedArtifactError> {
        Self::new(
            self.frequency_hz(),
            self.w_coordinate(),
            self.mueller_element,
            self.polarization,
            self.parallactic_angle_deg(),
            self.conjugate_frequency_hz(),
            self.conjugate_polarization,
            self.telescope.clone(),
            self.band.clone(),
            self.antenna_diameter_m(),
            self.w_increment(),
            self.interpretation,
            self.rotationally_symmetric,
            self.normalization.clone(),
        )
        .map(|_| ())
    }

    fn hash(&self, hasher: &mut Sha256) -> Result<(), PreparedArtifactError> {
        hasher.update(CELL_IDENTITY_DOMAIN);
        hasher.update(IDENTITY_VERSION.to_le_bytes());
        hasher.update(self.frequency_hz_bits.to_le_bytes());
        hasher.update(self.w_coordinate_bits.to_le_bytes());
        hasher.update(self.mueller_element.to_le_bytes());
        hasher.update(self.polarization.to_le_bytes());
        hasher.update(self.parallactic_angle_deg_bits.to_le_bytes());
        hasher.update(self.conjugate_frequency_hz_bits.to_le_bytes());
        hasher.update(self.conjugate_polarization.to_le_bytes());
        hash_bytes(hasher, self.telescope.as_bytes())?;
        hash_bytes(hasher, self.band.as_bytes())?;
        hasher.update(self.antenna_diameter_m_bits.to_le_bytes());
        hasher.update(self.w_increment_bits.to_le_bytes());
        hasher.update([aw_interpretation_tag(self.interpretation)]);
        hasher.update([u8::from(self.rotationally_symmetric)]);
        hash_bytes(hasher, self.normalization.as_bytes())
    }
}

/// Exact scientific key for one spectral map artifact.
///
/// `owner_artifact_key` is required because the runtime cannot enumerate every
/// implementation-specific channel-routing semantic. The selected owner must
/// derive a distinct key for artifacts that differ scientifically.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedArtifactSpectralMapKey {
    owner_artifact_key: String,
    reference_frequency_hz_bits: u64,
    channel_index: u64,
    channel_width_hz_bits: u64,
    frame: String,
}

impl PreparedArtifactSpectralMapKey {
    /// Validate and retain one exact spectral-coordinate map cell.
    pub fn new(
        owner_artifact_key: impl Into<String>,
        reference_frequency_hz: f64,
        channel_index: u64,
        channel_width_hz: f64,
        frame: impl Into<String>,
    ) -> Result<Self, PreparedArtifactError> {
        let owner_artifact_key = owner_artifact_key.into();
        let frame = frame.into();
        if !valid_identifier(&owner_artifact_key)
            || !reference_frequency_hz.is_finite()
            || reference_frequency_hz <= 0.0
            || !channel_width_hz.is_finite()
            || channel_width_hz == 0.0
            || !valid_identifier(&frame)
        {
            return Err(PreparedArtifactError::InvalidScientificKey);
        }
        Ok(Self {
            owner_artifact_key,
            reference_frequency_hz_bits: reference_frequency_hz.to_bits(),
            channel_index,
            channel_width_hz_bits: channel_width_hz.to_bits(),
            frame,
        })
    }

    fn validate(&self) -> Result<(), PreparedArtifactError> {
        Self::new(
            self.owner_artifact_key.clone(),
            f64::from_bits(self.reference_frequency_hz_bits),
            self.channel_index,
            f64::from_bits(self.channel_width_hz_bits),
            self.frame.clone(),
        )
        .map(|_| ())
    }

    fn hash(&self, hasher: &mut Sha256) -> Result<(), PreparedArtifactError> {
        hasher.update([2]);
        hash_bytes(hasher, self.owner_artifact_key.as_bytes())?;
        hasher.update(self.reference_frequency_hz_bits.to_le_bytes());
        hasher.update(self.channel_index.to_le_bytes());
        hasher.update(self.channel_width_hz_bits.to_le_bytes());
        hash_bytes(hasher, self.frame.as_bytes())
    }
}

/// Numerical algorithm family used by a generic prepared kernel key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PreparedArtifactKernelAlgorithm {
    /// Visibility-to-grid accumulation.
    Gridding,
    /// Grid-to-visibility interpolation.
    Degridding,
    /// Imaging-weight accumulation.
    Weighting,
}

/// Exact scientific key for one generic numerical kernel artifact.
///
/// `owner_artifact_key` closes the identity over implementation-specific
/// numerical semantics that are not represented by the generic layout fields.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedArtifactKernelKey {
    owner_artifact_key: String,
    algorithm: PreparedArtifactKernelAlgorithm,
    input_shape: Vec<u64>,
    output_shape: Vec<u64>,
    support: Vec<u64>,
    sampling: Vec<u64>,
    precision: PreparedArtifactPrecision,
}

impl PreparedArtifactKernelKey {
    /// Validate and retain one exact kernel geometry and numerical contract.
    pub fn new(
        owner_artifact_key: impl Into<String>,
        algorithm: PreparedArtifactKernelAlgorithm,
        input_shape: Vec<u64>,
        output_shape: Vec<u64>,
        support: Vec<u64>,
        sampling: Vec<u64>,
        precision: PreparedArtifactPrecision,
    ) -> Result<Self, PreparedArtifactError> {
        let owner_artifact_key = owner_artifact_key.into();
        if !valid_identifier(&owner_artifact_key)
            || input_shape.is_empty()
            || input_shape.len() > 16
            || output_shape.len() != input_shape.len()
            || support.len() != input_shape.len()
            || sampling.len() != input_shape.len()
            || input_shape.contains(&0)
            || output_shape.contains(&0)
            || sampling.contains(&0)
        {
            return Err(PreparedArtifactError::InvalidScientificKey);
        }
        Ok(Self {
            owner_artifact_key,
            algorithm,
            input_shape,
            output_shape,
            support,
            sampling,
            precision,
        })
    }

    fn validate(&self) -> Result<(), PreparedArtifactError> {
        Self::new(
            self.owner_artifact_key.clone(),
            self.algorithm,
            self.input_shape.clone(),
            self.output_shape.clone(),
            self.support.clone(),
            self.sampling.clone(),
            self.precision,
        )
        .map(|_| ())
    }

    fn hash(&self, hasher: &mut Sha256) -> Result<(), PreparedArtifactError> {
        hasher.update([3, kernel_algorithm_tag(self.algorithm)]);
        hash_bytes(hasher, self.owner_artifact_key.as_bytes())?;
        hash_u64s(hasher, &self.input_shape)?;
        hash_u64s(hasher, &self.output_shape)?;
        hash_u64s(hasher, &self.support)?;
        hash_u64s(hasher, &self.sampling)?;
        hasher.update([precision_tag(self.precision)]);
        Ok(())
    }
}

/// Scientific identity input required by every prepared-artifact kind.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum PreparedArtifactScientificKey {
    /// Exact AW convolution-function cell metadata.
    ConvolutionFunction(PreparedArtifactCellKey),
    /// Exact spectral-map coordinate metadata.
    SpectralMap(PreparedArtifactSpectralMapKey),
    /// Exact numerical-kernel geometry and precision metadata.
    Kernel(PreparedArtifactKernelKey),
}

impl PreparedArtifactScientificKey {
    fn validate_any(&self) -> Result<(), PreparedArtifactError> {
        match self {
            Self::ConvolutionFunction(key) => key.validate(),
            Self::SpectralMap(key) => key.validate(),
            Self::Kernel(key) => key.validate(),
        }
    }

    fn validate_for_kind(&self, kind: PreparedArtifactKind) -> Result<(), PreparedArtifactError> {
        match (kind, self) {
            (PreparedArtifactKind::ConvolutionFunction, Self::ConvolutionFunction(key)) => {
                key.validate()
            }
            (PreparedArtifactKind::SpectralMap, Self::SpectralMap(key)) => key.validate(),
            (PreparedArtifactKind::Kernel, Self::Kernel(key)) => key.validate(),
            _ => Err(PreparedArtifactError::InvalidScientificKey),
        }
    }

    fn hash(&self, hasher: &mut Sha256) -> Result<(), PreparedArtifactError> {
        hasher.update(SCIENTIFIC_KEY_DOMAIN);
        hasher.update(IDENTITY_VERSION.to_le_bytes());
        match self {
            Self::ConvolutionFunction(key) => {
                hasher.update([1]);
                key.hash(hasher)?;
            }
            Self::SpectralMap(key) => key.hash(hasher)?,
            Self::Kernel(key) => key.hash(hasher)?,
        }
        Ok(())
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
///
/// The registry identity is checked against the plan's running registry before
/// any private-cache operation. Provider and implementation fields therefore
/// remain descriptive inputs to the owner-derived identity, never an authority
/// to replace the selected registry entry.
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
    scientific_key: PreparedArtifactScientificKey,
}

impl ScientificCommitments {
    fn from_problem(
        problem: &CompiledProblem,
        scientific_key: PreparedArtifactScientificKey,
    ) -> Self {
        Self {
            compiled_problem: encode_hex(&problem.problem_id().as_bytes()),
            observation_snapshot: encode_hex(&problem.inputs().observation().as_bytes()),
            compiled_geometry: encode_hex(&problem.geometry().geometry_id().as_bytes()),
            numerics_contract: encode_hex(&problem.numerics_id().as_bytes()),
            scientific_key,
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
        self.scientific_key.validate_any()
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
    /// Describe an asymmetric named imaging/weight CF pair.
    pub fn convolution_function(
        store: &PreparedArtifactStore,
        owner: PreparedArtifactOwner,
        problem: &CompiledProblem,
        cell: PreparedArtifactCellKey,
        imaging: PreparedArtifactPlaneDescriptor,
        weight: PreparedArtifactPlaneDescriptor,
    ) -> Result<Self, PreparedArtifactError> {
        let scientific = ScientificCommitments::from_problem(
            problem,
            PreparedArtifactScientificKey::ConvolutionFunction(cell),
        );
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
    pub fn new(
        store: &PreparedArtifactStore,
        owner: PreparedArtifactOwner,
        kind: PreparedArtifactKind,
        problem: &CompiledProblem,
        scientific_key: PreparedArtifactScientificKey,
        segments: Vec<PreparedArtifactSegmentDescriptor>,
    ) -> Result<Self, PreparedArtifactError> {
        let scientific = ScientificCommitments::from_problem(problem, scientific_key);
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
        scientific.scientific_key.validate_for_kind(kind)?;
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
        hash_bytes(&mut hasher, self.owner.provider.as_bytes())
            .expect("validated provider identity length");
        hash_bytes(&mut hasher, self.owner.provider_version.as_bytes())
            .expect("validated provider version identity length");
        hash_bytes(&mut hasher, self.owner.implementation.as_str().as_bytes())
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

    /// Return the exact payload/source streaming-buffer claim.
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
    state: Arc<RootState>,
    #[cfg(test)]
    fail_after_evictions: Option<usize>,
    #[cfg(test)]
    fail_after_publication_rename: bool,
}

#[derive(Debug)]
struct RootState {
    mutation: Mutex<()>,
}

static ROOT_STATES: OnceLock<Mutex<BTreeMap<PathBuf, Weak<RootState>>>> = OnceLock::new();

impl PreparedArtifactStore {
    /// Open an explicitly configured private casa-rs cache root.
    ///
    /// Canonicalization and CASA-boundary checks happen before any missing
    /// directory is created. Opening the store does not scan, validate, evict,
    /// or otherwise mutate cache entries.
    pub fn open(
        root: impl AsRef<Path>,
        budget: PreparedArtifactBudget,
    ) -> Result<Self, PreparedArtifactError> {
        let root = prepare_private_root(root.as_ref())?;
        reject_casa_cache_contents(&root)?;
        let cache = root.join(CACHE_DIRECTORY);
        ensure_private_child_directory(&root, &cache)?;
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
            #[cfg(test)]
            fail_after_evictions: None,
            #[cfg(test)]
            fail_after_publication_rename: false,
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
        let streaming_buffer_bytes = u64::try_from(streaming_buffer_len(self.budget, descriptor)?)
            .map_err(|_| PreparedArtifactError::ArtifactTooLarge)?;
        let inventory_resident_bytes = inventory_resident_reservation(&self.cache, self.budget)?;
        let resident_buffer_bytes = streaming_buffer_bytes
            .checked_add(MANIFEST_RESIDENT_BYTES)
            .and_then(|bytes| bytes.checked_add(inventory_resident_bytes))
            .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
        Ok(PreparedArtifactReservation {
            persistent_cache_bytes: self.budget.cache_bytes,
            entry_bytes,
            temporary_staging_bytes: if operation == PreparedArtifactOperation::Reuse {
                0
            } else {
                entry_bytes
            },
            streaming_buffer_bytes,
            resident_buffer_bytes,
        })
    }

    /// Generate, validate, and atomically publish exact cold bytes.
    ///
    /// The returned identity exposes no payload access. The measurements cover
    /// the complete private-store operation through final validation.
    pub fn generate(
        &self,
        context: &WorkExecutionContext<'_>,
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
    /// The returned identity and measurements have the same operation boundary
    /// as [`Self::generate`].
    pub fn load(
        &self,
        context: &WorkExecutionContext<'_>,
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
    ///
    /// A successful result exposes identity only; a rejection returns durable
    /// evidence and no payload access. The
    /// measurements include the lock, cache scan, metadata, manifest, payload,
    /// and integrity operations performed before the disposition is known.
    pub fn reuse(
        &self,
        context: &WorkExecutionContext<'_>,
        descriptor: &PreparedArtifactDescriptor,
    ) -> Result<(PreparedArtifactReuseOutcome, WorkMeasurements), PreparedArtifactError> {
        let reservation = self.reservation(descriptor, PreparedArtifactOperation::Reuse)?;
        validate_plan_binding(
            *context,
            descriptor,
            PreparedArtifactOperation::Reuse,
            reservation,
        )?;
        let mut evidence = ValidationEvidence::new(self.budget);
        let mut lock = match self.lock(&mut evidence) {
            Ok(lock) => lock,
            Err(error) => {
                let measurements = failed_measurements(
                    *context,
                    descriptor,
                    reservation,
                    PreparedArtifactOperation::Reuse,
                    &evidence,
                );
                return Err(error.with_measurements(measurements));
            }
        };
        let evaluation = self.reuse_locked(descriptor, &mut evidence);
        let unlock = lock.release(&mut evidence);
        let evaluation = match (evaluation, unlock) {
            (Ok(evaluation), Ok(())) => evaluation,
            (Err(error), _) | (Ok(_), Err(error)) => {
                let measurements = failed_measurements(
                    *context,
                    descriptor,
                    reservation,
                    PreparedArtifactOperation::Reuse,
                    &evidence,
                );
                return Err(error.with_measurements(measurements));
            }
        };
        match evaluation {
            ReuseEvaluation::Rejected {
                rejection,
                path,
                cache_bytes,
            } => {
                let measurements = rejected_measurements(
                    *context,
                    descriptor,
                    reservation,
                    rejection,
                    &path,
                    cache_bytes,
                    evidence,
                );
                Ok((
                    PreparedArtifactReuseOutcome::Rejected(rejection),
                    measurements,
                ))
            }
            ReuseEvaluation::Reused {
                validated,
                cache_bytes,
            } => {
                let measurements = measurements(
                    *context,
                    descriptor,
                    ArtifactDisposition::Reused,
                    &validated,
                    MeasurementInput {
                        reservation,
                        operation: PreparedArtifactOperation::Reuse,
                        cache_bytes,
                        evidence,
                    },
                );
                Ok((
                    PreparedArtifactReuseOutcome::Reused(validated.into_handle(descriptor)),
                    measurements,
                ))
            }
        }
    }

    fn reuse_locked(
        &self,
        descriptor: &PreparedArtifactDescriptor,
        evidence: &mut ValidationEvidence,
    ) -> Result<ReuseEvaluation, PreparedArtifactError> {
        let cache_bytes = self.validate_raw_budget(descriptor.identity, evidence)?;
        let path = self.entry_path(descriptor.identity);
        evidence.store_read_operation();
        match path.symlink_metadata() {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                Ok(ReuseEvaluation::Rejected {
                    rejection: PreparedArtifactRejection::Missing,
                    path,
                    cache_bytes,
                })
            }
            Err(error) => Err(error.into()),
            Ok(_) => match self.validate_entry_with_evidence(
                descriptor.identity,
                Some(descriptor),
                evidence,
            ) {
                Ok(validated) => Ok(ReuseEvaluation::Reused {
                    validated,
                    cache_bytes,
                }),
                Err(error) => {
                    let Some(rejection) = rejection_for(&error) else {
                        return Err(error);
                    };
                    Ok(ReuseEvaluation::Rejected {
                        rejection,
                        path,
                        cache_bytes,
                    })
                }
            },
        }
    }

    fn publish(
        &self,
        context: &WorkExecutionContext<'_>,
        descriptor: &PreparedArtifactDescriptor,
        operation: PreparedArtifactOperation,
        disposition: ArtifactDisposition,
        segments: &mut [PreparedArtifactSegmentInput<'_>],
    ) -> Result<(PreparedArtifact, WorkMeasurements), PreparedArtifactError> {
        let reservation = self.reservation(descriptor, operation)?;
        validate_plan_binding(*context, descriptor, operation, reservation)?;
        let mut evidence = ValidationEvidence::new(self.budget);
        let mut lock = match self.lock(&mut evidence) {
            Ok(lock) => lock,
            Err(error) => {
                let measurements =
                    failed_measurements(*context, descriptor, reservation, operation, &evidence);
                return Err(error.with_measurements(measurements));
            }
        };
        let mut published = self.publish_bytes_locked(
            descriptor,
            disposition,
            segments,
            reservation,
            &mut evidence,
        );
        if published.is_err()
            && let Err(rollback) = self.rollback_materialized(&mut evidence)
        {
            published = Err(rollback);
        }
        let unlock = lock.release(&mut evidence);
        let (validated, final_disposition, cache_bytes) = match (published, unlock) {
            (Ok(published), Ok(())) => published,
            (Err(error), _) => {
                let measurements =
                    failed_measurements(*context, descriptor, reservation, operation, &evidence);
                return Err(error.with_measurements(measurements));
            }
            (Ok(_), Err(error)) => {
                let error = match self.rollback_materialized(&mut evidence) {
                    Ok(()) => error,
                    Err(rollback) => rollback,
                };
                let measurements =
                    failed_measurements(*context, descriptor, reservation, operation, &evidence);
                return Err(error.with_measurements(measurements));
            }
        };
        let measurements = measurements(
            *context,
            descriptor,
            final_disposition,
            &validated,
            MeasurementInput {
                reservation,
                operation,
                cache_bytes,
                evidence,
            },
        );
        Ok((validated.into_handle(descriptor), measurements))
    }

    fn publish_bytes_locked(
        &self,
        descriptor: &PreparedArtifactDescriptor,
        disposition: ArtifactDisposition,
        segments: &mut [PreparedArtifactSegmentInput<'_>],
        reservation: PreparedArtifactReservation,
        evidence: &mut ValidationEvidence,
    ) -> Result<(ValidatedArtifact, ArtifactDisposition, u64), PreparedArtifactError> {
        validate_segment_inputs(descriptor, segments)?;
        self.remove_orphan_staging(evidence)?;
        evidence.store_write_operation();
        let staging = Builder::new()
            .prefix(STAGING_PREFIX)
            .tempdir_in(&self.cache)?;
        let staging_path = staging.keep();
        let result = (|| -> Result<_, PreparedArtifactError> {
            let payload_path = staging_path.join(PAYLOAD_FILE);
            evidence.store_write_operation();
            let payload_file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&payload_path)?;
            evidence.observe_file_descriptors(2);
            let mut payload = payload_file;
            let buffer_len = usize::try_from(reservation.streaming_buffer_bytes)
                .map_err(|_| PreparedArtifactError::ArtifactTooLarge)?;
            let mut buffer = vec![0_u8; buffer_len];
            evidence.observe_source_read_buffer(buffer_len as u64);
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
                    evidence,
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
            drop(buffer);
            evidence.store_write_operation();
            payload.sync_all()?;
            drop(payload);
            let payload_sha256: [u8; 32] = payload_hasher.finalize().into();
            {
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
                evidence.observe_resident(MANIFEST_RESIDENT_BYTES);
                let manifest_path = staging_path.join(MANIFEST_FILE);
                evidence.store_write_operation();
                let mut manifest_output = OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&manifest_path)?;
                evidence.observe_file_descriptors(2);
                {
                    let mut bounded_manifest = BoundedFileWriter::new(
                        &mut manifest_output,
                        MANIFEST_RESERVATION_BYTES,
                        evidence,
                    );
                    let serialization = serde_json::to_writer(&mut bounded_manifest, &manifest);
                    if bounded_manifest.exceeded() {
                        return Err(PreparedArtifactError::ManifestReservationExceeded {
                            actual: MANIFEST_RESERVATION_BYTES.saturating_add(1),
                            reserved: MANIFEST_RESERVATION_BYTES,
                        });
                    }
                    serialization?;
                    if let Err(error) = bounded_manifest.write_all(b"\n") {
                        if bounded_manifest.exceeded() {
                            return Err(PreparedArtifactError::ManifestReservationExceeded {
                                actual: MANIFEST_RESERVATION_BYTES.saturating_add(1),
                                reserved: MANIFEST_RESERVATION_BYTES,
                            });
                        }
                        return Err(error.into());
                    }
                }
                evidence.store_write_operation();
                manifest_output.sync_all()?;
            }
            sync_directory_counted(&staging_path, evidence)?;

            let incoming_bytes = directory_size_counted(&staging_path, evidence)?;
            if incoming_bytes > reservation.entry_bytes {
                return Err(PreparedArtifactError::ManifestReservationExceeded {
                    actual: incoming_bytes,
                    reserved: reservation.entry_bytes,
                });
            }
            let mut staged = self.validate_entry_at_path(
                staging_path.clone(),
                descriptor.identity,
                Some(descriptor),
                evidence,
            )?;
            if staged.payload_sha256 != payload_sha256 {
                return Err(PreparedArtifactError::CorruptArtifact);
            }
            let target = self.entry_path(descriptor.identity);
            evidence.store_read_operation();
            match target.symlink_metadata() {
                Ok(_) => {
                    let existing = self.validate_entry_with_evidence(
                        descriptor.identity,
                        Some(descriptor),
                        evidence,
                    )?;
                    if existing.payload_sha256 != payload_sha256 {
                        return Err(PreparedArtifactError::PublicationConflict);
                    }
                    let cache_bytes = self.validate_budget_without_eviction(evidence)?;
                    Ok((existing, disposition, cache_bytes))
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    self.evict_for(descriptor.identity, incoming_bytes, evidence)?;
                    let cache_bytes = self.validate_budget_with_incoming(
                        descriptor.identity,
                        incoming_bytes,
                        evidence,
                    )?;
                    sync_directory_counted(&self.cache, evidence)?;
                    self.rename_staging_for_publication(
                        &staging_path,
                        &target,
                        MaterializedArtifactEvidence {
                            payload_sha256: staged.payload_sha256,
                            payload_bytes: staged.payload_bytes,
                            path: target.clone(),
                            disposition,
                        },
                        evidence,
                    )?;
                    staged.path = target;
                    Ok((staged, disposition, cache_bytes))
                }
                Err(error) => Err(error.into()),
            }
        })();
        let cleanup = remove_staging_counted(&staging_path, evidence);
        match (result, cleanup) {
            (Ok(result), Ok(())) => Ok(result),
            (Err(error), _) | (Ok(_), Err(error)) => Err(error),
        }
    }

    fn rename_staging_for_publication(
        &self,
        staging_path: &Path,
        target: &Path,
        materialized: MaterializedArtifactEvidence,
        evidence: &mut ValidationEvidence,
    ) -> Result<(), PreparedArtifactError> {
        evidence.store_write_operation();
        fs::rename(staging_path, target)?;
        evidence.materialized = Some(materialized);
        let completed = self.complete_publication_rename(evidence);
        if let Err(error) = completed {
            remove_staging_counted(target, evidence)?;
            return Err(error);
        }
        Ok(())
    }

    fn complete_publication_rename(
        &self,
        evidence: &mut ValidationEvidence,
    ) -> Result<(), PreparedArtifactError> {
        #[cfg(test)]
        if self.fail_after_publication_rename {
            return Err(io::Error::other("injected post-publication-rename failure").into());
        }
        sync_directory_counted(&self.cache, evidence)
    }

    fn rollback_materialized(
        &self,
        evidence: &mut ValidationEvidence,
    ) -> Result<(), PreparedArtifactError> {
        let Some(path) = evidence
            .materialized
            .as_ref()
            .map(|materialized| materialized.path.clone())
        else {
            return Ok(());
        };
        remove_staging_counted(&path, evidence)
    }

    fn evict_for(
        &self,
        incoming: ArtifactIdentity,
        incoming_bytes: u64,
        evidence: &mut ValidationEvidence,
    ) -> Result<(), PreparedArtifactError> {
        let mut entries = self.entries(evidence)?;
        entries.retain(|entry| entry.identity != incoming);
        let mut total = incoming_bytes;
        let mut existing_bytes = 0_u64;
        for entry in &entries {
            existing_bytes = existing_bytes
                .checked_add(entry.bytes)
                .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
            total = total
                .checked_add(entry.bytes)
                .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
        }
        evidence.observe_cache_bytes(existing_bytes);
        let mut evicted = 0_usize;
        while total > self.budget.cache_bytes
            || entries.len().saturating_sub(evicted).saturating_add(1) > self.budget.entries
        {
            let Some(entry) = entries.get(evicted).copied() else {
                return Err(PreparedArtifactError::CacheBudgetExceeded {
                    required: total,
                    budget: self.budget.cache_bytes,
                });
            };
            let entry_path = self.entry_path(entry.identity);
            let eviction_path = self
                .cache
                .join(format!("{STAGING_PREFIX}evicted-{}", entry.identity));
            evidence.store_write_operation();
            fs::rename(entry_path, &eviction_path)?;
            evidence.record_eviction(entry);
            total = total.saturating_sub(entry.bytes);
            evicted = evicted.saturating_add(1);
            #[cfg(test)]
            if self.fail_after_evictions == Some(evicted) {
                return Err(io::Error::other("injected post-eviction failure").into());
            }
            evidence.store_write_operation();
            fs::remove_dir_all(eviction_path)?;
        }
        Ok(())
    }

    fn validate_budget_without_eviction(
        &self,
        evidence: &mut ValidationEvidence,
    ) -> Result<u64, PreparedArtifactError> {
        let entries = self.entries(evidence)?;
        let total = entries.iter().try_fold(0_u64, |total, entry| {
            total
                .checked_add(entry.bytes)
                .ok_or(PreparedArtifactError::ArtifactTooLarge)
        })?;
        evidence.observe_cache_bytes(total);
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

    fn validate_budget_with_incoming(
        &self,
        incoming: ArtifactIdentity,
        incoming_bytes: u64,
        evidence: &mut ValidationEvidence,
    ) -> Result<u64, PreparedArtifactError> {
        let entries = self.entries(evidence)?;
        if entries.iter().any(|entry| entry.identity == incoming) {
            return Err(PreparedArtifactError::PublicationConflict);
        }
        let total = entries.iter().try_fold(incoming_bytes, |total, entry| {
            total
                .checked_add(entry.bytes)
                .ok_or(PreparedArtifactError::ArtifactTooLarge)
        })?;
        let count = entries
            .len()
            .checked_add(1)
            .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
        evidence.observe_cache_bytes(total);
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

    fn validate_raw_budget(
        &self,
        planned: ArtifactIdentity,
        evidence: &mut ValidationEvidence,
    ) -> Result<u64, PreparedArtifactError> {
        let mut total = 0_u64;
        let mut count = 0_usize;
        for path in
            directory_paths_counted(&self.cache, evidence, root_inventory_limit(self.budget)?)?
        {
            let path = path.into_path_buf();
            let name = path
                .file_name()
                .ok_or_else(|| PreparedArtifactError::UnknownCacheEntry(path.clone()))?
                .to_string_lossy();
            evidence.store_read_operation();
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
                if identity != planned {
                    validate_entry_inventory(&path, evidence)?;
                }
                directory_size_counted(&path, evidence)?
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
        evidence.observe_cache_bytes(total);
        Ok(total)
    }

    fn entries(
        &self,
        evidence: &mut ValidationEvidence,
    ) -> Result<Vec<CacheInventoryEntry>, PreparedArtifactError> {
        let mut entries = Vec::with_capacity(self.budget.entries);
        for path in
            directory_paths_counted(&self.cache, evidence, root_inventory_limit(self.budget)?)?
        {
            let path = path.into_path_buf();
            let name = path
                .file_name()
                .ok_or_else(|| PreparedArtifactError::UnknownCacheEntry(path.clone()))?;
            let name = name.to_string_lossy();
            evidence.store_read_operation();
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
            if entries.len() == entries.capacity() {
                return Err(PreparedArtifactError::CacheEntryBudgetExceeded {
                    required: entries.len().saturating_add(1),
                    budget: self.budget.entries,
                });
            }
            validate_entry_inventory(&path, evidence)?;
            entries.push(CacheInventoryEntry {
                identity,
                bytes: directory_size_counted(&path, evidence)?,
            });
        }
        entries.sort_unstable_by_key(|entry| entry.identity);
        Ok(entries)
    }

    fn remove_orphan_staging(
        &self,
        evidence: &mut ValidationEvidence,
    ) -> Result<(), PreparedArtifactError> {
        let mut removed = false;
        for path in
            directory_paths_counted(&self.cache, evidence, root_inventory_limit(self.budget)?)?
        {
            let path = path.into_path_buf();
            if path
                .file_name()
                .ok_or_else(|| PreparedArtifactError::UnknownCacheEntry(path.clone()))?
                .to_string_lossy()
                .starts_with(STAGING_PREFIX)
            {
                evidence.store_read_operation();
                if !path.symlink_metadata()?.file_type().is_dir() {
                    return Err(PreparedArtifactError::UnknownCacheEntry(path));
                }
                directory_size_counted(&path, evidence)?;
                evidence.store_write_operation();
                fs::remove_dir_all(path)?;
                removed = true;
            }
        }
        if removed {
            sync_directory_counted(&self.cache, evidence)?;
        }
        Ok(())
    }

    fn validate_entry_with_evidence(
        &self,
        identity: ArtifactIdentity,
        expected: Option<&PreparedArtifactDescriptor>,
        evidence: &mut ValidationEvidence,
    ) -> Result<ValidatedArtifact, PreparedArtifactError> {
        let directory = self.entry_path(identity);
        self.validate_entry_at_path(directory, identity, expected, evidence)
    }

    fn validate_entry_at_path(
        &self,
        directory: PathBuf,
        identity: ArtifactIdentity,
        expected: Option<&PreparedArtifactDescriptor>,
        evidence: &mut ValidationEvidence,
    ) -> Result<ValidatedArtifact, PreparedArtifactError> {
        evidence.store_read_operation();
        let directory_type = directory
            .symlink_metadata()
            .map_err(map_incomplete)?
            .file_type();
        if !directory_type.is_dir() {
            return Err(PreparedArtifactError::UnknownCacheEntry(directory));
        }
        validate_entry_inventory(&directory, evidence)?;
        let manifest_path = directory.join(MANIFEST_FILE);
        evidence.store_read_operation();
        if manifest_path.symlink_metadata()?.len() > MANIFEST_RESERVATION_BYTES {
            return Err(PreparedArtifactError::InvalidManifest);
        }
        let manifest = read_manifest_counted(&manifest_path, evidence)?;
        if manifest.schema != CACHE_SCHEMA || manifest.schema_version != CACHE_SCHEMA_VERSION {
            return Err(PreparedArtifactError::UnknownSchema {
                schema: manifest.schema,
                version: manifest.schema_version,
            });
        }
        evidence.store_validation();
        if manifest.segments.is_empty() || manifest.segments.len() > MAX_MANIFEST_SEGMENTS {
            return Err(PreparedArtifactError::InvalidManifest);
        }
        if manifest
            .segments
            .windows(2)
            .any(|pair| pair[0].descriptor.name.as_str() >= pair[1].descriptor.name.as_str())
        {
            return Err(PreparedArtifactError::SegmentLayoutMismatch);
        }
        let ArtifactManifest {
            identity: manifest_identity,
            cache_identity: manifest_cache_identity,
            descriptor: manifest_descriptor,
            payload_sha256: manifest_payload_sha256,
            payload_bytes: manifest_payload_bytes,
            segments,
            ..
        } = manifest;
        let mut segment_descriptors = Vec::with_capacity(segments.len());
        let mut segment_integrity = Vec::with_capacity(segments.len());
        for segment in segments {
            segment_descriptors.push(segment.descriptor);
            segment_integrity.push(ManifestSegmentIntegrity {
                offset: segment.offset,
                bytes: segment.bytes,
                sha256: segment.sha256,
            });
        }
        let descriptor = manifest_descriptor.into_descriptor(segment_descriptors)?;
        if descriptor.identity != identity
            || manifest_identity != identity.to_string()
            || manifest_cache_identity != descriptor.cache_identity().to_string()
            || descriptor.cache_scope.root_identity != self.scope.root_identity
        {
            return Err(PreparedArtifactError::IdentityMismatch);
        }
        if expected.is_some_and(|expected| expected != &descriptor) {
            return Err(PreparedArtifactError::StaleArtifact);
        }
        validate_manifest_segments(&descriptor, &segment_integrity, manifest_payload_bytes)?;
        evidence.store_validation();
        let expected_payload_digest = decode_digest(&manifest_payload_sha256)
            .ok_or(PreparedArtifactError::InvalidManifest)?;
        let payload_path = directory.join(PAYLOAD_FILE);
        let disk_bytes = directory_size_counted(&directory, evidence)?;
        evidence.store_read_operation();
        let payload = File::open(&payload_path).map_err(map_incomplete)?;
        evidence.observe_file_descriptors(2);
        let buffer_len = streaming_buffer_len(self.budget, &descriptor)?;
        evidence.observe_resident(
            MANIFEST_RESIDENT_BYTES
                .checked_add(buffer_len as u64)
                .ok_or(PreparedArtifactError::ArtifactTooLarge)?,
        );
        let (payload_sha256, payload_bytes) = validate_payload(
            &payload,
            &descriptor.segments,
            &segment_integrity,
            buffer_len,
            evidence,
        )?;
        if payload_bytes != manifest_payload_bytes || payload_sha256 != expected_payload_digest {
            return Err(PreparedArtifactError::CorruptArtifact);
        }
        evidence.store_validation();
        Ok(ValidatedArtifact {
            payload_sha256,
            payload_bytes,
            disk_bytes,
            path: directory,
        })
    }

    fn entry_path(&self, identity: ArtifactIdentity) -> PathBuf {
        self.cache.join(identity.to_string())
    }

    fn lock(
        &self,
        evidence: &mut ValidationEvidence,
    ) -> Result<StoreLock<'_>, PreparedArtifactError> {
        let in_process = self
            .state
            .mutation
            .lock()
            .map_err(|_| PreparedArtifactError::PoisonedStore)?;
        evidence.observe_locks(1);
        evidence.store_control_operation();
        let file = match OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&self.lock_path)
        {
            Ok(file) => {
                evidence.store_write_operation();
                file
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                evidence.store_control_operation();
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&self.lock_path)?
            }
            Err(error) => return Err(error.into()),
        };
        evidence.observe_file_descriptors(1);
        evidence.store_control_operation();
        FileExt::lock_exclusive(&file)?;
        Ok(StoreLock {
            _in_process: in_process,
            file,
            locked: true,
        })
    }
}

struct StoreLock<'a> {
    _in_process: MutexGuard<'a, ()>,
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
            provider: descriptor.owner.provider.clone(),
            provider_version: descriptor.owner.provider_version.clone(),
            implementation: descriptor.owner.implementation.as_str().to_string(),
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
    /// Named byte streams did not exactly match the canonical descriptor order.
    SegmentMismatch,
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
                .write_str("prepared-artifact streams do not match the canonical named segments"),
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
    descriptor.scientific.scientific_key.hash(&mut hasher)?;
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
    if descriptor.owner.implementation_registry != context.implementation_registry_id() {
        return Err(PreparedArtifactError::ImplementationRegistryMismatch);
    }
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
    if planned.len() != 2 {
        return Err(PreparedArtifactError::UnplannedOperation);
    }
    let expected = descriptor.planned_artifact(operation);
    let expected_ledger = descriptor.eviction_artifact(operation);
    let selected = planned
        .iter()
        .find(|artifact| artifact.identity() == expected.identity());
    let ledger = planned
        .iter()
        .find(|artifact| artifact.identity() == expected_ledger.identity());
    if selected.is_none()
        || ledger.is_none()
        || selected.is_some_and(|artifact| {
            artifact.cache_identity() != Some(descriptor.cache_identity())
                || artifact.role() != expected.role()
                || artifact.node() != expected.node()
        })
        || ledger.is_some_and(|artifact| {
            artifact.cache_identity().is_some()
                || artifact.role() != ArtifactRole::Input
                || artifact.node() != expected_ledger.node()
        })
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
        |resource| {
            matches!(
                resource,
                LeaseResource::IoBuffer(IoBufferKind::StorageManager)
            )
        },
        reservation.resident_buffer_bytes,
        "private-store buffer",
    )?;
    require_claim(
        node,
        |resource| matches!(resource, LeaseResource::IoBuffer(IoBufferKind::Writeback)),
        writeback_buffer_bytes(reservation, operation),
        "private-store writeback buffer",
    )?;
    if operation != PreparedArtifactOperation::Reuse {
        require_claim(
            node,
            |resource| {
                matches!(
                    resource,
                    LeaseResource::IoBuffer(IoBufferKind::SourceReadAhead)
                )
            },
            reservation.streaming_buffer_bytes,
            "source-read-ahead buffer",
        )?;
    }
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
    let mut required_io = vec![IoBufferKind::StorageManager, IoBufferKind::Writeback];
    if operation != PreparedArtifactOperation::Reuse {
        required_io.push(IoBufferKind::SourceReadAhead);
    }
    let predicted = stage
        .io()
        .iter()
        .map(|prediction| prediction.kind())
        .collect::<BTreeSet<_>>();
    let payload_bytes = descriptor.payload_bytes()?;
    if predicted.len() != required_io.len()
        || required_io.iter().any(|kind| !predicted.contains(kind))
        || stage.io().iter().any(|prediction| {
            let minimum_bytes = match prediction.kind() {
                IoBufferKind::SourceReadAhead => payload_bytes,
                IoBufferKind::StorageManager => reservation.entry_bytes,
                IoBufferKind::Writeback if operation != PreparedArtifactOperation::Reuse => {
                    reservation.entry_bytes
                }
                IoBufferKind::Writeback => 0,
                _ => u64::MAX,
            };
            prediction.bytes() < minimum_bytes || prediction.operations() == 0
        })
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

const fn writeback_buffer_bytes(
    reservation: PreparedArtifactReservation,
    operation: PreparedArtifactOperation,
) -> u64 {
    if matches!(operation, PreparedArtifactOperation::Reuse) {
        1
    } else {
        reservation.streaming_buffer_bytes
    }
}

struct MeasurementInput {
    reservation: PreparedArtifactReservation,
    operation: PreparedArtifactOperation,
    cache_bytes: u64,
    evidence: ValidationEvidence,
}

fn measurements(
    context: WorkExecutionContext<'_>,
    descriptor: &PreparedArtifactDescriptor,
    disposition: ArtifactDisposition,
    validated: &ValidatedArtifact,
    input: MeasurementInput,
) -> WorkMeasurements {
    let resources = resource_measurements(
        context,
        input.operation,
        input.cache_bytes,
        validated.disk_bytes,
        input.reservation,
        &input.evidence,
    );
    let io = context
        .stage_prediction()
        .io()
        .iter()
        .map(|prediction| input.evidence.measurement(prediction.kind()))
        .collect();
    let ledger = descriptor.eviction_artifact(input.operation);
    let evicted_bytes = input
        .evidence
        .evictions
        .iter()
        .map(|(_, bytes)| *bytes)
        .sum();
    let artifacts = vec![
        ArtifactMeasurement::new(
            descriptor.identity,
            Some(derive_content_identity(
                descriptor,
                validated.payload_sha256,
            )),
            disposition,
            validated.payload_bytes,
            Some(RedactedPath::from_path(&validated.path)),
        ),
        ArtifactMeasurement::new(
            ledger.identity(),
            Some(derive_eviction_observed_identity(
                ledger.identity(),
                &input.evidence.evictions,
            )),
            ArtifactDisposition::Loaded,
            evicted_bytes,
            None,
        ),
    ];
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
        reservation,
        &evidence,
    );
    let io = context
        .stage_prediction()
        .io()
        .iter()
        .map(|prediction| evidence.measurement(prediction.kind()))
        .collect();
    let ledger = descriptor.eviction_artifact(PreparedArtifactOperation::Reuse);
    let evicted_bytes = evidence.evictions.iter().map(|(_, bytes)| *bytes).sum();
    let artifacts = vec![
        ArtifactMeasurement::new(
            descriptor.identity,
            Some(rejection.evidence_identity(descriptor.identity)),
            ArtifactDisposition::RejectedStale,
            evidence.inspected_bytes(),
            Some(RedactedPath::from_path(path)),
        ),
        ArtifactMeasurement::new(
            ledger.identity(),
            Some(derive_eviction_observed_identity(
                ledger.identity(),
                &evidence.evictions,
            )),
            ArtifactDisposition::Loaded,
            evicted_bytes,
            None,
        ),
    ];
    WorkMeasurements::new(resources, io, artifacts)
}

fn failed_measurements(
    context: WorkExecutionContext<'_>,
    descriptor: &PreparedArtifactDescriptor,
    reservation: PreparedArtifactReservation,
    operation: PreparedArtifactOperation,
    evidence: &ValidationEvidence,
) -> WorkMeasurements {
    let resources = resource_measurements(
        context,
        operation,
        evidence.cache_bytes_peak,
        evidence.cache_write.bytes.min(reservation.entry_bytes),
        reservation,
        evidence,
    );
    let io = context
        .stage_prediction()
        .io()
        .iter()
        .map(|prediction| evidence.measurement(prediction.kind()))
        .collect();
    let ledger = descriptor.eviction_artifact(operation);
    let evicted_bytes = evidence.evictions.iter().map(|(_, bytes)| *bytes).sum();
    let mut artifacts = Vec::with_capacity(2);
    if let Some(materialized) = &evidence.materialized {
        artifacts.push(ArtifactMeasurement::new(
            descriptor.identity,
            Some(derive_content_identity(
                descriptor,
                materialized.payload_sha256,
            )),
            materialized.disposition,
            materialized.payload_bytes,
            Some(RedactedPath::from_path(&materialized.path)),
        ));
    }
    artifacts.push(ArtifactMeasurement::new(
        ledger.identity(),
        Some(derive_eviction_observed_identity(
            ledger.identity(),
            &evidence.evictions,
        )),
        ArtifactDisposition::Loaded,
        evicted_bytes,
        None,
    ));
    WorkMeasurements::new(resources, io, artifacts)
}

fn resource_measurements(
    context: WorkExecutionContext<'_>,
    operation: PreparedArtifactOperation,
    cache_bytes: u64,
    entry_bytes: u64,
    reservation: PreparedArtifactReservation,
    evidence: &ValidationEvidence,
) -> Vec<ResourceMeasurement> {
    context
        .resources()
        .iter()
        .map(|capability| {
            let peak = match capability.resource() {
                LeaseResource::Workers => 1,
                LeaseResource::Locks => evidence.locks_peak,
                LeaseResource::FileDescriptors => evidence.file_descriptors_peak,
                LeaseResource::IoBuffer(IoBufferKind::StorageManager) => evidence
                    .resident_buffer_bytes
                    .min(reservation.resident_buffer_bytes),
                LeaseResource::IoBuffer(IoBufferKind::SourceReadAhead)
                    if operation != PreparedArtifactOperation::Reuse =>
                {
                    evidence
                        .source_read_buffer_bytes
                        .min(reservation.streaming_buffer_bytes)
                }
                LeaseResource::IoBuffer(IoBufferKind::Writeback)
                    if evidence.cache_write.operations > 0 =>
                {
                    writeback_buffer_bytes(reservation, operation)
                }
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
        | PreparedArtifactError::InvalidCellKey
        | PreparedArtifactError::InvalidScientificKey
        | PreparedArtifactError::InvalidDescriptor
        | PreparedArtifactError::InvalidLayout
        | PreparedArtifactError::InvalidUvAffine
        | PreparedArtifactError::ArtifactTooLarge
        | PreparedArtifactError::ImplementationRegistryMismatch
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
    input: &mut dyn Read,
    output: &mut dyn Write,
    payload_hasher: &mut Sha256,
    buffer: &mut [u8],
    segment: &PreparedArtifactSegmentDescriptor,
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
        read_exact_counted(input, &mut buffer[..limit], evidence, IoClass::SourceRead)?;
        validate_finite(&buffer[..limit], segment.precision, &segment.name, scalar)?;
        evidence.source_validation();
        write_all_counted(output, &buffer[..limit], evidence, IoClass::StoreWrite)?;
        payload_hasher.update(&buffer[..limit]);
        segment_hasher.update(&buffer[..limit]);
        remaining -= limit as u64;
        scalar += (limit / scalar_bytes) as u64;
    }
    let mut extra = [0_u8; 1];
    if read_counted(input, &mut extra, evidence, IoClass::SourceRead)? != 0 {
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
                IoClass::StoreRead,
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
    if read_counted(&mut payload, &mut extra, evidence, IoClass::StoreRead)? != 0 {
        return Err(PreparedArtifactError::OversizedArtifact);
    }
    Ok((payload_hasher.finalize().into(), total))
}

#[derive(Clone, Copy, Debug, Default)]
struct IoCounter {
    bytes: u64,
    operations: u64,
}

#[derive(Clone, Copy, Debug)]
enum IoClass {
    SourceRead,
    StoreRead,
    StoreControl,
    StoreWrite,
}

#[derive(Clone, Debug, Default)]
struct ValidationEvidence {
    source_read: IoCounter,
    cache_read: IoCounter,
    cache_control: IoCounter,
    cache_write: IoCounter,
    resident_baseline_bytes: u64,
    resident_buffer_bytes: u64,
    source_read_buffer_bytes: u64,
    cache_bytes_peak: u64,
    locks_peak: u64,
    file_descriptors_peak: u64,
    evictions: Vec<(ArtifactIdentity, u64)>,
    materialized: Option<MaterializedArtifactEvidence>,
}

impl ValidationEvidence {
    fn new(budget: PreparedArtifactBudget) -> Self {
        let evictions = Vec::with_capacity(budget.entries);
        let resident_buffer_bytes = u64::try_from(evictions.capacity().saturating_mul(size_of::<(
            ArtifactIdentity,
            u64,
        )>(
        )))
        .unwrap_or(u64::MAX);
        Self {
            resident_baseline_bytes: resident_buffer_bytes,
            resident_buffer_bytes,
            evictions,
            ..Self::default()
        }
    }

    fn source_validation(&mut self) {
        self.record(IoClass::SourceRead, 0);
    }

    fn store_read_operation(&mut self) {
        self.record(IoClass::StoreRead, 0);
    }

    fn store_control_operation(&mut self) {
        self.record(IoClass::StoreControl, 0);
    }

    fn store_write_operation(&mut self) {
        self.record(IoClass::StoreWrite, 0);
    }

    fn store_validation(&mut self) {
        self.record(IoClass::StoreRead, 0);
    }

    fn observe_resident(&mut self, bytes: u64) {
        self.resident_buffer_bytes = self
            .resident_buffer_bytes
            .max(self.resident_baseline_bytes.saturating_add(bytes));
    }

    fn observe_source_read_buffer(&mut self, bytes: u64) {
        self.source_read_buffer_bytes = self.source_read_buffer_bytes.max(bytes);
        self.observe_resident(bytes);
    }

    fn observe_cache_bytes(&mut self, bytes: u64) {
        self.cache_bytes_peak = self.cache_bytes_peak.max(bytes);
    }

    fn observe_locks(&mut self, locks: u64) {
        self.locks_peak = self.locks_peak.max(locks);
    }

    fn observe_file_descriptors(&mut self, file_descriptors: u64) {
        self.file_descriptors_peak = self.file_descriptors_peak.max(file_descriptors);
    }

    fn record(&mut self, class: IoClass, bytes: u64) {
        let counter = match class {
            IoClass::SourceRead => &mut self.source_read,
            IoClass::StoreRead => &mut self.cache_read,
            IoClass::StoreControl => &mut self.cache_control,
            IoClass::StoreWrite => &mut self.cache_write,
        };
        counter.bytes = counter.bytes.saturating_add(bytes);
        counter.operations = counter.operations.saturating_add(1);
    }

    fn counter(&self, kind: IoBufferKind) -> IoCounter {
        match kind {
            IoBufferKind::SourceReadAhead => self.source_read,
            IoBufferKind::Writeback => self.cache_write,
            IoBufferKind::StorageManager => IoCounter {
                bytes: self.cache_read.bytes,
                operations: self
                    .cache_read
                    .operations
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
        self.evictions.push((entry.identity, entry.bytes));
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
        self.evidence.record(IoClass::StoreWrite, written as u64);
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
            self.evidence.record(IoClass::StoreRead, bytes as u64);
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
        self.evidence.record(IoClass::StoreRead, bytes as u64);
        Ok(bytes)
    }
}

fn read_manifest_counted(
    path: &Path,
    evidence: &mut ValidationEvidence,
) -> Result<ArtifactManifest, PreparedArtifactError> {
    evidence.store_read_operation();
    let file = File::open(path).map_err(map_incomplete)?;
    evidence.observe_file_descriptors(2);
    evidence.observe_resident(MANIFEST_RESIDENT_BYTES);
    let bounded = BoundedFileReader {
        file,
        evidence,
        remaining: MANIFEST_RESERVATION_BYTES,
        exceeded: false,
    };
    let mut reader = BufReader::with_capacity(4096, bounded);
    let parsed = serde_json::from_reader(&mut reader);
    if reader.get_ref().exceeded() {
        return Err(PreparedArtifactError::InvalidManifest);
    }
    parsed.map_err(PreparedArtifactError::Json)
}

fn read_exact_counted<R: Read + ?Sized>(
    input: &mut R,
    output: &mut [u8],
    evidence: &mut ValidationEvidence,
    class: IoClass,
) -> Result<(), PreparedArtifactError> {
    let mut offset = 0;
    while offset < output.len() {
        let bytes = read_counted(input, &mut output[offset..], evidence, class)?;
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
    class: IoClass,
) -> Result<usize, PreparedArtifactError> {
    let bytes = input.read(output).map_err(map_incomplete)?;
    evidence.record(class, bytes as u64);
    Ok(bytes)
}

fn write_all_counted<W: Write + ?Sized>(
    output: &mut W,
    mut input: &[u8],
    evidence: &mut ValidationEvidence,
    class: IoClass,
) -> Result<(), PreparedArtifactError> {
    while !input.is_empty() {
        let written = output.write(input).map_err(PreparedArtifactError::Io)?;
        evidence.record(class, written as u64);
        if written == 0 {
            return Err(PreparedArtifactError::Io(io::Error::new(
                io::ErrorKind::WriteZero,
                "prepared artifact write made no progress",
            )));
        }
        input = &input[written..];
    }
    Ok(())
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
    integrity: &[ManifestSegmentIntegrity],
    payload_bytes: u64,
) -> Result<(), PreparedArtifactError> {
    if integrity.len() != descriptor.segments.len() {
        return Err(PreparedArtifactError::SegmentLayoutMismatch);
    }
    let mut offset = 0_u64;
    for (expected, actual) in descriptor.segments.iter().zip(integrity) {
        if actual.offset != offset
            || actual.bytes != expected.byte_len()?
            || decode_digest(&actual.sha256).is_none()
        {
            return Err(PreparedArtifactError::SegmentLayoutMismatch);
        }
        offset = offset
            .checked_add(actual.bytes)
            .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
    }
    if offset != payload_bytes || offset != descriptor.payload_bytes()? {
        return Err(PreparedArtifactError::SegmentLayoutMismatch);
    }
    Ok(())
}

fn validate_entry_inventory(
    directory: &Path,
    evidence: &mut ValidationEvidence,
) -> Result<(), PreparedArtifactError> {
    let mut names = BTreeSet::new();
    for path in
        directory_paths_counted(directory, evidence, MAX_ENTRY_FILES).map_err(
            |error| match error {
                PreparedArtifactError::Io(error) => map_incomplete(error),
                other => other,
            },
        )?
    {
        let path = path.into_path_buf();
        evidence.store_read_operation();
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
    evidence.store_validation();
    Ok(())
}

fn prepare_private_root(path: &Path) -> Result<PathBuf, PreparedArtifactError> {
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    reject_casa_visible_root(&path)?;
    let mut missing = Vec::new();
    let mut cursor = path.as_path();
    loop {
        match cursor.symlink_metadata() {
            Ok(_) => {
                let existing = validate_existing_private_ancestors(cursor)?;
                let root = create_private_missing(existing, &missing)?;
                let canonical = fs::canonicalize(&root)?;
                reject_casa_visible_root(&canonical)?;
                reject_casa_cache_contents(&canonical)?;
                if !canonical.is_dir() {
                    return Err(PreparedArtifactError::UnknownCacheEntry(canonical));
                }
                return Ok(canonical);
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let component = cursor
                    .file_name()
                    .ok_or_else(|| PreparedArtifactError::UnknownCacheEntry(path.clone()))?;
                missing.push(component.to_owned());
                cursor = cursor
                    .parent()
                    .filter(|parent| !parent.as_os_str().is_empty())
                    .unwrap_or_else(|| Path::new("."));
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn validate_existing_private_ancestors(path: &Path) -> Result<PathBuf, PreparedArtifactError> {
    let mut nearest = None;
    for (index, ancestor) in path.ancestors().enumerate() {
        let metadata = ancestor.symlink_metadata()?;
        let canonical = fs::canonicalize(ancestor)?;
        reject_casa_visible_root(&canonical)?;
        if (index == 0 && metadata.file_type().is_symlink()) || !canonical.is_dir() {
            return Err(PreparedArtifactError::UnknownCacheEntry(
                ancestor.to_path_buf(),
            ));
        }
        reject_casa_cache_contents(&canonical)?;
        nearest.get_or_insert(canonical);
    }
    nearest.ok_or_else(|| PreparedArtifactError::UnknownCacheEntry(path.to_path_buf()))
}

fn create_private_missing(
    mut parent: PathBuf,
    missing: &[std::ffi::OsString],
) -> Result<PathBuf, PreparedArtifactError> {
    for component in missing.iter().rev() {
        let child = parent.join(component);
        match child.symlink_metadata() {
            Ok(metadata) => {
                let canonical = fs::canonicalize(&child)?;
                reject_casa_visible_root(&canonical)?;
                if metadata.file_type().is_symlink() || !metadata.file_type().is_dir() {
                    return Err(PreparedArtifactError::UnknownCacheEntry(child));
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                fs::create_dir(&child)?;
            }
            Err(error) => return Err(error.into()),
        }
        parent = child;
    }
    Ok(parent)
}

fn ensure_private_child_directory(
    parent: &Path,
    child: &Path,
) -> Result<(), PreparedArtifactError> {
    match child.symlink_metadata() {
        Ok(metadata) => {
            let canonical = fs::canonicalize(child)?;
            reject_casa_visible_root(&canonical)?;
            if metadata.file_type().is_symlink() || canonical != child {
                return Err(PreparedArtifactError::UnknownCacheEntry(
                    child.to_path_buf(),
                ));
            }
            if !metadata.file_type().is_dir() {
                return Err(PreparedArtifactError::UnknownCacheEntry(
                    child.to_path_buf(),
                ));
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if fs::canonicalize(parent)? != parent {
                return Err(PreparedArtifactError::UnknownCacheEntry(
                    parent.to_path_buf(),
                ));
            }
            fs::create_dir(child)?;
            let canonical = fs::canonicalize(child)?;
            reject_casa_visible_root(&canonical)?;
            if canonical != child {
                return Err(PreparedArtifactError::UnknownCacheEntry(
                    child.to_path_buf(),
                ));
            }
        }
        Err(error) => return Err(error.into()),
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
        let path = entry?.path();
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
    name.to_ascii_lowercase().ends_with(".im")
}

fn valid_segment_name(name: &str) -> bool {
    valid_identifier(name)
        && name != MANIFEST_FILE
        && name != PAYLOAD_FILE
        && !name.starts_with("CFS_")
        && !name.starts_with("WTCFS_")
        && !casa_visible_name(name)
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

fn directory_size_counted(
    path: &Path,
    evidence: &mut ValidationEvidence,
) -> Result<u64, PreparedArtifactError> {
    directory_paths_counted(path, evidence, MAX_ENTRY_FILES)?
        .into_iter()
        .try_fold(0_u64, |total, entry| {
            let entry = entry.into_path_buf();
            evidence.store_read_operation();
            let metadata = entry.symlink_metadata()?;
            if !metadata.file_type().is_file() {
                return Err(PreparedArtifactError::UnknownCacheEntry(entry));
            }
            total
                .checked_add(metadata.len())
                .ok_or(PreparedArtifactError::ArtifactTooLarge)
        })
}

fn directory_paths_counted(
    path: &Path,
    evidence: &mut ValidationEvidence,
    limit: usize,
) -> Result<Vec<Box<Path>>, PreparedArtifactError> {
    evidence.store_read_operation();
    let entries = fs::read_dir(path)?;
    evidence.observe_file_descriptors(2);
    let mut paths = Vec::with_capacity(limit);
    for entry in entries {
        evidence.store_read_operation();
        if paths.len() == limit {
            return Err(PreparedArtifactError::CacheEntryBudgetExceeded {
                required: limit.saturating_add(1),
                budget: limit,
            });
        }
        let path = entry?.path();
        let component = path
            .file_name()
            .ok_or_else(|| PreparedArtifactError::UnknownCacheEntry(path.clone()))?;
        if component.as_encoded_bytes().len() > MAX_CACHE_COMPONENT_BYTES {
            return Err(PreparedArtifactError::UnknownCacheEntry(path));
        }
        paths.push(path.into_boxed_path());
    }
    Ok(paths)
}

fn root_inventory_limit(budget: PreparedArtifactBudget) -> Result<usize, PreparedArtifactError> {
    budget
        .entries
        .checked_mul(2)
        .and_then(|entries| entries.checked_add(1))
        .ok_or(PreparedArtifactError::ArtifactTooLarge)
}

fn inventory_resident_reservation(
    cache: &Path,
    budget: PreparedArtifactBudget,
) -> Result<u64, PreparedArtifactError> {
    let cache_path_bytes = cache.as_os_str().as_encoded_bytes().len();
    let root_paths = path_inventory_resident_reservation(
        cache_path_bytes,
        root_inventory_limit(budget)?,
        MAX_CACHE_COMPONENT_BYTES,
    )?;
    let entry_directory_bytes = cache_path_bytes
        .checked_add(1 + MAX_CACHE_COMPONENT_BYTES)
        .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
    let entry_paths = path_inventory_resident_reservation(
        entry_directory_bytes,
        MAX_ENTRY_FILES,
        MAX_CACHE_COMPONENT_BYTES,
    )?;
    let cache_entries = fixed_vec_resident_reservation::<CacheInventoryEntry>(budget.entries)?;
    let evictions = fixed_vec_resident_reservation::<(ArtifactIdentity, u64)>(budget.entries)?;
    root_paths
        .checked_add(entry_paths)
        .and_then(|bytes| bytes.checked_add(cache_entries))
        .and_then(|bytes| bytes.checked_add(evictions))
        .ok_or(PreparedArtifactError::ArtifactTooLarge)
}

fn path_inventory_resident_reservation(
    directory_bytes: usize,
    entries: usize,
    component_bytes: usize,
) -> Result<u64, PreparedArtifactError> {
    let path_bytes = directory_bytes
        .checked_add(1)
        .and_then(|bytes| bytes.checked_add(component_bytes))
        .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
    let entry_bytes = size_of::<Box<Path>>()
        .checked_add(path_bytes)
        .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
    let allocation = entries
        .checked_mul(entry_bytes)
        .and_then(|bytes| bytes.checked_add(size_of::<Vec<Box<Path>>>()))
        .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
    u64::try_from(allocation).map_err(|_| PreparedArtifactError::ArtifactTooLarge)
}

fn fixed_vec_resident_reservation<T>(entries: usize) -> Result<u64, PreparedArtifactError> {
    let allocation = entries
        .checked_mul(size_of::<T>())
        .and_then(|bytes| bytes.checked_add(size_of::<Vec<T>>()))
        .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
    u64::try_from(allocation).map_err(|_| PreparedArtifactError::ArtifactTooLarge)
}

fn sync_directory_counted(
    path: &Path,
    evidence: &mut ValidationEvidence,
) -> Result<(), PreparedArtifactError> {
    evidence.store_control_operation();
    let directory = File::open(path)?;
    evidence.observe_file_descriptors(2);
    evidence.store_write_operation();
    directory.sync_all()?;
    Ok(())
}

fn remove_staging_counted(
    path: &Path,
    evidence: &mut ValidationEvidence,
) -> Result<(), PreparedArtifactError> {
    evidence.store_control_operation();
    match path.symlink_metadata() {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
        Ok(metadata) if metadata.file_type().is_dir() => {
            evidence.store_write_operation();
            fs::remove_dir_all(path)?;
            let parent = path
                .parent()
                .ok_or_else(|| PreparedArtifactError::UnknownCacheEntry(path.to_path_buf()))?;
            sync_directory_counted(parent, evidence)
        }
        Ok(_) => Err(PreparedArtifactError::UnknownCacheEntry(path.to_path_buf())),
    }
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

const fn aw_interpretation_tag(value: PreparedArtifactAwInterpretation) -> u8 {
    match value {
        PreparedArtifactAwInterpretation::Wavelength => 1,
        PreparedArtifactAwInterpretation::BaselineMeters => 2,
    }
}

const fn kernel_algorithm_tag(value: PreparedArtifactKernelAlgorithm) -> u8 {
    match value {
        PreparedArtifactKernelAlgorithm::Gridding => 1,
        PreparedArtifactKernelAlgorithm::Degridding => 2,
        PreparedArtifactKernelAlgorithm::Weighting => 3,
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

#[cfg(test)]
mod tests {
    use super::*;

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
    fn private_store_lock_creation_is_counted_once_as_writeback() {
        let directory = tempfile::tempdir().expect("private lock cache");
        let budget = PreparedArtifactBudget::new(200, 3, 1).expect("bounded cache");
        let store = PreparedArtifactStore::open(directory.path(), budget).expect("store");
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
        let mut store = PreparedArtifactStore::open(directory.path(), budget).expect("store");
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
    fn post_rename_failure_removes_visibility_and_retains_mutation_evidence() {
        let directory = tempfile::tempdir().expect("publication failure cache");
        let budget = PreparedArtifactBudget::new(200, 3, 1).expect("bounded cache");
        let mut store = PreparedArtifactStore::open(directory.path(), budget).expect("store");
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
