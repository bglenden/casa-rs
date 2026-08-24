// SPDX-License-Identifier: LGPL-3.0-or-later

//! Owner-minted scientific identities for immutable operator preparation.

use sha2::{Digest, Sha256};
use thiserror::Error;

const IDENTITY_DOMAIN: &[u8] = b"casa-rs/imaging-model/prepared-artifact-science\0";
const IDENTITY_VERSION: u32 = 1;
const MAX_TEXT_BYTES: usize = 256;

/// Scientific family of an owner-minted prepared artifact.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreparedArtifactScientificKind {
    /// Paired imaging and weight convolution-function planes.
    ConvolutionFunction,
    /// Spectral-coordinate or channel-routing map.
    SpectralMap,
    /// Immutable numerical operator kernel.
    Kernel,
}

/// Opaque scientific identity minted by the logical model owner.
///
/// Execution and persistence code can bind and compare this value, but cannot
/// reinterpret W/frequency, polarization, frame, instrument, normalization,
/// or kernel semantics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PreparedArtifactScientificIdentity {
    kind: PreparedArtifactScientificKind,
    digest: [u8; 32],
}

impl PreparedArtifactScientificIdentity {
    /// Mint a convolution-function identity from complete paired-operator semantics.
    pub fn convolution_function(
        semantics: PreparedArtifactCellSemantics,
    ) -> Result<Self, PreparedArtifactScientificIdentityError> {
        semantics.validate()?;
        let mut hasher = start_hash(1);
        semantics.hash(&mut hasher)?;
        Ok(Self::finish(
            PreparedArtifactScientificKind::ConvolutionFunction,
            hasher,
        ))
    }

    /// Mint a spectral-map identity from complete spectral-routing semantics.
    pub fn spectral_map(
        semantics: PreparedArtifactSpectralMapSemantics,
    ) -> Result<Self, PreparedArtifactScientificIdentityError> {
        semantics.validate()?;
        let mut hasher = start_hash(2);
        semantics.hash(&mut hasher)?;
        Ok(Self::finish(
            PreparedArtifactScientificKind::SpectralMap,
            hasher,
        ))
    }

    /// Mint a kernel identity from complete logical operator semantics.
    pub fn kernel(
        semantics: PreparedArtifactKernelSemantics,
    ) -> Result<Self, PreparedArtifactScientificIdentityError> {
        semantics.validate()?;
        let mut hasher = start_hash(3);
        semantics.hash(&mut hasher)?;
        Ok(Self::finish(PreparedArtifactScientificKind::Kernel, hasher))
    }

    /// Return the scientific family committed by the owner.
    #[must_use]
    pub const fn kind(self) -> PreparedArtifactScientificKind {
        self.kind
    }

    /// Return the opaque owner-minted digest.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.digest
    }

    fn finish(kind: PreparedArtifactScientificKind, hasher: Sha256) -> Self {
        Self {
            kind,
            digest: hasher.finalize().into(),
        }
    }
}

/// Exact W-coordinate interpretation of an AW prepared cell.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreparedArtifactAwInterpretation {
    /// W is measured in wavelengths at the cell frequency.
    Wavelength,
    /// W is measured in baseline metres.
    BaselineMeters,
}

/// Complete logical semantics of one AW convolution-function cell.
#[derive(Clone, Debug, PartialEq)]
pub struct PreparedArtifactCellSemantics {
    frequency_hz: f64,
    w_coordinate: f64,
    mueller_element: u32,
    polarization: u32,
    parallactic_angle_deg: f64,
    conjugate_frequency_hz: f64,
    conjugate_polarization: u32,
    telescope: String,
    band: String,
    antenna_diameter_m: f64,
    w_increment: f64,
    interpretation: PreparedArtifactAwInterpretation,
    rotationally_symmetric: bool,
    normalization: String,
}

impl PreparedArtifactCellSemantics {
    /// Describe every logical science commitment of one AW cell.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        frequency_hz: f64,
        w_coordinate: f64,
        mueller_element: u32,
        polarization: u32,
        parallactic_angle_deg: f64,
        conjugate_frequency_hz: f64,
        conjugate_polarization: u32,
        telescope: impl Into<String>,
        band: impl Into<String>,
        antenna_diameter_m: f64,
        w_increment: f64,
        interpretation: PreparedArtifactAwInterpretation,
        rotationally_symmetric: bool,
        normalization: impl Into<String>,
    ) -> Result<Self, PreparedArtifactScientificIdentityError> {
        let result = Self {
            frequency_hz,
            w_coordinate,
            mueller_element,
            polarization,
            parallactic_angle_deg,
            conjugate_frequency_hz,
            conjugate_polarization,
            telescope: telescope.into(),
            band: band.into(),
            antenna_diameter_m,
            w_increment,
            interpretation,
            rotationally_symmetric,
            normalization: normalization.into(),
        };
        result.validate()?;
        Ok(result)
    }

    fn validate(&self) -> Result<(), PreparedArtifactScientificIdentityError> {
        let finite = [
            self.frequency_hz,
            self.w_coordinate,
            self.parallactic_angle_deg,
            self.conjugate_frequency_hz,
            self.antenna_diameter_m,
            self.w_increment,
        ];
        if finite.iter().any(|value| !value.is_finite())
            || self.frequency_hz <= 0.0
            || self.conjugate_frequency_hz <= 0.0
            || self.antenna_diameter_m <= 0.0
            || self.w_increment <= 0.0
            || !valid_text(&self.telescope)
            || !valid_text(&self.band)
            || !valid_text(&self.normalization)
        {
            return Err(PreparedArtifactScientificIdentityError::InvalidSemantics);
        }
        Ok(())
    }

    fn hash(&self, hasher: &mut Sha256) -> Result<(), PreparedArtifactScientificIdentityError> {
        hasher.update(self.frequency_hz.to_bits().to_le_bytes());
        hasher.update(self.w_coordinate.to_bits().to_le_bytes());
        hasher.update(self.mueller_element.to_le_bytes());
        hasher.update(self.polarization.to_le_bytes());
        hasher.update(self.parallactic_angle_deg.to_bits().to_le_bytes());
        hasher.update(self.conjugate_frequency_hz.to_bits().to_le_bytes());
        hasher.update(self.conjugate_polarization.to_le_bytes());
        hash_text(hasher, &self.telescope)?;
        hash_text(hasher, &self.band)?;
        hasher.update(self.antenna_diameter_m.to_bits().to_le_bytes());
        hasher.update(self.w_increment.to_bits().to_le_bytes());
        hasher.update([match self.interpretation {
            PreparedArtifactAwInterpretation::Wavelength => 1,
            PreparedArtifactAwInterpretation::BaselineMeters => 2,
        }]);
        hasher.update([u8::from(self.rotationally_symmetric)]);
        hash_text(hasher, &self.normalization)
    }
}

/// Complete logical semantics of one spectral map.
#[derive(Clone, Debug, PartialEq)]
pub struct PreparedArtifactSpectralMapSemantics {
    reference_frequency_hz: f64,
    channel_index: u64,
    channel_width_hz: f64,
    frame: String,
}

impl PreparedArtifactSpectralMapSemantics {
    /// Describe one exact spectral-coordinate map cell.
    pub fn new(
        reference_frequency_hz: f64,
        channel_index: u64,
        channel_width_hz: f64,
        frame: impl Into<String>,
    ) -> Result<Self, PreparedArtifactScientificIdentityError> {
        let result = Self {
            reference_frequency_hz,
            channel_index,
            channel_width_hz,
            frame: frame.into(),
        };
        result.validate()?;
        Ok(result)
    }

    fn validate(&self) -> Result<(), PreparedArtifactScientificIdentityError> {
        if !self.reference_frequency_hz.is_finite()
            || self.reference_frequency_hz <= 0.0
            || !self.channel_width_hz.is_finite()
            || self.channel_width_hz == 0.0
            || !valid_text(&self.frame)
        {
            return Err(PreparedArtifactScientificIdentityError::InvalidSemantics);
        }
        Ok(())
    }

    fn hash(&self, hasher: &mut Sha256) -> Result<(), PreparedArtifactScientificIdentityError> {
        hasher.update(self.reference_frequency_hz.to_bits().to_le_bytes());
        hasher.update(self.channel_index.to_le_bytes());
        hasher.update(self.channel_width_hz.to_bits().to_le_bytes());
        hash_text(hasher, &self.frame)
    }
}

/// Numerical operator family of a generic prepared kernel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreparedArtifactKernelAlgorithm {
    /// Visibility-to-grid accumulation.
    Gridding,
    /// Grid-to-visibility interpolation.
    Degridding,
    /// Imaging-weight accumulation.
    Weighting,
}

/// Complete logical semantics of one prepared numerical kernel.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedArtifactKernelSemantics {
    algorithm: PreparedArtifactKernelAlgorithm,
    input_shape: Vec<u64>,
    output_shape: Vec<u64>,
}

impl PreparedArtifactKernelSemantics {
    /// Describe one exact logical kernel mapping.
    pub fn new(
        algorithm: PreparedArtifactKernelAlgorithm,
        input_shape: Vec<u64>,
        output_shape: Vec<u64>,
    ) -> Result<Self, PreparedArtifactScientificIdentityError> {
        let result = Self {
            algorithm,
            input_shape,
            output_shape,
        };
        result.validate()?;
        Ok(result)
    }

    fn validate(&self) -> Result<(), PreparedArtifactScientificIdentityError> {
        if self.input_shape.is_empty()
            || self.input_shape.len() > 16
            || self.output_shape.len() != self.input_shape.len()
            || self.input_shape.contains(&0)
            || self.output_shape.contains(&0)
        {
            return Err(PreparedArtifactScientificIdentityError::InvalidSemantics);
        }
        Ok(())
    }

    fn hash(&self, hasher: &mut Sha256) -> Result<(), PreparedArtifactScientificIdentityError> {
        hasher.update([match self.algorithm {
            PreparedArtifactKernelAlgorithm::Gridding => 1,
            PreparedArtifactKernelAlgorithm::Degridding => 2,
            PreparedArtifactKernelAlgorithm::Weighting => 3,
        }]);
        hash_u64s(hasher, &self.input_shape)?;
        hash_u64s(hasher, &self.output_shape)
    }
}

/// Invalid or unbounded owner science cannot mint a prepared identity.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum PreparedArtifactScientificIdentityError {
    /// A semantic field was invalid, nonfinite, empty, or unbounded.
    #[error("invalid prepared-artifact scientific semantics")]
    InvalidSemantics,
}

fn start_hash(tag: u8) -> Sha256 {
    let mut hasher = Sha256::new();
    hasher.update(IDENTITY_DOMAIN);
    hasher.update(IDENTITY_VERSION.to_le_bytes());
    hasher.update([tag]);
    hasher
}

fn valid_text(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_TEXT_BYTES && !value.chars().any(char::is_control)
}

fn hash_text(
    hasher: &mut Sha256,
    value: &str,
) -> Result<(), PreparedArtifactScientificIdentityError> {
    let len = u64::try_from(value.len())
        .map_err(|_| PreparedArtifactScientificIdentityError::InvalidSemantics)?;
    hasher.update(len.to_le_bytes());
    hasher.update(value.as_bytes());
    Ok(())
}

fn hash_u64s(
    hasher: &mut Sha256,
    values: &[u64],
) -> Result<(), PreparedArtifactScientificIdentityError> {
    let len = u64::try_from(values.len())
        .map_err(|_| PreparedArtifactScientificIdentityError::InvalidSemantics)?;
    hasher.update(len.to_le_bytes());
    for value in values {
        hasher.update(value.to_le_bytes());
    }
    Ok(())
}
