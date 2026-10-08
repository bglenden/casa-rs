// SPDX-License-Identifier: LGPL-3.0-or-later

//! Immutable scientific inputs for native EVLA convolution-function generation.

use std::sync::Arc;

use sha2::{Digest, Sha256};
use thiserror::Error;

/// Invalid or unsupported native EVLA generation inputs.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum NativeAwRequestError {
    /// Radial samples are not finite, uniformly spaced, or physically valid.
    #[error("EVLA surface requires finite, uniformly spaced radius/height/slope samples")]
    InvalidSurface,
    /// The requested geometry, sampling, frequency or polarization is unsupported.
    #[error("invalid or unsupported native EVLA A/W sampling")]
    InvalidSampling,
    /// Catalog axes are empty, inconsistent, duplicated, or exceed the declared bound.
    #[error("native A/W catalog axes are invalid or exceed the declared cell bound")]
    InvalidCatalog,
}

/// Explicit EVLA radial dish samples, in metres and dimensionless surface slope.
///
/// Content is validated and identified here, not located by a runtime search or
/// substituted with an opaque caller-supplied digest. Clones share immutable
/// samples. Data acquisition and its bounded I/O belong to the application.
#[derive(Clone, Debug)]
pub struct EvlaDishSurface {
    samples: Arc<[[f64; 3]]>,
    identity: [u8; 32],
}

impl EvlaDishSurface {
    /// Validate uniformly spaced samples from the optical axis to the 12.5 m rim.
    pub fn new(samples: Vec<[f64; 3]>) -> Result<Self, NativeAwRequestError> {
        if samples.len() < 3
            || samples.iter().flatten().any(|value| !value.is_finite())
            || samples[0] != [0.0; 3]
            || samples.last().is_none_or(|sample| sample[0] != 12.5)
        {
            return Err(NativeAwRequestError::InvalidSurface);
        }
        let step = 12.5 / (samples.len() - 1) as f64;
        if samples.iter().enumerate().any(|(index, sample)| {
            (sample[0] - index as f64 * step).abs() > step * 1e-4
                || sample[1] < 0.0
                || sample[2] < 0.0
        }) {
            return Err(NativeAwRequestError::InvalidSurface);
        }
        let mut identity = Sha256::new();
        identity.update(b"casa-rs/evla/radial-dish-surface/v1\0");
        identity.update((samples.len() as u64).to_le_bytes());
        for value in samples.iter().flatten() {
            identity.update(value.to_bits().to_le_bytes());
        }
        Ok(Self {
            samples: samples.into(),
            identity: identity.finalize().into(),
        })
    }

    /// Parse a caller-supplied three-column radius/height/slope text input.
    pub fn from_surface_text(text: &str) -> Result<Self, NativeAwRequestError> {
        let samples = text
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                let mut values = line.split_whitespace();
                let mut sample = [0.0; 3];
                for value in &mut sample {
                    *value = values
                        .next()
                        .and_then(|v| v.parse::<f64>().ok())
                        .ok_or(NativeAwRequestError::InvalidSurface)?;
                }
                if values.next().is_some() {
                    return Err(NativeAwRequestError::InvalidSurface);
                }
                Ok(sample)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Self::new(samples)
    }

    /// Validated immutable physical samples.
    #[must_use]
    pub fn samples(&self) -> &[[f64; 3]] {
        &self.samples
    }

    /// Identity of the parsed sample content, independent of source path.
    #[must_use]
    pub const fn content_identity(&self) -> [u8; 32] {
        self.identity
    }

    /// Shared sample payload bytes, charged once to generation residency.
    #[must_use]
    pub fn resident_bytes(&self) -> usize {
        size_of_val(self.samples.as_ref())
    }
}

/// Scientific sampling of one native paired EVLA convolution cell.
///
/// Increments describe the working sky grid, not the final cropped UV plane.
/// The first provider uses centered, even, square SIN grids, circular parallel
/// hands and f64 geometry / Complex32 field and transform arithmetic.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct EvlaAwCellRequest {
    /// Full working FFT extent, before support selection.
    pub size: usize,
    /// Signed sky increments in radians per pixel.
    pub sky_increment_rad: [f64; 2],
    /// Beam frequency in Hz, before CASA-compatible float rounding.
    pub frequency_hz: f64,
    /// Explicit beam frequency for the paired weight product, in Hz.
    pub conjugate_frequency_hz: f64,
    /// Nonnegative W coordinate in wavelengths at the cell frequency.
    pub w_wavelengths: f64,
    /// Physical parallactic angle in radians.
    pub parallactic_angle_rad: f64,
    /// Circular parallel-hand Mueller element: 0 or 15.
    pub mueller: usize,
    /// Integer CF sampling per normal grid pixel.
    pub oversampling: usize,
    /// Whether the prolate-spheroidal anti-aliasing term participates.
    pub prolate_spheroidal: bool,
    /// Whether the EVLA aperture response participates.
    pub aperture: bool,
}

impl EvlaAwCellRequest {
    /// Numerical residency ceiling for six Complex32 working planes, one
    /// disposable FFTW planning plane and one opaque native-plan allowance.
    pub fn generation_workspace_bytes(&self) -> Result<usize, NativeAwRequestError> {
        self.validate()?;
        self.size
            .checked_mul(self.size)
            .and_then(|pixels| pixels.checked_mul(8))
            .and_then(|values| values.checked_add(64))
            .and_then(|values| values.checked_mul(8))
            .ok_or(NativeAwRequestError::InvalidSampling)
    }

    /// Validate a cell before numerical generation or scientific identity minting.
    pub fn validate(&self) -> Result<(), NativeAwRequestError> {
        if self.size < 8
            || !self.size.is_multiple_of(2)
            || self
                .size
                .checked_mul(self.size)
                .and_then(|n| n.checked_mul(6 * 8))
                .is_none()
            || self
                .sky_increment_rad
                .iter()
                .any(|v| !v.is_finite() || *v == 0.0)
            || self.sky_increment_rad[0] >= 0.0
            || self.sky_increment_rad[1] <= 0.0
            || self.sky_increment_rad[0].abs() != self.sky_increment_rad[1].abs()
            || !self.frequency_hz.is_finite()
            || !(0.9e9..=8e9).contains(&self.frequency_hz)
            || !self.conjugate_frequency_hz.is_finite()
            || !(0.9e9..=8e9).contains(&self.conjugate_frequency_hz)
            || !self.w_wavelengths.is_finite()
            || self.w_wavelengths < 0.0
            || !self.parallactic_angle_rad.is_finite()
            || !matches!(self.mueller, 0 | 15)
            || self.oversampling == 0
            || self.oversampling > self.size / 4
        {
            return Err(NativeAwRequestError::InvalidSampling);
        }
        Ok(())
    }
}

/// One selected SPW's explicit native beam-frequency sampling.
#[derive(Clone, Debug, PartialEq)]
pub struct NativeAwFrequencyGroup {
    /// Selected MeasurementSet spectral-window identifier.
    pub spectral_window: u32,
    /// Selected physical channel frequencies, in native ascending/descending order.
    pub channel_frequencies_hz: Vec<f64>,
    /// Representative CF frequency in Hz resolved by the application planner.
    pub cf_frequency_hz: f64,
}

/// Working image geometry and discrete convolution sampling.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct NativeAwGrid {
    /// Even full working FFT extent, before support selection.
    pub size: usize,
    /// Signed square SIN sky increments, in radians per working pixel.
    pub sky_increment_rad: [f64; 2],
    /// Integer convolution sampling per normal grid pixel.
    pub oversampling: usize,
}

/// Explicit choices for every independently enabled A/W term.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeAwTerms {
    /// Apply the EVLA aperture response.
    pub aperture: bool,
    /// Apply nonzero W phase screens.
    pub w_term: bool,
    /// Apply the prolate-spheroidal anti-aliasing function.
    pub prolate_spheroidal: bool,
    /// Use per-SPW beam-frequency sampling rather than only the image reference.
    pub wideband: bool,
    /// Use the nearest sampled conjugate frequency in the weight beam product.
    pub conjugate_beams: bool,
}

/// Complete application-resolved science inputs and an explicit metadata bound.
#[derive(Clone, Debug)]
pub struct NativeAwRequestInput {
    /// Explicit content-identified EVLA dish model.
    pub surface: EvlaDishSurface,
    /// Antenna diameter in metres; this EVLA model requires exactly 25 m.
    pub antenna_diameter_m: f64,
    /// Ordered frequency groups, including selected physical SPW coordinates.
    pub frequencies: Vec<NativeAwFrequencyGroup>,
    /// Sorted nonnegative W samples in wavelengths.
    pub w_values: Vec<f64>,
    /// W-grid scale used by the shared prepared-cell selector.
    pub w_increment: f64,
    /// Sorted physical parallactic angles in radians.
    pub pa_values: Vec<f64>,
    /// Sorted required circular parallel-hand Mueller elements, 0 and/or 15.
    pub mueller_elements: Vec<usize>,
    /// Image reference frequency defining conjugate-frequency selection.
    pub reference_frequency_hz: f64,
    /// Working grid and oversampling; no output crop is assumed.
    pub grid: NativeAwGrid,
    /// Explicit independently enabled scientific terms.
    pub terms: NativeAwTerms,
    /// Maximum expected cells admitted by the application metadata policy.
    pub maximum_cells: usize,
}

impl NativeAwRequestInput {
    /// Validate the catalog axes and terms (`NativeAwRequest::new`'s
    /// rules): sorted finite w values from zero, a positive w increment,
    /// sorted parallactic angles, parallel-hand Mueller elements, ascending
    /// distinct frequency groups in the EVLA bands, a single reference group
    /// without wideband selection, and a bounded non-empty cell count.
    pub fn validate(&self) -> Result<(), NativeAwRequestError> {
        let Self {
            frequencies,
            w_values,
            pa_values,
            mueller_elements,
            grid,
            terms,
            ..
        } = self;
        frequencies
            .len()
            .checked_mul(w_values.len())
            .and_then(|n| n.checked_mul(pa_values.len()))
            .and_then(|n| n.checked_mul(mueller_elements.len()))
            .filter(|n| *n > 0 && *n <= self.maximum_cells)
            .ok_or(NativeAwRequestError::InvalidCatalog)?;
        let strictly_sorted = |values: &[f64]| {
            values.iter().all(|v| v.is_finite()) && values.windows(2).all(|pair| pair[0] < pair[1])
        };
        if self.antenna_diameter_m != 25.0
            || !strictly_sorted(w_values)
            || w_values[0] < 0.0
            || !self.w_increment.is_finite()
            || self.w_increment <= 0.0
            || !strictly_sorted(pa_values)
            || !self.reference_frequency_hz.is_finite()
            || self.reference_frequency_hz <= 0.0
            || mueller_elements.iter().any(|m| !matches!(m, 0 | 15))
            || mueller_elements.windows(2).any(|p| p[0] >= p[1])
            || frequencies
                .windows(2)
                .any(|p| p[0].cf_frequency_hz >= p[1].cf_frequency_hz)
            || frequencies.iter().any(|group| {
                !group.cf_frequency_hz.is_finite()
                    || !(0.9e9..=8e9).contains(&group.cf_frequency_hz)
                    || group.channel_frequencies_hz.is_empty()
                    || group
                        .channel_frequencies_hz
                        .iter()
                        .any(|f| !f.is_finite() || *f <= 0.0)
            })
            || frequencies.iter().enumerate().any(|(i, g)| {
                frequencies[..i]
                    .iter()
                    .any(|p| p.spectral_window == g.spectral_window)
            })
            || (!terms.w_term && w_values.as_slice() != [0.0])
            || (!terms.wideband
                && (frequencies.len() != 1
                    || frequencies[0].cf_frequency_hz != self.reference_frequency_hz))
        {
            return Err(NativeAwRequestError::InvalidCatalog);
        }
        EvlaAwCellRequest {
            size: grid.size,
            sky_increment_rad: grid.sky_increment_rad,
            frequency_hz: frequencies[0].cf_frequency_hz,
            conjugate_frequency_hz: frequencies[0].cf_frequency_hz,
            w_wavelengths: w_values[0],
            parallactic_angle_rad: pa_values[0],
            mueller: mueller_elements[0],
            oversampling: grid.oversampling,
            prolate_spheroidal: terms.prolate_spheroidal,
            aperture: terms.aperture,
        }
        .validate()
    }
}
