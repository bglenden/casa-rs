// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_core::measures::frequency::FrequencyRef;

use crate::{CoordinateError, TabularCoordinate};

use super::SpectralCoordinate;

/// Native spectral representation stored in casacore's `nativeType` field.
/// Internally the coordinate continues to map pixels to frequency.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum SpectralNativeType {
    /// Frequency (`FREQ`).
    Frequency = 0,
    /// Radio velocity (`VRAD`).
    RadioVelocity = 1,
    /// Optical velocity (`VOPT`).
    OpticalVelocity = 2,
    /// Relativistic velocity relative to the speed of light (`BETA`).
    Beta = 3,
    /// Vacuum wavelength (`WAVE`).
    Wavelength = 4,
    /// Air wavelength (`AWAV`).
    AirWavelength = 5,
}

impl SpectralNativeType {
    pub(super) fn from_code(code: i32) -> Self {
        match code {
            1 => Self::RadioVelocity,
            2 => Self::OpticalVelocity,
            3 => Self::Beta,
            4 => Self::Wavelength,
            5 => Self::AirWavelength,
            _ => Self::Frequency,
        }
    }
}

impl SpectralCoordinate {
    /// Rebuild a sampled frequency axis at zero-based integral pixel positions.
    /// Frequencies and the rest frequency are in Hz.
    ///
    /// # Errors
    /// Returns an error if the table is empty, nonfinite, or unusable.
    pub fn from_channel_frequencies(
        frame: FrequencyRef,
        frequencies: Vec<f64>,
        rest_frequency: f64,
    ) -> Result<Self, CoordinateError> {
        if !rest_frequency.is_finite() || rest_frequency < 0.0 {
            return Err(CoordinateError::InvalidRecord(
                "invalid spectral rest frequency".into(),
            ));
        }
        let mut pixels = Vec::new();
        pixels.try_reserve_exact(frequencies.len()).map_err(|_| {
            CoordinateError::InvalidRecord("cannot allocate the spectral channel positions".into())
        })?;
        pixels.extend((0..frequencies.len()).map(|channel| channel as f64));
        TabularCoordinate::validate_samples(&pixels, &frequencies)?;
        let first = frequencies[0];
        let last = frequencies.len() - 1;
        let increment = (frequencies[last] - first) / last.max(1) as f64;
        Self::from_tabular(
            frame,
            pixels,
            frequencies,
            first,
            increment,
            0.0,
            rest_frequency,
        )
    }

    /// Relabel the native frequency frame without changing its samples.
    /// A native frame change resets the now-incompatible display conversion.
    pub fn with_frequency_reference(mut self, frame: FrequencyRef) -> Self {
        let declared = if self.undefined_frame {
            FrequencyRef::Undefined
        } else {
            self.frequency_ref
        };
        if frame != declared {
            self.undefined_frame = frame == FrequencyRef::Undefined;
            self.frequency_ref = if self.undefined_frame {
                FrequencyRef::TOPO
            } else {
                frame
            };
            self.conversion = None;
        }
        self
    }

    /// Change the active rest frequency while retaining the other entries.
    /// `frequency` is in the coordinate's stored frequency unit.
    ///
    /// # Errors
    /// Returns an error for a nonfinite or negative rest frequency.
    pub fn with_replaced_rest_frequency(mut self, frequency: f64) -> Result<Self, CoordinateError> {
        if !frequency.is_finite() || frequency < 0.0 {
            return Err(CoordinateError::InvalidRecord(
                "invalid spectral rest frequency".into(),
            ));
        }
        self.rest_frequencies[self.rest_frequency_index] = frequency;
        Ok(self)
    }

    /// Set the rest-frequency list and select its active entry by index.
    /// The list uses the coordinate's stored frequency unit.
    ///
    /// # Errors
    /// Returns an error for an absent index or a negative or nonfinite entry.
    pub fn with_selected_rest_frequencies(
        mut self,
        frequencies: Vec<f64>,
        active: usize,
    ) -> Result<Self, CoordinateError> {
        if active >= frequencies.len() {
            return Err(CoordinateError::InvalidRecord(
                "Failed AlwaysAssert which<restfreqs_p.nelements()".into(),
            ));
        }
        if frequencies
            .iter()
            .any(|value| !value.is_finite() || *value < 0.0)
        {
            return Err(CoordinateError::InvalidRecord(
                "Rest frequencies must be non-negative and finite".into(),
            ));
        }
        self.rest_frequency_index = active;
        self.rest_frequencies = frequencies;
        Ok(self)
    }

    /// Set the serialized spectral representation without changing frequency samples.
    pub fn with_native_type(mut self, native_type: SpectralNativeType) -> Self {
        self.native_type = native_type;
        self
    }
}
