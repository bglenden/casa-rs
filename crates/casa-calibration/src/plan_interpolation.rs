// SPDX-License-Identifier: LGPL-3.0-or-later
//! Independent temporal and spectral calibration interpolation.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Calibration interpolation curves and CASA context modifiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum ApplyInterpolationMode {
    /// Nearest-neighbor lookup.
    Nearest,
    /// Linear interpolation on the primary solve axis.
    Linear,
    /// Nearest in time, linear in frequency.
    NearestLinear,
    /// CASA's independent time/frequency curves and context modifiers.
    Casa {
        /// Amplitude/phase interpolation along time.
        time: crate::TimeInterpolation,
        /// Amplitude/phase interpolation along frequency.
        frequency: crate::FrequencyInterpolation,
        /// Keep observation time series separate when their metadata agrees.
        per_observation: bool,
        /// Keep scan time series separate, taking precedence over observations.
        per_scan: bool,
        /// Scale unwrapped phase by the data/calibration reference frequency.
        phase_delay: bool,
        /// Match frequencies relative to each window's center and sideband.
        relative_frequency: bool,
    },
    /// An invalid CASA interpolation string, rejected during task setup.
    Invalid,
}

impl ApplyInterpolationMode {
    /// Returns `true` when the mode depends on frequency-grid metadata.
    pub fn uses_frequency_axis(self) -> bool {
        matches!(self, Self::NearestLinear | Self::Casa { .. })
    }

    /// Time curve and context used by the shared calibration interpolator.
    pub fn time_options(self, observation_count: usize) -> crate::TimeInterpolationOptions {
        let mut options = crate::TimeInterpolationOptions::new(match self {
            Self::Nearest | Self::NearestLinear => crate::TimeInterpolation::Nearest,
            Self::Casa { time, .. } => time,
            _ => crate::TimeInterpolation::Linear,
        });
        options.observation_count = observation_count;
        if let Self::Casa {
            per_observation,
            per_scan,
            phase_delay,
            ..
        } = self
        {
            options.per_observation = per_observation;
            options.per_scan = per_scan;
            options.phase_delay = phase_delay;
        }
        options
    }

    /// Frequency curve, defaulting to CASA's linear bandpass resampling.
    pub fn frequency_mode(self) -> crate::FrequencyInterpolation {
        match self {
            Self::Casa { frequency, .. } => frequency,
            _ => crate::FrequencyInterpolation::Linear,
        }
    }
}
