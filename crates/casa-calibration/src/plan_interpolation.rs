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
    pub(crate) fn parse_casa(value: &str) -> Result<Self, String> {
        use crate::{FrequencyInterpolation as F, TimeInterpolation as T};
        let (time, frequency) = value.split_once(',').unwrap_or((value, ""));
        let time = if time.is_empty() { "linear" } else { time };
        if time.contains("aips") || frequency.contains("aips") {
            return Err("aipslin interpolation is deprecated in CASA".to_string());
        }
        let curve = if time.contains("nearest") {
            T::Nearest
        } else if time.contains("linear") {
            T::Linear
        } else if time.contains("cubic") {
            T::Cubic
        } else if time.contains("spline") {
            T::Spline
        } else {
            return Err(format!("unknown time interpolation {time:?}"));
        };
        match value {
            "" | "linear" => return Ok(Self::Linear),
            "nearest" => return Ok(Self::Nearest),
            "nearest,linear" => return Ok(Self::NearestLinear),
            _ => {}
        }
        let frequency_curve = if frequency.contains("nearest") {
            F::Nearest
        } else if frequency.contains("cubic") && !frequency.contains("linear") {
            F::Cubic
        } else if frequency.contains("spline") && !frequency.contains("linear") {
            F::Spline
        } else {
            F::Linear
        };
        let frequency_curve = if frequency.contains("flag") {
            match frequency_curve {
                F::Nearest => F::NearestFlag,
                F::Linear => F::LinearFlag,
                F::Cubic => F::CubicFlag,
                F::Spline => F::SplineFlag,
                _ => unreachable!("base interpolation"),
            }
        } else {
            frequency_curve
        };
        let contextual = time.contains("perobs") || time.contains("perscan") || time.contains("PD");
        if !contextual && !frequency.contains("rel") && frequency_curve == F::Linear {
            match curve {
                T::Nearest => {
                    return Ok(if frequency.is_empty() {
                        Self::Nearest
                    } else {
                        Self::NearestLinear
                    });
                }
                T::Linear => return Ok(Self::Linear),
                _ => {}
            }
        }
        Ok(Self::Casa {
            time: curve,
            frequency: frequency_curve,
            per_observation: time.contains("perobs"),
            per_scan: time.contains("perscan"),
            phase_delay: time.contains("PD"),
            relative_frequency: frequency.contains("rel"),
        })
    }

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
