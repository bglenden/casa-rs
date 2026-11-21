// SPDX-License-Identifier: LGPL-3.0-or-later

use std::f64::consts::{FRAC_PI_2, LN_2, PI, SQRT_2};

#[derive(Clone, Copy)]
pub(super) struct Uncertainty {
    pub(super) peak: f64,
    pub(super) flux: f64,
    pub(super) direction: [f64; 2],
    pub(super) major: f64,
    pub(super) minor: f64,
    pub(super) angle: f64,
}

pub(super) struct Noise {
    pub(super) rms: f64,
    pub(super) pixel_width: f64,
    pub(super) correlation: Option<f64>,
}

impl Noise {
    // Condon's uncertainty equations as used by CASA's ImageFitter.
    // Axes, angle, correlation and direction errors are all in radians.
    pub(super) fn uncertainty(
        &self,
        brightness: f64,
        integrated: f64,
        [major, minor, angle]: [f64; 3],
        [fix_flux, fix_x, fix_y, fix_major, fix_ratio, fix_angle]: [bool; 6],
    ) -> Uncertainty {
        let fwhm_sigma = (8. * LN_2).sqrt();
        let signal_to_noise = brightness.abs() / self.rms;
        let [peak_factor, mut major_factor, mut minor_factor] =
            if let Some(noise) = self.correlation {
                let effective_snr = signal_to_noise * 0.5 * (major * minor).sqrt() / noise;
                let major_term = 1. + (noise / major).powi(2);
                let minor_term = 1. + (noise / minor).powi(2);
                [[1.5, 1.5], [2.5, 0.5], [0.5, 2.5]].map(|[a, b]| {
                    SQRT_2 / (effective_snr * major_term.powf(a / 2.) * minor_term.powf(b / 2.))
                })
            } else {
                let effective_snr = PI.sqrt() / fwhm_sigma / self.pixel_width
                    * signal_to_noise
                    * (major * minor).sqrt();
                [SQRT_2 / effective_snr; 3]
            };
        if self.correlation.is_some() {
            if fix_major && fix_x {
                major_factor = 0.;
            }
            if fix_major && fix_ratio && fix_y && fix_angle {
                minor_factor = 0.;
            }
        }
        let peak_error = if fix_flux {
            0.
        } else {
            peak_factor * brightness.abs()
        };
        let major_error = if fix_major { 0. } else { major_factor * major };
        let minor_error = if fix_major && fix_ratio {
            0.
        } else {
            minor_factor * minor
        };
        let angle_error = if fix_angle {
            0.
        } else if major == minor {
            FRAC_PI_2
        } else {
            minor_factor * SQRT_2 * major * minor / (major * major - minor * minor)
        };

        let (sin, cos) = (angle + FRAC_PI_2).sin_cos();
        let err_x = major_factor * major / fwhm_sigma;
        let err_y = minor_factor * minor / fwhm_sigma;
        let longitude = if fix_x {
            0.
        } else {
            (err_x * cos).hypot(err_y * sin)
        };
        let latitude = if fix_y {
            0.
        } else {
            (err_x * sin).hypot(err_y * cos)
        };

        let flux_error = if let Some(noise) = self.correlation {
            let area_ratio = noise * noise / major / minor;
            let axis_term = (major_error / major).powi(2) + (minor_error / minor).powi(2);
            integrated * ((peak_error / brightness).powi(2) + area_ratio * axis_term).sqrt()
        } else if fix_flux && fix_major && fix_ratio {
            0.
        } else {
            peak_factor * integrated
        };
        Uncertainty {
            peak: peak_error,
            flux: flux_error,
            direction: [longitude, latitude],
            major: major_error,
            minor: minor_error,
            angle: angle_error,
        }
    }
}
