// SPDX-License-Identifier: LGPL-3.0-or-later
//! PBMath::initByTelescope's radial defaults, CASA 6.7.6.14.
use super::{BeamPattern, Lookup};

/// The radial profiles selected by PBMath's telescope and reference band.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum DefaultRadial {
    /// The OVRO numeric power samples, scaled to 10.4 m.
    Ovro,
    /// The same numeric samples scaled to 45 m.
    Gmrt,
    /// The same numeric samples scaled to 12 m.
    Nrao12m,
    /// GBT's 340 MHz measured power polynomial.
    Gbt,
    /// WSRT's high-frequency cosine voltage fit.
    WsrtHigh,
    /// WSRT's low-frequency cosine voltage fit.
    WsrtLow,
    /// ATCA's legacy fallback inverse polynomial.
    AtcaLegacy,
    /// ATCA's seven frequency-dependent 16 cm fits.
    Atca16,
    /// ATCA's two frequency-dependent C/X fits.
    AtcaCx,
    /// ATCA's K/Q inverse polynomial.
    AtcaKq,
    /// ATCA's W inverse polynomial.
    AtcaW,
    /// The signed measured ATCA_C_RI voltage samples.
    AtcaMeasured,
}

const NUMERIC_POWER: [f32; 19] = [
    1.,
    0.997634,
    0.972516,
    0.913722,
    0.837871,
    0.750356,
    0.651549,
    0.549903,
    0.449083,
    0.352819,
    0.266025,
    0.190533,
    0.128047,
    0.0794855,
    0.0438381,
    0.0201386,
    0.0065117,
    0.000690041,
    8.87288e-5,
];
const ATCA_VOLTAGE: [f32; 22] = [
    1., 0.98132, 0.96365, 0.87195, 0.75109, 0.62176, 0.48793, 0.34985, 0.21586, 0.10546, 0.03669,
    -0.03556, -0.08266, -0.12810, -0.15440, -0.16090, -0.15360, -0.13566, -0.10666, -0.06847,
    -0.03136, -0.00854,
];
const ATCA_16: [(f64, [f64; 5]); 7] = [
    (
        1.332e9,
        [1., 1.06274e-3, 1.32342e-6, -8.72013e-10, 1.08020e-12],
    ),
    (
        1.588e9,
        [1., 9.80817e-4, 1.17898e-6, -7.83160e-10, 8.66199e-13],
    ),
    (
        1.844e9,
        [1., 9.53553e-4, 9.33233e-7, -4.26759e-10, 5.63667e-13],
    ),
    (
        2.100e9,
        [1., 9.78268e-4, 6.63231e-7, 4.18235e-11, 2.62297e-13],
    ),
    (
        2.356e9,
        [1., 1.02424e-3, 6.12726e-7, 2.25733e-10, 2.04834e-13],
    ),
    (
        2.612e9,
        [1., 1.05818e-3, 5.37473e-7, 4.22386e-10, 1.17530e-13],
    ),
    (
        2.868e9,
        [1., 1.10650e-3, 5.11574e-7, 5.89732e-10, 8.13628e-14],
    ),
];
const ATCA_CX: [(f64, [f64; 5]); 2] = [
    (4.8e9, [1., 1.08e-3, 1.31e-6, -1.17e-9, 1.07e-12]),
    (8.64e9, [1., 1.04e-3, 8.36e-7, -4.68e-10, 5.50e-13]),
];

impl DefaultRadial {
    pub(super) fn for_telescope(telescope: &str, frequency: f64) -> Option<Self> {
        let ghz = frequency / 1e9;
        Some(if telescope.starts_with("OVRO") {
            Self::Ovro
        } else if telescope.starts_with("GMRT") {
            Self::Gmrt
        } else if telescope.starts_with("NRAO12M") {
            Self::Nrao12m
        } else if telescope.starts_with("GBT") {
            Self::Gbt
        } else if telescope.starts_with("WSRT") {
            if ghz < 0.5 || (ghz > 0.5 && ghz < 0.8) {
                Self::WsrtLow
            } else {
                Self::WsrtHigh
            }
        } else if telescope.starts_with("ATCA_C_RI") {
            Self::AtcaMeasured
        } else if telescope.starts_with("ATCA") {
            if (7. ..12.5).contains(&ghz) || (ghz > 4. && ghz < 7.) {
                Self::AtcaCx
            } else if ghz > 1. && ghz < 3.5 {
                Self::Atca16
            } else if (ghz > 15. && ghz < 26.) || (ghz > 30. && ghz < 50.) {
                Self::AtcaKq
            } else if ghz > 80. && ghz < 120. {
                Self::AtcaW
            } else {
                Self::AtcaLegacy
            }
        } else {
            return None;
        })
    }

    pub(super) const fn maximum_radius(self) -> f64 {
        match self {
            Self::Ovro => 1.1998662 * 25. / 10.4 * 43.,
            Self::Gmrt => 1.1998662 * 25. / 45. * 43.,
            Self::Nrao12m => 1.1998662 * 25. / 12. * 43.,
            Self::Gbt => 60. * 0.34,
            Self::WsrtHigh | Self::WsrtLow => 60.,
            Self::AtcaMeasured => 21.07 * 5.5,
            _ => 53.,
        }
    }

    pub(super) fn pattern(self) -> BeamPattern {
        let radius = self.maximum_radius();
        let pattern = match self {
            Self::Ovro | Self::Gmrt | Self::Nrao12m => {
                BeamPattern::numeric(radius, &NUMERIC_POWER, false)
            }
            Self::AtcaMeasured => BeamPattern::numeric(radius, &ATCA_VOLTAGE, true),
            Self::WsrtHigh | Self::WsrtLow => BeamPattern::cosine_polynomial(
                radius,
                &[0., 0., 0., 1.],
                &[
                    0.,
                    0.,
                    0.,
                    if self == Self::WsrtLow {
                        0.01830
                    } else {
                        0.01891
                    },
                ],
                true,
            ),
            Self::AtcaLegacy => BeamPattern::polynomial(
                radius,
                &[1., 8.99e-4, 2.15e-6, -2.23e-9, 1.56e-12],
                true,
                false,
            ),
            Self::AtcaKq => BeamPattern::polynomial(
                radius,
                &[1., 9.832e-4, 1.081e-6, -4.676e-10, 6.650e-13],
                true,
                false,
            ),
            Self::AtcaW => BeamPattern::polynomial(
                radius,
                &[1., 1.271e-3, -3.040e-7, 1.410e-9, 0.],
                true,
                false,
            ),
            Self::Atca16 | Self::AtcaCx => return BeamPattern(Lookup::WideInverse(self)),
            Self::Gbt => BeamPattern::tabulated_voltage(radius, |r| {
                // The fitted power becomes negative near the outer support.
                // CASA's apply cutoff excludes these invalid samples; retain zero response.
                let coefficients = [
                    1.00632057,
                    -0.00181068914,
                    1.32948262e-6,
                    -4.72454392e-10,
                    7.11800547e-14,
                    -3.50646851e-20,
                    -7.40533706e-22,
                ];
                let mut y = 1.;
                let mut power: f64 = 0.;
                for c in coefficients {
                    power += y * c;
                    y *= (r / 0.34).powi(2);
                }
                power.max(0.).sqrt() as f32
            }),
        };
        pattern.expect("finite CASA default profile")
    }

    pub(super) fn fit(self, frequency: f64) -> InverseFit {
        let fits: &[(f64, [f64; 5])] = if self == Self::Atca16 {
            &ATCA_16
        } else {
            &ATCA_CX
        };
        let hi = fits
            .partition_point(|(f, _)| *f < frequency)
            .min(fits.len() - 1);
        let lo = hi.saturating_sub(1);
        let weight = if hi == lo {
            0.
        } else {
            ((frequency - fits[lo].0) / (fits[hi].0 - fits[lo].0)).clamp(0., 1.) as f32
        };
        InverseFit {
            low: fits[lo].1,
            high: fits[hi].1,
            weight,
        }
    }
}

/// Evaluate the same quantized Float samples without retaining a table per fit.
pub(super) struct InverseFit {
    low: [f64; 5],
    high: [f64; 5],
    weight: f32,
}
impl InverseFit {
    pub(super) fn voltage(&self, radius: f64) -> f32 {
        if !radius.is_finite() || !(0. ..=53.).contains(&radius) {
            return 0.;
        }
        let index = (radius * (9999. / 53.)).floor();
        let x2 = (index / (9999. / 53.)).powi(2);
        let sample = |coefficients: &[f64; 5]| {
            let mut y = 1.;
            let mut taper: f64 = 0.;
            for c in coefficients {
                taper += y * c;
                y *= x2;
            }
            (1. / taper.sqrt()) as f32
        };
        sample(&self.low) * (1. - self.weight) + sample(&self.high) * self.weight
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn negative_gbt_fitted_power_is_excluded_from_the_beam() {
        let beam = DefaultRadial::Gbt.pattern();
        let power = beam.at_frequency(1e9);
        assert_eq!(power.power(20.39), 0.);
        assert_eq!(power.power(20.4), 0.);
        // The central fitted response exceeds unity slightly; do not renormalize it.
        assert!((f64::from(power.power(0.)) - 1.00632057).abs() < 1e-6);
    }
}
