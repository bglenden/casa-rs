// SPDX-License-Identifier: LGPL-3.0-or-later
//! Published fixed flux polynomials used by CASA's FluxStdsQS2.

#[derive(Clone, Copy)]
/// Absolute calibrator standards evaluated from fixed coefficients.
pub enum FixedScale {
    /// Baars et al. flux-density scale.
    Baars,
    /// Perley 1990 flux-density scale.
    Perley90,
    /// Perley–Taylor 1995 flux-density scale.
    PerleyTaylor95,
    /// Perley–Taylor 1999 flux-density scale.
    PerleyTaylor99,
    /// Perley–Butler 2010 flux-density scale.
    PerleyButler2010,
    /// Stevens–Reynolds 2016 flux-density scale.
    StevensReynolds2016,
}

/// A fixed-coefficient frequency spectrum for one calibrator in the selected standard.
pub struct FixedSpectrum {
    reference_hz: f64,
    coefficients: &'static [f32],
    upper: Option<(f64, &'static [f32])>,
}

const PKS_1934_LOW: &[f32] = &[1.170418, 0.248618, -1.649694, 0.605334];
const PKS_1934_HIGH: &[f32] = &[-2.5739, 10.0707, -10.0595, 2.9372];
const PKS_1934_STEVENS_HIGH: &[f32] = &[1.7581, -1.3763];

impl FixedScale {
    pub(super) fn spectrum(self, name: &str) -> Option<FixedSpectrum> {
        use FixedScale::*;

        let reference_hz = match self {
            Baars | Perley90 | PerleyTaylor95 => 1e6,
            PerleyTaylor99 | PerleyButler2010 | StevensReynolds2016 => 1e9,
        };
        let coefficients: &[f32] = match (self, name) {
            (Baars, "3C286") => &[1.480, 0.292, -0.124],
            (Baars, "3C48") => &[2.345, 0.071, -0.138],
            (Baars, "3C147") => &[1.766, 0.447, -0.184],
            (Baars | Perley90, "3C138") => &[2.009, -0.07176, -0.0862],
            (Baars, "1934-638") => &[-23.839, 19.569, -4.8168, 0.35836],
            (Baars | Perley90, "3C295") => &[1.485, 0.759, -0.255],
            (Perley90, "3C286") => &[1.35899, 0.3599, -0.13338],
            (Perley90, "3C48") => &[2.0868, 0.20889, -0.15498],
            (Perley90, "3C147") => &[1.92641, 0.36072, -0.17389],
            (Perley90 | PerleyTaylor95, "1934-638") => &[-30.7667, 26.4908, -7.0977, 0.605334],
            (PerleyTaylor95, "3C286") => &[0.50344, 1.05026, -0.31666, 0.01602],
            (PerleyTaylor95, "3C48") => &[1.16801, 1.07526, -0.42254, 0.02699],
            (PerleyTaylor95, "3C147") => &[0.05702, 2.09340, -0.7076, 0.05477],
            (PerleyTaylor95, "3C138") => &[1.97498, -0.23918, 0.01333, -0.01389],
            (PerleyTaylor95, "3C295") => &[1.28872, 0.94172, -0.31113, 0.00569],
            (PerleyTaylor99, "3C286") => &[1.23734, -0.43276, -0.14223, 0.00345],
            (PerleyTaylor99, "3C48") => &[1.31752, -0.74090, -0.16708, 0.01525],
            (PerleyTaylor99, "3C147") => &[1.44856, -0.67252, -0.21124, 0.04077],
            (PerleyTaylor99, "3C138") => &[1.00761, -0.55629, -0.11134, -0.0146],
            (PerleyTaylor99, "3C295") => &[1.46744, -0.7735, -0.25912, 0.00752],
            (PerleyButler2010, "3C286") => &[1.2361, -0.4127, -0.1864, 0.0294],
            (PerleyButler2010, "3C48") => &[1.3197, -0.7253, -0.2023, 0.0540],
            (PerleyButler2010, "3C147") => &[1.4428, -0.6300, -0.3142, 0.1032],
            (PerleyButler2010, "3C138") => &[1.0053, -0.4384, -0.1855, 0.0511],
            (PerleyButler2010, "3C295") => &[1.4605, -0.7043, -0.3951, 0.0815],
            (PerleyButler2010, "3C196") => &[1.2753, -0.7971, -0.2255, 0.0380],
            (PerleyTaylor99 | PerleyButler2010 | StevensReynolds2016, "1934-638") => PKS_1934_LOW,
            _ => return None,
        };
        let upper = match (self, name) {
            (PerleyTaylor99 | PerleyButler2010, "1934-638") => Some((10e9, PKS_1934_HIGH)),
            (StevensReynolds2016, "1934-638") => Some((11.1496e9, PKS_1934_STEVENS_HIGH)),
            _ => None,
        };
        Some(FixedSpectrum {
            reference_hz,
            coefficients,
            upper,
        })
    }
}

impl FixedSpectrum {
    pub(super) fn flux(&self, frequency_hz: f64) -> f64 {
        let coefficients = self
            .upper
            .filter(|(limit, _)| frequency_hz > *limit)
            .map_or(self.coefficients, |(_, coefficients)| coefficients);
        let log_frequency = (frequency_hz / self.reference_hz).log10();
        10_f64.powf(super::log_polynomial(coefficients, log_frequency))
    }
}
