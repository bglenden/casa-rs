// SPDX-License-Identifier: LGPL-3.0-or-later
//! The standard prolate-spheroidal kernel set (CASA `ConvolveGridder`:
//! support 3, oversampling 100, `grdsf` correction).

use crate::convolution::{
    ConvolutionFunctionSet, ImageCorrection, MuellerRouting, RowContext, TapLayout,
};
use crate::geometry::GridGeometry;
use crate::polarization::PolarizationRouting;
use crate::sample::CfKey;

/// Taps per axis of the standard kernel.
pub const SPHEROIDAL_SUPPORT: u16 = 7;
/// Fine offsets per cell of the standard kernel.
pub const SPHEROIDAL_OVERSAMPLING: u16 = 100;

const HALF_SUPPORT: usize = SPHEROIDAL_SUPPORT as usize / 2;

/// The standard separable spheroidal kernel set: one cell, real taps
/// normalised to unit sum per fractional offset, scalar Mueller routing and
/// the `1/grdsf` image correction on both sides of the operator.
#[derive(Clone, Debug)]
pub struct Spheroidal {
    rows: Box<[f32]>,
    mueller: MuellerRouting,
    correction: ImageCorrection,
}

impl Spheroidal {
    /// Kernel set for `geometry`'s padded grid routing `polarization`'s
    /// correlations.
    #[must_use]
    pub fn new(geometry: &GridGeometry, polarization: &PolarizationRouting) -> Self {
        let [nx, ny] = geometry.grid_shape();
        Self {
            rows: build_rows().into_boxed_slice(),
            mueller: MuellerRouting::scalar(polarization.pol_map(), polarization.grid_pols()),
            correction: ImageCorrection::new(correction_axis(nx), correction_axis(ny)),
        }
    }
}

impl ConvolutionFunctionSet for Spheroidal {
    fn key(&self, _row: &RowContext, _freq_hz: f64, _w_lambda: f64) -> CfKey {
        CfKey::default()
    }

    fn taps(&self, _key: CfKey) -> TapLayout<'_> {
        TapLayout::SeparableReal {
            rows: &self.rows,
            support: SPHEROIDAL_SUPPORT,
            oversampling: SPHEROIDAL_OVERSAMPLING,
        }
    }

    fn weight_taps(&self, _key: CfKey) -> Option<TapLayout<'_>> {
        None
    }

    fn mueller(&self) -> &MuellerRouting {
        &self.mueller
    }

    fn image_correction(&self) -> &ImageCorrection {
        &self.correction
    }
}

/// One fractional-offset row per fine offset `o ∈ 0..=oversampling`, each
/// normalised to unit sum so `sumwt = ΣW` exactly.
fn build_rows() -> Vec<f32> {
    let oversampling = i64::from(SPHEROIDAL_OVERSAMPLING);
    let taps = SPHEROIDAL_SUPPORT as usize;
    let mut rows = Vec::with_capacity((oversampling as usize + 1) * taps);
    for row in 0..=oversampling {
        let offset = row - oversampling / 2;
        let mut weights = [0.0_f64; SPHEROIDAL_SUPPORT as usize];
        let mut sum = 0.0;
        for (tap, delta) in (-(HALF_SUPPORT as i64)..=HALF_SUPPORT as i64).enumerate() {
            let lookup = (delta * oversampling + offset).unsigned_abs();
            let value = if lookup < oversampling as u64 * HALF_SUPPORT as u64 {
                spheroidal_kernel(lookup as f64 / oversampling as f64, HALF_SUPPORT as f64)
            } else {
                0.0
            };
            weights[tap] = value;
            sum += value;
        }
        if sum > 0.0 {
            for value in &mut weights {
                *value /= sum;
            }
        }
        rows.extend(weights.iter().map(|value| *value as f32));
    }
    rows
}

fn spheroidal_kernel(distance: f64, half_support: f64) -> f64 {
    if !(distance.is_finite() && distance <= half_support) {
        return 0.0;
    }
    let nu = distance / half_support;
    (1.0 - nu * nu) * grdsf(nu)
}

/// casacore's `ConvolveGridder` correction vector: the raw spheroidal
/// response per axis, inverted. It is not renormalised at the centre
/// (`grdsf(0)` is close to, not exactly, one), which keeps the paired
/// operator identical to CASA's.
fn correction_axis(size: usize) -> Vec<f64> {
    let centre = size as f64 / 2.0;
    (0..size)
        .map(|index| {
            let nu = ((index as f64 - centre).abs() / centre).clamp(0.0, 1.0);
            let value = grdsf(nu);
            if value > 1.0e-6 { 1.0 / value } else { 0.0 }
        })
        .collect()
}

/// CASA `grdsf`: the rational approximation of the zero-order prolate
/// spheroidal function for `α = 1`, `m = 6`, on `nu ∈ [0, 1]`.
#[must_use]
pub fn grdsf(nu: f64) -> f64 {
    const P0: [f64; 5] = [
        8.203_343e-2,
        -3.644_705e-1,
        6.278_660e-1,
        -5.335_581e-1,
        2.312_756e-1,
    ];
    const P1: [f64; 5] = [
        4.028_559e-3,
        -3.697_768e-2,
        1.021_332e-1,
        -1.201_436e-1,
        6.412_774e-2,
    ];
    const Q0: [f64; 3] = [1.0, 8.212_018e-1, 2.078_043e-1];
    const Q1: [f64; 3] = [1.0, 9.599_102e-1, 2.918_724e-1];
    if !(0.0..=1.0).contains(&nu) {
        return 0.0;
    }
    let (p, q, end) = if nu < 0.75 {
        (&P0, &Q0, 0.75)
    } else {
        (&P1, &Q1, 1.0)
    };
    let delta = nu * nu - end * end;
    let numerator = p
        .iter()
        .enumerate()
        .map(|(order, value)| value * delta.powi(order as i32))
        .sum::<f64>();
    let denominator = q
        .iter()
        .enumerate()
        .map(|(order, value)| value * delta.powi(order as i32))
        .sum::<f64>();
    if denominator == 0.0 {
        0.0
    } else {
        numerator / denominator
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_are_unit_sum_and_symmetric_about_the_centre_offset() {
        let rows = build_rows();
        let taps = SPHEROIDAL_SUPPORT as usize;
        assert_eq!(rows.len(), (SPHEROIDAL_OVERSAMPLING as usize + 1) * taps);
        for row in rows.chunks_exact(taps) {
            let sum = row.iter().map(|value| f64::from(*value)).sum::<f64>();
            assert!((sum - 1.0).abs() < 1.0e-6, "row sum {sum}");
        }
        let centre = &rows[50 * taps..51 * taps];
        for tap in 0..taps {
            assert_eq!(centre[tap], centre[taps - 1 - tap]);
        }
        assert!(centre[3] > centre[2] && centre[2] > centre[1]);
    }

    #[test]
    fn grdsf_matches_casa_reference_points() {
        assert!(
            (grdsf(0.0) - 1.0).abs() < 1.0e-3,
            "grdsf(0) = {}",
            grdsf(0.0)
        );
        assert!(
            (grdsf(1.0) - 4.028_559e-3).abs() < 1.0e-9,
            "grdsf(1) = {}",
            grdsf(1.0)
        );
        assert_eq!(grdsf(1.0 + 1.0e-9), 0.0);
        assert!(grdsf(0.5) > grdsf(0.75) && grdsf(0.75) > grdsf(0.9));
    }
}
