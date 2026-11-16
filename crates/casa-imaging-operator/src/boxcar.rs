// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA ConvolveGridder's one-pixel BOX kernel and inverse-sinc correction.
use crate::convolution::{
    CellHold, ConvolutionFunctionSet, ImageCorrection, KernelNormalisation, MuellerRouting,
    RowContext, TapFootprint, TapLayout,
};
use crate::{CfKey, GridGeometry, PolarizationRouting};

const SAMPLING: u16 = 100;
const ROWS: [f32; 101] = [1.; 101];

/// Nearest-pixel gridding, with CASA's BOX deapodization on both sides.
#[derive(Clone, Debug)]
pub struct Boxcar {
    mueller: MuellerRouting,
    correction: ImageCorrection,
}

impl Boxcar {
    /// The one-pixel kernel for the padded `geometry` and polarization route.
    #[must_use]
    pub fn new(geometry: &GridGeometry, polarization: &PolarizationRouting) -> Self {
        let axis = |n: usize| {
            (0..n)
                .map(|i| {
                    let x = std::f64::consts::PI * (i as f64 - (n / 2) as f64) / n as f64;
                    if x == 0. { 1. } else { x / x.sin() }
                })
                .collect()
        };
        let [nx, ny] = geometry.grid_shape();
        Self {
            mueller: MuellerRouting::scalar(polarization.pol_map(), polarization.grid_pols()),
            correction: ImageCorrection::new(axis(nx), axis(ny)),
        }
    }

    /// Upper bound for the kernel's routing, corrections and fine-offset row.
    #[must_use]
    pub const fn bytes(geometry: &GridGeometry) -> u64 {
        size_of::<[f32; 101]>() as u64
            + MuellerRouting::MAXIMUM_BYTES
            + ImageCorrection::bytes(geometry.grid_shape())
    }
}

impl ConvolutionFunctionSet for Boxcar {
    fn key(&self, _row: &RowContext, _freq_hz: f64, _w_lambda: f64) -> CfKey {
        CfKey::default()
    }
    fn uniform_key(&self) -> Option<CfKey> {
        Some(CfKey::default())
    }
    fn taps<'s>(&'s self, _key: CfKey, _hold: &'s mut CellHold) -> TapLayout<'s> {
        TapLayout::SeparableReal {
            rows: &ROWS,
            support: 1,
            oversampling: SAMPLING,
        }
    }
    fn max_half_support(&self) -> [u16; 2] {
        [0; 2]
    }
    fn weight_taps<'s>(&'s self, _key: CfKey, _hold: &'s mut CellHold) -> Option<TapLayout<'s>> {
        None
    }
    fn mueller(&self) -> &MuellerRouting {
        &self.mueller
    }
    fn image_correction(&self) -> &ImageCorrection {
        &self.correction
    }
    fn normalisation(&self) -> KernelNormalisation {
        KernelNormalisation::UnitSum
    }
    fn pointing_ramp(&self) -> bool {
        false
    }
    fn tap_footprint(&self) -> TapFootprint {
        TapFootprint::of([self.taps(CfKey::default(), &mut CellHold::new())])
    }
}
