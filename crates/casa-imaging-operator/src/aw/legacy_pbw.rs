// SPDX-License-Identifier: LGPL-3.0-or-later
//! Prediction taps from the legacy single-W-plane nPBWProjectFT cache.

use ndarray::ArrayD;
use num_complex::Complex32;

use crate::convolution::{
    CellHold, ConvolutionFunctionSet, DenseCell, ImageCorrection, KernelNormalisation,
    MuellerRouting, RowContext, TapFootprint, TapLayout,
};
use crate::dense::{OversampledPlane, dense_cell, dense_cell_bytes};
use crate::error::OperatorError;
use crate::sample::CfKey;

/// Correlation-ordered taps for an nPBWProjectFT prediction already
/// corrected by the associated average primary beam.
pub struct LegacyPbWConvolution {
    cell: DenseCell,
    half_support: [u16; 2],
    mueller: MuellerRouting,
    correction: ImageCorrection,
}

impl LegacyPbWConvolution {
    /// Peak bytes for building the prediction cell from a square cache image.
    #[must_use]
    pub fn forming_bytes(
        kernel_side: usize,
        correlations: usize,
        sampling: u16,
        support: u16,
        grid: [usize; 2],
    ) -> u64 {
        let copied_planes = (kernel_side as u64)
            .saturating_mul(kernel_side as u64)
            .saturating_mul(correlations as u64)
            .saturating_mul(size_of::<Complex32>() as u64);
        copied_planes
            .saturating_add(dense_cell_bytes(correlations, sampling, [support; 2]))
            .saturating_add(MuellerRouting::MAXIMUM_BYTES)
            .saturating_add((grid[0] as u64 + grid[1] as u64) * 2 * size_of::<f64>() as u64)
            .saturating_add(4096)
    }

    /// Repack the CASA convolution's correlation planes. The Fourier
    /// model has already received the paired primary-beam correction.
    ///
    /// # Errors
    /// The image or correlation layout is incompatible with its support.
    pub fn new(
        samples: &ArrayD<Complex32>,
        sampling: u16,
        support: u16,
        grid: [usize; 2],
        grid_pols: usize,
        pol_map: &[Option<u8>],
    ) -> Result<Self, OperatorError> {
        let [side, ny, correlations, 1] = *samples.shape() else {
            return Err(invalid("expected one legacy W plane"));
        };
        let radius = usize::from(support) * usize::from(sampling) + usize::from(sampling / 2);
        if side != ny
            || correlations != pol_map.len()
            || !(1..=4).contains(&correlations)
            || !(1..=4).contains(&grid_pols)
            || pol_map
                .iter()
                .flatten()
                .any(|&pol| usize::from(pol) >= grid_pols)
            || sampling == 0
            || support > u16::MAX / 2
            || radius > side / 2
            || side / 2 + radius >= side
        {
            return Err(invalid(
                "the legacy PB/W kernel cannot cover its fine sampling",
            ));
        }
        let values = (0..correlations)
            .map(|pol| {
                (0..side)
                    .flat_map(|y| (0..side).map(move |x| samples[[x, y, pol, 0]]))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let planes = values
            .iter()
            .map(|values| OversampledPlane {
                values,
                side,
                centre: [side / 2; 2],
                sampling,
            })
            .collect::<Vec<_>>();
        let half_support = [support; 2];
        let cell = dense_cell(&planes, half_support);
        let direct = (0..grid_pols)
            .map(|grid_pol| {
                pol_map
                    .iter()
                    .enumerate()
                    .map(|(pol, &target)| (target == Some(grid_pol as u8)).then_some(pol as u8))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let mueller = MuellerRouting {
            adjoint: [direct.clone(), direct.clone()],
            forward: [direct.clone(), direct],
        };
        let correction = ImageCorrection::new(vec![1.; grid[0]], vec![1.; grid[1]]);
        Ok(Self {
            cell,
            half_support,
            mueller,
            correction,
        })
    }
}

impl ConvolutionFunctionSet for LegacyPbWConvolution {
    fn key(&self, _row: &RowContext, _frequency_hz: f64, _w_lambda: f64) -> CfKey {
        CfKey::default()
    }

    fn uniform_key(&self) -> Option<CfKey> {
        Some(CfKey::default())
    }

    fn taps<'s>(&'s self, _key: CfKey, _hold: &'s mut CellHold) -> TapLayout<'s> {
        self.cell.layout()
    }

    fn max_half_support(&self) -> [u16; 2] {
        self.half_support
    }

    fn weight_taps<'s>(&'s self, _key: CfKey, _hold: &'s mut CellHold) -> Option<TapLayout<'s>> {
        None
    }

    fn mueller(&self) -> &MuellerRouting {
        &self.mueller
    }

    fn prediction_conjugates(&self, _key: CfKey, _w_positive: bool) -> bool {
        true
    }

    fn image_correction(&self) -> &ImageCorrection {
        &self.correction
    }

    fn normalisation(&self) -> KernelNormalisation {
        KernelNormalisation::KernelSum
    }

    fn pointing_ramp(&self) -> bool {
        false
    }

    fn tap_footprint(&self) -> TapFootprint {
        TapFootprint::of([self.cell.layout()])
    }
}

fn invalid(reason: &'static str) -> OperatorError {
    OperatorError::ConvolutionFunction { reason }
}
