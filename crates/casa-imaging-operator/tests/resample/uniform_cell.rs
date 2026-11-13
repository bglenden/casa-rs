// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_operator::{
    CellHold, CfKey, ConvolutionFunctionSet, GridPrecision, ImageCorrection, KernelNormalisation,
    MeasurementOperator, MuellerRouting, NativeRow, PolarizationRouting, RowContext, SampleBuffer,
    SpectralKernel, SpectralResampler, Spheroidal, TapLayout,
};
use num_complex::Complex32;

use super::common::{Rng, geometry};
use super::{STOKES_I, XX_YY, axis, context, natural};

/// The spheroidal set without its declared one cell, so placement asks it
/// for the key, admission and prediction w of every sample.
struct Undeclared(Spheroidal);

impl ConvolutionFunctionSet for Undeclared {
    fn key(&self, row: &RowContext, freq_hz: f64, w_lambda: f64) -> CfKey {
        self.0.key(row, freq_hz, w_lambda)
    }

    fn taps<'s>(&'s self, key: CfKey, hold: &'s mut CellHold) -> TapLayout<'s> {
        self.0.taps(key, hold)
    }

    fn max_half_support(&self) -> [u16; 2] {
        self.0.max_half_support()
    }

    fn weight_taps<'s>(&'s self, key: CfKey, hold: &'s mut CellHold) -> Option<TapLayout<'s>> {
        self.0.weight_taps(key, hold)
    }

    fn mueller(&self) -> &MuellerRouting {
        self.0.mueller()
    }

    fn image_correction(&self) -> &ImageCorrection {
        self.0.image_correction()
    }

    fn normalisation(&self) -> KernelNormalisation {
        self.0.normalisation()
    }

    fn pointing_ramp(&self) -> bool {
        self.0.pointing_ramp()
    }

    fn tap_footprint(&self) -> casa_imaging_operator::TapFootprint {
        self.0.tap_footprint()
    }
}

/// A set that declares its one cell ([`ConvolutionFunctionSet::uniform_key`])
/// places every row exactly as the per-sample route does: the same
/// placements, values and weights, for linear output channels over rows of
/// both signs of w, with phase shifts and flags.
#[test]
fn a_declared_uniform_cell_places_as_the_per_sample_route() {
    let polarization = PolarizationRouting::compile(&XX_YY, &STOKES_I).expect("routing");
    let resampler = SpectralResampler::channel_local(axis(1.0, 0.006, 6), SpectralKernel::Linear);
    let declared = Spheroidal::new(&geometry(), &polarization);
    assert_eq!(declared.uniform_key(), Some(CfKey::default()));
    let undeclared = Undeclared(Spheroidal::new(&geometry(), &polarization));
    assert_eq!(undeclared.uniform_key(), None);
    let operator = |cf: Box<dyn ConvolutionFunctionSet>| {
        MeasurementOperator::new(
            geometry(),
            resampler.basis(),
            polarization.clone(),
            cf,
            GridPrecision::F64,
        )
    };
    let (declared, undeclared) = (operator(Box::new(declared)), operator(Box::new(undeclared)));
    let frequencies = (0..16)
        .map(|channel| 1.0e9 + f64::from(channel) * 2.3e6)
        .collect::<Vec<_>>();
    let mut rng = Rng::new(71);
    for _ in 0..40 {
        let cells = frequencies.len() * 2;
        let values = (0..cells)
            .map(|_| Complex32::new(rng.signed() as f32, rng.signed() as f32))
            .collect::<Vec<_>>();
        let weights = (0..cells)
            .map(|_| (0.5 + rng.unit()) as f32)
            .collect::<Vec<_>>();
        let flags = (0..cells).map(|_| rng.unit() < 0.1).collect::<Vec<_>>();
        let row = NativeRow {
            uvw_m: [
                rng.signed() * 400.0,
                rng.signed() * 400.0,
                rng.signed() * 50.0,
            ],
            phase_shift_m: rng.signed() * 0.05,
            pointing_offset_rad: [0.0; 2],
            frequencies_hz: &frequencies,
            values: &values,
            weights: &weights,
            flags: &flags,
            row_flag: false,
            context: context(),
        };
        let place = |operator: &MeasurementOperator| {
            let mut out = SampleBuffer::new(2);
            resampler
                .place(operator, &natural(), &row, &mut out)
                .expect("place");
            out
        };
        let (fast, general) = (place(&declared), place(&undeclared));
        assert!(!general.is_empty());
        assert_eq!(fast.placements(), general.placements());
        let (fast, general) = (fast.block(), general.block());
        assert_eq!(fast.values, general.values);
        assert_eq!(fast.weights, general.weights);
    }
}
