// SPDX-License-Identifier: LGPL-3.0-or-later
//! CPU f32 accuracy across apply boundaries on the real spheroidal (#991).
mod common;
use casa_imaging_model::PolarizationCoordinate;
use casa_imaging_operator::{
    Basis, CpuBackend, GridBackend, GridPrecision, Mode, ModeSet, PlaneRange, SampleBuffer, Work,
};
use casa_types::Correlation;
use num_complex::{Complex32, Complex64};

#[test]
fn repeated_spheroidal_psf_and_data_match_f64_and_reset_replays() {
    let operator = |precision| {
        common::operator(
            precision,
            Basis::Constant,
            &[Correlation::XX],
            &[PolarizationCoordinate::LinearXx],
        )
    };
    let f32_operator = operator(GridPrecision::F32);
    let f64_operator = operator(GridPrecision::F64);
    let mut rng = common::Rng::new(991);
    let mut placed = common::placements(f32_operator.geometry(), 1, 1, &mut rng)[0];
    placed.u = 0.0;
    placed.v = 0.0;
    placed.phase = 0.0;
    let mut block = SampleBuffer::new(1);
    for _ in 0..1024 {
        block.push(placed, &[Complex32::new(0.3, -0.2)], &[0.7]);
        // Masked nonfinite samples must not enter either sum or correction.
        block.push(placed, &[Complex32::new(f32::NAN, f32::INFINITY)], &[0.0]);
    }
    for mode in [Mode::Data, Mode::Psf] {
        let mut single = f32_operator.accumulator(PlaneRange::single(0), None, ModeSet::DATA_PSF);
        let mut double = f64_operator.accumulator(PlaneRange::single(0), None, ModeSet::DATA_PSF);
        let mut backend = CpuBackend::new();
        for _ in 0..128 {
            backend
                .apply(
                    &block.block(),
                    f32_operator.cf(),
                    Work::Grid {
                        mode,
                        acc: &mut single,
                    },
                )
                .expect("f32");
            backend
                .apply(
                    &block.block(),
                    f64_operator.cf(),
                    Work::Grid {
                        mode,
                        acc: &mut double,
                    },
                )
                .expect("f64");
        }
        let term = single.layout().term_range(mode).expect("mode").start;
        let reference = double.block::<f64>(0, 0, term);
        let result = single.block::<f32>(0, 0, term).to_vec();
        let error: f64 = result
            .iter()
            .zip(reference)
            .map(|(a, b)| (Complex64::new(f64::from(a.re), f64::from(a.im)) - b).norm_sqr())
            .sum();
        let norm: f64 = reference.iter().map(|value| value.norm_sqr()).sum();
        assert!((error / norm).sqrt() < 1e-6);
        single.reset();
        for _ in 0..128 {
            backend
                .apply(
                    &block.block(),
                    f32_operator.cf(),
                    Work::Grid {
                        mode,
                        acc: &mut single,
                    },
                )
                .expect("replay");
        }
        assert_eq!(single.block::<f32>(0, 0, term), result);
    }
}
