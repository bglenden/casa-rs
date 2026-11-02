// SPDX-License-Identifier: LGPL-3.0-or-later
//! #991: repeated compact-array contributions must retain scientific accuracy.

mod common;

use casa_imaging_metal::MetalBackend;
use casa_imaging_operator::{
    Basis, CfKey, CpuBackend, GridAccumulator, GridBackend, GridPrecision, GridScalar, Mode,
    ModeSet, Placement, PlaneRange, SampleBuffer, Work,
};
use common::{Kernels, Rng, metal, operator};
use num_complex::Complex32;

#[test]
fn changing_apply_boundaries_preserves_the_compensated_sum() {
    let operator = operator(Kernels::Spheroidal, Basis::Constant, &mut Rng::new(4));
    let Some(backend) = metal(&operator) else {
        return;
    };
    let mut backend = backend.with_deferred_grid_completion();
    let layout = operator.accumulator_layout(PlaneRange::single(0), None, ModeSet::DATA);
    let mut whole = MetalBackend::accumulator(layout.clone()).unwrap();
    let mut streamed = MetalBackend::accumulator(layout).unwrap();
    let make = |count| {
        let mut buffer = SampleBuffer::with_capacity(2, count);
        for _ in 0..count {
            buffer.push(
                Placement {
                    u: 0.0,
                    v: 0.0,
                    w: 0.0,
                    phase: 0.0,
                    plane: 0,
                    spectral: 0.0,
                    cf: CfKey { group: 0, cube: 0 },
                    gradient: [0.0; 2],
                    prediction_w_positive: false,
                },
                &[Complex32::new(1.1, -0.7); 2],
                &[0.7; 2],
            );
        }
        buffer
    };
    for (buffer, repeat, acc) in [
        (make(1 << 18), 1, &mut whole),
        (make(1 << 10), 1 << 8, &mut streamed),
    ] {
        for _ in 0..repeat {
            backend
                .apply(
                    &buffer.block(),
                    operator.cf(),
                    Work::Grid {
                        mode: Mode::Data,
                        acc,
                    },
                )
                .unwrap();
        }
        backend.complete().unwrap();
    }
    let mut error = 0.0_f64;
    let mut power = 0.0_f64;
    for (a, b) in f32::cells(whole.storage())
        .iter()
        .zip(f32::cells(streamed.storage()))
    {
        error += f64::from((*a - *b).norm_sqr());
        power += f64::from(a.norm_sqr());
    }
    let nrms = (error / power).sqrt();
    assert!(nrms <= 1e-6, "apply-boundary NRMS={nrms}");
    assert_eq!(whole.sumwt(), streamed.sumwt());
}

#[test]
fn compact_contributions_match_f64_across_applies_and_reset() {
    let operator = operator(Kernels::Spheroidal, Basis::Constant, &mut Rng::new(4));
    let Some(mut backend) = metal(&operator) else {
        return;
    };
    let layout = operator.accumulator_layout(PlaneRange::single(0), None, ModeSet::DATA_PSF);
    let mut reference = GridAccumulator::new(layout.clone(), GridPrecision::F64);
    let mut actual = MetalBackend::accumulator(layout).unwrap();
    let mut samples = SampleBuffer::with_capacity(2, 1 << 18);
    let placement = Placement {
        u: 0.0,
        v: 0.0,
        w: 0.0,
        phase: 0.0,
        plane: 0,
        spectral: 0.0,
        cf: CfKey { group: 0, cube: 0 },
        gradient: [0.0; 2],
        prediction_w_positive: false,
    };
    for _ in 0..(1 << 18) {
        samples.push(placement, &[Complex32::new(1.1, -0.7); 2], &[0.7; 2]);
    }
    for iteration in 0..2 {
        reference.reset();
        actual.reset();
        for _ in 0..3 {
            for mode in [Mode::Data, Mode::Psf] {
                CpuBackend::new()
                    .apply(
                        &samples.block(),
                        operator.cf(),
                        Work::Grid {
                            mode,
                            acc: &mut reference,
                        },
                    )
                    .unwrap();
                backend
                    .apply(
                        &samples.block(),
                        operator.cf(),
                        Work::Grid {
                            mode,
                            acc: &mut actual,
                        },
                    )
                    .unwrap();
            }
        }
        backend.complete().unwrap();
        let expected = f64::cells(reference.storage());
        let mut difference = 0.0_f64;
        let mut power = 0.0_f64;
        for (a, b) in f32::cells(actual.storage()).iter().zip(expected) {
            difference +=
                (num_complex::Complex64::new(f64::from(a.re), f64::from(a.im)) - b).norm_sqr();
            power += b.norm_sqr();
        }
        let nrms = (difference / power).sqrt();
        eprintln!("iteration={iteration}, compact grid NRMS={nrms}");
        assert!(nrms < 1e-5, "compact accumulation NRMS={nrms}");
        assert_eq!(actual.sumwt(), reference.sumwt());
    }
}
