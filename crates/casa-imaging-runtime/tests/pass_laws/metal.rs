// SPDX-License-Identifier: LGPL-3.0-or-later
//! The pass on Metal equals the pass on the CPU, both in `f32`, to the order
//! of the device's atomic additions (1e-4 of each image's peak, `sumwt`
//! included): plane owners and region tiles, waves, the fused residual of a
//! nearest-mapped cube and the native-channel residual of a linearly
//! interpolated one.

use casa_imaging_operator::{Basis, GridPrecision, ModeSet, PlaneRange, SpectralResampler};
use casa_imaging_runtime::pass::{BackendChoice, Partition, PassDomain, PassError, Residency};

use crate::fixture::{
    IMAGE, Rows, Run, nearest_cube, offset_linear_cube, operator, planes, run, sparse_model,
    window_of, worst_relative,
};

const TOLERANCE: f64 = 1.0e-4;

fn available() -> bool {
    let available = casa_imaging_metal::available();
    if !available {
        eprintln!("skipped: no Metal device");
    }
    available
}

#[test]
fn metal_cube_passes_equal_cpu_passes_in_waves_and_with_a_model() {
    if !available() {
        return;
    }
    for resampler in [nearest_cube(), offset_linear_cube()] {
        let basis = resampler.basis();
        let operator = operator(GridPrecision::F32, basis);
        let model = sparse_model(IMAGE, basis.planes() as usize, 41);
        let prepare = |_: usize, planes: PlaneRange| -> Result<_, PassError> {
            Ok(window_of(&operator, &model, planes))
        };
        let mut rows = Rows::random(300, 43, operator.geometry());
        for (modes, with_model) in [(ModeSet::DATA_PSF, false), (ModeSet::DATA, true)] {
            for (workers, residency) in [
                (2, Residency::All),
                (3, Residency::Waves { planes_per_wave: 1 }),
            ] {
                let mut pass = |backend| {
                    run(
                        &[planes(&operator, &resampler, workers)],
                        &mut rows,
                        &Run {
                            residency,
                            workers,
                            model: with_model.then_some(&prepare as &_),
                            modes,
                            backend,
                            ..Run::initial()
                        },
                    )
                };
                let cpu = pass(BackendChoice::Cpu);
                let metal = pass(BackendChoice::Metal);
                let worst = worst_relative(&metal[0], &cpu[0]);
                assert!(
                    worst <= TOLERANCE,
                    "{basis:?} {modes:?} {workers} workers {residency:?}: {worst}"
                );
            }
        }
    }
}

#[test]
fn metal_region_tiles_equal_the_cpu_for_the_initial_and_residual_passes() {
    if !available() {
        return;
    }
    let operator = operator(GridPrecision::F32, Basis::Constant);
    let resampler = SpectralResampler::direct(Basis::Constant).expect("direct");
    let model = sparse_model(IMAGE, 1, 47);
    let prepare = |_: usize, planes: PlaneRange| -> Result<_, PassError> {
        Ok(window_of(&operator, &model, planes))
    };
    let mut rows = Rows::random(500, 53, operator.geometry());
    for (modes, with_model) in [(ModeSet::DATA_PSF, false), (ModeSet::DATA, true)] {
        let mut pass = |backend| {
            run(
                &[PassDomain {
                    operator: &operator,
                    resampler: &resampler,
                    partition: Partition::regions(&operator, 3),
                }],
                &mut rows,
                &Run {
                    workers: 3,
                    model: with_model.then_some(&prepare as &_),
                    modes,
                    backend,
                    ..Run::initial()
                },
            )
        };
        let cpu = pass(BackendChoice::Cpu);
        let metal = pass(BackendChoice::Metal);
        let worst = worst_relative(&metal[0], &cpu[0]);
        assert!(worst <= TOLERANCE, "{modes:?}: {worst}");
    }
}
