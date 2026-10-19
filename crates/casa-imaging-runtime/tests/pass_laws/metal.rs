// SPDX-License-Identifier: LGPL-3.0-or-later
//! The pass on Metal equals the pass on the CPU, both in `f32`, to the order
//! of the device's atomic additions (1e-4 of each image's peak, `sumwt`
//! included): plane owners and region tiles, waves, the fused residual of a
//! nearest-mapped cube and the native-channel residual of a linearly
//! interpolated one.

use casa_imaging_operator::{Basis, GridPrecision, ModeSet, PlaneRange, SpectralResampler};
use casa_imaging_runtime::pass::{BackendChoice, Partition, PassDomain, PassError, Residency};

use crate::fixture::{
    IMAGE, Rows, Run, TestKernels, nearest_cube, offset_linear_cube, operator, operator_with,
    planes, run, sparse_model, window_of, worst_relative,
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

/// #792 review (Fable 2): with a weight image (`ModeSet::ALL`), the Metal
/// pass equals the CPU's, the weight image included: on region tiles, where
/// the centre-row owner sums and grids it, and on plane owners in one-plane
/// waves, where each owner does. Both grid the summed groups after their
/// deferred device work (#787).
#[test]
fn metal_weight_images_equal_the_cpu_on_region_tiles_and_plane_owners() {
    if !available() {
        return;
    }
    let weighted = |basis| {
        operator_with(
            IMAGE,
            GridPrecision::F32,
            basis,
            |geometry, polarization| Box::new(TestKernels::new(geometry, polarization, true, None)),
        )
    };
    let constant = weighted(Basis::Constant);
    let direct = SpectralResampler::direct(Basis::Constant).expect("direct");
    let cube_resampler = nearest_cube();
    let cube = weighted(cube_resampler.basis());
    let mut rows = Rows::random(400, 59, constant.geometry());
    for (label, domain, residency) in [
        (
            "region tiles",
            PassDomain {
                operator: &constant,
                resampler: &direct,
                partition: Partition::regions(&constant, 3),
            },
            Residency::All,
        ),
        (
            "plane owners in waves",
            planes(&cube, &cube_resampler, 3),
            Residency::Waves { planes_per_wave: 1 },
        ),
    ] {
        let mut pass = |backend| {
            run(
                std::slice::from_ref(&domain),
                &mut rows,
                &Run {
                    workers: 3,
                    modes: ModeSet::ALL,
                    residency,
                    backend,
                    ..Run::initial()
                },
            )
        };
        let cpu = pass(BackendChoice::Cpu);
        let metal = pass(BackendChoice::Metal);
        assert!(
            cpu[0]
                .planes
                .iter()
                .flat_map(|plane| &plane.weight)
                .any(|image| image.iter().any(|value| *value != 0.0)),
            "{label}: a weight image"
        );
        let worst = worst_relative(&metal[0], &cpu[0]);
        assert!(worst <= TOLERANCE, "{label}: {worst}");
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
