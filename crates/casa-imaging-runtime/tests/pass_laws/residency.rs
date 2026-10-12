// SPDX-License-Identifier: LGPL-3.0-or-later
//! Residency: the planned waves fit the budget and equal one resident pass;
//! a policy below one plane is refused at admission; a writing pass hands
//! each row its own prediction; a writing pass in waves and rows wider
//! apart than the planned spacing are typed errors.

use casa_imaging_operator::{
    Basis, CpuBackend, GridPrecision, ModeSet, PlaneRange, PredictionScratch, SpectralAxis,
    SpectralKernel, SpectralResampler, WeightingGeneration,
};
use casa_imaging_runtime::pass::{
    BackendChoice, BoundedSource, Cancel, MajorCyclePass, NativeBlock, PassError, Residency,
    SourceError, VisibilitySink, WaveDemand, WorkerTeam, run_major_cycle,
};
use casa_imaging_runtime::{Admission, Demand, HostResources, ResourcePolicy, admit};
use num_complex::Complex32;

use crate::fixture::{
    IMAGE, Rows, Run, WIDTH_HZ, nearest_cube, offset_linear_cube, operator, planes, run,
    sparse_model, try_run, window_of, worst_relative,
};

/// A host whose memory never binds, so the policy's ceiling does.
const HOST: HostResources = HostResources {
    threads: 1,
    performance_cores: 1,
    available_memory: u64::MAX,
    metal: true,
};

/// A budget that fits exactly one plane gives one-plane waves whose images
/// equal one resident pass; a byte less is refused at admission. Every
/// plane fitting gives one resident wave. Nearest mapping, so no model halo
/// moves the boundary.
#[test]
fn a_one_plane_budget_gives_one_plane_waves_equal_to_the_resident_pass() {
    one_plane_budget(BackendChoice::Cpu, GridPrecision::F64);
}

/// The same law under the Metal demand (each owner's ring, the device copy
/// of the model); its waves equal the resident pass to the order of the
/// device's `f32` additions.
#[test]
fn a_one_plane_metal_budget_gives_one_plane_waves() {
    if !casa_imaging_metal::available() {
        eprintln!("skipped: no Metal device");
        return;
    }
    one_plane_budget(BackendChoice::Metal, GridPrecision::F32);
}

fn one_plane_budget(backend: BackendChoice, precision: GridPrecision) {
    let operator = operator(precision, Basis::ChannelLocal { planes: 3 });
    let resampler = nearest_cube();
    let model = sparse_model(IMAGE, 3, 7);
    let prepare = |_: usize, planes: PlaneRange| -> Result<_, PassError> {
        Ok(window_of(&operator, &model, planes))
    };
    let mut rows = Rows::random(200, 13, operator.geometry());
    for (modes, with_model) in [(ModeSet::DATA_PSF, false), (ModeSet::DATA, true)] {
        let domains = [planes(&operator, &resampler, 1)];
        let demand = WaveDemand {
            domains: &domains,
            modes,
            with_model,
            native_spacing_hz: WIDTH_HZ,
            workers: 1,
            backend,
            block: rows.maximum_block(),
        };
        let one = demand.bytes(1);
        assert_eq!(
            Residency::plan(&demand, one),
            Residency::Waves { planes_per_wave: 1 }
        );
        assert_eq!(Residency::plan(&demand, demand.bytes(3)), Residency::All);
        // A byte less than one plane still plans one-plane waves, and their
        // admission is refused: a policy that cannot hold one plane rejects
        // the pass before it reads anything.
        assert_eq!(
            Residency::plan(&demand, one - 1),
            Residency::Waves { planes_per_wave: 1 }
        );
        let ceiling = ResourcePolicy::Explicit {
            workers: 1,
            memory: one - 1,
        };
        assert_eq!(
            admit(
                &HOST,
                &ceiling,
                &Demand {
                    phase: "pass",
                    memory: one
                }
            )
            .expect_err("one plane does not fit"),
            Admission {
                phase: "pass",
                required: one,
                available: one - 1,
            }
        );
        let model = with_model.then_some(&prepare as &_);
        let resident = run(
            &domains,
            &mut rows,
            &Run {
                model,
                modes,
                backend,
                ..Run::initial()
            },
        );
        let waved = run(
            &domains,
            &mut rows,
            &Run {
                residency: Residency::plan(&demand, one),
                model,
                modes,
                backend,
                ..Run::initial()
            },
        );
        match backend {
            BackendChoice::Cpu => assert_eq!(waved, resident, "{modes:?}"),
            BackendChoice::Metal => {
                let worst = worst_relative(&waved[0], &resident[0]);
                assert!(worst <= 1.0e-4, "{modes:?}: {worst}");
            }
        }
    }
}

/// A pass that writes visibilities must hold every plane: each row is
/// written once with every output channel's prediction.
#[test]
fn a_writing_pass_in_waves_is_a_typed_error() {
    let operator = operator(GridPrecision::F64, Basis::ChannelLocal { planes: 3 });
    let resampler = nearest_cube();
    let mut rows = Rows::random(50, 17, operator.geometry());
    let weighting = WeightingGeneration::Natural { taper: None };
    let domains = [planes(&operator, &resampler, 1)];
    let pass = MajorCyclePass {
        domains: &domains,
        weighting: &weighting,
        modes: ModeSet::DATA_PSF,
        model: None,
        residency: Residency::Waves { planes_per_wave: 1 },
        native_spacing_hz: WIDTH_HZ,
        backend: BackendChoice::Cpu,
        metal_stage_timing: false,
    };
    let mut write = |_: &_, _: &[_]| -> Result<(), _> { panic!("nothing is written") };
    let mut sink = VisibilitySink {
        predictions: true,
        write: &mut write,
    };
    let result = run_major_cycle(
        &pass,
        &mut rows,
        &WorkerTeam::new(1).expect("team"),
        &Cancel::new(),
        &mut |_, _| panic!("no wave runs"),
        Some(&mut sink),
    );
    assert!(
        matches!(result, Err(PassError::VisibilityWriteWaves)),
        "{result:?}"
    );
}

/// A pass that writes visibilities hands its sink each row with the
/// prediction made from that row, although a pass otherwise takes a block's
/// rows grouped by baseline and the fixture's rows interleave five
/// baselines: every physical row arrives once, with its own prediction.
#[test]
fn a_writing_pass_hands_each_row_its_own_prediction() {
    let resampler = offset_linear_cube();
    let operator = operator(GridPrecision::F64, resampler.basis());
    let model = sparse_model(IMAGE, 3, 59);
    let prepare = |_: usize, planes: PlaneRange| -> Result<_, PassError> {
        Ok(window_of(&operator, &model, planes))
    };
    let mut rows = Rows::random(100, 61, operator.geometry());
    let whole = window_of(&operator, &model, PlaneRange::new(0, 3));
    let mut scratch = PredictionScratch::default();
    let expected = (0..rows.rows.len())
        .map(|index| {
            let mut predicted = vec![Complex32::default(); rows.rows[index].values.len()];
            resampler
                .predict_row(
                    &operator,
                    &mut CpuBackend::new(),
                    &whole,
                    &rows.native(0, index),
                    &mut scratch,
                    &mut predicted,
                )
                .expect("predict");
            predicted
        })
        .collect::<Vec<_>>();
    let weighting = WeightingGeneration::Natural { taper: None };
    for workers in [1, 3] {
        let domains = [planes(&operator, &resampler, workers)];
        let pass = MajorCyclePass {
            domains: &domains,
            weighting: &weighting,
            modes: ModeSet::DATA,
            model: Some(&prepare),
            residency: Residency::All,
            native_spacing_hz: WIDTH_HZ,
            backend: BackendChoice::Cpu,
            metal_stage_timing: false,
        };
        let mut written = vec![None; expected.len()];
        let mut write = |block: &NativeBlock, predictions: &[Complex32]| {
            let cells = block.channels() * block.correlations();
            for index in 0..block.len() {
                let row = block.header(index).address.physical_row as usize;
                assert!(written[row].is_none(), "row {row} is written once");
                written[row] = Some(predictions[index * cells..(index + 1) * cells].to_vec());
            }
            Ok::<_, SourceError>(())
        };
        let mut sink = VisibilitySink {
            predictions: true,
            write: &mut write,
        };
        run_major_cycle(
            &pass,
            &mut rows,
            &WorkerTeam::new(workers).expect("team"),
            &Cancel::new(),
            &mut |_, _| Ok(()),
            Some(&mut sink),
        )
        .expect("pass");
        for (row, (written, expected)) in written.iter().zip(&expected).enumerate() {
            assert_eq!(
                written.as_ref(),
                Some(expected),
                "{workers} workers, row {row}"
            );
        }
    }
}

/// A wave's model halo is sized from the planned native spacing; a row
/// whose natives lie further apart is refused rather than predicted from a
/// model window that misses planes.
#[test]
fn rows_wider_apart_than_the_planned_spacing_are_refused_in_waves() {
    let resampler = SpectralResampler::channel_local(
        SpectralAxis::new(1.003e9, 1.0e6, 10).expect("axis"),
        SpectralKernel::Linear,
    );
    let operator = operator(GridPrecision::F64, resampler.basis());
    let model = sparse_model(IMAGE, 10, 47);
    let prepare = |_: usize, planes: PlaneRange| -> Result<_, PassError> {
        Ok(window_of(&operator, &model, planes))
    };
    let mut rows = Rows::at(20, 53, operator.geometry(), vec![1.000e9, 1.010e9, 1.020e9]);
    let result = try_run(
        &[planes(&operator, &resampler, 1)],
        &mut rows,
        &Run {
            residency: Residency::Waves { planes_per_wave: 2 },
            model: Some(&prepare),
            modes: ModeSet::DATA,
            native_spacing_hz: 4.0e6,
            ..Run::initial()
        },
    );
    assert!(
        matches!(
            result,
            Err(PassError::NativeSpacing { observed_hz, bound_hz })
                if observed_hz == 10.0e6 && bound_hz == 4.0e6
        ),
        "{:?}",
        result.map(|_| ())
    );
}
