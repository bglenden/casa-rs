// SPDX-License-Identifier: LGPL-3.0-or-later
//! Residency: the planned waves fit the budget and equal one resident pass;
//! a budget below one plane, a writing pass in waves and rows wider apart
//! than the planned spacing are typed errors.

use casa_imaging_operator::{
    Basis, GridPrecision, ModeSet, PlaneRange, SpectralAxis, SpectralKernel, SpectralResampler,
    WeightingGeneration,
};
use casa_imaging_runtime::pass::{
    Cancel, MajorCyclePass, PassError, Residency, VisibilitySink, WaveDemand, WorkerTeam,
    run_major_cycle,
};

use crate::fixture::{
    IMAGE, Rows, Run, WIDTH_HZ, nearest_cube, operator, planes, run, sparse_model, try_run,
    window_of,
};

/// A budget that fits exactly one plane gives one-plane waves whose images
/// equal one resident pass; a byte less is the typed memory error. Every
/// plane fitting gives one resident wave. Nearest mapping, so no model halo
/// moves the boundary.
#[test]
fn a_one_plane_budget_gives_one_plane_waves_equal_to_the_resident_pass() {
    let operator = operator(GridPrecision::F64, Basis::ChannelLocal { planes: 3 });
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
        };
        let one = demand.bytes(1);
        assert_eq!(
            Residency::plan(&demand, one).expect("one plane fits"),
            Residency::Waves { planes_per_wave: 1 }
        );
        assert!(matches!(
            Residency::plan(&demand, one - 1),
            Err(PassError::Memory { required, available }) if required == one && available == one - 1
        ));
        assert_eq!(
            Residency::plan(&demand, demand.bytes(3)).expect("every plane fits"),
            Residency::All
        );
        let model = with_model.then_some(&prepare as &_);
        let resident = run(
            &domains,
            &mut rows,
            &Run {
                model,
                modes,
                ..Run::initial()
            },
        );
        let waved = run(
            &domains,
            &mut rows,
            &Run {
                residency: Residency::plan(&demand, one).expect("one plane fits"),
                model,
                modes,
                ..Run::initial()
            },
        );
        assert_eq!(waved, resident, "{modes:?}");
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
