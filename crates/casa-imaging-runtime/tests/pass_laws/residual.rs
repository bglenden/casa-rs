// SPDX-License-Identifier: LGPL-3.0-or-later
//! Residual passes: data that are a model's prediction leave nothing, in one
//! resident pass and in waves, and waves equal one resident pass.

use casa_imaging_operator::{
    Basis, GridPrecision, ModeSet, ModelImages, NormalImages, PlaneRange, SpectralAxis,
    SpectralKernel, SpectralResampler,
};
use casa_imaging_runtime::pass::{Partition, PassDomain, PassError, Residency};

use crate::fixture::{
    IMAGE, Rows, Run, WIDTH_HZ, normalised_peak, offset_linear_cube, operator, planes, run,
    sparse_model, window_of,
};

/// Whether every data plane of `images` is exactly zero.
fn vanishes(images: &NormalImages) -> bool {
    images
        .planes
        .iter()
        .flat_map(|plane| &plane.data)
        .all(|image| image.iter().all(|value| *value == 0.0))
}

/// Data that are exactly the prediction of a model leave no residual:
/// predict the model at every native sample, then run a residual pass
/// with the same model.
#[test]
fn residual_pass_of_the_predicted_model_vanishes() {
    let operator = operator(GridPrecision::F64, Basis::Constant);
    let resampler = SpectralResampler::direct(Basis::Constant).expect("direct");
    let mut rows = Rows::random(300, 11, operator.geometry());
    let model = window_of(
        &operator,
        &sparse_model(IMAGE, 1, 19),
        PlaneRange::single(0),
    );
    let largest = f64::from(rows.predict(&[(0, &operator, &resampler, &model)]));
    let prepare = |domain: usize, planes: PlaneRange| -> Result<_, PassError> {
        assert_eq!((domain, planes), (0, PlaneRange::single(0)));
        Ok(model.clone())
    };
    let residual = run(
        &[PassDomain {
            operator: &operator,
            resampler: &resampler,
            partition: Partition::regions(&operator, 2),
        }],
        &mut rows,
        &Run {
            workers: 2,
            model: Some(&prepare),
            modes: ModeSet::DATA,
            ..Run::initial()
        },
    );
    let peak = normalised_peak(&residual[0]);
    assert!(largest > 0.0);
    assert!(peak < 1.0e-5 * largest, "residual peak {peak} of {largest}");
}

/// Under the native spacing `spacing_hz` that sizes the waves' model halo:
/// residual passes of `model` over `rows`' random data in waves equal one
/// resident pass bit for bit; then, with `rows`' data replaced by `model`'s
/// prediction, residual passes vanish exactly, resident and in waves.
fn residual_vanishes_in_waves(
    resampler: &SpectralResampler,
    model: &ModelImages,
    mut rows: Rows,
    spacing_hz: f64,
) {
    let planes_total = resampler.basis().planes();
    let operator = operator(GridPrecision::F64, resampler.basis());
    let prepare = |_: usize, planes: PlaneRange| -> Result<_, PassError> {
        Ok(window_of(&operator, model, planes))
    };
    let residual_pass = |rows: &mut Rows, workers: usize, residency: Residency| {
        run(
            &[planes(&operator, resampler, workers)],
            rows,
            &Run {
                residency,
                workers,
                model: Some(&prepare),
                modes: ModeSet::DATA,
                native_spacing_hz: spacing_hz,
                ..Run::initial()
            },
        )
    };
    let reference = residual_pass(&mut rows, 1, Residency::All);
    assert!(!vanishes(&reference[0]), "random data leave a residual");
    for (workers, residency) in [
        (2, Residency::Waves { planes_per_wave: 1 }),
        (3, Residency::Waves { planes_per_wave: 2 }),
    ] {
        let images = residual_pass(&mut rows, workers, residency);
        assert_eq!(images, reference, "{workers} workers, {residency:?}");
    }
    let full = window_of(&operator, model, PlaneRange::new(0, planes_total));
    assert!(rows.predict(&[(0, &operator, resampler, &full)]) > 0.0);
    let dirty = run(
        &[planes(&operator, resampler, 1)],
        &mut rows,
        &Run {
            modes: ModeSet::DATA,
            native_spacing_hz: spacing_hz,
            ..Run::initial()
        },
    );
    assert!(!vanishes(&dirty[0]), "the predicted data image something");
    for (workers, residency) in [
        (1, Residency::All),
        (2, Residency::Waves { planes_per_wave: 1 }),
        (3, Residency::Waves { planes_per_wave: 2 }),
    ] {
        rows.restricted.clear();
        let residual = residual_pass(&mut rows, workers, residency);
        assert!(vanishes(&residual[0]), "{residency:?} keeps a residual");
        assert!(
            rows.restricted.iter().all(|restricted| !restricted),
            "native-channel predictions read whole rows"
        );
    }
}

/// CASA forms a linear cube's residual at native channels
/// (`interpolateFrequencyFromgrid`, `SIMapperCollection::grid`): data that
/// are the model's native-channel prediction leave exactly nothing, even
/// where output samples fall between native channels, in one resident pass
/// and in waves whose model windows carry the model halo.
#[test]
fn linear_cube_residual_of_the_predicted_model_vanishes_at_native_channels() {
    let resampler = offset_linear_cube();
    assert!(resampler.forms_native_residuals());
    let operator = operator(GridPrecision::F64, resampler.basis());
    residual_vanishes_in_waves(
        &resampler,
        &sparse_model(IMAGE, 3, 23),
        Rows::random(300, 29, operator.geometry()),
        WIDTH_HZ,
    );
}

/// Output channels ten times narrower than native ones: the natives a wave's
/// samples interpolate are predicted from output channels far outside the
/// wave, and a native in `matchChannel`'s halo is predicted from channel 0
/// (`getInterpolateArrays`), so the wave's model halo must reach them. Ten
/// 1 MHz channels at 1003 … 1012 MHz over natives at 1000, 1010 and
/// 1020 MHz.
#[test]
fn output_channels_narrower_than_native_ones_keep_waves_exact() {
    let resampler = SpectralResampler::channel_local(
        SpectralAxis::new(1.003e9, 1.0e6, 10).expect("axis"),
        SpectralKernel::Linear,
    );
    let operator = operator(GridPrecision::F64, resampler.basis());
    let rows = Rows::at(
        200,
        41,
        operator.geometry(),
        vec![1.000e9, 1.010e9, 1.020e9],
    );
    // Every plane carries flux, so a missing model plane shows.
    let mut model = sparse_model(IMAGE, 10, 43);
    for plane in &mut model.planes {
        plane.images[0][(IMAGE / 2, IMAGE / 2)] = 1.0;
    }
    residual_vanishes_in_waves(&resampler, &model, rows, 10.0e6);
}
