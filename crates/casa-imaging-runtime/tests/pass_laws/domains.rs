// SPDX-License-Identifier: LGPL-3.0-or-later
//! Several image domains in one pass: every domain grids the residual of
//! the combined model, `V − Σ_d A_d·m_d` (CASA `SIMapperCollection::degrid`
//! sums every mapper's prediction before `grid` subtracts it).

use casa_imaging_operator::{
    Basis, GridPrecision, ModeSet, PlaneRange, PreparedModelGrids, SpectralResampler,
};
use casa_imaging_runtime::pass::{Partition, PassDomain, PassError, Residency};

use crate::fixture::{
    IMAGE, Rows, Run, WIDTH_HZ, normalised_peak, offset_linear_cube, operator, operator_of, planes,
    run, sparse_model, window_of,
};

/// The outlier field's image size, unlike the main field's.
const OUTLIER: usize = 48;
/// The rotation of the outlier's uv frame about the main field's.
const OUTLIER_ROTATION_RAD: f64 = 0.3;

/// A main field and an outlier of another size and phase centre, each with
/// its own source: data that are the sum of both predictions leave no
/// residual in either domain, while the main field's model alone leaves the
/// outlier's source in the main residual.
#[test]
fn outlier_residuals_vanish_only_for_the_combined_model() {
    let main = operator(GridPrecision::F64, Basis::Constant);
    let outlier = operator_of(OUTLIER, GridPrecision::F64, Basis::Constant);
    let resampler = SpectralResampler::direct(Basis::Constant).expect("direct");
    let mut rows = Rows::random(300, 59, outlier.geometry());
    rows.add_domain(OUTLIER_ROTATION_RAD, 61);
    let models = [
        window_of(&main, &sparse_model(IMAGE, 1, 67), PlaneRange::single(0)),
        window_of(
            &outlier,
            &sparse_model(OUTLIER, 1, 71),
            PlaneRange::single(0),
        ),
    ];
    let largest = f64::from(rows.predict(&[
        (0, &main, &resampler, &models[0]),
        (1, &outlier, &resampler, &models[1]),
    ]));
    assert!(largest > 0.0);
    let prepare = |domain: usize, _: PlaneRange| -> Result<PreparedModelGrids, PassError> {
        Ok(models[domain].clone())
    };
    let domains = [
        PassDomain {
            operator: &main,
            resampler: &resampler,
            partition: Partition::regions(&main, 2),
        },
        PassDomain {
            operator: &outlier,
            resampler: &resampler,
            partition: Partition::regions(&outlier, 2),
        },
    ];
    let residual = run(
        &domains,
        &mut rows,
        &Run {
            workers: 2,
            model: Some(&prepare),
            modes: ModeSet::DATA,
            ..Run::initial()
        },
    );
    for (domain, images) in residual.iter().enumerate() {
        let peak = normalised_peak(images);
        assert!(
            peak < 1.0e-5 * largest,
            "domain {domain}: residual peak {peak} of {largest}"
        );
    }
    let main_alone = run(
        &domains[..1],
        &mut rows,
        &Run {
            workers: 2,
            model: Some(&prepare),
            modes: ModeSet::DATA,
            ..Run::initial()
        },
    );
    assert!(
        normalised_peak(&main_alone[0]) > 1.0e-3 * largest,
        "the outlier's source stays in a residual without its model"
    );
}

/// Two linear-cube domains in waves: the combined residual of the predicted
/// models vanishes exactly in both, and with random data the waves equal one
/// resident pass.
#[test]
fn outlier_cube_waves_equal_one_resident_pass() {
    let resampler = offset_linear_cube();
    let main = operator(GridPrecision::F64, resampler.basis());
    let outlier = operator_of(OUTLIER, GridPrecision::F64, resampler.basis());
    let mut rows = Rows::random(200, 73, outlier.geometry());
    rows.add_domain(OUTLIER_ROTATION_RAD, 79);
    let models = [sparse_model(IMAGE, 3, 83), sparse_model(OUTLIER, 3, 89)];
    let operators = [&main, &outlier];
    let prepare = |domain: usize, planes: PlaneRange| -> Result<_, PassError> {
        Ok(window_of(operators[domain], &models[domain], planes))
    };
    let pass = |rows: &mut Rows, workers: usize, residency: Residency| {
        run(
            &[
                planes(&main, &resampler, workers),
                planes(&outlier, &resampler, workers),
            ],
            rows,
            &Run {
                residency,
                workers,
                model: Some(&prepare),
                modes: ModeSet::DATA,
                native_spacing_hz: WIDTH_HZ,
                ..Run::initial()
            },
        )
    };
    let reference = pass(&mut rows, 1, Residency::All);
    let waved = pass(&mut rows, 2, Residency::Waves { planes_per_wave: 1 });
    assert_eq!(waved, reference);
    let full = PlaneRange::new(0, 3);
    let prepared = [
        window_of(&main, &models[0], full),
        window_of(&outlier, &models[1], full),
    ];
    rows.predict(&[
        (0, &main, &resampler, &prepared[0]),
        (1, &outlier, &resampler, &prepared[1]),
    ]);
    for residency in [Residency::All, Residency::Waves { planes_per_wave: 1 }] {
        for (domain, images) in pass(&mut rows, 2, residency).iter().enumerate() {
            assert!(
                images
                    .planes
                    .iter()
                    .flat_map(|plane| &plane.data)
                    .all(|image| image.iter().all(|value| *value == 0.0)),
                "{residency:?}: domain {domain} keeps a residual"
            );
        }
    }
}
