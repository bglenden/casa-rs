// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{
    path::{Path, PathBuf},
    sync::Mutex,
};

use casa_coordinates::{
    CoordinateModel, CoordinateSystem, DirectionCoordinate, Projection, ProjectionType, StokesType,
};
use casa_images::PagedImage;
use casa_imaging_application::{
    ApplicationDispatchError, Cancel, CleanStop, HostResources, ImagingOutcome, ImagingRequest,
    PrepareError, ResourcePolicy, RunContext,
};
use casa_imaging_model::{
    ImageDomainRole, ProductBeamRule, ProductRole, ProductTerm, ProductUnit, ProductValidityRule,
    WeightDensityScope,
};
use casa_ms::{
    MeasurementSet, MeasurementSetBuilder, OptionalMainColumn, SubtableId, VisibilityDataColumn,
    column_def::{ColumnDef, ColumnKind},
    schema,
};
use casa_types::{
    ArrayValue, Complex32, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
};
use ndarray::ArrayD;
use serde_json::json;

const PRODUCT_SUFFIXES: [&str; 6] = [".psf", ".residual", ".model", ".image", ".sumwt", ".mask"];
const DIRTY_PRODUCT_SUFFIXES: [&str; 5] = [".psf", ".residual", ".model", ".image", ".sumwt"];

fn assert_real_agreement<T: Copy + Into<f64>>(expected: &[T], actual: &[T]) {
    assert_eq!(expected.len(), actual.len());
    let scale = expected
        .iter()
        .map(|&value| value.into().powi(2))
        .sum::<f64>()
        .sqrt();
    let error = expected
        .iter()
        .zip(actual)
        .map(|(&a, &b)| (a.into() - b.into()).powi(2))
        .sum::<f64>()
        .sqrt();
    assert!(
        error <= (1e-3 * scale).max(1e-12),
        "error={error:e}, scale={scale:e}"
    );
}

fn assert_complex_agreement(
    expected: &[num_complex::Complex64],
    actual: &[num_complex::Complex64],
) {
    assert_eq!(expected.len(), actual.len());
    let scale = expected
        .iter()
        .map(|value| value.norm_sqr())
        .sum::<f64>()
        .sqrt();
    let error = expected
        .iter()
        .zip(actual)
        .map(|(a, b)| (*a - *b).norm_sqr())
        .sum::<f64>()
        .sqrt();
    assert!(
        error <= (1e-3 * scale).max(1e-12),
        "error={error:e}, scale={scale:e}"
    );
}

fn assert_model_agreement(
    expected: &[casa_imaging_model::ModelSample],
    actual: &[casa_imaging_model::ModelSample],
) {
    assert_eq!(expected.len(), actual.len());
    for (expected, actual) in expected.iter().zip(actual) {
        assert_eq!(expected.support(), actual.support());
    }
    assert_real_agreement(
        &expected
            .iter()
            .map(|value| value.value().value())
            .collect::<Vec<_>>(),
        &actual
            .iter()
            .map(|value| value.value().value())
            .collect::<Vec<_>>(),
    );
}

fn fixture_model_samples(
    model: &casa_imaging_reconstruction::ModelGeneration,
) -> Vec<casa_imaging_model::ModelSample> {
    let mut samples = Vec::with_capacity(model.sample_count());
    for domain in 0..model.shape().domains().len() {
        for coefficient in 0..model.shape().coefficients() {
            for polarization in 0..model.shape().polarizations() {
                samples.extend_from_slice(
                    &model
                        .read_plane(domain, coefficient, polarization)
                        .expect("fixture model plane"),
                );
            }
        }
    }
    samples
}

static EXECUTION_LOCK: Mutex<()> = Mutex::new(());

#[path = "common/imaging.rs"]
mod imaging;

#[path = "common/continuum_fixture.rs"]
mod continuum_fixture;
use continuum_fixture::*;

#[path = "continuum_application/clean_cycles.rs"]
mod clean_cycles;

#[path = "continuum_application/visibility_writes.rs"]
mod visibility_writes;

#[path = "continuum_application/t53_spectral_joins.rs"]
mod t53_spectral_joins;

#[path = "continuum_application/domains_and_waves.rs"]
mod domains_and_waves;

#[path = "continuum_application/metal.rs"]
mod metal;

#[path = "continuum_application/t55_cube_pipeline.rs"]
mod t55_cube_pipeline;

#[path = "continuum_application/t55_real_cube.rs"]
mod t55_real_cube;

#[path = "continuum_application/t55_c_array_turnaround.rs"]
mod t55_c_array_turnaround;

#[path = "continuum_application/t55_mfs_pilot.rs"]
mod t55_mfs_pilot;

#[test]
fn unsupported_primary_beam_frequency_rejects_before_any_phase() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    // This existing fixture labels 44 GHz as EVLA; the selected common EVLA
    // model represents only L/S/C, unlike the separate legacy-VLA Q model.
    let measurement_set = vla_aw_measurement_set(root.path());
    let image_name = root.path().join("unsupported-beam");
    let imaging = request(
        &measurement_set,
        &image_name,
        json!({ "niter": 0, "write_pb": true }),
    );
    let error = execute(&imaging).err().expect("unsupported beam frequency");
    let ApplicationDispatchError::Native(error) = error else {
        panic!("expected native coverage validation, found {error:?}");
    };
    let Some(casa_imaging_products::ProductsError::UnsupportedPrimaryBeamFrequency {
        model: casa_imaging_products::AnalyticPrimaryBeamModel::CasaEvlaCommon,
        output_channel: 0,
        frequency_hz,
    }) = error.downcast_ref::<casa_imaging_products::ProductsError>()
    else {
        panic!("expected explicit coverage error, found {error:?}");
    };
    // The default continuum axis is LSRK: validate the transformed output
    // frequency, not the source's exactly 44-GHz TOPO channel.
    assert_ne!(*frequency_hz, 44.0e9);
    assert!((*frequency_hz - 44.0e9).abs() < 2.0e6);
    // No phase ran: nothing but the input is in the output directory.
    let entries = std::fs::read_dir(root.path())
        .expect("output directory")
        .map(|entry| entry.expect("entry").file_name())
        .collect::<Vec<_>>();
    assert_eq!(entries, ["vla-aw-input.ms"], "no pass or product may run");
}

#[test]
fn image_pointing_center_preserves_casa_positive_pi_longitude() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let image_name = root.path().join("antimeridian");
    let imaging = request(
        &tiny_measurement_set(root.path()),
        &image_name,
        json!({ "niter": 0, "phasecenter": "J2000 12h00m00s +34d04m43.5s" }),
    );
    execute(&imaging).unwrap_or_else(|error| panic!("dirty image: {error}"));
    let image = PagedImage::<f32>::open(root.path().join("antimeridian.image")).expect("image");
    assert_eq!(
        image.coordinates().obs_info().pointing_center_rad[0],
        std::f64::consts::PI
    );
}

#[test]
fn image_observation_metadata_accepts_matching_labels_across_observations() {
    let _execution_guard = EXECUTION_LOCK.lock().unwrap();
    for (second_telescope, second_observer, accepted) in [
        ("EVLA", "casa-rs-test", true),
        ("VLA", "casa-rs-test", false),
        ("EVLA", "another-observer", false),
    ] {
        let root = tempfile::tempdir().unwrap();
        let path = four_spw_vla_measurement_set(root.path());
        let mut ms = MeasurementSet::open(&path).unwrap();
        ms.subtable_mut(SubtableId::Observation)
            .unwrap()
            .add_row(required_row(
                schema::observation::REQUIRED_COLUMNS,
                &[
                    ("TELESCOPE_NAME", string(second_telescope)),
                    ("OBSERVER", string(second_observer)),
                ],
            ))
            .unwrap();
        // Selected DDID 0 rows include both observations, with distinct times.
        for row in 12..ms.row_count() {
            ms.main_table_mut()
                .row_accessor_mut()
                .set_cell(row, "OBSERVATION_ID", int(1))
                .unwrap();
        }
        ms.save().unwrap();
        drop(ms);
        let prefix = root.path().join("joint-observation");
        let result = execute(&request(&path, &prefix, json!({ "niter": 0 })));
        if accepted {
            let result = result.unwrap_or_else(|error| panic!("joint observation: {error}"));
            assert_dirty_products(&prefix, &result.product_names());
            let image =
                PagedImage::<f32>::open(root.path().join("joint-observation.image")).unwrap();
            assert_eq!(image.coordinates().obs_info().telescope, "EVLA");
            assert_eq!(image.coordinates().obs_info().observer, "casa-rs-test");
        } else {
            let error = result
                .err()
                .expect("conflicting image metadata must reject");
            assert!(
                matches!(
                    error,
                    ApplicationDispatchError::Preparation(PrepareError::ObservationLabels { .. })
                ),
                "{error}"
            );
            assert!(!root.path().join("joint-observation.image").exists());
        }
    }
}

#[test]
fn a_selection_of_no_rows_is_refused() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let imaging = request(
        &tiny_measurement_set(root.path()),
        &root.path().join("no-rows"),
        json!({ "niter": 0, "uvrange": ">1000000km" }),
    );
    let error = execute(&imaging).err().expect("an empty selection");
    assert!(
        matches!(
            error,
            ApplicationDispatchError::Preparation(PrepareError::NoSelectedRows)
        ),
        "{error}"
    );
}

/// A λ or intent selection that leaves data descriptions without a row
/// drops them with their spectral windows, and images exactly what an
/// explicit selection of the remaining windows images. Spectral windows
/// 0 to 3 sit at 0.01, 0.02, 0.04 and 0.08 m, so every 100 m baseline is
/// 10,000, 5,000, 2,500 and 1,250 λ long; the target intent holds the rows
/// of data descriptions 0 and 1 (DATA_DESC_ID is the row number modulo
/// 4), the phase calibrator those of 2 and 3.
#[test]
fn selections_that_empty_data_descriptions_image_the_remaining_rows() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let path = four_spw_vla_measurement_set(root.path());
    let mut ms = MeasurementSet::open(&path).unwrap();
    for (spw, wavelength_m) in [0.01, 0.02, 0.04, 0.08].into_iter().enumerate() {
        let frequency_hz = 299_792_458.0 / wavelength_m;
        let channels = (0..8)
            .map(|channel| frequency_hz + f64::from(channel) * 1.0e6)
            .collect();
        let spectral_window = ms.subtable_mut(SubtableId::SpectralWindow).unwrap();
        spectral_window
            .row_accessor_mut()
            .set_cell(spw, "REF_FREQUENCY", float(frequency_hz))
            .unwrap();
        spectral_window
            .row_accessor_mut()
            .set_cell(
                spw,
                "CHAN_FREQ",
                Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(vec![8], channels).unwrap(),
                )),
            )
            .unwrap();
    }
    for mode in ["OBSERVE_TARGET#ON_SOURCE", "CALIBRATE_PHASE#ON_SOURCE"] {
        ms.subtable_mut(SubtableId::State)
            .unwrap()
            .add_row(required_row(
                schema::state::REQUIRED_COLUMNS,
                &[("OBS_MODE", string(mode))],
            ))
            .unwrap();
    }
    for row in 0..ms.row_count() {
        let angle = row as f64 * 0.25;
        let uvw = vec![100.0 * angle.cos(), 100.0 * angle.sin(), 0.0];
        ms.main_table_mut()
            .row_accessor_mut()
            .set_cell(
                row,
                "UVW",
                Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(vec![3], uvw).unwrap(),
                )),
            )
            .unwrap();
        ms.main_table_mut()
            .row_accessor_mut()
            .set_cell(row, "STATE_ID", int(i32::from(row % 4 >= 2)))
            .unwrap();
    }
    ms.save().unwrap();
    drop(ms);

    let image = |name: &str, selection: serde_json::Value| {
        let image_name = root.path().join(name);
        let mut overrides = json!({ "niter": 0, "ddid": null });
        overrides
            .as_object_mut()
            .unwrap()
            .extend(selection.as_object().unwrap().clone());
        execute(&request(&path, &image_name, overrides)).map(|outcome| (image_name, outcome))
    };
    let plane = |image_name: &Path, suffix: &str| {
        product_plane(image_name, suffix)
            .iter()
            .copied()
            .collect::<Vec<_>>()
    };
    let (windows, _) = image("windows", json!({ "spw": "0,1" })).expect("windows 0 and 1");
    for (name, selection) in [
        ("lambda", json!({ "uvrange": ">4000lambda" })),
        ("intent", json!({ "intent": "OBSERVE_TARGET*" })),
    ] {
        let (image_name, outcome) =
            image(name, selection).unwrap_or_else(|error| panic!("{name}: {error}"));
        let selected = outcome.problem.observation().sources()[0].selection();
        assert_eq!(selected.rows().selected_row_count(), 12, "{name}");
        assert_eq!(
            selected
                .data_descriptions()
                .iter()
                .map(|description| (
                    description.data_description_id(),
                    description.spectral_window_id()
                ))
                .collect::<Vec<_>>(),
            [(0, 0), (1, 1)],
            "{name}"
        );
        for suffix in [".psf", ".residual", ".sumwt"] {
            assert_real_agreement(&plane(&windows, suffix), &plane(&image_name, suffix));
        }
    }

    // Each selector alone selects rows; together they select none.
    let error = image(
        "none",
        json!({ "uvrange": ">4000lambda", "intent": "CALIBRATE_PHASE*" }),
    )
    .err()
    .expect("an empty selection");
    assert!(
        matches!(
            error,
            ApplicationDispatchError::Preparation(PrepareError::NoSelectedRows)
        ),
        "{error}"
    );
}

#[test]
fn outlier_domains_naming_one_output_are_refused() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let outlier = root.path().join("outlier");
    let outlier_file = root.path().join("twice.outlier");
    let record = format!(
        "imagename={}\nphasecenter=J2000 1.001rad 0.499rad\n",
        outlier.display()
    );
    std::fs::write(&outlier_file, record.repeat(2)).expect("write the outlier file");
    let mut imaging = request(
        &tiny_measurement_set(root.path()),
        &root.path().join("main"),
        json!({ "niter": 0 }),
    );
    imaging.outlierfile = Some(outlier_file);
    let error = execute(&imaging).err().expect("two domains, one output");
    let ApplicationDispatchError::Preparation(PrepareError::DuplicateOutput { path }) = error
    else {
        panic!("expected a duplicate output, found {error}");
    };
    assert_eq!(path, outlier);
}

fn assert_standard_products(image_name: &Path, product_names: &[String]) {
    assert_products(image_name, product_names, &PRODUCT_SUFFIXES);
}

fn assert_dirty_products(image_name: &Path, product_names: &[String]) {
    assert_products(image_name, product_names, &DIRTY_PRODUCT_SUFFIXES);
}

fn assert_products(image_name: &Path, product_names: &[String], suffixes: &[&str]) {
    let expected = suffixes
        .iter()
        .map(|suffix| (*suffix).to_string())
        .collect::<Vec<_>>();
    assert_eq!(product_names, expected);
    for suffix in suffixes {
        let path = PathBuf::from(format!("{}{}", image_name.display(), suffix));
        assert!(
            path.is_dir(),
            "missing CASA product directory {}",
            path.display()
        );
        if matches!(*suffix, ".psf" | ".psf.tt0") {
            assert_unit_psf_planes(&path);
        }
    }
}

fn assert_unit_psf_planes(path: &Path) {
    let product = PagedImage::<f32>::open(path).expect("open principal PSF");
    let shape = product.shape();
    for channel in 0..shape[3] {
        for polarization in 0..shape[2] {
            let plane = product
                .get_slice(&[0, 0, polarization, channel], &[shape[0], shape[1], 1, 1])
                .expect("read PSF plane");
            assert!(plane.iter().all(|value| value.is_finite()));
            if plane.iter().any(|value| *value != 0.0) {
                assert_eq!(
                    plane.iter().copied().fold(f32::NEG_INFINITY, f32::max),
                    1.0,
                    "{} polarization {polarization} channel {channel}",
                    path.display()
                );
            }
        }
    }
}

fn product_plane(image_name: &Path, suffix: &str) -> ArrayD<f32> {
    product_plane_with_size(image_name, suffix, if suffix == ".sumwt" { 1 } else { 16 })
}

fn product_plane_with_size(image_name: &Path, suffix: &str, image_size: usize) -> ArrayD<f32> {
    PagedImage::<f32>::open(PathBuf::from(format!("{}{}", image_name.display(), suffix)))
        .expect("open application product")
        .get_slice(&[0, 0, 0, 0], &[image_size, image_size, 1, 1])
        .expect("read application product plane")
}

fn assert_model_residual_respect_mask(image_name: &Path, expected_mask_pixels: usize) {
    let mask = product_plane(image_name, ".mask");
    let model = product_plane(image_name, ".model");
    let residual = product_plane(image_name, ".residual");
    assert_eq!(
        mask.iter().filter(|value| **value != 0.0).count(),
        expected_mask_pixels
    );
    assert!(
        model
            .iter()
            .zip(mask.iter())
            .all(|(model, mask)| *mask != 0.0 || *model == 0.0),
        "the model must remain zero outside the reconstruction mask"
    );
    assert!(residual.iter().all(|value| value.is_finite()));
}

#[test]
fn application_executes_single_ddid_stokes_i_mfs_dirty_and_publishes_products() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = tiny_measurement_set(root.path());
    let image_name = root.path().join("dirty");

    let result = execute(&request(
        &measurement_set,
        &image_name,
        json!({ "niter": 0 }),
    ))
    .expect("native dirty application execution");

    assert_eq!(result.total_minor_iterations, 0);
    assert_eq!(result.stop, None);
    assert_dirty_products(&image_name, &result.product_names());
    for suffix in [".residual", ".image"] {
        let product =
            PagedImage::<f32>::open(PathBuf::from(format!("{}{}", image_name.display(), suffix)))
                .expect("reopen validity-bearing product");
        assert_eq!(product.default_mask_name(), None);
        assert_eq!(
            product.units(),
            if suffix == ".residual" { "" } else { "Jy/beam" },
            "CASA {suffix} units"
        );
    }
    assert_eq!(
        PagedImage::<f32>::open(PathBuf::from(format!("{}.psf", image_name.display())))
            .expect("reopen ordinary PSF")
            .units(),
        "",
        "CASA PSF has an empty serialized unit label"
    );
    assert_eq!(
        PagedImage::<f32>::open(PathBuf::from(format!("{}.model", image_name.display())))
            .expect("reopen ordinary model")
            .units(),
        "Jy/pixel",
        "ordinary model units remain unchanged"
    );
    assert!(!PathBuf::from(format!("{}.mask", image_name.display())).exists());
}

#[test]
fn t51_taylor_publication_persists_casa_metadata_without_changing_logical_contract() {
    std::thread::Builder::new()
        .name("t51-taylor-metadata".to_string())
        .stack_size(8 * 1024 * 1024)
        .spawn(t51_taylor_publication_persists_casa_metadata_without_changing_logical_contract_impl)
        .expect("spawn Taylor metadata test")
        .join()
        .expect("Taylor metadata test thread");
}

fn t51_taylor_publication_persists_casa_metadata_without_changing_logical_contract_impl() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = alma_primary_beam_measurement_set(root.path());
    let image_name = root.path().join("taylor-metadata");
    let imaging = request(
        &measurement_set,
        &image_name,
        json!({
            "deconvolver": "mtmfs",
            "nterms": 2,
            "ddid": null,
            "imsize": 8,
            "niter": 0,
            "write_pb": true,
        }),
    );

    let result = execute(&imaging).expect("native Taylor metadata execution");
    let planned = &result.planned_products;
    let published = &result.products;
    assert_eq!(planned.members().len(), published.members().len());
    for (planned, published) in planned.members().iter().zip(published.members()) {
        assert_eq!(
            planned.node(),
            published.node(),
            "{} identity",
            planned.name()
        );
        assert_eq!(planned.name(), published.name());
    }

    let open = |suffix: &str| {
        PagedImage::<f32>::open(PathBuf::from(format!("{}{suffix}", image_name.display())))
            .expect("reopen Taylor publication")
    };
    for suffix in [
        ".psf.tt0",
        ".psf.tt1",
        ".psf.tt2",
        ".residual.tt0",
        ".residual.tt1",
        ".pb.tt0",
        ".pb.tt1",
        ".alpha",
        ".alpha.error",
    ] {
        assert_eq!(open(suffix).units(), "", "{suffix} CASA unit");
    }
    for suffix in [".model.tt0", ".model.tt1"] {
        assert_eq!(open(suffix).units(), "Jy/pixel", "{suffix} CASA unit");
    }
    for suffix in [".image.tt0", ".image.tt1"] {
        assert_eq!(open(suffix).units(), "Jy/beam", "{suffix} CASA unit");
    }
    for suffix in [
        ".image.tt0",
        ".image.tt1",
        ".residual.tt0",
        ".residual.tt1",
        ".pb.tt0",
        ".alpha",
        ".alpha.error",
    ] {
        assert_eq!(
            open(suffix).default_mask_name().as_deref(),
            Some("mask0"),
            "{suffix} explicit CASA mask"
        );
    }
    {
        let suffix = ".pb.tt0";
        let product = open(suffix);
        let shape = product.shape().to_vec();
        let valid = product
            .get_mask_slice(&vec![0; shape.len()], &shape, &vec![1; shape.len()])
            .expect("primary-beam mask")
            .expect("explicit primary-beam mask");
        assert!(
            valid.iter().all(|valid| *valid),
            "{suffix} all-valid primary-beam mask remains explicit"
        );
    }
    for suffix in [
        ".psf.tt0",
        ".psf.tt1",
        ".psf.tt2",
        ".model.tt0",
        ".model.tt1",
        ".sumwt.tt0",
        ".sumwt.tt1",
        ".sumwt.tt2",
    ] {
        assert_eq!(
            open(suffix).default_mask_name(),
            None,
            "{suffix} remains implicitly all-valid"
        );
    }
    for suffix in [".residual.tt0", ".residual.tt1"] {
        assert!(
            open(suffix)
                .image_info()
                .expect("residual ImageInfo")
                .beam_set
                .is_empty(),
            "{suffix} Taylor residual does not persist a beam"
        );
    }

    let psf = planned
        .members()
        .iter()
        .find(|member| member.role() == ProductRole::Psf(ProductTerm::Taylor(0)))
        .expect("Taylor PSF contract");
    assert_eq!(psf.unit(), ProductUnit::JyPerBeam);
    assert_eq!(psf.beam_rule(), ProductBeamRule::Fitted);
    assert_eq!(psf.validity(), ProductValidityRule::All);
    let residual = planned
        .members()
        .iter()
        .find(|member| member.role() == ProductRole::Residual(ProductTerm::Taylor(0)))
        .expect("Taylor residual contract");
    assert_eq!(residual.unit(), ProductUnit::JyPerBeam);
    assert_eq!(residual.beam_rule(), ProductBeamRule::Fitted);
    assert_eq!(residual.validity(), ProductValidityRule::FinalNormalState);
    assert!(matches!(
        residual.storage().pixel_mask(),
        casa_imaging_model::ProductPixelMask::Explicit(ProductValidityRule::PrimaryBeam(_))
    ));
    let primary_beam = planned
        .members()
        .iter()
        .find(|member| member.role() == ProductRole::PrimaryBeam(ProductTerm::Taylor(0)))
        .expect("Taylor primary-beam contract");
    assert_eq!(primary_beam.unit(), ProductUnit::Dimensionless);
    assert_eq!(primary_beam.beam_rule(), ProductBeamRule::None);
    assert!(matches!(
        primary_beam.validity(),
        ProductValidityRule::PrimaryBeam(_)
    ));
}

#[test]
fn stokes_i_uses_one_shared_imaging_weight_for_each_linear_parallel_hand() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");

    let run = |name: &str, weights: [f32; 2]| {
        let measurement_set =
            unequal_linear_parallel_hand_measurement_set(root.path(), name, weights);
        let image_name = root.path().join(format!("{name}-dirty"));
        execute(&request(
            &measurement_set,
            &image_name,
            json!({ "niter": 0 }),
        ))
        .expect("native unequal-XX/YY dirty execution");
        let psf = product_plane(&image_name, ".psf");
        let residual = product_plane(&image_name, ".residual");
        let sumwt =
            PagedImage::<f32>::open(PathBuf::from(format!("{}.sumwt", image_name.display())))
                .expect("open Stokes-I sum weights")
                .get()
                .expect("read Stokes-I sum weights");
        (psf, residual, sumwt)
    };

    let (equal_psf, equal_residual, equal_sumwt) = run("equal-hands", [2.0, 2.0]);
    let (unequal_psf, unequal_residual, unequal_sumwt) = run("unequal-hands", [1.0, 3.0]);

    assert_eq!(equal_psf, unequal_psf, "the common mean preserves the PSF");
    assert!(
        equal_residual.iter().any(|value| value.abs() > 0.0),
        "the numerator comparison must be non-vacuous"
    );
    for (equal, unequal) in equal_residual.iter().zip(unequal_residual.iter()) {
        assert!(
            (equal - unequal).abs() <= 1.0e-6,
            "shared per-hand weighting changed the Stokes-I numerator: equal={equal} unequal={unequal}"
        );
    }
    assert_eq!(
        equal_sumwt.as_slice().expect("contiguous sum weights"),
        &[4.0]
    );
    assert_eq!(
        unequal_sumwt.as_slice().expect("contiguous sum weights"),
        &[4.0],
        "both mapped hands contribute their shared row/channel imaging weight"
    );
}

#[test]
fn application_executes_full_stokes_mfs_clean_with_complete_products_and_axes() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = full_stokes_measurement_set(root.path());
    let image_name = root.path().join("full-stokes-dirty");
    let imaging = request(
        &measurement_set,
        &image_name,
        json!({
            "stokes": "IQUV",
            "imsize": 64,
            "niter": 4,
            "minor_cycle_length": 4,
            "savemodel": "modelcolumn",
        }),
    );

    let result = execute(&imaging).expect("native full-Stokes Högbom execution");
    assert_eq!(result.total_minor_iterations, 1);
    assert_eq!(result.total_actual_minor_iterations, 1);
    // The fixture allows one major cycle after the initial one (nmajor).
    assert_eq!(result.stop, Some(CleanStop::MajorCycles));
    assert_eq!(
        result.minor_cycles[0].stop_reason,
        casa_imaging_application::NativeMinorCycleStopReason::ThresholdReached,
        "an early scientific stop reports the actual component count without CASA iteration-bound clamping"
    );
    assert_eq!(
        result
            .visibility_products
            .as_ref()
            .expect("full-Stokes visibility completion")
            .sample_count(),
        2_808
    );
    assert_standard_products(&image_name, &result.product_names());
    for suffix in [".psf", ".residual", ".model", ".image"] {
        let product =
            PagedImage::<f32>::open(PathBuf::from(format!("{}{suffix}", image_name.display())))
                .expect("reopen full-Stokes product");
        assert_eq!(product.shape(), &[64, 64, 4, 1]);
        let CoordinateModel::Stokes(stokes) = product.coordinates().coordinate(1) else {
            panic!("full-Stokes product has no polarization coordinate")
        };
        assert_eq!(
            stokes.stokes(),
            &[StokesType::I, StokesType::Q, StokesType::U, StokesType::V]
        );
        assert_eq!(
            product.units(),
            match suffix {
                ".model" => "Jy/pixel",
                ".image" => "Jy/beam",
                _ => "",
            }
        );
        assert!(
            product
                .get()
                .expect("read full-Stokes payload")
                .iter()
                .all(|value| value.is_finite())
        );
    }
    let sum_weights =
        PagedImage::<f32>::open(PathBuf::from(format!("{}.sumwt", image_name.display())))
            .expect("reopen full-Stokes sum weights");
    assert_eq!(sum_weights.shape(), &[1, 1, 4, 1]);
    assert_eq!(
        sum_weights.get().expect("read full-Stokes sum weights"),
        ArrayD::from_shape_vec(vec![1, 1, 4, 1], vec![452.0; 4])
            .expect("full-Stokes sum-weight shape")
    );
    let reopened = MeasurementSet::open(&measurement_set).expect("reopen full-Stokes MODEL_DATA");
    let model = reopened
        .data_column(VisibilityDataColumn::ModelData)
        .expect("full-Stokes MODEL_DATA");
    let ArrayValue::Complex32(model) = model.get(0).expect("full-Stokes MODEL_DATA row") else {
        panic!("full-Stokes MODEL_DATA is complex")
    };
    assert!(
        model
            .iter()
            .all(|value| value.re.is_finite() && value.im.is_finite())
    );
    assert!(model.iter().any(|value| *value != Complex32::new(9.0, 9.0)));
}

#[test]
fn application_executes_raw_linear_correlation_products_with_exact_axis() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let image_name = root.path().join("linear-correlations");
    let linear_request = |measurement_set: PathBuf| {
        request(
            &measurement_set,
            &image_name,
            json!({ "niter": 0, "stokes": "XXXYYXYY", "imsize": 64 }),
        )
    };

    // Circular feeds hold no linear correlation to image.
    let circular = execute(&linear_request(full_stokes_measurement_set(root.path())));
    assert!(
        circular.err().is_some_and(|error| error
            .to_string()
            .contains("requested correlation is not selected")),
        "linear products of circular-feed data must be rejected"
    );
    assert!(!PathBuf::from(format!("{}.residual", image_name.display())).exists());

    let result = execute(&linear_request(full_polarization_linear_measurement_set(
        root.path(),
    )))
    .expect("native raw-correlation dirty execution");
    assert_dirty_products(&image_name, &result.product_names());
    let product =
        PagedImage::<f32>::open(PathBuf::from(format!("{}.residual", image_name.display())))
            .expect("reopen raw-correlation residual");
    assert_eq!(product.shape(), &[64, 64, 4, 1]);
    let CoordinateModel::Stokes(stokes) = product.coordinates().coordinate(1) else {
        panic!("raw-correlation product has no polarization coordinate")
    };
    assert_eq!(
        stokes.stokes(),
        &[
            StokesType::XX,
            StokesType::XY,
            StokesType::YX,
            StokesType::YY
        ]
    );
    assert_eq!(product.units(), "");
}

#[test]
fn application_uses_weight_when_selected_weight_spectrum_cells_are_undefined() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = undefined_weight_spectrum_measurement_set(root.path());
    let image_name = root.path().join("undefined-weight-spectrum-dirty");

    let result = execute(&request(
        &measurement_set,
        &image_name,
        json!({ "niter": 0 }),
    ))
    .expect("undefined WEIGHT_SPECTRUM cells select scalar WEIGHT before traversal");

    assert_dirty_products(&image_name, &result.product_names());
}

/// The compiled `FiniteValuePolicy::FlagInputRejectGenerated`: a non-finite
/// visibility is a flagged sample, so the image equals the image of the
/// same data with that sample flagged instead.
#[test]
fn nonfinite_visibilities_image_as_flagged_samples() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    const ROW: usize = 3;
    let mut images = Vec::new();
    for nonfinite in [true, false] {
        let root = tempfile::tempdir().expect("test root");
        let path = multi_row_measurement_set(root.path());
        let mut ms = MeasurementSet::open(&path).expect("open fixture");
        if nonfinite {
            let mut data = ms
                .data_column_mut(VisibilityDataColumn::Data)
                .expect("DATA column");
            let ArrayValue::Complex32(cell) = data.get(ROW).expect("DATA cell").clone() else {
                panic!("DATA is complex");
            };
            data.put(
                ROW,
                ArrayValue::Complex32(cell.mapv(|_| Complex32::new(f32::NAN, 0.0))),
            )
            .expect("write NaN");
        } else {
            let flags = ms.flag_column();
            let ArrayValue::Bool(cell) = flags.get(ROW).expect("FLAG cell").clone() else {
                panic!("FLAG is boolean");
            };
            ms.main_table_mut()
                .row_accessor_mut()
                .set_cell(
                    ROW,
                    "FLAG",
                    Value::Array(ArrayValue::Bool(cell.mapv(|_| true))),
                )
                .expect("flag the row's samples");
        }
        ms.save().expect("save fixture");
        drop(ms);
        let image_name = root.path().join("finite");
        let result = execute(&request(&path, &image_name, json!({ "niter": 0 })))
            .unwrap_or_else(|error| panic!("nonfinite={nonfinite}: {error}"));
        assert_dirty_products(&image_name, &result.product_names());
        images.push([".residual", ".psf", ".sumwt"].map(|suffix| {
            product_plane_with_size(&image_name, suffix, if suffix == ".sumwt" { 1 } else { 16 })
        }));
    }
    assert!(images[0][0].iter().all(|value| value.is_finite()));
    assert_eq!(images[0], images[1]);
}

#[test]
fn t31_application_executes_recentered_domains_through_one_scientific_route() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = tiny_measurement_set(root.path());

    for (label, niter) in [("dirty", 0), ("hogbom", 1)] {
        let product_suffixes = if niter == 0 {
            DIRTY_PRODUCT_SUFFIXES.as_slice()
        } else {
            PRODUCT_SUFFIXES.as_slice()
        };
        let image_name = root.path().join(format!("t31-{label}-main"));
        let outlier_name = root.path().join(format!("t31-{label}-outlier"));
        let outlier_file = root.path().join(format!("t31-{label}.outlier"));
        std::fs::write(
            &outlier_file,
            format!(
                "imagename={}\nimsize=[16,16]\ncell=[1arcsec,1arcsec]\nphasecenter=J2000 1.001rad 0.499rad\nusemask=user\nmask=circle[[8pix,8pix],4pix]\nspecmode=mfs\nnchan=1\nnterms=1\ngridder=standard\ndeconvolver=hogbom\nwprojplanes=1\n",
                outlier_name.display()
            ),
        )
        .expect("write CASA outlier fixture");
        let imaging = request(
            &measurement_set,
            &image_name,
            json!({ "niter": niter, "outlierfile": outlier_file }),
        );

        let result = execute(&imaging).expect("execute T31 multi-domain application");
        assert_eq!(result.scientific.normal_state().domain_count(), 2);
        assert_eq!(
            result.planned_products.members().len(),
            2 * product_suffixes.len()
        );
        assert_eq!(
            result
                .planned_products
                .members()
                .iter()
                .filter(|member| member.axes().domain() == &ImageDomainRole::Main)
                .count(),
            product_suffixes.len()
        );
        assert_eq!(
            result
                .planned_products
                .members()
                .iter()
                .filter(|member| {
                    member.axes().domain()
                        == &ImageDomainRole::Outlier(outlier_name.display().to_string())
                })
                .count(),
            product_suffixes.len()
        );

        for (base, expected) in [(&image_name, [1.0, 0.5]), (&outlier_name, [1.001, 0.499])] {
            for suffix in product_suffixes {
                assert!(
                    PathBuf::from(format!("{}{suffix}", base.display())).is_dir(),
                    "missing {label} domain product {}{suffix}",
                    base.display()
                );
            }
            let psf = PagedImage::<f32>::open(PathBuf::from(format!("{}.psf", base.display())))
                .expect("open domain PSF");
            let world = psf
                .coordinates()
                .to_world(&[8.0, 8.0, 0.0, 0.0])
                .expect("domain reference world coordinate");
            assert!((world[0] - expected[0]).abs() < 1.0e-12);
            assert!((world[1] - expected[1]).abs() < 1.0e-12);
            for suffix in [".psf", ".residual", ".model", ".image"] {
                assert!(
                    product_plane(base, suffix)
                        .iter()
                        .all(|value| value.is_finite()),
                    "{label} {suffix} must be finite on valid support"
                );
            }
        }

        let model_nonzero = fixture_model_samples(result.scientific.final_model())
            .iter()
            .filter(|sample| sample.value().value() != 0.0)
            .count();
        match label {
            "dirty" => assert_eq!(model_nonzero, 0),
            "hogbom" => assert_eq!(model_nonzero, 2),
            _ => unreachable!(),
        }
    }
}

/// Image domains are independent measurements of one observation: a dirty
/// cube's main domain is the same with or without a recentred outlier cube,
/// and the outlier carries the same spectral axis.
#[test]
fn outlier_cube_domains_image_independently_of_the_main_cube() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = thirty_two_channel_multi_row_measurement_set(root.path());
    let cube = |image_name: &Path, outlier_file: Option<PathBuf>| {
        let imaging = request(
            &measurement_set,
            image_name,
            json!({
                "niter": 0,
                "imsize": 32,
                "field": "0,1",
                "outlierfile": outlier_file,
                "spw": "0:0~31",
                "channel_count": 32,
                "specmode": "cube",
                "outframe": "TOPO",
            }),
        );
        execute(&imaging).expect("dirty cube execution")
    };
    let alone = root.path().join("cube-alone");
    cube(&alone, None);
    let main = root.path().join("cube-main");
    let outlier = root.path().join("cube-outlier");
    let outlier_file = root.path().join("cube.outlier");
    std::fs::write(
        &outlier_file,
        format!(
            "imagename={}\nimsize=[32,32]\ncell=[1arcsec,1arcsec]\nphasecenter=J2000 1.001rad 0.499rad\n",
            outlier.display()
        ),
    )
    .expect("write recentred outlier cube");
    let result = cube(&main, Some(outlier_file));
    assert_eq!(result.scientific.normal_state().domain_count(), 2);
    let read = |base: &Path, suffix: &str| {
        let image = PagedImage::<f32>::open(PathBuf::from(format!("{}{suffix}", base.display())))
            .expect("open cube product");
        let shape = image.shape().to_vec();
        (
            shape.clone(),
            image.get_slice(&[0, 0, 0, 0], &shape).expect("read"),
        )
    };
    for suffix in DIRTY_PRODUCT_SUFFIXES {
        assert_eq!(read(&main, suffix), read(&alone, suffix), "main {suffix}");
        let (shape, values) = read(&outlier, suffix);
        let expected: &[usize] = if suffix == ".sumwt" {
            &[1, 1, 1, 32]
        } else {
            &[32, 32, 1, 32]
        };
        assert_eq!(shape, expected, "outlier {suffix}");
        assert!(
            values.iter().all(|value| value.is_finite()),
            "outlier {suffix}"
        );
    }
    // Natural weights do not depend on the phase centre: the outlier
    // grids every sample of every channel the main cube does.
    let (_, outlier_sumwt) = read(&outlier, ".sumwt");
    let (_, main_sumwt) = read(&main, ".sumwt");
    assert!(main_sumwt.iter().all(|value| *value > 0.0));
    assert_eq!(outlier_sumwt, main_sumwt);
}

#[test]
fn t31_application_canonicalizes_reversed_outliers_before_domain_indexed_derivations() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = tiny_measurement_set(root.path());
    let main = root.path().join("main");
    let alpha = root.path().join("alpha");
    let zeta = root.path().join("zeta");
    let outlier_file = root.path().join("reversed.outlier");
    std::fs::write(
        &outlier_file,
        "imagename=zeta\nimsize=[10,10]\ncell=[1arcsec,1arcsec]\nphasecenter=J2000 1.003rad 0.497rad\nusemask=user\nmask=circle[[5pix,5pix],2pix]\n\
         imagename=alpha\nimsize=[12,12]\ncell=[1arcsec,1arcsec]\nphasecenter=J2000 0.998rad 0.502rad\nusemask=user\nmask=circle[[3pix,3pix],1pix]\n",
    )
    .expect("write reversed CASA outlier fixture");
    let imaging = request(
        &measurement_set,
        &main,
        json!({ "outlierfile": outlier_file }),
    );

    let result = execute(&imaging).expect("execute canonical multi-domain application");
    let expected = [
        (ImageDomainRole::Main, main.as_path(), 16, [1.0, 0.5], 256),
        (
            ImageDomainRole::Outlier("alpha".to_string()),
            alpha.as_path(),
            12,
            [0.998, 0.502],
            5,
        ),
        (
            ImageDomainRole::Outlier("zeta".to_string()),
            zeta.as_path(),
            10,
            [1.003, 0.497],
            13,
        ),
    ];

    let normal_roles = result
        .scientific
        .normal_state()
        .read_window(0..1)
        .expect("fixture domain window")
        .domains()
        .map(|domain| domain.role().clone())
        .collect::<Vec<_>>();
    assert_eq!(
        normal_roles,
        expected
            .iter()
            .map(|(role, ..)| role.clone())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        result.planned_products.members().len(),
        result.products.members().len(),
        "publication must retain the canonical domain inventory"
    );
    for (planned, published) in result
        .planned_products
        .members()
        .iter()
        .zip(result.products.members())
    {
        assert_eq!(planned.node(), published.node());
    }

    for (role, base, image_size, direction, expected_mask_pixels) in expected {
        let members = result
            .planned_products
            .members()
            .iter()
            .filter(|member| member.axes().domain() == &role)
            .collect::<Vec<_>>();
        assert_eq!(members.len(), 6, "wrong product association for {role:?}");
        assert!(members.iter().any(|member| member.name() == ".mask"));
        assert!(
            members
                .iter()
                .filter(|member| member.name() != ".sumwt")
                .all(|member| member.shape()[..2] == [image_size, image_size])
        );

        let psf = PagedImage::<f32>::open(PathBuf::from(format!("{}.psf", base.display())))
            .expect("open domain PSF");
        let reference = image_size as f64 / 2.0;
        let world = psf
            .coordinates()
            .to_world(&[reference, reference, 0.0, 0.0])
            .expect("domain reference world coordinate");
        assert!((world[0] - direction[0]).abs() < 1.0e-12);
        assert!((world[1] - direction[1]).abs() < 1.0e-12);

        let mask = product_plane_with_size(base, ".mask", image_size);
        assert_eq!(
            mask.iter().filter(|value| **value != 0.0).count(),
            expected_mask_pixels,
            "wrong mask support published for {role:?}"
        );
    }
}

#[test]
fn application_executes_a_multi_row_dirty_image() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = multi_row_measurement_set(root.path());
    let image_name = root.path().join("multi-row-dirty");

    let result = execute(&request(
        &measurement_set,
        &image_name,
        json!({ "niter": 0 }),
    ))
    .expect("native multi-row dirty execution");
    assert_dirty_products(&image_name, &result.product_names());
}

#[test]
fn application_compiles_common_beam_requests_with_common_spectral_coupling() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = tiny_measurement_set(root.path());
    let image_name = root.path().join("common-beam");
    let imaging = request(
        &measurement_set,
        &image_name,
        json!({ "niter": 0, "restoringbeam": "common" }),
    );

    let result = execute(&imaging).expect("native common-beam application execution");
    assert_dirty_products(&image_name, &result.product_names());
    let restored =
        PagedImage::<f32>::open(PathBuf::from(format!("{}.image", image_name.display())))
            .expect("reopen common-beam restored image");
    assert!(
        restored
            .image_info()
            .expect("read restored image info")
            .beam_set
            .has_single_beam()
    );
}

#[test]
fn cube_common_beam_products_preserve_blank_pixels_and_casa_metadata_without_pb() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = spectral_line_measurement_set(root.path());
    let image_name = root.path().join("common-beam-cube");
    let imaging = request(
        &measurement_set,
        &image_name,
        json!({
            "niter": 0,
            "spw": "0:0~3",
            "channel_count": 4,
            "specmode": "cube",
            "outframe": "TOPO",
            "start": "3",
            "width": "-1",
            "restoringbeam": "common",
        }),
    );

    let result = execute(&imaging).expect("native common-beam cube execution");
    assert_dirty_products(&image_name, &result.product_names());
    let open = |suffix: &str| {
        PagedImage::<f32>::open(PathBuf::from(format!("{}{suffix}", image_name.display())))
            .expect("reopen cube product")
    };
    let psf = open(".psf");
    let residual = open(".residual");
    let restored = open(".image");
    for product in [&psf, &residual, &restored] {
        assert_eq!(product.shape(), &[16, 16, 1, 4]);
    }
    assert_eq!(psf.units(), "");
    for channel in 0..4 {
        let plane = psf.get_slice(&[0, 0, 0, channel], &[16, 16, 1, 1]).unwrap();
        assert_eq!(
            plane.iter().copied().fold(0.0_f32, f32::max),
            if channel == 0 { 0.0 } else { 1.0 }
        );
    }
    assert_eq!(residual.units(), "");
    assert_eq!(restored.units(), "Jy/beam");

    let psf_beams = psf.image_info().expect("PSF ImageInfo").beam_set;
    let residual_beams = residual.image_info().expect("residual ImageInfo").beam_set;
    assert!(residual_beams.is_empty());
    assert!(
        restored
            .image_info()
            .expect("restored ImageInfo")
            .beam_set
            .has_single_beam()
    );
    let largest_valid = (1..4)
        .map(|channel| *psf_beams.beam(channel, 0))
        .max_by(|left, right| left.area().total_cmp(&right.area()))
        .expect("valid fitted beams");
    assert_eq!(*psf_beams.beam(0, 0), largest_valid);

    for product in [&residual, &restored] {
        assert_eq!(product.default_mask_name(), None);
        assert!(product.mask_names().is_empty());
        let blank = product
            .get_slice(&[0, 0, 0, 0], &[16, 16, 1, 1])
            .expect("blank channel");
        assert!(blank.iter().all(|value| *value == 0.0));
        let valid = product
            .get_slice(&[0, 0, 0, 1], &[16, 16, 1, 1])
            .expect("emitting channel");
        assert!(valid.iter().any(|value| *value != 0.0));
    }
    let first = restored
        .coordinates()
        .to_world(&[8.0, 8.0, 0.0, 0.0])
        .expect("first channel world coordinate");
    let second = restored
        .coordinates()
        .to_world(&[8.0, 8.0, 0.0, 1.0])
        .expect("second channel world coordinate");
    assert!(first[3] > second[3], "descending spectral WCS");
}

#[test]
fn t607_application_preserves_channel_topology_and_wcs_through_cube_planning() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = thirty_two_channel_measurement_set(root.path());
    let image_name = root.path().join("t607-channel-local-cube");
    let imaging = request(
        &measurement_set,
        &image_name,
        json!({
            "niter": 0,
            "spw": "0:0~31",
            "channel_count": 32,
            "specmode": "cube",
            "outframe": "TOPO",
        }),
    );

    let result = execute(&imaging).expect("native 32-channel cube execution");
    assert_dirty_products(&image_name, &result.product_names());
    assert_eq!(
        result.scientific.normal_state().catalog(),
        casa_imaging_reconstruction::NormalStateCatalog::UnnormalizedChannelSlabV1
    );
    let residual =
        PagedImage::<f32>::open(PathBuf::from(format!("{}.residual", image_name.display())))
            .expect("reopen 32-channel residual");
    assert_eq!(residual.shape(), &[16, 16, 1, 32]);
    let first = residual
        .coordinates()
        .to_world(&[8.0, 8.0, 0.0, 0.0])
        .expect("first channel world coordinate");
    let last = residual
        .coordinates()
        .to_world(&[8.0, 8.0, 0.0, 31.0])
        .expect("last channel world coordinate");
    assert!(last[3] > first[3], "ascending spectral WCS");
}
