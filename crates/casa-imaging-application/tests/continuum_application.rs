// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{
    path::{Path, PathBuf},
    sync::Mutex,
};

use casa_coordinates::{
    CoordinateModel, CoordinateSystem, DirectionCoordinate, LinearCoordinate, Projection,
    ProjectionType, SpectralCoordinate, StokesCoordinate, StokesType,
};
use casa_images::PagedImage;
use casa_imaging_application::{
    ContinuumAlgorithm, ContinuumAutoMaskControls, ContinuumAwProjection, ContinuumBeamPolicy,
    ContinuumImagingRequest, ContinuumMask, ContinuumMaskBox, ContinuumStopReason,
    ContinuumWeighting, SpectralImagingMode, TaskRequirement, VisibilityContinuumSubtraction,
    execute_continuum, resource_policy_for_task_requirements,
};
use casa_imaging_model::{
    ImageDomainRole, ProductBeamRule, ProductRole, ProductTerm, ProductUnit, ProductValidityRule,
};
use casa_imaging_runtime::ReceiptStatus;
use casa_ms::{
    CubeAxisConfig, CubeAxisValue, MeasurementSet, MeasurementSetBuilder, OptionalMainColumn,
    SubtableId, VisibilityDataColumn,
    column_def::{ColumnDef, ColumnKind},
    initialize_measurement_set_owner_manifest, schema,
};
use casa_types::{
    ArrayValue, Complex32, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
    measures::frequency::FrequencyRef,
};
use ndarray::ArrayD;

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

#[path = "common/continuum_fixture.rs"]
mod continuum_fixture;
use continuum_fixture::*;

#[path = "continuum_application/clean_cycles.rs"]
mod clean_cycles;

#[path = "continuum_application/visibility_writes.rs"]
mod visibility_writes;

#[path = "continuum_application/t53_spectral_joins.rs"]
mod t53_spectral_joins;

#[path = "continuum_application/t55_cube_pipeline.rs"]
mod t55_cube_pipeline;

#[path = "continuum_application/t55_real_cube.rs"]
mod t55_real_cube;

#[path = "continuum_application/t55_c_array_turnaround.rs"]
mod t55_c_array_turnaround;

#[path = "continuum_application/t55_mfs_pilot.rs"]
mod t55_mfs_pilot;

#[test]
fn unsupported_primary_beam_frequency_rejects_before_execution_receipts() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    // This existing fixture labels 44 GHz as EVLA; the selected common EVLA
    // model represents only L/S/C, unlike the separate legacy-VLA Q model.
    let measurement_set = vla_aw_measurement_set(root.path());
    let image_name = root.path().join("unsupported-beam");
    let mut imaging = request(measurement_set, image_name, ContinuumAlgorithm::Dirty);
    imaging.write_primary_beam = true;
    let error = execute_continuum(imaging)
        .err()
        .expect("unsupported beam frequency");
    let casa_imaging_application::ApplicationDispatchError::Native(error) = error else {
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
    assert!(
        std::fs::read_dir(root.path().join(".casa-rs-imaging-receipts"))
            .expect("prepared receipt directory")
            .next()
            .is_none(),
        "no weighting, replay, reconstruction or publication may execute"
    );
    assert!(!root.path().join("unsupported-beam.pb").exists());
}

#[test]
fn image_pointing_center_preserves_casa_positive_pi_longitude() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let image_name = root.path().join("antimeridian");
    let mut imaging = request(
        tiny_measurement_set(root.path()),
        image_name.clone(),
        ContinuumAlgorithm::Dirty,
    );
    imaging.phase_center = Some("J2000 12h00m00s +34d04m43.5s".to_string());
    execute_continuum(imaging).unwrap_or_else(|error| panic!("dirty image: {error}"));
    let image = PagedImage::<f32>::open(root.path().join("antimeridian.image")).expect("image");
    assert_eq!(
        image.coordinates().obs_info().pointing_center_rad[0],
        std::f64::consts::PI
    );
}

#[test]
fn image_observation_metadata_accepts_matching_labels_across_observations() {
    let _execution_guard = EXECUTION_LOCK.lock().unwrap();
    set_production_io_environment();
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
        let result = execute_continuum(request(path, prefix.clone(), ContinuumAlgorithm::Dirty));
        if accepted {
            let result = result.unwrap_or_else(|error| panic!("joint observation: {error}"));
            assert_dirty_products(&prefix, &result.product_names);
            let image =
                PagedImage::<f32>::open(root.path().join("joint-observation.image")).unwrap();
            assert_eq!(image.coordinates().obs_info().telescope, "EVLA");
            assert_eq!(image.coordinates().obs_info().observer, "casa-rs-test");
        } else {
            let error = result
                .err()
                .expect("conflicting image metadata must reject");
            assert!(
                error
                    .to_string()
                    .contains("consistent telescope and observer")
            );
            assert!(!root.path().join("joint-observation.image").exists());
        }
    }
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
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = tiny_measurement_set(root.path());
    let image_name = root.path().join("dirty");

    let result = execute_continuum(request(
        measurement_set,
        image_name.clone(),
        ContinuumAlgorithm::Dirty,
    ))
    .expect("native dirty application execution");

    assert_eq!(result.minor_iterations, 0);
    assert_eq!(result.minor_stop_reason, None);
    assert_dirty_products(&image_name, &result.product_names);
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
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = alma_primary_beam_measurement_set(root.path());
    let image_name = root.path().join("taylor-metadata");
    let mut imaging = request(
        measurement_set,
        image_name.clone(),
        ContinuumAlgorithm::Mtmfs {
            terms: 2,
            scales_px: vec![0.0],
            small_scale_bias: 0.0,
        },
    );
    imaging.data_description = None;
    imaging.image_size = 8;
    imaging.channel_count = Some(1);
    imaging.iterations = 0;
    imaging.write_primary_beam = true;
    imaging.task_requirements = vec![TaskRequirement::SerialCpu];

    let result = execute_continuum(imaging).expect("native Taylor metadata execution");
    let planned = &result.outcome.output.planned_products;
    let published = &result.outcome.output.products;
    assert_eq!(planned.graph_id(), published.graph_id());
    assert_eq!(planned.members().len(), published.members().len());
    for (planned, published) in planned.members().iter().zip(published.members()) {
        assert_eq!(
            planned.node(),
            published.node(),
            "{} identity",
            planned.name()
        );
        assert_eq!(
            planned.role(),
            published.contract().role(),
            "{} role",
            planned.name()
        );
        assert_eq!(
            planned.unit(),
            published.contract().unit(),
            "{} unit",
            planned.name()
        );
        assert_eq!(
            planned.beam_rule(),
            published.contract().beam_rule(),
            "{} beam rule",
            planned.name()
        );
        assert_eq!(
            planned.validity(),
            published.contract().validity(),
            "{} validity",
            planned.name()
        );
        assert_eq!(
            planned.storage(),
            published.contract().storage(),
            "{} storage contract",
            planned.name()
        );
        assert_eq!(
            planned.axes(),
            published.contract().axes(),
            "{} axes",
            planned.name()
        );
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
fn t49_plane_count_does_not_infer_w_projection() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = tiny_measurement_set(root.path());
    let image_name = root.path().join("w-planes-without-capability");
    let mut imaging = request(
        measurement_set,
        image_name.clone(),
        ContinuumAlgorithm::Dirty,
    );
    imaging.w_projection_planes = Some(5);

    let error = match execute_continuum(imaging) {
        Ok(_) => panic!("plane count inferred W projection"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("requires the explicit W- or AW-projection task capability"),
        "wrong explicit-W error: {error}"
    );
    assert!(!PathBuf::from(format!("{}.psf", image_name.display())).exists());
}

#[test]
fn stokes_i_uses_one_shared_imaging_weight_for_each_linear_parallel_hand() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");

    let run = |name: &str, weights: [f32; 2]| {
        let measurement_set =
            unequal_linear_parallel_hand_measurement_set(root.path(), name, weights);
        let image_name = root.path().join(format!("{name}-dirty"));
        execute_continuum(request(
            measurement_set,
            image_name.clone(),
            ContinuumAlgorithm::Dirty,
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
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = full_stokes_measurement_set(root.path());
    let image_name = root.path().join("full-stokes-dirty");
    let mut imaging = request(
        measurement_set.clone(),
        image_name.clone(),
        ContinuumAlgorithm::Hogbom,
    );
    imaging.polarizations = vec![
        casa_imaging_application::PolarizationCoordinate::StokesI,
        casa_imaging_application::PolarizationCoordinate::StokesQ,
        casa_imaging_application::PolarizationCoordinate::StokesU,
        casa_imaging_application::PolarizationCoordinate::StokesV,
    ];
    imaging.image_size = 64;
    imaging.iterations = 4;
    imaging.cycle_iterations = 4;
    imaging.save_model_column = true;
    imaging.task_requirements = vec![
        TaskRequirement::PolarizationSelection,
        TaskRequirement::ModelColumnWrite,
    ];

    let result = execute_continuum(imaging).expect("native full-Stokes Högbom execution");
    assert_eq!(result.minor_iterations, 1);
    assert_eq!(result.actual_minor_iterations, 1);
    assert_eq!(
        result.minor_stop_reason,
        Some(ContinuumStopReason::ThresholdReached),
        "an early scientific stop reports the actual component count without CASA iteration-bound clamping"
    );
    assert_eq!(
        result
            .outcome
            .output
            .visibility_products
            .as_ref()
            .expect("full-Stokes visibility completion")
            .sample_count(),
        2_808
    );
    assert_standard_products(&image_name, &result.product_names);
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
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let image_name = root.path().join("linear-correlations");
    let linear_request = |measurement_set| {
        let mut imaging = request(
            measurement_set,
            image_name.clone(),
            ContinuumAlgorithm::Dirty,
        );
        imaging.polarizations = vec![
            casa_imaging_application::PolarizationCoordinate::LinearXx,
            casa_imaging_application::PolarizationCoordinate::LinearXy,
            casa_imaging_application::PolarizationCoordinate::LinearYx,
            casa_imaging_application::PolarizationCoordinate::LinearYy,
        ];
        imaging.image_size = 64;
        imaging.task_requirements = vec![TaskRequirement::PolarizationSelection];
        imaging
    };

    // Circular feeds hold no linear correlation to image.
    let circular = execute_continuum(linear_request(full_stokes_measurement_set(root.path())));
    assert!(
        circular.err().is_some_and(|error| error
            .to_string()
            .contains("requested correlation is not selected")),
        "linear products of circular-feed data must be rejected"
    );
    assert!(!PathBuf::from(format!("{}.residual", image_name.display())).exists());

    let result = execute_continuum(linear_request(full_polarization_linear_measurement_set(
        root.path(),
    )))
    .expect("native raw-correlation dirty execution");
    assert_dirty_products(&image_name, &result.product_names);
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
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = undefined_weight_spectrum_measurement_set(root.path());
    let image_name = root.path().join("undefined-weight-spectrum-dirty");

    let result = execute_continuum(request(
        measurement_set,
        image_name.clone(),
        ContinuumAlgorithm::Dirty,
    ))
    .expect("undefined WEIGHT_SPECTRUM cells select scalar WEIGHT before traversal");

    assert_dirty_products(&image_name, &result.product_names);
}

#[test]
fn t31_application_executes_recentered_domains_through_one_scientific_route() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = tiny_measurement_set(root.path());

    for (label, algorithm) in [
        ("dirty", ContinuumAlgorithm::Dirty),
        ("hogbom", ContinuumAlgorithm::Hogbom),
    ] {
        let product_suffixes = if algorithm == ContinuumAlgorithm::Dirty {
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
        let mut imaging = request(measurement_set.clone(), image_name.clone(), algorithm);
        imaging.outlier_file = Some(outlier_file);
        imaging.task_requirements = vec![TaskRequirement::SerialCpu, TaskRequirement::FixedTileCpu];

        let result = execute_continuum(imaging).expect("execute T31 multi-domain application");
        assert_eq!(
            result
                .outcome
                .output
                .scientific
                .normal_state()
                .domain_count(),
            2
        );
        assert_eq!(
            result.outcome.output.planned_products.members().len(),
            2 * product_suffixes.len()
        );
        assert_eq!(
            result
                .outcome
                .output
                .planned_products
                .members()
                .iter()
                .filter(|member| member.axes().domain() == &ImageDomainRole::Main)
                .count(),
            product_suffixes.len()
        );
        assert_eq!(
            result
                .outcome
                .output
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

        let model_nonzero = fixture_model_samples(result.outcome.output.scientific.final_model())
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

#[test]
fn t31_application_canonicalizes_reversed_outliers_before_domain_indexed_derivations() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
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
    let mut imaging = request(measurement_set, main.clone(), ContinuumAlgorithm::Hogbom);
    imaging.outlier_file = Some(outlier_file);
    imaging.task_requirements = vec![TaskRequirement::SerialCpu, TaskRequirement::FixedTileCpu];

    let result = execute_continuum(imaging).expect("execute canonical multi-domain application");
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
        .outcome
        .output
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
        result.outcome.output.planned_products.graph_id(),
        result.outcome.output.products.graph_id(),
        "publication must retain the canonical domain inventory"
    );
    for (planned, published) in result
        .outcome
        .output
        .planned_products
        .members()
        .iter()
        .zip(result.outcome.output.products.members())
    {
        assert_eq!(planned.node(), published.node());
        assert_eq!(
            planned.axes().domain(),
            published.contract().axes().domain()
        );
    }

    for (role, base, image_size, direction, expected_mask_pixels) in expected {
        let members = result
            .outcome
            .output
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
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = multi_row_measurement_set(root.path());
    let image_name = root.path().join("multi-row-dirty");

    let result = execute_continuum(request(
        measurement_set,
        image_name.clone(),
        ContinuumAlgorithm::Dirty,
    ))
    .expect("native multi-row dirty execution");
    assert_dirty_products(&image_name, &result.product_names);
}

#[test]
fn application_compiles_common_beam_requests_with_common_spectral_coupling() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = tiny_measurement_set(root.path());
    let image_name = root.path().join("common-beam");
    let mut imaging = request(
        measurement_set,
        image_name.clone(),
        ContinuumAlgorithm::Dirty,
    );
    imaging.beam_policy = ContinuumBeamPolicy::Common;

    let result = execute_continuum(imaging).expect("native common-beam application execution");
    assert_dirty_products(&image_name, &result.product_names);
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
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = spectral_line_measurement_set(root.path());
    let image_name = root.path().join("common-beam-cube");
    let mut imaging = request(
        measurement_set,
        image_name.clone(),
        ContinuumAlgorithm::Dirty,
    );
    imaging.spectral_window = Some("0:0~3".to_string());
    imaging.channel_count = Some(4);
    imaging.spectral_mode = SpectralImagingMode::Cube {
        axis: CubeAxisConfig {
            outframe: FrequencyRef::TOPO,
            start: Some(CubeAxisValue::Channel(3)),
            width: Some(CubeAxisValue::Channel(-1)),
            ..CubeAxisConfig::default()
        },
        output_channels: Some(4),
    };
    imaging.beam_policy = ContinuumBeamPolicy::Common;

    let result = execute_continuum(imaging).expect("native common-beam cube execution");
    assert_dirty_products(&image_name, &result.product_names);
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
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = thirty_two_channel_measurement_set(root.path());
    let image_name = root.path().join("t607-channel-local-cube");
    let mut imaging = request(
        measurement_set,
        image_name.clone(),
        ContinuumAlgorithm::Dirty,
    );
    imaging.spectral_window = Some("0:0~31".to_string());
    imaging.channel_count = Some(32);
    imaging.spectral_mode = SpectralImagingMode::Cube {
        axis: CubeAxisConfig {
            outframe: FrequencyRef::TOPO,
            ..CubeAxisConfig::default()
        },
        output_channels: Some(32),
    };

    let result = execute_continuum(imaging).expect("native 32-channel cube execution");
    assert_dirty_products(&image_name, &result.product_names);
    assert_eq!(
        result.outcome.output.scientific.normal_state().catalog(),
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
