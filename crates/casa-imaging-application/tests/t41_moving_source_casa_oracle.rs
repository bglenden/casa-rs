// SPDX-License-Identifier: LGPL-3.0-or-later

//! Focused T41 moving-source gate against a frozen CASA Uranus cube.

use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
};

use casa_coordinates::{
    Coordinate, CoordinateModel, CoordinateType, DirectionCoordinate, ProjectionType,
    SpectralCoordinate,
};
use casa_images::PagedImage;
use casa_imaging_application::{
    ContinuumAlgorithm, ContinuumBeamPolicy, ContinuumImagingRequest, ContinuumMask,
    ContinuumWeighting, HogbomIterationAccounting, PolarizationCoordinate, SpectralImagingMode,
    TaskRequirement, execute_continuum,
};
use casa_imaging_model::SpectralWindowSelection;
use casa_ms::{
    CubeAxisConfig, CubeAxisValue, MeasurementSet, MsSelectionIoBudget,
    SelectedObservationContentBudget, SelectedObservationEphemeris, SelectedObservationRow,
};
use casa_test_support::{CasaTestDataTier, casatestdata_path_for_tier};
use casa_types::{ScalarValue, Value, measures::frequency::FrequencyRef};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};

const DATASET: &str = "measurementset/alma/alma_ephemobj_icrs.ms";
const CASA_PREFIX_ENV: &str = "CASA_RS_T41_CASA_PREFIX";
const PRODUCTS: [&str; 5] = [".psf", ".residual", ".model", ".image", ".sumwt"];
const SELECTED_SAMPLE_COUNT: u64 = 1_620 * 1_024 * 2;
const MVC_MS_ENV: &str = "CASA_RS_T41_MVC_MS";
const MVC_CASA_PREFIX_ENV: &str = "CASA_RS_T41_MVC_CASA_PREFIX";
const MVC_RUST_PREFIX_ENV: &str = "CASA_RS_T41_MVC_RUST_PREFIX";
const MVC_TURNAROUND_PREFIX_ENV: &str = "CASA_RS_T41_MVC_TURNAROUND_PREFIX";
const MVC_SELECTED_SAMPLE_COUNT: u64 = 1_620 * (1_024 + 256) * 2;
// The T41 cubesource gate below already accepts this direction-world bound.
const DIRECTION_WORLD_TOLERANCE_RAD: f64 = 1.0e-10;
const COORDINATE_COEFFICIENT_TOLERANCE: f64 = 1.0e-12;
// The selected-range gate above predeclares this Measures conversion bound.
const SPECTRAL_WCS_TOLERANCE_HZ: f64 = 5.0;
const MVC_PUBLIC_PRODUCTS: [&str; 15] = [
    ".psf.tt0",
    ".psf.tt1",
    ".psf.tt2",
    ".residual.tt0",
    ".residual.tt1",
    ".model.tt0",
    ".model.tt1",
    ".image.tt0",
    ".image.tt1",
    ".sumwt.tt0",
    ".sumwt.tt1",
    ".sumwt.tt2",
    ".mask",
    ".alpha",
    ".alpha.error",
];

#[test]
#[ignore = "requires the representative T41 MS and frozen CASA MVC spectral coordinates"]
fn t41_mvc_selected_spectral_range_matches_casa_edge_topology() -> Result<(), Box<dyn Error>> {
    set_production_io_environment();
    let measurement_set = MeasurementSet::open(required_table(MVC_MS_ENV)?)?;
    let row_selection =
        measurement_set.selected_observation_row_selection(&[0, 1], Some(&[1]), None, None)?;
    let mut first_time_mjd_seconds = None;
    let selection_io = MsSelectionIoBudget {
        available_bytes: 64 << 20,
        maximum_live_blocks: 2,
        requested_bytes_per_row: SelectedObservationRow::STORAGE_BYTES_PER_ROW,
        storage_alignment_rows: None,
    };
    measurement_set.visit_selected_observation_rows(&row_selection, selection_io, |row| {
        first_time_mjd_seconds.get_or_insert(row.time_mjd_seconds());
    })?;
    let first_time_mjd_seconds = first_time_mjd_seconds.ok_or("empty T41 selection")?;
    let engine = casa_ms::derived::engine::MsCalEngine::new(&measurement_set)?;
    let ephemeris = SelectedObservationEphemeris::tracked_fields(
        &measurement_set,
        [1],
        SelectedObservationContentBudget::new(64 << 20, 2, 4).reference_data_budget(),
    )?;
    let phase =
        engine.ephemeris_direction_j2000(first_time_mjd_seconds, 1, "TRACKFIELD", &ephemeris)?;
    let range = measurement_set.selected_observation_spectral_range(
        &row_selection,
        &[
            SpectralWindowSelection::new(0, (0..1_024).collect()),
            SpectralWindowSelection::new(1, (0..256).collect()),
        ],
        FrequencyRef::TOPO,
        FrequencyRef::LSRK,
        1,
        first_time_mjd_seconds,
        phase,
        Some(&ephemeris),
        &engine,
        selection_io,
    )?;
    let [low_hz, high_hz] = range.selected_edges_hz();
    let [reference_low_hz, reference_high_hz] = range.reference_edges_hz();
    let increment_hz = (high_hz - low_hz) / 40.0;
    let first_centre_hz = low_hz.max(reference_low_hz) + increment_hz / 2.0;
    let public_reference_hz = first_centre_hz + 19.5 * increment_hz;
    eprintln!(
        "t41_mvc_range low={low_hz:.17} high={high_hz:.17} reference_low={reference_low_hz:.17} reference_high={reference_high_hz:.17} first_centre={first_centre_hz:.17} public_reference={public_reference_hz:.17} rows={} evaluations={}",
        range.measurements().selected_rows(),
        range.measurements().edge_evaluations(),
    );

    // The current Rust Measures transform follows CASA's selected-row extrema
    // algorithm and edge/centre topology. Its high-edge conversion differs by
    // 4.22 Hz on this frozen observation, which this evidence gate bounds
    // without changing either implementation's coordinates.
    assert!((low_hz - 230_388_238_202.374_33).abs() <= 5.0);
    assert!((high_hz - 235_307_541_333.341_28).abs() <= 5.0);
    assert!((first_centre_hz - 230_449_729_492.188_84).abs() <= 5.0);
    assert!((public_reference_hz - 232_847_889_768.535_16).abs() <= 5.0);
    assert_eq!(range.measurements().selected_rows(), 3_240);
    assert_eq!(range.measurements().edge_evaluations(), 120);
    Ok(())
}

#[test]
#[ignore = "requires the representative T41 MS for a mode-faithful PB/MVC turnaround"]
fn t41_mvc_primary_beam_turnaround_completes_on_full_selected_data() -> Result<(), Box<dyn Error>> {
    let source = required_table(MVC_MS_ENV)?;
    let rust_prefix = output_prefix_for(MVC_TURNAROUND_PREFIX_ENV, &[".psf.tt0"])?;
    let staging = tempfile::tempdir()?;
    let measurement_set = staging.path().join("alma_ephemobj_icrs.ms");
    copy_tree(&source, &measurement_set)?;
    casa_ms::initialize_measurement_set_owner_manifest(&measurement_set)?;
    set_production_io_environment();

    let mut request = mvc_request(measurement_set, rust_prefix);
    request.image_size = 256;
    let result = execute_continuum(request)?;
    assert_eq!(
        result
            .outcome
            .output
            .scientific
            .normal_state()
            .sample_count(),
        MVC_SELECTED_SAMPLE_COUNT,
    );
    assert_eq!(result.minor_iterations, 1);
    assert_eq!(result.outcome.output.major_cycle_count, 2);
    Ok(())
}

#[test]
#[ignore = "requires slow-parity casatestdata and matching frozen CASA T41 products"]
fn t41_tracked_cubesource_matches_casa_geometry_and_dirty_products() -> Result<(), Box<dyn Error>> {
    let source = casatestdata_path_for_tier(CasaTestDataTier::SlowParity, DATASET)
        .ok_or("slow-parity casatestdata root is unavailable")?;
    let casa_prefix = PathBuf::from(
        std::env::var_os(CASA_PREFIX_ENV).ok_or("CASA_RS_T41_CASA_PREFIX is not set")?,
    );
    let staging = tempfile::tempdir()?;
    let measurement_set = staging.path().join("alma_ephemobj_icrs.ms");
    copy_tree(&source, &measurement_set)?;
    casa_ms::initialize_measurement_set_owner_manifest(&measurement_set)?;
    set_production_io_environment();
    let rust_prefix = staging.path().join("rust-uranus-cubesource");

    let result = execute_continuum(request(measurement_set, rust_prefix.clone()))?;
    assert_eq!(
        result
            .outcome
            .output
            .scientific
            .normal_state()
            .sample_count(),
        SELECTED_SAMPLE_COUNT,
        "production traversal must retain all 1,024 channels and both parallel hands",
    );

    assert_matching_wcs(&rust_prefix, &casa_prefix)?;
    let casa_primary_beam = read_product(&casa_prefix, ".pb")?;
    let mut failures = Vec::new();
    for suffix in PRODUCTS {
        let rust = read_product(&rust_prefix, suffix)?;
        let casa = read_product(&casa_prefix, suffix)?;
        let expected_shape = if suffix == ".sumwt" {
            [1, 1, 1, 16]
        } else {
            [512, 512, 1, 16]
        };
        assert_eq!(rust.shape, expected_shape, "Rust {suffix} shape");
        assert_eq!(rust.shape, casa.shape, "CASA and Rust {suffix} shape");
        if matches!(suffix, ".residual" | ".image") {
            assert_eq!(
                casa.valid, casa_primary_beam.valid,
                "CASA {suffix} validity is exactly its primary-beam blanking support; broader PB blanking remains owned by T47/#533"
            );
        } else if rust.valid != casa.valid {
            failures.push(format!("{suffix} validity/support differs"));
        }
        let common_valid = rust
            .valid
            .iter()
            .zip(&casa.valid)
            .map(|(rust, casa)| *rust && *casa)
            .collect::<Vec<_>>();
        let nrms = normalized_rms(&rust.values, &casa.values, &common_valid);
        let rust_stats = statistics(&rust.values, &common_valid);
        let casa_stats = statistics(&casa.values, &common_valid);
        eprintln!(
            "t41_casa_parity product={suffix} nrms={nrms:.9e} rust_peak={} casa_peak={}",
            rust_stats.maximum, casa_stats.maximum,
        );
        if nrms > 0.001 {
            failures.push(format!("{suffix} normalized RMS {nrms:.6e} exceeds 0.1%"));
        }
        if matches!(suffix, ".psf" | ".residual") {
            if relative_difference(rust_stats.maximum, casa_stats.maximum) > 0.001 {
                failures.push(format!(
                    "{suffix} peak flux differs: Rust {} CASA {}",
                    rust_stats.maximum, casa_stats.maximum,
                ));
            }
            if rust_stats.maximum_position != casa_stats.maximum_position {
                failures.push(format!(
                    "{suffix} peak position differs: Rust {} CASA {}",
                    rust_stats.maximum_position, casa_stats.maximum_position,
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

#[test]
#[ignore = "requires the representative T41 MS and the one frozen CASA multi-SPW MVC oracle"]
fn t41_multi_spw_mvc_matches_casa_taylor_products() -> Result<(), Box<dyn Error>> {
    let source = required_table(MVC_MS_ENV)?;
    let casa_prefix = required_prefix(MVC_CASA_PREFIX_ENV, ".psf.tt0")?;
    let rust_prefix = output_prefix(MVC_RUST_PREFIX_ENV)?;
    let staging = tempfile::tempdir()?;
    let measurement_set = staging.path().join("alma_ephemobj_icrs.ms");
    copy_tree(&source, &measurement_set)?;
    casa_ms::initialize_measurement_set_owner_manifest(&measurement_set)?;
    set_production_io_environment();

    let started = std::time::Instant::now();
    let result = execute_continuum(mvc_request(measurement_set, rust_prefix.clone()))?;
    let production_wall = started.elapsed();
    eprintln!(
        "t41_mvc_serial_production_wall_seconds={:.9}",
        production_wall.as_secs_f64()
    );
    assert!(
        production_wall.as_secs_f64() <= 10.474_082,
        "T41 serial MVC production wall {:.9} s exceeds 2x frozen CASA",
        production_wall.as_secs_f64()
    );
    assert_eq!(
        result
            .outcome
            .output
            .scientific
            .normal_state()
            .sample_count(),
        MVC_SELECTED_SAMPLE_COUNT,
        "MVC traversal must retain every selected sample from SPWs 0 and 1",
    );
    assert_eq!(result.minor_iterations, 1);
    assert!(
        result.outcome.output.major_cycle_count >= 2,
        "MVC CLEAN must feed a Taylor model through more than one channel-major cycle"
    );
    assert_eq!(
        result.product_names,
        MVC_PUBLIC_PRODUCTS.map(str::to_string),
        "MVC must retain the existing public MT-MFS Taylor topology",
    );

    assert_casa_mvc_cube_topology(&casa_prefix)?;
    let mut failures = compare_mvc_wcs(&rust_prefix, &casa_prefix)?;
    for suffix in MVC_PUBLIC_PRODUCTS {
        let rust = read_product(&rust_prefix, suffix)?;
        let casa = read_product(&casa_prefix, suffix)?;
        let expected_shape = if suffix.starts_with(".sumwt.") {
            vec![1, 1, 1, 1]
        } else {
            vec![512, 512, 1, 1]
        };
        if rust.shape != expected_shape {
            failures.push(format!("{suffix} Rust shape is {:?}", rust.shape));
            continue;
        }
        if rust.shape != casa.shape {
            failures.push(format!(
                "{suffix} shape differs: Rust {:?} CASA {:?}",
                rust.shape, casa.shape
            ));
            continue;
        }
        if rust.valid != casa.valid {
            failures.push(format!("{suffix} validity/support differs"));
        }
        let common_valid = rust
            .valid
            .iter()
            .zip(&casa.valid)
            .map(|(rust, casa)| *rust && *casa)
            .collect::<Vec<_>>();
        let nrms = normalized_rms(&rust.values, &casa.values, &common_valid);
        let rust_stats = statistics(&rust.values, &common_valid);
        let casa_stats = statistics(&casa.values, &common_valid);
        eprintln!(
            "t41_mvc_casa_parity product={suffix} nrms={nrms:.9e} rust_peak={} casa_peak={} rust_peak_pixel={} casa_peak_pixel={}",
            rust_stats.maximum,
            casa_stats.maximum,
            rust_stats.maximum_position,
            casa_stats.maximum_position,
        );
        if nrms > 0.001 {
            failures.push(format!("{suffix} normalized RMS {nrms:.6e} exceeds 0.1%"));
        }
        if matches!(suffix, ".psf.tt0" | ".residual.tt0") {
            if relative_difference(rust_stats.maximum, casa_stats.maximum) > 0.001 {
                failures.push(format!(
                    "{suffix} peak flux differs: Rust {} CASA {}",
                    rust_stats.maximum, casa_stats.maximum,
                ));
            }
            if rust_stats.maximum_position != casa_stats.maximum_position {
                failures.push(format!(
                    "{suffix} peak position differs: Rust {} CASA {}",
                    rust_stats.maximum_position, casa_stats.maximum_position,
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

#[test]
#[ignore = "requires existing Rust MVC products and the frozen CASA MVC oracle"]
fn t41_existing_mvc_products_match_frozen_casa_metadata() -> Result<(), Box<dyn Error>> {
    let rust_prefix = required_prefix(MVC_RUST_PREFIX_ENV, ".residual.tt0")?;
    let casa_prefix = required_prefix(MVC_CASA_PREFIX_ENV, ".residual.tt0")?;
    let failures = compare_mvc_wcs(&rust_prefix, &casa_prefix)?;
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

fn request(measurement_set: PathBuf, image_name: PathBuf) -> ContinuumImagingRequest {
    ContinuumImagingRequest {
        measurement_set,
        image_name,
        image_size: 512,
        facets: 1,
        cell_arcsec: 0.1,
        phase_center_field: None,
        phase_center: Some("TRACKFIELD".to_string()),
        outlier_file: None,
        field_ids: Some(vec![1]),
        uv_range: None,
        intent: None,
        data_description: None,
        spectral_window: Some("0".to_string()),
        channel_start: None,
        channel_count: None,
        spectral_mode: SpectralImagingMode::CubeSource {
            axis: CubeAxisConfig {
                outframe: FrequencyRef::REST,
                start: Some(CubeAxisValue::Channel(0)),
                width: Some(CubeAxisValue::Channel(64)),
                ..CubeAxisConfig::default()
            },
            output_channels: Some(16),
        },
        continuum_subtraction: None,
        data_column: Some("DATA".to_string()),
        polarizations: vec![PolarizationCoordinate::StokesI],
        algorithm: ContinuumAlgorithm::Dirty,
        weighting: ContinuumWeighting::Natural,
        iterations: 0,
        cycle_iterations: 1,
        hogbom_iteration_accounting: HogbomIterationAccounting::Strict,
        maximum_major_cycles: None,
        noise_sigma: None,
        cycle_factor: 1.0,
        minimum_psf_fraction: 0.1,
        maximum_psf_fraction: 0.8,
        gain: 0.1,
        threshold_jy: 0.0,
        psf_cutoff: casa_imaging_products::DEFAULT_PSF_CUTOFF,
        primary_beam_cutoff: 0.1,
        normalization: casa_imaging_model::ProductNormalization::UnitResponse,
        beam_policy: ContinuumBeamPolicy::PerPlane,
        mask: ContinuumMask::FullPlane,
        save_model_column: false,
        save_continuum_residual: false,
        write_primary_beam: false,
        pbcor: false,
        task_requirements: vec![
            TaskRequirement::SpectralCubeSource,
            TaskRequirement::SerialCpu,
        ],
        resource_policy: casa_imaging_runtime::ResourcePolicy::Explicit(
            casa_imaging_runtime::ResourceOverride {
                workers: Some(1),
                ..Default::default()
            },
        ),
    }
}

fn mvc_request(measurement_set: PathBuf, image_name: PathBuf) -> ContinuumImagingRequest {
    ContinuumImagingRequest {
        measurement_set,
        image_name,
        image_size: 512,
        facets: 1,
        cell_arcsec: 0.1,
        phase_center_field: None,
        phase_center: Some("TRACKFIELD".to_string()),
        outlier_file: None,
        field_ids: Some(vec![1]),
        uv_range: None,
        intent: None,
        data_description: None,
        spectral_window: Some("0,1".to_string()),
        channel_start: None,
        channel_count: None,
        spectral_mode: SpectralImagingMode::MtmfsViaCube {
            axis: CubeAxisConfig::default(),
            output_channels: Some(40),
        },
        continuum_subtraction: None,
        data_column: Some("DATA".to_string()),
        polarizations: vec![PolarizationCoordinate::StokesI],
        algorithm: ContinuumAlgorithm::Mtmfs {
            terms: 2,
            scales_px: vec![0.0],
            small_scale_bias: 0.0,
        },
        weighting: ContinuumWeighting::Natural,
        iterations: 1,
        cycle_iterations: 1,
        hogbom_iteration_accounting: HogbomIterationAccounting::Strict,
        maximum_major_cycles: None,
        noise_sigma: None,
        cycle_factor: 1.0,
        minimum_psf_fraction: 0.05,
        maximum_psf_fraction: 0.8,
        gain: 0.1,
        threshold_jy: 0.0,
        psf_cutoff: casa_imaging_products::DEFAULT_PSF_CUTOFF,
        primary_beam_cutoff: 0.1,
        normalization: casa_imaging_model::ProductNormalization::UnitResponse,
        beam_policy: ContinuumBeamPolicy::Common,
        mask: ContinuumMask::FullPlane,
        save_model_column: false,
        save_continuum_residual: false,
        write_primary_beam: false,
        pbcor: false,
        task_requirements: vec![
            TaskRequirement::SpectralMtmfsViaCube,
            TaskRequirement::SerialCpu,
        ],
        resource_policy: casa_imaging_runtime::ResourcePolicy::Explicit(
            casa_imaging_runtime::ResourceOverride {
                workers: Some(1),
                ..Default::default()
            },
        ),
    }
}

struct Statistics {
    maximum: f64,
    maximum_position: usize,
}

fn statistics(values: &[f32], valid: &[bool]) -> Statistics {
    let mut maximum = f32::NEG_INFINITY;
    let mut maximum_position = 0;
    for (position, (value, valid)) in values.iter().zip(valid).enumerate() {
        if *valid && *value > maximum {
            maximum = *value;
            maximum_position = position;
        }
    }
    Statistics {
        maximum: f64::from(maximum),
        maximum_position,
    }
}

fn normalized_rms(rust: &[f32], casa: &[f32], valid: &[bool]) -> f64 {
    let (error, reference) = rust
        .iter()
        .zip(casa)
        .zip(valid)
        .filter(|(_, valid)| **valid)
        .fold((0.0, 0.0), |(error, reference), ((actual, expected), _)| {
            let actual = f64::from(*actual);
            let expected = f64::from(*expected);
            (
                error + (actual - expected).powi(2),
                reference + expected.powi(2),
            )
        });
    (error / reference.max(f64::MIN_POSITIVE)).sqrt()
}

fn relative_difference(actual: f64, expected: f64) -> f64 {
    (actual - expected).abs() / expected.abs().max(f64::MIN_POSITIVE)
}

struct Product {
    shape: Vec<usize>,
    values: Vec<f32>,
    valid: Vec<bool>,
}

fn read_product(prefix: &Path, suffix: &str) -> Result<Product, Box<dyn Error>> {
    let image = PagedImage::<f32>::open(PathBuf::from(format!("{}{suffix}", prefix.display())))?;
    let shape = image.shape().to_vec();
    let values = image
        .get_slice(&vec![0; shape.len()], &shape)?
        .iter()
        .copied()
        .collect();
    let valid = image
        .get_mask_slice(&vec![0; shape.len()], &shape, &vec![1; shape.len()])?
        .map_or_else(
            || vec![true; shape.iter().product()],
            |mask| mask.iter().copied().collect(),
        );
    Ok(Product {
        shape,
        values,
        valid,
    })
}

fn assert_matching_wcs(rust_prefix: &Path, casa_prefix: &Path) -> Result<(), Box<dyn Error>> {
    let rust =
        PagedImage::<f32>::open(PathBuf::from(format!("{}.residual", rust_prefix.display())))?;
    let casa =
        PagedImage::<f32>::open(PathBuf::from(format!("{}.residual", casa_prefix.display())))?;
    for pixel in [[256.0, 256.0, 0.0, 0.0], [256.0, 256.0, 0.0, 15.0]] {
        let rust_world = rust.coordinates().to_world(&pixel)?;
        let casa_world = casa.coordinates().to_world(&pixel)?;
        for axis in 0..2 {
            assert!(
                (rust_world[axis] - casa_world[axis]).abs() <= 1.0e-10,
                "tracked direction WCS axis {axis} differs at {pixel:?}"
            );
        }
        let spectral_tolerance_hz = casa_world[3].abs().max(1.0) * 2.0e-12;
        assert!(
            (rust_world[3] - casa_world[3]).abs() <= spectral_tolerance_hz,
            "REST spectral WCS differs at {pixel:?}: Rust {} CASA {}",
            rust_world[3],
            casa_world[3],
        );
    }
    for image in [&rust, &casa] {
        let CoordinateModel::Spectral(spectral) = image.coordinates().coordinate(2) else {
            return Err("T41 product has no spectral coordinate".into());
        };
        assert_eq!(spectral.world_frequency_ref(), FrequencyRef::REST);
    }
    Ok(())
}

fn assert_casa_mvc_cube_topology(prefix: &Path) -> Result<(), Box<dyn Error>> {
    for (suffix, expected) in [
        (".psf", vec![512, 512, 1, 40]),
        (".residual", vec![512, 512, 1, 40]),
        (".model", vec![512, 512, 1, 40]),
        (".sumwt", vec![1, 1, 1, 40]),
    ] {
        assert_eq!(
            read_product(prefix, suffix)?.shape,
            expected,
            "CASA {suffix}"
        );
    }
    let sumwt = read_product(prefix, ".sumwt")?;
    let supported = sumwt
        .values
        .iter()
        .enumerate()
        .filter_map(|(channel, weight)| weight.is_finite().then_some((channel, *weight)))
        .filter_map(|(channel, weight)| (weight > 0.0).then_some(channel))
        .collect::<Vec<_>>();
    eprintln!("t41_mvc_casa_supported_channels={supported:?}");
    assert!(
        supported.len() >= 16,
        "CASA supported MVC channel planes: {supported:?}"
    );
    Ok(())
}

fn compare_mvc_wcs(rust_prefix: &Path, casa_prefix: &Path) -> Result<Vec<String>, Box<dyn Error>> {
    let rust = PagedImage::<f32>::open(PathBuf::from(format!(
        "{}.residual.tt0",
        rust_prefix.display()
    )))?;
    let casa = PagedImage::<f32>::open(PathBuf::from(format!(
        "{}.residual.tt0",
        casa_prefix.display()
    )))?;
    let rust_coordinates = rust.coordinates();
    let casa_coordinates = casa.coordinates();
    let pixel = [256.0, 256.0, 0.0, 0.0];
    let rust_world = rust_coordinates.to_world(&pixel)?;
    let casa_world = casa_coordinates.to_world(&pixel)?;
    eprintln!(
        "t41_mvc_wcs rust_direction_rad={:?} casa_direction_rad={:?} rust_spectral_hz={} casa_spectral_hz={}",
        &rust_world[..2],
        &casa_world[..2],
        rust_world[3],
        casa_world[3],
    );
    let mut failures = Vec::new();
    if rust.units() != "Jy/beam" {
        failures.push(format!(
            "Rust residual.tt0 brightness semantics differ: expected \"Jy/beam\", found {:?}",
            rust.units(),
        ));
    }
    if !casa.units().is_empty() {
        failures.push(format!(
            "frozen CASA residual.tt0 brightness serialization differs: expected an empty label, found {:?}",
            casa.units()
        ));
    }
    let expected_coordinate_types = [
        CoordinateType::Direction,
        CoordinateType::Stokes,
        CoordinateType::Spectral,
    ];
    for (label, coordinates) in [("Rust", rust_coordinates), ("CASA", casa_coordinates)] {
        let coordinate_types = (0..coordinates.n_coordinates())
            .map(|index| coordinates.coordinate(index).coordinate_type())
            .collect::<Vec<_>>();
        if coordinate_types != expected_coordinate_types {
            failures.push(format!(
                "{label} coordinate topology differs: {coordinate_types:?}"
            ));
        }
    }
    if rust_coordinates.n_coordinates() != casa_coordinates.n_coordinates() {
        failures.push(format!(
            "coordinate count differs: Rust {} CASA {}",
            rust_coordinates.n_coordinates(),
            casa_coordinates.n_coordinates()
        ));
    } else {
        for coordinate_index in 0..rust_coordinates.n_coordinates() {
            let rust_coordinate = rust_coordinates.coordinate(coordinate_index);
            let casa_coordinate = casa_coordinates.coordinate(coordinate_index);
            if rust_coordinate.coordinate_type() != casa_coordinate.coordinate_type() {
                failures.push(format!(
                    "coordinate {coordinate_index} type differs: Rust {} CASA {}",
                    rust_coordinate.coordinate_type(),
                    casa_coordinate.coordinate_type()
                ));
                continue;
            }
            if rust_coordinate.axis_names() != casa_coordinate.axis_names() {
                failures.push(format!("coordinate {coordinate_index} axis names differ"));
            }
            if rust_coordinate.axis_units() != casa_coordinate.axis_units() {
                failures.push(format!("coordinate {coordinate_index} axis units differ"));
            }
            if rust_coordinate.reference_pixel() != casa_coordinate.reference_pixel() {
                failures.push(format!(
                    "coordinate {coordinate_index} reference pixel differs: Rust {:?} CASA {:?}",
                    rust_coordinate.reference_pixel(),
                    casa_coordinate.reference_pixel(),
                ));
            }
            match (rust_coordinate, casa_coordinate) {
                (
                    CoordinateModel::Direction(rust_direction),
                    CoordinateModel::Direction(casa_direction),
                ) => compare_mvc_direction_coordinate(
                    coordinate_index,
                    rust_direction,
                    casa_direction,
                    &mut failures,
                ),
                (
                    CoordinateModel::Stokes(rust_stokes),
                    CoordinateModel::Stokes(casa_stokes),
                ) => {
                    if rust_stokes.stokes() != casa_stokes.stokes() {
                        failures.push(format!(
                            "coordinate {coordinate_index} Stokes values differ"
                        ));
                    }
                    compare_exact_coordinate_values(
                        coordinate_index,
                        "reference value",
                        &rust_coordinate.reference_value(),
                        &casa_coordinate.reference_value(),
                        &mut failures,
                    );
                    compare_exact_coordinate_values(
                        coordinate_index,
                        "increment",
                        &rust_coordinate.increment(),
                        &casa_coordinate.increment(),
                        &mut failures,
                    );
                }
                (
                    CoordinateModel::Spectral(rust_spectral),
                    CoordinateModel::Spectral(casa_spectral),
                ) => compare_mvc_spectral_coordinate(
                    coordinate_index,
                    rust_spectral,
                    casa_spectral,
                    &mut failures,
                ),
                _ => failures.push(format!(
                    "coordinate {coordinate_index} is outside the T41 Direction/Stokes/Spectral contract"
                )),
            }
        }
    }
    compare_mvc_sampled_world_coordinates(&rust, &casa, &mut failures)?;
    compare_rust_product_provenance(&rust, &mut failures);
    compare_frozen_mvc_manifest(casa_prefix, &casa, &mut failures)?;
    Ok(failures)
}

fn compare_mvc_direction_coordinate(
    coordinate: usize,
    rust: &DirectionCoordinate,
    casa: &DirectionCoordinate,
    failures: &mut Vec<String>,
) {
    compare_coordinate_values(
        coordinate,
        "reference value",
        &rust.reference_value(),
        &casa.reference_value(),
        DIRECTION_WORLD_TOLERANCE_RAD,
        failures,
    );
    compare_coordinate_values(
        coordinate,
        "increment",
        &rust.increment(),
        &casa.increment(),
        COORDINATE_COEFFICIENT_TOLERANCE,
        failures,
    );
    if rust.direction_ref() != casa.direction_ref() {
        failures.push(format!(
            "coordinate {coordinate} direction frame differs: Rust {:?} CASA {:?}",
            rust.direction_ref(),
            casa.direction_ref(),
        ));
    }
    if rust.projection().projection_type() != casa.projection().projection_type() {
        failures.push(format!(
            "coordinate {coordinate} projection differs: Rust {:?} CASA {:?}",
            rust.projection().projection_type(),
            casa.projection().projection_type(),
        ));
    } else if rust.projection().parameters() != casa.projection().parameters()
        && !(rust.projection().projection_type() == ProjectionType::SIN
            && is_zero_sin_projection(rust.projection().parameters())
            && is_zero_sin_projection(casa.projection().parameters()))
    {
        failures.push(format!(
            "coordinate {coordinate} projection parameters differ: Rust {:?} CASA {:?}",
            rust.projection().parameters(),
            casa.projection().parameters(),
        ));
    }
    if rust.pc_matrix() != casa.pc_matrix() {
        failures.push(format!(
            "coordinate {coordinate} direction PC matrix differs: Rust {:?} CASA {:?}",
            rust.pc_matrix(),
            casa.pc_matrix(),
        ));
    }
    if (rust.longpole() - casa.longpole()).abs() > COORDINATE_COEFFICIENT_TOLERANCE {
        failures.push(format!(
            "coordinate {coordinate} direction LONGPOLE differs: Rust {} CASA {}",
            rust.longpole(),
            casa.longpole(),
        ));
    }
    if (rust.latpole() - casa.latpole()).abs() > COORDINATE_COEFFICIENT_TOLERANCE
        && !equivalent_zenithal_sin_latpoles(rust, casa)
    {
        failures.push(format!(
            "coordinate {coordinate} direction LATPOLE differs: Rust {} CASA {}",
            rust.latpole(),
            casa.latpole(),
        ));
    }
}

fn is_zero_sin_projection(parameters: &[f64]) -> bool {
    parameters.is_empty() || (parameters.len() == 2 && parameters == [0.0, 0.0])
}

fn equivalent_zenithal_sin_latpoles(
    rust: &DirectionCoordinate,
    casa: &DirectionCoordinate,
) -> bool {
    if rust.projection().projection_type() != ProjectionType::SIN
        || casa.projection().projection_type() != ProjectionType::SIN
        || !is_zero_sin_projection(rust.projection().parameters())
        || !is_zero_sin_projection(casa.projection().parameters())
    {
        return false;
    }
    let accepted_latpole = |coordinate: &DirectionCoordinate| {
        let reference_latitude = coordinate.reference_value()[1];
        (coordinate.latpole() - std::f64::consts::FRAC_PI_2).abs()
            <= COORDINATE_COEFFICIENT_TOLERANCE
            || (coordinate.latpole() - reference_latitude).abs() <= COORDINATE_COEFFICIENT_TOLERANCE
    };
    accepted_latpole(rust) && accepted_latpole(casa)
}

fn compare_mvc_spectral_coordinate(
    coordinate: usize,
    rust: &SpectralCoordinate,
    casa: &SpectralCoordinate,
    failures: &mut Vec<String>,
) {
    compare_coordinate_values(
        coordinate,
        "reference value",
        &rust.reference_value(),
        &casa.reference_value(),
        SPECTRAL_WCS_TOLERANCE_HZ,
        failures,
    );
    compare_coordinate_values(
        coordinate,
        "increment",
        &rust.increment(),
        &casa.increment(),
        SPECTRAL_WCS_TOLERANCE_HZ,
        failures,
    );
    if rust.frequency_ref() != FrequencyRef::LSRK
        || casa.frequency_ref() != FrequencyRef::LSRK
        || rust.frequency_ref() != casa.frequency_ref()
    {
        failures.push(format!(
            "coordinate {coordinate} spectral storage frame differs: Rust {:?} CASA {:?}",
            rust.frequency_ref(),
            casa.frequency_ref(),
        ));
    }
    if rust.world_frequency_ref() != FrequencyRef::LSRK
        || casa.world_frequency_ref() != FrequencyRef::LSRK
        || rust.world_frequency_ref() != casa.world_frequency_ref()
    {
        failures.push(format!(
            "coordinate {coordinate} effective spectral frame differs: Rust {:?} CASA {:?}",
            rust.world_frequency_ref(),
            casa.world_frequency_ref(),
        ));
    }
}

fn compare_mvc_sampled_world_coordinates(
    rust: &PagedImage<f32>,
    casa: &PagedImage<f32>,
    failures: &mut Vec<String>,
) -> Result<(), Box<dyn Error>> {
    let last_x = (rust.shape()[0] - 1) as f64;
    let last_y = (rust.shape()[1] - 1) as f64;
    for pixel in [
        [256.0, 256.0, 0.0, 0.0],
        [0.0, 0.0, 0.0, 0.0],
        [last_x, last_y, 0.0, 0.0],
        [0.0, last_y, 0.0, 0.0],
        [last_x, 0.0, 0.0, 0.0],
    ] {
        let rust_world = rust.coordinates().to_world(&pixel)?;
        let casa_world = casa.coordinates().to_world(&pixel)?;
        for axis in 0..2 {
            if (rust_world[axis] - casa_world[axis]).abs() > DIRECTION_WORLD_TOLERANCE_RAD {
                failures.push(format!(
                    "sampled direction WCS axis {axis} differs at {pixel:?}: Rust {} CASA {}",
                    rust_world[axis], casa_world[axis],
                ));
            }
        }
        if (rust_world[3] - casa_world[3]).abs() > SPECTRAL_WCS_TOLERANCE_HZ {
            failures.push(format!(
                "sampled spectral WCS differs at {pixel:?}: Rust {} Hz CASA {} Hz",
                rust_world[3], casa_world[3],
            ));
        }
    }
    Ok(())
}

fn compare_rust_product_provenance(image: &PagedImage<f32>, failures: &mut Vec<String>) {
    let misc_info = image.misc_info();
    if record_string(&misc_info, "casars_imager_role") != Some("residual") {
        failures.push("Rust residual.tt0 product role provenance differs".to_string());
    }
    for field in [
        "casa_rs_planned_product_identity",
        "casa_rs_observed_product_identity",
    ] {
        if !record_string(&misc_info, field).is_some_and(is_sha256_hex) {
            failures.push(format!("Rust residual.tt0 {field} provenance is invalid"));
        }
    }
}

fn record_string<'a>(record: &'a casa_types::RecordValue, name: &str) -> Option<&'a str> {
    match record.get(name) {
        Some(Value::Scalar(ScalarValue::String(value))) => Some(value),
        _ => None,
    }
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn compare_coordinate_values(
    coordinate: usize,
    field: &str,
    rust: &[f64],
    casa: &[f64],
    tolerance: f64,
    failures: &mut Vec<String>,
) {
    if rust.len() != casa.len()
        || rust
            .iter()
            .zip(casa)
            .any(|(rust, casa)| (rust - casa).abs() > tolerance)
    {
        failures.push(format!(
            "coordinate {coordinate} {field} differs: Rust {rust:?} CASA {casa:?} tolerance {tolerance}"
        ));
    }
}

fn compare_exact_coordinate_values(
    coordinate: usize,
    field: &str,
    rust: &[f64],
    casa: &[f64],
    failures: &mut Vec<String>,
) {
    if rust != casa {
        failures.push(format!(
            "coordinate {coordinate} {field} differs: Rust {rust:?} CASA {casa:?}"
        ));
    }
}

fn compare_frozen_mvc_manifest(
    casa_prefix: &Path,
    casa: &PagedImage<f32>,
    failures: &mut Vec<String>,
) -> Result<(), Box<dyn Error>> {
    let manifest_path = casa_prefix
        .parent()
        .ok_or("CASA MVC prefix has no parent")?
        .join("manifest.json");
    let manifest: JsonValue = serde_json::from_slice(&fs::read(&manifest_path)?)?;
    if manifest["kind"] != "casa_rs_t41_multi_spw_mvc_oracle" {
        failures.push("CASA MVC manifest kind differs".to_string());
    }
    for (field, expected) in [
        ("casatasks_version", "6.7.6.14"),
        ("casatools_version", "6.7.6-14"),
        ("measurement_set", "/tmp/t41-alma-ephemobj-icrs.ms"),
    ] {
        if manifest[field].as_str() != Some(expected) {
            failures.push(format!("CASA MVC manifest {field} provenance differs"));
        }
    }
    let parameters = &manifest["parameters"];
    for (field, expected) in [
        ("field", "1"),
        ("spw", "0,1"),
        ("phasecenter", "TRACKFIELD"),
        ("specmode", "mvc"),
        ("outframe", "LSRK"),
        ("gridder", "standard"),
        ("deconvolver", "mtmfs"),
        ("stokes", "I"),
        ("weighting", "natural"),
    ] {
        if parameters[field].as_str() != Some(expected) {
            failures.push(format!("CASA MVC manifest parameter {field} differs"));
        }
    }
    for (field, expected) in [("nchan", 40), ("nterms", 2), ("niter", 1)] {
        if parameters[field].as_u64() != Some(expected) {
            failures.push(format!("CASA MVC manifest parameter {field} differs"));
        }
    }
    let recipe_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tools/perf/imager/experiments/issue527_t41_mvc_casa_oracle.py");
    let recipe_digest = format!("{:x}", Sha256::digest(fs::read(recipe_path)?));
    if manifest["recipe_sha256"].as_str() != Some(&recipe_digest) {
        failures.push("CASA MVC manifest recipe identity differs".to_string());
    }
    let product = &manifest["products"][".residual.tt0"];
    let shape = json_usize_array(&product["shape"])?;
    if shape != casa.shape() {
        failures.push(format!(
            "CASA MVC manifest shape differs: manifest {shape:?} image {:?}",
            casa.shape()
        ));
    }
    if product["brightness_unit"].as_str() != Some(casa.units()) {
        failures.push("CASA MVC manifest brightness unit differs".to_string());
    }
    let manifest_types = product["axis_coordinate_types"]
        .as_array()
        .ok_or("manifest axis_coordinate_types is not an array")?
        .iter()
        .map(|value| value.as_str().ok_or("manifest coordinate type is not text"))
        .collect::<Result<Vec<_>, _>>()?;
    let image_types = (0..casa.coordinates().n_coordinates())
        .flat_map(|index| {
            let coordinate = casa.coordinates().coordinate(index);
            std::iter::repeat_n(
                coordinate.coordinate_type().to_string(),
                coordinate.n_world_axes(),
            )
        })
        .collect::<Vec<_>>();
    if manifest_types != image_types {
        failures.push("CASA MVC manifest coordinate types differ".to_string());
    }
    let manifest_units = product["axis_units"]
        .as_array()
        .ok_or("manifest axis_units is not an array")?
        .iter()
        .map(|value| value.as_str().ok_or("manifest axis unit is not text"))
        .collect::<Result<Vec<_>, _>>()?;
    let image_units = (0..casa.coordinates().n_coordinates())
        .flat_map(|index| casa.coordinates().coordinate(index).axis_units())
        .collect::<Vec<_>>();
    if manifest_units != image_units {
        failures.push("CASA MVC manifest axis units differ".to_string());
    }
    let image_reference_pixels = (0..casa.coordinates().n_coordinates())
        .flat_map(|index| casa.coordinates().coordinate(index).reference_pixel())
        .collect::<Vec<_>>();
    let image_reference_values = (0..casa.coordinates().n_coordinates())
        .flat_map(|index| casa.coordinates().coordinate(index).reference_value())
        .collect::<Vec<_>>();
    let image_increments = (0..casa.coordinates().n_coordinates())
        .flat_map(|index| casa.coordinates().coordinate(index).increment())
        .collect::<Vec<_>>();
    compare_manifest_coordinate_record(
        product,
        "reference_pixel",
        "pixel",
        &image_reference_pixels,
        failures,
    )?;
    compare_manifest_coordinate_record(
        product,
        "reference_value",
        "world",
        &image_reference_values,
        failures,
    )?;
    compare_manifest_coordinate_record(product, "increment", "world", &image_increments, failures)?;
    Ok(())
}

fn compare_manifest_coordinate_record(
    product: &JsonValue,
    field: &str,
    coordinate_kind: &str,
    image_values: &[f64],
    failures: &mut Vec<String>,
) -> Result<(), String> {
    let record = &product[field];
    if record["ar_type"].as_str() != Some("absolute")
        || record["pw_type"].as_str() != Some(coordinate_kind)
    {
        failures.push(format!("CASA MVC manifest {field} topology differs"));
    }
    let manifest_values = json_f64_array(&record["numeric"])?;
    if manifest_values != image_values {
        failures.push(format!(
            "CASA MVC manifest {field} differs: manifest {manifest_values:?} image {image_values:?}"
        ));
    }
    Ok(())
}

fn json_usize_array(value: &JsonValue) -> Result<Vec<usize>, String> {
    value
        .as_array()
        .ok_or_else(|| "manifest value is not an array".to_string())?
        .iter()
        .map(|value| {
            value
                .as_u64()
                .ok_or_else(|| "manifest array value is not unsigned".to_string())
                .and_then(|value| usize::try_from(value).map_err(|error| error.to_string()))
        })
        .collect()
}

fn json_f64_array(value: &JsonValue) -> Result<Vec<f64>, String> {
    value
        .as_array()
        .ok_or_else(|| "manifest value is not an array".to_string())?
        .iter()
        .map(|value| {
            value
                .as_f64()
                .ok_or_else(|| "manifest array value is not numeric".to_string())
        })
        .collect()
}

fn required_table(name: &str) -> Result<PathBuf, Box<dyn Error>> {
    let path = PathBuf::from(std::env::var_os(name).ok_or_else(|| format!("{name} is not set"))?);
    if !path.is_dir() {
        return Err(format!("{name} does not name a table: {}", path.display()).into());
    }
    Ok(path)
}

fn required_prefix(name: &str, required_suffix: &str) -> Result<PathBuf, Box<dyn Error>> {
    let prefix = PathBuf::from(std::env::var_os(name).ok_or_else(|| format!("{name} is not set"))?);
    if !PathBuf::from(format!("{}{required_suffix}", prefix.display())).is_dir() {
        return Err(format!(
            "{name} has no {required_suffix} product at {}",
            prefix.display()
        )
        .into());
    }
    Ok(prefix)
}

fn output_prefix(name: &str) -> Result<PathBuf, Box<dyn Error>> {
    output_prefix_for(name, &MVC_PUBLIC_PRODUCTS)
}

fn output_prefix_for(name: &str, suffixes: &[&str]) -> Result<PathBuf, Box<dyn Error>> {
    let prefix = PathBuf::from(std::env::var_os(name).ok_or_else(|| format!("{name} is not set"))?);
    let parent = prefix.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    if suffixes
        .iter()
        .any(|suffix| PathBuf::from(format!("{}{suffix}", prefix.display())).exists())
    {
        return Err(format!("refusing to overwrite MVC products at {}", prefix.display()).into());
    }
    Ok(prefix)
}

fn copy_tree(source: &Path, destination: &Path) -> Result<(), Box<dyn Error>> {
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let target = destination.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_tree(&entry.path(), &target)?;
        } else {
            fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

fn set_production_io_environment() {
    // SAFETY: this ignored gate runs serially before any MeasurementSet is opened.
    unsafe {
        std::env::set_var("CASA_RS_IMAGING_SPILL_READ_BYTES_PER_SECOND", "1000000000");
        std::env::set_var("CASA_RS_IMAGING_SPILL_WRITE_BYTES_PER_SECOND", "1000000000");
        std::env::set_var("CASA_RS_IO_BACKEND", "mmap");
        std::env::set_var("CASA_RS_IO_MMAP_TILES", "true");
    }
}
