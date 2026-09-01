// SPDX-License-Identifier: LGPL-3.0-or-later

//! Focused T41 moving-source gate against a frozen CASA Uranus cube.

use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
};

use casa_coordinates::CoordinateModel;
use casa_images::PagedImage;
use casa_imaging_application::{
    ContinuumAlgorithm, ContinuumBeamPolicy, ContinuumImagingRequest, ContinuumMask,
    ContinuumWeighting, HogbomIterationAccounting, PolarizationCoordinate, SpectralImagingMode,
    TaskRequirement, execute_continuum,
};
use casa_ms::{CubeAxisConfig, CubeAxisValue, MeasurementSet};
use casa_test_support::{CasaTestDataTier, casatestdata_path_for_tier};
use casa_types::measures::frequency::FrequencyRef;

const DATASET: &str = "measurementset/alma/alma_ephemobj_icrs.ms";
const CASA_PREFIX_ENV: &str = "CASA_RS_T41_CASA_PREFIX";
const PRODUCTS: [&str; 5] = [".psf", ".residual", ".model", ".image", ".sumwt"];
const SELECTED_SAMPLE_COUNT: u64 = 1_620 * 1_024 * 2;
const MVC_MS_ENV: &str = "CASA_RS_T41_MVC_MS";
const MVC_CASA_PREFIX_ENV: &str = "CASA_RS_T41_MVC_CASA_PREFIX";
const MVC_RUST_PREFIX_ENV: &str = "CASA_RS_T41_MVC_RUST_PREFIX";
const MVC_SELECTED_SAMPLE_COUNT: u64 = 1_620 * (1_024 + 256 + 1_024 + 4_096) * 2;
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
#[ignore = "requires slow-parity casatestdata and matching frozen CASA T41 products"]
fn t41_tracked_cubesource_matches_casa_geometry_and_dirty_products() -> Result<(), Box<dyn Error>> {
    set_production_io_environment();
    let source = casatestdata_path_for_tier(CasaTestDataTier::SlowParity, DATASET)
        .ok_or("slow-parity casatestdata root is unavailable")?;
    let casa_prefix = PathBuf::from(
        std::env::var_os(CASA_PREFIX_ENV).ok_or("CASA_RS_T41_CASA_PREFIX is not set")?,
    );
    let staging = tempfile::tempdir()?;
    let measurement_set = staging.path().join("alma_ephemobj_icrs.ms");
    MeasurementSet::open(&source)?.save_as(&measurement_set)?;
    copy_attached_ephemerides(&source, &measurement_set)?;
    casa_ms::initialize_measurement_set_owner_manifest(&measurement_set)?;
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
    set_production_io_environment();
    let source = required_table(MVC_MS_ENV)?;
    let casa_prefix = required_prefix(MVC_CASA_PREFIX_ENV, ".psf.tt0")?;
    let rust_prefix = output_prefix(MVC_RUST_PREFIX_ENV)?;
    let staging = tempfile::tempdir()?;
    let measurement_set = staging.path().join("alma_ephemobj_icrs.ms");
    MeasurementSet::open(&source)?.save_as(&measurement_set)?;
    copy_attached_ephemerides(&source, &measurement_set)?;
    casa_ms::initialize_measurement_set_owner_manifest(&measurement_set)?;

    let result = execute_continuum(mvc_request(measurement_set, rust_prefix.clone()))?;
    assert_eq!(
        result
            .outcome
            .output
            .scientific
            .normal_state()
            .sample_count(),
        MVC_SELECTED_SAMPLE_COUNT,
        "MVC traversal must retain every selected sample from all four SPWs",
    );
    assert_eq!(result.minor_iterations, 4);
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
    }
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
        spectral_window: Some("0,1,2,3".to_string()),
        channel_start: None,
        channel_count: None,
        spectral_mode: SpectralImagingMode::MtmfsViaCube {
            axis: CubeAxisConfig::default(),
            output_channels: Some(16),
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
        iterations: 4,
        cycle_iterations: 2,
        hogbom_iteration_accounting: HogbomIterationAccounting::Strict,
        maximum_major_cycles: None,
        noise_sigma: None,
        cycle_factor: 1.0,
        minimum_psf_fraction: 0.05,
        maximum_psf_fraction: 0.8,
        gain: 0.1,
        threshold_jy: 0.0,
        psf_cutoff: casa_imaging_products::DEFAULT_PSF_CUTOFF,
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
        (".psf", vec![512, 512, 1, 16]),
        (".residual", vec![512, 512, 1, 16]),
        (".model", vec![512, 512, 1, 16]),
        (".sumwt", vec![1, 1, 1, 16]),
    ] {
        assert_eq!(
            read_product(prefix, suffix)?.shape,
            expected,
            "CASA {suffix}"
        );
    }
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
    let pixel = [256.0, 256.0, 0.0, 0.0];
    let rust_world = rust.coordinates().to_world(&pixel)?;
    let casa_world = casa.coordinates().to_world(&pixel)?;
    eprintln!(
        "t41_mvc_wcs rust_direction_rad={:?} casa_direction_rad={:?} rust_spectral_hz={} casa_spectral_hz={}",
        &rust_world[..2],
        &casa_world[..2],
        rust_world[3],
        casa_world[3],
    );
    let mut failures = Vec::new();
    for axis in 0..2 {
        if (rust_world[axis] - casa_world[axis]).abs() > 1.0e-10 {
            failures.push(format!(
                "tracked direction WCS axis {axis} differs: Rust {} CASA {}",
                rust_world[axis], casa_world[axis]
            ));
        }
    }
    let tolerance_hz = casa_world[3].abs().max(1.0) * 2.0e-12;
    if (rust_world[3] - casa_world[3]).abs() > tolerance_hz {
        failures.push(format!(
            "Taylor reference frequency differs: Rust {} Hz CASA {} Hz",
            rust_world[3], casa_world[3]
        ));
    }
    Ok(failures)
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
    let prefix = PathBuf::from(std::env::var_os(name).ok_or_else(|| format!("{name} is not set"))?);
    let parent = prefix.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    if MVC_PUBLIC_PRODUCTS
        .iter()
        .any(|suffix| PathBuf::from(format!("{}{suffix}", prefix.display())).exists())
    {
        return Err(format!("refusing to overwrite MVC products at {}", prefix.display()).into());
    }
    Ok(prefix)
}

fn copy_attached_ephemerides(source: &Path, destination: &Path) -> Result<(), Box<dyn Error>> {
    for entry in fs::read_dir(source.join("FIELD"))? {
        let entry = entry?;
        let name = entry.file_name();
        if !name.to_string_lossy().starts_with("EPHEM") || !entry.file_type()?.is_dir() {
            continue;
        }
        copy_tree(&entry.path(), &destination.join("FIELD").join(name))?;
    }
    Ok(())
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
