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
