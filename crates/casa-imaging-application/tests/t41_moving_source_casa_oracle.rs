// SPDX-License-Identifier: LGPL-3.0-or-later

//! Focused T41 moving-source gate against CASA's pinned Venus regression.

use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
};

use casa_images::PagedImage;
use casa_imaging_application::{
    ContinuumAlgorithm, ContinuumBeamPolicy, ContinuumImagingRequest, ContinuumMask,
    ContinuumWeighting, HogbomIterationAccounting, PolarizationCoordinate, SpectralImagingMode,
    TaskRequirement, execute_continuum,
};
use casa_ms::CubeAxisConfig;

const MS_ENV: &str = "CASA_RS_T41_VENUS_MS";
const EPHEMERIS_ENV: &str = "CASA_RS_T41_VENUS_EPHEMERIS";
const CASA_RESIDUAL_ENV: &str = "CASA_RS_T41_CASA_RESIDUAL";

#[test]
#[ignore = "requires pinned Venus MeasurementSet, external ephemeris, and CASA residual"]
fn t41_external_ephemeris_cubesource_matches_casa_geometry_and_dirty_image()
-> Result<(), Box<dyn Error>> {
    set_production_io_environment();
    let source = required_path(MS_ENV)?;
    let ephemeris = required_path(EPHEMERIS_ENV)?;
    let casa_residual = required_path(CASA_RESIDUAL_ENV)?;
    let staging = tempfile::tempdir()?;
    let measurement_set = staging.path().join("venus.ms");
    casa_ms::MeasurementSet::open(&source)?.save_as(&measurement_set)?;
    copy_attached_ephemerides(&source, &measurement_set)?;
    casa_ms::initialize_measurement_set_owner_manifest(&measurement_set)?;
    let rust_prefix = staging.path().join("rust-venus");

    let result = execute_continuum(request(
        measurement_set,
        ephemeris.display().to_string(),
        rust_prefix.clone(),
    ))?;
    assert!(
        result
            .outcome
            .output
            .scientific
            .normal_state()
            .sample_count()
            > 0
    );

    let rust =
        PagedImage::<f32>::open(PathBuf::from(format!("{}.residual", rust_prefix.display())))?;
    let casa = PagedImage::<f32>::open(casa_residual)?;
    assert_eq!(rust.shape(), casa.shape(), "CASA and Rust residual shape");
    let rust_values = rust.get_slice(&vec![0; rust.shape().len()], rust.shape())?;
    let casa_values = casa.get_slice(&vec![0; casa.shape().len()], casa.shape())?;
    let rust_stats = statistics(rust_values.iter().copied());
    let casa_stats = statistics(casa_values.iter().copied());
    let nrms = normalized_rms(rust_values.iter().copied(), casa_values.iter().copied());
    assert!(
        nrms <= 0.001,
        "dirty image normalized RMS {nrms} exceeds 0.1%; Rust peak {} at {}, CASA peak {} at {}",
        rust_stats.maximum,
        rust_stats.maximum_position,
        casa_stats.maximum,
        casa_stats.maximum_position,
    );
    assert!(
        relative_difference(rust_stats.maximum, casa_stats.maximum) <= 0.01,
        "dirty peak flux differs: Rust {} CASA {}",
        rust_stats.maximum,
        casa_stats.maximum,
    );
    assert_eq!(rust_stats.maximum_position, casa_stats.maximum_position);
    Ok(())
}

fn request(
    measurement_set: PathBuf,
    ephemeris: String,
    image_name: PathBuf,
) -> ContinuumImagingRequest {
    ContinuumImagingRequest {
        measurement_set,
        image_name,
        image_size: 288,
        facets: 1,
        cell_arcsec: 0.14,
        phase_center_field: None,
        phase_center: Some(ephemeris),
        outlier_file: None,
        field_ids: Some(vec![0]),
        uv_range: None,
        intent: None,
        data_description: None,
        spectral_window: None,
        channel_start: None,
        channel_count: None,
        spectral_mode: SpectralImagingMode::CubeSource {
            axis: CubeAxisConfig::default(),
            output_channels: None,
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

fn statistics(values: impl Iterator<Item = f32>) -> Statistics {
    let mut maximum = f32::NEG_INFINITY;
    let mut maximum_position = 0;
    for (position, value) in values.enumerate() {
        if value > maximum {
            maximum = value;
            maximum_position = position;
        }
    }
    Statistics {
        maximum: f64::from(maximum),
        maximum_position,
    }
}

fn normalized_rms(rust: impl Iterator<Item = f32>, casa: impl Iterator<Item = f32>) -> f64 {
    let (error, reference) =
        rust.zip(casa)
            .fold((0.0, 0.0), |(error, reference), (actual, expected)| {
                let actual = f64::from(actual);
                let expected = f64::from(expected);
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

fn required_path(name: &str) -> Result<PathBuf, Box<dyn Error>> {
    let path = PathBuf::from(std::env::var_os(name).ok_or_else(|| format!("{name} is not set"))?);
    if !path.is_dir() {
        return Err(format!("{name} does not name a table: {}", path.display()).into());
    }
    Ok(path)
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
