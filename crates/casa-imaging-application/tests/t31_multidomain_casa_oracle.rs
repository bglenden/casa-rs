// SPDX-License-Identifier: LGPL-3.0-or-later

//! Focused T31 end-to-end gate against one frozen CASA multifield oracle.

use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
};

use casa_images::PagedImage;
use casa_imaging_application::{
    ContinuumAlgorithm, ContinuumBeamPolicy, ContinuumImagingRequest, ContinuumMask,
    ContinuumWeighting, HogbomIterationAccounting, SpectralImagingMode, TaskRequirement,
    execute_continuum,
};
use casa_test_support::{CasaTestDataTier, casatestdata_path_for_tier};

const DATASET: &str = "measurementset/vla/refim_twopoints_twochan.ms";
const CASA_PREFIX_ENV: &str = "CASA_RS_T31_CASA_PREFIX";
const MAIN_PHASE_CENTRE: &str = "J2000 19:59:28.500 +40.44.01.50";
const OUTLIER_PHASE_CENTRE: &str = "J2000 19:58:40.895 +40.55.58.543";
const PRODUCTS: [&str; 4] = [".psf", ".residual", ".model", ".image"];

#[test]
#[ignore = "requires slow-parity casatestdata and the frozen CASA T31 image products"]
fn t31_multidomain_geometry_matches_frozen_casa_dirty_and_hogbom() -> Result<(), Box<dyn Error>> {
    set_production_io_environment();
    let casa_root = PathBuf::from(
        std::env::var_os(CASA_PREFIX_ENV).ok_or("CASA_RS_T31_CASA_PREFIX is not set")?,
    );
    let source = casatestdata_path_for_tier(CasaTestDataTier::SlowParity, DATASET)
        .ok_or("slow-parity casatestdata root is unavailable")?;
    let staging = tempfile::tempdir()?;
    let measurement_set = staging.path().join("refim_twopoints_twochan.ms");
    copy_tree(&source, &measurement_set)?;
    casa_ms::initialize_measurement_set_owner_manifest(&measurement_set)?;
    let mut failures = Vec::new();

    for (label, algorithm, iterations) in [
        ("dirty", ContinuumAlgorithm::Dirty, 0),
        ("clean", ContinuumAlgorithm::Hogbom, 10),
    ] {
        let rust_main = staging.path().join(format!("rust-{label}-main"));
        let rust_outlier = staging.path().join(format!("rust-{label}-outlier"));
        let outlier_file = staging.path().join(format!("rust-{label}.outlier"));
        write_outlier_file(&outlier_file, &rust_outlier)?;
        let result = execute_continuum(request(
            measurement_set.clone(),
            rust_main.clone(),
            outlier_file,
            algorithm,
            iterations,
        ))?;
        assert_eq!(
            result
                .outcome
                .output
                .scientific
                .normal_state()
                .domain_count(),
            2
        );
        let expected_counts = if label == "dirty" { (0, 0) } else { (13, 14) };
        assert_eq!(
            (result.minor_iterations, result.actual_minor_iterations),
            expected_counts,
            "CASA compares the shared iteration budget only after every image field exits its minor cycle"
        );
        assert_eq!(
            result.outcome.output.major_cycle_count,
            if label == "dirty" { 1 } else { 2 },
            "clean publication requires the final shared major cycle"
        );

        for (role, rust_prefix, expected_shape, expected_centre) in [
            (
                "main",
                rust_main.as_path(),
                [100, 100, 1, 1],
                main_centre_rad(),
            ),
            (
                "outlier",
                rust_outlier.as_path(),
                [80, 80, 1, 1],
                outlier_centre_rad(),
            ),
        ] {
            let casa_prefix = casa_root.join(format!("{label}-{role}"));
            assert_domain_wcs(rust_prefix, expected_shape, expected_centre)?;
            assert_domain_wcs(&casa_prefix, expected_shape, expected_centre)?;
            for product in PRODUCTS {
                let rust = read_product(rust_prefix, product)?;
                let casa = read_product(&casa_prefix, product)?;
                let nrms = normalized_rms(&rust.values, &casa.values, &rust.valid, &casa.valid);
                eprintln!(
                    "t31_casa_parity label={label} role={role} product={product} nrms={nrms:.9e}"
                );
                if nrms > 1.0e-3 {
                    failures.push(format!(
                        "{label} {role} {product} normalized RMS {nrms:.6e} exceeds 0.1%"
                    ));
                }
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

fn request(
    measurement_set: PathBuf,
    image_name: PathBuf,
    outlier_file: PathBuf,
    algorithm: ContinuumAlgorithm,
    iterations: usize,
) -> ContinuumImagingRequest {
    ContinuumImagingRequest {
        measurement_set,
        image_name,
        image_size: 100,
        facets: 1,
        cell_arcsec: 8.0,
        phase_center_field: None,
        phase_center: Some(MAIN_PHASE_CENTRE.to_string()),
        outlier_file: Some(outlier_file),
        field_ids: None,
        uv_range: None,
        intent: None,
        data_description: None,
        spectral_window: None,
        channel_start: None,
        channel_count: None,
        spectral_mode: SpectralImagingMode::Continuum,
        continuum_subtraction: None,
        data_column: Some("DATA".to_string()),
        algorithm,
        weighting: ContinuumWeighting::Natural,
        iterations,
        cycle_iterations: iterations.max(1),
        hogbom_iteration_accounting: HogbomIterationAccounting::CasaInclusive,
        maximum_major_cycles: None,
        noise_sigma: None,
        cycle_factor: 1.0,
        minimum_psf_fraction: 0.1,
        maximum_psf_fraction: 0.8,
        gain: 0.1,
        threshold_jy: 0.0,
        psf_cutoff: 0.35,
        beam_policy: ContinuumBeamPolicy::PerPlane,
        mask: ContinuumMask::FullPlane,
        save_model_column: false,
        save_continuum_residual: false,
        write_primary_beam: false,
        pbcor: false,
        task_requirements: vec![TaskRequirement::SerialCpu, TaskRequirement::FixedTileCpu],
        resource_policy: casa_imaging_runtime::ResourcePolicy::Balanced,
    }
}

fn write_outlier_file(path: &Path, output: &Path) -> Result<(), Box<dyn Error>> {
    fs::write(
        path,
        format!(
            "imagename={}\nnchan=1\nimsize=[80,80]\ncell=[8.0arcsec,8.0arcsec]\nphasecenter={OUTLIER_PHASE_CENTRE}\nusemask=user\nmask=circle[[40pix,40pix],10pix]\n",
            output.display()
        ),
    )?;
    Ok(())
}

struct ProductPlane {
    values: Vec<f32>,
    valid: Vec<bool>,
}

fn read_product(prefix: &Path, suffix: &str) -> Result<ProductPlane, Box<dyn Error>> {
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
    Ok(ProductPlane { values, valid })
}

fn normalized_rms(rust: &[f32], casa: &[f32], rust_valid: &[bool], casa_valid: &[bool]) -> f64 {
    assert_eq!(rust.len(), casa.len());
    assert_eq!(rust.len(), rust_valid.len());
    assert_eq!(rust.len(), casa_valid.len());
    let (squared_error, squared_reference, count) = rust
        .iter()
        .zip(casa)
        .zip(rust_valid.iter().zip(casa_valid))
        .filter(|(_, (rust_valid, casa_valid))| **rust_valid && **casa_valid)
        .fold(
            (0.0, 0.0, 0_usize),
            |(error, reference, count), ((rust, casa), _)| {
                let rust = f64::from(*rust);
                let casa = f64::from(*casa);
                (
                    error + (rust - casa).powi(2),
                    reference + casa.powi(2),
                    count + 1,
                )
            },
        );
    assert!(count > 0, "product has no common valid support");
    if squared_reference == 0.0 {
        (squared_error / count as f64).sqrt()
    } else {
        (squared_error / squared_reference).sqrt()
    }
}

fn assert_domain_wcs(
    prefix: &Path,
    expected_shape: [usize; 4],
    expected_centre: [f64; 2],
) -> Result<(), Box<dyn Error>> {
    let psf = PagedImage::<f32>::open(PathBuf::from(format!("{}.psf", prefix.display())))?;
    assert_eq!(psf.shape(), expected_shape);
    let world = psf.coordinates().to_world(&[
        expected_shape[0] as f64 / 2.0,
        expected_shape[1] as f64 / 2.0,
        0.0,
        0.0,
    ])?;
    assert!((world[0] - expected_centre[0]).abs() <= 1.0e-12);
    assert!((world[1] - expected_centre[1]).abs() <= 1.0e-12);
    Ok(())
}

fn main_centre_rad() -> [f64; 2] {
    [
        (19.0 + 59.0 / 60.0 + 28.5 / 3600.0) * std::f64::consts::PI / 12.0,
        (40.0 + 44.0 / 60.0 + 1.5 / 3600.0) * std::f64::consts::PI / 180.0,
    ]
}

fn outlier_centre_rad() -> [f64; 2] {
    [
        (19.0 + 58.0 / 60.0 + 40.895 / 3600.0) * std::f64::consts::PI / 12.0,
        (40.0 + 55.0 / 60.0 + 58.543 / 3600.0) * std::f64::consts::PI / 180.0,
    ]
}

fn set_production_io_environment() {
    unsafe {
        std::env::set_var("CASA_RS_IMAGING_SPILL_READ_BYTES_PER_SECOND", "1000000000");
        std::env::set_var("CASA_RS_IMAGING_SPILL_WRITE_BYTES_PER_SECOND", "1000000000");
    }
}

fn copy_tree(source: &Path, destination: &Path) -> Result<(), Box<dyn Error>> {
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let source = entry.path();
        let destination = destination.join(entry.file_name());
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            copy_tree(&source, &destination)?;
        } else if file_type.is_file() {
            fs::copy(source, destination)?;
        } else if file_type.is_symlink() {
            let target = fs::canonicalize(source)?;
            if target.is_dir() {
                copy_tree(&target, &destination)?;
            } else {
                fs::copy(target, destination)?;
            }
        }
    }
    Ok(())
}
