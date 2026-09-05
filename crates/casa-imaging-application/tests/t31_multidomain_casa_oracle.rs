// SPDX-License-Identifier: LGPL-3.0-or-later

//! Focused T31 end-to-end gate against one frozen CASA multifield oracle.

use std::{
    error::Error,
    fs,
    io::{BufReader, Read},
    path::{Path, PathBuf},
};

use casa_images::PagedImage;
use casa_imaging_application::{
    ContinuumAlgorithm, ContinuumBeamPolicy, ContinuumImagingRequest, ContinuumMask,
    ContinuumMaskBox, ContinuumWeighting, HogbomIterationAccounting, SpectralImagingMode,
    TaskRequirement, execute_continuum,
};
use casa_test_support::{CasaTestDataTier, casatestdata_path_for_tier};
use sha2::{Digest, Sha256};

const DATASET: &str = "measurementset/vla/refim_twopoints_twochan.ms";
const CASA_PREFIX_ENV: &str = "CASA_RS_T31_CASA_PREFIX";
const REPRESENTATIVE_MS_ENV: &str = "CASA_RS_ISSUE607_OUTLIER_MS";
const REPRESENTATIVE_CASA_ENV: &str = "CASA_RS_ISSUE607_OUTLIER_CASA_ROOT";
const REPRESENTATIVE_OUTPUT_ENV: &str = "CASA_RS_ISSUE607_OUTLIER_RUST_ROOT";
const REPRESENTATIVE_SOURCE_SHA256: &str =
    "5748c8b8cc95c1777a8925030e3c63f858874024013d0cc969fdb368152fe57a";
const REPRESENTATIVE_OUTLIER_PHASE_CENTRE: &str = "J2000 4.71239123rad -0.40249038rad";
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

#[test]
#[ignore = "requires the issue #607 representative VLA MS and frozen CASA main/outlier products"]
fn issue607_representative_main_and_outlier_match_casa() -> Result<(), Box<dyn Error>> {
    set_production_io_environment();
    let source = PathBuf::from(
        std::env::var_os(REPRESENTATIVE_MS_ENV).ok_or("CASA_RS_ISSUE607_OUTLIER_MS is not set")?,
    );
    let casa_root = PathBuf::from(
        std::env::var_os(REPRESENTATIVE_CASA_ENV)
            .ok_or("CASA_RS_ISSUE607_OUTLIER_CASA_ROOT is not set")?,
    );
    let rust_root = PathBuf::from(
        std::env::var_os(REPRESENTATIVE_OUTPUT_ENV)
            .ok_or("CASA_RS_ISSUE607_OUTLIER_RUST_ROOT is not set")?,
    );
    assert_eq!(tree_sha256(&source)?, REPRESENTATIVE_SOURCE_SHA256);
    fs::create_dir_all(&rust_root)?;
    let staging = tempfile::Builder::new()
        .prefix("issue607-outlier-staging-")
        .tempdir_in(&rust_root)?;
    let measurement_set = staging.path().join("input.ms");
    copy_tree(&source, &measurement_set)?;
    casa_ms::initialize_measurement_set_owner_manifest(&measurement_set)?;

    for (label, algorithm, iterations) in [
        ("dirty", ContinuumAlgorithm::Dirty, 0),
        ("clean", ContinuumAlgorithm::Hogbom, 25),
    ] {
        let rust_case = rust_root.join(label);
        fs::create_dir_all(&rust_case)?;
        let rust_main = rust_case.join("main");
        let rust_outlier = rust_case.join("outlier");
        let outlier_file = rust_case.join("outlier.txt");
        fs::write(
            &outlier_file,
            format!(
                "imagename={}\nnchan=1\nimsize=[512,512]\ncell=[0.35arcsec,0.35arcsec]\nphasecenter={REPRESENTATIVE_OUTLIER_PHASE_CENTRE}\nusemask=user\nmask=circle[[256pix,256pix],64pix]\n",
                rust_outlier.display()
            ),
        )?;
        let mut request = request(
            measurement_set.clone(),
            rust_main.clone(),
            outlier_file,
            algorithm,
            iterations,
        );
        request.image_size = 512;
        request.cell_arcsec = 0.35;
        request.phase_center = None;
        request.field_ids = Some(vec![0]);
        request.spectral_window = Some("0:0~23".to_string());
        request.channel_count = Some(24);
        request.minimum_psf_fraction = 0.05;
        request.mask = ContinuumMask::Boxes(vec![ContinuumMaskBox {
            blc: [192, 192],
            trc: [319, 319],
        }]);
        let result = execute_continuum(request)?;
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
            result
                .outcome
                .output
                .scientific
                .normal_state()
                .sample_count(),
            6_284_304,
        );

        for (role, rust_prefix) in [("main", &rust_main), ("outlier", &rust_outlier)] {
            let casa_prefix = casa_root.join(label).join(role);
            assert_matching_domain_wcs(rust_prefix, &casa_prefix, [512, 512, 1, 1])?;
            let products: &[&str] = if label == "dirty" {
                &[".psf", ".residual", ".model", ".image", ".sumwt"]
            } else {
                &[".psf", ".residual", ".model", ".image", ".mask", ".sumwt"]
            };
            for product in products {
                let rust = read_product(rust_prefix, product)?;
                let casa = read_product(&casa_prefix, product)?;
                assert_eq!(
                    rust.valid, casa.valid,
                    "{label} {role} {product} validity differs"
                );
                assert!(rust.values.iter().all(|value| value.is_finite()));
                assert!(casa.values.iter().all(|value| value.is_finite()));
                let nrms = normalized_rms(&rust.values, &casa.values, &rust.valid, &casa.valid);
                eprintln!(
                    "issue607_outlier_parity label={label} role={role} product={product} nrms={nrms:.9e}"
                );
                assert!(nrms <= 1.0e-3, "{label} {role} {product} NRMS {nrms:.6e}");
            }
        }
    }
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
        polarizations: vec![casa_imaging_application::PolarizationCoordinate::StokesI],
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
        primary_beam_cutoff: 0.2,
        normalization: casa_imaging_model::ProductNormalization::UnitResponse,
        beam_policy: ContinuumBeamPolicy::PerPlane,
        mask: ContinuumMask::FullPlane,
        save_model_column: false,
        save_continuum_residual: false,
        write_primary_beam: false,
        pbcor: false,
        w_projection_planes: None,
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

fn assert_matching_domain_wcs(
    rust_prefix: &Path,
    casa_prefix: &Path,
    expected_shape: [usize; 4],
) -> Result<(), Box<dyn Error>> {
    let rust = PagedImage::<f32>::open(PathBuf::from(format!("{}.psf", rust_prefix.display())))?;
    let casa = PagedImage::<f32>::open(PathBuf::from(format!("{}.psf", casa_prefix.display())))?;
    assert_eq!(rust.shape(), expected_shape);
    assert_eq!(casa.shape(), expected_shape);
    let pixel = [256.0, 256.0, 0.0, 0.0];
    let rust_world = rust.coordinates().to_world(&pixel)?;
    let casa_world = casa.coordinates().to_world(&pixel)?;
    assert_eq!(rust_world.len(), casa_world.len());
    for (axis, (rust, casa)) in rust_world.iter().zip(casa_world).enumerate() {
        let difference = (rust - casa).abs();
        let tolerance = 1.0e-12_f64.max(1.0e-12 * rust.abs().max(casa.abs()));
        assert!(
            difference <= tolerance,
            "WCS axis {axis} differs: {rust} versus {casa}; delta={difference} tolerance={tolerance}",
        );
    }
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

fn tree_sha256(root: &Path) -> Result<String, Box<dyn Error>> {
    let mut files = Vec::new();
    collect_files(root, root, &mut files)?;
    files.sort_by(|left, right| left.0.cmp(&right.0));
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    for (relative, path) in files {
        if Path::new(&relative)
            .file_name()
            .is_some_and(|name| name == "table.lock")
        {
            continue;
        }
        digest.update(relative.as_bytes());
        digest.update([0]);
        digest.update(fs::metadata(&path)?.len().to_be_bytes());
        let mut input = BufReader::new(fs::File::open(path)?);
        loop {
            let count = input.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            digest.update(&buffer[..count]);
        }
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn collect_files(
    root: &Path,
    directory: &Path,
    files: &mut Vec<(String, PathBuf)>,
) -> Result<(), Box<dyn Error>> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            collect_files(root, &path, files)?;
        } else if entry.file_type()?.is_file() {
            files.push((
                path.strip_prefix(root)?
                    .to_string_lossy()
                    .replace('\\', "/"),
                path,
            ));
        }
    }
    Ok(())
}
