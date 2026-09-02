// SPDX-License-Identifier: LGPL-3.0-or-later

//! Source-backed end-to-end correctness gate for joint continuum-line reconstruction.

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
    TaskRequirement, VisibilityContinuumSubtraction, execute_continuum,
};
use casa_ms::{CubeAxisConfig, CubeAxisValue};
use casa_test_support::{CasaTestDataTier, casatestdata_path_for_tier};
use casa_types::measures::frequency::FrequencyRef;
use ndarray::Dimension;
use sha2::{Digest, Sha256};

const DATASET: &str = "unittest/uvcontsub/sim_alma_cont_poly_order_0_nonoise.ms";
const OUTPUT_ENV: &str = "CASA_RS_JOINT_REAL_DATA_PREFIX";
const REPRESENTATIVE_MS_ENV: &str = "CASA_RS_ISSUE607_JOINT_MS";
const REPRESENTATIVE_OUTPUT_ENV: &str = "CASA_RS_ISSUE607_JOINT_PREFIX";
const IMAGE_SIZE: usize = 32;
const CHANNELS: usize = 16;
const SOURCE_TREE_SHA256: &str = "ae80d9199e2d313e951b650ed670881bebc8d686eff4b38c017d3df917fb2710";

#[test]
#[ignore = "requires slow-parity casatestdata"]
fn joint_continuum_line_recovers_the_noiseless_alma_simulation() -> Result<(), Box<dyn Error>> {
    set_production_io_environment();
    let source = casatestdata_path_for_tier(CasaTestDataTier::SlowParity, DATASET)
        .ok_or("slow-parity casatestdata root is unavailable")?;
    if !source.is_dir() {
        return Err(format!("joint MeasurementSet is missing at {}", source.display()).into());
    }
    assert_eq!(
        tree_sha256(&source)?,
        SOURCE_TREE_SHA256,
        "source fixture provenance changed"
    );

    let staging = tempfile::tempdir()?;
    let staged = staging.path().join("joint-input.ms");
    casa_ms::MeasurementSet::open(&source)?.save_as(&staged)?;
    casa_ms::initialize_measurement_set_owner_manifest(&staged)?;
    let image_name = std::env::var_os(OUTPUT_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|| staging.path().join("joint"));
    if let Some(parent) = image_name.parent() {
        fs::create_dir_all(parent)?;
    }

    // Preserve the first transition from CASA's line-free fit support into the
    // injected line while keeping the permanent correctness gate interactive.
    let continuum_anchor_channels = (0..8).collect::<Vec<_>>();
    let line_channels = (8..CHANNELS).collect::<Vec<_>>();
    eprintln!(
        "joint_real_data_start rows=3612 source_channels=52~67 output_channels={CHANNELS} image={IMAGE_SIZE}x{IMAGE_SIZE} workers=1"
    );
    let mut joint = base_request(staged.clone(), image_name.clone());
    joint.spectral_window = Some("0:52~67".to_string());
    joint.channel_count = Some(CHANNELS);
    joint.spectral_mode = SpectralImagingMode::JointContinuumLine;
    joint.algorithm = ContinuumAlgorithm::JointContinuumLine {
        continuum_terms: 1,
        continuum_anchor_channels,
        line_channels,
        maximum_condition_number: 1.0e12,
        scales_px: vec![0.0],
        small_scale_bias: 0.0,
    };
    joint.hogbom_iteration_accounting = HogbomIterationAccounting::Strict;
    joint.beam_policy = ContinuumBeamPolicy::Common;
    joint.mask = ContinuumMask::Coupled {
        continuum: Box::new(ContinuumMask::FullPlane),
        line: Box::new(line_mask()),
    };
    let result = execute_continuum(joint)?;

    eprintln!(
        "joint_real_data_complete major_cycles={} minor_iterations={}",
        result.outcome.output.major_cycle_count, result.minor_iterations
    );
    for suffix in [
        ".total.residual",
        ".continuum.model.ct0",
        ".line.model",
        ".total.model",
        ".line.image",
        ".total.image",
        ".continuum.mask",
        ".line.mask",
    ] {
        assert!(
            result.product_names.iter().any(|name| name == suffix),
            "missing joint product {suffix}"
        );
        assert!(product_path(&image_name, suffix).is_dir());
    }

    let continuum = product_values(&image_name, ".continuum.model.ct0")?;
    let peak = continuum.iter().copied().fold(f32::NEG_INFINITY, f32::max);
    let flux = continuum.iter().copied().map(f64::from).sum::<f64>();
    let nonzero = continuum.iter().filter(|value| **value != 0.0).count();
    eprintln!(
        "joint_real_data_continuum shape={:?} peak={peak:.9} model_flux={flux:.9} nonzero={nonzero}",
        continuum.shape()
    );
    for suffix in [
        ".total.residual",
        ".line.model",
        ".total.model",
        ".line.image",
        ".total.image",
    ] {
        let product = open_product(&image_name, suffix)?;
        let product_values = product.get_slice(&vec![0; product.shape().len()], product.shape())?;
        let maximum = product_values
            .iter()
            .copied()
            .map(f32::abs)
            .fold(0.0_f32, f32::max);
        let sum = product_values.iter().copied().map(f64::from).sum::<f64>();
        eprintln!(
            "joint_real_data_product suffix={suffix} shape={:?} abs_max={maximum:.9} sum={sum:.9}",
            product.shape()
        );
    }
    assert_joint_truth(&image_name, &continuum, flux, peak)?;

    let continuum_prefix = suffixed_prefix(&image_name, "-sequential-continuum");
    let mut sequential_continuum = base_request(staged.clone(), continuum_prefix.clone());
    sequential_continuum.spectral_window = Some("0:52~59".to_string());
    sequential_continuum.channel_count = Some(8);
    sequential_continuum.algorithm = ContinuumAlgorithm::Hogbom;
    sequential_continuum.hogbom_iteration_accounting = HogbomIterationAccounting::CasaInclusive;
    execute_continuum(sequential_continuum)?;

    let line_prefix = suffixed_prefix(&image_name, "-sequential-line");
    let mut sequential_line = base_request(staged, line_prefix.clone());
    sequential_line.spectral_window = Some("0:60~67".to_string());
    sequential_line.channel_count = Some(8);
    sequential_line.spectral_mode = SpectralImagingMode::Cube {
        axis: CubeAxisConfig {
            outframe: FrequencyRef::TOPO,
            start: Some(CubeAxisValue::Channel(60)),
            width: Some(CubeAxisValue::Channel(1)),
            ..CubeAxisConfig::default()
        },
        output_channels: Some(8),
    };
    sequential_line.continuum_subtraction = Some(VisibilityContinuumSubtraction {
        fit_spw: "0:52~59".to_string(),
        fit_order: 0,
    });
    sequential_line.algorithm = ContinuumAlgorithm::Hogbom;
    sequential_line.hogbom_iteration_accounting = HogbomIterationAccounting::CasaInclusive;
    sequential_line.beam_policy = ContinuumBeamPolicy::Common;
    sequential_line.mask = line_mask();
    execute_continuum(sequential_line)?;
    eprintln!(
        "joint_real_data_sequential continuum={} line={}",
        continuum_prefix.display(),
        line_prefix.display()
    );
    Ok(())
}

#[test]
#[ignore = "requires the frozen issue #607 real-observation-shaped joint fixture"]
fn issue607_representative_joint_continuum_line_recovers_analytic_sky() -> Result<(), Box<dyn Error>>
{
    const REPRESENTATIVE_IMAGE_SIZE: usize = 512;
    const REPRESENTATIVE_CHANNELS: usize = 256;
    const LINE_CHANNELS: std::ops::Range<usize> = 124..132;
    const LINE_FLUX_JY: [f32; 8] = [0.2, 0.4, 0.6, 0.8, 1.0, 0.8, 0.6, 0.4];
    const FIXTURE_SHA256: &str = "978667029e3843ce49ab704a7b01b5662b6a493750fa3af021b5be385f01d586";

    set_production_io_environment();
    let source = PathBuf::from(
        std::env::var_os(REPRESENTATIVE_MS_ENV).ok_or("CASA_RS_ISSUE607_JOINT_MS is not set")?,
    );
    let output = PathBuf::from(
        std::env::var_os(REPRESENTATIVE_OUTPUT_ENV)
            .ok_or("CASA_RS_ISSUE607_JOINT_PREFIX is not set")?,
    );
    assert_eq!(tree_sha256(&source)?, FIXTURE_SHA256);
    for suffix in [
        ".total.residual",
        ".continuum.model.ct0",
        ".line.model",
        ".total.model",
        ".line.image",
        ".total.image",
        ".continuum.mask",
        ".line.mask",
    ] {
        if product_path(&output, suffix).exists() {
            return Err(format!("representative output already exists: {suffix}").into());
        }
    }

    let output_parent = output
        .parent()
        .ok_or("representative joint output has no parent directory")?;
    fs::create_dir_all(output_parent)?;
    let staging = tempfile::Builder::new()
        .prefix("issue607-joint-staging-")
        .tempdir_in(output_parent)?;
    let staged = staging.path().join("issue607-joint.ms");
    let mut measurement_set = casa_ms::MeasurementSet::open(&source)?;
    measurement_set.save_as(&staged)?;
    casa_ms::initialize_measurement_set_owner_manifest(&staged)?;
    let continuum_anchor_channels = (0..REPRESENTATIVE_CHANNELS)
        .filter(|channel| !LINE_CHANNELS.contains(channel))
        .collect::<Vec<_>>();
    let line_channels = LINE_CHANNELS.collect::<Vec<_>>();
    let mut request = base_request(staged, output.clone());
    request.image_size = REPRESENTATIVE_IMAGE_SIZE;
    request.cell_arcsec = 0.1;
    request.spectral_window = Some("0:0~255".to_string());
    request.channel_count = Some(REPRESENTATIVE_CHANNELS);
    request.spectral_mode = SpectralImagingMode::JointContinuumLine;
    request.algorithm = ContinuumAlgorithm::JointContinuumLine {
        continuum_terms: 1,
        continuum_anchor_channels,
        line_channels: line_channels.clone(),
        maximum_condition_number: 1.0e12,
        scales_px: vec![0.0],
        small_scale_bias: 0.0,
    };
    request.iterations = 128;
    request.cycle_iterations = 32;
    request.maximum_major_cycles = Some(8);
    request.threshold_jy = 1.0e-5;
    request.hogbom_iteration_accounting = HogbomIterationAccounting::Strict;
    request.beam_policy = ContinuumBeamPolicy::Common;
    request.mask = ContinuumMask::Coupled {
        continuum: Box::new(ContinuumMask::FullPlane),
        line: Box::new(ContinuumMask::FullPlane),
    };
    let result = execute_continuum(request)?;
    assert_eq!(
        result
            .outcome
            .output
            .scientific
            .normal_state()
            .sample_count(),
        1_228_800,
        "representative joint selected sample count changed",
    );
    let mut expected_products = Vec::new();
    for row in 0..9 {
        for column in 0..9 {
            expected_products.push(format!(".psf.joint{row}_{column}"));
        }
    }
    expected_products.extend(
        [
            ".total.residual",
            ".continuum.model.ct0",
            ".line.model",
            ".total.model",
            ".line.image",
            ".total.image",
        ]
        .map(str::to_string),
    );
    for row in 0..9 {
        for column in 0..9 {
            expected_products.push(format!(".sumwt.joint{row}_{column}"));
        }
    }
    expected_products.extend([".continuum.mask", ".line.mask"].map(str::to_string));
    assert_eq!(result.product_names, expected_products);

    let continuum = product_values(&output, ".continuum.model.ct0")?;
    let continuum_flux = continuum.iter().copied().map(f64::from).sum::<f64>();
    assert!(
        (continuum_flux - 1.0).abs() <= 1.0e-3,
        "representative continuum flux changed: {continuum_flux}",
    );
    assert_eq!(
        continuum
            .indexed_iter()
            .max_by(|left, right| left.1.abs().total_cmp(&right.1.abs()))
            .map(|(index, _)| index.slice().to_vec()),
        Some(vec![256, 256, 0, 0]),
        "representative continuum centroid moved",
    );

    let line = product_values(&output, ".line.model")?;
    let total = product_values(&output, ".total.model")?;
    for channel in 0..REPRESENTATIVE_CHANNELS {
        let line_value = line
            .index_axis(ndarray::Axis(3), channel)
            .iter()
            .copied()
            .sum::<f32>();
        let expected_line = line_channels
            .iter()
            .position(|candidate| *candidate == channel)
            .map_or(0.0, |index| LINE_FLUX_JY[index]);
        assert!(
            (line_value - expected_line).abs() <= 1.0e-3,
            "representative integrated line flux changed at channel {channel}: {line_value}",
        );
    }
    for ((index, total_value), line_value) in total.indexed_iter().zip(line.iter()) {
        let expected = continuum[[index[0], index[1], 0, 0]] + line_value;
        assert_eq!(*total_value, expected, "joint model decomposition changed");
    }
    drop(total);
    drop(line);

    let residual = product_values(&output, ".total.residual")?;
    let residual_peak = residual
        .iter()
        .copied()
        .map(f32::abs)
        .fold(0.0_f32, f32::max);
    assert!(
        residual_peak <= 1.0e-3,
        "representative joint residual peak is {residual_peak}",
    );
    assert!(residual.iter().all(|value| value.is_finite()));
    drop(residual);

    let continuum_mask = product_values(&output, ".continuum.mask")?;
    let line_mask = product_values(&output, ".line.mask")?;
    assert_eq!(
        continuum_mask.iter().filter(|value| **value != 0.0).count(),
        REPRESENTATIVE_IMAGE_SIZE * REPRESENTATIVE_IMAGE_SIZE * REPRESENTATIVE_CHANNELS,
    );
    assert_eq!(
        line_mask.iter().filter(|value| **value != 0.0).count(),
        REPRESENTATIVE_IMAGE_SIZE * REPRESENTATIVE_IMAGE_SIZE * REPRESENTATIVE_CHANNELS,
    );
    assert_ne!(continuum_mask, line_mask);

    let line_beam = open_product(&output, ".line.image")?.image_info()?.beam_set;
    let total_beam = open_product(&output, ".total.image")?
        .image_info()?
        .beam_set;
    assert!(line_beam.has_single_beam());
    assert!(total_beam.equivalent(&line_beam));
    Ok(())
}

fn base_request(measurement_set: PathBuf, image_name: PathBuf) -> ContinuumImagingRequest {
    ContinuumImagingRequest {
        measurement_set,
        image_name,
        image_size: IMAGE_SIZE,
        facets: 1,
        cell_arcsec: 0.01,
        phase_center_field: None,
        phase_center: None,
        outlier_file: None,
        field_ids: Some(vec![0]),
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
        algorithm: ContinuumAlgorithm::Dirty,
        weighting: ContinuumWeighting::Natural,
        iterations: 512,
        cycle_iterations: 16,
        hogbom_iteration_accounting: HogbomIterationAccounting::Strict,
        maximum_major_cycles: Some(8),
        noise_sigma: None,
        cycle_factor: 1.0,
        minimum_psf_fraction: 0.05,
        maximum_psf_fraction: 0.8,
        gain: 0.1,
        threshold_jy: 0.002,
        psf_cutoff: casa_imaging_products::DEFAULT_PSF_CUTOFF,
        primary_beam_cutoff: 0.2,
        normalization: casa_imaging_model::ProductNormalization::UnitResponse,
        beam_policy: ContinuumBeamPolicy::PerPlane,
        mask: ContinuumMask::FullPlane,
        save_model_column: false,
        save_continuum_residual: false,
        write_primary_beam: false,
        pbcor: false,
        task_requirements: vec![TaskRequirement::SerialCpu],
        resource_policy: casa_imaging_runtime::ResourcePolicy::Explicit(
            casa_imaging_runtime::ResourceOverride {
                workers: Some(1),
                ..Default::default()
            },
        ),
    }
}

fn line_mask() -> ContinuumMask {
    ContinuumMask::Boxes(vec![ContinuumMaskBox {
        blc: [15, 15],
        trc: [16, 16],
    }])
}

fn assert_joint_truth(
    image_name: &Path,
    continuum: &ndarray::ArrayD<f32>,
    flux: f64,
    peak: f32,
) -> Result<(), Box<dyn Error>> {
    const CONTINUUM_TRUTH: f64 = 0.525;
    assert!(
        (flux - CONTINUUM_TRUTH).abs() / CONTINUUM_TRUTH <= 0.001,
        "known continuum model flux changed: {flux} (peak {peak})"
    );

    let line = product_values(image_name, ".line.model")?;
    let total = product_values(image_name, ".total.model")?;
    let residual = product_values(image_name, ".total.residual")?;
    let continuum_mask = product_values(image_name, ".continuum.mask")?;
    let line_mask = product_values(image_name, ".line.mask")?;
    assert_eq!(
        continuum_mask.iter().filter(|value| **value != 0.0).count(),
        IMAGE_SIZE * IMAGE_SIZE * CHANNELS
    );
    assert_eq!(
        line_mask.iter().filter(|value| **value != 0.0).count(),
        4 * CHANNELS
    );
    assert_ne!(continuum_mask, line_mask);
    assert!(residual.iter().all(|value| value.is_finite()));
    assert!(line.iter().any(|value| *value != 0.0));

    for x in 0..IMAGE_SIZE {
        for y in 0..IMAGE_SIZE {
            for channel in 0..CHANNELS {
                let line_value = line[[x, y, 0, channel]];
                if channel < 8 {
                    assert_eq!(line_value, 0.0, "line model leaked into anchor channel");
                }
                let expected = continuum[[x, y, 0, 0]] + line_value;
                assert_eq!(total[[x, y, 0, channel]], expected);
            }
        }
    }
    Ok(())
}

fn product_path(prefix: &Path, suffix: &str) -> PathBuf {
    PathBuf::from(format!("{}{suffix}", prefix.display()))
}

fn open_product(prefix: &Path, suffix: &str) -> Result<PagedImage<f32>, Box<dyn Error>> {
    Ok(PagedImage::open(product_path(prefix, suffix))?)
}

fn product_values(prefix: &Path, suffix: &str) -> Result<ndarray::ArrayD<f32>, Box<dyn Error>> {
    let product = open_product(prefix, suffix)?;
    Ok(product.get_slice(&vec![0; product.shape().len()], product.shape())?)
}

fn suffixed_prefix(prefix: &Path, suffix: &str) -> PathBuf {
    PathBuf::from(format!("{}{suffix}", prefix.display()))
}

fn set_production_io_environment() {
    unsafe {
        std::env::set_var("CASA_RS_IMAGING_SPILL_READ_BYTES_PER_SECOND", "1000000000");
        std::env::set_var("CASA_RS_IMAGING_SPILL_WRITE_BYTES_PER_SECOND", "1000000000");
    }
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
