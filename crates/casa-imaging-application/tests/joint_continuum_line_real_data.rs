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
use sha2::{Digest, Sha256};

const DATASET: &str = "unittest/uvcontsub/sim_alma_cont_poly_order_0_nonoise.ms";
const OUTPUT_ENV: &str = "CASA_RS_JOINT_REAL_DATA_PREFIX";
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

fn base_request(measurement_set: PathBuf, image_name: PathBuf) -> ContinuumImagingRequest {
    ContinuumImagingRequest {
        measurement_set,
        image_name,
        image_size: IMAGE_SIZE,
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
        beam_policy: ContinuumBeamPolicy::PerPlane,
        mask: ContinuumMask::FullPlane,
        save_model_column: false,
        save_continuum_residual: false,
        write_primary_beam: false,
        pbcor: false,
        task_requirements: vec![TaskRequirement::SerialCpu],
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
