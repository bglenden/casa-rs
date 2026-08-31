// SPDX-License-Identifier: LGPL-3.0-or-later

//! End-to-end T44 application/publication gate against the frozen CASA oracle.

use std::{
    collections::BTreeSet,
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
use casa_imaging_runtime::{ReceiptStatus, ResourceOverride, ResourcePolicy, WorkNodeId};
use casa_test_support::{CasaTestDataTier, casatestdata_path_for_tier};

const DATASET: &str = "measurementset/vla/ref_vlass_wtsp_creation.ms";
const OUTPUT_ENV: &str = "CASA_RS_T44_APPLICATION_PREFIX";
const IMAGE_SIZE: usize = 128;
const IMAGE_SHAPE: [usize; 4] = [IMAGE_SIZE, IMAGE_SIZE, 1, 1];
const STATE_SHAPE: [usize; 4] = [1, 1, 1, 1];
const PRODUCT_NAMES: [&str; 19] = [
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
    ".pb.tt0",
    ".pb.tt1",
    ".image.tt0.pbcor",
    ".image.tt1.pbcor",
    ".alpha",
    ".alpha.error",
];
const COMMON_BEAM_PRODUCTS: [&str; 6] = [
    ".image.tt0",
    ".image.tt1",
    ".image.tt0.pbcor",
    ".image.tt1.pbcor",
    ".alpha",
    ".alpha.error",
];

#[test]
#[ignore = "requires slow-parity casatestdata and the frozen CASA T44 image products"]
fn t44_application_mtmfs_publishes_frozen_casa_product_contract() -> Result<(), Box<dyn Error>> {
    set_production_io_environment();
    let output = PathBuf::from(
        std::env::var_os(OUTPUT_ENV).ok_or("CASA_RS_T44_APPLICATION_PREFIX is not set")?,
    );
    let source = casatestdata_path_for_tier(CasaTestDataTier::SlowParity, DATASET)
        .ok_or("slow-parity casatestdata root is unavailable")?;
    if !source.is_dir() {
        return Err(format!("T44 MeasurementSet is missing at {}", source.display()).into());
    }
    let staging = tempfile::tempdir()?;
    let staged = staging.path().join("ref_vlass_wtsp_creation.ms");
    copy_tree(&source, &staged)?;
    casa_ms::initialize_measurement_set_owner_manifest(&staged)?;

    let request = ContinuumImagingRequest {
        measurement_set: staged,
        image_name: output.clone(),
        image_size: IMAGE_SIZE,
        cell_arcsec: 2.5,
        phase_center_field: None,
        phase_center: None,
        outlier_file: None,
        field_ids: Some(vec![0]),
        uv_range: None,
        intent: None,
        data_description: None,
        spectral_window: Some("0:0~15,1:0~15".to_string()),
        channel_start: None,
        channel_count: None,
        spectral_mode: SpectralImagingMode::Continuum,
        continuum_subtraction: None,
        data_column: Some("DATA".to_string()),
        algorithm: ContinuumAlgorithm::Mtmfs {
            terms: 2,
            scales_px: vec![0.0, 5.0],
            small_scale_bias: 0.0,
        },
        weighting: ContinuumWeighting::Natural,
        iterations: 8,
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
        write_primary_beam: true,
        pbcor: true,
        task_requirements: vec![TaskRequirement::SerialCpu],
    };
    let result = execute_continuum(request)?;
    let expected_names = PRODUCT_NAMES.map(str::to_string).to_vec();
    assert_eq!(result.product_names, expected_names);
    assert_eq!(result.minor_iterations, 8);
    assert_eq!(result.outcome.output.major_cycle_count, 5);

    let outcome = &result.outcome.output;
    assert_eq!(outcome.products.payload_residency_bytes(), 0);
    assert_eq!(
        outcome
            .products
            .members()
            .iter()
            .map(|member| member.name())
            .collect::<Vec<_>>(),
        PRODUCT_NAMES
    );
    assert_eq!(
        outcome.publication_receipt.status(),
        ReceiptStatus::Completed
    );
    assert_eq!(
        outcome.publication_receipt.publication_layout_count(),
        PRODUCT_NAMES.len()
    );
    assert_eq!(
        outcome.publication_receipt.projected_resource_policy(),
        ResourcePolicy::Explicit(ResourceOverride {
            workers: Some(1),
            ..ResourceOverride::default()
        })
    );
    let nodes = outcome.publication_receipt.plan_node_identities();
    for node in [
        "product-generation-generate",
        "product-generation-seal",
        "product-publication-stage",
        "product-publication-commit",
    ] {
        assert!(
            nodes.contains(&WorkNodeId::new(node)),
            "missing {node} node"
        );
    }

    assert_persisted_inventory(&output)?;
    assert_persisted_metadata(&output)?;
    eprintln!("t44_application_rust_products {}", output.display());
    Ok(())
}

fn assert_persisted_inventory(prefix: &Path) -> Result<(), Box<dyn Error>> {
    let expected = PRODUCT_NAMES.into_iter().collect::<BTreeSet<_>>();
    let parent = prefix.parent().unwrap_or_else(|| Path::new("."));
    let stem = prefix
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or("T44 application prefix is not UTF-8")?;
    let mut actual = BTreeSet::new();
    for entry in fs::read_dir(parent)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if let Some(suffix) = name.strip_prefix(stem)
            && suffix.starts_with('.')
            && !suffix.starts_with(".casa-rs-stage-")
        {
            actual.insert(suffix.to_string());
        }
    }
    assert_eq!(
        actual,
        expected.into_iter().map(str::to_string).collect(),
        "persisted T44 inventory changed"
    );
    assert!(!actual.iter().any(|name| name.starts_with(".weight")));
    assert!(!actual.contains(".alpha.pbcor"));
    Ok(())
}

fn assert_persisted_metadata(prefix: &Path) -> Result<(), Box<dyn Error>> {
    let open =
        |name: &str| PagedImage::<f32>::open(PathBuf::from(format!("{}{name}", prefix.display())));
    for name in PRODUCT_NAMES {
        let image = open(name)?;
        let expected_shape = if name.starts_with(".sumwt.") {
            &STATE_SHAPE
        } else {
            &IMAGE_SHAPE
        };
        assert_eq!(image.shape(), expected_shape, "{name} shape");
        let expected_unit = if name.starts_with(".model.") {
            "Jy/pixel"
        } else if name.starts_with(".psf.")
            || name.starts_with(".residual.")
            || name.starts_with(".image.")
        {
            "Jy/beam"
        } else {
            ""
        };
        assert_eq!(image.units(), expected_unit, "{name} unit");
    }

    let common = open(COMMON_BEAM_PRODUCTS[0])?.image_info()?.beam_set;
    assert!(common.has_single_beam());
    for name in &COMMON_BEAM_PRODUCTS[1..] {
        let candidate = open(name)?.image_info()?.beam_set;
        assert!(candidate.has_single_beam(), "{name} common beam");
        assert!(candidate.equivalent(&common), "{name} common beam identity");
    }

    let alpha = open(".alpha")?;
    let alpha_error = open(".alpha.error")?;
    assert_eq!(alpha.default_mask_name().as_deref(), Some("mask0"));
    assert_eq!(alpha_error.default_mask_name().as_deref(), Some("mask0"));
    let alpha_mask = alpha
        .get_mask_slice(&[0; 4], &IMAGE_SHAPE, &[1; 4])?
        .ok_or("alpha validity mask is missing")?;
    let error_mask = alpha_error
        .get_mask_slice(&[0; 4], &IMAGE_SHAPE, &[1; 4])?
        .ok_or("alpha-error validity mask is missing")?;
    assert_eq!(alpha_mask, error_mask);
    assert!(alpha_mask.iter().any(|valid| *valid));
    assert!(alpha_mask.iter().any(|valid| !*valid));
    Ok(())
}

fn set_production_io_environment() {
    // The application requires measured local storage rates at its production boundary.
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
