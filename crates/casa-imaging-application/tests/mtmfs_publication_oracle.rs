// SPDX-License-Identifier: LGPL-3.0-or-later

//! End-to-end T44 application/publication gate against the frozen CASA oracle.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fs,
    path::{Path, PathBuf},
};

use casa_coordinates::{CoordinateModel, StokesType};
use casa_images::PagedImage;
use casa_imaging_application::{
    ContinuumAlgorithm, ContinuumBeamPolicy, ContinuumImagingRequest, ContinuumMask,
    ContinuumWeighting, HogbomIterationAccounting, SpectralImagingMode, TaskRequirement,
    execute_continuum,
};
use casa_imaging_runtime::{
    CapacityDomainId, ClaimLifetime, FenceKind, IoBufferKind, LeaseResource, ReceiptStatus,
    ResourceOverride, ResourcePolicy, WorkNodeId,
};
use casa_test_support::{CasaTestDataTier, casatestdata_path_for_tier};

const DATASET: &str = "measurementset/vla/ref_vlass_wtsp_creation.ms";
const OUTPUT_ENV: &str = "CASA_RS_T44_APPLICATION_PREFIX";
const REPRESENTATIVE_MS_ENV: &str = "CASA_RS_ISSUE607_MTMFS_MS";
const REPRESENTATIVE_CASA_PREFIX_ENV: &str = "CASA_RS_ISSUE607_MTMFS_CASA_PREFIX";
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
        facets: 1,
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
        polarizations: vec![casa_imaging_application::PolarizationCoordinate::StokesI],
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
        primary_beam_cutoff: 0.2,
        normalization: casa_imaging_model::ProductNormalization::UnitResponse,
        beam_policy: ContinuumBeamPolicy::Common,
        mask: ContinuumMask::FullPlane,
        save_model_column: false,
        save_continuum_residual: false,
        write_primary_beam: true,
        pbcor: true,
        w_projection_planes: None,
        aw_projection: None,
        task_requirements: vec![TaskRequirement::SerialCpu],
        resource_policy: casa_imaging_runtime::ResourcePolicy::Explicit(
            casa_imaging_runtime::ResourceOverride {
                workers: Some(1),
                ..Default::default()
            },
        ),
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

#[test]
#[ignore = "requires the frozen issue #607 multi-SPW MT-MFS fixture and CASA products"]
fn issue607_representative_mtmfs_matches_casa_products() -> Result<(), Box<dyn Error>> {
    set_production_io_environment();
    let source = PathBuf::from(
        std::env::var_os(REPRESENTATIVE_MS_ENV).ok_or("CASA_RS_ISSUE607_MTMFS_MS is not set")?,
    );
    let casa_prefix = PathBuf::from(
        std::env::var_os(REPRESENTATIVE_CASA_PREFIX_ENV)
            .ok_or("CASA_RS_ISSUE607_MTMFS_CASA_PREFIX is not set")?,
    );
    let mut measurement_set = casa_ms::MeasurementSet::open(&source)?;
    let spectral_window = measurement_set.spectral_window()?;
    let frequencies = (0..4)
        .map(|row| spectral_window.chan_freq(row))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let minimum_frequency = frequencies.iter().copied().fold(f64::INFINITY, f64::min);
    let maximum_frequency = frequencies
        .iter()
        .copied()
        .fold(f64::NEG_INFINITY, f64::max);
    let fractional_span =
        (maximum_frequency - minimum_frequency) / ((maximum_frequency + minimum_frequency) / 2.0);
    assert!(
        fractional_span >= 0.15,
        "MT-MFS frequency lever arm narrowed"
    );

    let staging = tempfile::tempdir()?;
    let staged = staging.path().join("issue607-mtmfs.ms");
    measurement_set.save_as(&staged)?;
    casa_ms::initialize_measurement_set_owner_manifest(&staged)?;
    let output = staging.path().join("rust-mtmfs-representative");
    let mut request = representative_mtmfs_request(staged, output.clone());
    request.spectral_window = Some("0~3".to_string());
    let result = execute_continuum(request)?;
    assert_eq!(result.minor_iterations, 8);
    assert_eq!(result.product_names, PRODUCT_NAMES.map(str::to_string));
    assert_eq!(
        result
            .outcome
            .output
            .scientific
            .normal_state()
            .sample_count(),
        1_336_320,
        "representative MT-MFS selected sample count changed",
    );
    assert_initial_spill_receipt(&result.outcome.output.initial_receipt);
    assert_low_memory_receipt(
        result
            .outcome
            .output
            .final_major_receipt
            .as_ref()
            .ok_or("representative low-memory run omitted its final-major receipt")?,
    );
    assert_representative_products_match_casa(&output, &casa_prefix)?;
    Ok(())
}

fn assert_initial_spill_receipt(receipt: &casa_imaging_runtime::ExecutionReceipt) {
    assert!(receipt.adaptation_identities().is_empty());
    let spill = receipt
        .plan_node_identities()
        .into_iter()
        .find(|node| {
            receipt
                .stage_predicted_io(node, IoBufferKind::SpillWrite)
                .is_some()
        })
        .expect("initial plan lacks its fixed managed spill");
    let (bytes, operations) = receipt
        .stage_actual_io(&spill, IoBufferKind::SpillWrite)
        .expect("initial managed spill lacks actual I/O evidence");
    assert!(bytes > 0 && operations > 0);
}

fn assert_low_memory_receipt(receipt: &casa_imaging_runtime::ExecutionReceipt) {
    let adaptation = receipt
        .adaptation_identities()
        .into_iter()
        .next()
        .and_then(|id| receipt.adaptation_projection(&id))
        .expect("production low-memory adaptation receipt");
    assert!(adaptation.was_applied());
    assert!(
        adaptation.transition().to.batch_size < adaptation.transition().from.batch_size,
        "low-memory run did not execute its plan-sealed smaller batch: from={}, to={}",
        adaptation.transition().from.batch_size,
        adaptation.transition().to.batch_size,
    );
    assert!(adaptation.transition().to.recomputation);
    assert!(!adaptation.transition().to.spill);
    assert!(adaptation.transition().to.prefetch);

    let io_kind = IoBufferKind::SpillRead;
    let io_node = receipt
        .plan_node_identities()
        .into_iter()
        .find(|node| receipt.stage_predicted_io(node, io_kind).is_some())
        .expect("low-memory plan lacks predicted managed I/O");
    assert!(receipt.stage_actual_io(&io_node, io_kind).is_some());
    assert!(
        receipt
            .actual_resource_peak(
                &io_node,
                &LeaseResource::IoBuffer(io_kind),
                &ClaimLifetime::through_fence(FenceKind::Io),
            )
            .is_some(),
        "low-memory managed-I/O residency was not receipted",
    );
    let preparation = receipt
        .plan_node_identities()
        .into_iter()
        .find(|node| node.as_str().contains("fft-plan"))
        .expect("low-memory plan lacks recomputation work");
    assert!(receipt.stage_actual_elapsed_nanos(&preparation).is_some());
}

fn representative_mtmfs_request(
    measurement_set: PathBuf,
    image_name: PathBuf,
) -> ContinuumImagingRequest {
    ContinuumImagingRequest {
        measurement_set,
        image_name,
        image_size: 512,
        facets: 1,
        cell_arcsec: 1.0,
        phase_center_field: Some(0),
        phase_center: None,
        outlier_file: None,
        field_ids: Some(vec![0]),
        uv_range: None,
        intent: None,
        data_description: None,
        spectral_window: None,
        channel_start: None,
        channel_count: Some(8),
        spectral_mode: SpectralImagingMode::Continuum,
        continuum_subtraction: None,
        data_column: Some("DATA".to_string()),
        polarizations: vec![casa_imaging_application::PolarizationCoordinate::StokesI],
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
        primary_beam_cutoff: 0.2,
        normalization: casa_imaging_model::ProductNormalization::UnitResponse,
        beam_policy: ContinuumBeamPolicy::Common,
        mask: ContinuumMask::FullPlane,
        save_model_column: false,
        save_continuum_residual: false,
        write_primary_beam: true,
        pbcor: true,
        w_projection_planes: None,
        aw_projection: None,
        task_requirements: vec![TaskRequirement::SerialCpu],
        resource_policy: ResourcePolicy::Explicit(ResourceOverride {
            workers: Some(1),
            memory_bytes: BTreeMap::from([(CapacityDomainId::new("host-memory"), 1 << 30)]),
            ..ResourceOverride::default()
        }),
    }
}

struct OracleProduct {
    shape: Vec<usize>,
    units: String,
    stokes: Vec<StokesType>,
    values: Vec<f32>,
    valid: Vec<bool>,
    beam: Option<casa_images::GaussianBeam>,
}

fn read_oracle_product(prefix: &Path, suffix: &str) -> Result<OracleProduct, Box<dyn Error>> {
    let image = PagedImage::<f32>::open(PathBuf::from(format!("{}{suffix}", prefix.display())))?;
    let shape = image.shape().to_vec();
    let CoordinateModel::Stokes(stokes) = image.coordinates().coordinate(1) else {
        return Err(format!("{suffix} has no polarization coordinate").into());
    };
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
    Ok(OracleProduct {
        shape,
        units: image.units().to_string(),
        stokes: stokes.stokes().to_vec(),
        values,
        valid,
        beam: image.image_info()?.beam_set.single_beam(),
    })
}

fn assert_representative_products_match_casa(
    rust_prefix: &Path,
    casa_prefix: &Path,
) -> Result<(), Box<dyn Error>> {
    let mut failures = Vec::new();
    let rust_primary_beam = read_oracle_product(rust_prefix, ".pb.tt0")?;
    let casa_primary_beam = read_oracle_product(casa_prefix, ".pb.tt0")?;
    if rust_primary_beam.valid != casa_primary_beam.valid {
        failures.push(".pb.tt0 validity differs".to_string());
    }
    for product in PRODUCT_NAMES {
        let rust = read_oracle_product(rust_prefix, product)?;
        let casa = read_oracle_product(casa_prefix, product)?;
        if rust.shape != casa.shape {
            failures.push(format!("{product} shape differs"));
            continue;
        }
        if rust.stokes != casa.stokes {
            failures.push(format!("{product} polarization axis differs"));
        }
        let alpha_product = matches!(product, ".alpha" | ".alpha.error");
        if alpha_product {
            let differing_within_primary_beam = rust
                .valid
                .iter()
                .zip(&casa.valid)
                .zip(&casa_primary_beam.valid)
                .filter(|((left, right), primary_beam)| **primary_beam && left != right)
                .count();
            let differing_outside_primary_beam = rust
                .valid
                .iter()
                .zip(&casa.valid)
                .zip(&casa_primary_beam.valid)
                .filter(|((left, right), primary_beam)| !**primary_beam && left != right)
                .count();
            eprintln!(
                "issue607_mtmfs_alpha_validity product={product} within_pb_differing={differing_within_primary_beam} outside_pb_differing={differing_outside_primary_beam}",
            );
            if differing_within_primary_beam != 0 {
                failures.push(format!(
                    "{product} validity differs within primary-beam support"
                ));
            }
        } else if rust.valid != casa.valid {
            eprintln!(
                "issue607_mtmfs_validity product={product} rust_valid={} casa_valid={} differing={}",
                rust.valid.iter().filter(|valid| **valid).count(),
                casa.valid.iter().filter(|valid| **valid).count(),
                rust.valid
                    .iter()
                    .zip(&casa.valid)
                    .filter(|(left, right)| left != right)
                    .count(),
            );
            failures.push(format!("{product} validity differs"));
        }
        let expected_units = if product.starts_with(".model.") {
            "Jy/pixel"
        } else if product.starts_with(".psf.")
            || product.starts_with(".residual.")
            || product.starts_with(".image.")
        {
            "Jy/beam"
        } else {
            ""
        };
        if rust.units != expected_units {
            failures.push(format!("{product} Rust units {:?}", rust.units));
        }
        let casa_omits_units = (product.starts_with(".psf.") || product.starts_with(".residual."))
            && casa.units.is_empty();
        if casa.units != expected_units && !casa_omits_units {
            failures.push(format!("{product} CASA units {:?}", casa.units));
        }
        let comparison_support = if matches!(product, ".alpha" | ".alpha.error") {
            casa_primary_beam.valid.clone()
        } else {
            rust.valid
                .iter()
                .zip(&casa.valid)
                .map(|(left, right)| *left && *right)
                .collect::<Vec<_>>()
        };
        let nrms = representative_normalized_rms(&rust.values, &casa.values, &comparison_support);
        eprintln!("issue607_mtmfs_product product={product} nrms={nrms:.9e}");
        if !nrms.is_finite() || nrms > 1.0e-3 {
            failures.push(format!("{product} normalized RMS {nrms:.6e} exceeds 0.1%"));
        }
        if matches!(
            product,
            ".image.tt0" | ".image.tt1" | ".image.tt0.pbcor" | ".image.tt1.pbcor"
        ) {
            match (rust.beam, casa.beam) {
                (Some(rust), Some(casa)) => {
                    for (name, actual, expected) in [
                        ("major", rust.major, casa.major),
                        ("minor", rust.minor, casa.minor),
                    ] {
                        if (actual - expected).abs() / expected.abs().max(f64::EPSILON) > 1.0e-3 {
                            failures.push(format!("{product} {name} restoring beam differs"));
                        }
                    }
                }
                _ => failures.push(format!("{product} restoring beam topology differs")),
            }
        }
        if product == ".image.tt0" {
            let rust_peak = peak_index(&rust.values, &comparison_support);
            let casa_peak = peak_index(&casa.values, &comparison_support);
            let width = rust.shape[1];
            let distance = rust_peak.abs_diff(casa_peak) / width
                + (rust_peak % width).abs_diff(casa_peak % width);
            if distance > 1 {
                failures.push(format!(
                    ".image.tt0 peak centroid differs by {distance} pixels"
                ));
            }
            let rust_flux = valid_sum(&rust.values, &comparison_support);
            let casa_flux = valid_sum(&casa.values, &comparison_support);
            if (rust_flux - casa_flux).abs() / casa_flux.abs().max(f64::EPSILON) > 1.0e-3 {
                failures.push(".image.tt0 integrated flux differs".to_string());
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

fn representative_normalized_rms(rust: &[f32], casa: &[f32], valid: &[bool]) -> f64 {
    let (error, reference, count) = rust
        .iter()
        .zip(casa)
        .zip(valid)
        .filter(|(_, valid)| **valid)
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
    assert!(count > 0, "product has no valid support");
    if reference == 0.0 {
        (error / count as f64).sqrt()
    } else {
        (error / reference).sqrt()
    }
}

fn peak_index(values: &[f32], valid: &[bool]) -> usize {
    values
        .iter()
        .zip(valid)
        .enumerate()
        .filter(|(_, (_, valid))| **valid)
        .max_by(|(_, (left, _)), (_, (right, _))| left.abs().total_cmp(&right.abs()))
        .map(|(index, _)| index)
        .expect("representative product has valid support")
}

fn valid_sum(values: &[f32], valid: &[bool]) -> f64 {
    values
        .iter()
        .zip(valid)
        .filter(|(_, valid)| **valid)
        .map(|(value, _)| f64::from(*value))
        .sum()
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
