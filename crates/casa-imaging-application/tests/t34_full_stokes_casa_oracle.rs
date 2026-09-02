// SPDX-License-Identifier: LGPL-3.0-or-later

//! Focused T34 end-to-end gate against CASA full-Stokes products.

use std::{
    error::Error,
    path::{Path, PathBuf},
};

use casa_coordinates::{CoordinateModel, StokesType};
use casa_images::PagedImage;
use casa_imaging_application::{
    ContinuumAlgorithm, ContinuumBeamPolicy, ContinuumImagingRequest, ContinuumMask,
    ContinuumWeighting, HogbomIterationAccounting, PolarizationCoordinate, SpectralImagingMode,
    TaskRequirement, execute_continuum,
};
use casa_ms::{
    MeasurementSet, MsSelection, MsSelectionIoBudget, ResolvedMsSelectionRow, VisibilityDataColumn,
};
use casa_test_support::{CasaTestDataTier, casatestdata_path_for_tier};
use casa_types::ArrayValue;

const DATASET: &str = "measurementset/vla/refim_point_stokes.ms";
const CASA_PREFIX_ENV: &str = "CASA_RS_T34_CASA_PREFIX";
const REPRESENTATIVE_MS_ENV: &str = "CASA_RS_ISSUE607_FULL_STOKES_MS";
const REPRESENTATIVE_CASA_PREFIX_ENV: &str = "CASA_RS_ISSUE607_FULL_STOKES_CASA_PREFIX";
const PRODUCTS: [&str; 6] = [".psf", ".residual", ".model", ".image", ".sumwt", ".pb"];

#[test]
#[ignore = "requires slow-parity casatestdata and matching CASA T34 products"]
fn t34_full_stokes_hogbom_matches_casa_products() -> Result<(), Box<dyn Error>> {
    set_production_io_environment();
    let casa_prefix = PathBuf::from(
        std::env::var_os(CASA_PREFIX_ENV).ok_or("CASA_RS_T34_CASA_PREFIX is not set")?,
    );
    let source = casatestdata_path_for_tier(CasaTestDataTier::SlowParity, DATASET)
        .ok_or("slow-parity casatestdata root is unavailable")?;
    let selected = selected_correlation_contract(&source)?;
    let staging = tempfile::tempdir()?;
    let measurement_set = staging.path().join("refim_point_stokes.ms");
    MeasurementSet::open(&source)?.save_as(&measurement_set)?;
    casa_ms::initialize_measurement_set_owner_manifest(&measurement_set)?;
    let rust_prefix = staging.path().join("rust-full-stokes");

    let result = execute_continuum(request(measurement_set, rust_prefix.clone()))?;
    assert_eq!(result.minor_iterations, 20);
    assert!(result.outcome.output.major_cycle_count >= 2);
    assert_eq!(
        result
            .outcome
            .output
            .scientific
            .normal_state()
            .sample_count(),
        selected.sample_count,
        "Rust selected exactly the CASA RR/RL/LR/LL cells",
    );

    compare_products(&rust_prefix, &casa_prefix, &selected)?;
    Ok(())
}

fn compare_products(
    rust_prefix: &Path,
    casa_prefix: &Path,
    selected: &SelectedCorrelationContract,
) -> Result<(), Box<dyn Error>> {
    let mut failures = Vec::new();
    for product in PRODUCTS {
        let rust = read_product(&rust_prefix, product)?;
        let casa = read_product(&casa_prefix, product)?;
        assert_eq!(rust.shape, casa.shape, "{product} shape");
        let expected_units = match product {
            ".psf" | ".residual" | ".image" => "Jy/beam",
            ".model" => "Jy/pixel",
            ".sumwt" | ".pb" => "",
            _ => unreachable!("fixed product oracle"),
        };
        if rust.units != expected_units {
            failures.push(format!(
                "{product} Rust units {:?} != {expected_units:?}",
                rust.units
            ));
        }
        let casa_omits_units = matches!(product, ".psf" | ".residual") && casa.units.is_empty();
        if casa.units != expected_units && !casa_omits_units {
            failures.push(format!(
                "{product} CASA units {:?} != {expected_units:?}",
                casa.units
            ));
        }
        if rust.stokes != casa.stokes {
            failures.push(format!("{product} polarization axis differs"));
        }
        if rust.valid != casa.valid {
            failures.push(format!("{product} validity differs"));
        }
        let common_valid = rust
            .valid
            .iter()
            .zip(&casa.valid)
            .map(|(rust, casa)| *rust && *casa)
            .collect::<Vec<_>>();
        let nrms = normalized_rms(&rust.values, &casa.values, &common_valid);
        if matches!(product, ".residual" | ".image") {
            product_diagnostics(product, &rust, &casa);
        }
        if product == ".sumwt" {
            eprintln!("t34_sumwt rust={:?} casa={:?}", rust.values, casa.values);
            if let Some(sum_weights) = selected.sum_weights {
                for (actual, expected) in rust.values.iter().zip(sum_weights) {
                    if (f64::from(*actual) - expected).abs() > 1.0e-5 * expected.max(1.0) {
                        failures.push(format!(
                            ".sumwt Rust value {actual} != selected CASA flag/weight reduction {expected}"
                        ));
                    }
                }
            }
        }
        eprintln!(
            "t34_casa_parity product={product} nrms={nrms:.9e} rust_units={:?} casa_units={:?}",
            rust.units, casa.units
        );
        if nrms > 1.0e-3 {
            failures.push(format!("{product} normalized RMS {nrms:.6e} exceeds 0.1%"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

fn product_diagnostics(product: &str, rust: &Product, casa: &Product) {
    for plane in 0..rust.shape[2] {
        let indices = (0..rust.values.len())
            .filter(|index| (index / rust.shape[3]) % rust.shape[2] == plane)
            .collect::<Vec<_>>();
        let rust_values = indices
            .iter()
            .map(|index| rust.values[*index])
            .collect::<Vec<_>>();
        let casa_values = indices
            .iter()
            .map(|index| casa.values[*index])
            .collect::<Vec<_>>();
        let rust_valid_values = indices
            .iter()
            .map(|index| rust.valid[*index])
            .collect::<Vec<_>>();
        let casa_valid_values = indices
            .iter()
            .map(|index| casa.valid[*index])
            .collect::<Vec<_>>();
        let valid = rust_valid_values
            .iter()
            .zip(&casa_valid_values)
            .map(|(rust, casa)| *rust && *casa)
            .collect::<Vec<_>>();
        let rust_valid = rust_valid_values.iter().filter(|valid| **valid).count();
        let casa_valid = casa_valid_values.iter().filter(|valid| **valid).count();
        let rust_peak = rust_values
            .iter()
            .copied()
            .map(f32::abs)
            .fold(0.0_f32, f32::max);
        let casa_peak = casa_values
            .iter()
            .copied()
            .map(f32::abs)
            .fold(0.0_f32, f32::max);
        eprintln!(
            "issue607_product_plane product={product} plane={plane} nrms={:.9e} rust_valid={rust_valid} casa_valid={casa_valid} rust_peak={rust_peak:.9e} casa_peak={casa_peak:.9e}",
            normalized_rms(&rust_values, &casa_values, &valid)
        );
    }
}

#[test]
#[ignore = "requires the frozen issue #607 full-Stokes fixture and CASA products"]
fn issue607_representative_full_stokes_matches_casa_products() -> Result<(), Box<dyn Error>> {
    set_production_io_environment();
    let source = PathBuf::from(
        std::env::var_os(REPRESENTATIVE_MS_ENV)
            .ok_or("CASA_RS_ISSUE607_FULL_STOKES_MS is not set")?,
    );
    let casa_prefix = PathBuf::from(
        std::env::var_os(REPRESENTATIVE_CASA_PREFIX_ENV)
            .ok_or("CASA_RS_ISSUE607_FULL_STOKES_CASA_PREFIX is not set")?,
    );
    let selected = representative_selected_correlation_contract(&source)?;
    assert_eq!(selected.sample_count, 1_336_320);
    assert!(selected.cross_hand_nonzero > 0);
    assert!(selected.cross_hand_flagged > 0);
    assert!(selected.cross_hand_unflagged > 0);
    assert!(selected.minimum_weight > 0.0);
    assert!(selected.maximum_weight > selected.minimum_weight);

    let staging = tempfile::tempdir()?;
    let measurement_set = staging.path().join("full-stokes-shaped.ms");
    MeasurementSet::open(&source)?.save_as(&measurement_set)?;
    casa_ms::initialize_measurement_set_owner_manifest(&measurement_set)?;
    let rust_prefix = staging.path().join("rust-full-stokes-representative");
    let mut imaging = request(measurement_set, rust_prefix.clone());
    imaging.image_size = 512;
    imaging.cell_arcsec = 1.0;
    imaging.spectral_window = Some("0~3".to_string());
    imaging.channel_count = Some(8);
    imaging.iterations = 25;
    imaging.cycle_iterations = 25;
    imaging.write_primary_beam = true;

    let result = execute_continuum(imaging)?;
    assert_eq!(result.minor_iterations, 25);
    assert!(result.outcome.output.major_cycle_count >= 2);
    assert_eq!(
        result
            .outcome
            .output
            .scientific
            .normal_state()
            .sample_count(),
        selected.sample_count,
    );
    compare_products(&rust_prefix, &casa_prefix, &selected)?;
    Ok(())
}

struct SelectedCorrelationContract {
    sample_count: u64,
    sum_weights: Option<[f64; 4]>,
    cross_hand_nonzero: usize,
    cross_hand_flagged: usize,
    cross_hand_unflagged: usize,
    minimum_weight: f32,
    maximum_weight: f32,
}

fn selected_correlation_contract(
    source: &Path,
) -> Result<SelectedCorrelationContract, Box<dyn Error>> {
    let measurement_set = MeasurementSet::open(source)?;
    let polarization_id = usize::try_from(measurement_set.data_description()?.polarization_id(0)?)?;
    assert_eq!(
        measurement_set.polarization()?.corr_type(polarization_id)?,
        [5, 6, 7, 8],
        "CASA source correlation order must be RR/RL/LR/LL",
    );
    let mut sum_weights = [0.0_f64; 4];
    let flag_column = measurement_set.flag_column();
    let flag_row_column = measurement_set.flag_row_column();
    let weight_column = measurement_set.weight_column();
    for row in 0..measurement_set.row_count() {
        let ArrayValue::Bool(flags) = flag_column.get(row)? else {
            return Err("CASA FLAG cell is not boolean".into());
        };
        let ArrayValue::Float32(weights) = weight_column.get(row)? else {
            return Err("CASA WEIGHT cell is not float32".into());
        };
        if flags.shape().first().copied() != Some(4) || weights.len() != 4 {
            return Err("CASA correlation cell does not contain four selected lanes".into());
        }
        let row_flag = flag_row_column.get(row)?;
        let paired = |first: usize, second: usize| {
            if row_flag || flags[[first, 0]] || flags[[second, 0]] {
                0.0
            } else {
                f64::from(weights[first].min(weights[second]))
            }
        };
        let parallel = paired(0, 3);
        let cross = paired(1, 2);
        for (sum, value) in sum_weights
            .iter_mut()
            .zip([parallel, cross, cross, parallel])
        {
            *sum += value;
        }
    }
    Ok(SelectedCorrelationContract {
        sample_count: u64::try_from(measurement_set.row_count())?
            .checked_mul(4)
            .ok_or("selected sample count overflow")?,
        sum_weights: Some(sum_weights),
        cross_hand_nonzero: 0,
        cross_hand_flagged: 0,
        cross_hand_unflagged: 0,
        minimum_weight: 0.0,
        maximum_weight: 0.0,
    })
}

fn representative_selected_correlation_contract(
    source: &Path,
) -> Result<SelectedCorrelationContract, Box<dyn Error>> {
    let measurement_set = MeasurementSet::open(source)?;
    let data_description = measurement_set.data_description()?;
    assert_eq!(data_description.row_count(), 4);
    for row in 0..data_description.row_count() {
        let polarization_id = usize::try_from(data_description.polarization_id(row)?)?;
        assert_eq!(
            measurement_set.polarization()?.corr_type(polarization_id)?,
            [9, 10, 11, 12],
            "derived source correlation order must be XX/XY/YX/YY",
        );
    }
    let selection = measurement_set.resolve_selection(
        &MsSelection::new().field(&[0]).spw(&[0, 1, 2, 3]),
        MsSelectionIoBudget {
            available_bytes: 4 * 1024 * 1024,
            maximum_live_blocks: 1,
            requested_bytes_per_row: std::mem::size_of::<ResolvedMsSelectionRow>(),
            storage_alignment_rows: None,
        },
    )?;
    let data_column = measurement_set.data_column(VisibilityDataColumn::Data)?;
    let flag_column = measurement_set.flag_column();
    let flag_row_column = measurement_set.flag_row_column();
    let weight_column = measurement_set.weight_column();
    let mut cross_hand_nonzero = 0;
    let mut cross_hand_flagged = 0;
    let mut cross_hand_unflagged = 0;
    let mut minimum_weight = f32::INFINITY;
    let mut maximum_weight = f32::NEG_INFINITY;
    for row in selection.row_indices() {
        let ArrayValue::Complex32(data) = data_column.get(row)? else {
            return Err("representative DATA cell is not complex32".into());
        };
        let ArrayValue::Bool(flags) = flag_column.get(row)? else {
            return Err("representative FLAG cell is not boolean".into());
        };
        let ArrayValue::Float32(weights) = weight_column.get(row)? else {
            return Err("representative WEIGHT cell is not float32".into());
        };
        if data.shape() != [4, 8] || flags.shape() != [4, 8] || weights.len() != 4 {
            return Err("representative correlation cell shape changed".into());
        }
        let row_flag = flag_row_column.get(row)?;
        for weight in weights.iter().copied() {
            minimum_weight = minimum_weight.min(weight);
            maximum_weight = maximum_weight.max(weight);
        }
        for channel in 0..8 {
            for correlation in [1, 2] {
                if data[[correlation, channel]].norm_sqr() > 0.0 {
                    cross_hand_nonzero += 1;
                }
                if row_flag || flags[[correlation, channel]] {
                    cross_hand_flagged += 1;
                } else {
                    cross_hand_unflagged += 1;
                }
            }
        }
    }
    Ok(SelectedCorrelationContract {
        sample_count: u64::try_from(selection.row_indices().len())? * 8 * 4,
        sum_weights: None,
        cross_hand_nonzero,
        cross_hand_flagged,
        cross_hand_unflagged,
        minimum_weight,
        maximum_weight,
    })
}

fn request(measurement_set: PathBuf, image_name: PathBuf) -> ContinuumImagingRequest {
    ContinuumImagingRequest {
        measurement_set,
        image_name,
        image_size: 64,
        facets: 1,
        cell_arcsec: 8.0,
        phase_center_field: Some(0),
        phase_center: None,
        outlier_file: None,
        field_ids: Some(vec![0]),
        uv_range: None,
        intent: None,
        data_description: None,
        spectral_window: Some("0".to_string()),
        channel_start: Some(0),
        channel_count: Some(1),
        spectral_mode: SpectralImagingMode::Continuum,
        continuum_subtraction: None,
        data_column: Some("DATA".to_string()),
        polarizations: vec![
            PolarizationCoordinate::StokesI,
            PolarizationCoordinate::StokesQ,
            PolarizationCoordinate::StokesU,
            PolarizationCoordinate::StokesV,
        ],
        algorithm: ContinuumAlgorithm::Hogbom,
        weighting: ContinuumWeighting::Natural,
        iterations: 20,
        cycle_iterations: 20,
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
        task_requirements: vec![
            TaskRequirement::PolarizationSelection,
            TaskRequirement::SerialCpu,
            TaskRequirement::FixedTileCpu,
        ],
        resource_policy: casa_imaging_runtime::ResourcePolicy::Balanced,
    }
}

struct Product {
    shape: Vec<usize>,
    units: String,
    stokes: Vec<StokesType>,
    values: Vec<f32>,
    valid: Vec<bool>,
}

fn read_product(prefix: &Path, suffix: &str) -> Result<Product, Box<dyn Error>> {
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
    Ok(Product {
        shape,
        units: image.units().to_string(),
        stokes: stokes.stokes().to_vec(),
        values,
        valid,
    })
}

fn normalized_rms(rust: &[f32], casa: &[f32], valid: &[bool]) -> f64 {
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

fn set_production_io_environment() {
    unsafe {
        std::env::set_var("CASA_RS_IMAGING_SPILL_READ_BYTES_PER_SECOND", "1000000000");
        std::env::set_var("CASA_RS_IMAGING_SPILL_WRITE_BYTES_PER_SECOND", "1000000000");
    }
}
