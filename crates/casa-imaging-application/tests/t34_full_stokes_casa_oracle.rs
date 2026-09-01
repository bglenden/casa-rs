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
use casa_ms::MeasurementSet;
use casa_test_support::{CasaTestDataTier, casatestdata_path_for_tier};
use casa_types::ArrayValue;

const DATASET: &str = "measurementset/vla/refim_point_stokes.ms";
const CASA_PREFIX_ENV: &str = "CASA_RS_T34_CASA_PREFIX";
const PRODUCTS: [&str; 5] = [".psf", ".residual", ".model", ".image", ".sumwt"];

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

    let mut failures = Vec::new();
    for product in PRODUCTS {
        let rust = read_product(&rust_prefix, product)?;
        let casa = read_product(&casa_prefix, product)?;
        assert_eq!(rust.shape, casa.shape, "{product} shape");
        let expected_units = match product {
            ".psf" | ".residual" | ".image" => "Jy/beam",
            ".model" => "Jy/pixel",
            ".sumwt" => "",
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
        if product == ".sumwt" {
            eprintln!("t34_sumwt rust={:?} casa={:?}", rust.values, casa.values);
            for (actual, expected) in rust.values.iter().zip(selected.sum_weights) {
                if (f64::from(*actual) - expected).abs() > 1.0e-5 * expected.max(1.0) {
                    failures.push(format!(
                        ".sumwt Rust value {actual} != selected CASA flag/weight reduction {expected}"
                    ));
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

struct SelectedCorrelationContract {
    sample_count: u64,
    sum_weights: [f64; 4],
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
        sum_weights,
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
