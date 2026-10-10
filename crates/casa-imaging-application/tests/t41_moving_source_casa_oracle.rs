// SPDX-License-Identifier: LGPL-3.0-or-later

//! Focused T41 moving-source gate against a frozen CASA Uranus cube, and the
//! casa-ms Measures edge topology of the selected spectral range on the
//! representative T41 observation.

use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
};

use casa_coordinates::CoordinateModel;
use casa_images::PagedImage;
use casa_imaging_application::execute;
use casa_imaging_model::SpectralWindowSelection;
use casa_ms::{
    MeasurementSet, MsSelectionIoBudget, SelectedObservationContentBudget,
    SelectedObservationEphemeris, SelectedObservationRow,
};
use casa_test_support::{CasaTestDataTier, casatestdata_path_for_tier};
use casa_types::measures::frequency::FrequencyRef;
use serde_json::json;

#[path = "common/imaging.rs"]
mod imaging;

const DATASET: &str = "measurementset/alma/alma_ephemobj_icrs.ms";
const CASA_PREFIX_ENV: &str = "CASA_RS_T41_CASA_PREFIX";
const PRODUCTS: [&str; 5] = [".psf", ".residual", ".model", ".image", ".sumwt"];
const SELECTED_SAMPLE_COUNT: u64 = 1_620 * 1_024 * 2;
// The selected-range gate pins the casa-ms Measures edge topology on the
// representative T41 observation; it executes no imaging.
const MVC_MS_ENV: &str = "CASA_RS_T41_MVC_MS";

#[test]
#[ignore = "requires the representative T41 MS and frozen CASA MVC spectral coordinates"]
fn t41_mvc_selected_spectral_range_matches_casa_edge_topology() -> Result<(), Box<dyn Error>> {
    set_mmap_io_environment();
    let measurement_set = MeasurementSet::open(required_table(MVC_MS_ENV)?)?;
    let row_selection =
        measurement_set.selected_observation_row_selection(&[0, 1], Some(&[1]), None, None)?;
    let mut first_time_mjd_seconds = None;
    let selection_io = MsSelectionIoBudget {
        available_bytes: 64 << 20,
        maximum_live_blocks: 2,
        requested_bytes_per_row: SelectedObservationRow::STORAGE_BYTES_PER_ROW,
        storage_alignment_rows: None,
    };
    measurement_set.visit_selected_observation_rows(&row_selection, selection_io, |row| {
        first_time_mjd_seconds.get_or_insert(row.time_mjd_seconds());
    })?;
    let first_time_mjd_seconds = first_time_mjd_seconds.ok_or("empty T41 selection")?;
    let engine = casa_ms::derived::engine::MsCalEngine::new(&measurement_set)?;
    let ephemeris = SelectedObservationEphemeris::tracked_fields(
        &measurement_set,
        [1],
        SelectedObservationContentBudget::new(64 << 20, 2, 4).reference_data_budget(),
    )?;
    let phase =
        engine.ephemeris_direction_j2000(first_time_mjd_seconds, 1, "TRACKFIELD", &ephemeris)?;
    let range = measurement_set.selected_observation_spectral_range(
        &row_selection,
        &[
            SpectralWindowSelection::new(0, (0..1_024).collect()),
            SpectralWindowSelection::new(1, (0..256).collect()),
        ],
        FrequencyRef::TOPO,
        FrequencyRef::LSRK,
        1,
        first_time_mjd_seconds,
        phase,
        Some(&ephemeris),
        &engine,
        selection_io,
    )?;
    let [low_hz, high_hz] = range.selected_edges_hz();
    let [reference_low_hz, reference_high_hz] = range.reference_edges_hz();
    let increment_hz = (high_hz - low_hz) / 40.0;
    let first_centre_hz = low_hz.max(reference_low_hz) + increment_hz / 2.0;
    let public_reference_hz = first_centre_hz + 19.5 * increment_hz;
    eprintln!(
        "t41_mvc_range low={low_hz:.17} high={high_hz:.17} reference_low={reference_low_hz:.17} reference_high={reference_high_hz:.17} first_centre={first_centre_hz:.17} public_reference={public_reference_hz:.17} rows={} evaluations={}",
        range.measurements().selected_rows(),
        range.measurements().edge_evaluations(),
    );

    // The current Rust Measures transform follows CASA's selected-row extrema
    // algorithm and edge/centre topology. Its high-edge conversion differs by
    // 4.22 Hz on this frozen observation, which this evidence gate bounds
    // without changing either implementation's coordinates.
    assert!((low_hz - 230_388_238_202.374_33).abs() <= 5.0);
    assert!((high_hz - 235_307_541_333.341_28).abs() <= 5.0);
    assert!((first_centre_hz - 230_449_729_492.188_84).abs() <= 5.0);
    assert!((public_reference_hz - 232_847_889_768.535_16).abs() <= 5.0);
    assert_eq!(range.measurements().selected_rows(), 3_240);
    assert_eq!(range.measurements().edge_evaluations(), 120);
    Ok(())
}

#[test]
#[ignore = "requires slow-parity casatestdata and matching frozen CASA T41 products"]
fn t41_tracked_cubesource_matches_casa_geometry_and_dirty_products() -> Result<(), Box<dyn Error>> {
    let source = casatestdata_path_for_tier(CasaTestDataTier::SlowParity, DATASET)
        .ok_or("slow-parity casatestdata root is unavailable")?;
    let casa_prefix = PathBuf::from(
        std::env::var_os(CASA_PREFIX_ENV).ok_or("CASA_RS_T41_CASA_PREFIX is not set")?,
    );
    let staging = tempfile::tempdir()?;
    let measurement_set = staging.path().join("alma_ephemobj_icrs.ms");
    copy_tree(&source, &measurement_set)?;
    set_mmap_io_environment();
    let rust_prefix = staging.path().join("rust-uranus-cubesource");

    // A dirty 16-channel source-frame cube of field 1 tracking its
    // ephemeris, each output channel 64 native channels wide.
    let request = imaging::request(json!({
        "vis": measurement_set,
        "imagename": rust_prefix,
        "imsize": 512,
        "cell": "0.1arcsec",
        "phasecenter": "TRACKFIELD",
        "field": "1",
        "spw": "0",
        "datacolumn": "DATA",
        "specmode": "cubesource",
        "outframe": "REST",
        "start": "0",
        "width": "64",
        "channel_count": 16,
        "minpsffraction": 0.1,
        "pblimit": 0.1,
    }));
    let result = execute(&request, imaging::context(request.resource_policy()))?;
    assert_eq!(
        result.scientific.normal_state().sample_count(),
        SELECTED_SAMPLE_COUNT,
        "production traversal must retain all 1,024 channels and both parallel hands",
    );

    assert_matching_wcs(&rust_prefix, &casa_prefix)?;
    let casa_primary_beam = read_product(&casa_prefix, ".pb")?;
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
        if matches!(suffix, ".residual" | ".image") {
            assert_eq!(
                casa.valid, casa_primary_beam.valid,
                "CASA {suffix} validity is exactly its primary-beam blanking support; broader PB blanking remains owned by T47/#533"
            );
        } else if rust.valid != casa.valid {
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

fn required_table(name: &str) -> Result<PathBuf, Box<dyn Error>> {
    let path = PathBuf::from(std::env::var_os(name).ok_or_else(|| format!("{name} is not set"))?);
    if !path.is_dir() {
        return Err(format!("{name} does not name a table: {}", path.display()).into());
    }
    Ok(path)
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

fn set_mmap_io_environment() {
    // SAFETY: this ignored gate runs serially before any MeasurementSet is opened.
    unsafe {
        std::env::set_var("CASA_RS_IO_BACKEND", "mmap");
        std::env::set_var("CASA_RS_IO_MMAP_TILES", "true");
    }
}
