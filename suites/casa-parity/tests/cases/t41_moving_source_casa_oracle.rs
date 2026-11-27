// SPDX-License-Identifier: LGPL-3.0-or-later

//! Frozen CASA Uranus cubesource and moving-field MFS gates.

use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
};

use casa_coordinates::CoordinateModel;
use casa_core::Correlation;
use casa_core::measures::frequency::FrequencyRef;
use casa_images::PagedImage;
use casa_imaging::execute;
use casa_test_support::{CasaTestDataTier, casatestdata_path_for_tier};
use serde_json::json;

use super::imaging;

const DATASET: &str = "measurementset/alma/alma_ephemobj_icrs.ms";
const CASA_PREFIX_ENV: &str = "CASA_RS_T41_CASA_PREFIX";
const PRODUCTS: [&str; 5] = [".psf", ".residual", ".model", ".image", ".sumwt"];
const SELECTED_ROWS: u64 = 1_620;
const SELECTED_CHANNELS: u32 = 1_024;
/// The 1,080 unflagged cross baselines each place 1,007 in-band REST samples.
/// The two parallel hands contribute to the same Stokes I placement.
const PLACED_SAMPLES: u64 = 1_080 * 1_007;

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

    // A dirty 16-channel source-frame cube of field 1 tracking its
    // ephemeris (#942), each output channel 64 native channels wide.
    let mut failures = Vec::new();
    let rust_prefix = staging.path().join("rust-uranus-cubesource");
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
    let samples = result.scientific().normal_state().sample_count();
    let [source] = result
        .problem
        .observation_transaction()
        .read_set()
        .sources()
    else {
        return Err("expected one Uranus input".into());
    };
    let selected = source.selection();
    assert_eq!(
        selected.samples(),
        SELECTED_ROWS,
        "selected MS time/baseline rows"
    );
    let [window] = selected.spectral_windows() else {
        return Err("expected one Uranus spectral window".into());
    };
    assert_eq!(window.spectral_window_id(), 0);
    assert!(
        window
            .channel_indices()
            .iter()
            .copied()
            .eq(0..SELECTED_CHANNELS),
        "the production selection must retain all native channels (#986)"
    );
    for partition in selected.partitions() {
        let hand = partition
            .products()
            .iter()
            .map(|product| product.correlation_type());
        assert!(
            hand.eq([Correlation::XX, Correlation::YY]),
            "both input hands are selected"
        );
    }
    eprintln!(
        "t41 selected rows={} placed samples={samples}",
        selected.samples()
    );
    if samples != PLACED_SAMPLES {
        failures.push(format!(
            "placed {samples} samples, expected {PLACED_SAMPLES}"
        ));
    }
    assert_matching_wcs(&rust_prefix, &casa_prefix)?;
    failures.extend(compare_with_casa(&rust_prefix, &casa_prefix)?);
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

/// CASA 6.7.6.14 `tclean(field="1", spw="0", specmode="mfs")` with the
/// default phase centre and output frame records this MFS spectral
/// increment. `MSUtil::getFreqRangeInSpw` converts towards
/// `phaseDirMeas(field)` at `FIELD.TIME` while the conversion epoch follows
/// each selected integration; converting towards the body's direction at
/// each integration's time instead misses it.
#[test]
#[ignore = "requires the slow-parity ALMA ephemeris MeasurementSet"]
fn moving_field_mfs_envelope_matches_casa_field_origin_direction() -> Result<(), Box<dyn Error>> {
    const CASA_INCREMENT_HZ: f64 = 125_020_996.111_328_12;
    let source = casatestdata_path_for_tier(CasaTestDataTier::SlowParity, DATASET)
        .ok_or("the slow-parity ALMA ephemeris MeasurementSet is unavailable")?;
    let staging = tempfile::tempdir()?;
    let measurement_set = staging.path().join("alma_ephemobj_icrs.ms");
    copy_tree(&source, &measurement_set)?;
    let prefix = staging.path().join("moving-mfs");
    let request = imaging::request(json!({
        "vis": measurement_set,
        "imagename": prefix,
        "imsize": 64,
        "cell": "0.1arcsec",
        "field": "1",
        "spw": "0",
        "datacolumn": "DATA",
        "specmode": "mfs",
        "niter": 0,
    }));
    // The envelope spans field 1's selected rows whatever this small grid
    // keeps of them; the gridded count is not compared here.
    execute(&request, imaging::context(request.resource_policy()))?;
    let image = PagedImage::<f32>::open(PathBuf::from(format!("{}.psf", prefix.display())))?;
    let CoordinateModel::Spectral(spectral) = image.coordinates().coordinate(2) else {
        return Err("the MFS PSF has no spectral coordinate".into());
    };
    assert_eq!(spectral.world_frequency_ref(), FrequencyRef::LSRK);
    let increment = image.coordinates().coordinate(2).increment()[0];
    let relative_error = (increment - CASA_INCREMENT_HZ).abs() / CASA_INCREMENT_HZ;
    eprintln!(
        "moving MFS increment: native={increment:.8} CASA={CASA_INCREMENT_HZ:.8} \
         relative_error={relative_error:.3e}"
    );
    assert!(
        relative_error <= 2.0e-10,
        "MFS increment differs from CASA: {relative_error:.3e}"
    );
    Ok(())
}

/// How the dirty products under `rust_prefix` depart from CASA's.
fn compare_with_casa(
    rust_prefix: &Path,
    casa_prefix: &Path,
) -> Result<Vec<String>, Box<dyn Error>> {
    let casa_primary_beam = read_product(casa_prefix, ".pb")?;
    let mut failures = Vec::new();
    for suffix in PRODUCTS {
        let rust = read_product(rust_prefix, suffix)?;
        let casa = read_product(casa_prefix, suffix)?;
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
            "t41_casa_parity {} product={suffix} nrms={nrms:.9e} rust_peak={} casa_peak={}",
            rust_prefix.display(),
            rust_stats.maximum,
            casa_stats.maximum,
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
    Ok(failures)
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
    for [x, y] in [
        [0.0, 0.0],
        [0.0, 511.0],
        [256.0, 256.0],
        [511.0, 0.0],
        [511.0, 511.0],
    ] {
        for channel in 0..16 {
            let pixel = [x, y, 0.0, f64::from(channel)];
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
    }
    for image in [&rust, &casa] {
        let CoordinateModel::Spectral(spectral) = image.coordinates().coordinate(2) else {
            return Err("T41 product has no spectral coordinate".into());
        };
        assert_eq!(spectral.world_frequency_ref(), FrequencyRef::REST);
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
