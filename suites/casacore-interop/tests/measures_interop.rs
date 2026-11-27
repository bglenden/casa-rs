// SPDX-License-Identifier: LGPL-3.0-or-later
//! 2x2 interop tests for measures: Rust vs C++ casacore.
#![cfg(all(feature = "cpp-interop-tests", has_casacore_cpp))]

use casa_core::measures::direction::{DirectionRef, MDirection};
use casa_core::measures::doppler::{DopplerRef, MDoppler};
use casa_core::measures::frequency::{FrequencyRef, MFrequency};
use casa_core::measures::radial_velocity::{MRadialVelocity, RadialVelocityRef};
use casa_core::measures::{
    EpochRef, IauModel, MEpoch, MPosition, MeasFrame, MeasuresProvider, PositionRef,
    epoch_to_record, position_to_record,
};
use casa_measures::MeasuresRuntime;
use casa_test_support::measures_interop::MeasuresOracle;
use std::sync::{Arc, OnceLock};

const J2000_MJD: f64 = 51544.5;
const SECONDS_PER_DAY: f64 = 86_400.0;

// Direction for frequency tests: M31 in J2000
const M31_LON: f64 = 0.185_948_8; // ~10.68° RA
const M31_LAT: f64 = 0.722_777_4; // ~41.27° Dec
// VLA WGS84 coordinates
const VLA_LON: f64 = -1.878_283_2; // -107.618°
const VLA_LAT: f64 = 0.595_370_3; // 34.079°
const VLA_H: f64 = 2124.0;

fn measures_runtime() -> Arc<MeasuresRuntime> {
    static RUNTIME: OnceLock<Arc<MeasuresRuntime>> = OnceLock::new();
    Arc::clone(RUNTIME.get_or_init(|| {
        Arc::new(
            MeasuresRuntime::open_discovered(Default::default()).expect("test measures runtime"),
        )
    }))
}

fn measures_frame() -> MeasFrame {
    MeasFrame::new().with_measures(measures_runtime())
}

#[test]
#[ignore = "VLASS metadata diagnosis only"]
fn vlass_mfs_frequency_converter_lifecycle_probe() {
    let frequency_hz = 4_012_000_000.0;
    let direction_lon_rad = -2.746_631_494_525_407;
    let direction_lat_rad = 0.295_309_709_400_346_7;
    let epoch_mjd = 5_060_832_843.375 / SECONDS_PER_DAY;
    let observatory_lon_rad = -1.878_288_434_411_257_6;
    let observatory_lat_rad = 0.594_787_741_027_755_8;
    let observatory_height_m = 2_118.614_142_014_556_4;
    let initial_direction_lon_rad = -2.743_916_984_344_138_7;
    let initial_direction_lat_rad = 0.291_120_919_200_755_9;
    let initial_epoch_mjd = 5_060_832_436.125 / SECONDS_PER_DAY;
    let selected_initial_epoch_mjd = 5_060_832_454.125 / SECONDS_PER_DAY;

    for (name, result) in [
        (
            "direct",
            MeasuresOracle::frequency_convert(
                frequency_hz,
                "TOPO",
                "LSRK",
                direction_lon_rad,
                direction_lat_rad,
                "J2000",
                epoch_mjd,
                observatory_lon_rad,
                observatory_lat_rad,
                observatory_height_m,
            ),
        ),
        (
            "model",
            MeasuresOracle::frequency_convert_via_model(
                frequency_hz,
                "TOPO",
                "LSRK",
                direction_lon_rad,
                direction_lat_rad,
                "J2000",
                epoch_mjd,
                observatory_lon_rad,
                observatory_lat_rad,
                observatory_height_m,
            ),
        ),
        (
            "mutated-model",
            MeasuresOracle::frequency_convert_via_mutated_model(
                frequency_hz,
                "TOPO",
                "LSRK",
                direction_lon_rad,
                direction_lat_rad,
                "J2000",
                epoch_mjd,
                observatory_lon_rad,
                observatory_lat_rad,
                observatory_height_m,
            ),
        ),
        (
            "casa-msutil-reset-frame",
            MeasuresOracle::frequency_convert_via_reset_frame(
                frequency_hz,
                "TOPO",
                "LSRK",
                initial_direction_lon_rad,
                initial_direction_lat_rad,
                initial_epoch_mjd,
                direction_lon_rad,
                direction_lat_rad,
                epoch_mjd,
                "J2000",
                observatory_lon_rad,
                observatory_lat_rad,
                observatory_height_m,
            ),
        ),
        (
            "casa-msutil-scalar-field",
            MeasuresOracle::frequency_convert_via_reset_frame(
                frequency_hz,
                "TOPO",
                "LSRK",
                direction_lon_rad,
                direction_lat_rad,
                initial_epoch_mjd,
                direction_lon_rad,
                direction_lat_rad,
                epoch_mjd,
                "J2000",
                observatory_lon_rad,
                observatory_lat_rad,
                observatory_height_m,
            ),
        ),
        (
            "casa-msutil-selected-scalar-field",
            MeasuresOracle::frequency_convert_via_reset_frame(
                frequency_hz,
                "TOPO",
                "LSRK",
                direction_lon_rad,
                direction_lat_rad,
                selected_initial_epoch_mjd,
                direction_lon_rad,
                direction_lat_rad,
                epoch_mjd,
                "J2000",
                observatory_lon_rad,
                observatory_lat_rad,
                observatory_height_m,
            ),
        ),
    ] {
        eprintln!("{name}={:.17e}", result.unwrap());
    }
}

fn close(a: f64, b: f64, tol: f64) -> bool {
    (a - b).abs() < tol
}

/// Compare two angles, accounting for wrapping at ±π / 2π boundaries.
fn close_angle(a: f64, b: f64, tol: f64) -> bool {
    let diff = (a - b).rem_euclid(std::f64::consts::TAU);
    let diff = if diff > std::f64::consts::PI {
        std::f64::consts::TAU - diff
    } else {
        diff
    };
    diff < tol
}

/// Angular separation between two (lon, lat) positions on the sphere (radians).
/// Uses the Vincenty formula for numerical precision at all separations.
fn angular_sep(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let dlon = lon2 - lon1;
    let (s1, c1) = lat1.sin_cos();
    let (s2, c2) = lat2.sin_cos();
    let (sd, cd) = dlon.sin_cos();
    let y = ((c2 * sd).powi(2) + (c1 * s2 - s1 * c2 * cd).powi(2)).sqrt();
    let x = s1 * s2 + c1 * c2 * cd;
    y.atan2(x)
}

/// Angular separation in arcseconds (convenience wrapper).
fn sep_arcsec(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    angular_sep(lon1, lat1, lon2, lat2).to_degrees() * 3600.0
}

// VLA ITRF coordinates — reused across position tests.
const VLA_X: f64 = -1601185.4;
const VLA_Y: f64 = -5041977.5;
const VLA_Z: f64 = 3554875.9;

/// Extract a quantity value from a sub-record field in a RecordValue.
fn extract_quantity(rec: &casa_core::RecordValue, field: &str) -> f64 {
    match rec.get(field) {
        Some(casa_core::Value::Record(sub)) => match sub.get("value") {
            Some(casa_core::Value::Scalar(casa_core::ScalarValue::Float64(v))) => *v,
            other => panic!("expected Float64 in {field}/value, got {other:?}"),
        },
        other => panic!("expected Record for {field}, got {other:?}"),
    }
}

#[path = "measures_interop_cases/creation_and_conversion.rs"]
mod creation_and_conversion;
#[path = "measures_interop_cases/earth_orientation.rs"]
mod earth_orientation;
#[path = "measures_interop_cases/iau2000a.rs"]
mod iau2000a;
#[path = "measures_interop_cases/roundtrip_and_frames.rs"]
mod roundtrip_and_frames;
