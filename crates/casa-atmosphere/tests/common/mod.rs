// SPDX-License-Identifier: LGPL-3.0-or-later
//! Fixture reading and comparison for the ATM oracle tests.
//!
//! The fixtures in `tests/data` come from CASA's ATM C++, compiled and run
//! by `tools/atmosphere/build-atm-oracle.sh`.

#![allow(dead_code, reason = "each test binary uses part of the helpers")]

use casa_atmosphere::{AtmosphereProfile, AtmosphereType, ProfileParameters, SpectralGrid};
use std::path::PathBuf;

/// The atmosphere of an `absorption.json` profile (the oracle passes ATM's
/// pressure-step factor too, which ATM ignores).
pub fn sky_atmosphere(profile: &serde_json::Value) -> AtmosphereProfile {
    let number = u32::try_from(profile["atmosphere"].as_u64().unwrap()).unwrap();
    AtmosphereProfile::new(&ProfileParameters {
        altitude_m: num(&profile["altitude_m"]),
        ground_pressure_hpa: num(&profile["ground_pressure_hpa"]),
        ground_temperature_k: num(&profile["ground_temperature_k"]),
        lapse_rate_k_per_km: num(&profile["lapse_rate_k_per_km"]),
        relative_humidity_percent: num(&profile["relative_humidity_percent"]),
        water_vapour_scale_height_km: num(&profile["water_vapour_scale_height_km"]),
        pressure_step_hpa: num(&profile["pressure_step_hpa"]),
        top_altitude_km: num(&profile["top_altitude_km"]),
        atmosphere: AtmosphereType::from_number(number).unwrap(),
    })
    .unwrap()
}

/// The spectral grid of `absorption.json`'s windows.
pub fn sky_grid(windows: &serde_json::Value) -> SpectralGrid {
    let mut grid = SpectralGrid::new();
    for window in windows.as_array().unwrap() {
        let count = usize::try_from(window["channels"].as_u64().unwrap()).unwrap();
        let reference = usize::try_from(window["ref_chan"].as_u64().unwrap()).unwrap();
        grid.add_regular(
            count,
            reference,
            num(&window["ref_hz"]),
            num(&window["sep_hz"]),
        )
        .unwrap();
    }
    grid
}

/// 1e-13 of the largest magnitude in `values`: the floor below which a
/// difference is the rounding of a cancelled sum.
pub fn peak_floor(values: &[f64]) -> f64 {
    1e-13 * values.iter().fold(0.0_f64, |peak, v| peak.max(v.abs()))
}

/// A fixture file's path.
pub fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/data")
        .join(name)
}

/// A JSON fixture, parsed.
pub fn json(name: &str) -> serde_json::Value {
    let text =
        std::fs::read_to_string(fixture(name)).unwrap_or_else(|e| panic!("reading {name}: {e}"));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("parsing {name}: {e}"))
}

/// A little-endian f64 fixture.
pub fn f64_le(name: &str) -> Vec<f64> {
    let bytes = std::fs::read(fixture(name)).unwrap_or_else(|e| panic!("reading {name}: {e}"));
    assert_eq!(bytes.len() % 8, 0, "{name} is not a whole number of f64");
    bytes
        .as_chunks::<8>()
        .0
        .iter()
        .map(|b| f64::from_le_bytes(*b))
        .collect()
}

/// A JSON number.
pub fn num(value: &serde_json::Value) -> f64 {
    value
        .as_f64()
        .unwrap_or_else(|| panic!("{value} is not a number"))
}

/// A JSON array of numbers.
pub fn nums(value: &serde_json::Value) -> Vec<f64> {
    value
        .as_array()
        .unwrap_or_else(|| panic!("{value} is not an array"))
        .iter()
        .map(num)
        .collect()
}

/// The worst disagreement seen in one family of compared values.
///
/// Each value is compared as |rust − c++| ≤ rel·|c++| + floor, and the
/// family records the largest relative error among values above the
/// floor, so a report shows how close the port is, not just that it passed.
pub struct Agreement {
    family: String,
    rel: f64,
    worst_rel: f64,
    worst_at: String,
    worst_floor_fraction: f64,
    compared: usize,
    failures: Vec<String>,
}

impl Agreement {
    /// A family compared at relative tolerance `rel`.
    pub fn new(family: &str, rel: f64) -> Self {
        Self {
            family: family.to_string(),
            rel,
            worst_rel: 0.0,
            worst_at: String::new(),
            worst_floor_fraction: 0.0,
            compared: 0,
            failures: Vec::new(),
        }
    }

    /// Compares one value; `floor` is the absolute tolerance below which
    /// differences are rounding of a near-zero (cancelled) quantity. The
    /// fixtures are finite, so a NaN on either side fails.
    pub fn check(&mut self, rust: f64, cpp: f64, floor: f64, at: impl Fn() -> String) {
        self.compared += 1;
        let diff = (rust - cpp).abs();
        let ok = diff <= self.rel * cpp.abs() + floor;
        if floor > 0.0 {
            self.worst_floor_fraction = self.worst_floor_fraction.max(diff / floor);
        }
        if cpp.abs() > floor && cpp != 0.0 {
            let rel = diff / cpp.abs();
            if rel > self.worst_rel {
                self.worst_rel = rel;
                self.worst_at = at();
            }
        }
        if !ok && self.failures.len() < 20 {
            self.failures.push(format!(
                "{}: rust {rust:e} c++ {cpp:e} diff {diff:e} floor {floor:e}",
                at()
            ));
        }
    }

    /// Prints the family's worst relative error and panics on failures.
    pub fn finish(self) {
        println!(
            "{}: {} values, worst relative error {:.3e} at {}; worst difference {:.3} of the floor",
            self.family, self.compared, self.worst_rel, self.worst_at, self.worst_floor_fraction
        );
        assert!(
            self.failures.is_empty(),
            "{} disagrees with ATM:\n{}",
            self.family,
            self.failures.join("\n")
        );
    }
}
