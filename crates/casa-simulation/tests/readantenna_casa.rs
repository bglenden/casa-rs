// SPDX-License-Identifier: LGPL-3.0-or-later
//! `readantenna` against CASA's `simutil.readantenna`.
//!
//! `tests/data/readantenna_casa.json` holds what CASA 6.7 returned for six of
//! CASA's own configuration files (copied under `tests/data/readantenna/`:
//! XYZ, LOC, LOC by prefix name, UTM on SAM56 and UTM on NAD27) and for
//! synthetic files covering the remaining rules and errors, whose text the
//! JSON carries. It also holds CASA's observatory table, each entry in the
//! frame CASA tabulates it in, which the first test serves through a
//! measures provider so that the comparison isolates `readantenna` itself.
//! The second test uses casa-rs's own measures runtime when one is
//! installed.
//!
//! Tolerances:
//! - positions 1e-6 m. CASA's `COFA` cases put the observatory at casacore's
//!   1e-6 m floor for a zero-height position, so their local-coordinate
//!   antennas sit 1e-6 m higher in CASA: 2e-6 m there;
//! - observatory angles 1e-12 rad (casa-types' SOFA conversion of ITRF
//!   entries and casacore's iteration differ by about 1e-14 rad), heights
//!   1e-6 m (2e-6 m for `COFA`);
//! - diameters, names and the telescope exactly.
//!
//! The rules for CASA's recorded bugs (module docs of
//! `casa_simulation::readantenna`):
//! - CASA's name lists carry one extra leading `A%02d` entry per blank line,
//!   so its names are compared after dropping the extra leading entries;
//! - where CASA averages a stale variable or raises (`UnboundLocalError`),
//!   the observatory is compared with `intended_mean_wgs84`, CASA's own
//!   `xyz2long` of the mean antenna position, which the oracle computed.

use std::path::{Path, PathBuf};

use casa_simulation::readantenna::{
    AntennaConfigFile, ObservatorySource, ReadAntennaError, geodesy, read_antenna_config,
    read_antenna_config_str,
};
use casa_types::measures::{MPosition, MeasuresProvider, ObservatoryPosition};
use serde_json::Value;

fn data_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data")
}

fn fixture() -> Value {
    let path = data_dir().join("readantenna_casa.json");
    let text = std::fs::read_to_string(&path).expect("the CASA fixture is checked in");
    serde_json::from_str(&text).expect("the CASA fixture is JSON")
}

fn number(value: &Value, key: &str) -> f64 {
    value[key]
        .as_f64()
        .unwrap_or_else(|| panic!("{key} in {value}"))
}

fn numbers(value: &Value, key: &str) -> Vec<f64> {
    value[key]
        .as_array()
        .unwrap_or_else(|| panic!("{key}"))
        .iter()
        .map(|v| v.as_f64().expect("a number"))
        .collect()
}

fn strings(value: &Value, key: &str) -> Vec<String> {
    value[key]
        .as_array()
        .unwrap_or_else(|| panic!("{key}"))
        .iter()
        .map(|v| v.as_str().expect("a string").to_string())
        .collect()
}

/// CASA's observatory table, looked up as casa-measures-data looks names up
/// (trimmed, case-insensitive, first match).
#[derive(Debug)]
struct CasaObservatories(Vec<(String, ObservatoryPosition)>);

impl CasaObservatories {
    fn from_fixture(fixture: &Value) -> Self {
        let entries = fixture["observatories"]
            .as_array()
            .expect("observatories")
            .iter()
            .map(|entry| {
                let name = entry["name"].as_str().expect("name").to_string();
                let position = if entry["refer"] == "WGS84" {
                    ObservatoryPosition::Wgs84 {
                        longitude_rad: number(entry, "longitude_rad"),
                        latitude_rad: number(entry, "latitude_rad"),
                        height_m: number(entry, "height_m"),
                    }
                } else {
                    ObservatoryPosition::Itrf {
                        x_m: number(entry, "x_m"),
                        y_m: number(entry, "y_m"),
                        z_m: number(entry, "z_m"),
                    }
                };
                (name, position)
            })
            .collect();
        Self(entries)
    }
}

impl MeasuresProvider for CasaObservatories {
    fn observatory(&self, name: &str) -> Result<Option<ObservatoryPosition>, String> {
        let name = name.trim();
        Ok(self
            .0
            .iter()
            .find(|(entry, _)| entry.eq_ignore_ascii_case(name))
            .map(|(_, position)| position.clone()))
    }
}

fn read_case(
    case: &Value,
    measures: &dyn MeasuresProvider,
) -> Result<AntennaConfigFile, ReadAntennaError> {
    match case["file"].as_str() {
        Some(file) => read_antenna_config(&data_dir().join(file), measures),
        None => read_antenna_config_str(case["text"].as_str().expect("text"), measures),
    }
}

/// The largest differences of one case, for the report.
#[derive(Debug, Default)]
struct Differences {
    position_m: f64,
    angle_rad: f64,
    height_m: f64,
}

fn max_abs_difference(got: &[f64], want: &[f64]) -> f64 {
    assert_eq!(got.len(), want.len(), "lengths");
    got.iter()
        .zip(want)
        .map(|(g, w)| (g - w).abs())
        .fold(0.0, f64::max)
}

fn compare_observatory(got: &MPosition, want: &Value, differences: &mut Differences) {
    let [lon, lat, height] = got.values();
    differences.angle_rad = (lon - number(want, "longitude_rad"))
        .abs()
        .max((lat - number(want, "latitude_rad")).abs());
    differences.height_m = (height - number(want, "height_m")).abs();
}

/// CASA's extra leading default names (one per blank line) dropped.
fn without_leading_defaults<'a>(casa: &'a [String], ours: &[String]) -> &'a [String] {
    let extra = casa.len().saturating_sub(ours.len());
    let (defaults, rest) = casa.split_at(extra);
    for name in defaults {
        let digits = name.strip_prefix('A').unwrap_or("");
        assert!(
            digits.len() >= 2 && digits.bytes().all(|b| b.is_ascii_digit()),
            "CASA's extra name {name} is not a default name"
        );
    }
    rest
}

fn compare_result(name: &str, case: &Value, ours: &AntennaConfigFile) -> Differences {
    let casa = &case["result"];
    let mut differences = Differences::default();
    for (axis, values) in [("x", &ours.x), ("y", &ours.y), ("z", &ours.z)] {
        let difference = max_abs_difference(values, &numbers(casa, axis));
        differences.position_m = differences.position_m.max(difference);
    }
    assert_eq!(
        ours.dish_diameter,
        numbers(casa, "diam"),
        "{name}: diameters"
    );
    let pads = strings(casa, "pads");
    let names = strings(casa, "names");
    assert_eq!(
        without_leading_defaults(&pads, &ours.pad_names),
        ours.pad_names,
        "{name}: pads"
    );
    assert_eq!(
        without_leading_defaults(&names, &ours.antenna_names),
        ours.antenna_names,
        "{name}: names"
    );
    assert_eq!(
        ours.telescope,
        casa["telescope"].as_str().expect("telescope"),
        "{name}"
    );
    if case.get("intended_mean_wgs84").is_some() {
        assert_eq!(
            ours.observatory_source,
            ObservatorySource::MeanAntennaPosition,
            "{name}"
        );
        compare_observatory(
            &ours.observatory,
            &case["intended_mean_wgs84"],
            &mut differences,
        );
        assert_casa_swapped_last_antenna(name, casa);
    } else {
        compare_observatory(&ours.observatory, &casa["observatory"], &mut differences);
    }
    differences
}

/// CASA's own position for an unknown observatory with UTM antennas is the
/// last antenna with longitude and latitude swapped (the bug the module
/// docs record): check that diagnosis against CASA's numbers.
fn assert_casa_swapped_last_antenna(name: &str, casa: &Value) {
    let last = |axis| *numbers(casa, axis).last().expect("antennas");
    let (lon, lat, height) =
        geodesy::xyz_to_geodetic(last("x"), last("y"), last("z"), &geodesy::WGS84).expect("valid");
    let swapped = MPosition::new_wgs84(lat, lon, height).as_itrf();
    let observatory = &casa["observatory"];
    let recorded = MPosition::new_wgs84(
        number(observatory, "longitude_rad"),
        number(observatory, "latitude_rad"),
        number(observatory, "height_m"),
    )
    .as_itrf();
    assert!(
        max_abs_difference(&swapped, &recorded) < 1e-3,
        "{name}: CASA's position is not the swapped last antenna"
    );
}

#[test]
fn readantenna_agrees_with_casa() {
    let fixture = fixture();
    let measures = CasaObservatories::from_fixture(&fixture);
    let cases = fixture["cases"].as_array().expect("cases");
    assert!(cases.len() >= 15);
    for case in cases {
        let name = case["name"].as_str().expect("name");
        let ours = read_case(case, &measures);
        if let Some(error) = case.get("error") {
            if error["type"] == "UnboundLocalError" {
                // CASA averages a variable that does not exist; compare with
                // its intended mean position.
                let ours = ours.unwrap_or_else(|e| panic!("{name}: {e}"));
                let mut differences = Differences::default();
                assert_eq!(
                    ours.observatory_source,
                    ObservatorySource::MeanAntennaPosition
                );
                compare_observatory(
                    &ours.observatory,
                    &case["intended_mean_wgs84"],
                    &mut differences,
                );
                println!("{name}: CASA raises UnboundLocalError; intended mean {differences:?}");
                assert!(
                    differences.angle_rad < 1e-12 && differences.height_m < 1e-6,
                    "{name}"
                );
            } else {
                let message = error["message"].as_str().expect("CASA's message");
                let ours = ours
                    .err()
                    .unwrap_or_else(|| panic!("{name}: CASA fails, we read"));
                assert_eq!(ours.to_string(), message, "{name}");
                println!("{name}: same error as CASA");
            }
            continue;
        }
        let ours = ours.unwrap_or_else(|e| panic!("{name}: {e}"));
        let differences = compare_result(name, case, &ours);
        println!("{name}: {differences:?}");
        let cofa = ours.observatory_source == ObservatorySource::Cofa;
        let (position, height) = if cofa { (2e-6, 2e-6) } else { (1e-6, 1e-6) };
        assert!(
            differences.position_m <= position,
            "{name}: {differences:?}"
        );
        assert!(differences.angle_rad <= 1e-12, "{name}: {differences:?}");
        assert!(differences.height_m <= height, "{name}: {differences:?}");
    }
}

#[test]
fn casa_rs_observatory_table_agrees_with_casas() {
    let measures = match casa_ms::open_measures_runtime() {
        Ok(measures) => measures,
        Err(error) => {
            println!("skipping: no casa-rs measures runtime ({error})");
            return;
        }
    };
    let fixture = fixture();
    let casa = CasaObservatories::from_fixture(&fixture);
    let mut worst = Differences::default();
    for case in fixture["cases"].as_array().expect("cases") {
        let name = case["name"].as_str().expect("name");
        let (Ok(ours), Ok(theirs)) = (read_case(case, measures.as_ref()), read_case(case, &casa))
        else {
            continue;
        };
        if ours.observatory_source != ObservatorySource::Catalog {
            continue;
        }
        let mut differences = Differences::default();
        for (axis_ours, axis_casa) in [
            (&ours.x, &theirs.x),
            (&ours.y, &theirs.y),
            (&ours.z, &theirs.z),
        ] {
            differences.position_m = differences
                .position_m
                .max(max_abs_difference(axis_ours, axis_casa));
        }
        let [lon, lat, height] = ours.observatory.values();
        let [lon_casa, lat_casa, height_casa] = theirs.observatory.values();
        differences.angle_rad = (lon - lon_casa).abs().max((lat - lat_casa).abs());
        differences.height_m = (height - height_casa).abs();
        println!("{name} with the casa-rs runtime: {differences:?}");
        worst.position_m = worst.position_m.max(differences.position_m);
        worst.angle_rad = worst.angle_rad.max(differences.angle_rad);
        worst.height_m = worst.height_m.max(differences.height_m);
    }
    println!("largest differences from CASA's observatory table: {worst:?}");
    assert!(
        worst.position_m <= 1e-6 && worst.angle_rad <= 1e-12 && worst.height_m <= 1e-6,
        "{worst:?}"
    );
}
