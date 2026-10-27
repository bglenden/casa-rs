// SPDX-License-Identifier: LGPL-3.0-or-later
//! `casa_simulation::simutil` against CASA 6.7.6's `simutil`.
//!
//! `tests/data/simutil_casa.json` is what CASA returned for a grid of
//! inputs (`tools/simulation/simutil_oracle.py`). Pointing lists, pointing
//! files, directions, band names and polarization settings are text, which
//! the plan's acceptance requires to be CASA's ("pointing lists identical to
//! CASA's"), so they are compared exactly. Numbers are compared to a
//! tolerance, since bitwise agreement is not a goal: the receiver
//! efficiencies, temperatures and split angles to 1e-12, offsets to 1e-12
//! deg, and the approximate beams to 1e-9 (casa-rs reads the configuration
//! files with its own `readantenna`, whose positions agree with CASA's to
//! 1e-6 m).
//!
//! The documented departures (module docs of `casa_simulation::simutil`):
//! - where CASA's ALMA tiling raises `NameError` (no beam), casa-rs's
//!   pointings are those of the beam `spacing × 1.13 × √3`;
//! - where `noisetemp` raises `IndexError`, casa-rs keeps the last receiver
//!   temperature; at a frequency equal to the first band edge, where CASA
//!   reads the last temperature, casa-rs reads the first;
//! - where `direction_splitter` raises, casa-rs refuses the direction.

use std::path::{Path, PathBuf};

use casa_simulation::readantenna::read_antenna_config;
use casa_simulation::simutil::baselines::{approximate_beam, baseline_lengths};
use casa_simulation::simutil::direction::{
    Directions, average_direction, median_direction, split_direction, wrap_angle,
};
use casa_simulation::simutil::pointings::{
    PBCOEFF, calc_pointings, format_pointings, parse_pointings,
};
use casa_simulation::simutil::receivers::{band_name, noise_temperature, polarization_settings};
use casa_simulation::simutil::{SimutilError, python_float};
use casa_types::quanta::casacore::casacore_unit;
use casa_types::quanta::{Quantity, casacore};
use serde_json::Value;

#[path = "support/casa_observatories.rs"]
mod casa_observatories;
use casa_observatories::CasaObservatories;

/// Whether two numbers agree to 1e-12, relative (absolute near zero).
fn close(ours: f64, casa: f64) -> bool {
    (ours - casa).abs() <= 1e-12 * casa.abs().max(1.0)
}

fn data_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data")
}

fn fixture() -> Value {
    let text = std::fs::read_to_string(data_dir().join("simutil_casa.json"))
        .expect("the CASA fixture is checked in");
    serde_json::from_str(&text).expect("the CASA fixture is JSON")
}

fn cases<'a>(fixture: &'a Value, key: &str) -> &'a [Value] {
    fixture[key].as_array().unwrap_or_else(|| panic!("{key}"))
}

fn text(value: &Value) -> &str {
    value
        .as_str()
        .unwrap_or_else(|| panic!("a string, not {value}"))
}

fn strings(value: &Value) -> Vec<String> {
    value
        .as_array()
        .unwrap_or_else(|| panic!("a list, not {value}"))
        .iter()
        .map(|v| text(v).to_string())
        .collect()
}

/// A size or spacing as `qa.quantity` takes it: a string or a record.
fn quantity(value: &Value) -> Quantity {
    match value {
        Value::String(text) => Quantity::read(text).expect("a quantity"),
        Value::Object(record) => Quantity::with_unit(
            record["value"].as_f64().expect("a value"),
            casacore_unit(record["unit"].as_str().expect("a unit")).expect("a unit"),
        ),
        other => panic!("not a quantity: {other}"),
    }
}

/// A direction as CASA's `direction` takes one: a string or a list.
fn directions(value: &Value) -> Directions {
    match value {
        Value::String(direction) => Directions::One(direction.clone()),
        listed => Directions::List(strings(listed)),
    }
}

#[test]
fn pointing_lists_are_casas() {
    let fixture = fixture();
    let mut compared = 0;
    let (mut listed, mut refused) = (0, 0);
    let mut failures = Vec::new();
    for case in cases(&fixture, "calc_pointings2") {
        let given = directions(&case["direction"]);
        let direction = &case["direction"];
        listed += usize::from(matches!(given, Directions::List(_)));
        let spacing = quantity(&case["spacing"]);
        let sizes: Vec<Quantity> = match &case["size"] {
            Value::Array(sizes) => sizes.iter().map(quantity).collect(),
            single => vec![quantity(single); 2],
        };
        let maptype = text(&case["maptype"]);
        let relmargin = case["relmargin"].as_f64().expect("relmargin");
        let mut beam = case["beam"].as_f64().expect("beam");
        if let Some(raised) = case.get("refused") {
            // CASA raises: an unsplittable direction, or a count Python
            // cannot convert, with Python's message after the error's name.
            let ours = calc_pointings(
                &spacing,
                [&sizes[0], &sizes[1]],
                maptype,
                &given,
                relmargin,
                beam,
            );
            match ours {
                Err(SimutilError::Direction(_)) if text(raised).starts_with("Exception: ") => {}
                Err(SimutilError::Arithmetic(message)) if text(raised).ends_with(&message) => {}
                other => failures.push(format!("{case}: {other:?}, where CASA raises {raised}")),
            }
            refused += 1;
            continue;
        }
        let expected = if case.get("error").is_some() {
            // CASA's NameError: compare with the beam casa-rs defaults to.
            let spacing_arcsec =
                casacore::convert(&spacing, Some(&casacore_unit("arcsec").unwrap()))
                    .unwrap()
                    .value();
            let defaulted = calc_pointings(
                &spacing,
                [&sizes[0], &sizes[1]],
                maptype,
                &given,
                relmargin,
                beam,
            )
            .expect("casa-rs defaults the ALMA beam");
            beam = spacing_arcsec * PBCOEFF * 3.0_f64.sqrt();
            defaulted
        } else {
            strings(&case["pointings"])
        };
        let ours = calc_pointings(
            &spacing,
            [&sizes[0], &sizes[1]],
            maptype,
            &given,
            relmargin,
            beam,
        )
        .unwrap_or_else(|error| panic!("{case}: {error}"));
        compared += ours.len();
        if ours != expected {
            let first = ours
                .iter()
                .zip(&expected)
                .position(|(a, b)| a != b)
                .unwrap_or(ours.len().min(expected.len()));
            failures.push(format!(
                "{maptype} {} {} {direction}: {} pointings for {}; first difference at {first}: {:?} vs CASA {:?}",
                case["spacing"],
                case["size"],
                ours.len(),
                expected.len(),
                ours.get(first),
                expected.get(first)
            ));
        }
    }
    assert!(compared > 10_000, "{compared} pointings compared");
    // Lists of one direction (#913 review) and of more, and CASA's refusals.
    assert!(listed >= 11, "{listed} lists compared");
    assert!(refused >= 7, "{refused} refusals compared");
    assert!(
        failures.is_empty(),
        "{} cases differ:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[test]
fn pointing_files_read_and_write_as_casas() {
    let fixture = fixture();
    for case in cases(&fixture, "read_pointings") {
        let file =
            parse_pointings(text(&case["text"])).unwrap_or_else(|error| panic!("{case}: {error}"));
        assert_eq!(
            file.pointings,
            strings(&case["pointings"]),
            "{}",
            case["name"]
        );
        let times: Vec<f64> = case["times"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t.as_f64().unwrap())
            .collect();
        assert_eq!(file.times, times, "{}", case["name"]);
    }
    assert!(parse_pointings("# nothing\n").is_err());
    for case in cases(&fixture, "write_pointings") {
        let pointings = match &case["pointings"] {
            Value::String(one) => vec![one.clone()],
            many => strings(many),
        };
        // Python's `str` of each time: an int as it is, a float as Python writes it.
        let shown = |time: &Value| {
            if time.is_i64() || time.is_u64() {
                time.to_string()
            } else {
                python_float(time.as_f64().unwrap())
            }
        };
        let times: Vec<String> = match &case["time"] {
            Value::Array(times) => times.iter().map(shown).collect(),
            one => vec![shown(one)],
        };
        assert_eq!(
            format_pointings(&pointings, &times).unwrap(),
            text(&case["text"]),
            "{}",
            case["time"]
        );
    }
}

#[test]
fn directions_split_average_and_wrap_as_casas() {
    let fixture = fixture();
    for case in cases(&fixture, "direction_splitter") {
        // A list, even of one direction, is split at its mean.
        let direction = &case["direction"];
        let ours = match direction.as_str() {
            Some(direction) => split_direction(direction),
            None => directions(direction).split(),
        };
        if case.get("error").is_some() {
            assert!(ours.is_err(), "{direction}: CASA raises {}", case["error"]);
            continue;
        }
        let ours = ours.unwrap_or_else(|error| panic!("{direction}: {error}"));
        assert_eq!(ours.epoch, text(&case["epoch"]), "{direction}");
        assert!(
            close(ours.longitude.value(), case["x"].as_f64().unwrap()),
            "{direction}"
        );
        assert!(
            close(ours.latitude.value(), case["y"].as_f64().unwrap()),
            "{direction}"
        );
        assert_eq!(
            ours.longitude.unit().name(),
            text(&case["x_unit"]),
            "{direction}"
        );
    }
    for (key, centre) in [
        ("average_direction", average_direction as fn(&[String]) -> _),
        ("median_direction", median_direction),
    ] {
        for case in cases(&fixture, key) {
            let ours = centre(&strings(&case["directions"])).unwrap();
            assert_eq!(
                ours.centre,
                text(&case["direction"]),
                "{key} {}",
                case["directions"]
            );
            let offsets = case["offsets"].as_array().unwrap();
            for (i, offset) in ours.offsets.iter().enumerate() {
                for axis in 0..2 {
                    let casa = offsets[axis][i].as_f64().unwrap();
                    assert!(
                        (offset[axis] - casa).abs() <= 1e-12,
                        "{key} offset {i}.{axis}: {} vs CASA {casa}",
                        offset[axis]
                    );
                }
            }
        }
    }
    for case in cases(&fixture, "wrapang") {
        let get = |key: &str| case[key].as_f64().unwrap();
        assert!(
            close(
                wrap_angle(get("ang"), get("target"), get("period")),
                get("out")
            ),
            "{case}"
        );
    }
}

/// The receiver tables' first band edges, where casa-rs reads the first
/// temperature and CASA the last (`t0[z-1]` with `z = 0`).
fn at_first_edge(telescope: &str, freq: &str) -> bool {
    let telescope = telescope.to_uppercase();
    (matches!(telescope.as_str(), "ALMA" | "ACA" | "ALMASD") && freq == "35GHz")
        || (telescope == "EVLA" && freq == "1GHz")
}

#[test]
fn receivers_bands_and_feeds_are_casas() {
    let fixture = fixture();
    let mut departures = 0;
    for case in cases(&fixture, "noisetemp") {
        let telescope = text(&case["telescope"]);
        let freq = text(&case["freq"]);
        let ours = noise_temperature(
            telescope,
            &Quantity::read(freq).unwrap(),
            case["diam"].as_f64(),
            case["epsilon"].as_f64(),
            case["correlator"].as_str(),
        )
        .unwrap();
        let ours = [
            ours.phase,
            ours.spillover,
            ours.blockage,
            ours.taper,
            ours.correlator,
            ours.receiver_temperature_k,
        ];
        if case.get("error").is_some() {
            // CASA's IndexError above an unknown telescope's last edge.
            assert_eq!(ours[5], 200.0, "{case}");
            departures += 1;
            continue;
        }
        let casa: Vec<f64> = case["result"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_f64().unwrap())
            .collect();
        assert!(
            ours[..5].iter().zip(&casa[..5]).all(|(a, b)| close(*a, *b)),
            "{case}: {ours:?}"
        );
        if at_first_edge(telescope, freq) {
            assert!(!close(ours[5], casa[5]), "{case}: CASA's last temperature");
            departures += 1;
        } else {
            assert!(close(ours[5], casa[5]), "{case}: {ours:?}");
        }
    }
    assert!(departures > 0, "the departures are exercised");
    for case in cases(&fixture, "bandname") {
        assert_eq!(
            band_name(case["freq"].as_f64().unwrap()),
            text(&case["band"]),
            "{case}"
        );
    }
    for case in cases(&fixture, "polsettings") {
        let (stokes, feed) =
            polarization_settings(text(&case["telescope"]), case["poltype"].as_str());
        assert_eq!(
            vec![stokes.to_string(), feed.to_string()],
            strings(&case["out"]),
            "{case}"
        );
    }
}

#[test]
fn approximate_beams_are_casas() {
    let fixture = fixture();
    let measures = CasaObservatories::checked_in();
    let mut compared = 0;
    for case in cases(&fixture, "beams") {
        let name = text(&case["config"]);
        let config = read_antenna_config(&data_dir().join("readantenna").join(name), &measures)
            .unwrap_or_else(|error| panic!("{name}: {error}"));
        if let Some(beam) = case["beam_arcsec"].as_f64() {
            let ours = approximate_beam(&config, case["freq_ghz"].as_f64().unwrap()).unwrap();
            assert!(
                ((ours - beam) / beam).abs() < 1e-9,
                "{name}: {ours} vs CASA {beam}"
            );
            compared += 1;
        } else {
            let lengths = baseline_lengths(&config).unwrap();
            assert_eq!(
                lengths.len() as u64,
                case["baselines"].as_u64().unwrap(),
                "{name}"
            );
            let sum: f64 = lengths.iter().sum();
            let casa_sum = case["sum_m"].as_f64().unwrap();
            assert!(
                ((sum - casa_sum) / casa_sum).abs() < 1e-9,
                "{name}: {sum} vs CASA {casa_sum}"
            );
            let max = lengths.iter().copied().fold(0.0, f64::max);
            assert!(
                (max - case["max_m"].as_f64().unwrap()).abs() < 1e-5,
                "{name}"
            );
        }
    }
    assert!(compared >= 20, "{compared} beams compared");
}
