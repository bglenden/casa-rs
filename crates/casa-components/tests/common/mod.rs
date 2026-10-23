// SPDX-License-Identifier: LGPL-3.0-or-later
//! Comparing a casa-rs component list with what CASA reads from a table.
//!
//! `tools/simulation/componentlist_oracle.py dump` records, for a table, CASA's
//! `cl.getcomponent(i)` for every component and every cell as `tb.getcell`
//! returns it. [`assert_matches_casa`] checks a [`ComponentList`] against
//! both: the cells exactly (they are the stored numbers), and the
//! `getcomponent` records to 1e-14 relative, after converting CASA's display
//! units (arcmin, degrees, GHz) to radians and hertz.

#![allow(dead_code)]

use std::path::{Path, PathBuf};

use casa_components::{
    ComponentList, Polarisation, ShapeType, SkyComponent, SpectralKind, SpectralType,
};
use casa_types::measures::direction::MDirection;
use casa_types::quanta::{Quantity, Unit};
use num_complex::Complex64;
use serde_json::Value;

/// The committed fixtures written by CASA 6.7.6.
pub fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/casa-6.7.6")
}

/// Copy a committed table into `scratch`, so that opening it (which creates
/// casacore's lock file) leaves the source tree untouched.
pub fn copy_table(source: &Path, scratch: &Path) -> PathBuf {
    let target = scratch.join(source.file_name().expect("table name"));
    copy_dir(source, &target);
    target
}

/// Copy the files of table directory `source` into a new directory `target`.
pub fn copy_dir(source: &Path, target: &Path) {
    std::fs::create_dir_all(target).expect("create copy");
    for entry in std::fs::read_dir(source).expect("read table directory") {
        let entry = entry.expect("table entry");
        std::fs::copy(entry.path(), target.join(entry.file_name())).expect("copy table file");
    }
}

pub fn load_json(path: &Path) -> Value {
    let text =
        std::fs::read_to_string(path).unwrap_or_else(|error| panic!("{}: {error}", path.display()));
    serde_json::from_str(&text).expect("valid JSON")
}

/// Check `list` against a CASA dump of the same list.
pub fn assert_matches_casa(list: &ComponentList, document: &Value, what: &str) {
    let components = document["components"].as_array().expect("components");
    assert_eq!(list.len(), components.len(), "{what}: component count");
    let cells = &document["cells"];
    for (row, component) in list.iter().enumerate() {
        let context = format!("{what} row {row}");
        assert_cells(component, cells, row, &context);
        assert_getcomponent(component, &components[row], &context);
    }
}

fn assert_cells(component: &SkyComponent, cells: &Value, row: usize, context: &str) {
    let cell = |column: &str| -> Option<&Value> { cells.get(column).map(|values| &values[row]) };
    let flux = component.flux();
    assert_eq!(
        complexes(cell("Flux").expect("Flux")),
        flux.values().to_vec(),
        "{context}: Flux"
    );
    assert_eq!(
        cell("Flux_Unit").and_then(Value::as_str),
        Some(flux.unit().name()),
        "{context}: Flux_Unit"
    );
    assert_eq!(
        cell("Flux_Polarisation").and_then(Value::as_str),
        Some(flux.polarisation().casa_name()),
        "{context}: Flux_Polarisation"
    );
    if let Some(errors) = cell("Flux_Error") {
        assert_eq!(
            complexes(errors),
            flux.errors().to_vec(),
            "{context}: Flux_Error"
        );
    } else {
        assert_eq!(
            flux.errors(),
            &[Complex64::new(0.0, 0.0); 4],
            "{context}: default flux errors"
        );
    }

    let shape = component.shape();
    assert_eq!(
        cell("Shape").and_then(Value::as_str),
        Some(shape.shape_type().casa_name()),
        "{context}: Shape"
    );
    let stored = reals(cell("Reference_Direction").expect("Reference_Direction"));
    let frame = cell("Direction_Frame")
        .and_then(Value::as_str)
        .expect("Direction_Frame");
    assert_eq!(
        shape.reference_direction().refer().as_str(),
        frame,
        "{context}: Direction_Frame"
    );
    // casa-rs keeps direction cosines and stores casacore's angles of them,
    // so a direction built from other angles moves by an ulp or so.
    let stored = MDirection::from_angles(stored[0], stored[1], shape.reference_direction().refer());
    for (stored, kept) in stored
        .cosines()
        .into_iter()
        .zip(shape.reference_direction().cosines())
    {
        assert!(
            (stored - kept).abs() <= 4.0 * f64::EPSILON,
            "{context}: Reference_Direction cosine {stored} vs {kept}"
        );
    }
    if let Some(errors) = cell("Direction_Error") {
        let errors = reals(errors);
        let units = strings(cell("Direction_Error_Units").expect("Direction_Error_Units"));
        let error = shape.direction_error();
        assert_eq!(
            (error.latitude().value(), error.latitude().unit().name()),
            (errors[0], units[0].as_str()),
            "{context}: latitude error"
        );
        assert_eq!(
            (error.longitude().value(), error.longitude().unit().name()),
            (errors[1], units[1].as_str()),
            "{context}: longitude error"
        );
    }
    assert_eq!(
        reals(cell("Shape_Parameters").expect("Shape_Parameters")),
        shape.parameters(),
        "{context}: Shape_Parameters"
    );
    if let Some(errors) = cell("Shape_Error") {
        assert_eq!(reals(errors), shape.errors(), "{context}: Shape_Error");
    }

    let spectrum = component.spectrum();
    assert_eq!(
        cell("Spectrum_Shape").and_then(Value::as_str),
        Some(spectrum.spectral_type().casa_name()),
        "{context}: Spectrum_Shape"
    );
    assert_eq!(
        cell("Reference_Frequency").and_then(Value::as_f64),
        Some(spectrum.reference_frequency().hz()),
        "{context}: Reference_Frequency"
    );
    assert_eq!(
        cell("Frequency_Frame").and_then(Value::as_str),
        Some(spectrum.reference_frequency().refer().as_str()),
        "{context}: Frequency_Frame"
    );
    if let Some(error) = cell("Frequency_Error") {
        let unit = cell("Frequency_Error_Units")
            .and_then(Value::as_str)
            .expect("Frequency_Error_Units");
        let actual = spectrum.reference_frequency_error();
        assert_eq!(
            (actual.value(), actual.unit().name()),
            (error.as_f64().expect("number"), unit),
            "{context}: Frequency_Error"
        );
    }
    let parameters = reals(cell("Spectral_Parameters").expect("Spectral_Parameters"));
    assert_eq!(
        parameters,
        spectrum.parameters(),
        "{context}: Spectral_Parameters"
    );
    if let Some(errors) = cell("Spectral_Error") {
        assert_eq!(
            reals(errors),
            spectrum.errors(),
            "{context}: Spectral_Error"
        );
    }
    if let Some(record) = cell("Spectral_Record") {
        assert_record_only_values(component, record, context);
    }
    assert_eq!(
        cell("Label").and_then(Value::as_str),
        Some(component.label()),
        "{context}: Label"
    );
    match cell("Optional_Parameters") {
        Some(Value::Null) | None => {
            assert!(
                component.optional_parameters().is_empty(),
                "{context}: no optional parameters"
            );
        }
        Some(values) => assert_eq!(
            reals(values),
            component.optional_parameters(),
            "{context}: Optional_Parameters"
        ),
    }
}

/// The values only `Spectral_Record` holds.
fn assert_record_only_values(component: &SkyComponent, record: &Value, context: &str) {
    match component.spectrum().kind() {
        SpectralKind::SpectralIndex(index) => {
            let stokes = reals(&record["stokesindex"]);
            match index.stokes_indices() {
                Some(indices) => assert_eq!(stokes, indices.to_vec(), "{context}: stokesindex"),
                None => assert_eq!(stokes.len(), 1, "{context}: one-element stokesindex"),
            }
        }
        SpectralKind::Tabular(tabular) => {
            assert_eq!(
                record["freqRef"]["refer"].as_str(),
                Some(tabular.frame().as_str()),
                "{context}: tabulation frame"
            );
            assert_eq!(
                reals(&record["tabFreqVal"]),
                tabular.frequencies_hz(),
                "{context}: tabFreqVal"
            );
            for (stokes, name) in ["ival", "qval", "uval", "vval"].into_iter().enumerate() {
                let tabulated: Vec<f64> = tabular
                    .stokes_flux()
                    .iter()
                    .map(|flux| flux[stokes])
                    .collect();
                assert_eq!(reals(&record[name]), tabulated, "{context}: {name}");
            }
        }
        SpectralKind::Constant | SpectralKind::PowerLogPolynomial(_) => {}
    }
}

fn assert_getcomponent(component: &SkyComponent, casa: &Value, context: &str) {
    let flux = component.flux();
    let casa_flux = &casa["flux"];
    assert_eq!(
        casa_flux["polarisation"].as_str(),
        Some(flux.polarisation().casa_name()),
        "{context}: flux polarisation"
    );
    assert_eq!(
        casa_flux["unit"].as_str(),
        Some(flux.unit().name()),
        "{context}: flux unit"
    );
    let (values, errors) = if flux.polarisation() == Polarisation::Stokes {
        (
            reals(&casa_flux["value"])
                .into_iter()
                .map(|re| Complex64::new(re, 0.0))
                .collect(),
            reals(&casa_flux["error"])
                .into_iter()
                .map(|re| Complex64::new(re, 0.0))
                .collect(),
        )
    } else {
        (
            complexes(&casa_flux["value"]),
            complexes(&casa_flux["error"]),
        )
    };
    assert_eq!(values, flux.values().to_vec(), "{context}: flux values");
    assert_eq!(errors, flux.errors().to_vec(), "{context}: flux errors");

    let shape = component.shape();
    let casa_shape = &casa["shape"];
    assert_eq!(
        ShapeType::parse_casa(casa_shape["type"].as_str().expect("shape type")),
        Ok(shape.shape_type()),
        "{context}: shape type"
    );
    let direction = &casa_shape["direction"];
    assert_eq!(
        direction["refer"].as_str(),
        Some(shape.reference_direction().refer().as_str()),
        "{context}: frame"
    );
    let casa_direction = MDirection::from_angles(
        si(&direction["m0"], "rad"),
        si(&direction["m1"], "rad"),
        shape.reference_direction().refer(),
    );
    for (casa, rust) in casa_direction
        .cosines()
        .into_iter()
        .zip(shape.reference_direction().cosines())
    {
        assert_close(casa, rust, 1e-15, &format!("{context}: direction cosine"));
    }
    let error = shape.direction_error();
    assert_close(
        si(&direction["error"]["latitude"], "rad"),
        error
            .latitude()
            .get_value_in(&Unit::new("rad").expect("rad"))
            .expect("angle"),
        1e-14,
        &format!("{context}: latitude error"),
    );
    assert_close(
        si(&direction["error"]["longitude"], "rad"),
        error
            .longitude()
            .get_value_in(&Unit::new("rad").expect("rad"))
            .expect("angle"),
        1e-14,
        &format!("{context}: longitude error"),
    );
    if shape.shape_type() != ShapeType::Point {
        let casa_parameters: Vec<f64> = ["majoraxis", "minoraxis", "positionangle"]
            .iter()
            .map(|name| si(&casa_shape[*name], "rad"))
            .collect();
        let casa_errors: Vec<f64> = ["majoraxiserror", "minoraxiserror", "positionangleerror"]
            .iter()
            .map(|name| si(&casa_shape[*name], "rad"))
            .collect();
        assert_all_close(
            &casa_parameters,
            &shape.parameters(),
            &format!("{context}: shape parameters"),
        );
        assert_all_close(
            &casa_errors,
            &shape.errors(),
            &format!("{context}: shape errors"),
        );
    }

    let spectrum = component.spectrum();
    let casa_spectrum = &casa["spectrum"];
    assert_eq!(
        SpectralType::parse_casa(casa_spectrum["type"].as_str().expect("spectrum type")),
        Ok(spectrum.spectral_type()),
        "{context}: spectrum type"
    );
    let frequency = &casa_spectrum["frequency"];
    assert_eq!(
        frequency["refer"].as_str(),
        Some(spectrum.reference_frequency().refer().as_str()),
        "{context}: frequency frame"
    );
    assert_close(
        si(&frequency["m0"], "Hz"),
        spectrum.reference_frequency().hz(),
        1e-14,
        &format!("{context}: frequency"),
    );
    match spectrum.kind() {
        SpectralKind::Constant => {}
        SpectralKind::SpectralIndex(index) => {
            assert_eq!(
                casa_spectrum["index"].as_f64(),
                Some(index.index()),
                "{context}: index"
            );
            assert_eq!(
                casa_spectrum["error"].as_f64(),
                Some(index.error()),
                "{context}: index error"
            );
            assert_eq!(
                reals(&casa_spectrum["stokesindex"]),
                index.casa_stokes_index(),
                "{context}: stokesindex"
            );
        }
        SpectralKind::Tabular(tabular) => {
            assert_eq!(
                casa_spectrum["freqRef"]["refer"].as_str(),
                Some(tabular.frame().as_str()),
                "{context}: freqRef"
            );
            assert_eq!(
                reals(&casa_spectrum["tabFreqVal"]),
                tabular.frequencies_hz(),
                "{context}: tabFreqVal"
            );
            let ival: Vec<f64> = tabular.stokes_flux().iter().map(|flux| flux[0]).collect();
            assert_eq!(reals(&casa_spectrum["ival"]), ival, "{context}: ival");
            let (max, min) = tabular.frequency_range_hz();
            assert_eq!(
                casa_spectrum["maxFreq"].as_f64(),
                Some(max),
                "{context}: maxFreq"
            );
            assert_eq!(
                casa_spectrum["minFreq"].as_f64(),
                Some(min),
                "{context}: minFreq"
            );
            let reference = tabular
                .reference_frequency_in_frame(spectrum.reference_frequency())
                .expect("reference in frame");
            assert_close(
                casa_spectrum["referenceFreq"]
                    .as_f64()
                    .expect("referenceFreq"),
                reference,
                1e-14,
                &format!("{context}: referenceFreq"),
            );
        }
        SpectralKind::PowerLogPolynomial(polynomial) => {
            assert_eq!(
                reals(&casa_spectrum["coeffs"]),
                polynomial.coefficients(),
                "{context}: coeffs"
            );
            assert_eq!(
                reals(&casa_spectrum["error"]),
                polynomial.errors(),
                "{context}: plp errors"
            );
        }
    }
    assert_eq!(
        casa["label"].as_str(),
        Some(component.label()),
        "{context}: label"
    );
    match casa.get("optionalParameters") {
        Some(values) => assert_eq!(
            reals(values),
            component.optional_parameters(),
            "{context}: optionalParameters"
        ),
        None => assert!(
            component.optional_parameters().is_empty(),
            "{context}: optionalParameters"
        ),
    }
}

/// Check that `actual` is `expected` read back from a table: equal, except
/// that a direction (kept as direction cosines, stored as casacore's angles)
/// and a Gaussian's axes (CASA rederives the minor axis from the axial ratio)
/// may move by a few units in the last place.
pub fn assert_same_list(expected: &ComponentList, actual: &ComponentList, what: &str) {
    assert_eq!(expected.len(), actual.len(), "{what}: component count");
    for (row, (expected, actual)) in expected.iter().zip(actual).enumerate() {
        let context = format!("{what} row {row}");
        assert_eq!(expected.flux(), actual.flux(), "{context}: flux");
        assert_eq!(
            expected.spectrum(),
            actual.spectrum(),
            "{context}: spectrum"
        );
        assert_eq!(expected.label(), actual.label(), "{context}: label");
        assert_eq!(
            expected.optional_parameters(),
            actual.optional_parameters(),
            "{context}: optional parameters"
        );
        let (expected, actual) = (expected.shape(), actual.shape());
        assert_eq!(
            expected.shape_type(),
            actual.shape_type(),
            "{context}: shape"
        );
        assert_eq!(
            expected.reference_direction().refer(),
            actual.reference_direction().refer(),
            "{context}: frame"
        );
        for (e, a) in expected
            .reference_direction()
            .cosines()
            .into_iter()
            .zip(actual.reference_direction().cosines())
        {
            assert!(
                (e - a).abs() <= 4.0 * f64::EPSILON,
                "{context}: direction cosine {e} vs {a}"
            );
        }
        assert_eq!(
            expected.direction_error(),
            actual.direction_error(),
            "{context}: direction error"
        );
        assert_all_close_ulps(
            &expected.parameters(),
            &actual.parameters(),
            &format!("{context}: shape parameters"),
        );
        assert_eq!(
            expected.errors(),
            actual.errors(),
            "{context}: shape errors"
        );
    }
}

fn assert_all_close_ulps(expected: &[f64], actual: &[f64], what: &str) {
    assert_eq!(expected.len(), actual.len(), "{what}: length");
    for (e, a) in expected.iter().zip(actual) {
        assert!(
            (e - a).abs() <= 4.0 * f64::EPSILON * e.abs().max(a.abs()),
            "{what}: {e} vs {a}"
        );
    }
}

/// A `{value, unit}` record in `si` units.
fn si(quantity: &Value, si: &str) -> f64 {
    let value = quantity["value"].as_f64().expect("quantity value");
    let unit = quantity["unit"].as_str().expect("quantity unit");
    Quantity::new(value, unit)
        .and_then(|quantity| quantity.get_value_in(&Unit::new(si)?))
        .unwrap_or_else(|error| panic!("{value} {unit} in {si}: {error}"))
}

pub fn assert_close(expected: f64, actual: f64, relative: f64, what: &str) {
    let scale = expected.abs().max(actual.abs());
    assert!(
        (expected - actual).abs() <= relative * scale,
        "{what}: CASA {expected:e}, casa-rs {actual:e}"
    );
}

fn assert_all_close(expected: &[f64], actual: &[f64], what: &str) {
    assert_eq!(expected.len(), actual.len(), "{what}: length");
    for (expected, actual) in expected.iter().zip(actual) {
        assert_close(*expected, *actual, 1e-14, what);
    }
}

pub fn reals(value: &Value) -> Vec<f64> {
    value
        .as_array()
        .unwrap_or_else(|| panic!("expected an array, got {value}"))
        .iter()
        .map(|item| item.as_f64().expect("number"))
        .collect()
}

fn strings(value: &Value) -> Vec<String> {
    value
        .as_array()
        .expect("array")
        .iter()
        .map(|item| item.as_str().expect("string").to_string())
        .collect()
}

fn complexes(value: &Value) -> Vec<Complex64> {
    value
        .as_array()
        .expect("array")
        .iter()
        .map(|pair| {
            let pair = reals(pair);
            Complex64::new(pair[0], pair[1])
        })
        .collect()
}
