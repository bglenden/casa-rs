// SPDX-License-Identifier: LGPL-3.0-or-later
//! casa-rs's machine records against the ones CASA 6.7.6's `tclean`
//! wrote into `refim_twochan.ms` (`casa-ms`'s fixture
//! `virtual_model_casa.tgz`): given CASA's inputs, casa-rs writes CASA's
//! record, every field, its order, type and value.

use std::path::{Path, PathBuf};

use casa_tables::{Table, TableOptions};

use super::*;

/// `ft_0.container` of the model record CASA left in SOURCE row 0 of
/// `name` (`casa_mfs_source`, `casa_mtmfs_source`).
fn casa_container(name: &str) -> RecordValue {
    let root = tempfile::tempdir().expect("root");
    let archive = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../casa-ms/tests/fixtures/virtual_model_casa.tgz");
    tar::Archive::new(flate2::read::GzDecoder::new(
        std::fs::File::open(archive).expect("fixture"),
    ))
    .unpack(root.path())
    .expect("unpack");
    let record = Table::open(TableOptions::new(root.path().join(name)))
        .expect("CASA's SOURCE")
        .record_cell(0, "SOURCE_MODEL")
        .expect("CASA's model record");
    let Some(Value::Record(entry)) = record.get("ft_0") else {
        panic!("no ft_0");
    };
    let Some(Value::Record(container)) = entry.get("container") else {
        panic!("no container");
    };
    container.clone()
}

fn field<'a>(record: &'a RecordValue, name: &str) -> &'a Value {
    record.get(name).unwrap_or_else(|| panic!("{name}"))
}

fn f64s(value: &Value) -> Vec<f64> {
    match value {
        Value::Array(ArrayValue::Float64(values)) => values.iter().copied().collect(),
        other => panic!("{other:?}"),
    }
}

fn i32s(value: &Value) -> Vec<i32> {
    match value {
        Value::Array(ArrayValue::Int32(values)) => values.iter().copied().collect(),
        other => panic!("{other:?}"),
    }
}

fn i32_of(value: &Value) -> i32 {
    match value {
        Value::Scalar(ScalarValue::Int32(value)) => *value,
        other => panic!("{other:?}"),
    }
}

fn quantity_of(measure: &Value, axis: &str) -> f64 {
    let Value::Record(measure) = measure else {
        panic!("{measure:?}");
    };
    let Some(Value::Record(quantity)) = measure.get(axis) else {
        panic!("{axis}");
    };
    match quantity.get("value") {
        Some(Value::Scalar(ScalarValue::Float64(value))) => *value,
        other => panic!("{other:?}"),
    }
}

/// casa-rs's `GridFT` record for the inputs CASA's record states.
fn rebuilt(casa: &RecordValue) -> RecordValue {
    let Value::Scalar(ScalarValue::String(disk_image)) = field(casa, "diskimage") else {
        panic!("diskimage");
    };
    let grid = [
        i32_of(field(casa, "nx")) as usize,
        i32_of(field(casa, "ny")) as usize,
    ];
    let scale = f64s(field(casa, "uvscale"));
    let buffer = FirstBuffer {
        field: i32_of(field(casa, "lastfieldid")),
        frequencies_lsrk_hz: f64s(field(casa, "lsrfreq")),
        channel_map: i32s(field(casa, "chanmap")),
        correlations: i32s(field(casa, "cfstokes")),
        polarization_map: i32s(field(casa, "polmap")),
    };
    let Value::Record(spectral) = field(casa, "spectralcoord") else {
        panic!("spectralcoord");
    };
    let centre = field(casa, "mimage_rec");
    let location = field(casa, "mlocation_rec");
    GridFt {
        disk_image: &PathBuf::from(disk_image),
        grid,
        polarizations: i32_of(field(casa, "npol")) as usize,
        image_frequencies_hz: &f64s(field(casa, "imagefreq")),
        increment_rad: [scale[0] / grid[0] as f64, scale[1] / grid[1] as f64],
        phase_centre_rad: [quantity_of(centre, "m0"), quantity_of(centre, "m1")],
        direction_frame: "J2000",
        observatory: [
            quantity_of(location, "m0"),
            quantity_of(location, "m1"),
            quantity_of(location, "m2"),
        ],
        spectral: spectral.clone(),
        buffer: &buffer,
        interpolation: i32_of(field(casa, "freqinterpmethod")),
        frequency_frame_valid: field(casa, "freqframevalid")
            == &Value::Scalar(ScalarValue::Bool(true)),
    }
    .record()
}

/// The first field, by path, where `ours` and `casa` differ.
fn difference(ours: &RecordValue, casa: &RecordValue, path: &str) -> Option<String> {
    let names = |record: &RecordValue| {
        record
            .fields()
            .iter()
            .map(|field| field.name.clone())
            .collect::<Vec<_>>()
    };
    if names(ours) != names(casa) {
        return Some(format!(
            "{path}: fields {:?} != {:?}",
            names(ours),
            names(casa)
        ));
    }
    ours.fields()
        .iter()
        .zip(casa.fields())
        .find_map(|(a, b)| match (&a.value, &b.value) {
            (Value::Record(a_record), Value::Record(b_record)) => {
                difference(a_record, b_record, &format!("{path}.{}", a.name))
            }
            (a_value, b_value) if a_value != b_value => {
                Some(format!("{path}.{}: {a_value:?} != {b_value:?}", a.name))
            }
            _ => None,
        })
}

/// The continuum image's `GridFT`: casa-rs's record is CASA's.
#[test]
fn a_grid_record_is_casa_s() {
    let casa = casa_container("casa_mfs_source");
    let ours = rebuilt(&casa);
    assert_eq!(difference(&ours, &casa, "container"), None);
    assert_eq!(ours, casa);
}

/// The MT-MFS image's `MultiTermFTNew` and its terms' `GridFT`s.
#[test]
fn a_multi_term_record_is_casa_s() {
    let casa = casa_container("casa_mtmfs_source");
    let terms = (0..2)
        .map(|term| {
            let Value::Record(machine) = field(&casa, &format!("subftm_{term}")) else {
                panic!("subftm_{term}");
            };
            rebuilt(machine)
        })
        .collect();
    let Value::Scalar(ScalarValue::Float64(reference_hz)) = field(&casa, "reffreq") else {
        panic!("reffreq");
    };
    let ours = multi_term(terms, *reference_hz);
    assert_eq!(difference(&ours, &casa, "container"), None);
    assert_eq!(ours, casa);
}

/// A phase centre east of π is written as CASA's angle, west of zero.
#[test]
fn a_longitude_is_casa_s_angle() {
    let written = direction([5.233697011339746, 0.7109380541842404], "J2000");
    assert!((quantity_of(&written, "m0") + 1.0494882958398402).abs() < 1e-15);
}
