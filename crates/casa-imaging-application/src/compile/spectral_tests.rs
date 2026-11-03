// SPDX-License-Identifier: LGPL-3.0-or-later

use super::continuum_rest_frequency_hz;
use casa_ms::{MeasurementSet, MeasurementSetBuilder, SubtableId};
use casa_tables::Table;
use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use ndarray::ArrayD;

fn integer(name: &str, value: i32) -> RecordField {
    RecordField::new(name, Value::Scalar(ScalarValue::Int32(value)))
}

fn source_row(source: i32, spw: i32, frequencies: &[f64]) -> RecordValue {
    RecordValue::new(vec![
        integer("SOURCE_ID", source),
        integer("SPECTRAL_WINDOW_ID", spw),
        RecordField::new(
            "REST_FREQUENCY",
            Value::Array(ArrayValue::Float64(
                ArrayD::from_shape_vec(vec![frequencies.len()], frequencies.to_vec()).unwrap(),
            )),
        ),
    ])
}

fn fixture(rows: Vec<RecordValue>) -> MeasurementSet {
    let mut ms = MeasurementSet::create_memory(
        MeasurementSetBuilder::new()
            .with_optional_subtable(SubtableId::Source)
            .with_optional_subtable(SubtableId::Doppler),
    )
    .unwrap();
    *ms.subtable_mut(SubtableId::Field).unwrap() = Table::from_rows_memory(vec![
        RecordValue::new(vec![integer("SOURCE_ID", 2)]),
        RecordValue::new(vec![integer("SOURCE_ID", 3)]),
    ]);
    let columns = casa_ms::schema::source::REQUIRED_COLUMNS
        .iter()
        .chain(casa_ms::schema::source::OPTIONAL_COLUMNS)
        .filter(|column| {
            matches!(
                column.name,
                "SOURCE_ID" | "SPECTRAL_WINDOW_ID" | "REST_FREQUENCY"
            )
        })
        .copied()
        .collect::<Vec<_>>();
    let schema = casa_ms::column_def::build_table_schema(&columns).unwrap();
    *ms.subtable_mut(SubtableId::Source).unwrap() =
        Table::from_rows_with_schema_memory(rows, schema).unwrap();
    ms
}

#[test]
fn mfs_header_rest_frequency_uses_first_matching_source_not_band_centre() {
    let ms = fixture(vec![
        source_row(3, 0, &[9.0e9]),
        source_row(2, 1, &[8.0e9]),
        source_row(2, 0, &[1.2e9, 1.8e9]),
        source_row(2, 0, &[1.3e9]),
    ]);
    assert_eq!(
        continuum_rest_frequency_hz(&ms, 0, 0, 1.5e9).unwrap(),
        1.2e9
    );
    assert_eq!(
        continuum_rest_frequency_hz(&ms, 0, 1, 1.5e9).unwrap(),
        8.0e9
    );
    assert_eq!(
        continuum_rest_frequency_hz(&ms, 1, 0, 1.5e9).unwrap(),
        9.0e9
    );
}

#[test]
fn mfs_header_rest_frequency_uses_all_spw_source_and_midpoint_fallback() {
    let ms = fixture(vec![source_row(2, -1, &[340.0e9])]);
    assert_eq!(
        continuum_rest_frequency_hz(&ms, 0, 4, 341.4e9).unwrap(),
        340.0e9
    );
    for rows in [
        vec![],
        vec![source_row(2, 0, &[])],
        vec![source_row(2, 0, &[0.0])],
    ] {
        let ms = fixture(rows);
        assert_eq!(
            continuum_rest_frequency_hz(&ms, 0, 0, 341.4e9).unwrap(),
            341.4e9
        );
    }
}

#[test]
fn mfs_header_rest_frequency_refuses_unresolved_doppler_metadata() {
    let mut ms = fixture(vec![source_row(2, 0, &[1.2e9, 1.8e9])]);
    set_active_doppler(&mut ms);
    let error = continuum_rest_frequency_hz(&ms, 0, 0, 1.5e9).unwrap_err();
    assert!(error.to_string().contains("DOPPLER table is not supported"));
}

#[test]
fn mfs_header_rest_frequency_without_source_uses_midpoint() {
    let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();
    *ms.subtable_mut(SubtableId::Field).unwrap() =
        Table::from_rows_memory(vec![RecordValue::new(vec![integer("SOURCE_ID", -1)])]);
    assert_eq!(
        continuum_rest_frequency_hz(&ms, 0, 0, 1.5e9).unwrap(),
        1.5e9
    );
}

#[test]
fn mfs_header_rest_frequency_refuses_overlapping_source_key_groups() {
    for rows in [
        vec![source_row(2, 0, &[1.2e9]), source_row(2, -1, &[340.0e9])],
        vec![source_row(2, -1, &[340.0e9]), source_row(2, 0, &[1.2e9])],
        vec![source_row(2, 0, &[]), source_row(2, -1, &[340.0e9])],
    ] {
        let ms = fixture(rows);
        let error = continuum_rest_frequency_hz(&ms, 0, 0, 1.5e9).unwrap_err();
        assert!(error.to_string().contains("overlapping SOURCE key groups"));
    }
}

#[test]
fn mfs_header_rest_frequency_refuses_multigroup_wildcard_search_ambiguity() {
    for (rows, source_spw) in [
        (
            vec![
                source_row(2, -1, &[340.0e9]),
                source_row(2, 0, &[1.2e9]),
                source_row(2, 1, &[1.8e9]),
            ],
            4,
        ),
        (
            vec![source_row(-1, 9, &[340.0e9]), source_row(2, 2, &[1.2e9])],
            2,
        ),
    ] {
        let ms = fixture(rows);
        let error = continuum_rest_frequency_hz(&ms, 0, source_spw, 1.5e9).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("multiple SOURCE key groups with wildcard ids")
        );
    }
}

#[test]
fn mfs_header_rest_frequency_wildcard_is_the_stored_key_not_the_query() {
    for (stored_source, expected_hz) in [(2, 1.5e9), (-1, 1.2e9)] {
        let mut ms = fixture(vec![source_row(stored_source, 0, &[1.2e9])]);
        *ms.subtable_mut(SubtableId::Field).unwrap() =
            Table::from_rows_memory(vec![RecordValue::new(vec![integer("SOURCE_ID", -1)])]);
        assert_eq!(
            continuum_rest_frequency_hz(&ms, 0, 0, 1.5e9).unwrap(),
            expected_hz,
        );
    }
}

fn set_active_doppler(ms: &mut MeasurementSet) {
    let column = casa_ms::schema::spectral_window::OPTIONAL_COLUMNS
        .iter()
        .find(|column| column.name == "DOPPLER_ID")
        .unwrap();
    let schema = casa_ms::column_def::build_table_schema(&[*column]).unwrap();
    *ms.subtable_mut(SubtableId::SpectralWindow).unwrap() = Table::from_rows_with_schema_memory(
        vec![RecordValue::new(vec![integer("DOPPLER_ID", 7)])],
        schema,
    )
    .unwrap();
}

#[test]
fn mfs_header_rest_frequency_refuses_active_doppler_even_without_source() {
    let mut ms = MeasurementSet::create_memory(
        MeasurementSetBuilder::new().with_optional_subtable(SubtableId::Doppler),
    )
    .unwrap();
    *ms.subtable_mut(SubtableId::Field).unwrap() =
        Table::from_rows_memory(vec![RecordValue::new(vec![integer("SOURCE_ID", 2)])]);
    set_active_doppler(&mut ms);
    let error = continuum_rest_frequency_hz(&ms, 0, 0, 1.5e9).unwrap_err();
    assert!(error.to_string().contains("DOPPLER table is not supported"));
}
