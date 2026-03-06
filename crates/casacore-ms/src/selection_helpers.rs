// SPDX-License-Identifier: LGPL-3.0-or-later
//! Helper functions for resolving names to IDs and querying unique values.
//!
//! These work with the subtables of a [`MeasurementSet`] to translate
//! human-readable identifiers (antenna names, field names) into the
//! integer IDs used in the main table.

use crate::error::MsResult;
use crate::ms::MeasurementSet;
use crate::subtables::get_f64;

/// Resolve antenna names to row indices (= antenna IDs) in the ANTENNA subtable.
pub fn resolve_antenna_names(ms: &MeasurementSet, names: &[&str]) -> MsResult<Vec<i32>> {
    let ant = ms.antenna()?;
    let mut ids = Vec::new();
    for row in 0..ant.row_count() {
        let name = ant.name(row)?;
        if names.iter().any(|&n| n == name) {
            ids.push(row as i32);
        }
    }
    Ok(ids)
}

/// Resolve field names to row indices (= field IDs) in the FIELD subtable.
pub fn resolve_field_names(ms: &MeasurementSet, names: &[&str]) -> MsResult<Vec<i32>> {
    let field = ms.field()?;
    let mut ids = Vec::new();
    for row in 0..field.row_count() {
        let name = field.name(row)?;
        if names.iter().any(|&n| n == name) {
            ids.push(row as i32);
        }
    }
    Ok(ids)
}

/// Return the sorted unique FIELD_ID values present in the main table.
pub fn unique_field_ids(ms: &MeasurementSet) -> MsResult<Vec<i32>> {
    unique_i32_column(ms.main_table(), "FIELD_ID")
}

/// Return the sorted unique DATA_DESC_ID values present in the main table.
pub fn unique_spw_ids(ms: &MeasurementSet) -> MsResult<Vec<i32>> {
    unique_i32_column(ms.main_table(), "DATA_DESC_ID")
}

/// Return the sorted unique ANTENNA1 and ANTENNA2 values present in the main table.
pub fn unique_antenna_ids(ms: &MeasurementSet) -> MsResult<Vec<i32>> {
    use crate::subtables::get_i32;
    let table = ms.main_table();
    let mut ids = std::collections::BTreeSet::new();
    for row in 0..table.row_count() {
        ids.insert(get_i32(table, row, "ANTENNA1")?);
        ids.insert(get_i32(table, row, "ANTENNA2")?);
    }
    Ok(ids.into_iter().collect())
}

/// Return the (min, max) TIME values in the main table, or `None` if empty.
pub fn time_range(ms: &MeasurementSet) -> MsResult<Option<(f64, f64)>> {
    let table = ms.main_table();
    if table.row_count() == 0 {
        return Ok(None);
    }
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;
    for row in 0..table.row_count() {
        let t = get_f64(table, row, "TIME")?;
        if t < min {
            min = t;
        }
        if t > max {
            max = t;
        }
    }
    Ok(Some((min, max)))
}

fn unique_i32_column(table: &casacore_tables::Table, column: &str) -> MsResult<Vec<i32>> {
    use crate::subtables::get_i32;
    let mut ids = std::collections::BTreeSet::new();
    for row in 0..table.row_count() {
        ids.insert(get_i32(table, row, column)?);
    }
    Ok(ids.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::MeasurementSetBuilder;
    use crate::test_helpers::default_value;
    use casacore_types::{RecordField, RecordValue, ScalarValue, Value};

    fn add_row(ms: &mut MeasurementSet, overrides: &[(&str, Value)]) {
        let schema = ms.main_table().schema().unwrap().clone();
        let fields: Vec<RecordField> = schema
            .columns()
            .iter()
            .map(|col| {
                if let Some((_, v)) = overrides.iter().find(|(n, _)| *n == col.name()) {
                    RecordField::new(col.name(), v.clone())
                } else {
                    RecordField::new(col.name(), default_value(col.name()))
                }
            })
            .collect();
        ms.main_table_mut()
            .add_row(RecordValue::new(fields))
            .unwrap();
    }

    #[test]
    fn unique_fields() {
        let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();
        let i = |v: i32| Value::Scalar(ScalarValue::Int32(v));
        add_row(&mut ms, &[("FIELD_ID", i(1))]);
        add_row(&mut ms, &[("FIELD_ID", i(0))]);
        add_row(&mut ms, &[("FIELD_ID", i(1))]);

        let ids = unique_field_ids(&ms).unwrap();
        assert_eq!(ids, vec![0, 1]);
    }

    #[test]
    fn unique_antennas() {
        let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();
        let i = |v: i32| Value::Scalar(ScalarValue::Int32(v));
        add_row(&mut ms, &[("ANTENNA1", i(0)), ("ANTENNA2", i(1))]);
        add_row(&mut ms, &[("ANTENNA1", i(1)), ("ANTENNA2", i(2))]);

        let ids = unique_antenna_ids(&ms).unwrap();
        assert_eq!(ids, vec![0, 1, 2]);
    }

    #[test]
    fn time_range_empty() {
        let ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();
        assert_eq!(time_range(&ms).unwrap(), None);
    }

    #[test]
    fn time_range_with_data() {
        let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();
        let f = |v: f64| Value::Scalar(ScalarValue::Float64(v));
        add_row(&mut ms, &[("TIME", f(300.0))]);
        add_row(&mut ms, &[("TIME", f(100.0))]);
        add_row(&mut ms, &[("TIME", f(200.0))]);

        let (min, max) = time_range(&ms).unwrap().unwrap();
        assert!((min - 100.0).abs() < 1e-10);
        assert!((max - 300.0).abs() < 1e-10);
    }

    #[test]
    fn resolve_antenna_names_test() {
        let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();
        {
            let mut ant = ms.antenna_mut().unwrap();
            ant.add_antenna(
                "VLA01",
                "N01",
                "GROUND-BASED",
                "ALT-AZ",
                [0.0; 3],
                [0.0; 3],
                25.0,
            )
            .unwrap();
            ant.add_antenna(
                "VLA02",
                "N02",
                "GROUND-BASED",
                "ALT-AZ",
                [0.0; 3],
                [0.0; 3],
                25.0,
            )
            .unwrap();
        }

        let ids = resolve_antenna_names(&ms, &["VLA02"]).unwrap();
        assert_eq!(ids, vec![1]);
    }

    #[test]
    fn resolve_field_names_test() {
        let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();

        // Add fields
        use crate::schema;
        use crate::test_helpers::default_value_for_def;

        let field_table = ms.subtable_mut(crate::schema::SubtableId::Field).unwrap();
        for name in ["3C273", "M87"] {
            let fields: Vec<RecordField> = schema::field::REQUIRED_COLUMNS
                .iter()
                .map(|c| {
                    if c.name == "NAME" {
                        RecordField::new(
                            "NAME",
                            Value::Scalar(ScalarValue::String(name.to_string())),
                        )
                    } else {
                        RecordField::new(c.name, default_value_for_def(c))
                    }
                })
                .collect();
            field_table.add_row(RecordValue::new(fields)).unwrap();
        }

        let ids = resolve_field_names(&ms, &["M87"]).unwrap();
        assert_eq!(ids, vec![1]);
    }
}
