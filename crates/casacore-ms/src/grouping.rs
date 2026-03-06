// SPDX-License-Identifier: LGPL-3.0-or-later
//! Canonical MS iteration by ARRAY_ID, FIELD_ID, DATA_DESC_ID, and TIME.
//!
//! [`MsIterator`] groups rows of the MS main table using
//! [`Table::iter_groups`], following the canonical C++ `MSIter` sort order.
//!
//! Each group is represented as an [`MsGroup`] with the key values and
//! row indices.
//!
//! Cf. C++ `MSIter`.

use casacore_tables::{Table, TableGroup};

use crate::error::MsResult;
use crate::ms::MeasurementSet;

/// Default columns used for canonical MS iteration.
///
/// These match the C++ `MSIter` default sort columns.
pub const DEFAULT_SORT_COLUMNS: &[&str] = &["ARRAY_ID", "FIELD_ID", "DATA_DESC_ID", "TIME"];

/// A group of MS rows sharing equal key column values.
///
/// Cf. C++ `MSIter::table()`.
#[derive(Debug, Clone)]
pub struct MsGroup {
    /// ARRAY_ID for this group (if grouped by ARRAY_ID).
    pub array_id: Option<i32>,
    /// FIELD_ID for this group (if grouped by FIELD_ID).
    pub field_id: Option<i32>,
    /// DATA_DESC_ID for this group (if grouped by DATA_DESC_ID).
    pub data_desc_id: Option<i32>,
    /// Row indices in the parent table belonging to this group.
    pub row_indices: Vec<usize>,
}

impl MsGroup {
    fn from_table_group(group: &TableGroup) -> Self {
        use casacore_types::{ScalarValue, Value};

        let get_i32 = |name: &str| -> Option<i32> {
            group.keys.get(name).and_then(|v| match v {
                Value::Scalar(ScalarValue::Int32(i)) => Some(*i),
                _ => None,
            })
        };

        Self {
            array_id: get_i32("ARRAY_ID"),
            field_id: get_i32("FIELD_ID"),
            data_desc_id: get_i32("DATA_DESC_ID"),
            row_indices: group.row_indices.clone(),
        }
    }
}

/// Iterator over MS groups using canonical sort order.
///
/// Created via [`MsIterator::new`] or [`MsIterator::with_columns`].
/// Wraps [`Table::iter_groups`] and converts each [`TableGroup`] to
/// an [`MsGroup`].
///
/// Cf. C++ `MSIter`.
pub struct MsIterator<'a> {
    inner: casacore_tables::TableIterator<'a>,
}

impl<'a> MsIterator<'a> {
    /// Create an iterator with the default canonical sort order:
    /// ARRAY_ID, FIELD_ID, DATA_DESC_ID, TIME (all ascending).
    pub fn new(table: &'a Table) -> MsResult<Self> {
        use casacore_tables::SortOrder;
        let keys: Vec<(&str, SortOrder)> = DEFAULT_SORT_COLUMNS
            .iter()
            .map(|&col| (col, SortOrder::Ascending))
            .collect();
        let inner = table.iter_groups(&keys)?;
        Ok(Self { inner })
    }

    /// Create an iterator with custom grouping columns (all ascending).
    pub fn with_columns(table: &'a Table, columns: &[&str]) -> MsResult<Self> {
        use casacore_tables::SortOrder;
        let keys: Vec<(&str, SortOrder)> = columns
            .iter()
            .map(|&col| (col, SortOrder::Ascending))
            .collect();
        let inner = table.iter_groups(&keys)?;
        Ok(Self { inner })
    }
}

impl<'a> Iterator for MsIterator<'a> {
    type Item = MsGroup;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|g| MsGroup::from_table_group(&g))
    }
}

/// Convenience method: iterate over the main table of an MS with canonical sort.
pub fn iter_ms(ms: &MeasurementSet) -> MsResult<MsIterator<'_>> {
    MsIterator::new(ms.main_table())
}

/// Convenience method: iterate over the main table with custom columns.
pub fn iter_ms_by<'a>(ms: &'a MeasurementSet, columns: &[&str]) -> MsResult<MsIterator<'a>> {
    MsIterator::with_columns(ms.main_table(), columns)
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
    fn canonical_grouping() {
        let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();

        // Add rows: 2 fields × 1 ddid
        let i = |v: i32| Value::Scalar(ScalarValue::Int32(v));
        let f = |v: f64| Value::Scalar(ScalarValue::Float64(v));

        add_row(
            &mut ms,
            &[
                ("FIELD_ID", i(0)),
                ("DATA_DESC_ID", i(0)),
                ("TIME", f(100.0)),
            ],
        );
        add_row(
            &mut ms,
            &[
                ("FIELD_ID", i(0)),
                ("DATA_DESC_ID", i(0)),
                ("TIME", f(200.0)),
            ],
        );
        add_row(
            &mut ms,
            &[
                ("FIELD_ID", i(1)),
                ("DATA_DESC_ID", i(0)),
                ("TIME", f(100.0)),
            ],
        );
        add_row(
            &mut ms,
            &[
                ("FIELD_ID", i(1)),
                ("DATA_DESC_ID", i(0)),
                ("TIME", f(200.0)),
            ],
        );

        let groups: Vec<MsGroup> = iter_ms(&ms).unwrap().collect();
        // Should group by ARRAY_ID(0), FIELD_ID(0,1), DATA_DESC_ID(0), TIME(100,200)
        // = 4 groups (field0/t100, field0/t200, field1/t100, field1/t200)
        assert_eq!(groups.len(), 4);

        // All groups should have 1 row each
        for g in &groups {
            assert_eq!(g.row_indices.len(), 1);
        }
    }

    #[test]
    fn custom_grouping_by_field() {
        let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();

        let i = |v: i32| Value::Scalar(ScalarValue::Int32(v));
        let f = |v: f64| Value::Scalar(ScalarValue::Float64(v));

        add_row(&mut ms, &[("FIELD_ID", i(0)), ("TIME", f(100.0))]);
        add_row(&mut ms, &[("FIELD_ID", i(0)), ("TIME", f(200.0))]);
        add_row(&mut ms, &[("FIELD_ID", i(1)), ("TIME", f(100.0))]);

        let groups: Vec<MsGroup> = iter_ms_by(&ms, &["FIELD_ID"]).unwrap().collect();
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].field_id, Some(0));
        assert_eq!(groups[0].row_indices.len(), 2);
        assert_eq!(groups[1].field_id, Some(1));
        assert_eq!(groups[1].row_indices.len(), 1);
    }
}
