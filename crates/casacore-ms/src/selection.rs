// SPDX-License-Identifier: LGPL-3.0-or-later
//! Builder-pattern MS selection that translates to TaQL queries.
//!
//! [`MsSelection`] lets callers build up selection criteria (field, spectral
//! window, antenna, time range, scan, observation) and then apply them to a
//! [`MeasurementSet`] via TaQL WHERE clauses.
//!
//! # Example
//!
//! ```rust
//! use casacore_ms::selection::MsSelection;
//!
//! let sel = MsSelection::new()
//!     .field(&[0, 1])
//!     .spw(&[0])
//!     .scan(&[1, 2, 3]);
//! assert!(sel.to_taql().contains("FIELD_ID"));
//! ```
//!
//! Cf. C++ `MSSelection`.

use crate::error::{MsError, MsResult};
use crate::ms::MeasurementSet;

/// Builder for MS row selection criteria.
///
/// Each method adds a constraint. Constraints are ANDed together when
/// converted to TaQL. Call [`to_taql`](MsSelection::to_taql) to get the
/// WHERE clause, or [`apply`](MsSelection::apply) to execute it against
/// a [`MeasurementSet`].
///
/// Cf. C++ `MSSelection`.
#[derive(Debug, Clone, Default)]
pub struct MsSelection {
    field_ids: Vec<i32>,
    spw_ids: Vec<i32>,
    antenna_ids: Vec<i32>,
    antenna_names: Vec<String>,
    baselines: Vec<(i32, i32)>,
    time_range: Option<(f64, f64)>,
    scan_numbers: Vec<i32>,
    observation_ids: Vec<i32>,
    taql_expr: Option<String>,
}

impl MsSelection {
    /// Create an empty selection (selects all rows).
    pub fn new() -> Self {
        Self::default()
    }

    /// Select rows matching the given FIELD_IDs.
    pub fn field(mut self, ids: &[i32]) -> Self {
        self.field_ids.extend_from_slice(ids);
        self
    }

    /// Select rows matching the given spectral window IDs (via DATA_DESC_ID lookup).
    ///
    /// Note: this filters on DATA_DESC_ID values whose SPECTRAL_WINDOW_ID
    /// matches. For simple MSes where DATA_DESC_ID == SPW_ID, this is a
    /// direct filter on DATA_DESC_ID.
    pub fn spw(mut self, ids: &[i32]) -> Self {
        self.spw_ids.extend_from_slice(ids);
        self
    }

    /// Select rows where ANTENNA1 or ANTENNA2 matches any of the given IDs.
    pub fn antenna(mut self, ids: &[i32]) -> Self {
        self.antenna_ids.extend_from_slice(ids);
        self
    }

    /// Select by antenna name (resolved via the ANTENNA subtable).
    pub fn antenna_name(mut self, names: &[&str]) -> Self {
        self.antenna_names
            .extend(names.iter().map(|s| s.to_string()));
        self
    }

    /// Select rows matching specific antenna pairs (baselines).
    pub fn baseline(mut self, pairs: &[(i32, i32)]) -> Self {
        self.baselines.extend_from_slice(pairs);
        self
    }

    /// Select rows within a TIME range `[start, end]` (MJD seconds).
    pub fn time_range(mut self, start: f64, end: f64) -> Self {
        self.time_range = Some((start, end));
        self
    }

    /// Select rows matching the given SCAN_NUMBERs.
    pub fn scan(mut self, numbers: &[i32]) -> Self {
        self.scan_numbers.extend_from_slice(numbers);
        self
    }

    /// Select rows matching the given OBSERVATION_IDs.
    pub fn observation(mut self, ids: &[i32]) -> Self {
        self.observation_ids.extend_from_slice(ids);
        self
    }

    /// Add a raw TaQL WHERE expression (ANDed with other criteria).
    pub fn taql(mut self, expr: &str) -> Self {
        self.taql_expr = Some(expr.to_string());
        self
    }

    /// Convert the selection to a TaQL SELECT statement.
    ///
    /// Returns a string like `SELECT * WHERE FIELD_ID IN [0,1] AND ...`.
    pub fn to_taql(&self) -> String {
        let clauses = self.where_clauses();
        if clauses.is_empty() {
            "SELECT *".to_string()
        } else {
            format!("SELECT * WHERE {}", clauses.join(" AND "))
        }
    }

    /// Apply this selection to a MeasurementSet, returning the selected row indices.
    ///
    /// If antenna names were specified, they are resolved against the ANTENNA
    /// subtable first.
    pub fn apply(&self, ms: &mut MeasurementSet) -> MsResult<Vec<usize>> {
        let mut sel = self.clone();

        // Resolve antenna names to IDs
        if !sel.antenna_names.is_empty() {
            let resolved = resolve_antenna_names(ms, &sel.antenna_names)?;
            sel.antenna_ids.extend(resolved);
            sel.antenna_names.clear();
        }

        // Resolve SPW IDs to DATA_DESC_IDs
        let mut effective_ddids = Vec::new();
        if !sel.spw_ids.is_empty() {
            effective_ddids = resolve_spw_to_ddid(ms, &sel.spw_ids)?;
        }

        let taql = if sel.spw_ids.is_empty() {
            sel.to_taql()
        } else {
            // Replace spw_ids with direct DATA_DESC_ID filter
            let mut sel2 = sel.clone();
            sel2.spw_ids.clear();
            let mut clauses = sel2.where_clauses();
            if effective_ddids.is_empty() {
                clauses.push("FALSE".to_string());
            } else {
                clauses.push(format!(
                    "DATA_DESC_ID IN [{}]",
                    effective_ddids
                        .iter()
                        .map(|id| id.to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                ));
            }
            if clauses.is_empty() {
                "SELECT *".to_string()
            } else {
                format!("SELECT * WHERE {}", clauses.join(" AND "))
            }
        };

        let view = ms.main_table_mut().query(&taql)?;
        Ok(view.row_numbers().to_vec())
    }

    fn where_clauses(&self) -> Vec<String> {
        let mut clauses = Vec::new();

        if !self.field_ids.is_empty() {
            clauses.push(format!("FIELD_ID IN [{}]", int_list(&self.field_ids)));
        }

        if !self.spw_ids.is_empty() {
            // Direct DATA_DESC_ID filter (simple case)
            clauses.push(format!("DATA_DESC_ID IN [{}]", int_list(&self.spw_ids)));
        }

        if !self.antenna_ids.is_empty() {
            let ids = int_list(&self.antenna_ids);
            clauses.push(format!("(ANTENNA1 IN [{ids}] OR ANTENNA2 IN [{ids}])"));
        }

        if !self.baselines.is_empty() {
            let pairs: Vec<String> = self
                .baselines
                .iter()
                .map(|(a1, a2)| format!("(ANTENNA1=={a1} AND ANTENNA2=={a2})"))
                .collect();
            clauses.push(format!("({})", pairs.join(" OR ")));
        }

        if let Some((start, end)) = self.time_range {
            clauses.push(format!("TIME>={start} AND TIME<={end}"));
        }

        if !self.scan_numbers.is_empty() {
            clauses.push(format!("SCAN_NUMBER IN [{}]", int_list(&self.scan_numbers)));
        }

        if !self.observation_ids.is_empty() {
            clauses.push(format!(
                "OBSERVATION_ID IN [{}]",
                int_list(&self.observation_ids)
            ));
        }

        if let Some(ref expr) = self.taql_expr {
            clauses.push(format!("({expr})"));
        }

        clauses
    }
}

fn int_list(ids: &[i32]) -> String {
    ids.iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

/// Resolve antenna names to IDs using the ANTENNA subtable.
fn resolve_antenna_names(ms: &MeasurementSet, names: &[String]) -> MsResult<Vec<i32>> {
    let ant = ms.antenna()?;
    let mut ids = Vec::new();
    for row in 0..ant.row_count() {
        let name = ant.name(row)?;
        if names.iter().any(|n| n == &name) {
            ids.push(row as i32);
        }
    }
    if ids.is_empty() {
        return Err(MsError::MissingColumn {
            column: format!("antenna names {names:?}"),
            table: "ANTENNA".to_string(),
        });
    }
    Ok(ids)
}

/// Resolve spectral window IDs to DATA_DESC_IDs using the DATA_DESCRIPTION subtable.
fn resolve_spw_to_ddid(ms: &MeasurementSet, spw_ids: &[i32]) -> MsResult<Vec<i32>> {
    let dd = ms.data_description()?;
    let mut ddids = Vec::new();
    for row in 0..dd.row_count() {
        let spw_id = dd.spectral_window_id(row)?;
        if spw_ids.contains(&spw_id) {
            ddids.push(row as i32);
        }
    }
    Ok(ddids)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::SubtableId;
    use crate::test_helpers::{default_value, default_value_for_def};

    #[test]
    fn empty_selection_selects_all() {
        let sel = MsSelection::new();
        assert_eq!(sel.to_taql(), "SELECT *");
    }

    #[test]
    fn field_selection() {
        let sel = MsSelection::new().field(&[0, 1]);
        assert_eq!(sel.to_taql(), "SELECT * WHERE FIELD_ID IN [0,1]");
    }

    #[test]
    fn combined_selection() {
        let sel = MsSelection::new().field(&[0]).scan(&[1, 2, 3]);
        let taql = sel.to_taql();
        assert!(taql.contains("FIELD_ID IN [0]"));
        assert!(taql.contains("SCAN_NUMBER IN [1,2,3]"));
        assert!(taql.contains(" AND "));
    }

    #[test]
    fn time_range_selection() {
        let sel = MsSelection::new().time_range(1000.0, 2000.0);
        let taql = sel.to_taql();
        assert!(taql.contains("TIME>=1000"));
        assert!(taql.contains("TIME<=2000"));
    }

    #[test]
    fn antenna_selection() {
        let sel = MsSelection::new().antenna(&[0, 1]);
        let taql = sel.to_taql();
        assert!(taql.contains("ANTENNA1 IN [0,1]"));
        assert!(taql.contains("ANTENNA2 IN [0,1]"));
    }

    #[test]
    fn baseline_selection() {
        let sel = MsSelection::new().baseline(&[(0, 1), (2, 3)]);
        let taql = sel.to_taql();
        assert!(taql.contains("ANTENNA1==0 AND ANTENNA2==1"));
        assert!(taql.contains("ANTENNA1==2 AND ANTENNA2==3"));
    }

    #[test]
    fn raw_taql_expression() {
        let sel = MsSelection::new().taql("UVW[0] > 100");
        let taql = sel.to_taql();
        assert!(taql.contains("(UVW[0] > 100)"));
    }

    #[test]
    fn apply_field_selection() {
        use crate::builder::MeasurementSetBuilder;
        use crate::test_helpers::default_value;
        use casacore_types::{RecordField, RecordValue, ScalarValue, Value};

        let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();

        // Add rows with different FIELD_IDs
        let schema = ms.main_table().schema().unwrap().clone();
        for field_id in [0, 0, 1, 1, 2] {
            let fields: Vec<RecordField> = schema
                .columns()
                .iter()
                .map(|col| {
                    if col.name() == "FIELD_ID" {
                        RecordField::new("FIELD_ID", Value::Scalar(ScalarValue::Int32(field_id)))
                    } else {
                        RecordField::new(col.name(), default_value(col.name()))
                    }
                })
                .collect();
            ms.main_table_mut()
                .add_row(RecordValue::new(fields))
                .unwrap();
        }

        let sel = MsSelection::new().field(&[0, 1]);
        let rows = sel.apply(&mut ms).unwrap();
        assert_eq!(rows.len(), 4);
    }

    #[test]
    fn apply_time_range_selection() {
        use crate::builder::MeasurementSetBuilder;
        use crate::test_helpers::default_value;
        use casacore_types::{RecordField, RecordValue, ScalarValue, Value};

        let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();

        let schema = ms.main_table().schema().unwrap().clone();
        for time in [100.0, 200.0, 300.0, 400.0, 500.0] {
            let fields: Vec<RecordField> = schema
                .columns()
                .iter()
                .map(|col| {
                    if col.name() == "TIME" {
                        RecordField::new("TIME", Value::Scalar(ScalarValue::Float64(time)))
                    } else {
                        RecordField::new(col.name(), default_value(col.name()))
                    }
                })
                .collect();
            ms.main_table_mut()
                .add_row(RecordValue::new(fields))
                .unwrap();
        }

        let sel = MsSelection::new().time_range(200.0, 400.0);
        let rows = sel.apply(&mut ms).unwrap();
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn apply_antenna_name_selection() {
        use crate::builder::MeasurementSetBuilder;
        use casacore_types::{RecordField, RecordValue, ScalarValue, Value};

        let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();

        // Add antennas
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
            ant.add_antenna(
                "VLA03",
                "N03",
                "GROUND-BASED",
                "ALT-AZ",
                [0.0; 3],
                [0.0; 3],
                25.0,
            )
            .unwrap();
        }

        // Add rows referencing different antennas
        let schema = ms.main_table().schema().unwrap().clone();
        for (ant1, ant2) in [(0, 1), (0, 2), (1, 2)] {
            let fields: Vec<RecordField> = schema
                .columns()
                .iter()
                .map(|col| match col.name() {
                    "ANTENNA1" => {
                        RecordField::new("ANTENNA1", Value::Scalar(ScalarValue::Int32(ant1)))
                    }
                    "ANTENNA2" => {
                        RecordField::new("ANTENNA2", Value::Scalar(ScalarValue::Int32(ant2)))
                    }
                    name => RecordField::new(name, default_value(name)),
                })
                .collect();
            ms.main_table_mut()
                .add_row(RecordValue::new(fields))
                .unwrap();
        }

        let sel = MsSelection::new().antenna_name(&["VLA01"]);
        let rows = sel.apply(&mut ms).unwrap();
        // VLA01 is antenna 0, which appears in rows 0 (ant1=0) and 1 (ant1=0)
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn apply_spw_selection_without_matching_ddid_returns_no_rows() {
        use crate::builder::MeasurementSetBuilder;
        use crate::schema;
        use casacore_types::{RecordField, RecordValue, ScalarValue, Value};

        let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();

        let dd_fields: Vec<RecordField> = schema::data_description::REQUIRED_COLUMNS
            .iter()
            .map(|col| match col.name {
                "SPECTRAL_WINDOW_ID" => {
                    RecordField::new(col.name, Value::Scalar(ScalarValue::Int32(0)))
                }
                "POLARIZATION_ID" => {
                    RecordField::new(col.name, Value::Scalar(ScalarValue::Int32(0)))
                }
                "FLAG_ROW" => RecordField::new(col.name, Value::Scalar(ScalarValue::Bool(false))),
                _ => RecordField::new(col.name, default_value_for_def(col)),
            })
            .collect();
        ms.subtable_mut(SubtableId::DataDescription)
            .unwrap()
            .add_row(RecordValue::new(dd_fields))
            .unwrap();

        let schema = ms.main_table().schema().unwrap().clone();
        let row_fields: Vec<RecordField> = schema
            .columns()
            .iter()
            .map(|col| match col.name() {
                "DATA_DESC_ID" => {
                    RecordField::new(col.name(), Value::Scalar(ScalarValue::Int32(7)))
                }
                name => RecordField::new(name, default_value(name)),
            })
            .collect();
        ms.main_table_mut()
            .add_row(RecordValue::new(row_fields))
            .unwrap();

        let rows = MsSelection::new().spw(&[7]).apply(&mut ms).unwrap();
        assert!(rows.is_empty());
    }
}
