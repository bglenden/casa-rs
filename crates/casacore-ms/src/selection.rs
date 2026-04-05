// SPDX-License-Identifier: LGPL-3.0-or-later
//! Builder-pattern MS selection that translates to TaQL queries.
//!
//! [`MsSelection`] lets callers build up selection criteria (field, spectral
//! window, antenna, time range, scan, observation, array) and then apply them
//! to a [`MeasurementSet`] via TaQL WHERE clauses.
//!
//! # Example
//!
//! ```rust
//! use casacore_ms::selection::MsSelection;
//!
//! let sel = MsSelection::new()
//!     .field(&[0, 1])
//!     .spw(&[0])
//!     .state(&[0])
//!     .scan(&[1, 2, 3]);
//! assert!(sel.to_taql().contains("FIELD_ID"));
//! ```
//!
//! Cf. C++ `MSSelection`.

use crate::error::{MsError, MsResult};
use crate::ms::MeasurementSet;
use casacore_tables::Table;
use casacore_types::{RecordField, RecordValue, ScalarValue, Value};
use std::collections::HashSet;

/// Builder for MS row selection criteria.
///
/// Each method adds a constraint. Constraints are ANDed together when
/// converted to TaQL. Call [`to_taql`](MsSelection::to_taql) to get the
/// WHERE clause, or [`apply`](MsSelection::apply) to execute it against a
/// [`MeasurementSet`].
///
/// Cf. C++ `MSSelection`.
#[derive(Debug, Clone, Default)]
pub struct MsSelection {
    field_ids: Vec<i32>,
    spw_ids: Vec<i32>,
    data_desc_ids: Vec<i32>,
    antenna_ids: Vec<i32>,
    antenna_names: Vec<String>,
    baselines: Vec<(i32, i32)>,
    time_range: Option<(f64, f64)>,
    scan_numbers: Vec<i32>,
    state_ids: Vec<i32>,
    observation_ids: Vec<i32>,
    array_ids: Vec<i32>,
    taql_exprs: Vec<String>,
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

    /// Select rows matching the given DATA_DESC_IDs directly.
    pub fn data_description(mut self, ids: &[i32]) -> Self {
        self.data_desc_ids.extend_from_slice(ids);
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

    /// Select rows matching the given STATE_IDs.
    pub fn state(mut self, ids: &[i32]) -> Self {
        self.state_ids.extend_from_slice(ids);
        self
    }

    /// Select rows matching the given OBSERVATION_IDs.
    pub fn observation(mut self, ids: &[i32]) -> Self {
        self.observation_ids.extend_from_slice(ids);
        self
    }

    /// Select rows matching the given ARRAY_IDs.
    pub fn array(mut self, ids: &[i32]) -> Self {
        self.array_ids.extend_from_slice(ids);
        self
    }

    /// Add a raw TaQL WHERE expression (ANDed with other criteria).
    pub fn taql(mut self, expr: &str) -> Self {
        self.taql_exprs.push(expr.to_string());
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
    pub fn apply(&self, ms: &MeasurementSet) -> MsResult<Vec<usize>> {
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

        if sel.taql_exprs.is_empty() {
            return apply_structured_selection(&sel, ms, &effective_ddids);
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

        let view = ms.main_table().query(&taql)?;
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

        if !self.data_desc_ids.is_empty() {
            clauses.push(format!(
                "DATA_DESC_ID IN [{}]",
                int_list(&self.data_desc_ids)
            ));
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

        if !self.state_ids.is_empty() {
            clauses.push(format!("STATE_ID IN [{}]", int_list(&self.state_ids)));
        }

        if !self.observation_ids.is_empty() {
            clauses.push(format!(
                "OBSERVATION_ID IN [{}]",
                int_list(&self.observation_ids)
            ));
        }

        if !self.array_ids.is_empty() {
            clauses.push(format!("ARRAY_ID IN [{}]", int_list(&self.array_ids)));
        }

        for expr in &self.taql_exprs {
            clauses.push(format!("({expr})"));
        }

        clauses
    }
}

fn apply_structured_selection(
    sel: &MsSelection,
    ms: &MeasurementSet,
    effective_ddids: &[i32],
) -> MsResult<Vec<usize>> {
    let field_ids: HashSet<i32> = sel.field_ids.iter().copied().collect();
    let spw_ddids: HashSet<i32> = effective_ddids.iter().copied().collect();
    let data_desc_ids: HashSet<i32> = sel.data_desc_ids.iter().copied().collect();
    let antenna_ids: HashSet<i32> = sel.antenna_ids.iter().copied().collect();
    let baselines: HashSet<(i32, i32)> = sel.baselines.iter().copied().collect();
    let scan_numbers: HashSet<i32> = sel.scan_numbers.iter().copied().collect();
    let state_ids: HashSet<i32> = sel.state_ids.iter().copied().collect();
    let observation_ids: HashSet<i32> = sel.observation_ids.iter().copied().collect();
    let array_ids: HashSet<i32> = sel.array_ids.iter().copied().collect();

    if !sel.spw_ids.is_empty() && spw_ddids.is_empty() {
        return Ok(Vec::new());
    }

    let table = ms.main_table();
    let columns = load_structured_selection_columns(
        table,
        StructuredSelectionColumnRequest {
            field: !field_ids.is_empty(),
            data_desc: !sel.spw_ids.is_empty() || !data_desc_ids.is_empty(),
            antenna1: !antenna_ids.is_empty() || !baselines.is_empty(),
            antenna2: !antenna_ids.is_empty() || !baselines.is_empty(),
            time: sel.time_range.is_some(),
            scan: !scan_numbers.is_empty(),
            state: !state_ids.is_empty(),
            observation: !observation_ids.is_empty(),
            array: !array_ids.is_empty(),
        },
    )?;
    let mut rows = Vec::new();

    for row in 0..table.row_count() {
        if let Some(field_column) = columns.field.as_ref() {
            if !field_ids.contains(&field_column[row]) {
                continue;
            }
        }

        if let Some(data_desc_column) = columns.data_desc.as_ref() {
            let data_desc_id = data_desc_column[row];
            if !spw_ddids.is_empty() && !spw_ddids.contains(&data_desc_id) {
                continue;
            }
            if !data_desc_ids.is_empty() && !data_desc_ids.contains(&data_desc_id) {
                continue;
            }
        }

        if let (Some(antenna1_column), Some(antenna2_column)) =
            (columns.antenna1.as_ref(), columns.antenna2.as_ref())
        {
            let antenna1 = antenna1_column[row];
            let antenna2 = antenna2_column[row];
            if !antenna_ids.is_empty()
                && !antenna_ids.contains(&antenna1)
                && !antenna_ids.contains(&antenna2)
            {
                continue;
            }
            if !baselines.is_empty() && !baselines.contains(&(antenna1, antenna2)) {
                continue;
            }
        }

        if let (Some((start, end)), Some(time_column)) = (sel.time_range, columns.time.as_ref()) {
            let time = time_column[row];
            if time < start || time > end {
                continue;
            }
        }

        if let Some(scan_column) = columns.scan.as_ref() {
            if !scan_numbers.contains(&scan_column[row]) {
                continue;
            }
        }
        if let Some(state_column) = columns.state.as_ref() {
            if !state_ids.contains(&state_column[row]) {
                continue;
            }
        }
        if let Some(observation_column) = columns.observation.as_ref() {
            if !observation_ids.contains(&observation_column[row]) {
                continue;
            }
        }
        if let Some(array_column) = columns.array.as_ref() {
            if !array_ids.contains(&array_column[row]) {
                continue;
            }
        }

        rows.push(row);
    }

    Ok(rows)
}

#[derive(Clone, Copy, Debug, Default)]
struct StructuredSelectionColumnRequest {
    field: bool,
    data_desc: bool,
    antenna1: bool,
    antenna2: bool,
    time: bool,
    scan: bool,
    state: bool,
    observation: bool,
    array: bool,
}

#[derive(Debug, Default)]
struct StructuredSelectionColumns {
    field: Option<Vec<i32>>,
    data_desc: Option<Vec<i32>>,
    antenna1: Option<Vec<i32>>,
    antenna2: Option<Vec<i32>>,
    time: Option<Vec<f64>>,
    scan: Option<Vec<i32>>,
    state: Option<Vec<i32>>,
    observation: Option<Vec<i32>>,
    array: Option<Vec<i32>>,
}

fn load_structured_selection_columns(
    table: &Table,
    request: StructuredSelectionColumnRequest,
) -> MsResult<StructuredSelectionColumns> {
    let rows = table.rows()?;
    let mut columns = StructuredSelectionColumns::default();

    if rows.is_empty() {
        if request.field {
            columns.field = Some(Vec::new());
        }
        if request.data_desc {
            columns.data_desc = Some(Vec::new());
        }
        if request.antenna1 {
            columns.antenna1 = Some(Vec::new());
        }
        if request.antenna2 {
            columns.antenna2 = Some(Vec::new());
        }
        if request.time {
            columns.time = Some(Vec::new());
        }
        if request.scan {
            columns.scan = Some(Vec::new());
        }
        if request.state {
            columns.state = Some(Vec::new());
        }
        if request.observation {
            columns.observation = Some(Vec::new());
        }
        if request.array {
            columns.array = Some(Vec::new());
        }
        return Ok(columns);
    }

    let first_row = &rows[0];
    let field_index = request
        .field
        .then(|| find_column_index(first_row, "FIELD_ID"))
        .transpose()?;
    let data_desc_index = request
        .data_desc
        .then(|| find_column_index(first_row, "DATA_DESC_ID"))
        .transpose()?;
    let antenna1_index = request
        .antenna1
        .then(|| find_column_index(first_row, "ANTENNA1"))
        .transpose()?;
    let antenna2_index = request
        .antenna2
        .then(|| find_column_index(first_row, "ANTENNA2"))
        .transpose()?;
    let time_index = request
        .time
        .then(|| find_column_index(first_row, "TIME"))
        .transpose()?;
    let scan_index = request
        .scan
        .then(|| find_column_index(first_row, "SCAN_NUMBER"))
        .transpose()?;
    let state_index = request
        .state
        .then(|| find_column_index(first_row, "STATE_ID"))
        .transpose()?;
    let observation_index = request
        .observation
        .then(|| find_column_index(first_row, "OBSERVATION_ID"))
        .transpose()?;
    let array_index = request
        .array
        .then(|| find_column_index(first_row, "ARRAY_ID"))
        .transpose()?;

    if request.field {
        columns.field = Some(Vec::with_capacity(rows.len()));
    }
    if request.data_desc {
        columns.data_desc = Some(Vec::with_capacity(rows.len()));
    }
    if request.antenna1 {
        columns.antenna1 = Some(Vec::with_capacity(rows.len()));
    }
    if request.antenna2 {
        columns.antenna2 = Some(Vec::with_capacity(rows.len()));
    }
    if request.time {
        columns.time = Some(Vec::with_capacity(rows.len()));
    }
    if request.scan {
        columns.scan = Some(Vec::with_capacity(rows.len()));
    }
    if request.state {
        columns.state = Some(Vec::with_capacity(rows.len()));
    }
    if request.observation {
        columns.observation = Some(Vec::with_capacity(rows.len()));
    }
    if request.array {
        columns.array = Some(Vec::with_capacity(rows.len()));
    }

    for row in rows {
        let fields = row.fields();
        if let (Some(index), Some(values)) = (field_index, columns.field.as_mut()) {
            values.push(read_i32_field(fields, index, "FIELD_ID")?);
        }
        if let (Some(index), Some(values)) = (data_desc_index, columns.data_desc.as_mut()) {
            values.push(read_i32_field(fields, index, "DATA_DESC_ID")?);
        }
        if let (Some(index), Some(values)) = (antenna1_index, columns.antenna1.as_mut()) {
            values.push(read_i32_field(fields, index, "ANTENNA1")?);
        }
        if let (Some(index), Some(values)) = (antenna2_index, columns.antenna2.as_mut()) {
            values.push(read_i32_field(fields, index, "ANTENNA2")?);
        }
        if let (Some(index), Some(values)) = (time_index, columns.time.as_mut()) {
            values.push(read_f64_field(fields, index, "TIME")?);
        }
        if let (Some(index), Some(values)) = (scan_index, columns.scan.as_mut()) {
            values.push(read_i32_field(fields, index, "SCAN_NUMBER")?);
        }
        if let (Some(index), Some(values)) = (state_index, columns.state.as_mut()) {
            values.push(read_i32_field(fields, index, "STATE_ID")?);
        }
        if let (Some(index), Some(values)) = (observation_index, columns.observation.as_mut()) {
            values.push(read_i32_field(fields, index, "OBSERVATION_ID")?);
        }
        if let (Some(index), Some(values)) = (array_index, columns.array.as_mut()) {
            values.push(read_i32_field(fields, index, "ARRAY_ID")?);
        }
    }

    Ok(columns)
}

fn find_column_index(row: &RecordValue, column: &str) -> MsResult<usize> {
    row.fields()
        .iter()
        .position(|field| field.name == column)
        .ok_or_else(|| MsError::MissingColumn {
            column: column.to_string(),
            table: "MAIN".to_string(),
        })
}

fn read_i32_field(fields: &[RecordField], index: usize, column: &str) -> MsResult<i32> {
    let field = fields.get(index).ok_or_else(|| MsError::MissingColumn {
        column: column.to_string(),
        table: "MAIN".to_string(),
    })?;
    if field.name != column {
        return Err(MsError::MissingColumn {
            column: column.to_string(),
            table: "MAIN".to_string(),
        });
    }
    match &field.value {
        Value::Scalar(ScalarValue::Int32(value)) => Ok(*value),
        other => Err(MsError::ColumnTypeMismatch {
            column: column.to_string(),
            table: "MAIN".to_string(),
            expected: "Int32".to_string(),
            found: format!("{:?}", other.kind()),
        }),
    }
}

fn read_f64_field(fields: &[RecordField], index: usize, column: &str) -> MsResult<f64> {
    let field = fields.get(index).ok_or_else(|| MsError::MissingColumn {
        column: column.to_string(),
        table: "MAIN".to_string(),
    })?;
    if field.name != column {
        return Err(MsError::MissingColumn {
            column: column.to_string(),
            table: "MAIN".to_string(),
        });
    }
    match &field.value {
        Value::Scalar(ScalarValue::Float64(value)) => Ok(*value),
        other => Err(MsError::ColumnTypeMismatch {
            column: column.to_string(),
            table: "MAIN".to_string(),
            expected: "Float64".to_string(),
            found: format!("{:?}", other.kind()),
        }),
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
        let sel = MsSelection::new().taql("UVW[0] > 100").taql("TIME > 0");
        let taql = sel.to_taql();
        assert!(taql.contains("(UVW[0] > 100)"));
        assert!(taql.contains("(TIME > 0)"));
    }

    #[test]
    fn data_description_selection() {
        let sel = MsSelection::new().data_description(&[2, 4]);
        let taql = sel.to_taql();
        assert!(taql.contains("DATA_DESC_ID IN [2,4]"));
    }

    #[test]
    fn state_selection() {
        let sel = MsSelection::new().state(&[1, 5]);
        let taql = sel.to_taql();
        assert!(taql.contains("STATE_ID IN [1,5]"));
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
        let rows = sel.apply(&ms).unwrap();
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
        let rows = sel.apply(&ms).unwrap();
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
        let rows = sel.apply(&ms).unwrap();
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

        let rows = MsSelection::new().spw(&[7]).apply(&ms).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn apply_field_and_spw_selection_returns_only_intersection() {
        use crate::builder::MeasurementSetBuilder;
        use casacore_types::{RecordField, RecordValue, ScalarValue, Value};

        let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();

        ms.subtable_mut(SubtableId::DataDescription)
            .unwrap()
            .add_row(RecordValue::new(vec![
                RecordField::new("SPECTRAL_WINDOW_ID", Value::Scalar(ScalarValue::Int32(0))),
                RecordField::new("POLARIZATION_ID", Value::Scalar(ScalarValue::Int32(0))),
                RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            ]))
            .unwrap();
        ms.subtable_mut(SubtableId::DataDescription)
            .unwrap()
            .add_row(RecordValue::new(vec![
                RecordField::new("SPECTRAL_WINDOW_ID", Value::Scalar(ScalarValue::Int32(1))),
                RecordField::new("POLARIZATION_ID", Value::Scalar(ScalarValue::Int32(0))),
                RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            ]))
            .unwrap();

        let schema = ms.main_table().schema().unwrap().clone();
        for (field_id, data_desc_id) in [(0, 0), (0, 1), (1, 0), (1, 1)] {
            let row_fields: Vec<RecordField> = schema
                .columns()
                .iter()
                .map(|col| match col.name() {
                    "FIELD_ID" => {
                        RecordField::new(col.name(), Value::Scalar(ScalarValue::Int32(field_id)))
                    }
                    "DATA_DESC_ID" => RecordField::new(
                        col.name(),
                        Value::Scalar(ScalarValue::Int32(data_desc_id)),
                    ),
                    name => RecordField::new(name, default_value(name)),
                })
                .collect();
            ms.main_table_mut()
                .add_row(RecordValue::new(row_fields))
                .unwrap();
        }

        let rows = MsSelection::new().field(&[0]).spw(&[1]).apply(&ms).unwrap();
        assert_eq!(rows, vec![1]);
    }
}
