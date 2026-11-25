// SPDX-License-Identifier: LGPL-3.0-or-later
//! Ordered copies for the public MeasurementSet tool.

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use casa_tables::{ColumnOverrides, ColumnType, Table, TableOptions};
use casa_types::ScalarValue;

use crate::ms::{measurement_set_main_table_bindings, measurement_set_table_options};
use crate::transform::task_history::append_history_messages;
use crate::{MeasurementSet, MsError, MsResult};

mod subtables;
pub(crate) use subtables::check_named_output as check_named_table_output;

/// Write a sorted, independent MeasurementSet at a staging path.
///
/// The caller owns publication. `scratch` holds the temporary projection of
/// the input and must remain available until the deep copy is complete.
pub fn write_sorted_measurement_set(
    input: &MeasurementSet,
    scratch: &Path,
    staged_output: &Path,
    columns: Option<&[String]>,
    in_place: bool,
    input_name: &Path,
    held_tables: &BTreeSet<PathBuf>,
) -> MsResult<()> {
    let main = input.main_table();
    let keys = columns.map_or_else(
        || vec!["TIME"],
        |columns| columns.iter().map(String::as_str).collect(),
    );
    let rows = ordered_rows(main, &keys)?;
    let projection = main.select_rows(&rows)?;
    let view_path = scratch.join("ordered.ref");
    projection.save(TableOptions::new(&view_path))?;
    let mut ordered = Table::open(TableOptions::new(&view_path))?;
    // These optional iterator caches refer to the input's row order.
    let mut keywords = main.keywords().clone();
    keywords.remove("SORTED_TABLE");
    subtables::copy_links(
        main,
        &mut keywords,
        &mut ordered,
        staged_output,
        held_tables,
    )?;
    *ordered.keywords_mut() = keywords;
    let bindings = measurement_set_main_table_bindings(&ordered);
    ordered.prepare_write().save_with_column_overrides(
        measurement_set_table_options(staged_output),
        &bindings,
        &ColumnOverrides::default(),
    )?;
    let (origin, names) = match columns {
        None => ("ms::timesort()", "TIME".to_string()),
        Some(columns) => ("ms::sort()", format!("[{}]", columns.join(","))),
    };
    let message = if in_place {
        format!("Sorted by {names} in ascending order.")
    } else {
        let expression = if columns.is_some() {
            format!(" {names} ")
        } else {
            names
        };
        format!(
            "Generated from {} by sorting by {expression} in ascending order.",
            input_name.display()
        )
    };
    append_history_messages(staged_output, origin, [message])
        .map_err(|error| MsError::InvalidInput(error.to_string()))
}

/// Reject a named output that would replace or be staged inside a backing
/// subtable. A new direct child of the source MAIN remains allowed.
pub fn check_named_sort_output(input: &MeasurementSet, output: &Path) -> MsResult<()> {
    subtables::check_named_output(input.main_table(), output)
}

fn ordered_rows(table: &Table, names: &[&str]) -> MsResult<Vec<usize>> {
    let mut rows = (0..table.row_count()).collect::<Vec<_>>();
    if names.is_empty() {
        return Ok(rows);
    }
    for name in names {
        let column = table
            .schema()
            .and_then(|schema| schema.column(name))
            .ok_or_else(|| MsError::InvalidInput(format!("Table column {name} is unknown")))?;
        if *column.column_type() != ColumnType::Scalar {
            return Err(MsError::InvalidInput(format!(
                "Invalid table sort: Sort column {name} is not a scalar"
            )));
        }
    }
    // Reading keys through ordinary cell access loads every visibility
    // column before the sorted copy can read it again.
    let values = table.scalar_columns_owned(names)?;
    let keys: Vec<_> = names.iter().map(|name| &values[*name]).collect();
    rows.sort_by(|&a, &b| {
        keys.iter()
            .map(|column| compare_keys(column[a].as_ref(), column[b].as_ref()))
            .find(|order| *order != Ordering::Equal)
            .unwrap_or(Ordering::Equal)
    });
    Ok(rows)
}

fn compare_keys(a: Option<&ScalarValue>, b: Option<&ScalarValue>) -> Ordering {
    match (a, b) {
        (Some(ScalarValue::Complex32(a)), Some(ScalarValue::Complex32(b))) => {
            a.norm_sqr().total_cmp(&b.norm_sqr())
        }
        (Some(ScalarValue::Complex64(a)), Some(ScalarValue::Complex64(b))) => {
            a.norm_sqr().total_cmp(&b.norm_sqr())
        }
        // CASA treats the two zero representations as equal sort keys.
        (Some(ScalarValue::Float32(a)), Some(ScalarValue::Float32(b))) => {
            a.partial_cmp(b).unwrap_or_else(|| a.total_cmp(b))
        }
        (Some(ScalarValue::Float64(a)), Some(ScalarValue::Float64(b))) => {
            a.partial_cmp(b).unwrap_or_else(|| a.total_cmp(b))
        }
        (Some(a), Some(b)) => a.sort_cmp(b).unwrap_or(Ordering::Equal),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}
