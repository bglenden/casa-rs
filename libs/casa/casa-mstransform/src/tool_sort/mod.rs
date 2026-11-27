// SPDX-License-Identifier: LGPL-3.0-or-later
//! Ordered copies for the public MeasurementSet tool.

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use casa_core::ScalarValue;
use casa_tables::{ColumnOverrides, ColumnType, EndianFormat, Table};

use crate::transform::task_history::append_history_messages;
use casa_ms::{MeasurementSet, MsError, MsResult};
use casa_ms::{measurement_set_main_table_bindings, measurement_set_table_options};

pub(crate) mod subtables;

/// Write a sorted, independent MeasurementSet at a staging path.
///
/// The caller owns publication and holds the guards for `held_tables`.
pub fn write_sorted_measurement_set(
    input: &MeasurementSet,
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
    write_selected_measurement_set_rows(
        main,
        &rows,
        staged_output,
        EndianFormat::default(),
        held_tables,
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

/// Write an independent MeasurementSet with the specified MAIN row order.
///
/// The caller owns publication and holds the guards for `held_tables`.
///
/// # Errors
/// Returns an error if the projected rows or linked subtables cannot be saved.
pub fn write_selected_measurement_set_rows(
    main: &Table,
    rows: &[usize],
    staged_output: &Path,
    endian: EndianFormat,
    held_tables: &BTreeSet<PathBuf>,
) -> MsResult<()> {
    let mut ordered = main.to_memory_rows(rows)?;
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
        measurement_set_table_options(staged_output).with_endian_format(endian),
        &bindings,
        &ColumnOverrides::default(),
    )?;
    Ok(())
}

/// Reject a named output that would replace or be staged inside a backing
/// subtable. A new direct child of the source MAIN remains allowed.
pub fn check_named_sort_output(input: &MeasurementSet, output: &Path) -> MsResult<()> {
    check_named_table_output(input.main_table(), output)
}

pub(crate) fn check_named_table_output(input: &Table, output: &Path) -> MsResult<()> {
    check_named_output_path(subtables::table_path(input)?, output)
}

/// Check a canonical named output against the persisted input and its linked tables.
///
/// # Errors
///
/// The output would overlap a source table, or a source table or link could not be opened.
pub fn check_named_output_path(input: &Path, output: &Path) -> MsResult<()> {
    subtables::check_named_output(input, output, None).map(|_| ())
}

/// Check a result published through `owner`'s `keyword` and choose a staging
/// directory outside any linked table trees that the writer may copy.
/// # Errors
/// The output would replace a source table, or a source table or link could not be opened.
pub fn named_keyword_output_staging_parent(
    input: &Path,
    output: &Path,
    owner: &Path,
    keyword: &str,
) -> MsResult<PathBuf> {
    subtables::check_named_output(input, output, Some((owner, keyword)))
}

/// Capture the filesystem dependencies to protect when writing a named
/// result later. The flag identifies a linked subtable whose descendants
/// are protected as well as its own path.
#[doc(hidden)]
pub fn capture_named_output_inputs(input: &Path) -> MsResult<Vec<(PathBuf, bool)>> {
    let mut paths = std::collections::BTreeMap::new();
    subtables::visit_source_tables(input, None, |path, subtable| {
        let stored = paths.entry(path.to_owned()).or_insert(false);
        *stored |= subtable;
        Ok(())
    })?;
    Ok(paths.into_iter().collect())
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
