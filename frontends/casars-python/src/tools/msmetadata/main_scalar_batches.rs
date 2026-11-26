// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_ms::{MeasurementSet, MsError, MsResult};
use casa_tables::{RequiredScalarColumnDestination, RequiredScalarColumnValuesMut, Table};
use casa_types::ScalarValue;

/// Read only the scalar columns needed by a MAIN metadata operation. Keep
/// chunk allocations reusable, including when the table is a reference or a
/// concatenation and its row map must be resolved by the selected-cell path.
pub(super) fn main_scalar_batches<const I: usize, const F: usize>(
    ms: &MeasurementSet,
    integer_columns: [&str; I],
    floating_columns: [&str; F],
    with_row_flags: bool,
    batch_rows: usize,
    mut visit: impl FnMut(&[usize], &[Vec<i32>; I], &[Vec<f64>; F], &[bool]) -> MsResult<()>,
) -> MsResult<()> {
    let main = ms.main_table();
    let root = ms.path().expect("an attached MS has a path");
    let direct = !main.is_reference_table()? && Table::concatenated_source_paths(root)?.is_none();
    let mut rows = Vec::new();
    let mut integers = std::array::from_fn(|_| Vec::new());
    let mut floats = std::array::from_fn(|_| Vec::new());
    let mut row_flags = Vec::new();
    for start in (0..main.row_count()).step_by(batch_rows) {
        rows.clear();
        rows.extend(start..(start + batch_rows).min(main.row_count()));
        if direct {
            let mut destinations = Vec::with_capacity(I + F + usize::from(with_row_flags));
            destinations.extend(integer_columns.into_iter().zip(&mut integers).map(
                |(name, values)| {
                    RequiredScalarColumnDestination::new(
                        name,
                        RequiredScalarColumnValuesMut::Int32(values),
                    )
                },
            ));
            destinations.extend(floating_columns.into_iter().zip(&mut floats).map(
                |(name, values)| {
                    RequiredScalarColumnDestination::new(
                        name,
                        RequiredScalarColumnValuesMut::Float64(values),
                    )
                },
            ));
            if with_row_flags {
                destinations.push(RequiredScalarColumnDestination::new(
                    "FLAG_ROW",
                    RequiredScalarColumnValuesMut::Bool(&mut row_flags),
                ));
            }
            main.required_scalar_columns_for_rows_into(&rows, &mut destinations)?;
        } else {
            for (column, values) in integer_columns.into_iter().zip(&mut integers) {
                read_selected(main, &rows, column, values, |value| match value {
                    ScalarValue::Int32(value) => Some(value),
                    _ => None,
                })?;
            }
            for (column, values) in floating_columns.into_iter().zip(&mut floats) {
                read_selected(main, &rows, column, values, |value| match value {
                    ScalarValue::Float64(value) => Some(value),
                    _ => None,
                })?;
            }
            if with_row_flags {
                read_selected(
                    main,
                    &rows,
                    "FLAG_ROW",
                    &mut row_flags,
                    |value| match value {
                        ScalarValue::Bool(value) => Some(value),
                        _ => None,
                    },
                )?;
            }
        }
        visit(&rows, &integers, &floats, &row_flags)?;
    }
    Ok(())
}

fn read_selected<T>(
    table: &Table,
    rows: &[usize],
    column: &str,
    values: &mut Vec<T>,
    cast: impl Fn(ScalarValue) -> Option<T>,
) -> MsResult<()> {
    values.clear();
    for cell in table
        .column_accessor(column)?
        .scalar_cells_owned_for_rows(rows)?
    {
        let value = cell.and_then(&cast).ok_or_else(|| {
            MsError::InvalidInput(format!(
                "MAIN.{column} has a missing or wrongly typed value"
            ))
        })?;
        values.push(value);
    }
    Ok(())
}
