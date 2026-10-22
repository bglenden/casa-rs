// SPDX-License-Identifier: LGPL-3.0-or-later
//! Array column sources: the data-manager groups a save writes from a
//! [`GeneratedArrayColumn`], and the checks on its cells.
//!
//! A group holding a generated array column is written from its column
//! sources, a cell at a time, by the StandardStMan or IncrementalStMan
//! group writer, so its rows are never materialized as records. That needs
//! every column of the group overridden; other storage managers, and
//! IncrementalStMan's variable-shape arrays and fixed-shape arrays without
//! the Direct option, refuse the override.

use std::borrow::Cow;

use casa_types::{PrimitiveType, Value};

use super::StorageError;
use super::incremental_stman::refuse_ism_indirect_fixed_arrays;
use super::scalar_sources::{ColumnSource, ColumnSources, scalar_column_source};
use super::table_control::ColumnDescContents;
use crate::table::{ColumnOverride, ColumnOverrides, DataManagerKind, GeneratedArrayColumn};

/// A generated array column with the type and shape its cells must have.
pub(crate) struct GeneratedArraySource<'a> {
    column: &'a GeneratedArrayColumn,
    name: String,
    data_type: Option<PrimitiveType>,
    /// The fixed cell shape, or `None` for a variable-shape column.
    shape: Option<Vec<usize>>,
    /// The dimensionality a variable-shape column fixes, if any.
    ndim: Option<usize>,
}

impl<'a> GeneratedArraySource<'a> {
    fn new(column: &'a GeneratedArrayColumn, desc: &ColumnDescContents) -> Self {
        let shape =
            (!desc.shape.is_empty()).then(|| desc.shape.iter().map(|&dim| dim as usize).collect());
        Self {
            column,
            name: desc.col_name.clone(),
            data_type: desc.primitive_type,
            shape,
            ndim: usize::try_from(desc.nrdim).ok().filter(|&ndim| ndim > 0),
        }
    }

    /// The cell in `row`, refused when its type or shape does not fit the
    /// column.
    pub(crate) fn cell(&self, row: usize) -> Result<Option<Cow<'a, Value>>, StorageError> {
        let Some(cell) = self.column.cell(row) else {
            return Ok(None);
        };
        let Value::Array(array) = cell.as_ref() else {
            unreachable!("a generated array cell is an array");
        };
        let mismatch = |what: String| {
            Err(StorageError::FormatMismatch(format!(
                "generated array column {} row {row}: {what}",
                self.name
            )))
        };
        if self.data_type != Some(array.primitive_type()) {
            return mismatch(format!(
                "{:?} cell in a {:?} column",
                array.primitive_type(),
                self.data_type
            ));
        }
        if let Some(shape) = &self.shape {
            if array.shape() != shape.as_slice() {
                return mismatch(format!(
                    "shape {:?} in a column of shape {shape:?}",
                    array.shape()
                ));
            }
        } else if let Some(ndim) = self.ndim
            && array.ndim() != ndim
        {
            return mismatch(format!("{} axes in a column of {ndim}", array.ndim()));
        }
        Ok(Some(cell))
    }
}

/// The column sources of a data-manager group that holds a generated array
/// column, or `None` when it holds none.
///
/// # Errors
///
/// The group's storage manager is not StandardStMan or IncrementalStMan; a
/// column of the group has no override, is deferred or is a record column;
/// an override does not fit its column; or an IncrementalStMan column has
/// arrays casacore keeps in its `table.fNi` array file, which its writer
/// does not store: variable-shape arrays, and fixed-shape arrays without the
/// Direct option ([`StorageError::IsmIndirectArrayColumn`]).
pub(super) fn generated_array_group_sources<'a>(
    dm_kind: DataManagerKind,
    group_col_descs: &[ColumnDescContents],
    column_overrides: &'a ColumnOverrides,
    row_count: usize,
) -> Result<Option<ColumnSources<'a>>, StorageError> {
    let Some(generated) = group_col_descs.iter().find(|desc| {
        matches!(
            column_overrides.get(&desc.col_name),
            Some(ColumnOverride::GeneratedArray(_))
        )
    }) else {
        return Ok(None);
    };
    let refuse = |message: String| Err(StorageError::FormatMismatch(message));
    if !matches!(
        dm_kind,
        DataManagerKind::StandardStMan | DataManagerKind::IncrementalStMan
    ) {
        return refuse(format!(
            "generated array column {} needs StandardStMan or IncrementalStMan, not {dm_kind:?}",
            generated.col_name
        ));
    }
    let mut columns = Vec::with_capacity(group_col_descs.len());
    for desc in group_col_descs {
        let Some(override_value) = column_overrides.get(&desc.col_name) else {
            return refuse(format!(
                "generated array column {} shares its {dm_kind:?} group with column {}, \
                 which has no override; every column of the group must be overridden",
                generated.col_name, desc.col_name
            ));
        };
        if desc.is_record() {
            return refuse(format!(
                "record column {} cannot share a group with generated array column {}",
                desc.col_name, generated.col_name
            ));
        }
        if desc.is_array && desc.shape.is_empty() && dm_kind == DataManagerKind::IncrementalStMan {
            return refuse(format!(
                "IncrementalStMan does not write variable-shape array column {}",
                desc.col_name
            ));
        }
        if dm_kind == DataManagerKind::IncrementalStMan {
            refuse_ism_indirect_fixed_arrays(std::slice::from_ref(desc))?;
        }
        let source = if desc.is_array {
            match override_value {
                ColumnOverride::Values(values) => ColumnSource::Cells(values),
                ColumnOverride::GeneratedArray(column) => {
                    ColumnSource::GeneratedArray(GeneratedArraySource::new(column, desc))
                }
                ColumnOverride::GeneratedScalar(_) | ColumnOverride::StreamedScalar(_) => {
                    return refuse(format!(
                        "array column {} cannot take a scalar override",
                        desc.col_name
                    ));
                }
                ColumnOverride::Deferred => return Err(deferred(desc, generated)),
            }
        } else {
            scalar_column_source(desc, override_value)?.ok_or_else(|| deferred(desc, generated))?
        };
        columns.push(source);
    }
    Ok(Some(ColumnSources::new(row_count, columns)))
}

fn deferred(desc: &ColumnDescContents, generated: &ColumnDescContents) -> StorageError {
    StorageError::FormatMismatch(format!(
        "deferred column {} cannot share a group with generated array column {}",
        desc.col_name, generated.col_name
    ))
}
