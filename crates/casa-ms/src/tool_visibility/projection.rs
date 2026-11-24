// SPDX-License-Identifier: LGPL-3.0-or-later
//! Batched scalar reads through a CASA reference table's projection.

use std::collections::HashMap;

use casa_tables::{PlainReferenceRead, RequiredScalarColumnValues};

use crate::MsResult;

/// Resolve exposed column names and batch-read already translated parent rows.
pub(crate) fn read_reference_scalars(
    reference: &PlainReferenceRead,
    columns: &[&str],
    parent_rows: &[usize],
) -> MsResult<HashMap<String, RequiredScalarColumnValues>> {
    let physical = columns
        .iter()
        .map(|name| reference.parent_column(name))
        .collect::<Result<Vec<_>, _>>()?;
    let mut values = reference
        .parent()
        .required_scalar_columns_owned_for_rows(&physical, parent_rows)?;
    if columns == physical.as_slice() {
        return Ok(values);
    }

    let mut logical = HashMap::with_capacity(columns.len());
    for (index, (&name, &parent_name)) in columns.iter().zip(&physical).enumerate() {
        // Several exposed names can refer to the same parent column.
        let column = if physical[index + 1..].contains(&parent_name) {
            values[parent_name].clone()
        } else {
            values.remove(parent_name).expect("required column loaded")
        };
        logical.insert(name.to_owned(), column);
    }
    Ok(logical)
}
