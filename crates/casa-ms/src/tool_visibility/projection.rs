// SPDX-License-Identifier: LGPL-3.0-or-later
//! Visibility projections and batched reads through CASA reference tables.

use std::collections::HashMap;

use casa_core::ArrayValue;
use casa_tables::{PlainReferenceRead, RequiredScalarColumnValues};
use num_complex::Complex32;

use super::{VisibilityPolarization, divide_visibility, input};

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

impl super::VisibilityRead<'_> {
    pub(super) fn model_product(
        &self,
        data: ArrayValue,
        prefix: &str,
        average: bool,
        convert: Option<&VisibilityPolarization>,
    ) -> MsResult<ArrayValue> {
        let model_values = self.matrix_cells("MODEL_DATA", false)?;
        let (data, model_values) = if let Some(plan) = convert.filter(|_| self.legacy) {
            (
                self.convert_visibility_product(data, plan, false)?,
                self.convert_visibility_product(model_values, plan, false)?,
            )
        } else {
            (data, model_values)
        };
        let (ArrayValue::Complex32(mut observed), ArrayValue::Complex32(expected)) =
            (data, model_values)
        else {
            return Err(input("visibility columns must contain Complex values"));
        };
        if observed.shape() != expected.shape() {
            return Err(input("visibility columns have different shapes"));
        }
        if prefix == "ratio_" {
            observed.zip_mut_with(&expected, |observed, expected| {
                *observed = if self.legacy && *expected == Complex32::ZERO {
                    Complex32::ONE
                } else {
                    divide_visibility(*observed, *expected)
                };
            });
            if !self.legacy
                && !average
                && let Some(channels) = self.channels
                && channels.width() > 1
                && !self.rows.is_empty()
            {
                observed.zip_mut_with(self.channel_flags(channels)?, |observed, &flagged| {
                    if flagged {
                        *observed = Complex32::default();
                    }
                });
            }
        } else {
            observed.zip_mut_with(&expected, |observed, expected| *observed -= *expected);
        }
        Ok(ArrayValue::Complex32(observed))
    }
}
