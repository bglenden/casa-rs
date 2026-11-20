// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read the visibility or weight samples used by the clipping agent.

use std::borrow::Cow;

use casa_tables::Table;
use casa_types::{ArrayValue, PrimitiveType};
use ndarray::{ArrayD, IxDyn};

use super::{FlagDataColumn, FlaggingError, checked_row_data};

pub(super) struct ClipColumns {
    primary: &'static str,
    subtract: Option<&'static str>,
    broadcast_weights: bool,
    fallback_weights: Option<bool>,
}

impl ClipColumns {
    pub(super) fn bind(table: &Table, column: FlagDataColumn) -> Result<Self, FlaggingError> {
        let has_column = |name| {
            table
                .schema()
                .is_some_and(|schema| schema.contains_column(name))
        };
        let spectrum = FlagDataColumn::WeightSpectrum.name();
        let model = FlagDataColumn::ModelData.name();
        let (primary, subtract, broadcast_weights) = match column {
            FlagDataColumn::Residual => (FlagDataColumn::CorrectedData.name(), Some(model), false),
            FlagDataColumn::ResidualData => (FlagDataColumn::Data.name(), Some(model), false),
            FlagDataColumn::Weight if has_column(spectrum) => (spectrum, None, false),
            FlagDataColumn::Weight => (column.name(), None, true),
            _ => (column.name(), None, false),
        };
        if !has_column(primary) || subtract.is_some_and(|name| !has_column(name)) {
            return Err(FlaggingError::MissingClipColumn);
        }
        Ok(Self {
            primary,
            subtract,
            broadcast_weights,
            fallback_weights: None,
        })
    }

    pub(super) fn bytes_per_sample(
        &self,
        table: &Table,
        path: &str,
    ) -> Result<usize, FlaggingError> {
        let size_of = |name| {
            table
                .schema()
                .and_then(|schema| schema.column(name))
                .and_then(|column| column.data_type())
                .and_then(PrimitiveType::fixed_width_bytes)
                .ok_or_else(|| FlaggingError::MutateFlags {
                    path: path.to_string(),
                    reason: format!("{name} does not expose a fixed-width numeric array type"),
                })
        };
        let primary = size_of(self.primary)?;
        // Account for the input columns and room to assemble a residual row.
        if let Some(name) = self.subtract {
            Ok(2 * primary + size_of(name)?)
        } else if self.primary == FlagDataColumn::WeightSpectrum.name() {
            Ok(primary + size_of("WEIGHT")?)
        } else {
            Ok(primary)
        }
    }

    pub(super) fn read<'a>(
        &'a mut self,
        table: &Table,
        rows: &[usize],
        path: &str,
    ) -> Result<ClipColumnCells<'a>, FlaggingError> {
        let read = |name| {
            table
                .column_accessor(name)
                .and_then(|column| column.array_cells_owned(rows))
                .map_err(|source| FlaggingError::MutateFlags {
                    path: path.to_string(),
                    reason: format!("read selected {name} rows: {source}"),
                })
        };
        let primary = read(self.primary)?;
        // Inspect the first selected spectrum within the admitted read block.
        // CASA permits scalar fallback when that first cell is undefined.
        let fallback_weights = self.primary == FlagDataColumn::WeightSpectrum.name()
            && *self
                .fallback_weights
                .get_or_insert_with(|| primary.first().is_some_and(Option::is_none));
        let scalar_weights = if fallback_weights && primary.iter().any(Option::is_none) {
            Some(read("WEIGHT")?)
        } else {
            None
        };
        Ok(ClipColumnCells {
            columns: self,
            primary,
            subtract: self.subtract.map(read).transpose()?,
            scalar_weights,
        })
    }
}

pub(super) struct ClipColumnCells<'a> {
    columns: &'a ClipColumns,
    primary: Vec<Option<ArrayValue>>,
    subtract: Option<Vec<Option<ArrayValue>>>,
    scalar_weights: Option<Vec<Option<ArrayValue>>>,
}

impl ClipColumnCells<'_> {
    pub(super) fn row(
        &self,
        slot: usize,
        flags: &ArrayD<bool>,
        row: usize,
        path: &str,
    ) -> Result<Cow<'_, ArrayValue>, FlaggingError> {
        let primary = self.primary.get(slot);
        if self.columns.broadcast_weights {
            return broadcast_weights(primary.and_then(Option::as_ref), flags.shape(), row, path)
                .map(Cow::Owned);
        }
        if primary.is_none_or(Option::is_none)
            && let Some(weights) = &self.scalar_weights
        {
            return broadcast_weights(
                weights.get(slot).and_then(Option::as_ref),
                flags.shape(),
                row,
                path,
            )
            .map(Cow::Owned);
        }
        let primary = checked_row_data(primary, flags, row, self.columns.primary, path)?;
        let Some((cells, name)) = self.subtract.as_ref().zip(self.columns.subtract) else {
            return Ok(Cow::Borrowed(primary));
        };
        let model = checked_row_data(cells.get(slot), flags, row, name, path)?;
        let residual = match (primary, model) {
            (ArrayValue::Complex32(data), ArrayValue::Complex32(model)) => {
                ArrayValue::Complex32(data - model)
            }
            (ArrayValue::Complex64(data), ArrayValue::Complex64(model)) => {
                ArrayValue::Complex64(data - model)
            }
            _ => {
                return Err(FlaggingError::MutateFlags {
                    path: path.to_string(),
                    reason: format!(
                        "{} and {name} row {row} must hold the same complex type",
                        self.columns.primary
                    ),
                });
            }
        };
        Ok(Cow::Owned(residual))
    }
}

fn broadcast_weights(
    cell: Option<&ArrayValue>,
    shape: &[usize],
    row: usize,
    path: &str,
) -> Result<ArrayValue, FlaggingError> {
    let expected = &shape[..1];
    match cell {
        Some(ArrayValue::Float32(weights)) if weights.shape() == expected => Ok(
            ArrayValue::Float32(ArrayD::from_shape_fn(IxDyn(shape), |index| {
                weights[index[0]]
            })),
        ),
        Some(ArrayValue::Float64(weights)) if weights.shape() == expected => Ok(
            ArrayValue::Float64(ArrayD::from_shape_fn(IxDyn(shape), |index| {
                weights[index[0]]
            })),
        ),
        _ => Err(FlaggingError::MutateFlags {
            path: path.to_string(),
            reason: format!("WEIGHT row {row} must hold one weight per correlation"),
        }),
    }
}
