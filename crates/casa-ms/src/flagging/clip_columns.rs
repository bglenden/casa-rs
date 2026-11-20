// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read the visibility or weight samples used by the clipping agent.

use std::borrow::Cow;
use std::collections::BTreeMap;

use casa_tables::Table;
use casa_types::{ArrayValue, Complex32, Complex64, PrimitiveType};
use ndarray::{ArrayD, IxDyn};

use super::{FlagDataColumn, FlaggingError, checked_row_data, load_i32_cells};
use crate::MeasurementSet;
use crate::scratch_columns::virtual_model_fields;

pub(super) fn require_physical_model(
    ms: &MeasurementSet,
    column: FlagDataColumn,
    selected_rows: &[usize],
    path: &str,
) -> Result<(), FlaggingError> {
    if !matches!(
        column,
        FlagDataColumn::ModelData | FlagDataColumn::Residual | FlagDataColumn::ResidualData
    ) {
        return Ok(());
    }
    let virtual_fields = virtual_model_fields(ms);
    if !virtual_fields.is_empty() {
        for rows in selected_rows.chunks(65_536) {
            if let Some(field) = load_i32_cells(ms.main_table(), rows, "FIELD_ID", path)?
                .into_iter()
                .find(|field| virtual_fields.contains(field))
            {
                return Err(FlaggingError::MutateFlags {
                    path: path.to_string(),
                    reason: format!(
                        "clipping a virtual model for FIELD_ID {field} is unimplemented (#1617)"
                    ),
                });
            }
        }
    }
    Ok(())
}

pub(super) struct ClipColumns {
    primary: &'static str,
    subtract: Option<&'static str>,
    broadcast_weights: bool,
    fallback_weights: BTreeMap<i32, bool>,
}

impl ClipColumns {
    pub(super) fn bind(
        table: &Table,
        column: FlagDataColumn,
    ) -> Result<Option<Self>, FlaggingError> {
        let has_column = |name| {
            table
                .schema()
                .is_some_and(|schema| schema.contains_column(name))
        };
        let spectrum = FlagDataColumn::WeightSpectrum.name();
        let model = FlagDataColumn::ModelData.name();
        let observed = FlagDataColumn::Data.name();
        let float_data = FlagDataColumn::FloatData.name();
        let (primary, subtract, broadcast_weights) = match column {
            FlagDataColumn::Residual => (FlagDataColumn::CorrectedData.name(), Some(model), false),
            FlagDataColumn::ResidualData if !has_column(observed) && has_column(float_data) => {
                (float_data, Some(model), false)
            }
            FlagDataColumn::ResidualData => (observed, Some(model), false),
            FlagDataColumn::Weight if has_column(spectrum) => (spectrum, None, false),
            FlagDataColumn::Weight => (column.name(), None, true),
            _ => (column.name(), None, false),
        };
        if subtract.is_some_and(|name| !has_column(name)) {
            return Err(FlaggingError::MissingClipColumn);
        }
        if !has_column(primary) {
            // CASA admits RESIDUAL when a model exists, then skips the
            // agent if corrected data is not available.
            return if column == FlagDataColumn::Residual {
                Ok(None)
            } else {
                Err(FlaggingError::MissingClipColumn)
            };
        }
        Ok(Some(Self {
            primary,
            subtract,
            broadcast_weights,
            fallback_weights: BTreeMap::new(),
        }))
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
            Ok(primary + 2 * size_of(name)?)
        } else if self.uses_weight_spectrum() {
            Ok(primary + size_of("WEIGHT")?)
        } else {
            Ok(primary)
        }
    }

    pub(super) fn uses_weight_spectrum(&self) -> bool {
        self.primary == FlagDataColumn::WeightSpectrum.name()
    }

    pub(super) fn read<'a>(
        &'a mut self,
        table: &Table,
        rows: &[usize],
        ddids: Option<&[i32]>,
        ddid_to_spw: &BTreeMap<i32, i32>,
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
        let mut scalar_weights = None;
        if self.uses_weight_spectrum() {
            let ddids = ddids.expect("weight spectrum reads project DATA_DESC_ID");
            // CASA decides whether scalar fallback is available in each
            // spectral window. Probe within the admitted read block.
            let mut needs_scalar = Vec::with_capacity(rows.len());
            for (&ddid, cell) in ddids.iter().zip(&primary) {
                let spw = ddid_to_spw
                    .get(&ddid)
                    .ok_or_else(|| FlaggingError::MutateFlags {
                        path: path.to_string(),
                        reason: format!("DATA_DESC_ID {ddid} has no SPW mapping"),
                    })?;
                let fallback = *self.fallback_weights.entry(*spw).or_insert(cell.is_none());
                needs_scalar.push(fallback && cell.is_none());
            }
            if needs_scalar.iter().any(|needs| *needs) {
                scalar_weights = Some(
                    read("WEIGHT")?
                        .into_iter()
                        .zip(needs_scalar)
                        .map(|(weights, needs_scalar)| if needs_scalar { weights } else { None })
                        .collect(),
                );
            }
        }
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
        if let Some(weights) = self
            .scalar_weights
            .as_ref()
            .and_then(|cells| cells.get(slot))
            .and_then(Option::as_ref)
        {
            return broadcast_weights(Some(weights), flags.shape(), row, path).map(Cow::Owned);
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
            (ArrayValue::Float32(data), ArrayValue::Complex32(model)) => ArrayValue::Complex32(
                ndarray::Zip::from(data)
                    .and(model)
                    .map_collect(|&data, &model| Complex32::new(data, 0.0) - model),
            ),
            (ArrayValue::Float64(data), ArrayValue::Complex64(model)) => ArrayValue::Complex64(
                ndarray::Zip::from(data)
                    .and(model)
                    .map_collect(|&data, &model| Complex64::new(data, 0.0) - model),
            ),
            _ => {
                return Err(FlaggingError::MutateFlags {
                    path: path.to_string(),
                    reason: format!(
                        "{} and {name} row {row} must hold matching numeric samples and a complex model",
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
