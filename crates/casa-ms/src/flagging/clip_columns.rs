// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read the visibility or weight samples used by the clipping agent.

use std::borrow::Cow;
use std::collections::BTreeMap;

use casa_tables::Table;
use casa_types::{ArrayValue, Complex32, Complex64, PrimitiveType};
use ndarray::{ArrayD, IxDyn};

use super::clip_average::{ChannelBins, ChannelReduction};
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
    primary: ClipColumn,
    subtract: Option<&'static str>,
    average_weights: Option<ClipColumn>,
    average: bool,
    inverse_sigma: bool,
    sum_weights: bool,
}

impl ClipColumns {
    pub(super) fn bind(
        table: &Table,
        column: FlagDataColumn,
        average: bool,
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
        let inverse_sigma = matches!(primary, "DATA" | "FLOAT_DATA");
        let average_weights = if average {
            match primary {
                // Flagdata reads both observed types through CASA's complex
                // visibility adapter, which weights DATA and FLOAT_DATA by sigma.
                "DATA" | "FLOAT_DATA" => Some(if has_column("SIGMA_SPECTRUM") {
                    ClipColumn::spectrum("SIGMA_SPECTRUM", "SIGMA")
                } else {
                    ClipColumn::scalar("SIGMA")
                }),
                "CORRECTED_DATA" => Some(if has_column(spectrum) {
                    ClipColumn::spectrum(spectrum, "WEIGHT")
                } else {
                    ClipColumn::scalar("WEIGHT")
                }),
                _ => None,
            }
        } else {
            None
        };
        Ok(Some(Self {
            primary: if primary == spectrum {
                ClipColumn::spectrum(spectrum, "WEIGHT")
            } else if broadcast_weights {
                ClipColumn::scalar(primary)
            } else {
                ClipColumn::array(primary)
            },
            subtract,
            average_weights,
            average,
            inverse_sigma,
            sum_weights: matches!(
                column,
                FlagDataColumn::Weight | FlagDataColumn::WeightSpectrum
            ),
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
        let primary = size_of(self.primary.name)?;
        let mut bytes = primary;
        // Account for the input columns and room to assemble a residual row.
        if let Some(name) = self.subtract {
            bytes += 2 * size_of(name)?;
            if self.average {
                bytes += size_of(name)?;
            }
        }
        if let Some(scalar) = self.primary.scalar_fallback {
            bytes += size_of(scalar)?;
        }
        if let Some(weights) = &self.average_weights {
            bytes += size_of(weights.name)?;
            bytes += weights
                .scalar_fallback
                .map_or_else(|| size_of(weights.name), size_of)?;
        }
        if self.average {
            // Include an averaged operand and worst case per-row bin indices,
            // flags and the original channel mapping.
            bytes += primary;
            bytes += std::mem::size_of::<usize>() + std::mem::size_of::<bool>();
        }
        Ok(bytes)
    }

    pub(super) fn uses_weight_spectrum(&self) -> bool {
        self.primary.scalar_fallback.is_some()
            || self
                .average_weights
                .as_ref()
                .is_some_and(|weights| weights.scalar_fallback.is_some())
    }

    pub(super) fn prepare_spectrum_fallbacks(
        &mut self,
        table: &Table,
        first_rows: &BTreeMap<i32, usize>,
        path: &str,
    ) -> Result<(), FlaggingError> {
        for operand in std::iter::once(&mut self.primary).chain(self.average_weights.iter_mut()) {
            if operand.scalar_fallback.is_none() {
                continue;
            }
            for (&spw, &row) in first_rows {
                let value = table
                    .column_accessor(operand.name)
                    .and_then(|column| column.array_cells_owned(&[row]))
                    .map_err(|source| FlaggingError::MutateFlags {
                        path: path.to_string(),
                        reason: format!("read first selected {} row {row}: {source}", operand.name),
                    })?;
                operand
                    .fallback_windows
                    .insert(spw, value.first().is_some_and(Option::is_none));
            }
        }
        Ok(())
    }

    pub(super) fn read<'a>(
        &'a mut self,
        table: &Table,
        rows: &[usize],
        ddids: Option<&[i32]>,
        ddid_to_spw: &BTreeMap<i32, i32>,
        path: &str,
    ) -> Result<ClipColumnCells<'a>, FlaggingError> {
        let read = |name: &str| {
            table
                .column_accessor(name)
                .and_then(|column| column.array_cells_owned(rows))
                .map_err(|source| FlaggingError::MutateFlags {
                    path: path.to_string(),
                    reason: format!("read selected {name} rows: {source}"),
                })
        };
        Ok(ClipColumnCells {
            primary: self.primary.read(&read, ddids, ddid_to_spw, path)?,
            average_weights: self
                .average_weights
                .as_mut()
                .map(|weights| weights.read(&read, ddids, ddid_to_spw, path))
                .transpose()?,
            subtract: self.subtract.map(read).transpose()?,
            model_name: self.subtract,
            inverse_sigma: self.inverse_sigma,
            sum_weights: self.sum_weights,
        })
    }
}

struct ClipColumn {
    name: &'static str,
    broadcast: bool,
    scalar_fallback: Option<&'static str>,
    fallback_windows: BTreeMap<i32, bool>,
}

impl ClipColumn {
    fn array(name: &'static str) -> Self {
        Self {
            name,
            broadcast: false,
            scalar_fallback: None,
            fallback_windows: BTreeMap::new(),
        }
    }

    fn scalar(name: &'static str) -> Self {
        Self {
            broadcast: true,
            ..Self::array(name)
        }
    }

    fn spectrum(name: &'static str, scalar: &'static str) -> Self {
        Self {
            scalar_fallback: Some(scalar),
            ..Self::array(name)
        }
    }

    fn read<'a>(
        &'a mut self,
        read: &impl Fn(&str) -> Result<Vec<Option<ArrayValue>>, FlaggingError>,
        ddids: Option<&[i32]>,
        ddid_to_spw: &BTreeMap<i32, i32>,
        path: &str,
    ) -> Result<ClipOperand<'a>, FlaggingError> {
        let cells = read(self.name)?;
        let mut fallback = None;
        if let Some(scalar) = self.scalar_fallback {
            let ddids = ddids.expect("spectrum reads project DATA_DESC_ID");
            // Decide fallback independently in each spectral window, and do
            // so within the admitted block. Retain defined later cells.
            let mut needs_scalar = Vec::with_capacity(cells.len());
            for (&ddid, cell) in ddids.iter().zip(&cells) {
                let spw = ddid_to_spw
                    .get(&ddid)
                    .ok_or_else(|| FlaggingError::MutateFlags {
                        path: path.to_string(),
                        reason: format!("DATA_DESC_ID {ddid} has no SPW mapping"),
                    })?;
                let can_fallback = *self.fallback_windows.entry(*spw).or_insert(cell.is_none());
                needs_scalar.push(can_fallback && cell.is_none());
            }
            if needs_scalar.iter().any(|needs| *needs) {
                fallback = Some(
                    read(scalar)?
                        .into_iter()
                        .zip(needs_scalar)
                        .map(|(weights, needs_scalar)| if needs_scalar { weights } else { None })
                        .collect(),
                );
            }
        }
        Ok(ClipOperand {
            column: self,
            cells,
            fallback,
        })
    }
}

struct ClipOperand<'a> {
    column: &'a ClipColumn,
    cells: Vec<Option<ArrayValue>>,
    fallback: Option<Vec<Option<ArrayValue>>>,
}

impl ClipOperand<'_> {
    fn row(
        &self,
        slot: usize,
        flags: &ArrayD<bool>,
        row: usize,
        path: &str,
    ) -> Result<Cow<'_, ArrayValue>, FlaggingError> {
        let cell = self.cells.get(slot);
        if self.column.broadcast {
            return broadcast_weights(
                cell.and_then(Option::as_ref),
                flags.shape(),
                row,
                self.column.name,
                path,
            )
            .map(Cow::Owned);
        }
        if let Some(weights) = self
            .fallback
            .as_ref()
            .and_then(|cells| cells[slot].as_ref())
        {
            return broadcast_weights(
                Some(weights),
                flags.shape(),
                row,
                self.column
                    .scalar_fallback
                    .expect("available fallback has a source"),
                path,
            )
            .map(Cow::Owned);
        }
        checked_row_data(cell, flags, row, self.column.name, path).map(Cow::Borrowed)
    }
}

pub(super) struct ClipColumnCells<'a> {
    primary: ClipOperand<'a>,
    subtract: Option<Vec<Option<ArrayValue>>>,
    model_name: Option<&'static str>,
    average_weights: Option<ClipOperand<'a>>,
    inverse_sigma: bool,
    sum_weights: bool,
}

impl ClipColumnCells<'_> {
    pub(super) fn measurements(
        &self,
        slot: usize,
        flags: &ArrayD<bool>,
        row: usize,
        path: &str,
    ) -> Result<Cow<'_, ArrayValue>, FlaggingError> {
        self.primary.row(slot, flags, row, path)
    }

    pub(super) fn model(
        &self,
        slot: usize,
        flags: &ArrayD<bool>,
        row: usize,
        path: &str,
    ) -> Result<Option<&ArrayValue>, FlaggingError> {
        self.subtract
            .as_ref()
            .zip(self.model_name)
            .map(|(cells, name)| checked_row_data(cells.get(slot), flags, row, name, path))
            .transpose()
    }

    pub(super) fn weights(
        &self,
        slot: usize,
        flags: &ArrayD<bool>,
        row: usize,
        path: &str,
    ) -> Result<Option<Cow<'_, ArrayValue>>, FlaggingError> {
        let weights = self
            .average_weights
            .as_ref()
            .map(|column| column.row(slot, flags, row, path))
            .transpose()?;
        if weights
            .as_deref()
            .is_some_and(|value| !matches!(value, ArrayValue::Float32(_) | ArrayValue::Float64(_)))
        {
            return Err(FlaggingError::MutateFlags {
                path: path.to_string(),
                reason: format!("averaging statistics on row {row} must be floating-point arrays"),
            });
        }
        Ok(weights)
    }

    pub(super) fn uses_sigma(&self) -> bool {
        self.inverse_sigma
    }

    pub(super) fn row(
        &self,
        slot: usize,
        flags: &ArrayD<bool>,
        bins: Option<&ChannelBins<'_>>,
        row: usize,
        path: &str,
    ) -> Result<Cow<'_, ArrayValue>, FlaggingError> {
        let mut primary = self.measurements(slot, flags, row, path)?;
        if let Some(bins) = bins {
            let weights = self.weights(slot, flags, row, path)?;
            let reduction = match weights.as_deref() {
                Some(spectrum) => ChannelReduction::Weighted {
                    spectrum,
                    inverse_sigma: self.inverse_sigma,
                },
                None if self.sum_weights => ChannelReduction::Sum,
                None => ChannelReduction::Mean,
            };
            primary = Cow::Owned(bins.average(primary.as_ref(), flags, reduction));
        }
        let Some(stored_model) = self.model(slot, flags, row, path)? else {
            return Ok(primary);
        };
        let averaged_model =
            bins.map(|bins| bins.average(stored_model, flags, ChannelReduction::Mean));
        let model = averaged_model.as_ref().unwrap_or(stored_model);
        subtract_model(primary.as_ref(), model, row, path).map(Cow::Owned)
    }
}

pub(super) fn subtract_model(
    data: &ArrayValue,
    model: &ArrayValue,
    row: usize,
    path: &str,
) -> Result<ArrayValue, FlaggingError> {
    let residual = match (data, model) {
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
                    "Residual row {row} must hold matching numeric samples and a complex model"
                ),
            });
        }
    };
    Ok(residual)
}

fn broadcast_weights(
    cell: Option<&ArrayValue>,
    shape: &[usize],
    row: usize,
    column: &str,
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
            reason: format!("{column} row {row} must hold one value per correlation"),
        }),
    }
}
