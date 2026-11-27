// SPDX-License-Identifier: LGPL-3.0-or-later
//! Scaling of stored UVW coordinates by the reference or channel frequencies.

use casa_core::quanta::constants;
use casa_core::{ArrayValue as CellArray, ScalarValue, Value};

use super::eval::{ArrayValue, EvalContext, ExprValue};
use super::mscal_lookup::{open_subtable, table_error};
use super::{BoundRowFunction, TaqlError};
use crate::Table;

pub(super) fn bind(
    expression: String,
    table: &Table,
    per_channel: bool,
) -> Result<BoundRowFunction, TaqlError> {
    let scale = UvwWavelengthScale::read(table, per_channel)?;
    Ok(BoundRowFunction::new(expression, move |context| {
        let Some(Value::Array(CellArray::Float64(uvw))) = context.row.get("UVW") else {
            return Err(TaqlError::Table(
                "MSCAL requires a double UVW column".into(),
            ));
        };
        if uvw.shape() != [3] {
            return Err(TaqlError::Table(
                "MSCAL requires three UVW coordinates".into(),
            ));
        }
        scale.apply(context, [uvw[0], uvw[1], uvw[2]])
    }))
}

/// A MeasurementSet's data-description and frequency lookup for UVW in wavelengths.
pub struct UvwWavelengthScale {
    dd_to_spw: Vec<Option<usize>>,
    scale: WavelengthScale,
}

impl UvwWavelengthScale {
    /// Prepares one factor per window, or one per channel when `per_channel` is set.
    ///
    /// # Errors
    ///
    /// Returns a table error when a required subtable or column cannot be read.
    pub fn read(table: &Table, per_channel: bool) -> Result<Self, TaqlError> {
        let descriptions = open_subtable(table, "DATA_DESCRIPTION")?;
        let windows = open_subtable(table, "SPECTRAL_WINDOW")?;
        let dd_to_spw = descriptions
            .column_accessor("SPECTRAL_WINDOW_ID")
            .and_then(|column| column.scalar_cells_owned())
            .map_err(table_error)?
            .into_iter()
            .map(|id| match id {
                Some(ScalarValue::Int32(id)) => usize::try_from(id).ok(),
                _ => None,
            })
            .collect();
        Ok(Self {
            dd_to_spw,
            scale: WavelengthScale::read(&windows, per_channel)?,
        })
    }

    /// Scales the row's UVW coordinates in metres into wavelengths.
    ///
    /// # Errors
    ///
    /// Returns a table error when the row's data description or frequency is missing.
    pub fn apply(&self, context: &EvalContext<'_>, uvw: [f64; 3]) -> Result<ExprValue, TaqlError> {
        self.scale
            .apply(spectral_window(context, &self.dd_to_spw)?, uvw)
    }
}

fn spectral_window(
    context: &EvalContext<'_>,
    dd_to_spw: &[Option<usize>],
) -> Result<usize, TaqlError> {
    let Some(Value::Scalar(ScalarValue::Int32(id))) = context.row.get("DATA_DESC_ID") else {
        return Err(TaqlError::Table(
            "MSCAL requires an integer DATA_DESC_ID".into(),
        ));
    };
    usize::try_from(*id)
        .ok()
        .and_then(|id| dd_to_spw.get(id).copied().flatten())
        .ok_or_else(|| {
            TaqlError::Table(format!(
                "MSCAL has no spectral window for DATA_DESC_ID {id}"
            ))
        })
}

enum WavelengthScale {
    Reference(Vec<Option<f64>>),
    Channels(Vec<Option<Vec<f64>>>),
}

impl WavelengthScale {
    fn read(windows: &Table, per_channel: bool) -> Result<Self, TaqlError> {
        let c = constants::c().value();
        if per_channel {
            let rows: Vec<_> = (0..windows.row_count()).collect();
            let factors = windows
                .column_accessor("CHAN_FREQ")
                .and_then(|column| column.array_cells_owned(&rows))
                .map_err(table_error)?
                .into_iter()
                .map(|value| match value {
                    Some(CellArray::Float64(freq)) if freq.ndim() == 1 => {
                        Some(freq.iter().map(|hz| hz / c).collect())
                    }
                    _ => None,
                })
                .collect();
            Ok(Self::Channels(factors))
        } else {
            let factors = windows
                .column_accessor("REF_FREQUENCY")
                .and_then(|column| column.scalar_cells_owned())
                .map_err(table_error)?
                .into_iter()
                .map(|value| match value {
                    Some(ScalarValue::Float64(hz)) => Some(hz / c),
                    _ => None,
                })
                .collect();
            Ok(Self::Reference(factors))
        }
    }

    fn apply(&self, spw: usize, uvw: [f64; 3]) -> Result<ExprValue, TaqlError> {
        let missing =
            || TaqlError::Table(format!("MSCAL has no frequency for spectral window {spw}"));
        let values = match self {
            Self::Reference(factors) => {
                let factor = factors.get(spw).copied().flatten().ok_or_else(missing)?;
                ArrayValue {
                    shape: vec![3],
                    data: uvw
                        .into_iter()
                        .map(|value| ExprValue::Float(value * factor))
                        .collect(),
                }
            }
            Self::Channels(factors) => {
                let factors = factors
                    .get(spw)
                    .and_then(Option::as_ref)
                    .ok_or_else(missing)?;
                ArrayValue {
                    shape: vec![3, factors.len()],
                    data: factors
                        .iter()
                        .flat_map(|factor| uvw.map(|value| ExprValue::Float(value * factor)))
                        .collect(),
                }
            }
        };
        Ok(ExprValue::Array(values))
    }
}
