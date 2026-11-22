// SPDX-License-Identifier: LGPL-3.0-or-later
//! VI2-style selected channel buffers and writeback to full array cells.

use super::*;
use crate::plan::ApplyChannelSelection;
use ndarray::Axis;

/// The compact buffer's physical frequencies and positions in the original SPW.
#[derive(Clone, Copy)]
pub(super) struct DataChannelGrid<'a> {
    pub frequencies_hz: &'a [f64],
    indices: Option<&'a [usize]>,
}

impl<'a> DataChannelGrid<'a> {
    pub fn all(frequencies_hz: &'a [f64]) -> Self {
        Self {
            frequencies_hz,
            indices: None,
        }
    }

    pub fn original_index(self, compact_index: usize) -> usize {
        self.indices
            .map_or(compact_index, |indices| indices[compact_index])
    }
}

fn select(
    value: &ArrayValue,
    channels: &[usize],
    column: &str,
) -> Result<ArrayValue, ApplyExecutionError> {
    if value.shape().len() != 2 || channels.iter().any(|&channel| channel >= value.shape()[1]) {
        return Err(ApplyExecutionError::UnsupportedParameterShape {
            path: format!("<measurement-set {column}>"),
            shape: value.shape().to_vec(),
        });
    }
    Ok(match value {
        ArrayValue::Complex32(values) => ArrayValue::Complex32(values.select(Axis(1), channels)),
        ArrayValue::Float32(values) => ArrayValue::Float32(values.select(Axis(1), channels)),
        ArrayValue::Bool(values) => ArrayValue::Bool(values.select(Axis(1), channels)),
        _ => {
            return Err(ApplyExecutionError::UnsupportedParameterShape {
                path: format!("<measurement-set {column}>"),
                shape: value.shape().to_vec(),
            });
        }
    })
}

fn scatter(
    base: &ArrayValue,
    selected: &ArrayValue,
    channels: &[usize],
    column: &str,
) -> Result<ArrayValue, ApplyExecutionError> {
    if base.shape().len() != 2
        || selected.shape() != [base.shape()[0], channels.len()]
        || channels.iter().any(|&channel| channel >= base.shape()[1])
    {
        return Err(ApplyExecutionError::UnsupportedParameterShape {
            path: format!("<measurement-set {column}>"),
            shape: base.shape().to_vec(),
        });
    }
    macro_rules! copy {
        ($base:expr, $selected:expr, $variant:ident) => {{
            let mut output = $base.clone();
            for (source, &target) in channels.iter().enumerate() {
                output
                    .index_axis_mut(Axis(1), target)
                    .assign(&$selected.index_axis(Axis(1), source));
            }
            ArrayValue::$variant(output)
        }};
    }
    Ok(match (base, selected) {
        (ArrayValue::Complex32(base), ArrayValue::Complex32(selected)) => {
            copy!(base, selected, Complex32)
        }
        (ArrayValue::Float32(base), ArrayValue::Float32(selected)) => {
            copy!(base, selected, Float32)
        }
        (ArrayValue::Bool(base), ArrayValue::Bool(selected)) => copy!(base, selected, Bool),
        _ => {
            return Err(ApplyExecutionError::UnsupportedParameterShape {
                path: format!("<measurement-set {column}>"),
                shape: base.shape().to_vec(),
            });
        }
    })
}

struct SelectedInputs {
    data: ArrayValue,
    flags: Option<ArrayValue>,
    weights: Option<ArrayValue>,
}

impl SelectedInputs {
    fn new(
        input: ExecutionRowInputs<'_>,
        selection: &ApplyChannelSelection,
    ) -> Result<Self, ApplyExecutionError> {
        Ok(Self {
            data: select(input.data, &selection.indices, "DATA")?,
            flags: input
                .original_flags
                .map(|value| select(value, &selection.indices, "FLAG"))
                .transpose()?,
            weights: input
                .original_weight_spectrum
                .map(|value| select(value, &selection.indices, "SIGMA_SPECTRUM"))
                .transpose()?,
        })
    }

    fn inputs<'a>(
        &'a self,
        input: ExecutionRowInputs<'a>,
        selection: &'a ApplyChannelSelection,
    ) -> ExecutionRowInputs<'a> {
        ExecutionRowInputs {
            data: &self.data,
            original_flags: self.flags.as_ref(),
            original_weight_spectrum: self.weights.as_ref(),
            channel_grid: Some(DataChannelGrid {
                frequencies_hz: &selection.frequencies_hz,
                indices: Some(&selection.indices),
            }),
            ..input
        }
    }
}

pub(super) fn apply_row(
    row: &ApplyRowPlan,
    input: ExecutionRowInputs<'_>,
    plan: &ApplyPlan,
    chain: &ApplyChain,
    geometry: Option<&mut RowGeometryCache>,
    profile: Option<&mut ApplyRowComputeProfile>,
    activity: Option<&mut ToolApplyActivity>,
) -> Result<ExecutionRowResult, ApplyExecutionError> {
    let Some(selection) = plan.channel_selections.get(&row.data_spw_id) else {
        return apply_row_all_channels(row, input, plan, chain, geometry, profile, activity);
    };
    let selected = SelectedInputs::new(input, selection)?;
    let mut result = apply_row_all_channels(
        row,
        selected.inputs(input, selection),
        plan,
        chain,
        geometry,
        profile,
        activity,
    )?;
    result.corrected_data = scatter(
        input.preserved_corrected_data.unwrap_or(input.data),
        &result.corrected_data,
        &selection.indices,
        "CORRECTED_DATA",
    )?;
    if let Some(flags) = &result.updated_flags {
        let original = input.original_flags.expect("CalFlag requires FLAG");
        let full = scatter(original, flags, &selection.indices, "FLAG")?;
        let ArrayValue::Bool(values) = &full else {
            unreachable!("FLAG type checked above")
        };
        result.row_became_fully_flagged = values.iter().all(|flag| *flag);
        result.updated_flags = Some(full);
    }
    if let Some(weights) = &result.updated_weight_spectrum {
        let expanded;
        let base = if let Some(value) = input
            .preserved_weight_spectrum
            .or(input.original_weight_spectrum)
        {
            value
        } else {
            let Some(ArrayValue::Float32(weight)) = input.original_weight else {
                return Err(ApplyExecutionError::UnsupportedParameterShape {
                    path: "<measurement-set WEIGHT>".into(),
                    shape: vec![],
                });
            };
            expanded =
                ArrayValue::Float32(expand_weight_to_spectrum(weight, input.data.shape()[1]));
            &expanded
        };
        result.updated_weight_spectrum = Some(scatter(
            base,
            weights,
            &selection.indices,
            "WEIGHT_SPECTRUM",
        )?);
    }
    Ok(result)
}
