// SPDX-License-Identifier: LGPL-3.0-or-later
//! Correlation selection and Stokes projection on the MS tool's leading axis.

use std::collections::HashMap;

use casa_coordinates::StokesType;
use casa_types::ArrayValue;
use ndarray::ArrayD;

use super::{input, shaped};
use crate::selection::ToolCorrelationSelection;
use crate::{MeasurementSet, MsResult};

mod conversion;
use conversion::Conversion;

/// In-row selection of recorded correlation products or derived Stokes values.
pub struct VisibilityPolarization {
    project: Projection,
}

enum Projection {
    Stored(HashMap<i32, ToolCorrelationSelection>),
    Converted {
        types: Vec<StokesType>,
        plans: HashMap<i32, Conversion>,
    },
}

impl VisibilityPolarization {
    /// Use the recorded products selected by the CASA MS selection grammar.
    pub fn stored(matches: Vec<ToolCorrelationSelection>) -> Self {
        Self {
            project: Projection::Stored(
                matches
                    .into_iter()
                    .map(|plan| (plan.description_id, plan))
                    .collect(),
            ),
        }
    }

    /// Convert the selected rows to the requested Stokes or correlation types.
    /// # Errors
    /// The DATA_DESCRIPTION or POLARIZATION rows cannot be read.
    pub fn converted(ms: &MeasurementSet, types: Vec<StokesType>) -> MsResult<Self> {
        let descriptions = ms.data_description()?;
        let polarization = ms.polarization()?;
        let mut plans = HashMap::with_capacity(descriptions.row_count());
        for id in 0..descriptions.row_count() {
            if descriptions.flag_row(id)? {
                continue;
            }
            let pol_id = usize::try_from(descriptions.polarization_id(id)?)
                .map_err(|_| input("negative POLARIZATION_ID"))?;
            let codes = polarization.corr_type(pol_id)?;
            plans.insert(id as i32, Conversion::new(&codes, &types));
        }
        Ok(Self {
            project: Projection::Converted { types, plans },
        })
    }

    /// Whether complex visibilities are converted to derived polarization values.
    pub fn is_conversion(&self) -> bool {
        matches!(self.project, Projection::Converted { .. })
    }

    pub(super) fn types(&self, id: i32) -> MsResult<&[StokesType]> {
        match &self.project {
            Projection::Stored(plans) => plans
                .get(&id)
                .map(|plan| plan.types.as_slice())
                .ok_or_else(|| missing_description(id)),
            Projection::Converted { types, .. } => Ok(types),
        }
    }

    pub(super) fn gather(&self, values: ArrayValue, ids: &[i32]) -> MsResult<ArrayValue> {
        let Projection::Stored(plans) = &self.project else {
            return Ok(values);
        };
        macro_rules! gather {
            ($($kind:ident),+) => {
                match values {
                    $(ArrayValue::$kind(values) => gather_correlations(&values, ids, plans)
                        .map(ArrayValue::$kind),)+
                    _ => Err(input("invalid correlation array element type")),
                }
            };
        }
        gather!(Bool, Float32, Complex32, Complex64)
    }

    pub(super) fn apply_conversion(&self, values: ArrayValue, ids: &[i32]) -> MsResult<ArrayValue> {
        let Projection::Converted { types, plans } = &self.project else {
            return Ok(values);
        };
        let ArrayValue::Complex32(values) = values else {
            return Err(input(
                "polarization conversion requires Complex visibility data",
            ));
        };
        project_rows(&values, ids, types.len(), plans, |plan, samples, output| {
            plan.append(samples, output)
        })
        .map(ArrayValue::Complex32)
    }

    pub(super) fn averaging_inputs(
        &self,
        flags: ArrayValue,
        weights: ArrayValue,
        ids: &[i32],
    ) -> MsResult<(ArrayValue, ArrayValue)> {
        let Projection::Converted { types, plans } = &self.project else {
            return Ok((flags, weights));
        };
        let (ArrayValue::Bool(flags), ArrayValue::Float32(weights)) = (flags, weights) else {
            return Err(input(
                "polarization averaging requires Bool FLAG and Float WEIGHT",
            ));
        };
        let width = types.len();
        let [_, channels, _] = flags.shape() else {
            return Err(input("polarization averaging requires a FLAG cube"));
        };
        let row_size = width * channels;
        if row_size == 0 {
            return Err(input(
                "polarization averaging requires nonempty correlation and channel axes",
            ));
        }
        let mut flags = project_rows(&flags, ids, width, plans, Conversion::append_flags)?;
        let weights = project_rows(&weights, ids, width, plans, Conversion::append_weights)?;
        let flags_mem = flags
            .as_slice_memory_order_mut()
            .expect("projected flags are contiguous");
        let weights_mem = weights
            .as_slice_memory_order()
            .expect("projected weights are contiguous");
        for (row_flags, row_weights) in flags_mem
            .chunks_exact_mut(row_size)
            .zip(weights_mem.chunks_exact(width))
        {
            for cell in row_flags.chunks_exact_mut(width) {
                for (flag, &weight) in cell.iter_mut().zip(row_weights) {
                    *flag |= weight <= 0.0;
                }
            }
        }
        Ok((ArrayValue::Bool(flags), ArrayValue::Float32(weights)))
    }
}

fn gather_correlations<T: Copy>(
    values: &ArrayD<T>,
    ids: &[i32],
    plans: &HashMap<i32, ToolCorrelationSelection>,
) -> MsResult<ArrayD<T>> {
    let id = ids
        .first()
        .ok_or_else(|| input("no selected polarization rows"))?;
    let width = plans
        .get(id)
        .ok_or_else(|| missing_description(*id))?
        .indices
        .len();
    project_rows(values, ids, width, plans, |plan, samples, output| {
        if plan.indices.len() != width {
            return Err(input(
                "selected data descriptions have different correlation counts",
            ));
        }
        for &index in &plan.indices {
            output.push(
                *samples
                    .get(index)
                    .ok_or_else(|| input("selected correlation is outside the visibility axis"))?,
            );
        }
        Ok(())
    })
}

fn project_rows<T: Copy, P>(
    values: &ArrayD<T>,
    ids: &[i32],
    width: usize,
    plans: &HashMap<i32, P>,
    mut project: impl FnMut(&P, &[T], &mut Vec<T>) -> MsResult<()>,
) -> MsResult<ArrayD<T>> {
    let mut shape = values.shape().to_vec();
    let input_width = shape.first().copied().unwrap_or(0);
    if shape.len() < 2 || input_width == 0 || shape.last().copied() != Some(ids.len()) {
        return Err(input(
            "visibility array has invalid correlation or row dimensions",
        ));
    }
    let memory = values
        .as_slice_memory_order()
        .ok_or_else(|| input("visibility correlation array is not contiguous"))?;
    let cells_per_row = shape[1..shape.len() - 1].iter().product::<usize>();
    shape[0] = width;
    let mut output = Vec::with_capacity(super::baseline_axis::element_count(&shape)?);
    if cells_per_row == 0 {
        return shaped(&shape, output);
    }
    for (&id, row) in ids
        .iter()
        .zip(memory.chunks_exact(input_width * cells_per_row))
    {
        let plan = plans.get(&id).ok_or_else(|| missing_description(id))?;
        for samples in row.chunks_exact(input_width) {
            project(plan, samples, &mut output)?;
        }
    }
    shaped(&shape, output)
}

fn missing_description(id: i32) -> crate::MsError {
    input(format!(
        "no polarization selection for data description {id}"
    ))
}
