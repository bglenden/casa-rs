// SPDX-License-Identifier: LGPL-3.0-or-later
//! Place prior calibration on the correct side of a solve.

use std::cell::RefCell;

use ndarray::ArrayD;

use super::chain::{ApplyChain, has_leakage, viscal_order};
use super::{
    ApplyExecutionError, ApplyPlan, ArrayValue, Complex32, EvaluatedApplyRow, ExecutionRowInputs,
    HashMap, MeasurementSet, PreviewWeightSource, RowGeometryCache, apply_row, evaluate_apply_rows,
    for_each_apply_row,
};

#[derive(Clone, Copy)]
pub(crate) enum PriorSolveBoundary {
    Bandpass,
    Delay,
    Gain,
    ScalarGain,
}

impl PriorSolveBoundary {
    fn viscal_order(self) -> u8 {
        viscal_order(match self {
            Self::Bandpass => "B Jones",
            Self::Delay => "K Jones",
            Self::Gain => "G Jones",
            Self::ScalarGain => "T Jones",
        })
    }
}

pub(crate) struct PreappliedRows {
    pub(crate) data: HashMap<usize, EvaluatedApplyRow>,
    pub(crate) model: Option<ForwardModelApply>,
    pub(crate) polarized: bool,
}

impl From<HashMap<usize, EvaluatedApplyRow>> for PreappliedRows {
    fn from(data: HashMap<usize, EvaluatedApplyRow>) -> Self {
        Self {
            data,
            model: None,
            polarized: false,
        }
    }
}

/// Deferred non-diagonal application consumes the solver's actual sky model.
pub(crate) struct ForwardModelApply {
    plan: ApplyPlan,
    chain: ApplyChain,
    rows: HashMap<usize, usize>,
    geometry: RefCell<RowGeometryCache>,
}

impl ForwardModelApply {
    fn new(ms: &MeasurementSet, plan: ApplyPlan) -> Result<Self, ApplyExecutionError> {
        let rows = plan
            .selected_rows
            .iter()
            .enumerate()
            .map(|(position, row)| (row.row_index, position))
            .collect();
        let chain = ApplyChain::load(ms, &plan, true)?;
        Ok(Self {
            plan,
            chain,
            rows,
            geometry: RefCell::default(),
        })
    }

    pub(crate) fn corrupt(
        &self,
        row: usize,
        correlations: &[i32],
        model: ArrayD<Complex32>,
        flags: &ArrayD<bool>,
    ) -> Result<(ArrayD<Complex32>, Option<ArrayD<bool>>), ApplyExecutionError> {
        let Some(&position) = self.rows.get(&row) else {
            return Ok((model, None));
        };
        let row = &self.plan.selected_rows[position];
        if !super::availability::available(row, &self.plan) {
            return Ok((model, Some(ArrayD::from_elem(flags.raw_dim(), true))));
        }
        let result = apply_row(
            row,
            ExecutionRowInputs {
                correlation_types: correlations,
                data: &ArrayValue::Complex32(model),
                original_flags: Some(&ArrayValue::Bool(flags.clone())),
                original_weight: None,
                original_weight_spectrum: None,
                has_weight_spectrum: false,
                preserved_corrected_data: None,
                preserved_weight_spectrum: None,
                channel_grid: None,
                forward_model: true,
            },
            &self.plan,
            &self.chain,
            Some(&mut self.geometry.borrow_mut()),
            None,
        )?;
        let ArrayValue::Complex32(model) = result.corrected_data else {
            unreachable!("forward application returns complex visibility data")
        };
        let flags = result.updated_flags.map(|value| match value {
            ArrayValue::Bool(flags) => flags,
            _ => unreachable!("apply flags are boolean"),
        });
        Ok((model, flags))
    }
}

pub(crate) fn evaluate_solve_priors(
    ms: &MeasurementSet,
    mut plan: ApplyPlan,
    boundary: PriorSolveBoundary,
    share_input_flags: bool,
) -> Result<PreappliedRows, ApplyExecutionError> {
    let polarized = has_leakage(&plan);
    if !polarized
        && matches!(
            boundary,
            PriorSolveBoundary::Gain | PriorSolveBoundary::ScalarGain
        )
    {
        return evaluate_apply_rows(ms, &plan, false).map(Into::into);
    }
    // VisEquation::collapse2 corrects DATA with earlier terms and corrupts
    // MODEL_DATA with terms at or after K. This distinction matters even for
    // diagonal terms because CASA leaves data over a zero model undivided.
    let (data_tables, mut model_tables): (Vec<_>, Vec<_>) =
        std::mem::take(&mut plan.calibration_tables)
            .into_iter()
            .partition(|table| {
                let subtype = table.summary.table_subtype.as_str();
                if polarized {
                    return viscal_order(subtype) < boundary.viscal_order();
                }
                match subtype {
                    "KAntPos Jones" => true,
                    "B Jones" | "BPOLY" => matches!(boundary, PriorSolveBoundary::Delay),
                    _ => false,
                }
            });
    let forward_parang =
        polarized && plan.parang && viscal_order("P Jones") >= boundary.viscal_order();
    plan.parang &= !forward_parang;
    plan.calibration_tables = data_tables;
    let mut rows = evaluate_apply_rows(ms, &plan, share_input_flags)?;
    if model_tables.is_empty() && !forward_parang {
        return Ok(PreappliedRows {
            data: rows,
            model: None,
            polarized,
        });
    }
    for table in &mut model_tables {
        table.calwt = false;
    }
    plan.calibration_tables = model_tables;
    plan.parang = forward_parang;
    if polarized {
        return Ok(PreappliedRows {
            data: rows,
            model: Some(ForwardModelApply::new(ms, plan)?),
            polarized,
        });
    }
    for_each_apply_row(
        ms,
        &plan,
        true,
        share_input_flags,
        PreviewWeightSource::UnitFactors,
        |index, row| {
            let data = rows.get_mut(&index).expect("same selection for both sides");
            let ArrayValue::Bool(data_flags) = &mut data.flags else {
                unreachable!("apply flags are boolean")
            };
            let ArrayValue::Bool(model_flags) = row.flags else {
                unreachable!("model calibration flags are boolean")
            };
            data_flags.zip_mut_with(&model_flags, |data, &model| *data |= model);
            let ArrayValue::Complex32(gains) = row.corrected_data else {
                unreachable!("unit visibilities are complex")
            };
            data.model_gains = Some(gains);
        },
    )?;
    Ok(rows.into())
}
