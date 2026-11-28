// SPDX-License-Identifier: LGPL-3.0-or-later
//! Place prior calibration on the correct side of a solve.

use std::cell::RefCell;

use ndarray::{ArrayD, IxDyn};

use super::preview::{PreviewControl, for_each_apply_row_controlled};
use crate::solve::control::SolveControl;

use super::chain::{ApplyChain, has_polarization_matrix, viscal_order};
use super::{
    ApplyExecutionError, ApplyPlan, ArrayValue, Complex32, EvaluatedApplyRow, ExecutionRowInputs,
    HashMap, MeasurementSet, PreviewWeightSource, RowGeometryCache, applies_preview_weights,
    apply_row, complex_visibility_from_float, evaluate_apply_rows, shared_channel_flags,
};

#[derive(Clone, Copy)]
pub(crate) enum PriorSolveBoundary {
    Accor,
    Baseline,
    Bandpass,
    Delay,
    Gain,
    GainSpline,
    ScalarGain,
    Leakage { parang_terms: usize },
    CrossHand { parang_terms: usize },
    PositionAngle { parang_terms: usize },
}

impl PriorSolveBoundary {
    fn viscal_order(self) -> u8 {
        viscal_order(match self {
            Self::Baseline => "M Mueller",
            Self::Bandpass => "B Jones",
            Self::Delay => "K Jones",
            Self::Accor | Self::Gain | Self::GainSpline => "G Jones",
            Self::ScalarGain => "T Jones",
            Self::Leakage { .. } => "D Jones",
            Self::CrossHand { .. } => "X Jones",
            Self::PositionAngle { .. } => "PosAng Jones",
        })
    }
}

pub(crate) struct PreappliedRows {
    pub(crate) data: HashMap<usize, EvaluatedApplyRow>,
    pub(crate) data_prior: Option<SolvePriorApply>,
    pub(crate) model: Option<SolvePriorApply>,
    pub(crate) polarized: bool,
}

impl From<HashMap<usize, EvaluatedApplyRow>> for PreappliedRows {
    fn from(data: HashMap<usize, EvaluatedApplyRow>) -> Self {
        Self {
            data,
            data_prior: None,
            model: None,
            polarized: false,
        }
    }
}

/// Apply priors as the solve reads each selected row.
pub struct SolvePriorApply {
    plan: ApplyPlan,
    chain: ApplyChain,
    rows: HashMap<usize, usize>,
    geometry: RefCell<RowGeometryCache>,
    forward: bool,
    calwt: bool,
    share_input_flags: bool,
}

impl SolvePriorApply {
    fn new(
        ms: &MeasurementSet,
        plan: ApplyPlan,
        forward: bool,
        share_input_flags: bool,
        parang_terms: usize,
    ) -> Result<Self, ApplyExecutionError> {
        let rows = plan
            .selected_rows
            .iter()
            .enumerate()
            .map(|(position, row)| (row.row_index, position))
            .collect();
        let mut chain = ApplyChain::load(ms, &plan, forward)?;
        chain.repeat_parang(parang_terms);
        let calwt = !forward && applies_preview_weights(&plan, &chain);
        Ok(Self {
            plan,
            chain,
            rows,
            geometry: RefCell::default(),
            forward,
            calwt,
            share_input_flags,
        })
    }

    /// Prepares prior calibration for single-dish data using the input MeasurementSet's antenna and
    /// spectral metadata.
    pub fn single_dish(ms: &MeasurementSet, plan: ApplyPlan) -> Result<Self, ApplyExecutionError> {
        let mut priors = Self::new(ms, plan, false, false, 1)?;
        priors.chain.single_dish_solve = true;
        Ok(priors)
    }

    pub(crate) fn correct_single_dish(
        &self,
        row: usize,
        correlations: &[i32],
        data: &ArrayValue,
        flags: ArrayValue,
    ) -> Result<Option<EvaluatedApplyRow>, ApplyExecutionError> {
        let Some(&position) = self.rows.get(&row) else {
            return Ok(None);
        };
        let converted;
        let data = if let ArrayValue::Float32(samples) = data {
            converted = complex_visibility_from_float(samples);
            &converted
        } else {
            data
        };
        self.apply(position, correlations, data, flags).map(Some)
    }

    pub(crate) fn correct(
        &self,
        row: usize,
        correlations: &[i32],
        data: &ArrayValue,
        flags: &ArrayValue,
    ) -> Result<Option<EvaluatedApplyRow>, ApplyExecutionError> {
        let Some(&position) = self.rows.get(&row) else {
            return Ok(None);
        };
        let flags = if self.share_input_flags {
            shared_channel_flags(flags).unwrap_or_else(|| flags.clone())
        } else {
            flags.clone()
        };
        self.apply(position, correlations, data, flags).map(Some)
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
        let applied = self.apply(
            position,
            correlations,
            &ArrayValue::Complex32(model),
            ArrayValue::Bool(flags.clone()),
        )?;
        let ArrayValue::Complex32(model) = applied.corrected_data else {
            unreachable!("forward application returns complex visibility data")
        };
        let ArrayValue::Bool(flags) = applied.flags else {
            unreachable!("apply flags are boolean")
        };
        Ok((model, Some(flags)))
    }

    fn apply(
        &self,
        position: usize,
        correlations: &[i32],
        data: &ArrayValue,
        flags: ArrayValue,
    ) -> Result<EvaluatedApplyRow, ApplyExecutionError> {
        let row = &self.plan.selected_rows[position];
        if !super::availability::available(row, &self.plan) {
            let shape = IxDyn(data.shape());
            return Ok(EvaluatedApplyRow {
                corrected_data: data.clone(),
                flags: ArrayValue::Bool(ArrayD::from_elem(shape.clone(), true)),
                model_gains: None,
                weight_factors: (!self.forward).then(|| ArrayD::zeros(shape)),
                plot_weights: None,
            });
        }
        let unit_weights = self
            .calwt
            .then(|| ArrayValue::Float32(ArrayD::from_elem(IxDyn(&[correlations.len()]), 1.0)));
        let result = apply_row(
            row,
            ExecutionRowInputs {
                correlation_types: correlations,
                data,
                original_flags: Some(&flags),
                original_weight: unit_weights.as_ref(),
                original_weight_spectrum: None,
                has_weight_spectrum: self.calwt,
                preserved_corrected_data: None,
                preserved_weight_spectrum: None,
                channel_grid: None,
                forward_model: self.forward,
                skip_unavailable_terms: false,
                trial: false,
                parang_calwt: false,
            },
            &self.plan,
            &self.chain,
            Some(&mut self.geometry.borrow_mut()),
            None,
            None,
        )?;
        Ok(EvaluatedApplyRow {
            corrected_data: result.corrected_data,
            flags: result.updated_flags.unwrap_or(flags),
            model_gains: None,
            weight_factors: match result.updated_weight_spectrum {
                Some(ArrayValue::Float32(factors)) => Some(factors),
                _ => None,
            },
            plot_weights: None,
        })
    }
}

pub(crate) fn evaluate_solve_priors(
    ms: &MeasurementSet,
    mut plan: ApplyPlan,
    boundary: PriorSolveBoundary,
    share_input_flags: bool,
    control: SolveControl<'_>,
) -> Result<PreappliedRows, ApplyExecutionError> {
    let deferred_data = matches!(
        boundary,
        PriorSolveBoundary::Leakage { .. }
            | PriorSolveBoundary::CrossHand { .. }
            | PriorSolveBoundary::PositionAngle { .. }
    );
    let parang_terms = match boundary {
        PriorSolveBoundary::Leakage { parang_terms }
        | PriorSolveBoundary::CrossHand { parang_terms }
        | PriorSolveBoundary::PositionAngle { parang_terms } => parang_terms,
        _ => usize::from(plan.parang),
    };
    let polarized = deferred_data || has_polarization_matrix(&plan);
    let baseline = plan
        .calibration_tables
        .iter()
        .any(|table| super::baseline::is_subtype(&table.summary.table_subtype));
    let spectral_opacity = plan
        .calibration_tables
        .iter()
        .any(|table| table.summary.table_subtype == "TfOpac");
    let gain_boundary = matches!(
        boundary,
        PriorSolveBoundary::Gain | PriorSolveBoundary::ScalarGain
    );
    let solve_order = boundary.viscal_order();
    // VisEquation::collapse2 corrupts MODEL_DATA with any prior at or after
    // the solved term. Its weights then come from division by that model,
    // regardless of those priors' calwt settings.
    let gain_model_prior = gain_boundary
        && plan
            .calibration_tables
            .iter()
            .any(|table| viscal_order(&table.summary.table_subtype) >= solve_order);
    let type_ordered = polarized
        || baseline
        || spectral_opacity
        || gain_model_prior
        || matches!(
            boundary,
            PriorSolveBoundary::Accor
                | PriorSolveBoundary::Baseline
                | PriorSolveBoundary::GainSpline
        );
    if !type_ordered && gain_boundary {
        return evaluate_apply_rows(ms, &plan, share_input_flags, control).map(Into::into);
    }
    // The measurement equation corrects DATA with earlier terms and corrupts
    // MODEL_DATA with the remaining terms. The channel-dependent opacity
    // then enters the solve weights through the model normalization.
    let (data_tables, mut model_tables): (Vec<_>, Vec<_>) =
        std::mem::take(&mut plan.calibration_tables)
            .into_iter()
            .partition(|table| {
                let subtype = table.summary.table_subtype.as_str();
                if type_ordered {
                    return viscal_order(subtype) < solve_order;
                }
                match subtype {
                    "KAntPos Jones" => true,
                    "B Jones" | "BPOLY" => matches!(boundary, PriorSolveBoundary::Delay),
                    _ => false,
                }
            });
    let forward_parang = type_ordered && plan.parang && viscal_order("P Jones") >= solve_order;
    plan.parang &= !forward_parang;
    plan.calibration_tables = data_tables;
    let data_prior = (deferred_data && (!plan.calibration_tables.is_empty() || plan.parang))
        .then(|| SolvePriorApply::new(ms, plan.clone(), false, share_input_flags, parang_terms))
        .transpose()?;
    let mut rows = if deferred_data {
        HashMap::new()
    } else {
        evaluate_apply_rows(ms, &plan, share_input_flags, control)?
    };
    if model_tables.is_empty() && !forward_parang {
        return Ok(PreappliedRows {
            data: rows,
            data_prior,
            model: None,
            polarized,
        });
    }
    for table in &mut model_tables {
        table.calwt = false;
    }
    plan.calibration_tables = model_tables;
    plan.parang = forward_parang;
    // Additive offsets must operate on the actual model, just as matrix terms
    // must. Applying them to unit visibilities cannot represent a gain factor.
    if polarized || forward_parang || super::additive::in_plan(&plan) {
        return Ok(PreappliedRows {
            data: rows,
            data_prior,
            model: Some(SolvePriorApply::new(
                ms,
                plan,
                true,
                share_input_flags && !gain_boundary,
                parang_terms,
            )?),
            polarized,
        });
    }
    for_each_apply_row_controlled(
        ms,
        &plan,
        "DATA",
        true,
        share_input_flags,
        PreviewWeightSource::UnitFactors,
        (None, PreviewControl::Solve(control)),
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
