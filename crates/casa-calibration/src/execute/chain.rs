// SPDX-License-Identifier: LGPL-3.0-or-later
//! Resolve calibration tables and the order of noncommuting corrections.

use super::*;

#[derive(Clone, Copy)]
pub(super) enum ApplyStep {
    Table(usize),
    ParallacticAngle,
}

pub(super) struct ApplyChain {
    pub(super) tables: Vec<LoadedCalibrationTable>,
    pub(super) steps: Vec<ApplyStep>,
    pub(super) parang: Option<ParallacticAngleState>,
}

impl ApplyChain {
    pub(super) fn load(
        ms: &MeasurementSet,
        plan: &ApplyPlan,
        forward_model: bool,
    ) -> Result<Self, ApplyExecutionError> {
        let engine = geometry_engine_for_plan(ms, plan)?;
        let tables = plan
            .calibration_tables
            .iter()
            .map(|table| fields::load(table, engine.as_ref(), ms))
            .collect::<Result<Vec<_>, _>>()?;
        let parang = plan
            .parang
            .then(|| load_parallactic_angle_state(ms))
            .transpose()
            .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                path: display_ms_path(ms),
                source,
            })?;
        let mut steps = (0..tables.len())
            .map(ApplyStep::Table)
            .chain(parang.as_ref().map(|_| ApplyStep::ParallacticAngle))
            .collect::<Vec<_>>();
        if has_polarization_matrix(plan) || (forward_model && plan.parang) {
            steps.sort_by_key(|step| match step {
                ApplyStep::Table(index) => {
                    viscal_order(&plan.calibration_tables[*index].summary.table_subtype)
                }
                ApplyStep::ParallacticAngle => viscal_order("P Jones"),
            });
            if forward_model {
                steps.reverse();
            }
        }
        Ok(Self {
            tables,
            steps,
            parang,
        })
    }
}

pub(crate) fn has_polarization_matrix(plan: &ApplyPlan) -> bool {
    plan.calibration_tables.iter().any(|table| {
        let subtype = table.summary.table_subtype.as_str();
        matches!(subtype, "PosAng Jones" | "F Jones")
            || polarization::Leakage::from_subtype(subtype).is_some()
    })
}

/// The supported CASA VisCal::Type order. Equal types keep supplied apply order.
pub(super) fn viscal_order(subtype: &str) -> u8 {
    match subtype {
        "M Mueller" | "Mf Mueller" => 2,
        "KAntPos Jones" => 3,
        "B Jones" | "B TSYS" | "BPOLY" => 4,
        "K Jones" | "Kcross Jones" | "K WVR" | "Fringe Jones" => 5,
        "J Jones" | "Jf Jones" => 7,
        "D Jones" | "Df Jones" | "Dgen Jones" | "Dfgen Jones" | "DLLS Jones" | "DfLLS Jones" => 8,
        "X Jones" | "Xf Jones" | "Xparang Jones" | "Xfparang Jones" | "X Mueller"
        | "GlinXph Jones" | "GlinXphf Jones" => 9,
        "PosAng Jones" => 10,
        "P Jones" => 11,
        "EGainCurve" | "EPowerCurve" => 12,
        "T Jones" | "Tf Jones" | "TOpac" | "TfOpac" => 13,
        "F Jones" => 14,
        _ => 6,
    }
}
