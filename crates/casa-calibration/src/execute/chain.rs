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
    pub(super) additive: Option<additive::ToolState>,
    pub(super) legacy_corruption: bool,
}

impl ApplyChain {
    pub(super) fn load(
        ms: &MeasurementSet,
        plan: &ApplyPlan,
        forward_model: bool,
    ) -> Result<Self, ApplyExecutionError> {
        let mut has_sky = false;
        for table in plan
            .calibration_tables
            .iter()
            .filter(|table| crate::single_dish::is_sky_subtype(&table.summary.table_subtype))
        {
            has_sky = true;
            if plan.selected_rows.iter().any(|row| {
                table.applies_to(row) && (row.antenna1 != row.antenna2 || row.feed1 != row.feed2)
            }) {
                return Err(ApplyExecutionError::UnsupportedCalibrationTable {
                    path: table.spec.path.display().to_string(),
                    reason: "single-dish sky calibration requires same-feed autocorrelations"
                        .into(),
                });
            }
        }
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
        if has_sky
            || tables.iter().any(|table| table.baseline)
            || has_polarization_matrix(plan)
            || (forward_model && plan.parang)
        {
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
            additive: None,
            legacy_corruption: false,
        })
    }

    /// The stateful tool sorts every configured term and allows repeated
    /// registration of the table-free parallactic-angle term.
    pub(super) fn arrange_for_tool(
        &mut self,
        plan: &ApplyPlan,
        parang_terms: usize,
        additive: &additive::ToolState,
    ) {
        if additive::in_plan(plan) {
            self.additive = Some(additive.clone());
        }
        self.repeat_parang(parang_terms);
        self.steps.sort_by_key(|step| match step {
            ApplyStep::Table(index) => {
                viscal_order(&plan.calibration_tables[*index].summary.table_subtype)
            }
            ApplyStep::ParallacticAngle => viscal_order("P Jones"),
        });
    }

    pub(super) fn repeat_parang(&mut self, count: usize) {
        if count > 1
            && let Some(index) = self
                .steps
                .iter()
                .position(|step| matches!(step, ApplyStep::ParallacticAngle))
        {
            self.steps.splice(
                index..index,
                std::iter::repeat_n(ApplyStep::ParallacticAngle, count - 1),
            );
        }
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
pub(crate) fn viscal_order(subtype: &str) -> u8 {
    match subtype {
        "M Mueller" | "Mf Mueller" | "SDSKY_PS" | "SDSKY_OTF" | "SDSKY_RASTER" => 2,
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
        additive::SUBTYPE => 15,
        _ => 6,
    }
}
