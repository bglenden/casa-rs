// SPDX-License-Identifier: LGPL-3.0-or-later
//! Direct calibrater configuration for task-owned single-dish solvers.

use std::path::{Path, PathBuf};

use casa_core::quanta::{Quantity, Unit};
use casa_ms::MeasurementSet;

use super::{edges, gain, lock_solve_output, position_switch};
use crate::execute::SolvePriorApply;
use crate::session::resolve_casa_calibration_selection;
use crate::solve_stats::{SolveStats, ToolSolveActivity};
use crate::table_maintenance::overlaps_measurement_set;
use crate::{ApplyPlanRequest, CalibrationOutputMode, GainSolveRequest, plan_apply};

/// Family-specific controls on `calibrater.setsolve` for total-power data.
pub struct SingleDishSolveControls {
    /// Fraction of a map used for the sky reference spectrum.
    pub fraction: f32,
    /// Explicit raster edge integrations, or a negative automatic count.
    pub numedge: i64,
    /// Angular radius of the double-circle gain reference region.
    pub radius: String,
    /// Smooth the gain reference timeline before taking the ratio.
    pub smooth: bool,
    /// The generic amplitude/phase selector could not be resolved.
    pub invalid_apmode: bool,
}

pub(crate) struct Solver {
    pub(crate) request: GainSolveRequest,
    pub(crate) mode: CalibrationOutputMode,
    kind: Kind,
    invalid_apmode: bool,
}

enum Kind {
    PositionSwitch,
    Edges(edges::Selection),
    Gain(gain::Controls),
}

impl Solver {
    pub(crate) fn new(
        family: &str,
        request: GainSolveRequest,
        mode: CalibrationOutputMode,
        controls: SingleDishSolveControls,
        legacy: bool,
    ) -> Result<Self, String> {
        let fraction = f64::from(controls.fraction);
        let kind = match family {
            "SDSKY_PS" => Kind::PositionSwitch,
            "SDSKY_OTF" if legacy => Kind::Edges(edges::Selection::Otf { fraction }),
            "SDSKY_OTF" => {
                return Err("SDSKY_OTF not yet supported via SingleDishOtfCal(msmc)".into());
            }
            "SDSKY_RASTER" => Kind::Edges(edges::Selection::Raster {
                fraction,
                noff: controls.numedge,
            }),
            "SDGAIN_OTFD" => Kind::Gain(gain::Controls {
                radius: parse_radius(&controls.radius)?,
                smooth: controls.smooth,
                solve_mode: request.solve_mode,
                correlation_dependent_flags: false,
            }),
            _ => return Err(format!("Unknown single-dish solve type {family}")),
        };
        Ok(Self {
            request,
            mode,
            kind,
            invalid_apmode: controls.invalid_apmode,
        })
    }

    pub(crate) fn subtype(&self) -> &'static str {
        match self.kind {
            Kind::PositionSwitch => position_switch::SUBTYPE,
            Kind::Edges(edges::Selection::Otf { .. }) => "SDSKY_OTF",
            Kind::Edges(edges::Selection::Raster { .. }) => "SDSKY_RASTER",
            Kind::Gain(_) => "SDGAIN_OTFD",
        }
    }

    pub(crate) fn gain_controls(&self) -> Option<(f64, bool)> {
        match self.kind {
            Kind::Gain(controls) => Some((controls.radius.unwrap_or(0.), controls.smooth)),
            _ => None,
        }
    }

    pub(crate) fn recorded_output(&self) -> Option<&PathBuf> {
        self.gain_controls()
            .is_none()
            .then_some(&self.request.output_table)
    }

    pub(crate) fn solve(
        &self,
        ms_path: &Path,
        apply: ApplyPlanRequest,
        legacy: bool,
        correlation_dependent_flags: bool,
        activity: &mut ToolSolveActivity,
    ) -> Result<Option<SolveStats>, String> {
        if legacy == matches!(self.kind, Kind::Gain(_)) {
            return Err(if legacy {
                "Attempt to call un-implemented selfSolveOne()".into()
            } else {
                format!("Can't do selfGatherAndSolve for {}", self.subtype())
            });
        }
        let ms = MeasurementSet::open(ms_path).map_err(|error| error.to_string())?;
        let output = &self.request.output_table;
        if overlaps_measurement_set(&ms, output)? {
            return Err("The MeasurementSet and output calibration table overlap.".into());
        }
        let _output_locks =
            lock_solve_output(output, self.mode).map_err(|error| error.to_string())?;
        let selection = resolve_casa_calibration_selection(&ms, &apply.selection)
            .map_err(|error| error.to_string())?;
        if let Kind::Gain(mut controls) = self.kind {
            if self.invalid_apmode {
                return Err("enforceAPonSoln divide-by-zero error.".into());
            }
            controls.correlation_dependent_flags = correlation_dependent_flags;
            if self.mode == CalibrationOutputMode::Append && !output.join("table.dat").is_file() {
                return Err(format!(
                    "Table {}/SPECTRAL_WINDOW does not exist",
                    output.display()
                ));
            }
            let priors = if apply.calibration_tables.is_empty() && !apply.parang {
                None
            } else {
                let plan = plan_apply(&ms, &apply).map_err(|error| error.to_string())?;
                Some(SolvePriorApply::single_dish(&ms, plan).map_err(|error| error.to_string())?)
            };
            let (_, stats) = gain::generate(
                &ms,
                &selection,
                priors.as_ref(),
                output,
                self.mode,
                controls,
                Some(activity),
            )
            .map_err(|error| error.into_message())?;
            return Ok(stats);
        }
        let rows = selection.row_indices().collect::<Vec<_>>();
        match self.kind {
            Kind::PositionSwitch => position_switch::generate(&ms, &rows, output, self.mode),
            Kind::Edges(controls) => edges::generate(&ms, &rows, output, self.mode, controls),
            Kind::Gain(_) => unreachable!("double-circle gain uses its own selection"),
        }?;
        Ok(None)
    }
}

fn parse_radius(text: &str) -> Result<Option<f64>, String> {
    if text.trim().is_empty() {
        return Ok(None);
    }
    let radians = "rad".parse::<Unit>().expect("standard angle unit");
    let quantity = text
        .parse::<Quantity>()
        .map_err(|error| error.to_string())?;
    let radius = quantity
        .get_value_in(&radians)
        .map_err(|error| error.to_string())?;
    if !radius.is_finite() || radius < 0. {
        return Err("Invalid central disk radius".into());
    }
    Ok(Some(radius))
}
