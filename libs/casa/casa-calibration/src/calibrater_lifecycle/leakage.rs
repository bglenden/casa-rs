// SPDX-License-Identifier: LGPL-3.0-or-later
//! Configure and run the retained polarization solvers.

use casa_core::{RecordField, Value};
use casa_ms::MeasurementSet;

use super::{CalibraterLifecycle, CalibraterLifecycleError, CalibraterModel, ConfiguredSolve};
use crate::solve::grouping::leakage::{
    LeakageSolveKind, LeakageSolveRequest, fractional_stokes_record,
    solve_polarization_with_activity,
};
use crate::solve_stats::ToolSolveActivity;
use crate::{
    attachment::AttachmentError,
    bandpass::BandpassFrequencyInterval,
    solve::{GainSolveModelSource, GainSolveRequest},
};

impl CalibraterLifecycle {
    /// Return whether the name configures a polarization solver.
    #[must_use]
    pub fn recognizes_polarization_solve_type(name: &str) -> bool {
        polarization_kind(name).is_some()
    }

    /// Configure a polarization solve for [`Self::solve_tool`]. The model, selection
    /// and priors are read from the retained equation when the fit runs.
    ///
    /// # Errors
    /// Returns an error without an open MS, for an unsupported solver under the
    /// current iterator, or when a channelized solver's frequency interval is invalid.
    pub fn set_tool_polarization(
        &mut self,
        name: &str,
        standard: GainSolveRequest,
        frequency: Option<&str>,
        append: bool,
        invalid_mode: bool,
    ) -> Result<(), CalibraterLifecycleError> {
        let kind = polarization_kind(name).ok_or_else(|| {
            CalibraterLifecycleError::LeakageSolve(format!("unknown type {name}"))
        })?;
        if self.legacy_iterator
            && matches!(
                kind,
                LeakageSolveKind::PosAng | LeakageSolveKind::Xparang | LeakageSolveKind::Xfparang
            )
        {
            return Err(CalibraterLifecycleError::LeakageSolve(format!(
                "unknown legacy calibration type {name}"
            )));
        }
        let frequency_interval = if kind.channelized() {
            BandpassFrequencyInterval::from_casa(frequency.unwrap_or_default()).ok_or_else(
                || {
                    CalibraterLifecycleError::LeakageSolve(
                        "invalid frequency solution interval".into(),
                    )
                },
            )?
        } else {
            None
        };
        let request = LeakageSolveRequest {
            output: standard.output_table,
            selection: standard.selection,
            kind,
            model_source: standard.model_source,
            stokes: standard.smodel,
            priors: standard.prior_calibration_tables,
            parang_terms: usize::from(standard.parang),
            correlation_dependent_flags: standard.correlation_dependent_flags,
            solve_mode: standard.solve_mode,
            normalization: standard
                .normalize_average_amplitude
                .then_some(standard.normalization_type),
            interval: standard.solve_interval,
            frequency_interval,
            combine: standard.combine,
            preaverage_seconds: standard.preaverage_seconds,
            min_baselines: standard.min_baselines_per_antenna,
            min_snr: standard.min_snr,
            refant: standard.refant,
            append,
        };
        let equation = self.equation_mut()?;
        equation.solve = Some(ConfiguredSolve::Polarization {
            request: Box::new(request),
            invalid_mode,
        });
        equation.solve_description = None;
        Ok(())
    }

    pub(super) fn solve_polarization_tool(&mut self) -> Result<(), CalibraterLifecycleError> {
        let equation = self.equation()?;
        equation.check_opacity_parameters()?;
        let Some(ConfiguredSolve::Polarization {
            request,
            invalid_mode,
        }) = &equation.solve
        else {
            return Err(CalibraterLifecycleError::NoSolver);
        };
        if *invalid_mode {
            return Err(CalibraterLifecycleError::InvalidSolveMode);
        }
        let mut request = request.as_ref().clone();
        match equation.model {
            CalibraterModel::FromMeasurementSet => {
                request.model_source = GainSolveModelSource::FromMeasurementSet;
            }
            CalibraterModel::Point(stokes) => {
                request.model_source = GainSolveModelSource::PointSource;
                request.stokes = stokes;
            }
        }
        request.selection = equation.attachment.selection().clone();
        request.priors = equation.apply.tables.clone();
        request.parang_terms = equation.apply.parang_terms;
        request.correlation_dependent_flags = self
            .correlation_dependent_flags
            .unwrap_or(request.correlation_dependent_flags);
        let ms = MeasurementSet::open(equation.attachment.path()).map_err(AttachmentError::from)?;
        let mut activity = ToolSolveActivity::default();
        let outcome = solve_polarization_with_activity(
            &ms,
            &request,
            Some(&mut activity),
            self.legacy_iterator,
        )
        .map_err(CalibraterLifecycleError::LeakageSolve)?;
        if !self.legacy_iterator
            && let Some(stats) = &outcome.gain_stats
        {
            self.last_solve_stats = Some(crate::solve_stats::solvestats_record(stats));
        }
        if let Some(mut record) = activity.record() {
            if matches!(
                request.kind,
                LeakageSolveKind::Xparang | LeakageSolveKind::Xfparang
            ) {
                let mut fields = record.into_fields();
                fields.insert(
                    0,
                    RecordField::new(
                        "fStokes",
                        Value::Record(fractional_stokes_record(outcome.source)),
                    ),
                );
                record = casa_core::RecordValue::new(fields);
            }
            self.last_activity = Some(record);
        }
        Ok(())
    }
}

fn polarization_kind(name: &str) -> Option<LeakageSolveKind> {
    use LeakageSolveKind::*;

    let upper = name.to_ascii_uppercase();
    let factory = super::traditional_apply::calibration_factory_type(&upper)?;
    let source_pol = upper.contains("QU");
    let source_angle = upper.contains('X');
    let variants = match factory {
        "D Jones" => [D, DQu, DX],
        "Df Jones" => [Df, DfQu, DfX],
        "Dgen Jones" => [DGen, DGenQu, DGenX],
        "Dfgen Jones" => [DfGen, DfGenQu, DfGenX],
        "DLLS Jones" => return Some(Dlls),
        "DfLLS Jones" => return Some(Dflls),
        "X Jones" => return Some(Xj),
        "X Mueller" => return Some(XMueller),
        "Xf Jones" => return Some(Xf),
        "Xparang Jones" => return Some(Xparang),
        "Xfparang Jones" => return Some(Xfparang),
        "PosAng Jones" => return Some(PosAng),
        "GlinXph Jones" => return Some(XY),
        "GlinXphf Jones" => return Some(XYf),
        _ => return None,
    };
    Some(if source_pol {
        variants[1]
    } else if source_angle {
        variants[2]
    } else {
        variants[0]
    })
}
