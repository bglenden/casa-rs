// SPDX-License-Identifier: LGPL-3.0-or-later
//! Apply terms configured on a persistent calibrater.

use std::path::Path;

use casa_ms::{MeasurementSet, MsSelection};
use casa_provider_contracts::CasaValue;
use casa_tables::{Table, TableOptions};
use casa_types::RecordValue;

use super::traditional_apply::{calibration_factory_type, non_calibration_table};
use super::{CalibraterLifecycle, CalibraterLifecycleError, ConfiguredSolve};
use crate::callib::{tool_library_specs, tool_table_record};
use crate::casa_return::selectvis_record;
use crate::constants::TABLE_INFO_TYPE;
use crate::execute::{ToolCorrection, execute_tool_apply};
use crate::plan::{
    ApplyInterpolationMode, ApplyMode, ApplyPlanRequest, plan_with_visibility_metadata,
};
use crate::solve::grouping::collect_selected_rows;
use crate::{
    AttachmentError, CalibrationError, CalibrationSolveRequest, GainSolveError, SelectedData,
};

impl CalibraterLifecycle {
    /// Apply the configured chain to the attached, selected visibilities.
    /// Returns false if a selected spectral window has no calibration.
    ///
    /// # Errors
    ///
    /// Returns an error without an attachment, or if calibration cannot be
    /// applied. A failed apply clears the prior terms without replaying writes.
    pub fn correct_visibilities(&mut self, mode: &str) -> Result<bool, CalibraterLifecycleError> {
        let equation = self.equation_mut()?;
        let mode = if mode.is_empty() {
            String::from("CALFLAG")
        } else {
            mode.to_ascii_uppercase()
        };
        let trial = mode.contains("TRIAL") || mode.contains("FLAGONLY");
        let apply_mode = match (mode.contains("CAL"), mode.contains("FLAG")) {
            (true, true) => ApplyMode::CalFlag,
            (true, false) => ApplyMode::CalOnly,
            (false, true) => ApplyMode::FlagOnly,
            (false, false) => ApplyMode::Trial,
        };
        let request = ApplyPlanRequest {
            selection: equation.attachment.selection().clone(),
            apply_mode,
            parang: equation.apply.parang_terms > 0,
            calibration_tables: equation.apply.tables.clone(),
        };
        let correction = ToolCorrection {
            has_library_registration: !equation.apply.library_tables.is_empty(),
            strict: mode.contains("STRICT"),
            trial,
            parang_terms: equation.apply.parang_terms,
            parang_calwt: equation.apply.parang_calwt,
        };
        let result = equation.attachment.with_write(|ms, selected, lock| {
            execute_tool_apply(ms, selected, lock, &request, correction)
                .map_err(|error| casa_ms::MsError::InvalidInput(error.to_string()).into())
        });
        result.map_err(|error| {
            equation.apply = super::ApplyTerms::default();
            tracing::error!(%error, "could not correct selected visibilities");
            CalibraterLifecycleError::ApplyConfiguration("Error in Calibrater::correct.".into())
        })
    }

    /// Append the tables in a calibration-library record to the attached
    /// equation. A missing table prevents registration of the library.
    /// Each successfully ingested table remains registered if a subsequent
    /// readable table fails to configure.
    ///
    /// # Errors
    ///
    /// There is no attached MeasurementSet, a table is unreadable, or one of
    /// the table records cannot configure an apply term.
    pub fn set_callibrary(
        &mut self,
        library: &RecordValue,
    ) -> Result<(), CalibraterLifecycleError> {
        let equation = self.equation_mut()?;
        let failure = CalibraterLifecycleError::ApplyConfiguration;
        let cwd = std::env::current_dir().map_err(|error| failure(error.to_string()))?;
        // CASA preflights the whole set of keys before reading any slices.
        let tables = library
            .fields()
            .iter()
            .map(|field| {
                Table::open(TableOptions::new(Path::new(&field.name)))
                    .map_err(|_| failure(format!("Caltable {} does not exist.", field.name)))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut ms = None;
        for (field, table) in library.fields().iter().zip(tables) {
            let info = table.info();
            if info.table_type != TABLE_INFO_TYPE {
                return Err(non_calibration_table(&field.name, &info.table_type));
            }
            let slices = tool_table_record(field).map_err(failure)?;
            let invalid_library = |reason: String| {
                tracing::error!(
                    table = field.name,
                    reason,
                    "could not register calibration library table"
                );
                failure("Error in Calibrater::callib2.".into())
            };
            if !calibration_factory_type(&info.sub_type)
                .is_some_and(|name| !matches!(name, "BPOLY" | "GSPLINE"))
            {
                return Err(invalid_library(format!(
                    "unsupported library type {}",
                    info.sub_type
                )));
            }
            let specs = tool_library_specs(&field.name, slices, &cwd).map_err(invalid_library)?;
            if specs.len() > 1
                || specs.iter().any(|spec| {
                    spec.interp == ApplyInterpolationMode::Invalid
                        || spec.gainfield.is_some()
                        || spec
                            .callibrary
                            .as_ref()
                            .is_some_and(|options| options.selection != MsSelection::new())
                })
            {
                let ms = if let Some(ms) = &ms {
                    ms
                } else {
                    ms.insert(
                        MeasurementSet::open(equation.attachment.path())
                            .map_err(|error| invalid_library(error.to_string()))?,
                    )
                };
                let request = ApplyPlanRequest {
                    selection: equation.attachment.selection().clone(),
                    apply_mode: ApplyMode::Trial,
                    parang: false,
                    calibration_tables: specs.clone(),
                };
                plan_with_visibility_metadata(ms, &request, None)
                    .map_err(|error| invalid_library(error.to_string()))?;
            }
            equation.apply.tables.extend(specs);
            equation.apply.library_tables.push(field.name.clone());
        }
        Ok(())
    }

    /// Sorted distinct paths of configured calibration tables, as the
    /// calibrater returns them in its `apply_tables` record field.
    #[must_use]
    pub fn applied_table_names(&self) -> Vec<String> {
        let Ok(equation) = self.equation() else {
            return Vec::new();
        };
        let mut names = equation
            .apply
            .tables
            .iter()
            .map(|table| {
                table.callibrary.as_ref().map_or_else(
                    || {
                        equation.apply.table_snapshots.get(&table.path).map_or_else(
                            || table.path.display().to_string(),
                            |snapshot| snapshot.name.clone(),
                        )
                    },
                    |library| library.table_name.clone(),
                )
            })
            .chain(equation.apply.library_tables.iter().cloned())
            .collect::<Vec<_>>();
        names.sort_unstable();
        names.dedup();
        names
    }

    /// Current selections and configured calibration table names.
    ///
    /// # Errors
    ///
    /// The tool has no attached dataset, or the selected MeasurementSet
    /// metadata cannot be read.
    pub fn configuration_record(&self) -> Result<CasaValue, CalibraterLifecycleError> {
        let equation = self.equation()?;
        let ms = MeasurementSet::open(equation.attachment.path()).map_err(AttachmentError::from)?;
        let rows = match collect_selected_rows(&ms, equation.attachment.selection(), true) {
            Ok(rows) => rows,
            Err(GainSolveError::EmptySelection) => Vec::new(),
            Err(error) => return Err(CalibrationError::Gain(Box::new(error)).into()),
        };
        let selected = SelectedData::of_rows(&ms, rows.iter())
            .map_err(|error| CalibrationError::Gain(Box::new(error)))?;
        let output = equation.solve.as_ref().map(|configured| {
            let path = match configured {
                ConfiguredSolve::LegacyEp(ep) => ep.output_path(),
                ConfiguredSolve::Task(request, _) => match request.as_ref() {
                    CalibrationSolveRequest::Gain(gain)
                    | CalibrationSolveRequest::GainSpline { gain, .. }
                    | CalibrationSolveRequest::Fringe { gain, .. }
                    | CalibrationSolveRequest::Accor(gain)
                    | CalibrationSolveRequest::Baseline { gain, .. } => &gain.output_table,
                    CalibrationSolveRequest::Bandpass(request) => &request.output_table,
                },
            };
            path.display().to_string()
        });
        Ok(CasaValue::dict([
            (
                "apply_tables",
                CasaValue::str_vector(self.applied_table_names()),
            ),
            ("selectvis", selectvis_record(Some(&selected))),
            ("solve_tables", CasaValue::str_vector(output)),
        ]))
    }
}
