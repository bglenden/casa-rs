// SPDX-License-Identifier: LGPL-3.0-or-later
//! Apply terms configured on a persistent calibrater.

use std::path::Path;

use casa_ms::MeasurementSet;
use casa_provider_contracts::CasaValue;
use casa_tables::{Table, TableOptions};
use casa_types::{RecordValue, Value};

use super::{CalibraterLifecycle, CalibraterLifecycleError, ConfiguredSolve};
use crate::callib::tool_library_specs;
use crate::casa_return::selectvis_record;
use crate::solve::grouping::collect_selected_rows;
use crate::{
    AttachmentError, CalibrationError, CalibrationSolveRequest, GainSolveError, SelectedData,
};

impl CalibraterLifecycle {
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
        for table in library.fields() {
            if Table::open(TableOptions::new(Path::new(&table.name))).is_err() {
                return Err(failure(format!("Caltable {} does not exist.", table.name)));
            }
        }
        for table in library.fields() {
            let Value::Record(slices) = &table.value else {
                return Err(failure("Error in Calibrater::callib2.".into()));
            };
            let specs = tool_library_specs(&table.name, slices, &cwd).map_err(|reason| {
                tracing::error!(
                    table = table.name,
                    reason,
                    "could not register calibration library table"
                );
                failure("Error in Calibrater::callib2.".into())
            })?;
            equation.apply.tables.extend(specs);
            equation.apply.library_tables.push(table.name.clone());
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
                    || table.path.display().to_string(),
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
