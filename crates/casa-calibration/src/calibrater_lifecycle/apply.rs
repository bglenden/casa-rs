// SPDX-License-Identifier: LGPL-3.0-or-later
//! Apply terms configured on a persistent calibrater.

use std::path::Path;

use casa_core::{ArrayValue, RecordField, RecordValue, Value};
use casa_ms::{MeasurementSet, MsSelection};

use super::traditional_apply::{
    calibration_factory_type, non_calibration_table, open_apply_table, snapshot_table,
};
use super::{CalibraterLifecycle, CalibraterLifecycleError, ConfiguredSolve};
use crate::callib::{tool_library_specs, tool_table_record};
use crate::constants::TABLE_INFO_TYPE;
use crate::execute::{
    LibraryApplyRegistration, ToolActivityInput, ToolCorrection, execute_tool_apply,
    execute_tool_corrupt,
};
use crate::plan::{
    ApplyInterpolationMode, ApplyMode, ApplyPlanRequest, plan_with_visibility_metadata,
};
use crate::solve::grouping::collect_selected_rows;
use crate::solve_stats::selectvis_record_value;
use crate::{
    AttachmentError, CalibrationError, CalibrationSolveRequest, GainSolveError, SelectedData,
};

impl CalibraterLifecycle {
    /// Activity from the last completed equation operation.
    #[must_use]
    pub fn activity_record(&self) -> RecordValue {
        self.last_activity
            .clone()
            .unwrap_or_else(|| RecordValue::new(Vec::new()))
    }

    /// Apply the configured chain to the attached, selected visibilities.
    /// Returns false if a selected spectral window has no calibration.
    ///
    /// # Errors
    ///
    /// Returns an error without an attachment, or if calibration cannot be
    /// applied. A failed apply clears the prior terms without replaying writes.
    pub fn correct_visibilities(&mut self, mode: &str) -> Result<bool, CalibraterLifecycleError> {
        self.apply_equation(ApplyOperation::Correct(mode))
    }

    /// Apply the selected chain forward to the attached physical model.
    /// # Errors
    /// Returns an error without an attachment or when MODEL_DATA cannot be
    /// read, corrupted or published. A failed run clears the apply terms.
    pub fn corrupt_model(&mut self) -> Result<bool, CalibraterLifecycleError> {
        self.apply_equation(ApplyOperation::Corrupt)
    }

    fn apply_equation(
        &mut self,
        operation: ApplyOperation<'_>,
    ) -> Result<bool, CalibraterLifecycleError> {
        let using_callibrary = self.using_callibrary;
        let legacy = self.legacy_iterator;
        let equation = self.equation_mut()?;
        let origin = match operation {
            ApplyOperation::Correct(_) => "Calibrater::correct",
            ApplyOperation::Corrupt if legacy => "Calibrater::corrupt",
            ApplyOperation::Corrupt => "Calibrater::corrupt2",
        };
        let mode = match operation {
            ApplyOperation::Correct("") => String::from("CALFLAG"),
            ApplyOperation::Correct(mode) => mode.to_ascii_uppercase(),
            ApplyOperation::Corrupt => String::new(),
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
            tableless_opacity: !equation.apply.tableless_opacity.is_empty(),
        };
        let names = equation
            .apply
            .tables
            .iter()
            .map(|table| equation.apply.source_name(table))
            .collect::<Vec<_>>();
        let opacity = equation
            .apply
            .tableless_opacity
            .iter()
            .map(|term| (term.subtype, term.index))
            .collect::<Vec<_>>();
        let tracking = ToolActivityInput {
            names: &names,
            libraries: &equation.apply.library_tables,
            opacity: &opacity,
            previous: equation.apply.counts,
            additive: &equation.apply.additive,
        };
        let result = equation.attachment.with_write(|ms, selected, lock| {
            let applied = match operation {
                ApplyOperation::Correct(_) => {
                    execute_tool_apply(ms, selected, lock, &request, correction, tracking)
                }
                ApplyOperation::Corrupt => execute_tool_corrupt(
                    ms,
                    selected,
                    lock,
                    &request,
                    correction,
                    tracking,
                    crate::execute::ModelCorruption {
                        legacy,
                        using_callibrary,
                    },
                ),
            };
            applied.map_err(|error| casa_ms::MsError::InvalidInput(error.to_string()).into())
        });
        let (all_calibrated, activity) = result.map_err(|error| {
            equation.apply = super::ApplyTerms::default();
            tracing::error!(%error, origin, "could not apply calibration to selected visibilities");
            CalibraterLifecycleError::ApplyConfiguration(format!("Error in {origin}."))
        })?;
        equation.apply.counts = activity.single_term_counts();
        if !legacy || matches!(operation, ApplyOperation::Correct(_)) {
            self.last_activity = Some(activity.into_record(origin));
        }
        Ok(all_calibrated)
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
                open_apply_table(Path::new(&field.name))
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
            let mut specs =
                tool_library_specs(&field.name, slices, &cwd).map_err(invalid_library)?;
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
            if let Some(first) = specs.first() {
                let (path, snapshot) = snapshot_table(&first.path, &table)
                    .map_err(|error| invalid_library(error.to_string()))?;
                for spec in &mut specs {
                    spec.path.clone_from(&path);
                }
                equation.apply.table_snapshots.insert(path, snapshot);
            }
            let start = equation.apply.tables.len();
            equation.apply.tables.extend(specs);
            equation
                .apply
                .library_tables
                .push(LibraryApplyRegistration {
                    name: field.name.clone(),
                    subtype: info.sub_type.clone(),
                    tables: start..equation.apply.tables.len(),
                });
        }
        self.using_callibrary = true;
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
            .map(|table| equation.apply.source_name(table))
            .chain(
                equation
                    .apply
                    .library_tables
                    .iter()
                    .map(|library| library.name.clone()),
            )
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
    pub fn configuration_record(&self) -> Result<RecordValue, CalibraterLifecycleError> {
        let equation = self.equation()?;
        let ms = MeasurementSet::open(equation.attachment.path()).map_err(AttachmentError::from)?;
        let rows = match collect_selected_rows(&ms, equation.attachment.selection(), true) {
            Ok(rows) => rows,
            Err(GainSolveError::EmptySelection) => Vec::new(),
            Err(error) => return Err(CalibrationError::Gain(Box::new(error)).into()),
        };
        let selected = SelectedData::of_rows(&ms, rows.iter())
            .map_err(|error| CalibrationError::Gain(Box::new(error)))?;
        let output = equation.solve.as_ref().and_then(|configured| {
            let path = match configured {
                ConfiguredSolve::LegacyEp(ep) => ep.output_path(),
                ConfiguredSolve::SingleDish(solver) => solver.recorded_output()?,
                ConfiguredSolve::Polarization { request, .. } => &request.output,
                ConfiguredSolve::GatherOnly { request, .. } => &request.output_table,
                ConfiguredSolve::Task { request, .. } => match request.as_ref() {
                    CalibrationSolveRequest::Gain(gain)
                    | CalibrationSolveRequest::GainSpline { gain, .. }
                    | CalibrationSolveRequest::Fringe { gain, .. }
                    | CalibrationSolveRequest::FringeTool { gain, .. }
                    | CalibrationSolveRequest::Accor(gain)
                    | CalibrationSolveRequest::AdditiveMueller(gain)
                    | CalibrationSolveRequest::AdditiveMuellerPolynomial { gain, .. }
                    | CalibrationSolveRequest::Baseline { gain, .. } => &gain.output_table,
                    CalibrationSolveRequest::Bandpass(request) => &request.output_table,
                },
            };
            Some(path.display().to_string())
        });
        let mut entries = vec![
            RecordField::new(
                "apply_tables",
                Value::Array(ArrayValue::from_string_vec(self.applied_table_names())),
            ),
            RecordField::new(
                "selectvis",
                Value::Record(selectvis_record_value(Some(&selected))),
            ),
            RecordField::new(
                "solve_tables",
                Value::Array(ArrayValue::from_string_vec(output.into_iter().collect())),
            ),
        ];
        if let Some(stats) = &self.last_solve_stats {
            entries.push(RecordField::new("solvestats", Value::Record(stats.clone())));
        }
        Ok(RecordValue::new(entries))
    }
}

#[derive(Clone, Copy)]
enum ApplyOperation<'a> {
    Correct(&'a str),
    Corrupt,
}

impl super::ApplyTerms {
    fn source_name(&self, table: &crate::ApplyCalibrationTableSpec) -> String {
        if let Some(library) = &table.callibrary {
            library.table_name.clone()
        } else {
            self.table_snapshots.get(&table.path).map_or_else(
                || table.path.display().to_string(),
                |snapshot| snapshot.name.clone(),
            )
        }
    }
}
