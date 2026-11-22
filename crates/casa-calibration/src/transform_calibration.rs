// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read-only callibrary correction for the native MeasurementSet transform.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use casa_ms::{MeasurementSet, MsSelection, TransformInputColumns, TransformInputView};
use casa_tables::{Table, TableError};

use crate::callib::{CallibError, load_transform_apply_specs};
use crate::execute::PreparedReadOnlyApply;
use crate::plan::{ApplyMode, ApplyPlanRequest, plan_apply};

/// Read a CASA callibrary for the transform's bounded array reader.
///
/// Calibration plans are prepared for the selected input table on first
/// use. A retained Multi-MS switches the plan as it moves between physical
/// members. The source table and calibration tables are read only.
pub fn transform_callibrary(
    path: &Path,
    selection: MsSelection,
) -> Result<Option<Arc<dyn TransformInputView>>, CallibError> {
    let calibration_tables = load_transform_apply_specs(path)?;
    if calibration_tables.is_empty() {
        return Ok(None);
    }
    Ok(Some(Arc::new(CallibraryView {
        request: ApplyPlanRequest {
            selection,
            apply_mode: ApplyMode::CalFlag,
            parang: false,
            calibration_tables,
        },
        active: Mutex::new(None),
    })))
}

struct CallibraryView {
    request: ApplyPlanRequest,
    active: Mutex<Option<(PathBuf, PreparedReadOnlyApply)>>,
}

impl fmt::Debug for CallibraryView {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CallibraryView")
            .finish_non_exhaustive()
    }
}

impl TransformInputView for CallibraryView {
    fn additional_bytes_per_sample(&self) -> usize {
        // Raw and corrected complex data, selected copies, input and
        // output flags, and sigma/weight work arrays during application.
        48
    }

    fn provides(&self, input: &Table, column: &str) -> bool {
        match column {
            "CORRECTED_DATA" | "FLAG" | "WEIGHT" => true,
            "WEIGHT_SPECTRUM" => input
                .schema()
                .is_some_and(|schema| schema.contains_column("WEIGHT_SPECTRUM")),
            _ => false,
        }
    }

    fn read(
        &self,
        input: &Table,
        rows: &[usize],
        columns: &[&str],
    ) -> Result<TransformInputColumns, TableError> {
        let error = |reason: String| TableError::Schema(reason);
        let path = input
            .path()
            .ok_or_else(|| error("calibration requires a persisted MeasurementSet".into()))?;
        let mut active = self
            .active
            .lock()
            .map_err(|_| error("callibrary reader lock was poisoned".into()))?;
        if active.as_ref().is_none_or(|(current, _)| current != path) {
            let ms = MeasurementSet::open(path).map_err(|source| error(source.to_string()))?;
            let plan =
                plan_apply(&ms, &self.request).map_err(|source| error(source.to_string()))?;
            let prepared = PreparedReadOnlyApply::new(&ms, plan)
                .map_err(|source| error(source.to_string()))?;
            *active = Some((path.into(), prepared));
        }
        let planned = &active
            .as_ref()
            .expect("calibration prepared for this input")
            .1;
        let mut output = TransformInputColumns::new();
        planned
            .visit_rows(input, rows, |value| {
                let weights = value
                    .plot_weights
                    .expect("transform uses the read-only noise model");
                for (name, array) in [
                    ("CORRECTED_DATA", Some(value.corrected_data)),
                    ("FLAG", Some(value.flags)),
                    ("WEIGHT", Some(weights.scalar)),
                    ("WEIGHT_SPECTRUM", weights.spectrum),
                ] {
                    if columns.contains(&name) {
                        output.entry(name).or_default().push(array);
                    }
                }
            })
            .map_err(|source| error(source.to_string()))?;
        Ok(output)
    }
}
