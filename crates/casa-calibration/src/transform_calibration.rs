// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read-only callibrary correction for the native MeasurementSet transform.

use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;
use std::sync::Arc;

use casa_ms::{
    MeasurementSet, MsSelection, PreparedTransformInputView, ResolvedMsSelectionRow,
    TransformInputColumns, TransformInputView,
};
use casa_tables::{Table, TableError};

use crate::callib::{CallibError, load_transform_apply_specs};
use crate::execute::PreparedReadOnlyApply;
use crate::plan::{ApplyMode, ApplyPlanRequest, plan_transform_rows};

/// Read a CASA callibrary for the transform's bounded array reader.
///
/// Calibration plans use the transform's resolved rows and channel slices.
/// A retained Multi-MS prepares a plan per physical member. The source
/// table and calibration tables are read only.
pub fn transform_callibrary(
    path: &Path,
    workspace: &Path,
) -> Result<Option<Arc<dyn TransformInputView>>, CallibError> {
    let calibration_tables = load_transform_apply_specs(path, workspace)?;
    if calibration_tables.is_empty() {
        return Ok(None);
    }
    Ok(Some(Arc::new(CallibraryView {
        request: ApplyPlanRequest {
            selection: MsSelection::default(),
            apply_mode: ApplyMode::CalFlag,
            parang: false,
            calibration_tables,
        },
    })))
}

struct CallibraryView {
    request: ApplyPlanRequest,
}

impl fmt::Debug for CallibraryView {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CallibraryView")
            .finish_non_exhaustive()
    }
}

impl TransformInputView for CallibraryView {
    fn prepare<'a>(
        &self,
        input: &'a MeasurementSet,
        rows: &[ResolvedMsSelectionRow],
        channels: &BTreeMap<i32, &[usize]>,
    ) -> Result<Box<dyn PreparedTransformInputView + 'a>, TableError> {
        let row_indices = rows.iter().map(|row| row.row_index).collect::<Vec<_>>();
        let plan = plan_transform_rows(input, &self.request, &row_indices, channels)
            .map_err(|source| TableError::Schema(source.to_string()))?;
        let prepared = PreparedReadOnlyApply::new(input, plan)
            .map_err(|source| TableError::Schema(source.to_string()))?;
        Ok(Box::new(prepared))
    }

    fn additional_bytes_per_sample(&self) -> usize {
        // Raw and corrected complex data, selected copies, input and
        // output flags, and sigma/weight work arrays during application.
        48
    }

    fn provides(&self, input: &MeasurementSet, column: &str) -> bool {
        match column {
            "CORRECTED_DATA" | "FLAG" | "WEIGHT" => true,
            "WEIGHT_SPECTRUM" => input
                .main_table()
                .schema()
                .is_some_and(|schema| schema.contains_column("WEIGHT_SPECTRUM")),
            _ => false,
        }
    }
}

impl PreparedTransformInputView for PreparedReadOnlyApply {
    fn read(
        &self,
        input: &Table,
        rows: &[usize],
        columns: &[&str],
    ) -> Result<TransformInputColumns, TableError> {
        let mut output = TransformInputColumns::new();
        self.visit_rows(input, rows, |corrected, flags, weights| {
            for (name, array) in [
                ("CORRECTED_DATA", Some(corrected)),
                ("FLAG", Some(flags)),
                ("WEIGHT", Some(weights.scalar)),
                ("WEIGHT_SPECTRUM", weights.spectrum),
            ] {
                if columns.contains(&name) {
                    output.entry(name).or_default().push(array);
                }
            }
        })
        .map_err(|source| TableError::Schema(source.to_string()))?;
        Ok(output)
    }
}
