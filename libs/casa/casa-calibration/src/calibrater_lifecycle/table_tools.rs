// SPDX-License-Identifier: LGPL-3.0-or-later
//! Table operations that use the calibrater's current attachment.

use std::path::Path;

use casa_ms::{MeasurementSet, MsError};
use casa_tables::{Table, TableOptions};

use crate::rerefant::rereference_calibration_table;
use crate::smooth::{Smoother, smooth_calibration_table};
use crate::{
    attachment::AttachmentError,
    table::{CalibrationAccumulationRequest, accumulate_calibration, create_calibration_table},
};

use super::{CalibraterLifecycle, CalibraterLifecycleError};

impl CalibraterLifecycle {
    /// Create an empty CASA calibration table using the attached MS metadata.
    ///
    /// # Errors
    /// The tool is unattached, the metadata cannot be read, or publication fails.
    pub fn create_table(
        &self,
        destination: &Path,
        parameter_type: &str,
        calibration_type: &str,
        single_channel: bool,
    ) -> Result<(), CalibraterLifecycleError> {
        let path = self.equation()?.attachment.path();
        let ms = MeasurementSet::open(path).map_err(AttachmentError::from)?;
        create_calibration_table(
            &ms,
            destination,
            parameter_type,
            calibration_type,
            single_channel,
        )
        .map_err(CalibraterLifecycleError::CreateTable)
    }

    /// Accumulate calibration through the original visibility iterator. A
    /// positive time interval replaces the current selection with the whole MS
    /// once the incremental table has been validated.
    ///
    /// # Errors
    /// The tool is unattached, the new iterator is active, or accumulation fails.
    pub fn accumulate_table(
        &mut self,
        request: &CalibrationAccumulationRequest,
    ) -> Result<(), CalibraterLifecycleError> {
        let legacy_iterator = self.legacy_iterator;
        let equation = self.equation_mut()?;
        if !legacy_iterator {
            return Err(CalibraterLifecycleError::AccumulateNeedsLegacyIterator);
        }
        let mut accumulate = || -> Result<(), MsError> {
            if !request.interval_seconds.is_finite() {
                return Err(MsError::InvalidInput(
                    "accumulation time must be finite".into(),
                ));
            }
            if request.interval_seconds > 0.0 {
                let incremental = Table::open(TableOptions::new(&request.incremental))?;
                if incremental.info().table_type != "Calibration" {
                    return Err(MsError::InvalidInput(
                        "input is not a calibration table".into(),
                    ));
                }
                equation.attachment.reset_selection();
            }
            let ms = MeasurementSet::open(equation.attachment.path())?;
            accumulate_calibration(&ms, equation.attachment.selection(), request)
                .map_err(MsError::InvalidInput)
        };
        accumulate().map_err(CalibraterLifecycleError::Accumulate)
    }

    /// Smooth solutions using the attached MS's field selectors and metadata.
    /// An empty destination replaces the input table.
    ///
    /// # Errors
    /// The tool is unattached or the smoothing settings, input, or output fail.
    pub fn smooth_table(
        &self,
        input: &Path,
        destination: &Path,
        field: &str,
        mode: &str,
        seconds: f64,
        rate_smooth: bool,
    ) -> Result<(), CalibraterLifecycleError> {
        let path = self.equation()?.attachment.path();
        let result = Smoother::from_name(mode)
            .ok_or_else(|| "Unrecognized smooth type!".to_owned())
            .and_then(|smoother| {
                smooth_calibration_table(
                    path,
                    input,
                    output_path(input, destination),
                    field,
                    smoother,
                    seconds,
                    rate_smooth,
                )
                .map_err(|error| error.to_string())
            });
        result.map_err(|message| CalibraterLifecycleError::Smooth(MsError::InvalidInput(message)))
    }

    /// Change the reference antenna in a calibration table using the attached
    /// MS metadata. An empty destination replaces the input table.
    ///
    /// # Errors
    /// The tool is unattached or the reference settings, input, or output fail.
    pub fn rereference_table(
        &self,
        input: &Path,
        destination: &Path,
        mode: &str,
        reference: &str,
    ) -> Result<(), CalibraterLifecycleError> {
        let path = self.equation()?.attachment.path();
        let result = match mode {
            "flex" | "strict" => rereference_calibration_table(
                path,
                input,
                output_path(input, destination),
                mode == "flex",
                reference,
            )
            .map_err(|error| error.to_string()),
            _ => Err("Unrecognized refantmode!".into()),
        };
        result.map_err(|message| {
            CalibraterLifecycleError::Rereference(MsError::InvalidInput(message))
        })
    }
}

fn output_path<'a>(input: &'a Path, destination: &'a Path) -> &'a Path {
    if destination.as_os_str().is_empty() {
        input
    } else {
        destination
    }
}
