// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_simulation::simutil::skymodel::{ComponentEdit, modify_components};
use casa_tables::publication::check_write_access;
use pyo3::exceptions::{PyNotImplementedError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::tools::{LogLine, componentlist::image_component_model, emit, table::casa_record};

use super::PyImageTool;
use super::handle::{ImageHandle, Storage, raised, with_storage};
use super::subimage::{Region, mask_argument};

fn check_native_write(path: &Path, history: bool) -> std::io::Result<()> {
    let check_table = |path: &Path| -> std::io::Result<()> {
        check_write_access(path)?;
        for entry in path.read_dir()? {
            let entry = entry?;
            if entry.file_name().to_string_lossy().starts_with("table.") {
                check_write_access(&entry.path())?;
            }
        }
        Ok(())
    };
    check_table(path)?;
    let log = path.join("logtable");
    if history && log.is_dir() {
        check_table(&log)?;
    }
    Ok(())
}

#[pymethods]
impl PyImageTool {
    /// Add or subtract a component model in the selected section of a float image.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (model, region=None, mask=None, subtract=true, list=true, stretch=false), text_signature = "($self, model, region={}, mask='', subtract=True, list=True, stretch=False)")]
    fn modify(
        &self,
        py: Python<'_>,
        model: &Bound<'_, PyAny>,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        subtract: bool,
        list: bool,
        stretch: bool,
    ) -> PyResult<bool> {
        let model = model
            .cast::<PyDict>()
            .map_err(|_| PyTypeError::new_err("_model is not a dictionary"))?;
        if self.state().image.is_none() {
            return Ok(false);
        }
        let model = casa_record(py, Some(model))?;
        let region = Region::argument(py, region, "modify").map_err(|error| {
            if error.is_instance_of::<PyNotImplementedError>(py) {
                raised("ImageRegion::fromRecord - record does not define a region")
            } else {
                error
            }
        })?;
        let mask = mask_argument(mask)?;
        let mut logs = Vec::new();
        let outcome = py.detach(|| {
            let state = self.state();
            let Some(attachment) = &state.image else {
                return Ok(false);
            };
            let mut image = attachment.lock();
            if !matches!(*image, ImageHandle::Float(_)) {
                return Err(raised("Exception: This method only supports float valued images."));
            }
            let model = image_component_model(&model, &mut logs)?;
            if image.fits().is_some() {
                return Err(raised("Failed AlwaysAssert to.isWritable()"));
            }
            if let Some(path) = image.path() {
                check_native_write(path, state.history)
                    .map_err(|_| raised("Failed AlwaysAssert to.isWritable()"))?;
            }
            Region::reject_compound_record(region.as_ref())?;
            attachment.require_lock(&mut image, casa_tables::LockType::Write)?;
            let ImageHandle::Float(storage) = &mut *image else {
                unreachable!("float image checked");
            };
            let options = ComponentEdit {
                region: region.as_ref().map(Region::as_region),
                mask: Some(&mask),
                stretch,
                subtract,
            };
            let report = with_storage!(storage, image =>
                modify_components(image.as_mut(), &model, options)
            ).map_err(raised)?;
            if list && region.is_some() {
                let (blc, trc) = report.bounds;
                logs.push(LogLine::info(format!("Selected bounding box {blc:?} to {trc:?}")));
            }
            if let Some(warning) = report.warning {
                logs.push(LogLine::warning(warning));
            }
            if state.history {
                let region = region.as_ref().map_or_else(|| "{...}".into(), Region::history);
                image.record_history("modify", format!(
                    "ia.modify(model={{...}}, region={region}, mask=\"{mask}\", subtract={subtract}, list={list}, stretch={stretch})"
                ))?;
            }
            image.save().map_err(raised)?;
            Ok(true)
        });
        emit(py, "casars.tools.image", &logs)?;
        outcome
    }
}
