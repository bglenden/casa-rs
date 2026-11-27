// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.subimage` wrapper and region argument parser delegating to [`casa_imageanalysis::tool::subimage`].

use casa_core::{RecordValue, ScalarValue, Value};
use casa_imageanalysis::analysis::SubimageRegion;
use casa_imageanalysis::tool::subimage::{SubimageRequest, extract_subimage};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyList, PyString, PyTuple};

use crate::convert::{IntList, Variant};
use crate::tools::table::casa_record;

use super::PyImageTool;
use super::handle::{emit_tool_logs, external_open_check, raised};

pub(super) enum Region {
    Text(String),
    Record(RecordValue),
}

impl Region {
    pub(super) fn argument(
        py: Python<'_>,
        value: Option<&Bound<'_, PyAny>>,
        method: &str,
    ) -> PyResult<Option<Self>> {
        let region = Self::capture(py, value)?;
        if let Some(Self::Record(record)) = &region {
            let Some(Value::Scalar(ScalarValue::String(name))) = record.get("name") else {
                return Err(super::stub(method).unwrap_err());
            };
            match name.as_str() {
                "LCSlicer" | "LCBox" | "WCBox" | "WCPolygon" | "WCEllipsoid" => {}
                "WCUnion" | "WCDifference" | "WCIntersection" | "WCComplement" | "WCExtension"
                | "WCConcatenation" => {
                    if record.get("regions").is_none() {
                        return Err(raised("RecordInterface: field regions is unknown"));
                    }
                }
                _ => return Err(super::stub(method).unwrap_err()),
            }
        }
        Ok(region)
    }

    pub(super) fn reject_compound_record(region: Option<&Self>) -> PyResult<()> {
        if matches!(region, Some(Self::Record(record)) if record.get("regions").is_some()) {
            return Err(raised(
                "Exception: Only a single n-dimensional rectangular region is supported.",
            ));
        }
        Ok(())
    }

    pub(super) fn capture(
        py: Python<'_>,
        value: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Option<Self>> {
        let Some(value) = value else {
            return Ok(None);
        };
        if let Ok(record) = value.cast::<PyDict>() {
            let record = casa_record(py, Some(record))?;
            return Ok((!record.is_empty()).then_some(Self::Record(record)));
        }
        if let Ok(text) = value.cast::<PyString>() {
            let text = text.to_str()?;
            return Ok((!text.is_empty()).then(|| Self::Text(text.to_owned())));
        }
        if empty_variant(value)? {
            return Ok(None);
        }
        Err(raised("Unsupported type for region"))
    }

    pub(super) fn as_region(&self) -> SubimageRegion<'_> {
        match self {
            Self::Text(text) => SubimageRegion::Text(text),
            Self::Record(record) => SubimageRegion::Record(record),
        }
    }

    pub(super) fn history(&self) -> String {
        match self {
            Self::Text(text) => format!("{text:?}"),
            Self::Record(_) => "{...}".to_owned(),
        }
    }
}

/// Draw the same distinction CASA makes between its default record and an
/// explicitly supplied empty text or array.
pub(super) struct RegionArgument {
    selection: Option<Region>,
    record: bool,
    empty_history: &'static str,
}

impl Default for RegionArgument {
    fn default() -> Self {
        Self {
            selection: None,
            record: true,
            empty_history: "{...}",
        }
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for RegionArgument {
    type Error = PyErr;

    fn extract(value: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        let record = value.is_instance_of::<PyDict>();
        let empty_history = if record {
            "{...}"
        } else if value.is_instance_of::<PyString>() {
            r#""""#
        } else {
            "[]"
        };
        Ok(Self {
            selection: Region::capture(value.py(), (!value.is_none()).then_some(&value))?,
            record,
            empty_history,
        })
    }
}

impl RegionArgument {
    pub(super) fn empty_text() -> Self {
        Self {
            selection: None,
            record: false,
            empty_history: r#""""#,
        }
    }

    pub(super) fn selection(&self) -> Option<SubimageRegion<'_>> {
        self.selection.as_ref().map(Region::as_region)
    }

    pub(super) fn validate_selectors(
        &self,
        spatial: &str,
        chans: &str,
        stokes: &str,
    ) -> PyResult<()> {
        if self.record || self.selection.is_some() {
            if !spatial.is_empty() {
                return Err(raised(
                    "box and regionPtr cannot be simultaneously specified",
                ));
            }
            if !chans.is_empty() || !stokes.is_empty() {
                return Err(raised(
                    "regionPtr and regionName, chans, and/or stokes cannot be simultaneously specified",
                ));
            }
        }
        Ok(())
    }

    pub(super) fn history(&self) -> String {
        self.selection
            .as_ref()
            .map_or_else(|| self.empty_history.into(), Region::history)
    }
}

pub(super) fn require_bool(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    value
        .cast::<PyBool>()
        .map(|value| value.is_true())
        .map_err(|_| PyTypeError::new_err("expected bool"))
}

fn empty_variant(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    if value.is_instance_of::<PyList>() || value.is_instance_of::<PyTuple>() {
        return Ok(value
            .try_iter()?
            .all(|item| item.is_ok_and(|item| item.is_instance_of::<PyBool>())));
    }
    Ok(value.extract::<Variant>()?.is_empty_sequence())
}

pub(super) fn mask_argument(value: Option<&Bound<'_, PyAny>>) -> PyResult<String> {
    let Some(value) = value else {
        return Ok(String::new());
    };
    if let Ok(text) = value.cast::<PyString>() {
        return Ok(text.to_str()?.to_owned());
    }
    if empty_variant(value)? {
        return Ok(String::new());
    }
    Err(raised("Mask is not understood, try a valid LEL string"))
}

#[pymethods]
impl PyImageTool {
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (outfile="", region=None, mask=None, dropdeg=false, overwrite=false, list=true, stretch=false, wantreturn=true, keepaxes=None), text_signature = "($self, outfile='', region='', mask='', dropdeg=False, overwrite=False, list=True, stretch=False, wantreturn=True, keepaxes=[])")]
    fn subimage(
        &self,
        py: Python<'_>,
        outfile: &str,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        dropdeg: bool,
        overwrite: bool,
        list: bool,
        stretch: bool,
        wantreturn: bool,
        keepaxes: Option<IntList>,
    ) -> PyResult<Py<Self>> {
        let region = Region::argument(py, region, "subimage")?;
        let mask = mask_argument(mask)?;
        let keepaxes = keepaxes.map_or_else(Vec::new, |axes| axes.0);
        let (result, logs) = py.detach(|| {
            let state = self.state();
            let attached = state
                .image
                .as_ref()
                .ok_or_else(|| raised("Unable to create image"))?;
            let request = SubimageRequest {
                outfile,
                region: region.as_ref().map(Region::as_region),
                mask: (!mask.is_empty()).then_some(mask.as_str()),
                dropdeg,
                overwrite,
                list,
                stretch,
                wantreturn,
                keepaxes: &keepaxes,
            };
            extract_subimage(attached, &request, state.history, external_open_check())
                .map_err(raised)
        })?;
        emit_tool_logs(py, &logs)?;
        Py::new(py, result.map_or_else(Self::new, Self::attached_handle))
    }
}
