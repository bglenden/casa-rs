// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.fromimage` / `ia.newimagefromimage` wrapper delegating to [`casa_imageanalysis::tool::copy`].

use std::path::Path;

use casa_core::RecordValue;
use casa_imageanalysis::tool::copy::{CopyImageRequest, CopyRegion, copy_image};
use numpy::{PyArrayDyn, PyArrayMethods};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyString};

use crate::convert::Variant;
use crate::tools::table::casa_record;

use super::PyImageTool;
use super::handle::{external_open_check, raised};
use super::lifecycle::resolve_image_path;
use super::region_read::is_boolean_sequence;

fn copy_region_argument(
    py: Python<'_>,
    raw: Option<&Bound<'_, PyAny>>,
    default_history: &str,
) -> PyResult<(CopyRegion, String)> {
    let empty_record = || CopyRegion::Record(RecordValue::default());
    let Some(raw) = raw else {
        return Ok((empty_record(), default_history.into()));
    };
    if let Ok(dict) = raw.cast::<PyDict>() {
        return Ok((
            CopyRegion::Record(casa_record(py, Some(dict))?),
            "{...}".into(),
        ));
    }
    if let Ok(text) = raw.cast::<PyString>() {
        let text = text.to_str()?.to_owned();
        let history = format!("{text:?}");
        return Ok((CopyRegion::Text(text), history));
    }
    if is_boolean_sequence(raw)? {
        let values = if let Ok(array) = raw.cast::<PyArrayDyn<bool>>() {
            array
                .readonly()
                .as_array()
                .reversed_axes()
                .iter()
                .copied()
                .collect()
        } else {
            raw.extract::<Vec<bool>>()?
        };
        let history = values
            .into_iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(",");
        return Ok((empty_record(), format!("[{history}]")));
    }
    if raw.extract::<Variant>()?.is_empty_sequence() {
        return Ok((empty_record(), "[]".into()));
    }
    Err(raised("Unsupported type for region"))
}

fn mask_argument(raw: Option<&Bound<'_, PyAny>>) -> PyResult<(String, String)> {
    let Some(raw) = raw else {
        return Ok((String::new(), "\"\"".into()));
    };
    if let Ok(text) = raw.cast::<PyString>() {
        let text = text.to_str()?.to_owned();
        let history = format!("{text:?}");
        return Ok((text, history));
    }
    let value: Variant = raw.extract()?;
    if value.is_empty_sequence() {
        return Ok((String::new(), "[]".into()));
    }
    Err(raised("Mask is not understood, try a valid LEL string"))
}

#[pymethods]
#[allow(clippy::too_many_arguments)]
impl PyImageTool {
    #[pyo3(signature = (outfile="", infile="", region=None, mask=None, dropdeg=false, overwrite=false), text_signature = "($self, outfile='', infile='', region=[], mask='', dropdeg=False, overwrite=False)")]
    fn fromimage(
        &self,
        py: Python<'_>,
        outfile: &str,
        infile: &str,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        dropdeg: bool,
        overwrite: bool,
    ) -> PyResult<bool> {
        let (region, region_history) = copy_region_argument(py, region, "[]")?;
        let (mask, mask_history) = mask_argument(mask)?;
        let expanded = resolve_image_path(py, infile)?;
        py.detach(|| {
            let mut state = self.state();
            let request = CopyImageRequest {
                method: "fromimage",
                infile,
                resolved_infile: Path::new(expanded.as_ref()),
                outfile,
                region,
                region_history,
                mask,
                mask_history,
                dropdeg,
                overwrite,
                history: state.history,
            };
            let output = copy_image(&request, state.image.as_ref(), external_open_check())
                .map_err(raised)?;
            state.image = Some(output);
            Ok(true)
        })
    }

    #[pyo3(signature = (infile="", outfile="", region=None, mask=None, dropdeg=false, overwrite=false), text_signature = "($self, infile='', outfile='', region={}, mask='', dropdeg=False, overwrite=False)")]
    fn newimagefromimage(
        &self,
        py: Python<'_>,
        infile: &str,
        outfile: &str,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        dropdeg: bool,
        overwrite: bool,
    ) -> PyResult<Py<Self>> {
        let (region, region_history) = copy_region_argument(py, region, "{...}")?;
        let (mask, mask_history) = mask_argument(mask)?;
        let expanded = resolve_image_path(py, infile)?;
        let output = py.detach(|| {
            let state = self.state();
            let request = CopyImageRequest {
                method: "newimagefromimage",
                infile,
                resolved_infile: Path::new(expanded.as_ref()),
                outfile,
                region,
                region_history,
                mask,
                mask_history,
                dropdeg,
                overwrite,
                history: state.history,
            };
            copy_image(&request, state.image.as_ref(), external_open_check()).map_err(raised)
        })?;
        Py::new(py, Self::attached(output))
    }
}
