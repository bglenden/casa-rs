// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.getregion` wrapper delegating to [`casa_imageanalysis::tool::region_read`].

use casa_core::RecordValue;
use casa_imageanalysis::tool::region_read::read_region_casa;
use numpy::{PyArrayDescrMethods, PyUntypedArray, PyUntypedArrayMethods};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyList, PyString, PyTuple};

use crate::convert::{IntList, Variant, value::CasaValue};
use crate::tools::table::casa_record;

use super::handle::{emit_tool_logs, raised};
use super::pixels::chunk_data_to_casa_value;
use super::subimage::Region;
use super::{PyImageTool, stub};

pub(super) fn is_boolean_sequence(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    if let Ok(array) = value.cast::<PyUntypedArray>() {
        return Ok(array.dtype().kind() == b'b');
    }
    if value.is_instance_of::<PyList>() || value.is_instance_of::<PyTuple>() {
        return Ok(value
            .try_iter()?
            .all(|item| item.is_ok_and(|item| item.is_instance_of::<PyBool>())));
    }
    Ok(false)
}

pub(super) fn region_argument(
    py: Python<'_>,
    value: Option<&Bound<'_, PyAny>>,
    method: &'static str,
) -> PyResult<RecordValue> {
    let Some(value) = value else {
        return Ok(RecordValue::default());
    };
    if let Ok(record) = value.cast::<PyDict>() {
        return casa_record(py, Some(record));
    }
    if let Ok(text) = value.cast::<PyString>() {
        return if text.to_str()?.is_empty() {
            Ok(RecordValue::default())
        } else {
            Err(stub(method).unwrap_err())
        };
    }
    if is_boolean_sequence(value)? {
        return Ok(RecordValue::default());
    }
    let variant = value.extract::<Variant>()?;
    let kind = match variant {
        Variant::Bool(_) => "bool",
        Variant::Number(_) => "numeric",
        Variant::Texts(_) => "stringvec",
        _ => "variant",
    };
    Err(raised(format!("Unsupported type for region {kind}")))
}

pub(super) fn read_region_argument(
    py: Python<'_>,
    value: Option<&Bound<'_, PyAny>>,
    method: &'static str,
) -> PyResult<Option<Region>> {
    if value.is_some_and(|value| value.is_instance_of::<PyString>()) {
        return Region::capture(py, value);
    }
    let record = region_argument(py, value, method)?;
    Ok((!record.is_empty()).then_some(Region::Record(record)))
}

fn mask_argument(value: Option<&Bound<'_, PyAny>>) -> PyResult<String> {
    let Some(value) = value else {
        return Ok(String::new());
    };
    if let Ok(text) = value.cast::<PyString>() {
        return Ok(text.to_str()?.to_owned());
    }
    if is_boolean_sequence(value)? {
        return Ok(String::new());
    }
    Err(raised("Mask is not understood, try a valid LEL string"))
}

#[pymethods]
impl PyImageTool {
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (region=None, axes=None, mask=None, list=false, dropdeg=false, getmask=false, stretch=false), text_signature = "($self, region={}, axes=[-1], mask='', list=False, dropdeg=False, getmask=False, stretch=False)")]
    fn getregion(
        &self,
        py: Python<'_>,
        region: Option<&Bound<'_, PyAny>>,
        axes: Option<IntList>,
        mask: Option<&Bound<'_, PyAny>>,
        list: bool,
        dropdeg: bool,
        getmask: bool,
        stretch: bool,
    ) -> PyResult<Py<PyAny>> {
        let region = read_region_argument(py, region, "getregion");
        let mask = mask_argument(mask);
        let axes = axes.map_or_else(Vec::new, |arg| arg.0);
        let (value, logs) = py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok((
                    CasaValue::Bools {
                        shape: vec![0],
                        values: Vec::new(),
                    },
                    Vec::new(),
                ));
            };
            let region = region?;
            let mask = mask?;
            let (data, logs) = read_region_casa(
                attachment,
                region.as_ref().map(Region::as_region),
                &axes,
                &mask,
                list,
                dropdeg,
                getmask,
                stretch,
            )
            .map_err(raised)?;
            Ok::<_, PyErr>((chunk_data_to_casa_value(data), logs))
        })?;
        emit_tool_logs(py, &logs)?;
        value.into_python(py)
    }
}
