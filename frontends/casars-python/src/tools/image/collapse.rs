// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.collapse` wrapper delegating to [`casa_imageanalysis::tool::ops_transform::collapse_image`].

use casa_imageanalysis::analysis::SubimageRegion;
use casa_imageanalysis::tool::ops_transform::{CollapseAxes, CollapseRequest, collapse_image};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyList, PyString, PyTuple};

use super::PyImageTool;
use super::handle::{external_open_check, raised};
use super::subimage::{RegionArgument, require_bool};

fn capture_axes(value: Option<&Bound<'_, PyAny>>) -> PyResult<(CollapseAxes, String)> {
    let history = |axes: &CollapseAxes| -> String {
        match axes {
            CollapseAxes::Numbers(values) if value.is_some_and(|v| integer(v).is_some()) => {
                values[0].to_string()
            }
            CollapseAxes::Numbers(values) => format!(
                "[{}]",
                values
                    .iter()
                    .map(i64::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            CollapseAxes::Names(values)
                if value.is_some_and(|v| v.is_instance_of::<PyString>()) =>
            {
                format!("'{}'", values[0])
            }
            CollapseAxes::Names(values) => format!(
                "[{}]",
                values
                    .iter()
                    .map(|val| format!("{val:?}"))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            _ => "[]".into(),
        }
    };
    let Some(value) = value.filter(|v| !v.is_none()) else {
        return Ok((CollapseAxes::Missing, "[]".into()));
    };
    if let Ok(s) = value.cast::<PyString>() {
        let axes = CollapseAxes::Names(vec![s.to_str()?.to_owned()]);
        let h = history(&axes);
        return Ok((axes, h));
    }
    if let Some(index) = integer(value) {
        let axes = CollapseAxes::Numbers(vec![index]);
        let h = history(&axes);
        return Ok((axes, h));
    }
    let numpy_kind = dtype_kind(value);
    if value.is_instance_of::<PyList>() || value.is_instance_of::<PyTuple>() || numpy_kind.is_some()
    {
        let Ok(items) = value.try_iter() else {
            return Ok((CollapseAxes::Unsupported, "[]".into()));
        };
        let items = items.collect::<PyResult<Vec<_>>>()?;
        let axes = if items.is_empty() {
            if matches!(numpy_kind.as_deref(), Some("i" | "u")) {
                CollapseAxes::Numbers(Vec::new())
            } else {
                CollapseAxes::Missing
            }
        } else if items.iter().all(is_bool) {
            CollapseAxes::Missing
        } else if let Some(values) = items.iter().map(integer).collect() {
            CollapseAxes::Numbers(values)
        } else if let Ok(values) = items
            .iter()
            .map(|item| item.extract::<String>())
            .collect::<PyResult<Vec<_>>>()
        {
            CollapseAxes::Names(values)
        } else {
            CollapseAxes::Unsupported
        };
        let h = history(&axes);
        return Ok((axes, h));
    }
    Ok((CollapseAxes::Unsupported, "[]".into()))
}

fn dtype_kind(value: &Bound<'_, PyAny>) -> Option<String> {
    value
        .getattr("dtype")
        .ok()?
        .getattr("kind")
        .ok()?
        .extract()
        .ok()
}

fn is_bool(value: &Bound<'_, PyAny>) -> bool {
    value.is_instance_of::<PyBool>() || dtype_kind(value).as_deref() == Some("b")
}

fn integer(value: &Bound<'_, PyAny>) -> Option<i64> {
    if is_bool(value) {
        return None;
    }
    value.extract().ok()
}

#[pymethods]
impl PyImageTool {
    /// Reduce each selected image section to a statistic along the specified axes.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (function="", axes=None, outfile="", region=None, r#box="", chans="", stokes="", mask="", overwrite=false, stretch=false), text_signature = "($self, function='', axes=[], outfile='', region='', box='', chans='', stokes='', mask='', overwrite=False, stretch=False)")]
    fn collapse(
        &self,
        py: Python<'_>,
        function: &str,
        axes: Option<&Bound<'_, PyAny>>,
        outfile: &str,
        region: Option<RegionArgument>,
        r#box: &str,
        chans: &str,
        stokes: &str,
        mask: &str,
        #[pyo3(from_py_with = require_bool)] overwrite: bool,
        #[pyo3(from_py_with = require_bool)] stretch: bool,
    ) -> PyResult<Py<Self>> {
        let (captured, axes_history) = capture_axes(axes)?;
        let product = py.detach(|| -> PyResult<_> {
            let state = self.state();
            let Some(attached) = state.image.as_ref() else {
                return Ok(None);
            };
            if let Some(region_arg) = &region {
                region_arg.validate_selectors(r#box, chans, stokes)?;
            }
            let history_command = state.history.then(|| {
                let region_hist = match region.as_ref().and_then(RegionArgument::selection) {
                    Some(SubimageRegion::Text(text)) => format!("'{text}'"),
                    Some(SubimageRegion::Record(_)) => "{...}".into(),
                    None => region.as_ref().map_or_else(
                        || "''".into(),
                        |reg| {
                            let h = reg.history();
                            if h == "\"\"" { "''".into() } else { h }
                        },
                    ),
                };
                format!(
                    "ia.collapse(function='{function}', axes={axes_history}, outfile='{outfile}', region={region_hist}, box='{box}', chans='{chans}', stokes='{stokes}', mask='{mask}', overwrite={overwrite}, stretch={stretch})",
                    box = r#box
                )
            });
            let request = CollapseRequest {
                function,
                axes: captured,
                outfile,
                region: region.as_ref().and_then(RegionArgument::selection),
                spatial: r#box,
                chans,
                stokes,
                mask,
                overwrite,
                stretch,
                history_command,
            };
            collapse_image(attached, &request, external_open_check())
                .map(Some)
                .map_err(raised)
        })?;
        Py::new(py, product.map_or_else(Self::new, Self::attached))
    }
}
