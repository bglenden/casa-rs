// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 image lifecycle wrappers delegating to [`casa_imageanalysis::tool::lifecycle`].

use std::borrow::Cow;
use std::path::Path;

use casa_imageanalysis::tool::attachment::{AttachedImage, remove_file};
use casa_imageanalysis::tool::lifecycle::ImageToolState;
use numpy::{IntoPyArray, PyArray1};
use pyo3::prelude::*;

use crate::tools::casacore_path::expand_path;
use crate::tools::{LogLine, emit};

use super::PyImageTool;
use super::handle::{external_open_check, raised};

pub(super) fn resolve_image_path_result<'a>(
    py: Python<'_>,
    name: &'a str,
) -> Result<Cow<'a, str>, String> {
    let expanded = expand_path(name)
        .ok_or_else(|| "Path::expandName: recursive environment variable".to_owned())?;
    if expanded
        .strip_prefix('~')
        .is_some_and(|suffix| !suffix.is_empty() && !suffix.starts_with('/'))
    {
        let resolved = py
            .import("os.path")
            .and_then(|m| m.call_method1("expanduser", (expanded.as_ref(),)))
            .and_then(|v| v.extract())
            .map_err(|e| e.to_string())?;
        return Ok(Cow::Owned(resolved));
    }
    Ok(expanded)
}

pub(super) fn resolve_image_path<'a>(py: Python<'_>, name: &'a str) -> PyResult<Cow<'a, str>> {
    resolve_image_path_result(py, name).map_err(raised)
}

pub(super) fn open_image(infile: &str, expanded: &str) -> PyResult<AttachedImage> {
    let mut state = ImageToolState::new();
    state
        .open_expanded(infile, expanded, true)
        .map_err(raised)?;
    Ok(state.detach().expect("opened image"))
}

#[pymethods]
impl PyImageTool {
    #[pyo3(signature = (infile, cache = true), text_signature = "($self, infile, cache=True)")]
    fn open(&self, py: Python<'_>, infile: String, cache: bool) -> PyResult<bool> {
        let expanded = resolve_image_path(py, &infile);
        py.detach(|| {
            let mut state = self.state();
            state.image = None;
            state
                .open_expanded(&infile, expanded?.as_ref(), cache)
                .map_err(raised)?;
            Ok(true)
        })
    }

    fn newimage(&self, py: Python<'_>, infile: &str) -> PyResult<Py<Self>> {
        self.newimagefromfile(py, infile)
    }

    fn newimagefromfile(&self, py: Python<'_>, infile: &str) -> PyResult<Py<Self>> {
        let expanded = resolve_image_path(py, infile)?;
        let image = py.detach(|| open_image(infile, expanded.as_ref()))?;
        Py::new(py, Self::attached(image))
    }

    fn close(&self, py: Python<'_>) -> bool {
        py.detach(|| {
            let _ = self.state().detach();
        });
        true
    }

    #[pyo3(signature = (remove = false, verbose = true), text_signature = "($self, remove=False, verbose=True)")]
    fn done(&self, py: Python<'_>, remove: bool, verbose: bool) -> PyResult<bool> {
        let removed = py.detach(|| {
            self.state()
                .done_casa(remove, external_open_check())
                .map_err(raised)
        })?;
        if let Some(name) = removed {
            report_removed(py, &name, verbose)?;
        }
        Ok(true)
    }

    #[pyo3(signature = (done = false, verbose = true), text_signature = "($self, done=False, verbose=True)")]
    fn remove(&self, py: Python<'_>, done: bool, verbose: bool) -> PyResult<bool> {
        let _ = done;
        let removed = py.detach(|| {
            self.state()
                .remove_casa(external_open_check())
                .map_err(raised)
        })?;
        if let Some(name) = removed {
            report_removed(py, &name, verbose)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn removefile(&self, py: Python<'_>, file: &str) -> PyResult<bool> {
        let removed = py.detach(|| remove_file(Path::new(file), external_open_check()));
        match removed {
            Ok(true) => Ok(true),
            Ok(false) => {
                let warning = if file.is_empty() {
                    "Empty filename".to_owned()
                } else {
                    format!("{file} does not exist or is not a table.")
                };
                emit(py, "casars.tools.image", &[LogLine::warning(warning)])?;
                Ok(false)
            }
            Err(error) => {
                emit(
                    py,
                    "casars.tools.image",
                    &[LogLine::warning(error.to_string())],
                )?;
                Ok(false)
            }
        }
    }

    #[pyo3(signature = (name, overwrite=false), text_signature = "($self, name, overwrite=False)")]
    fn rename(&self, py: Python<'_>, name: &str, overwrite: bool) -> PyResult<bool> {
        let expanded = resolve_image_path_result(py, name).map(|cow| cow.into_owned());
        py.detach(|| {
            self.state()
                .rename_casa(
                    name,
                    expanded.map_err(casa_imageanalysis::tool::ImageToolError::new),
                    overwrite,
                    external_open_check(),
                )
                .map_err(raised)
        })
    }

    fn isopen(&self, py: Python<'_>) -> bool {
        py.detach(|| self.state().is_open())
    }

    fn ispersistent(&self, py: Python<'_>) -> PyResult<bool> {
        py.detach(|| self.state().is_persistent().map_err(raised))
    }

    #[pyo3(signature = (strippath = false), text_signature = "($self, strippath=False)")]
    fn name(&self, py: Python<'_>, strippath: bool) -> PyResult<String> {
        py.detach(|| self.state().casa_name(strippath).map_err(raised))
    }

    fn shape<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray1<i64>> {
        py.detach(|| self.state().shape_i64()).into_pyarray(py)
    }

    fn pixeltype(&self, py: Python<'_>) -> &'static str {
        py.detach(|| self.state().pixel_type_casa())
    }

    fn r#type(&self) -> &'static str {
        "image"
    }

    #[pyo3(signature = (enable = true), text_signature = "($self, enable=True)")]
    fn dohistory(&self, py: Python<'_>, enable: bool) -> bool {
        py.detach(|| self.state().history = enable);
        true
    }
}

fn report_removed(py: Python<'_>, name: &str, verbose: bool) -> PyResult<()> {
    let message = format!("deleted table {name}");
    let log = if verbose {
        LogLine::info(message)
    } else {
        LogLine::debug(message)
    };
    emit(py, "casars.tools.image", &[log])
}
