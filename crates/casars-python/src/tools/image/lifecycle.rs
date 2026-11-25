// SPDX-License-Identifier: LGPL-3.0-or-later

use std::borrow::Cow;
use std::path::{Component, Path, PathBuf};

use casa_images::ImagePixelType;
use casa_tables::{expand_casacore_path, lexical_absolute};
use numpy::{IntoPyArray, PyArray1};
use pyo3::prelude::*;

use crate::tools::{LogLine, emit};

use super::PyImageTool;
use super::attachment::{self, AttachedImage};
use super::handle::raised;

pub(super) fn resolve_image_path<'a>(py: Python<'_>, name: &'a str) -> PyResult<Cow<'a, str>> {
    let expanded = expand_casacore_path(name)
        .ok_or_else(|| raised("Path::expandName: recursive environment variable"))?;
    if expanded
        .strip_prefix('~')
        .is_some_and(|suffix| !suffix.is_empty() && !suffix.starts_with('/'))
    {
        let resolved = py
            .import("os.path")?
            .call_method1("expanduser", (expanded.as_ref(),))?
            .extract()?;
        return Ok(Cow::Owned(resolved));
    }
    Ok(expanded)
}

pub(super) fn rename_destination(expanded: &str, name: &str) -> PyResult<PathBuf> {
    let destination = std::path::absolute(expanded).map_err(raised)?;
    if destination.components().next_back() == Some(Component::ParentDir) {
        return Err(raised(format!(
            "Cannot rename to {name} because its path ends with '..'"
        )));
    }
    let lexical = lexical_absolute(&destination);
    if destination != lexical
        && let Some(physical_parent) = destination
            .parent()
            .and_then(|path| path.canonicalize().ok())
        && lexical
            .parent()
            .and_then(|path| path.canonicalize().ok())
            .as_ref()
            != Some(&physical_parent)
    {
        return Err(raised(format!(
            "Cannot rename to {name} because its path resolves to a different directory after removing '..'"
        )));
    }
    Ok(destination)
}

pub(super) fn open_image(infile: &str, expanded: &str) -> PyResult<AttachedImage> {
    open_image_with_cache(infile, expanded, true)
}

fn open_image_with_cache(
    infile: &str,
    expanded: &str,
    component_cache: bool,
) -> PyResult<AttachedImage> {
    if infile.is_empty() {
        return Err(raised("Exception: File name is empty."));
    }
    let path = Path::new(expanded);
    if !path.exists() {
        return Err(raised(format!("Exception: File {infile} does not exist..")));
    }
    let path = std::path::absolute(path).map_err(raised)?;
    let absolute = lexical_absolute(&path);
    AttachedImage::open_resolved_with_cache(&absolute, &path, Path::new(infile), component_cache)
}

#[pymethods]
impl PyImageTool {
    #[pyo3(signature = (infile, cache = true), text_signature = "($self, infile, cache=True)")]
    fn open(&self, py: Python<'_>, infile: String, cache: bool) -> PyResult<bool> {
        let expanded = resolve_image_path(py, &infile);
        py.detach(|| {
            let mut state = self.state();
            state.image = None;
            state.image = Some(open_image_with_cache(&infile, expanded?.as_ref(), cache)?);
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
        py.detach(|| self.state().image = None);
        true
    }

    #[pyo3(signature = (remove = false, verbose = true), text_signature = "($self, remove=False, verbose=True)")]
    fn done(&self, py: Python<'_>, remove: bool, verbose: bool) -> PyResult<bool> {
        let removed = py.detach(|| {
            let image = self.state().image.take();
            if remove && let Some(image) = image {
                image.remove().map(Some)
            } else {
                Ok(None)
            }
        })?;
        if let Some(name) = removed {
            report_removed(py, &name, verbose)?;
        }
        Ok(true)
    }

    #[pyo3(signature = (done = false, verbose = true), text_signature = "($self, done=False, verbose=True)")]
    fn remove(&self, py: Python<'_>, done: bool, verbose: bool) -> PyResult<bool> {
        // Both CASA forms detach the tool before attempting the deletion.
        let _ = done;
        let removed = py.detach(|| {
            self.state()
                .image
                .take()
                .map(|image| image.remove())
                .transpose()
        })?;
        if let Some(name) = removed {
            report_removed(py, &name, verbose)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn removefile(&self, py: Python<'_>, file: &str) -> PyResult<bool> {
        let removed = py.detach(|| attachment::remove_file(Path::new(file)));
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
        let expanded = resolve_image_path(py, name);
        py.detach(|| {
            let mut state = self.state();
            let Some(current) = state.image.as_ref() else {
                return Ok(false);
            };
            current.lock().check_single_precision("rename")?;
            let current = state.image.take().expect("an attached image");
            state.image = Some(current.rename(name, expanded, overwrite, state.history)?);
            Ok(true)
        })
    }

    fn isopen(&self, py: Python<'_>) -> bool {
        py.detach(|| self.state().image.is_some())
    }

    fn ispersistent(&self, py: Python<'_>) -> PyResult<bool> {
        py.detach(|| {
            self.state()
                .image
                .as_ref()
                .map_or(Ok(false), AttachedImage::is_persistent)
        })
    }

    #[pyo3(signature = (strippath = false), text_signature = "($self, strippath=False)")]
    fn name(&self, py: Python<'_>, strippath: bool) -> PyResult<String> {
        py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok("none".into());
            };
            let image = attachment.lock();
            image.check_single_precision("name")?;
            let Some(path) = attachment.name_path(&image) else {
                let name = match image.image_type_name() {
                    "ImageExpr" => "",
                    "ComponentListImage" => "Temporary ComponentListImage",
                    _ => "Temporary_Image",
                };
                return Ok(name.into());
            };
            if strippath {
                Ok(path
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .into_owned())
            } else {
                let path = std::path::absolute(path).map_err(raised)?;
                Ok(path.to_string_lossy().into_owned())
            }
        })
    }

    fn shape<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray1<i64>> {
        py.detach(|| {
            self.state().image.as_ref().map_or_else(Vec::new, |image| {
                image
                    .lock()
                    .shape()
                    .iter()
                    .map(|&size| size as i64)
                    .collect()
            })
        })
        .into_pyarray(py)
    }

    fn pixeltype(&self, py: Python<'_>) -> &'static str {
        py.detach(
            || match self.state().image.as_ref().map(|image| image.lock().kind()) {
                Some(ImagePixelType::Float32) => "float",
                Some(ImagePixelType::Float64) => "double",
                Some(ImagePixelType::Complex32) => "complex",
                Some(ImagePixelType::Complex64) => "dcomplex",
                None => "",
            },
        )
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
