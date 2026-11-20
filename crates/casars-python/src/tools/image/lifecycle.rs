// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_images::ImagePixelType;
use numpy::{IntoPyArray, PyArray1};
use pyo3::prelude::*;

use crate::tools::{LogLine, emit};

use super::PyImageTool;
use super::attachment::{self, AttachedImage};
use super::handle::raised;

fn open_image(infile: &str) -> PyResult<AttachedImage> {
    if infile.is_empty() {
        return Err(raised("Exception: File name is empty."));
    }
    let path = Path::new(infile);
    if !path.exists() {
        return Err(raised(format!("Exception: File {infile} does not exist..")));
    }
    let path = std::path::absolute(path).map_err(raised)?;
    AttachedImage::open(&mut attachment::registry(), &path)
}

#[pymethods]
impl PyImageTool {
    #[pyo3(signature = (infile, cache = true), text_signature = "($self, infile, cache=True)")]
    fn open(&self, py: Python<'_>, infile: String, cache: bool) -> PyResult<bool> {
        py.detach(|| {
            let mut state = self.state();
            state.image = None;
            // ImageFactory's cache flag applies to component-list images.
            let _ = cache;
            state.image = Some(open_image(&infile)?);
            Ok(true)
        })
    }

    fn newimage(&self, py: Python<'_>, infile: &str) -> PyResult<Py<Self>> {
        self.newimagefromfile(py, infile)
    }

    fn newimagefromfile(&self, py: Python<'_>, infile: &str) -> PyResult<Py<Self>> {
        let image = py.detach(|| open_image(infile))?;
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
        py.detach(|| {
            let mut state = self.state();
            let Some(current) = state.image.as_ref() else {
                return Ok(false);
            };
            current.lock().check_single_precision("rename")?;
            let current = state.image.take().expect("an attached image");
            state.image = Some(current.rename(name, overwrite, state.history)?);
            Ok(true)
        })
    }

    fn isopen(&self, py: Python<'_>) -> bool {
        py.detach(|| self.state().image.is_some())
    }

    fn ispersistent(&self, py: Python<'_>) -> bool {
        py.detach(|| {
            self.state()
                .image
                .as_ref()
                .is_some_and(AttachedImage::is_persistent)
        })
    }

    #[pyo3(signature = (strippath = false), text_signature = "($self, strippath=False)")]
    fn name(&self, py: Python<'_>, strippath: bool) -> PyResult<String> {
        py.detach(|| {
            let state = self.state();
            let Some(image) = state.image.as_ref() else {
                return Ok("none".into());
            };
            let image = image.lock();
            image.check_single_precision("name")?;
            let Some(path) = image.path() else {
                return Ok("Temporary_Image".into());
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
