// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.imageconcat` binding delegating domain staging and concatenation to `casa_imageanalysis`.

use std::path::{Path, PathBuf};

use casa_imageanalysis::tool::concatenate::concatenate_casa_images;
use numpy::{PyArrayDescrMethods, PyUntypedArray, PyUntypedArrayMethods};
use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};

use crate::convert::Variant;

use super::PyImageTool;
use super::handle::{emit_tool_logs, external_open_check, raised};
use super::lifecycle::resolve_image_path;

fn filename_argument(value: Option<&Bound<'_, PyAny>>) -> PyResult<Variant> {
    let Some(value) = value else {
        return Ok(Variant::Empty);
    };
    if let Ok(array) = value.cast::<PyUntypedArray>() {
        return match array.dtype().kind() {
            b'b' | b'O' => Ok(Variant::Empty),
            b'U' if array.shape().iter().filter(|&&length| length > 1).count() > 1 => {
                Ok(Variant::Empty)
            }
            b'S' | b'U' => flatten_filenames(value),
            _ => value.extract(),
        };
    }
    let captured: Variant = value.extract()?;
    if matches!(captured, Variant::Other(_))
        && (value.is_instance_of::<PyList>() || value.is_instance_of::<PyTuple>())
    {
        let array = value
            .py()
            .import("numpy")?
            .call_method1("asarray", (value, "object"))?;
        return flatten_filenames(&array);
    }
    Ok(captured)
}

fn flatten_filenames(array: &Bound<'_, PyAny>) -> PyResult<Variant> {
    let flattened = array
        .call_method1("ravel", ("F",))?
        .call_method0("tolist")?;
    let mut names = Vec::new();
    for item in flattened.try_iter()? {
        match item?.extract()? {
            Variant::Text(name) => names.push(name),
            _ => return Ok(Variant::Empty),
        }
    }
    Ok(Variant::Texts(names))
}

fn filenames(py: Python<'_>, files: &Variant) -> PyResult<Vec<PathBuf>> {
    let names = match files {
        Variant::Empty => Vec::new(),
        Variant::Text(text) => {
            let delimiter = if text.contains(',') { ',' } else { ' ' };
            text.split(delimiter)
                .filter(|word| !word.is_empty())
                .map(str::to_owned)
                .collect()
        }
        Variant::Texts(names) => names.clone(),
        _ => return Err(raised("Exception: Unrecognized infiles datatype.")),
    };
    let glob = py.import("glob")?;
    let path_module = py.import("os.path")?;
    let mut result = Vec::new();
    for name in names {
        let name = path_module.call_method1("expandvars", (name,))?;
        let pattern: String = path_module.call_method1("expanduser", (name,))?.extract()?;
        if let Some(parent) = Path::new(&pattern)
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            && !parent.to_string_lossy().contains(['*', '?', '['])
            && !parent.is_dir()
        {
            return Err(raised(format!(
                "DirectoryIterator: error on directory {}: No such file or directory",
                parent.display()
            )));
        }
        let matches: Vec<String> = glob.call_method1("glob", (pattern,))?.extract()?;
        for entry in matches {
            result.push(std::path::absolute(entry).map_err(raised)?);
        }
    }
    if result.len() < 2 {
        return Err(raised(
            "Exception: You must provide at least two images to concatentate.",
        ));
    }
    Ok(result)
}

#[pymethods]
impl PyImageTool {
    /// Combine images along one axis and return the resulting image tool.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (outfile="", infiles=None, axis=-1, relax=false, tempclose=true, overwrite=false, reorder=false, mode="paged"), text_signature = "($self, outfile='', infiles=[], axis=-1, relax=False, tempclose=True, overwrite=False, reorder=False, mode='paged')")]
    fn imageconcat(
        &self,
        py: Python<'_>,
        outfile: &str,
        infiles: Option<&Bound<'_, PyAny>>,
        axis: i64,
        relax: bool,
        tempclose: bool,
        overwrite: bool,
        reorder: bool,
        mode: &str,
    ) -> PyResult<Py<PyImageTool>> {
        let files = filename_argument(infiles)?;
        let paths = filenames(py, &files)?;
        let mode_letter = mode.as_bytes().first().map(u8::to_ascii_lowercase);
        let expanded = if mode_letter != Some(b'p') {
            resolve_image_path(py, outfile)?
        } else {
            std::borrow::Cow::Borrowed(outfile)
        };
        let files_history = match &files {
            Variant::Text(text) => format!("'{text}'"),
            files => files.describe(),
        };
        let (result, logs) = py.detach(|| {
            let history = self.state().history;
            concatenate_casa_images(
                outfile,
                expanded.as_ref(),
                &paths,
                &files_history,
                axis,
                relax,
                tempclose,
                overwrite,
                reorder,
                mode,
                history,
                external_open_check(),
            )
            .map_err(raised)
        })?;
        emit_tool_logs(py, &logs)?;
        Py::new(py, Self::attached(result))
    }
}
