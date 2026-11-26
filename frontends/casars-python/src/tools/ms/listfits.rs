// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `ms` tool's attachment-independent FITS listing.

use std::ffi::CString;

use casa_images::fits_table::listing::{FitsListingMessage, list_fits};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyBytesMethods, PyString, PyStringMethods};

use super::PyMsTool;
use crate::tools::{LogLine, emit};

#[pymethods]
impl PyMsTool {
    /// Log the header and a data preview for each HDU in the FITS file.
    /// File errors are reported in the log. This does not need an open MS.
    #[pyo3(signature = (fitsfile), text_signature = "($self, fitsfile)")]
    fn listfits(&self, py: Python<'_>, fitsfile: &Bound<'_, PyAny>) -> PyResult<bool> {
        let path = if let Ok(text) = fitsfile.cast::<PyString>() {
            text.to_str()
                .map_err(|_| PyTypeError::new_err("argument 1_name could not be encoded as UTF-8"))?
                .as_bytes()
        } else {
            fitsfile
                .cast::<PyBytes>()
                .map_err(|_| PyTypeError::new_err("argument _fitsfile must be a string"))?
                .as_bytes()
        };
        // The CASA binding passes the supplied string to a C filename parameter.
        let path = CString::new(path.split(|&byte| byte == 0).next().unwrap_or_default())
            .expect("the filename was split on NUL bytes");
        let messages = py.detach(|| list_fits(&path));
        let lines = messages
            .into_iter()
            .map(|message| match message {
                FitsListingMessage::Info(text) => LogLine::info(text),
                FitsListingMessage::Debug(text) => LogLine::debug(text),
                FitsListingMessage::Warning(text) => LogLine::warning(text),
                FitsListingMessage::Error(text) => LogLine::error(text),
            })
            .collect::<Vec<_>>();
        emit(py, "casars.tools.ms", &lines)?;
        Ok(true)
    }
}
