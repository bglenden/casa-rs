// SPDX-License-Identifier: LGPL-3.0-or-later
//! Attach externally supplied CASA ephemeris tables to FIELD.

use std::path::Path;

use casa_ms::{ToolEphemeris, add_tool_ephemeris};
use casa_tables::{Table, TableOptions};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;

use super::selection_indices::selection_text;
use super::{PyMsTool, runtime_error};
use crate::convert::selector::listing_selector;
use crate::tools::casacore_path::expand_path;

#[pymethods]
impl PyMsTool {
    #[pyo3(signature = (id = -1, ephemerisname = None, comment = None, field = None), text_signature = "($self, id=-1, ephemerisname='', comment='', field=[])")]
    fn addephemeris(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = id_argument)] id: i64,
        ephemerisname: Option<&Bound<'_, PyAny>>,
        comment: Option<&Bound<'_, PyAny>>,
        field: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        let name = ephemerisname
            .map(|value| selection_text(value, "ephemerisname"))
            .transpose()?
            .unwrap_or_default();
        let comment = comment
            .map(|value| selection_text(value, "comment"))
            .transpose()?
            .unwrap_or_default();
        let mut field = listing_selector(field)?;
        if field == "-1" {
            field.clear();
        }
        py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            let Some(ms) = attached.as_mut() else {
                return Ok(false);
            };
            let link = ms.working_metadata_link();
            link.verify_directory().map_err(runtime_error)?;
            let expanded = expand_path(&name)
                .ok_or_else(|| runtime_error("Path::expandName: recursive environment variable"))?;
            let current = ms.visibility.current_main(&ms.path)?;
            let first = if ms.is_multi_ms {
                ms.sources
                    .first()
                    .map(|path| Table::open(TableOptions::new(path)))
                    .transpose()
                    .map_err(runtime_error)?
            } else {
                None
            };
            let main = first.as_ref().unwrap_or(&current.table);
            let request = ToolEphemeris {
                id,
                source: Path::new(expanded.as_ref()),
                source_name: &name,
                comment: &comment,
                field: &field,
                writable: ms.writable,
            };
            let attached =
                add_tool_ephemeris(main, &request, &mut ms.write_locks, &mut ms.read_locks)
                    .map_err(runtime_error)?;
            if attached {
                ms.subtables = Default::default();
                link.invalidate_view().map_err(runtime_error)?;
                if let Some(view) = &ms.exact_time_metadata {
                    view.invalidate_view().map_err(runtime_error)?;
                }
            }
            Ok(attached)
        })
    }
}

fn id_argument(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    value
        .extract()
        .map_err(|_| PyTypeError::new_err("argument _id must be an integer"))
}
