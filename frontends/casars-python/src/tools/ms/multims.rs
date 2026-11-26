// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use casa_ms::{MsError, tool_multims::create_multi_ms_then};
use casa_tables::table_cache_name;
use pyo3::prelude::*;

use super::selection_indices::selection_text;
use super::visibility::requested_strings;
use super::{AttachedMs, PyMsTool, runtime_error, strict_bool};

#[pymethods]
impl PyMsTool {
    /// Move the member MeasurementSets into a virtual concatenation.
    #[allow(non_snake_case, clippy::too_many_arguments)]
    #[pyo3(signature = (outputTableName, tables, subtables, nomodify = true, lock = false, copysubtables = false, omitsubtables = None), text_signature = "($self, outputTableName, tables, subtables, nomodify=True, lock=False, copysubtables=False, omitsubtables=[])")]
    fn createmultims(
        &self,
        py: Python<'_>,
        outputTableName: &Bound<'_, PyAny>,
        tables: &Bound<'_, PyAny>,
        subtables: &Bound<'_, PyAny>,
        #[pyo3(from_py_with = strict_bool)] nomodify: bool,
        #[pyo3(from_py_with = strict_bool)] lock: bool,
        #[pyo3(from_py_with = strict_bool)] copysubtables: bool,
        omitsubtables: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        let name = selection_text(outputTableName, "outputTableName")?;
        let tables = requested_strings(Some(tables), "tables")?;
        let subtables = requested_strings(Some(subtables), "subtables")?;
        let omitted = requested_strings(omitsubtables, "omitsubtables")?;
        py.detach(|| {
            let mut attached = self.attached.lock().map_err(runtime_error)?;
            let output = table_cache_name(Path::new(&name)).map_err(runtime_error)?;
            let Ok(inputs) = tables
                .iter()
                .map(|name| table_cache_name(Path::new(name)))
                .collect::<Result<Vec<_>, _>>()
            else {
                return Ok(false);
            };
            let released = attached
                .as_mut()
                .map(|ms| release_member_locks(ms, &output, &inputs));
            match create_multi_ms_then(
                &output,
                &inputs,
                &subtables,
                copysubtables.then_some(&omitted),
                || {
                    AttachedMs::open(output.clone(), !nomodify, lock, false)
                        .map_err(|error| MsError::MultiMsAttachment(error.to_string()))
                },
            ) {
                Ok(replacement) => {
                    *attached = Some(replacement);
                    Ok(true)
                }
                Err(error) => {
                    if matches!(error, MsError::MultiMsRecovery { .. }) {
                        return Err(runtime_error(error));
                    }
                    if let Some((ms, released)) = attached.as_mut().zip(released) {
                        ms.retain_tables(released)?;
                    }
                    if matches!(error, MsError::MultiMsAttachment(_)) {
                        return Err(runtime_error(error));
                    }
                    Ok(false)
                }
            }
        })
    }
}

fn release_member_locks(
    ms: &mut AttachedMs,
    output: &Path,
    inputs: &[PathBuf],
) -> BTreeSet<PathBuf> {
    let paths = std::iter::once(output)
        .chain(inputs.iter().map(PathBuf::as_path))
        .filter_map(|path| path.canonicalize().ok())
        .collect::<Vec<_>>();
    let released = ms
        .read_locks
        .keys()
        .chain(ms.write_locks.keys())
        .filter(|key| paths.iter().any(|path| key.starts_with(path)))
        .cloned()
        .collect::<BTreeSet<_>>();
    for key in &released {
        ms.read_locks.remove(key);
        ms.write_locks.remove(key);
    }
    released
}
