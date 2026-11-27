// SPDX-License-Identifier: LGPL-3.0-or-later
//! Independent persisted outputs of a legacy MS TaQL selection.

use std::path::Path;

use casa_mstransform::tool_sort::{check_named_output_path, write_selected_measurement_set_rows};
use casa_tables::publication::{check_write_access, lock_table_tree_for_replacement};
use casa_tables::taql::ast::{GivingClause, Literal};
use casa_tables::{EndianFormat, Table};
use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::*;

use super::sort::SortedDestination;
use super::{AttachedMs, runtime_error};

pub(super) struct QueryOutputOptions {
    pub(super) independent: bool,
    pub(super) overwrite: bool,
    endian: EndianFormat,
}

impl QueryOutputOptions {
    pub(super) fn parse(giving: &GivingClause) -> PyResult<Self> {
        let mut output = Self {
            independent: false,
            overwrite: true,
            endian: EndianFormat::LocalEndian,
        };
        if let Some(mode) = &giving.output_type {
            output.set_mode(mode)?;
        }
        if let Some(options) = &giving.output_options {
            for (name, value) in options {
                output.set(name, value)?;
            }
        }
        if giving.table_name.is_empty() {
            return Err(runtime_error("TaQL output must name a MeasurementSet"));
        }
        if giving.table_name.contains("::") {
            return Err(PyNotImplementedError::new_err(
                "ms.selecttaqlold subtable GIVING is not implemented in casa-rs (#1030)",
            ));
        }
        Ok(output)
    }

    fn set_mode(&mut self, mode: &str) -> PyResult<()> {
        match mode.to_ascii_lowercase().as_str() {
            "plain" | "scratch" => (),
            "plain_local" => self.endian = EndianFormat::LocalEndian,
            "plain_big" => self.endian = EndianFormat::BigEndian,
            "plain_little" => self.endian = EndianFormat::LittleEndian,
            "memory" => {
                return Err(PyNotImplementedError::new_err(
                    "ms.selecttaqlold GIVING AS MEMORY is not implemented in casa-rs (#1030)",
                ));
            }
            _ => {
                return Err(runtime_error(format!(
                    "{mode} is an invalid TaQL output type"
                )));
            }
        }
        self.independent = true;
        Ok(())
    }

    fn set(&mut self, name: &str, value: &Literal) -> PyResult<()> {
        let key = name.to_ascii_lowercase();
        match (key.as_str(), value) {
            ("type", Literal::String(mode)) => match mode.to_ascii_lowercase().as_str() {
                "plain" | "scratch" | "memory" => self.set_mode(mode),
                _ => Err(runtime_error(
                    "TaQL output type must be plain, scratch or memory",
                )),
            },
            ("endian", Literal::String(endian)) => {
                self.endian = match endian.to_ascii_lowercase().as_str() {
                    "local" | "aipsrc" => EndianFormat::LocalEndian,
                    "big" => EndianFormat::BigEndian,
                    "little" => EndianFormat::LittleEndian,
                    _ => return Err(runtime_error("Invalid TaQL output byte order")),
                };
                Ok(())
            }
            ("overwrite", Literal::Bool(overwrite)) => {
                self.overwrite = *overwrite;
                Ok(())
            }
            ("storage", Literal::String(storage)) => match storage.to_ascii_lowercase().as_str() {
                "sepfile" | "default" | "aipsrc" => Ok(()),
                "multifile" => Err(PyNotImplementedError::new_err(
                    "ms.selecttaqlold MultiFile output is not implemented in casa-rs (#1030)",
                )),
                "multihdf5" => Err(runtime_error("TaQL MultiHDF5 storage is unavailable")),
                _ => Err(runtime_error("Invalid TaQL output storage option")),
            },
            ("blocksize", Literal::Int(size)) if i32::try_from(*size).is_ok() => Ok(()),
            (_, Literal::Bool(true)) => self.set_mode(&key),
            _ => Err(runtime_error(format!("Invalid TaQL output option {name}"))),
        }
    }
}

impl AttachedMs {
    pub(super) fn save_typed_legacy_query(
        &self,
        table: &Table,
        rows: &[usize],
        output: &Path,
        options: QueryOutputOptions,
        original: &Path,
    ) -> PyResult<Self> {
        let name = output
            .file_name()
            .ok_or_else(|| runtime_error("TaQL output must name a MeasurementSet"))?;
        let parent = output
            .parent()
            .ok_or_else(|| runtime_error("TaQL output has no parent directory"))?
            .canonicalize()
            .map_err(runtime_error)?;
        let output = parent.join(name);
        let current = self.path.as_path();
        for input in std::iter::once(current).chain((original != current).then_some(original)) {
            if input.starts_with(&output) {
                return Err(runtime_error(
                    "cannot overwrite the attached MeasurementSet with a named copy",
                ));
            }
            check_named_output_path(input, &output).map_err(runtime_error)?;
        }
        let destination = SortedDestination::inspect(&output, "TaQL selection")?;
        if !options.overwrite && !matches!(destination, SortedDestination::Absent) {
            return Err(runtime_error("TaQL output already exists"));
        }
        let stage = tempfile::Builder::new()
            .prefix(".taql-ms-")
            .tempdir_in(&parent)
            .map_err(runtime_error)?;
        let staged = stage.path().join("selected.ms");
        let source_locks = self
            .read_locks
            .keys()
            .chain(self.write_locks.keys())
            .cloned()
            .collect();
        write_selected_measurement_set_rows(
            table,
            rows,
            stage.path(),
            &staged,
            options.endian,
            &source_locks,
        )
        .map_err(runtime_error)?;
        self.working_metadata_link()
            .verify_directory()
            .map_err(runtime_error)?;
        {
            let _publication = crate::tools::table::lock_attachment_publication();
            let _locks = if destination.is_table() {
                if crate::tools::table::replacement_target_is_in_use(&output) {
                    return Err(runtime_error("TaQL output is still open in this process"));
                }
                check_write_access(&output).map_err(runtime_error)?;
                lock_table_tree_for_replacement(&output).map_err(runtime_error)?
            } else {
                Vec::new()
            };
            destination.publish(&staged, &output, "TaQL selection")?;
        }
        Self::open(output, false, false, false)
    }
}
