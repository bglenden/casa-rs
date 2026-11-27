// SPDX-License-Identifier: LGPL-3.0-or-later
//! The legacy selector's independent transient MeasurementSet.

use std::cell::Ref;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use casa_ms::{MeasurementSet, MsError, MsResult, SubtableId};
use casa_tables::{Table, TablePartNames};
use pyo3::prelude::*;

use super::subtable_queries::Subtables;
use super::{AttachedMs, runtime_error};

pub(super) enum LegacySubtables<'a> {
    Disk(HashMap<SubtableId, Ref<'a, [Table]>>),
    Memory(&'a MeasurementSet),
}

impl<'a> LegacySubtables<'a> {
    pub(super) fn read(
        disk: &'a Subtables,
        memory: Option<&'a MeasurementSet>,
        main: &Table,
        ids: &[SubtableId],
    ) -> MsResult<Self> {
        if let Some(memory) = memory {
            Ok(Self::Memory(memory))
        } else {
            Ok(Self::Disk(disk.read_range(main, ids)?))
        }
    }

    pub(super) fn tables(&self, id: SubtableId) -> MsResult<&[Table]> {
        match self {
            Self::Disk(tables) => tables.get(&id).map(|table| &**table),
            Self::Memory(ms) => ms.subtable(id).map(std::slice::from_ref),
        }
        .ok_or_else(|| MsError::MissingSubtable(id.name().to_owned()))
    }
}

impl AttachedMs {
    pub(super) fn new_legacy_memory(&self, rows: &[usize], original: &Path) -> PyResult<Self> {
        let (source, _) = self
            .working_metadata_link()
            .source_and_rows()
            .map_err(runtime_error)?;
        let held = self
            .read_locks
            .keys()
            .chain(self.write_locks.keys())
            .cloned()
            .collect();
        let memory = source
            .ms
            .to_memory_rows_with_read_locks(rows, &held)
            .map_err(runtime_error)?;
        let mut memory_source_paths =
            casa_mstransform::tool_sort::capture_named_output_inputs(&self.path)
                .map_err(runtime_error)?;
        if original != self.path {
            memory_source_paths.extend(
                casa_mstransform::tool_sort::capture_named_output_inputs(original)
                    .map_err(runtime_error)?,
            );
        }
        self.working_metadata_link()
            .verify_directory()
            .map_err(runtime_error)?;
        Ok(Self {
            name: self.name.clone(),
            path: self.path.clone(),
            directory: self.directory.clone(),
            main: Table::new(),
            main_files: super::attachment_info::TableFiles::new(&self.path),
            main_stamp: None,
            asdm_index: None,
            memory_ms: Some(memory),
            memory_source_paths,
            #[cfg(unix)]
            history_snapshot: None,
            subtables: Default::default(),
            writable: true,
            original_rows: rows.len(),
            plain_rows: None,
            part_names: TablePartNames::Plain(self.path.clone()),
            is_multi_ms: false,
            sources: Vec::new(),
            selected_rows: None,
            legacy_selection: Default::default(),
            selection_state: Default::default(),
            visibility: Default::default(),
            metadata: Default::default(),
            exact_time_metadata: None,
            write_locks: Default::default(),
            read_locks: Default::default(),
            _cached_paths: Arc::new(Vec::new()),
        })
    }

    pub(super) fn legacy_main(&self) -> &Table {
        self.memory_ms
            .as_ref()
            .map_or(&self.main, |ms| ms.main_table())
    }
}
