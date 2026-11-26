// SPDX-License-Identifier: LGPL-3.0-or-later
//! Write complete model records under a fresh, jointly locked snapshot.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use casa_core::Value;
use casa_tables::{Table, TableWriteLock};

use crate::schema::SubtableId;
use crate::scratch_columns::{Write, change_in_place};
use crate::{MeasurementSet, MsResult};

use super::{
    ModelState, VirtualModelEntry, add_model, clear_model_selection, delete_images, invalid,
    record_images, virtual_model_target_for_field,
};

/// Check a planned placement against a fresh SOURCE snapshot under MAIN.
#[doc(hidden)]
pub fn verify_virtual_model_target_holding(
    ms: &mut MeasurementSet,
    lock: &mut TableWriteLock,
    first_field: i32,
    expected: &Path,
) -> MsResult<()> {
    with_model_locks(ms, Some(lock), |ms, _, _| {
        verify_target(ms, first_field, expected)
    })
}

fn verify_target(ms: &MeasurementSet, field: i32, expected: &Path) -> MsResult<()> {
    let current = virtual_model_target_for_field(ms, field)?
        .canonicalize()
        .map_err(|error| invalid(error.to_string()))?;
    if current != expected {
        return Err(invalid("the virtual model storage target changed"));
    }
    Ok(())
}

/// One attempted publication of all virtual entries for an independent target.
/// Keep this value when retrying the same operation with the same entries.
#[doc(hidden)]
#[derive(Default)]
pub struct VirtualModelPublication {
    stage: PublicationStage,
}

#[derive(Default)]
enum PublicationStage {
    #[default]
    Unwritten,
    // A storage error can leave a recipe readable even when save reports
    // failure. Retrying an increment with an unknown outcome is unsafe.
    Uncertain,
    MainCleanup {
        removed: Vec<(String, Value)>,
        retired_images: Vec<PathBuf>,
    },
    Published,
}

impl VirtualModelPublication {
    /// Whether a record can already refer to the supplied Fourier images.
    pub fn may_reference_images(&self) -> bool {
        !matches!(self.stage, PublicationStage::Unwritten)
    }

    /// Write one complete record (including all Taylor terms) while the
    /// caller holds MAIN. An interrupted MAIN cleanup can be resumed.
    pub fn publish_holding<'a>(
        &mut self,
        ms: &mut MeasurementSet,
        entries: impl IntoIterator<Item = (&'a VirtualModelEntry, bool)>,
        lock: &mut TableWriteLock,
        expected: (i32, &Path),
    ) -> MsResult<()> {
        self.publish(ms, entries, Some(lock), Some(expected), None)
    }

    /// Replace a field's selected windows, retaining its other windows.
    /// Stage the clear and all new entries under the same storage lock.
    pub fn replace_windows_holding<'a>(
        &mut self,
        ms: &mut MeasurementSet,
        entries: impl IntoIterator<Item = &'a VirtualModelEntry>,
        lock: &mut TableWriteLock,
        field: i32,
        spws: &[i32],
    ) -> MsResult<()> {
        self.publish(
            ms,
            entries.into_iter().map(|entry| (entry, true)),
            Some(lock),
            None,
            Some((field, spws)),
        )
    }

    /// Replace the windows only when the planned storage target still owns them.
    /// Verify the route and publish under the same locks.
    pub fn replace_windows_at_holding<'a>(
        &mut self,
        ms: &mut MeasurementSet,
        entries: impl IntoIterator<Item = &'a VirtualModelEntry>,
        lock: &mut TableWriteLock,
        field: i32,
        spws: &[i32],
        expected: &Path,
    ) -> MsResult<()> {
        self.publish(
            ms,
            entries.into_iter().map(|entry| (entry, true)),
            Some(lock),
            Some((field, expected)),
            Some((field, spws)),
        )
    }

    pub(super) fn publish<'a>(
        &mut self,
        ms: &mut MeasurementSet,
        entries: impl IntoIterator<Item = (&'a VirtualModelEntry, bool)>,
        lock: Option<&mut TableWriteLock>,
        expected: Option<(i32, &Path)>,
        replace: Option<(i32, &[i32])>,
    ) -> MsResult<()> {
        match self.stage {
            PublicationStage::Published => return Ok(()),
            PublicationStage::Uncertain => {
                return Err(invalid(
                    "the previous virtual-model write has an uncertain outcome",
                ));
            }
            _ => {}
        }
        let resuming_cleanup = matches!(self.stage, PublicationStage::MainCleanup { .. });
        with_model_locks(ms, lock, |ms, main, source| {
            if let Some((field, path)) = expected {
                verify_target(ms, field, path)?;
            }
            if resuming_cleanup && ms.main_table().path().is_some() {
                // The preceding save and its best-effort reload may both
                // have failed. Confirm the on-disk keywords before cleanup.
                ms.main_table_mut().resync()?;
            }
            if matches!(self.stage, PublicationStage::Unwritten) {
                let mut state = ModelState::read(ms)?;
                let mut retired_images = match replace {
                    Some((field, spws)) => clear_model_selection(ms, &mut state, &[field], spws)?,
                    None => Vec::new(),
                };
                for (entry, incremental) in entries {
                    retired_images.extend(add_model(ms, &mut state, entry, incremental)?);
                }
                if state.source.as_ref().is_some_and(|source| source.changed) {
                    let prior_main = ms.main_table().keywords();
                    let removed = prior_main
                        .fields()
                        .iter()
                        .filter(|field| state.main.get(&field.name).is_none())
                        .map(|field| (field.name.clone(), field.value.clone()))
                        .collect::<Vec<_>>();
                    if state.main_upserted {
                        return Err(invalid(
                            "one virtual model publication must target SOURCE or MAIN",
                        ));
                    }
                    state.write_source(ms, source, || {
                        self.stage = PublicationStage::Uncertain;
                    })?;
                    self.stage = PublicationStage::MainCleanup {
                        removed,
                        retired_images,
                    };
                } else {
                    self.stage = PublicationStage::Uncertain;
                    state.write_main(ms, main)?;
                    self.stage = PublicationStage::Published;
                    delete_unreferenced_images(ms, retired_images);
                    return Ok(());
                }
            }

            if let PublicationStage::MainCleanup {
                removed,
                retired_images,
            } = &self.stage
            {
                let table = ms.main_table_mut();
                if removed
                    .iter()
                    .any(|(name, value)| table.keywords().get(name) == Some(value))
                {
                    change_in_place(table, 0, Write::Metadata, main, |table| {
                        let keywords = table.keywords_mut();
                        for (name, value) in removed {
                            if keywords.get(name) == Some(value) {
                                keywords.remove(name);
                            }
                        }
                        Ok(())
                    })?;
                }
                delete_unreferenced_images(ms, retired_images.clone());
                self.stage = PublicationStage::Published;
            }
            Ok(())
        })
    }
}

fn delete_unreferenced_images(ms: &MeasurementSet, mut retired: Vec<PathBuf>) {
    // A SOURCE table has no reverse pointers to the MAINs that use it.
    // Another MAIN can still name a grid currently or formerly in SOURCE.
    // Only remove retired grids whose directory belongs to this MAIN.
    let Some(main) = ms
        .main_table()
        .path()
        .and_then(|path| path.canonicalize().ok())
    else {
        return;
    };
    retired.retain(|grid| {
        grid.parent()
            .is_some_and(|parent| parent.canonicalize().is_ok_and(|parent| parent == main))
    });
    if retired.is_empty() {
        return;
    }
    // A second field or a legacy MAIN record can still refer to an image
    // removed from the record just updated. An unreadable record leaves the
    // candidate grids intact.
    let referenced = (|| -> MsResult<BTreeSet<PathBuf>> {
        let mut state = ModelState::read(ms)?;
        let keys = state
            .main
            .fields()
            .iter()
            .chain(
                state
                    .source
                    .iter()
                    .flat_map(|source| source.keywords.fields()),
            )
            .filter(|field| field.name.starts_with("model_"))
            .map(|field| field.name.clone())
            .collect::<BTreeSet<_>>();
        let mut images = BTreeSet::new();
        for key in keys {
            images.extend(record_images(&state.record(ms, &key)?));
            if let Some(Value::Record(record)) = state.main.get(&key) {
                images.extend(record_images(record));
            }
        }
        Ok(images)
    })();
    if let Ok(referenced) = referenced {
        let referenced = referenced
            .iter()
            .filter_map(|path| path.canonicalize().ok())
            .collect::<BTreeSet<_>>();
        retired.retain(|path| {
            path.canonicalize()
                .is_ok_and(|path| !referenced.contains(&path))
        });
        retired.sort();
        retired.dedup();
        delete_images(ms, &retired);
    }
}

// MAIN fixes the identity of SOURCE and the selected fields. Keep SOURCE
// locked from its reread through record replacement and backing-image cleanup.
pub(super) fn with_model_locks(
    ms: &mut MeasurementSet,
    main: Option<&mut TableWriteLock>,
    edit: impl FnOnce(
        &mut MeasurementSet,
        Option<&mut TableWriteLock>,
        Option<&mut TableWriteLock>,
    ) -> MsResult<()>,
) -> MsResult<()> {
    let mut own_main = if main.is_some() {
        ModelTableLock::Unneeded
    } else {
        ModelTableLock::acquire(ms.main_table_mut(), 0, None)?
    };
    let result = (|| {
        let mut source = match ms.subtable_mut(SubtableId::Source) {
            Some(table) => ModelTableLock::acquire(table, 1, main.as_deref())?,
            None => ModelTableLock::Unneeded,
        };
        let edited = edit(ms, main.or_else(|| own_main.guard()), source.guard());
        let released = match ms.subtable_mut(SubtableId::Source) {
            Some(table) => source.release(table),
            None => Ok(()),
        };
        edited.and(released)
    })();
    let released = own_main.release(ms.main_table_mut());
    result.and(released)
}

enum ModelTableLock {
    Unneeded,
    Separate(TableWriteLock),
    #[cfg(unix)]
    InTable,
}

impl ModelTableLock {
    fn acquire(table: &mut Table, attempts: u32, main: Option<&TableWriteLock>) -> MsResult<Self> {
        let Some(path) = table.path().map(Path::to_path_buf) else {
            return Ok(Self::Unneeded);
        };
        #[cfg(unix)]
        if table.lock_options().is_some() {
            if table.has_lock(casa_tables::LockType::Write) {
                return Ok(Self::Unneeded);
            }
            let result = table.lock(casa_tables::LockType::Write, attempts);
            return match result {
                Ok(true) => Ok(Self::InTable),
                Ok(false) => Err(casa_tables::TableError::LockFailed {
                    path: path.display().to_string(),
                    message: "another process holds the table's lock".to_string(),
                }
                .into()),
                Err(error) => {
                    if table.has_lock(casa_tables::LockType::Write) {
                        let _ = table.unlock_metadata_only_or_discard();
                    }
                    Err(error.into())
                }
            };
        }
        let lock = match main {
            Some(main) => main.acquire_related_to_reread(&path, attempts)?,
            None => TableWriteLock::acquire_to_reread(&path, attempts)?,
        };
        table.resync()?;
        Ok(Self::Separate(lock))
    }

    fn guard(&mut self) -> Option<&mut TableWriteLock> {
        match self {
            Self::Separate(lock) => Some(lock),
            _ => None,
        }
    }

    fn release(self, _table: &mut Table) -> MsResult<()> {
        match self {
            Self::Unneeded => Ok(()),
            Self::Separate(lock) => Ok(lock.release()?),
            #[cfg(unix)]
            Self::InTable => Ok(_table.unlock_metadata_only_or_discard()?),
        }
    }
}
