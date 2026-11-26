// SPDX-License-Identifier: LGPL-3.0-or-later
//! Hold input identities and the caller's existing locks through publication.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, Metadata};
use std::path::{Path, PathBuf};

use casa_core::{RecordValue, Value};
use casa_tables::{Table, TableOptions, TableWriteLock, add_directory};
use rayon::prelude::*;

use super::publication::io;
use crate::ms::subtable_path_from_main;
use crate::{MeasurementSet, MsResult, SubtableId};

const CALIBRATION: &[&str] = &["GAIN_CURVE", "PHASE_CAL", "EARTH_ORIENTATION"];

pub(crate) fn held_reader(path: &Path) -> MsResult<Table> {
    let options = TableOptions::new(path);
    #[cfg(unix)]
    let table = Table::open_with_lock_attempts(
        options,
        casa_tables::LockOptions::new(casa_tables::LockMode::PermanentLocking).read_only(true),
        1,
    )?;
    #[cfg(not(unix))]
    let table = Table::open(options)?;
    Ok(table)
}

pub(super) struct WriteGuards<'a> {
    retained: &'a mut BTreeMap<PathBuf, TableWriteLock>,
    original: BTreeSet<PathBuf>,
    pub(super) guards: BTreeMap<PathBuf, TableWriteLock>,
}

impl<'a> WriteGuards<'a> {
    pub(super) fn new(retained: &'a mut BTreeMap<PathBuf, TableWriteLock>) -> Self {
        let guards = std::mem::take(retained);
        let original = guards.keys().cloned().collect();
        Self {
            retained,
            original,
            guards,
        }
    }

    pub(super) fn acquire(&mut self, paths: BTreeSet<PathBuf>) -> MsResult<()> {
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        let entries = paths
            .into_par_iter()
            .map(|path| {
                tracing::dispatcher::with_default(&dispatch, || {
                    if let Some(held) = self.guards.get(&path) {
                        if !held.matches_table_path(&path) {
                            return Err(io(format!(
                                "{} no longer has its retained lock",
                                path.display()
                            )));
                        }
                        return Ok(None);
                    }
                    let guard = TableWriteLock::acquire_to_reread(&path, 1)?;
                    Ok(Some((path, guard)))
                })
            })
            .collect::<MsResult<Vec<_>>>()?;
        self.guards.extend(entries.into_iter().flatten());
        Ok(())
    }
}

impl Drop for WriteGuards<'_> {
    fn drop(&mut self) {
        for path in &self.original {
            let guard = self
                .guards
                .remove(path)
                .expect("existing concatenation lock");
            self.retained.insert(path.clone(), guard);
        }
    }
}

struct LinkedLocation {
    path: PathBuf,
    identity: Metadata,
}

pub(super) struct InputTables {
    subtables: BTreeMap<SubtableId, PathBuf>,
    pub(super) calibration: BTreeMap<&'static str, PathBuf>,
    links: BTreeMap<PathBuf, LinkedLocation>,
    ephemerides: BTreeSet<PathBuf>,
}

impl InputTables {
    pub(super) fn capture(root: &Path) -> MsResult<(Self, Table)> {
        let main = Table::open(TableOptions::new(root))?;
        let mut result = Self {
            subtables: BTreeMap::new(),
            calibration: BTreeMap::new(),
            links: BTreeMap::new(),
            ephemerides: BTreeSet::new(),
        };
        for &id in SubtableId::ALL_REQUIRED
            .iter()
            .chain(SubtableId::ALL_OPTIONAL)
        {
            if let Some(name) = subtable_path_from_main(&main, root, id) {
                let path = result.capture_link(name)?;
                result.subtables.insert(id, path);
            }
        }
        if let Some(field) = result.subtables.get(&SubtableId::Field).cloned() {
            for entry in fs::read_dir(field).map_err(io)? {
                let entry = entry.map_err(io)?;
                if entry.file_name().to_string_lossy().starts_with("EPHEM")
                    && entry.path().join("table.dat").is_file()
                {
                    let path = result.capture_link(entry.path())?;
                    result.ephemerides.insert(path);
                }
            }
        }
        for &name in CALIBRATION {
            if let Some(Value::TableRef(value)) = main.keywords().get(name) {
                let path = result.capture_link(add_directory(value, root))?;
                result.calibration.insert(name, path);
            }
        }
        let mut other_links = Vec::new();
        gather_links(main.keywords(), root, &mut other_links);
        for name in other_links {
            if name.is_dir() {
                result.capture_link(name)?;
            }
        }
        result.capture_link(root.to_path_buf())?;
        Ok((result, main))
    }

    fn capture_link(&mut self, name: PathBuf) -> MsResult<PathBuf> {
        use std::collections::btree_map::Entry;
        Ok(match self.links.entry(name) {
            Entry::Occupied(entry) => entry.get().path.clone(),
            Entry::Vacant(entry) => {
                let path = entry.key().canonicalize().map_err(io)?;
                let identity = fs::metadata(&path).map_err(io)?;
                entry.insert(LinkedLocation { path, identity }).path.clone()
            }
        })
    }

    pub(super) fn contains(&self, path: &Path) -> bool {
        self.links.values().any(|link| link.path.starts_with(path))
    }

    pub(super) fn write_paths(&self, attached: bool) -> BTreeSet<PathBuf> {
        if !attached {
            return self
                .subtables
                .get(&SubtableId::History)
                .cloned()
                .into_iter()
                .collect();
        }
        self.subtables
            .iter()
            .filter(|(id, _)| participates(**id, false))
            .map(|(_, path)| path.clone())
            .chain(self.calibration.values().cloned())
            .chain(self.ephemerides.iter().cloned())
            .collect()
    }

    pub(super) fn open(&self, main: Table, donor: bool) -> MsResult<MeasurementSet> {
        let links = self
            .subtables
            .iter()
            .filter(|(id, _)| participates(**id, donor))
            .map(|(&id, path)| (id, path.clone()))
            .collect::<Vec<_>>();
        MeasurementSet::open_concatenation_metadata(main, links, donor)
    }

    pub(super) fn subtable_path(&self, id: SubtableId) -> Option<&Path> {
        self.subtables.get(&id).map(PathBuf::as_path)
    }

    pub(super) fn hold_ephemerides(&self) -> MsResult<Vec<Table>> {
        self.ephemerides
            .iter()
            .map(|path| held_reader(path))
            .collect()
    }

    pub(super) fn validate(&self) -> MsResult<()> {
        for (link, captured) in &self.links {
            if !matches!(link.canonicalize(), Ok(path) if path == captured.path)
                || !fs::metadata(&captured.path)
                    .is_ok_and(|current| same_identity(&current, &captured.identity))
            {
                return Err(io(format!(
                    "MeasurementSet table {} changed during concatenation",
                    link.display()
                )));
            }
        }
        Ok(())
    }
}

fn gather_links(record: &RecordValue, root: &Path, output: &mut Vec<PathBuf>) {
    for field in record.fields() {
        match &field.value {
            Value::TableRef(path) => output.push(add_directory(path, root)),
            Value::Record(record) => gather_links(record, root, output),
            _ => {}
        }
    }
}

fn participates(id: SubtableId, donor: bool) -> bool {
    match id {
        SubtableId::History => donor,
        SubtableId::Doppler
        | SubtableId::FreqOffset
        | SubtableId::FlagCmd
        | SubtableId::Pointing => false,
        _ => true,
    }
}

#[cfg(unix)]
pub(super) fn same_identity(left: &Metadata, right: &Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    left.dev() == right.dev() && left.ino() == right.ino()
}

#[cfg(not(unix))]
pub(super) fn same_identity(left: &Metadata, right: &Metadata) -> bool {
    left.created().ok() == right.created().ok()
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn preflight_detects_same_row_count_leaf_replacement_under_the_lock() {
        let scratch = tempfile::tempdir().unwrap();
        let path = scratch.path().join("attached.ms");
        MeasurementSet::create(&path, crate::MeasurementSetBuilder::new()).unwrap();
        let (inputs, _) = InputTables::capture(&path).unwrap();
        let pointing = path.join("POINTING").canonicalize().unwrap();
        let retained = TableWriteLock::acquire_to_reread(&pointing, 1).unwrap();
        let mut previous = BTreeMap::from([(pointing.clone(), retained)]);
        let mut writers = WriteGuards::new(&mut previous);
        writers.acquire(inputs.write_paths(true)).unwrap();
        let feed = path.join("FEED");
        assert!(TableWriteLock::acquire_to_reread(&feed, 1).is_err());
        assert!(TableWriteLock::acquire_to_reread(&pointing, 1).is_err());
        let old = scratch.path().join("original-feed");
        fs::rename(&feed, &old).unwrap();
        casa_tables::publication::copy_locked_table_tree(&old, &feed).unwrap();
        assert!(
            inputs
                .validate()
                .unwrap_err()
                .to_string()
                .contains("changed during concatenation")
        );
        drop(writers);
        assert!(previous[&pointing].matches_table_path(&pointing));
        assert!(TableWriteLock::acquire_to_reread(&pointing, 1).is_err());
    }
}
