// SPDX-License-Identifier: LGPL-3.0-or-later
//! Move MS members into a portable CASA MultiMS.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::ffi::OsString;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use casa_tables::publication::{
    check_write_access, copy_locked_table_tree, exchange_directories,
    lock_table_tree_for_replacement, rename_without_replacing,
};
use casa_tables::{Table, TableOptions, TablePartNames, TableWriteLock};
use tempfile::TempDir;

use crate::tool_sort::check_named_table_output;
use crate::{MsError, MsResult};

mod relocation;
mod subtables;

/// Assemble a portable CASA MultiMS from the input tables in order.
///
/// Members move into `SUBMSS` on success. Referencing a physical member
/// twice retains both logical occurrences and moves it once. Copying
/// subtables replaces the selected links with copies from the first member.
///
/// # Errors
/// A missing or incompatible member, a conflicting output, or a failed
/// publication. On failure member moves are rolled back. If a concurrent
/// change prevents restoration, surviving originals are retained for recovery.
pub fn create_multi_ms(
    output: &Path,
    inputs: &[PathBuf],
    joined_subtables: &[String],
    copy_subtables: Option<&[String]>,
) -> MsResult<()> {
    create_multi_ms_then(output, inputs, joined_subtables, copy_subtables, || Ok(()))
}

/// Create a MultiMS and retain rollback state until the caller accepts it.
///
/// # Errors
/// A MultiMS creation error or an error returned by the acceptance callback.
pub fn create_multi_ms_then<T>(
    output: &Path,
    inputs: &[PathBuf],
    joined_subtables: &[String],
    copy_subtables: Option<&[String]>,
    accept: impl FnOnce() -> MsResult<T>,
) -> MsResult<T> {
    let output = output_path(output)?;
    let destination = Destination::open(&output)?;
    let workspace = tempfile::Builder::new()
        .prefix(".casa-rs-multims-")
        .tempdir_in(output.parent().expect("canonical output parent"))
        .map_err(invalid)?;
    let product = workspace.path().join("mms");
    let (mut members, order) = plan_members(inputs, &output, &product)?;
    // Take the locks in path order. The row order comes from the caller.
    let ordered = members
        .iter()
        .map(|member| &member.physical)
        .collect::<BTreeSet<_>>();
    let mut locks = Vec::new();
    for member in ordered {
        check_write_access(&member.join("table.dat")).map_err(invalid)?;
        locks.extend(lock_table_tree_for_replacement(member)?);
    }
    if matches!(&destination, Destination::Table(_)) {
        locks.extend(lock_table_tree_for_replacement(&output)?);
    }
    let locations = members
        .iter()
        .filter(|member| member.source == member.physical)
        .map(|member| (member.source.clone(), member.target.clone()))
        .collect();
    let first = Table::open(TableOptions::new(&members[order[0]].physical))?;
    let original = order
        .iter()
        .map(|&index| Table::open(TableOptions::new(&members[index].physical)))
        .collect::<Result<Vec<_>, _>>()?;
    if matches!(&destination, Destination::Table(_)) {
        for table in &original {
            check_named_table_output(table, &output)?;
        }
    }
    let _ = Table::concat(original)?;
    let mut published = None;
    let result = assemble(Assembly {
        members: &mut members,
        order: &order,
        first: &first,
        product: &product,
        joined_subtables,
        copy_subtables,
        locks: &locks,
        locations: &locations,
        output: &output,
    })
    .and_then(|()| {
        let new = product.symlink_metadata().map_err(invalid)?;
        destination.publish(&product, &output)?;
        published = Some(new);
        destination.verify_publication(&product, &output, published.as_ref().unwrap())
    });
    let mut rollback_locks = Some(locks);
    let result = result.and_then(|()| {
        drop(rollback_locks.take());
        let accepted = accept()?;
        destination.verify_publication(&product, &output, published.as_ref().unwrap())?;
        Ok(accepted)
    });
    if let Err(error) = result {
        let mut failed = Vec::new();
        let restored_output = published.as_ref().map_or(Ok(()), |identity| {
            destination.restore(&product, &output, identity)
        });
        match restored_output {
            Ok(()) => {
                for member in members.iter_mut().rev() {
                    if let Err(error) = member.restore() {
                        failed.push(error.to_string());
                    }
                }
            }
            Err(error) => failed.push(error.to_string()),
        }
        if !failed.is_empty() {
            let recovery = workspace.keep();
            for member in &mut members {
                if member.location.is_some()
                    && let Some(retired) = member.retired.take()
                {
                    let _ = retired.keep();
                }
            }
            return Err(MsError::MultiMsRecovery {
                cause: error.to_string(),
                restore: failed.join(", "),
                staging: recovery,
            });
        }
        return Err(error);
    }
    result
}

struct Assembly<'a> {
    members: &'a mut [Member],
    order: &'a [usize],
    first: &'a Table,
    product: &'a Path,
    joined_subtables: &'a [String],
    copy_subtables: Option<&'a [String]>,
    locks: &'a [TableWriteLock],
    locations: &'a relocation::Locations,
    output: &'a Path,
}

fn assemble(stage: Assembly<'_>) -> MsResult<()> {
    let Assembly {
        members,
        order,
        first,
        product,
        joined_subtables,
        copy_subtables,
        locks,
        locations,
        output,
    } = stage;
    fs::create_dir_all(product.join("SUBMSS")).map_err(invalid)?;
    let first_path = first.path().expect("persisted member");
    let (copy_modes, private_bases) = if copy_subtables.is_some() {
        copy_modes(members, first_path, locations)?
    } else {
        (vec![CopyMode::Move; members.len()], HashSet::new())
    };
    for (member, &mode) in members.iter_mut().zip(&copy_modes) {
        let edit = relocation::MemberEdit::prepare(member, locations)?;
        let private_base = private_bases.contains(&member.physical);
        let links = if member.source == member.physical || mode != CopyMode::Move {
            relocation::filesystem_links(member, locations, output)?
        } else {
            Vec::new()
        };
        member.stage(edit, mode, private_base, locations, links)?;
    }
    if let Some(omit) = copy_subtables {
        for (member, mode) in members.iter().zip(copy_modes) {
            if mode != CopyMode::Move {
                subtables::copy_from_first(
                    first,
                    &member.target,
                    omit,
                    locks,
                    mode != CopyMode::CopyNested,
                    product,
                )?;
            }
        }
    }
    for member in members.iter() {
        if !locks
            .iter()
            .any(|lock| lock.matches_table_path(&member.physical))
            || member.source.canonicalize().ok().as_ref() != Some(&member.physical)
        {
            return Err(invalid(format!(
                "MultiMS member changed: {}",
                member.source.display()
            )));
        }
    }
    for member in members.iter_mut() {
        member.relocate()?;
    }
    let tables = order
        .iter()
        .map(|&index| Table::open(TableOptions::new(&members[index].target)))
        .collect::<Result<Vec<_>, _>>()?;
    subtables::validate_joins(&tables, joined_subtables)?;
    let mut info = first.info().clone();
    info.sub_type = "CONCATENATED".into();
    Table::concat(tables)?.save_with_subtables(
        TableOptions::new(product),
        &info,
        joined_subtables,
    )?;
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CopyMode {
    Move,
    CopySubtables,
    CopyNested,
    Materialize,
}

fn copy_modes(
    members: &[Member],
    first: &Path,
    locations: &relocation::Locations,
) -> MsResult<(Vec<CopyMode>, HashSet<PathBuf>)> {
    let mut modes = Vec::with_capacity(members.len());
    let mut private_bases = HashSet::new();
    for member in members {
        let path = &member.physical;
        if path == first {
            modes.push(CopyMode::Move);
            continue;
        }
        let mode = match Table::immediate_part_names(path, path)? {
            TablePartNames::Plain(_) => CopyMode::CopySubtables,
            TablePartNames::Reference(_) => {
                if let Some(base_member) = relocation::owned_reference_base(path, locations)? {
                    // RefTable keyword writes go to its base. Stage that base
                    // before changing the view or its inherited keywords.
                    private_bases.insert(base_member);
                    CopyMode::CopySubtables
                } else {
                    CopyMode::Materialize
                }
            }
            TablePartNames::Concatenation { .. } => CopyMode::CopyNested,
        };
        modes.push(mode);
    }
    Ok((modes, private_bases))
}

struct Member {
    source: PathBuf,
    physical: PathBuf,
    target: PathBuf,
    identity: fs::Metadata,
    copied: bool,
    location: Option<PathBuf>,
    retired: Option<TempDir>,
}

impl Member {
    fn stage(
        &mut self,
        edit: relocation::MemberEdit,
        mode: CopyMode,
        private_base: bool,
        locations: &relocation::Locations,
        links: relocation::FilesystemLinks,
    ) -> MsResult<()> {
        if self.source != self.physical && mode == CopyMode::Move {
            relocation::stage_alias(self, locations)?;
        } else if mode == CopyMode::Materialize {
            relocation::materialize_member(self, locations)?;
        } else if mode != CopyMode::Move || private_base || !edit.is_empty() || !links.is_empty() {
            copy_locked_table_tree(&self.physical, &self.target).map_err(invalid)?;
            relocation::apply_filesystem_links(links)?;
            relocation::privatize_descriptor(&self.target)?;
            edit.apply(&self.target)?;
        } else {
            return Ok(());
        }
        self.copied = true;
        Ok(())
    }

    fn relocate(&mut self) -> MsResult<()> {
        self.verify_location(&self.source)?;
        if !self.copied {
            match rename_without_replacing(&self.source, &self.target) {
                Ok(()) => {
                    self.location = Some(self.target.clone());
                    return self.verify_location(&self.target);
                }
                Err(error) if error.kind() == ErrorKind::CrossesDevices => {
                    copy_locked_table_tree(&self.physical, &self.target).map_err(invalid)?;
                }
                Err(error) => return Err(invalid(error)),
            }
        }
        let retired = tempfile::Builder::new()
            .prefix(".casa-rs-multims-member-")
            .tempdir_in(self.source.parent().expect("canonical member parent"))
            .map_err(invalid)?;
        let location = retired.path().join("original.ms");
        rename_without_replacing(&self.source, &location).map_err(invalid)?;
        self.location = Some(location);
        self.retired = Some(retired);
        self.verify_location(self.location.as_ref().unwrap())
    }

    fn verify_location(&self, location: &Path) -> MsResult<()> {
        if same_entry(
            &self.identity,
            &location.symlink_metadata().map_err(invalid)?,
        ) {
            return Ok(());
        }
        Err(invalid(format!(
            "MultiMS member changed: {}",
            self.source.display()
        )))
    }

    fn restore(&mut self) -> MsResult<()> {
        if let Some(location) = &self.location {
            rename_without_replacing(location, &self.source).map_err(|error| {
                invalid(format!(
                    "restore {} from {}: {error}",
                    self.source.display(),
                    location.display(),
                ))
            })?;
            self.location = None;
        }
        Ok(())
    }
}

fn plan_members(
    paths: &[PathBuf],
    output: &Path,
    product: &Path,
) -> MsResult<(Vec<Member>, Vec<usize>)> {
    if paths.is_empty() {
        return Err(invalid("MultiMS requires at least one member"));
    }
    let mut distinct: HashMap<PathBuf, usize> = HashMap::new();
    let mut names: HashSet<OsString> = HashSet::new();
    let mut members: Vec<Member> = Vec::new();
    let mut order = Vec::with_capacity(paths.len());
    for path in paths {
        let identity = path.symlink_metadata().map_err(invalid)?;
        let physical = path.canonicalize().map_err(invalid)?;
        let source = if identity.is_symlink() {
            output_path(path)?
        } else {
            physical.clone()
        };
        if output.starts_with(&source)
            || source.starts_with(output)
            || output.starts_with(&physical)
            || physical.starts_with(output)
        {
            return Err(invalid("the MultiMS destination overlaps an input"));
        }
        if let Some(&index) = distinct.get(&source) {
            order.push(index);
            continue;
        }
        if members.iter().any(|member| {
            source.starts_with(&member.source)
                || member.source.starts_with(&source)
                || (physical != member.physical
                    && (physical.starts_with(&member.physical)
                        || member.physical.starts_with(&physical)))
        }) {
            return Err(invalid("MultiMS members overlap"));
        }
        let name = source
            .file_name()
            .ok_or_else(|| invalid("member has no file name"))?;
        if !names.insert(name.to_owned()) {
            return Err(invalid(format!(
                "MultiMS members share the file name {}",
                name.to_string_lossy()
            )));
        }
        let table = Table::open(TableOptions::new(&physical))?;
        if !table
            .schema()
            .is_some_and(|schema| schema.contains_column("DATA_DESC_ID"))
            || table.keywords().get("MS_VERSION").is_none()
        {
            return Err(invalid(format!(
                "{} is not a MeasurementSet",
                source.display()
            )));
        }
        let index = members.len();
        let target = product.join("SUBMSS").join(name);
        distinct.insert(source.clone(), index);
        order.push(index);
        members.push(Member {
            source,
            physical,
            target,
            identity,
            copied: false,
            location: None,
            retired: None,
        });
    }
    Ok((members, order))
}

enum Destination {
    Absent,
    Empty(fs::Metadata),
    Table(fs::Metadata),
}

impl Destination {
    fn open(path: &Path) -> MsResult<Self> {
        match path.symlink_metadata() {
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(Self::Absent),
            Err(error) => Err(invalid(error)),
            Ok(metadata) if metadata.is_dir() => {
                if path.read_dir().map_err(invalid)?.next().is_none() {
                    Ok(Self::Empty(metadata))
                } else if Table::open(TableOptions::new(path)).is_ok() {
                    Ok(Self::Table(metadata))
                } else {
                    Err(invalid("existing MultiMS output directory is not a table"))
                }
            }
            Ok(_) => Err(invalid("existing MultiMS output is not a table directory")),
        }
    }

    fn publish(&self, product: &Path, output: &Path) -> MsResult<()> {
        let before = match self {
            Self::Absent => return rename_without_replacing(product, output).map_err(invalid),
            Self::Empty(before) | Self::Table(before) => before,
        };
        let current = output.symlink_metadata().map_err(invalid)?;
        if !same_entry(before, &current) {
            return Err(invalid("MultiMS destination changed during creation"));
        }
        match self {
            Self::Empty(_) | Self::Table(_) => {
                exchange_directories(product, output).map_err(invalid)
            }
            Self::Absent => unreachable!("handled above"),
        }
    }

    fn verify_publication(
        &self,
        product: &Path,
        output: &Path,
        new: &fs::Metadata,
    ) -> MsResult<()> {
        self.verify_output(output, new)?;
        match self {
            Self::Absent => Ok(()),
            Self::Empty(old) | Self::Table(old) => {
                if !same_entry(old, &product.symlink_metadata().map_err(invalid)?)
                    || (matches!(self, Self::Empty(_))
                        && product.read_dir().map_err(invalid)?.next().is_some())
                {
                    return Err(invalid("MultiMS destination changed during creation"));
                }
                Ok(())
            }
        }
    }

    fn verify_output(&self, output: &Path, identity: &fs::Metadata) -> MsResult<()> {
        if same_entry(identity, &output.symlink_metadata().map_err(invalid)?) {
            return Ok(());
        }
        Err(invalid(format!(
            "published MultiMS directory changed: {}",
            output.display()
        )))
    }

    fn restore(&self, product: &Path, output: &Path, new: &fs::Metadata) -> MsResult<()> {
        self.verify_output(output, new)?;
        match self {
            Self::Absent => rename_without_replacing(output, product).map_err(invalid),
            Self::Empty(_) | Self::Table(_) => {
                exchange_directories(product, output).map_err(invalid)
            }
        }
    }
}

fn same_entry(before: &fs::Metadata, current: &fs::Metadata) -> bool {
    if before.file_type() != current.file_type() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        (before.dev(), before.ino()) == (current.dev(), current.ino())
    }
    #[cfg(not(unix))]
    {
        before
            .created()
            .ok()
            .zip(current.created().ok())
            .is_some_and(|(a, b)| a == b)
    }
}

fn output_path(path: &Path) -> MsResult<PathBuf> {
    let name = path
        .file_name()
        .ok_or_else(|| invalid("MultiMS output must name a directory"))?;
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    Ok(parent.canonicalize().map_err(invalid)?.join(name))
}

fn invalid(error: impl std::fmt::Display) -> MsError {
    MsError::InvalidInput(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MeasurementSet, builder::MeasurementSetBuilder};

    #[test]
    fn restores_a_displaced_member_after_detecting_changed_identity() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source");
        let target = directory.path().join("stage");
        let original = directory.path().join("original");
        fs::create_dir(&source).unwrap();
        let mut member = Member {
            source: source.clone(),
            physical: source.clone(),
            target: target.clone(),
            identity: source.symlink_metadata().unwrap(),
            copied: false,
            location: None,
            retired: None,
        };
        // Reproduce the final state of a race between checking the input
        // name and moving its directory entry.
        fs::rename(&source, &original).unwrap();
        fs::create_dir(&source).unwrap();
        fs::write(source.join("unrelated"), b"keep").unwrap();
        fs::rename(&source, &target).unwrap();
        member.location = Some(target.clone());
        assert!(member.verify_location(&target).is_err());
        member.restore().unwrap();
        assert!(original.is_dir());
        assert_eq!(fs::read(source.join("unrelated")).unwrap(), b"keep");
    }

    #[test]
    fn restores_a_displaced_output_after_detecting_changed_identity() {
        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("output");
        let product = directory.path().join("product");
        let original = directory.path().join("original");
        fs::create_dir(&output).unwrap();
        fs::create_dir(&product).unwrap();
        let expected = Destination::Table(output.symlink_metadata().unwrap());
        let new = product.symlink_metadata().unwrap();
        fs::rename(&output, &original).unwrap();
        fs::create_dir(&output).unwrap();
        fs::write(output.join("unrelated"), b"keep").unwrap();
        exchange_directories(&product, &output).unwrap();
        assert!(
            expected
                .verify_publication(&product, &output, &new)
                .is_err()
        );
        expected.restore(&product, &output, &new).unwrap();
        assert!(original.is_dir());
        assert_eq!(fs::read(output.join("unrelated")).unwrap(), b"keep");
    }

    #[test]
    fn failed_multims_copy_leaves_keywords_on_the_original_reference_base() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source.ms");
        let intermediate = directory.path().join("intermediate.ms");
        let view = directory.path().join("view.ms");
        let output = directory.path().join("out.ms");
        MeasurementSet::create(&source, MeasurementSetBuilder::new()).unwrap();
        let main = Table::open(TableOptions::new(&source)).unwrap();
        let keywords = main.keywords().clone();
        for path in [&intermediate, &view] {
            main.select_rows(&[])
                .unwrap()
                .save(TableOptions::new(path))
                .unwrap();
        }
        Table::set_staged_table_parts(&view, std::slice::from_ref(&intermediate)).unwrap();
        let error = create_multi_ms(
            &output,
            &[source.clone(), view.clone()],
            &["MISSING".into()],
            Some(&[]),
        )
        .expect_err("joining an absent subtable must fail");
        assert!(
            error
                .to_string()
                .contains("MultiMS subtable MISSING is missing")
        );
        assert!(!output.exists());
        assert!(view.exists() && intermediate.exists());
        assert_eq!(
            Table::open(TableOptions::new(&source)).unwrap().keywords(),
            &keywords
        );
        create_multi_ms(&output, &[source, view], &[], Some(&[])).unwrap();
        assert!(intermediate.is_dir());
        assert!(output.join("SUBMSS/view.ms/ANTENNA/table.dat").is_file());
    }
}
