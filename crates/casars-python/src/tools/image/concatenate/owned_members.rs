// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};

use casa_images::concat_file::is_image_concat;
use casa_images::expr_file::is_image_expr;
use casa_images::miriad_read::MiriadImage;
use casa_tables::publication::{
    copy_locked_external_tree, copy_locked_table_tree, rename_without_replacing,
};
use pyo3::prelude::*;

use crate::tools::image::attachment::{OpenImages, conflicting_images, lock_table_tree};
use crate::tools::image::handle::{ImageHandle, raised, same_file_identity, same_file_snapshot};
use crate::tools::image::native_rename::{
    NativeTableMove, move_native_table, move_read_only_directory,
};

use super::{Input, InputRevision};

struct OwnedMember {
    source: PathBuf,
    resolved: PathBuf,
    metadata: fs::Metadata,
    revision: Option<InputRevision>,
    filename: OsString,
}

impl OwnedMember {
    fn same_source(&self, path: &Path) -> bool {
        path.symlink_metadata()
            .is_ok_and(|current| same_file_identity(&self.metadata, &current))
    }

    fn check_source(&self) -> PyResult<()> {
        if !self.same_source(&self.source) {
            return Err(raised(format!(
                "The input image entry at {} has changed",
                self.source.display()
            )));
        }
        Ok(())
    }

    fn check_revision(&self, generation: Option<u32>) -> PyResult<()> {
        self.check_source()?;
        if let Some(expected) = &self.revision
            && (!self
                .source
                .canonicalize()
                .is_ok_and(|actual| actual == self.resolved)
                || !fs::metadata(&self.resolved)
                    .is_ok_and(|actual| same_file_snapshot(&expected.file, &actual))
                || expected
                    .generation
                    .is_some_and(|expected| Some(expected) != generation))
        {
            return Err(raised(format!(
                "The input image {} changed after concatenation geometry was validated",
                self.source.display()
            )));
        }
        Ok(())
    }
}

pub(in crate::tools::image) struct OwnedOperands {
    members: Vec<OwnedMember>,
    input_order: Vec<usize>,
    moving: bool,
}

impl OwnedOperands {
    pub(in crate::tools::image) fn inspect(
        paths: &[PathBuf],
        owner: &Path,
        moving: bool,
    ) -> PyResult<Self> {
        let mut members: Vec<OwnedMember> = Vec::with_capacity(paths.len());
        let mut input_order = Vec::with_capacity(paths.len());
        let mut names: HashMap<OsString, usize> = HashMap::new();
        let mut move_sources = HashSet::new();
        let owner = entry_path(owner)?;
        for supplied in paths {
            let filename = supplied
                .file_name()
                .ok_or_else(|| raised("An input image has no basename"))?
                .to_owned();
            if filename == "imageconcat.json" {
                return Err(raised("An input basename conflicts with imageconcat.json"));
            }
            let resolved = supplied.canonicalize().map_err(raised)?;
            if !resolved.is_dir() {
                return Err(raised(format!(
                    "Directory: {} exists, but is no directory",
                    supplied.display()
                )));
            }
            if resolved.starts_with(&owner) || owner.starts_with(&resolved) {
                return Err(raised("Input and output image directories overlap"));
            }
            let source = if moving {
                entry_path(supplied)?
            } else {
                resolved.clone()
            };
            if moving && (source.starts_with(&owner) || owner.starts_with(&source)) {
                return Err(raised("Input and output image directories overlap"));
            }
            let metadata = source.symlink_metadata().map_err(raised)?;
            if moving && !move_sources.insert(source.clone()) {
                return Err(raised("Cannot move the same input image twice"));
            }
            if let Some(&previous) = names.get(&filename) {
                if !moving && same_file_identity(&members[previous].metadata, &metadata) {
                    input_order.push(previous);
                    continue;
                }
                return Err(raised(format!(
                    "Two concatenation inputs have the destination basename {}",
                    filename.to_string_lossy()
                )));
            }
            let position = members.len();
            names.insert(filename.clone(), position);
            input_order.push(position);
            members.push(OwnedMember {
                source,
                resolved,
                metadata,
                revision: None,
                filename,
            });
        }
        if moving
            && move_sources.iter().any(|source| {
                source
                    .ancestors()
                    .skip(1)
                    .any(|parent| move_sources.contains(parent))
            })
        {
            return Err(raised("Cannot move overlapping concatenation inputs"));
        }
        Ok(Self {
            members,
            input_order,
            moving,
        })
    }

    pub(super) fn destination(&self, owner: &Path, index: usize) -> PathBuf {
        owner.join(&self.members[self.input_order[index]].filename)
    }

    pub(super) fn validation_paths(&self) -> Vec<PathBuf> {
        self.input_order
            .iter()
            .map(|&index| self.members[index].resolved.clone())
            .collect()
    }

    pub(super) fn capture_validated_inputs(&mut self, inputs: Vec<Input<'_>>) -> PyResult<()> {
        for (&index, input) in self.input_order.iter().zip(inputs) {
            let member = &mut self.members[index];
            let revision = input
                .revision
                .expect("owned preflight releases the input handle");
            if input.resolved_path != member.resolved
                || member
                    .revision
                    .as_ref()
                    .is_some_and(|old| !old.matches(&revision))
            {
                return Err(raised(format!(
                    "The input {} changed during validation",
                    member.source.display()
                )));
            }
            member.revision = Some(revision);
        }
        Ok(())
    }

    pub(in crate::tools::image) fn stage(
        self,
        owner: &Path,
        product: &Path,
        registry: &OpenImages,
        moved: &mut MemberMoves,
    ) -> PyResult<()> {
        super::relocation::check_owned(
            &entry_path(owner)?,
            self.members
                .iter()
                .map(|member| (member.filename.as_os_str(), member.source.as_path())),
            self.moving,
        )?;
        #[cfg(unix)]
        if self.moving {
            // Validate each full link chain before transferring any entries.
            for member in self
                .members
                .iter()
                .filter(|member| member.metadata.is_symlink())
            {
                lock_referent_for_read(member)?;
            }
        }
        for (index, member) in self.members.into_iter().enumerate() {
            let destination = product.join(&member.filename);
            if self.moving {
                moved.transfer(member, destination, index, registry)?;
            } else {
                copy_member(&member, &destination)?;
            }
        }
        if !self.moving {
            // Every copy should open at its new relative layout before it
            // can displace a previous image. This reads headers, not pixels.
            super::virtual_image::open_handle(product).map_err(raised)?;
        }
        Ok(())
    }
}

fn entry_path(path: &Path) -> PyResult<PathBuf> {
    let parent = path
        .parent()
        .ok_or_else(|| raised("Image path has no parent"))?;
    let name = path
        .file_name()
        .ok_or_else(|| raised("Image path has no basename"))?;
    Ok(parent.canonicalize().map_err(raised)?.join(name))
}

fn copy_member(member: &OwnedMember, destination: &Path) -> PyResult<()> {
    #[cfg(unix)]
    let _locks = lock_referent_for_read(member)?;
    #[cfg(not(unix))]
    member.check_revision(None)?;
    if ImageHandle::recognizes_miriad(&member.source, &member.source) {
        copy_locked_external_tree(&member.source, destination, |name| {
            name.to_str().is_some_and(MiriadImage::is_image_file)
        })
    } else if !is_image_concat(&member.source) && is_image_expr(&member.source) {
        copy_locked_external_tree(&member.source, destination, |name| name == "imageexpr.json")
    } else {
        copy_locked_table_tree(&member.source, destination)
    }
    .map_err(raised)?;
    member.check_source()
}

#[cfg(unix)]
fn lock_referent_for_read(member: &OwnedMember) -> PyResult<Vec<casa_tables::Table>> {
    use casa_tables::{LockMode, LockOptions, Table, TableOptions};

    let mut locks = Vec::new();
    let mut pending = vec![member.resolved.clone()];
    while let Some(directory) = pending.pop() {
        if ImageHandle::has_native_control(&directory) {
            locks.push(
                Table::open_with_lock_attempts(
                    TableOptions::new(&directory),
                    LockOptions::new(LockMode::AutoLocking).read_only(true),
                    1,
                )
                .map_err(raised)?,
            );
        }
        for entry in fs::read_dir(directory).map_err(raised)? {
            let entry = entry.map_err(raised)?;
            if entry.file_type().map_err(raised)?.is_dir() {
                pending.push(entry.path());
            }
        }
    }
    let generation = if member
        .revision
        .as_ref()
        .is_some_and(|r| r.generation.is_some())
    {
        locks
            .first()
            .map(Table::locked_modify_counter)
            .transpose()
            .map_err(raised)?
    } else {
        None
    };
    member.check_revision(generation)?;
    Ok(locks)
}

struct MovedMember {
    original: OwnedMember,
    staged: PathBuf,
    transition: NativeTableMove,
}

#[derive(Default)]
pub(in crate::tools::image) struct MemberMoves(Vec<MovedMember>);

impl MemberMoves {
    fn transfer(
        &mut self,
        member: OwnedMember,
        destination: PathBuf,
        index: usize,
        registry: &OpenImages,
    ) -> PyResult<()> {
        let holding = destination
            .parent()
            .and_then(Path::parent)
            .expect("a private directory contains the staged product")
            .join(format!("member-{index}"));
        let mut transition = claim(&member, &holding, registry)?;
        transition.release_copy_write_lock();
        self.0.push(MovedMember {
            original: member,
            staged: holding.clone(),
            transition,
        });
        let staged = self.0.last_mut().expect("just staged this member");
        staged.transition.check_target(&holding)?;
        rename_without_replacing(&holding, &destination).map_err(raised)?;
        staged.staged = destination;
        staged.transition.check_target(&staged.staged)
    }

    pub(in crate::tools::image) fn rollback(self, mut error: PyErr) -> (PyErr, bool) {
        let mut restored = true;
        for moved in self.0.into_iter().rev() {
            let Err(next) =
                moved
                    .transition
                    .restore::<()>(&moved.original.source, &moved.staged, error)
            else {
                unreachable!("rollback returns the original or recovery error")
            };
            error = next;
            restored &= moved.original.same_source(&moved.original.source);
        }
        (error, restored)
    }

    pub(in crate::tools::image) fn finish(self, output: &Path) -> Vec<String> {
        let mut failures = Vec::new();
        for moved in self.0 {
            if let Err(error) = moved
                .transition
                .finish(&output.join(moved.original.filename))
            {
                failures.push(error.to_string());
            }
        }
        failures
    }

    pub(in crate::tools::image) fn verify_links(&self, output: &Path) -> PyResult<()> {
        for moved in self
            .0
            .iter()
            .filter(|moved| moved.original.metadata.is_symlink())
        {
            let alias = output.join(&moved.original.filename);
            let resolved = alias.canonicalize().map_err(raised)?;
            let file = fs::metadata(&resolved).map_err(raised)?;
            let expected = moved
                .original
                .revision
                .as_ref()
                .expect("published link was validated");
            if same_file_identity(&expected.file, &file) {
                continue;
            }
            if let Some(referent) = self.0.iter().find(|entry| {
                !entry.original.metadata.is_symlink()
                    && entry.original.source == moved.original.resolved
                    && output.join(&entry.original.filename) == resolved
            }) {
                referent.transition.check_target(&resolved)?;
                continue;
            }
            return Err(raised(format!(
                "The moved input symlink {} would resolve to a different image",
                alias.display()
            )));
        }
        Ok(())
    }
}

fn claim(member: &OwnedMember, holding: &Path, registry: &OpenImages) -> PyResult<NativeTableMove> {
    if member.metadata.is_symlink() {
        #[cfg(unix)]
        {
            member.check_source()?;
            return crate::tools::image::native_rename::move_symlink(
                &member.source,
                &member.metadata,
                holding,
            );
        }
        #[cfg(not(unix))]
        return Err(raised(
            "Moving a symbolic link is unsupported on this platform",
        ));
    }
    if conflicting_images(registry, &member.source, 0)
        || crate::tools::table::path_is_in_use(&member.source)
    {
        return Err(raised(format!(
            "Cannot move image because {} is still open in this process",
            member.source.display()
        )));
    }
    let mut locks = BTreeMap::new();
    lock_table_tree(&member.source, &mut locks)?;
    let generation = locks
        .get(&member.source)
        .map(|lock| lock.modify_counter().map(|n| n.unwrap_or(0)))
        .transpose()
        .map_err(raised)?;
    member.check_revision(generation)?;
    if ImageHandle::has_native_control(&member.source) {
        move_native_table(&member.source, &member.metadata, holding, None, |_| Ok(()))
    } else {
        move_read_only_directory(&member.source, &member.metadata, holding, None, |_| Ok(()))
    }
}
