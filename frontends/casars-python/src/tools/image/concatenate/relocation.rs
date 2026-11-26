// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::fs;
use std::io::{self, ErrorKind};
use std::path::{Component, Path, PathBuf};

use casa_imageanalysis::analysis::ImageToolExpressionInputs;
use casa_images::MiriadImage;
use casa_images::expr_file::{is_image_expr, read_tool_manifest};
use casa_images::{ConcatManifest, is_image_concat};
use pyo3::prelude::*;

use crate::tools::image::handle::{ImageHandle, raised, same_file_identity};

/// A virtual directory tree in its proposed location. Backing paths still
/// name the original entries, so this check reads metadata without moving
/// or copying the inputs. Resolve links relative to the proposed location.
struct Relocation<'a> {
    destination: &'a Path,
    entries: Entries<'a>,
    removed: HashSet<&'a Path>,
}

enum Entries<'a> {
    WholeTree(&'a Path),
    OwnedMembers(HashMap<&'a OsStr, &'a Path>),
}

pub(super) fn check_rename(source: &Path, destination: &Path) -> PyResult<()> {
    Relocation {
        destination,
        entries: Entries::WholeTree(source),
        removed: HashSet::from([source]),
    }
    .check([(source, destination.to_path_buf())])
}

pub(super) fn check_owned<'a>(
    destination: &'a Path,
    members: impl Iterator<Item = (&'a OsStr, &'a Path)>,
    moving: bool,
) -> PyResult<()> {
    let members: HashMap<_, _> = members.collect();
    let roots = members
        .iter()
        .map(|(name, source)| (*source, destination.join(name)))
        .collect::<Vec<_>>();
    let removed = if moving {
        members.values().copied().collect()
    } else {
        HashSet::new()
    };
    Relocation {
        destination,
        entries: Entries::OwnedMembers(members),
        removed,
    }
    .check(roots)
}

impl Relocation<'_> {
    fn check<'a>(&self, roots: impl IntoIterator<Item = (&'a Path, PathBuf)>) -> PyResult<()> {
        let mut images = roots
            .into_iter()
            .map(|(source, target)| (source.to_path_buf(), target))
            .collect::<Vec<_>>();
        let mut checked = HashSet::new();
        while let Some((source, target)) = images.pop() {
            let source = source.canonicalize().map_err(raised)?;
            let binding_error = |reason: &dyn std::fmt::Display| {
                raised(format!(
                    "Cannot relocate virtual concatenation: operand {} would resolve to {} ({reason})",
                    source.display(),
                    target.display()
                ))
            };
            let (resolved, backing) = self
                .resolve(&target)
                .map_err(|error| binding_error(&error))?;
            if source != backing {
                let same = fs::metadata(&source)
                    .and_then(|source| {
                        fs::metadata(&backing).map(|backing| same_file_identity(&source, &backing))
                    })
                    .map_err(|error| binding_error(&error))?;
                if !same {
                    return Err(binding_error(&"a different image"));
                }
            }
            if !checked.insert((source.clone(), resolved.clone())) {
                continue;
            }
            if is_image_concat(&source) {
                let manifest = ConcatManifest::open(&source).map_err(raised)?;
                images.extend(
                    manifest
                        .paths(&source)
                        .into_iter()
                        .zip(manifest.paths(&resolved)),
                );
            } else if ImageHandle::recognizes_miriad(&source, &source) {
                self.check_miriad_files(&source, &resolved)?;
            } else if is_image_expr(&source) {
                if !self.is_owned_copy(&source, &resolved) {
                    images.push((
                        source.join("imageexpr.json"),
                        resolved.join("imageexpr.json"),
                    ));
                }
                let manifest = read_tool_manifest(&source).map_err(raised)?;
                let directory = std::env::current_dir().map_err(raised)?;
                let operands = ImageToolExpressionInputs::resolve_in_directory(
                    manifest.expression,
                    &directory,
                )
                .map_err(raised)?;
                images.extend(
                    operands
                        .paths_and_entries()
                        .map(|(path, entry)| (path.to_owned(), entry.to_owned())),
                );
            }
        }
        Ok(())
    }

    fn is_owned_copy(&self, source: &Path, target: &Path) -> bool {
        match &self.entries {
            Entries::OwnedMembers(members) if self.removed.is_empty() => target
                .parent()
                .filter(|parent| *parent == self.destination)
                .and_then(|_| members.get(target.file_name()?))
                .is_some_and(|member| *member == source),
            _ => false,
        }
    }

    fn check_miriad_files(&self, source: &Path, target: &Path) -> PyResult<()> {
        let materialized = self.is_owned_copy(source, target);
        for entry in source.read_dir().map_err(raised)? {
            let entry = entry.map_err(raised)?;
            let name = entry.file_name();
            if !name.to_str().is_some_and(MiriadImage::is_image_file)
                || !entry.file_type().map_err(raised)?.is_symlink()
            {
                continue;
            }
            let original = regular_file(entry.path().metadata()).map_err(raised)?;
            if materialized && original.is_some() {
                continue;
            }
            let projected = regular_file(
                self.resolve(&target.join(name))
                    .and_then(|(_, backing)| backing.metadata()),
            )
            .map_err(raised)?;
            let same = match (original, projected) {
                (Some(original), Some(projected)) => same_file_identity(&original, &projected),
                (None, None) => true,
                _ => false,
            };
            if !same {
                return Err(raised(format!(
                    "Cannot relocate virtual concatenation: MIRIAD image file {} would resolve to a different file",
                    entry.path().display()
                )));
            }
        }
        Ok(())
    }

    fn backing(&self, path: &Path) -> io::Result<Option<PathBuf>> {
        if let Ok(relative) = path.strip_prefix(self.destination) {
            return match &self.entries {
                Entries::WholeTree(source) => Ok(Some(join_suffix(source, relative))),
                Entries::OwnedMembers(members) => {
                    let mut parts = relative.components();
                    let Some(first) = parts.next() else {
                        // The new manifest directory exists in the planned
                        // layout even though it has no original directory.
                        return Ok(None);
                    };
                    members
                        .get(first.as_os_str())
                        .map(|member| Some(join_suffix(member, parts.as_path())))
                        .ok_or_else(|| ErrorKind::NotFound.into())
                }
            };
        }
        if path
            .ancestors()
            .any(|ancestor| self.removed.contains(ancestor))
        {
            return Err(ErrorKind::NotFound.into());
        }
        Ok(Some(path.to_path_buf()))
    }

    fn resolve(&self, path: &Path) -> io::Result<(PathBuf, PathBuf)> {
        let mut requested = std::path::absolute(path)?;
        for _ in 0..=40 {
            let mut parts = requested.components();
            let mut resolved = PathBuf::new();
            let mut redirect = None;
            while let Some(part) = parts.next() {
                match part {
                    Component::ParentDir => {
                        resolved.pop();
                    }
                    Component::CurDir => {}
                    _ => resolved.push(part.as_os_str()),
                }
                if !matches!(part, Component::Normal(_)) {
                    continue;
                }
                let Some(backing) = self.backing(&resolved)? else {
                    continue;
                };
                if backing.symlink_metadata()?.is_symlink() {
                    let target = backing.read_link()?;
                    resolved.pop();
                    redirect = Some(resolved.join(target).join(parts.as_path()));
                    break;
                }
            }
            if let Some(redirected) = redirect {
                requested = redirected;
            } else {
                let backing = self
                    .backing(&resolved)?
                    .ok_or_else(|| io::Error::other("the operand is the virtual output itself"))?;
                return Ok((resolved, backing));
            }
        }
        Err(io::Error::other(
            "too many symbolic links resolving a concatenation member",
        ))
    }
}

fn regular_file(metadata: io::Result<fs::Metadata>) -> io::Result<Option<fs::Metadata>> {
    match metadata {
        Ok(metadata) if metadata.is_file() => Ok(Some(metadata)),
        Ok(_) => Ok(None),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn join_suffix(root: &Path, suffix: &Path) -> PathBuf {
    if suffix.as_os_str().is_empty() {
        root.to_path_buf()
    } else {
        root.join(suffix)
    }
}
