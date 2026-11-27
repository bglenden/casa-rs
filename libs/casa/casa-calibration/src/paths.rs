// SPDX-License-Identifier: LGPL-3.0-or-later
//! Resolve existing ancestors as well as paths of products not yet created.
use std::collections::VecDeque;
use std::io;
use std::path::{Component, Path, PathBuf};

/// Normalize a product path by resolving existing symlinks and `.`/`..` components.
pub fn normalized(path: &Path) -> io::Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut pending: VecDeque<_> = absolute
        .components()
        .map(|c| c.as_os_str().to_owned())
        .collect();
    let mut result = PathBuf::new();
    let mut links = 0;
    while let Some(part) = pending.pop_front() {
        match Path::new(&part)
            .components()
            .next()
            .expect("path component")
        {
            Component::ParentDir => {
                result.pop();
            }
            Component::CurDir => {}
            Component::RootDir | Component::Prefix(_) => result.push(&part),
            Component::Normal(_) => {
                result.push(&part);
                match std::fs::symlink_metadata(&result) {
                    Ok(meta) if meta.is_symlink() => {
                        links += 1;
                        if links > 40 {
                            return Err(io::Error::other(
                                "too many symbolic links in scientific product path",
                            ));
                        }
                        let target = std::fs::read_link(&result)?;
                        result.pop();
                        for component in target.components().rev() {
                            pending.push_front(component.as_os_str().to_owned());
                        }
                    }
                    Ok(_) => result = result.canonicalize()?,
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e),
                }
            }
        }
    }
    Ok(result)
}

/// Return whether either normalized path is a prefix of the other.
pub fn overlap(left: &Path, right: &Path) -> io::Result<bool> {
    let left = normalized(left)?;
    let right = normalized(right)?;
    Ok(left.starts_with(&right) || right.starts_with(&left))
}

/// The plain table whose record owns inherited subtable links.
pub fn subtable_owner(
    table: &casa_tables::Table,
    path: &Path,
) -> Result<PathBuf, casa_tables::TableError> {
    if table.is_reference_table()?
        && let Some(plain) = table.cache_plain_table_paths()?.into_iter().next()
    {
        return Ok(plain);
    }
    Ok(path.to_path_buf())
}

/// Copy a table tree, keeping its physical storage and relative links.
pub(crate) fn copy_dir_recursive(source: &Path, destination: &Path) -> std::io::Result<()> {
    copy_table_tree(source, destination, false)
}

/// Copy table files by value before editing, including regular file links.
/// Directory links remain available for the caller to resolve as subtables.
pub(crate) fn copy_editable_table_tree(source: &Path, destination: &Path) -> io::Result<()> {
    copy_table_tree(source, destination, true)
}

fn copy_table_tree(source: &Path, destination: &Path, detach_file_links: bool) -> io::Result<()> {
    std::fs::create_dir_all(destination)?;
    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            copy_table_tree(&source_path, &destination_path, detach_file_links)?;
        } else if file_type.is_file()
            || (detach_file_links && file_type.is_symlink() && source_path.is_file())
        {
            std::fs::copy(&source_path, &destination_path)?;
        } else if file_type.is_symlink() {
            let target = std::fs::read_link(&source_path)?;
            std::os::unix::fs::symlink(target, destination_path)?;
        }
    }
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn dangling_report_link_resolves_to_the_future_product() {
        let dir = tempfile::tempdir().unwrap();
        let product = dir.path().join("future.cal");
        let report = dir.path().join("report");
        std::os::unix::fs::symlink(product.join("table.dat"), &report).unwrap();
        assert!(overlap(&report, &product).unwrap());
    }

    #[test]
    fn a_parent_component_follows_the_symlink_target() {
        let dir = tempfile::tempdir().unwrap();
        let product = dir.path().join("existing.cal");
        std::fs::create_dir_all(product.join("ANTENNA")).unwrap();
        std::fs::write(product.join("table.dat"), "keep").unwrap();
        let alias = dir.path().join("alias");
        std::os::unix::fs::symlink(product.join("ANTENNA"), &alias).unwrap();
        assert!(overlap(&alias.join("../table.dat"), &product).unwrap());
    }
}
