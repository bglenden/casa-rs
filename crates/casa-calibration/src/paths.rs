// SPDX-License-Identifier: LGPL-3.0-or-later
//! Resolve existing ancestors as well as paths of products not yet created.
use std::collections::VecDeque;
use std::io;
use std::path::{Component, Path, PathBuf};

pub(crate) fn normalized(path: &Path) -> io::Result<PathBuf> {
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

pub(crate) fn overlap(left: &Path, right: &Path) -> io::Result<bool> {
    let left = normalized(left)?;
    let right = normalized(right)?;
    Ok(left.starts_with(&right) || right.starts_with(&left))
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
