// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use casa_tables::{
    Table, TableOptions, TablePartNames, add_directory, lexical_absolute, strip_directory,
};
use casa_types::{RecordValue, Value};

use super::{Member, invalid};
use crate::MsResult;

pub(super) type Locations = BTreeMap<PathBuf, PathBuf>;
pub(super) type FilesystemLinks = Vec<(PathBuf, PathBuf)>;

/// Plan rewrites for symbolic entries whose lexical target would change.
/// Do not follow a symbolic directory while walking the physical member.
pub(super) fn filesystem_links(
    member: &Member,
    locations: &Locations,
    output: &Path,
) -> MsResult<FilesystemLinks> {
    let mut directories = vec![member.physical.clone()];
    let mut links = Vec::new();
    while let Some(directory) = directories.pop() {
        for entry in directory.read_dir().map_err(invalid)? {
            let entry = entry.map_err(invalid)?;
            let kind = entry.file_type().map_err(invalid)?;
            let path = entry.path();
            if kind.is_dir() {
                directories.push(path);
            } else if kind.is_symlink() {
                let payload = path.read_link().map_err(invalid)?;
                let old = lexical_absolute(&directory.join(&payload));
                if old.starts_with(output)
                    || path
                        .canonicalize()
                        .is_ok_and(|referent| referent.starts_with(output))
                {
                    return Err(invalid(format!(
                        "MultiMS destination is used by {}",
                        path.display()
                    )));
                }
                let new = member
                    .target
                    .join(path.strip_prefix(&member.physical).unwrap());
                let parent = new.parent().expect("staged link parent");
                let referent = relocated_path(&member.physical, &member.target, &old, locations);
                if lexical_absolute(&parent.join(&payload)) != referent {
                    let stage = member.target.parent().expect("staged member parent");
                    let payload = if referent.starts_with(stage) {
                        relative_file_path(parent, &referent)
                    } else {
                        referent
                    };
                    links.push((new, payload));
                }
            }
        }
    }
    Ok(links)
}

fn relative_file_path(mut from: &Path, target: &Path) -> PathBuf {
    let mut path = PathBuf::new();
    while !target.starts_with(from) {
        path.push("..");
        from = from.parent().expect("paths share the staging directory");
    }
    let suffix = target.strip_prefix(from).unwrap();
    if path.as_os_str().is_empty() && suffix.as_os_str().is_empty() {
        path.push(".");
    } else {
        path.push(suffix);
    }
    path
}

pub(super) fn apply_filesystem_links(links: FilesystemLinks) -> MsResult<()> {
    for (path, target) in links {
        #[cfg(unix)]
        {
            fs::remove_file(&path).map_err(invalid)?;
            std::os::unix::fs::symlink(target, path).map_err(invalid)?;
        }
        #[cfg(not(unix))]
        {
            let _ = (path, target);
            return Err(invalid("symbolic-link MultiMS input is unsupported"));
        }
    }
    Ok(())
}

/// Return the top-level member containing the base when the entire
/// reference chain moves into the private MultiMS stage.
pub(super) fn owned_reference_base(
    path: &Path,
    locations: &Locations,
) -> MsResult<Option<PathBuf>> {
    let mut path = path.to_path_buf();
    for _ in 0..64 {
        match Table::immediate_part_names(&path, &path)? {
            TablePartNames::Reference(parent) => {
                path = parent.canonicalize().map_err(invalid)?;
                if !locations.keys().any(|member| path.starts_with(member)) {
                    return Ok(None);
                }
            }
            TablePartNames::Plain(_) => {
                return Ok(locations
                    .keys()
                    .find(|member| path.starts_with(member))
                    .cloned());
            }
            TablePartNames::Concatenation { .. } => return Ok(None),
        }
    }
    Err(invalid("member reference chain is too deep"))
}

pub(super) enum MemberEdit {
    None,
    Parts(Vec<PathBuf>),
    Keywords {
        keywords: RecordValue,
        columns: Vec<(String, RecordValue)>,
    },
}

impl MemberEdit {
    pub(super) fn prepare(member: &Member, locations: &Locations) -> MsResult<Self> {
        let source = &member.physical;
        let target = &member.target;
        match Table::immediate_part_names(source, source)? {
            TablePartNames::Plain(_) => {
                let table = Table::open(TableOptions::new(source))?;
                let mut keywords = table.keywords().clone();
                let changed = relocate_links(&mut keywords, source, target, locations);
                let mut columns = Vec::new();
                for column in table
                    .schema()
                    .into_iter()
                    .flat_map(|schema| schema.columns())
                {
                    let name = column.name();
                    if let Some(keys) = table.column_keywords(name) {
                        let mut keys = keys.clone();
                        if relocate_links(&mut keys, source, target, locations) {
                            columns.push((name.to_owned(), keys));
                        }
                    }
                }
                Ok(if changed || !columns.is_empty() {
                    Self::Keywords { keywords, columns }
                } else {
                    Self::None
                })
            }
            original => {
                let before = parts(original);
                let after = parts(Table::immediate_part_names(source, target)?);
                let mapped = before
                    .iter()
                    .map(|part| relocated_path(source, target, part, locations))
                    .collect::<Vec<_>>();
                Ok(if mapped == after {
                    Self::None
                } else {
                    Self::Parts(mapped)
                })
            }
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        matches!(self, Self::None)
    }

    pub(super) fn apply(self, target: &Path) -> MsResult<()> {
        match self {
            Self::None => {}
            Self::Parts(parts) => Table::set_staged_table_parts(target, &parts)?,
            Self::Keywords { keywords, columns } => {
                let mut table = Table::open(TableOptions::new(target))?;
                *table.keywords_mut() = keywords;
                for (column, keys) in columns {
                    table.set_column_keywords(column, keys);
                }
                table.save_metadata_only(TableOptions::new(target))?;
            }
        }
        Ok(())
    }
}

fn parts(layout: TablePartNames) -> Vec<PathBuf> {
    match layout {
        TablePartNames::Reference(path) => vec![path],
        TablePartNames::Concatenation { parts, .. } => parts,
        TablePartNames::Plain(_) => Vec::new(),
    }
}

fn relocate_links(
    record: &mut RecordValue,
    source: &Path,
    target: &Path,
    locations: &Locations,
) -> bool {
    let mut changed = false;
    for field in record.fields_mut() {
        match &mut field.value {
            Value::Record(nested) => changed |= relocate_links(nested, source, target, locations),
            Value::TableRef(link) => {
                let old = add_directory(link, source);
                let relocated = relocated_path(source, target, &old, locations);
                if lexical_absolute(&add_directory(link, target)) != relocated {
                    *link = strip_directory(&relocated, target);
                    changed = true;
                }
            }
            _ => {}
        }
    }
    changed
}

fn relocated_path(source: &Path, target: &Path, path: &Path, locations: &Locations) -> PathBuf {
    let path = lexical_absolute(path);
    if let Ok(relative) = path.strip_prefix(source) {
        return target.join(relative);
    }
    locations
        .iter()
        .find_map(|(source, target)| {
            path.strip_prefix(source)
                .ok()
                .map(|relative| target.join(relative))
        })
        .unwrap_or(path)
}

pub(super) fn stage_alias(member: &Member, locations: &Locations) -> MsResult<()> {
    let referent = locations.get(&member.physical).unwrap_or(&member.physical);
    let parent = member.target.parent().expect("member staging directory");
    let referent = referent.strip_prefix(parent).unwrap_or(referent);
    #[cfg(unix)]
    std::os::unix::fs::symlink(referent, &member.target).map_err(invalid)?;
    #[cfg(not(unix))]
    {
        let _ = referent;
        return Err(invalid("symbolic-link MultiMS input is unsupported"));
    }
    #[cfg(unix)]
    Ok(())
}

pub(super) fn materialize_member(member: &Member, locations: &Locations) -> MsResult<()> {
    // Reference-table keywords are written through to their parent. Give a
    // staged copy its own data and keywords before replacing any subtable.
    let mut table = Table::open(TableOptions::new(&member.physical))?;
    let columns = table
        .schema()
        .into_iter()
        .flat_map(|schema| schema.columns())
        .map(|column| column.name().to_owned())
        .collect::<Vec<_>>();
    relocate_links(
        table.keywords_mut(),
        &member.physical,
        &member.target,
        locations,
    );
    for column in columns {
        if let Some(keys) = table.column_keywords(&column) {
            let mut keys = keys.clone();
            if relocate_links(&mut keys, &member.physical, &member.target, locations) {
                table.set_column_keywords(column, keys);
            }
        }
    }
    table.deep_copy(TableOptions::new(&member.target))?;
    Ok(())
}

pub(super) fn privatize_descriptor(target: &Path) -> MsResult<()> {
    for name in ["table.dat", "table.info"] {
        let file = target.join(name);
        if file
            .symlink_metadata()
            .is_ok_and(|entry| entry.is_symlink())
        {
            let replacement = tempfile::NamedTempFile::new_in(target).map_err(invalid)?;
            fs::copy(&file, replacement.path()).map_err(invalid)?;
            replacement.persist(file).map_err(invalid)?;
        }
    }
    Ok(())
}
