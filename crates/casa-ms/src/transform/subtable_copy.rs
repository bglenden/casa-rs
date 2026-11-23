// SPDX-License-Identifier: LGPL-3.0-or-later

use std::fs;
use std::path::{Component, Path, PathBuf};

use casa_tables::{Table, add_directory, lexical_absolute, strip_directory};
use casa_types::Value;

use super::{MsTransformError, SubtableId};

pub(super) fn copy_subtables(
    input_path: &Path,
    metadata_path: &Path,
    output_path: &Path,
    main: &mut Table,
) -> Result<(), MsTransformError> {
    let copy_error = |source: std::io::Error| MsTransformError::PrepareOutput {
        path: output_path.display().to_string(),
        reason: source.to_string(),
    };
    for id in SubtableId::ALL_REQUIRED
        .iter()
        .chain(SubtableId::ALL_OPTIONAL)
    {
        let source = input_path.join(id.name());
        if source.exists() {
            copy_dir_recursive(&source, &output_path.join(id.name())).map_err(copy_error)?;
        }
    }

    // CASA copies CALDEVICE, SYSPOWER and ASDM metadata alongside the
    // standard MeasurementSet subtables.
    let owner = lexical_absolute(metadata_path);
    let output_path = lexical_absolute(output_path);
    for field in main.keywords_mut().fields_mut() {
        let Value::TableRef(reference) = &mut field.value else {
            continue;
        };
        let Some(name) = attached_subtable_name(&owner, &field.name, reference) else {
            continue;
        };
        let source = owner.join(&name);
        let destination = output_path.join(&name);
        if output_path.starts_with(&source) {
            return Err(copy_error(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("output is inside input subtable {}", source.display()),
            )));
        }
        if !destination.exists() {
            copy_dir_recursive(&source, &destination).map_err(copy_error)?;
        }
        *reference = strip_directory(&destination, &output_path);
    }
    Ok(())
}

pub(super) fn attached_subtable_name(
    owner: &Path,
    keyword: &str,
    reference: &str,
) -> Option<PathBuf> {
    if keyword == "SORTED_TABLE"
        || SubtableId::ALL_REQUIRED
            .iter()
            .chain(SubtableId::ALL_OPTIONAL)
            .any(|id| id.name() == keyword)
    {
        return None;
    }
    let source = lexical_absolute(&add_directory(reference, owner));
    let name = source.strip_prefix(owner).ok()?;
    let mut components = name.components();
    if !matches!(components.next(), Some(Component::Normal(_)))
        || components.next().is_some()
        || !source.join("table.dat").is_file()
    {
        return None;
    }
    Some(name.to_path_buf())
}

pub(super) fn copy_dir_recursive(source: &Path, destination: &Path) -> std::io::Result<()> {
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            copy_dir_recursive(&source_path, &destination_path)?;
        } else if file_type.is_file() {
            fs::copy(&source_path, &destination_path)?;
        } else if file_type.is_symlink() {
            let target = fs::read_link(&source_path)?;
            #[cfg(unix)]
            std::os::unix::fs::symlink(target, &destination_path)?;
        }
    }
    Ok(())
}
