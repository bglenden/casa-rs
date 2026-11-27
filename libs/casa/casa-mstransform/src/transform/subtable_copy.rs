// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeMap;
use std::fs;
use std::path::{Component, Path, PathBuf};

use casa_core::Value;
use casa_tables::{
    Table, TableOptions, TablePartNames, add_directory, lexical_absolute, strip_directory,
};

use super::{MeasurementSet, MsTransformError, SubtableId};

pub(super) fn input_subtable_path(
    input: &MeasurementSet,
    id: SubtableId,
) -> Result<&Path, MsTransformError> {
    input
        .subtable(id)
        .and_then(Table::path)
        .ok_or_else(|| MsTransformError::SpectralMetadata {
            path: input
                .path()
                .unwrap_or_else(|| Path::new("<memory>"))
                .display()
                .to_string(),
            reason: format!("missing persisted {} subtable", id.name()),
        })
}

pub(super) fn copy_subtables(
    input: &MeasurementSet,
    metadata_path: &Path,
    output_path: &Path,
    main: &mut Table,
    copy_external_links: bool,
) -> Result<(), MsTransformError> {
    fs::create_dir_all(output_path).map_err(|error| copy_error(output_path, error))?;
    let mut copied = BTreeMap::new();
    for id in SubtableId::ALL_REQUIRED
        .iter()
        .chain(SubtableId::ALL_OPTIONAL)
    {
        let Some(table) = input.subtable(*id) else {
            continue;
        };
        let Some(source) = table.path() else {
            continue;
        };
        let destination = output_path.join(id.name());
        copy_attached_subtable(source, output_path, &destination)?;
        if copy_external_links {
            copied.insert(
                destination,
                source
                    .canonicalize()
                    .map_err(|error| copy_error(output_path, error))?,
            );
        }
        main.keywords_mut()
            .upsert(id.name(), Value::TableRef(id.name().into()));
    }

    if copy_external_links {
        return crate::tool_sort::subtables::copy_additional_links(
            metadata_path,
            main,
            output_path,
            copied,
        )
        .map_err(|error| MsTransformError::PrepareOutput {
            path: output_path.display().to_string(),
            reason: error.to_string(),
        });
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
        if !destination.exists() {
            copy_attached_subtable(&source, &output_path, &destination)?;
        }
        *reference = strip_directory(&destination, &output_path);
    }
    Ok(())
}

fn copy_attached_subtable(
    source: &Path,
    output_root: &Path,
    destination: &Path,
) -> Result<(), MsTransformError> {
    let io_error = |error| copy_error(output_root, error);
    let source = source.canonicalize().map_err(io_error)?;
    if output_root
        .canonicalize()
        .map_err(io_error)?
        .starts_with(&source)
    {
        return Err(MsTransformError::PrepareOutput {
            path: output_root.display().to_string(),
            reason: format!("output is inside input subtable {}", source.display()),
        });
    }
    let table_error = |error| MsTransformError::MutateMeasurementSet {
        path: source.display().to_string(),
        source: Box::new(error),
    };
    let options = TableOptions::new(&source);
    #[cfg(unix)]
    let table = Table::open_with_lock(
        options,
        casa_tables::LockOptions::new(casa_tables::LockMode::PermanentLockingWait).read_only(true),
    )
    .map_err(table_error)?;
    #[cfg(not(unix))]
    let table = Table::open(options).map_err(table_error)?;
    if !matches!(
        Table::immediate_part_names(&source, &source).map_err(table_error)?,
        TablePartNames::Plain(_)
    ) {
        // Reference and concatenated descriptors still name backing tables.
        // Write their selected rows to an independent output table.
        table
            .deep_copy(TableOptions::new(destination))
            .map_err(table_error)
    } else {
        casa_tables::publication::copy_locked_table_tree(&source, destination).map_err(io_error)
    }
}

fn copy_error(output_path: &Path, source: std::io::Error) -> MsTransformError {
    MsTransformError::PrepareOutput {
        path: output_path.display().to_string(),
        reason: source.to_string(),
    }
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
    casa_tables::publication::copy_locked_table_tree(&source.canonicalize()?, destination)
}
