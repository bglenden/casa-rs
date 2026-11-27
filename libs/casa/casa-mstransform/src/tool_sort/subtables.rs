// SPDX-License-Identifier: LGPL-3.0-or-later
//! Materialize the table-valued keywords of a sorted copy.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use casa_core::{RecordValue, ScalarValue, Value};
use casa_tables::{Table, TableOptions, TablePartNames, add_directory, strip_directory};

use casa_ms::subtable_path_from_main;
use casa_ms::{MsError, MsResult, SubtableId};

pub(super) fn check_named_output(
    input: &Path,
    output: &Path,
    replace_keyword: Option<(&Path, &str)>,
) -> MsResult<PathBuf> {
    let root = reference_root(input)?;
    let mut staging_parent = output
        .parent()
        .ok_or_else(|| invalid(output, "output has no parent"))?
        .to_path_buf();
    let mut visited = BTreeSet::new();
    let mut pending = vec![(input.to_path_buf(), false)];
    let standard = SubtableId::ALL_REQUIRED
        .iter()
        .chain(SubtableId::ALL_OPTIONAL);
    while let Some((path, subtable)) = pending.pop() {
        let path = path.canonicalize().map_err(|error| invalid(&path, error))?;
        if path.starts_with(output) {
            return Err(invalid(output, "output overlaps a source table"));
        }
        if subtable && path != root && staging_parent.starts_with(&path) {
            if replace_keyword.is_none() {
                return Err(invalid(output, "output overlaps a source table"));
            }
            staging_parent = path
                .parent()
                .ok_or_else(|| invalid(output, "no staging parent outside the source table"))?
                .to_path_buf();
        }
        if !visited.insert(path.clone()) {
            continue;
        }
        match Table::immediate_part_names(&path, &path)? {
            TablePartNames::Reference(parent) => pending.push((parent, subtable)),
            TablePartNames::Concatenation { parts, .. } => {
                pending.extend(parts.into_iter().map(|part| (part, subtable)));
            }
            TablePartNames::Plain(_) => {}
        }
        let table = Table::open(TableOptions::new(&path))?;
        for field in table.keywords().fields() {
            let standard = standard.clone().any(|id| id.name() == field.name);
            let link = match &field.value {
                Value::TableRef(link) if field.name != "SORTED_TABLE" => Some(link),
                Value::Scalar(ScalarValue::String(link)) if standard => Some(link),
                Value::Record(nested) => {
                    protect_keyword_links(nested, &path, &mut pending);
                    None
                }
                _ => None,
            };
            if let Some(link) = link {
                let linked = add_directory(link, &path);
                // The designated result keyword is republished with the new table.
                // Protect standard MS subtables and all other names for that table.
                if standard || linked != output || !matches!(replace_keyword,
                    Some((owner, name)) if name == field.name && Table::keyword_descriptor_path(&path)? == owner)
                {
                    pending.push((linked, true));
                }
            }
        }
        for column in table
            .schema()
            .into_iter()
            .flat_map(|schema| schema.columns())
        {
            if let Some(keys) = table.column_keywords(column.name()) {
                protect_keyword_links(keys, &path, &mut pending);
            }
        }
    }
    Ok(staging_parent)
}

fn protect_keyword_links(record: &RecordValue, path: &Path, pending: &mut Vec<(PathBuf, bool)>) {
    for field in record.fields() {
        match &field.value {
            Value::TableRef(link) => pending.push((add_directory(link, path), true)),
            Value::Record(nested) => protect_keyword_links(nested, path, pending),
            _ => {}
        }
    }
}

pub(super) fn copy_links(
    input: &Table,
    keywords: &mut RecordValue,
    copy: &mut Table,
    output: &Path,
    held: &BTreeSet<PathBuf>,
) -> MsResult<()> {
    let input_path = table_path(input)?;
    for id in SubtableId::ALL_REQUIRED
        .iter()
        .chain(SubtableId::ALL_OPTIONAL)
    {
        if let Some(Value::Scalar(ScalarValue::String(reference))) = keywords.get(id.name()) {
            keywords.upsert(id.name(), Value::TableRef(reference.clone()));
        }
    }
    let mut copies = Copies {
        by_output: BTreeMap::new(),
        copying: Vec::new(),
        held,
    };
    copies.rewrite(keywords, input_path, output)?;
    copies.rewrite_columns(input, copy, input_path, output)
}

/// Copy the remaining table and column links after the standard subtables
/// have been installed under their MeasurementSet names.
pub(crate) fn copy_additional_links(
    input: &Path,
    copy: &mut Table,
    output: &Path,
    by_output: BTreeMap<PathBuf, PathBuf>,
) -> MsResult<()> {
    let mut copies = Copies {
        by_output,
        copying: Vec::new(),
        held: &BTreeSet::new(),
    };
    let standard = SubtableId::ALL_REQUIRED
        .iter()
        .chain(SubtableId::ALL_OPTIONAL);
    let mut extra = RecordValue::new(
        copy.keywords()
            .fields()
            .iter()
            .filter(|field| {
                matches!(&field.value, Value::TableRef(_))
                    && !standard.clone().any(|id| id.name() == field.name)
            })
            .cloned()
            .collect(),
    );
    for field in extra.fields() {
        copy.keywords_mut().remove(&field.name);
    }
    copies.rewrite(&mut extra, input, output)?;
    for field in extra.fields() {
        copy.keywords_mut().upsert(&field.name, field.value.clone());
    }
    let column_links: Vec<_> = copy
        .schema()
        .into_iter()
        .flat_map(|schema| schema.columns())
        .filter_map(|column| {
            let name = column.name();
            let record = copy.column_keywords(name)?;
            record
                .fields()
                .iter()
                .any(|field| matches!(&field.value, Value::TableRef(_)))
                .then(|| (name.to_owned(), record.clone()))
        })
        .collect();
    for (name, mut record) in column_links {
        copies.rewrite(&mut record, input, output)?;
        copy.set_column_keywords(&name, record);
    }
    Ok(())
}

/// Keep the donor's ordinary keywords and carry over independently copied
/// table links from the reconciled attached MS.
pub(crate) fn copy_concatenation_links(
    attached: &Table,
    donor: &Table,
    output_table: &mut Table,
    output: &Path,
    held: &BTreeSet<PathBuf>,
) -> MsResult<()> {
    let attached_path = table_path(attached)?;
    let donor_path = table_path(donor)?;
    let mut copies = Copies {
        by_output: BTreeMap::new(),
        copying: Vec::new(),
        held,
    };
    let mut keywords = standard_link_keywords(donor, donor_path);
    keywords.remove("SORTED_TABLE");
    let attached_keywords = standard_link_keywords(attached, attached_path);
    overlay_copied_links(
        &mut copies,
        &mut keywords,
        &attached_keywords,
        attached_path,
        donor_path,
        output,
    )?;
    *output_table.keywords_mut() = keywords;
    for column in donor
        .schema()
        .into_iter()
        .flat_map(|schema| schema.columns())
    {
        let name = column.name();
        let mut keywords = donor.column_keywords(name).cloned().unwrap_or_default();
        if let Some(attached_keywords) = attached.column_keywords(name) {
            overlay_copied_links(
                &mut copies,
                &mut keywords,
                attached_keywords,
                attached_path,
                donor_path,
                output,
            )?;
        } else {
            anchor_links(&mut keywords, donor_path);
        }
        output_table.set_column_keywords(name, keywords);
    }
    Ok(())
}

fn standard_link_keywords(table: &Table, root: &Path) -> RecordValue {
    let mut keywords = table.keywords().clone();
    for &id in SubtableId::ALL_REQUIRED
        .iter()
        .chain(SubtableId::ALL_OPTIONAL)
    {
        if let Some(path) = subtable_path_from_main(table, root, id) {
            keywords.upsert(
                id.name(),
                Value::TableRef(path.to_string_lossy().into_owned()),
            );
        }
    }
    keywords
}

fn anchor_links(keywords: &mut RecordValue, input: &Path) {
    for field in keywords.fields_mut() {
        if let Value::TableRef(link) = &mut field.value {
            *link = add_directory(link, input).to_string_lossy().into_owned();
        }
    }
}

fn overlay_copied_links(
    copies: &mut Copies<'_>,
    output_keys: &mut RecordValue,
    attached: &RecordValue,
    attached_path: &Path,
    donor_path: &Path,
    output: &Path,
) -> MsResult<()> {
    anchor_links(output_keys, donor_path);
    let mut links = RecordValue::new(
        attached
            .fields()
            .iter()
            .filter(|field| matches!(&field.value, Value::TableRef(_)))
            .cloned()
            .collect(),
    );
    for field in links.fields() {
        output_keys.remove(&field.name);
    }
    copies.rewrite(&mut links, attached_path, output)?;
    for field in links.fields() {
        output_keys.upsert(&field.name, field.value.clone());
    }
    Ok(())
}

struct Copies<'a> {
    by_output: BTreeMap<PathBuf, PathBuf>,
    copying: Vec<PathBuf>,
    held: &'a BTreeSet<PathBuf>,
}

impl Copies<'_> {
    fn rewrite(&mut self, record: &mut RecordValue, input: &Path, output: &Path) -> MsResult<()> {
        let root = reference_root(input)?;
        let mut omitted = Vec::new();
        for field in record.fields_mut() {
            let Value::TableRef(reference) = &mut field.value else {
                continue;
            };
            let declared = add_directory(reference, input);
            let source = declared
                .canonicalize()
                .map_err(|error| invalid(&declared, error))?;
            if reference_root(&source)? == root {
                omitted.push(field.name.clone());
                continue;
            }
            let name = declared
                .file_name()
                .ok_or_else(|| invalid(&declared, "linked table has no name"))?;
            let destination = output.join(name);
            match self.by_output.get(&destination) {
                Some(previous) if previous != &source => {
                    return Err(invalid(
                        &destination,
                        "different table links have the same name",
                    ));
                }
                Some(_) => {}
                None => self.copy(&source, &destination)?,
            }
            *reference = strip_directory(&destination, output);
        }
        for name in omitted {
            record.remove(&name);
        }
        Ok(())
    }

    fn rewrite_columns(
        &mut self,
        input_table: &Table,
        copy: &mut Table,
        input: &Path,
        output: &Path,
    ) -> MsResult<()> {
        for column in input_table
            .schema()
            .into_iter()
            .flat_map(|schema| schema.columns())
        {
            let name = column.name();
            if let Some(record) = input_table.column_keywords(name) {
                let mut record = record.clone();
                self.rewrite(&mut record, input, output)?;
                copy.set_column_keywords(name, record);
            }
        }
        Ok(())
    }

    fn copy(&mut self, source: &Path, output: &Path) -> MsResult<()> {
        if self.copying.iter().any(|path| path == source) {
            return Err(invalid(source, "cyclic table link"));
        }
        let table = open_source(source, self.held.contains(source))?;
        if matches!(
            Table::immediate_part_names(source, source)?,
            TablePartNames::Plain(_)
        ) {
            // A physical copy retains column metadata and nested table
            // directories, as casacore does for plain subtable deep copies.
            std::fs::create_dir_all(output.parent().expect("staging table parent"))
                .map_err(|error| invalid(output, error))?;
            casa_tables::publication::copy_locked_table_tree(source, output)
                .map_err(|error| invalid(source, error))?;
        } else {
            self.copying.push(source.to_path_buf());
            let mut copy = Table::open(TableOptions::new(source))?;
            let mut keywords = table.keywords().clone();
            self.rewrite(&mut keywords, source, output)?;
            *copy.keywords_mut() = keywords;
            self.rewrite_columns(&table, &mut copy, source, output)?;
            copy.deep_copy(TableOptions::new(output))?;
            self.copying.pop();
        }
        self.by_output
            .insert(output.to_path_buf(), source.to_path_buf());
        Ok(())
    }
}

pub(super) fn table_path(table: &Table) -> MsResult<&Path> {
    table
        .path()
        .ok_or_else(|| MsError::InvalidInput("the sorted source has no table path".into()))
}

pub(super) fn reference_root(path: &Path) -> MsResult<PathBuf> {
    let mut path = path.canonicalize().map_err(|error| invalid(path, error))?;
    for _ in 0..64 {
        let TablePartNames::Reference(parent) = Table::immediate_part_names(&path, &path)? else {
            return Ok(path);
        };
        path = parent
            .canonicalize()
            .map_err(|error| invalid(&parent, error))?;
    }
    Err(invalid(&path, "reference table chain is too deep"))
}

fn open_source(path: &Path, already_held: bool) -> MsResult<Table> {
    let options = TableOptions::new(path);
    #[cfg(unix)]
    if !already_held {
        return Ok(Table::open_with_lock(
            options,
            casa_tables::LockOptions::new(casa_tables::LockMode::PermanentLockingWait)
                .read_only(true),
        )?);
    }
    #[cfg(not(unix))]
    let _ = already_held;
    Ok(Table::open(options)?)
}

fn invalid(path: &Path, error: impl std::fmt::Display) -> MsError {
    MsError::InvalidInput(format!("copy linked table {}: {error}", path.display()))
}
