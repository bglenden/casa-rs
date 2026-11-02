// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's virtual model: a field's model visibilities held in the
//! MeasurementSet as a recipe instead of a `MODEL_DATA` column
//! (`refim::VisModelData`, `synthesis/TransformMachines2/VisModelData.cc`
//! of CASA 6.7.6). `tclean` with `savemodel='virtual'` writes one
//! (`SIMapperCollection::saveVirtualModel`, through
//! `VisibilityIteratorImpl2::writeModel`), and CASA's visibility iterator
//! predicts from it in place of `MODEL_DATA` (`VisModelData::getModelVis`):
//! `mstransform(datacolumn='model')`, the calibrater's solves and `plotms`
//! read it; `delmod` lists and clears it.
//!
//! # Layout
//!
//! A model is filed under a key, `model_<f>[_<f>…]`, naming the fields it
//! covers in ascending order, and each of those fields points to it with a
//! keyword `definedmodel_field_<f>` holding the key. When SOURCE has rows
//! and the first field's `SOURCE_ID` is not -1, the key and the pointers
//! are SOURCE keywords, the key's an `Int` naming the first SOURCE row of
//! that source, and the model record is that row's `SOURCE_MODEL` cell (the
//! column added when SOURCE lacks it); otherwise both are MAIN keywords,
//! the key's holding the record (`VisModelData::putModelRecord`,
//! `putRecordByKey`). CASA 6.7.6 measured on `refim_twochan.ms` (SOURCE
//! rows) and `refim_point.ms` (none) writes exactly this.
//!
//! The record of version 2 (`VisModelData::putModel` with an index
//! combination) is `{version: 2, numft: n, ft_0: …, numcl: m, cl_0: …}`,
//! its entries ([`VirtualModelEntry`]) each
//! `{version: 2, indexcombination, channelselection, container, type}`:
//! the `(FIELD_ID, spectral window, SCAN_NUMBER, STATE_ID)` combinations
//! it predicts for, the channels the writer selected, the serialized
//! `FTMachine` or `ComponentList` that predicts, and `"ftmachine"` or
//! `"componentlist"`. A reader matches a visibility buffer's combination
//! against `indexcombination`; nothing reads `channelselection` back.
//!
//! Clearing follows `VisModelData::clearModel`: every field's model when
//! no field is named ([`crate::scratch_columns`] holds that), else each
//! named field's model, or only its entries' combinations of the named
//! spectral windows ([`clear_virtual_model`]). As there, casa-rs deletes
//! the `FT_MODEL*` image tables of a removed model, but only those in the
//! MeasurementSet's or its SOURCE's directory, so that a path in a record
//! never deletes anything else. A multi-MS is written as one table, where
//! CASA writes each part.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};

use casa_tables::{ColumnSchema, Table};
use casa_types::{ArrayValue, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn};

use crate::schema::SubtableId;
use crate::schema::main_table::VisibilityDataColumn;
use crate::scratch_columns::{Write, change_in_place, held_open, prepare_write};
use crate::selection::MsSelection;
use crate::{MeasurementSet, MsError, MsResult};

/// The SOURCE column holding model records.
const SOURCE_MODEL: &str = "SOURCE_MODEL";

/// What a [`VirtualModelEntry`] predicts with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VirtualModelKind {
    /// A serialized `FTMachine` (`FTMachine::toRecord`) and the gridded
    /// model images it names; `tclean` writes these.
    FtMachine,
    /// A serialized `ComponentList`; `setjy` writes these.
    ComponentList,
}

impl VirtualModelKind {
    /// The entry's `type`.
    const fn type_name(self) -> &'static str {
        match self {
            Self::FtMachine => "ftmachine",
            Self::ComponentList => "componentlist",
        }
    }

    /// The record field counting this kind's entries.
    const fn count_key(self) -> &'static str {
        match self {
            Self::FtMachine => "numft",
            Self::ComponentList => "numcl",
        }
    }

    /// The prefix of this kind's entry fields, `ft_<k>` or `cl_<k>`.
    const fn prefix(self) -> &'static str {
        match self {
            Self::FtMachine => "ft_",
            Self::ComponentList => "cl_",
        }
    }

    const BOTH: [Self; 2] = [Self::FtMachine, Self::ComponentList];
}

/// One model of a version-2 virtual model record: the combinations it
/// predicts for, the channels its writer selected and the machine that
/// predicts (`VisModelData::putModel`'s `modrec`).
#[derive(Clone, Debug, PartialEq)]
pub struct VirtualModelEntry {
    /// An `FTMachine` or a component list.
    pub kind: VirtualModelKind,
    /// `(FIELD_ID, spectral window, SCAN_NUMBER, STATE_ID)` rows, in the
    /// order [`virtual_model_index_combination`] finds them.
    pub index_combination: Vec<[i32; 4]>,
    /// `(spectral window, first channel, channels, increment)` rows of the
    /// writer's channel selection, one per selected range.
    pub channel_selection: Vec<[i32; 4]>,
    /// The serialized `FTMachine` or `ComponentList`.
    pub container: RecordValue,
}

impl VirtualModelEntry {
    /// The fields the entry covers, ascending and each once.
    #[must_use]
    pub fn fields(&self) -> Vec<i32> {
        let mut fields: Vec<i32> = self.index_combination.iter().map(|row| row[0]).collect();
        fields.sort_unstable();
        fields.dedup();
        fields
    }

    /// The key CASA files the entry's model under: `model` and each field,
    /// joined by `_`.
    #[must_use]
    pub fn key(&self) -> String {
        model_key(&self.fields())
    }

    /// The entry as CASA records it: `version`, `indexcombination`,
    /// `channelselection`, `container` and `type`, in that order.
    #[must_use]
    pub fn to_record(&self) -> RecordValue {
        let mut record = RecordValue::default();
        record.upsert("version", int(2));
        record.upsert("indexcombination", matrix(&self.index_combination));
        record.upsert("channelselection", matrix(&self.channel_selection));
        record.upsert("container", Value::Record(self.container.clone()));
        record.upsert(
            "type",
            Value::Scalar(ScalarValue::String(self.kind.type_name().to_string())),
        );
        record
    }

    /// The entries of a model record, `ft_<k>` then `cl_<k>`.
    ///
    /// # Errors
    ///
    /// [`MsError::InvalidInput`] for a record of another version than 2,
    /// whose entries name fields and windows instead of combinations, and
    /// for an entry missing a field of version 2.
    pub fn entries(record: &RecordValue) -> MsResult<Vec<Self>> {
        if !record.is_empty() && int_field(record, "version") != Some(2) {
            return Err(invalid("the virtual model record is not of version 2"));
        }
        let mut entries = Vec::new();
        for kind in VirtualModelKind::BOTH {
            for index in 0..count(record, kind) {
                let name = format!("{}{index}", kind.prefix());
                let Some(Value::Record(entry)) = record.get(&name) else {
                    return Err(invalid(format!("the virtual model has no {name}")));
                };
                let rows = |field: &str| {
                    matrix_rows(entry.get(field))
                        .ok_or_else(|| invalid(format!("{name} has no {field} matrix")))
                };
                let Some(Value::Record(container)) = entry.get("container") else {
                    return Err(invalid(format!("{name} has no container")));
                };
                entries.push(Self {
                    kind,
                    index_combination: rows("indexcombination")?,
                    channel_selection: rows("channelselection")?,
                    container: container.clone(),
                });
            }
        }
        Ok(entries)
    }
}

/// A field's virtual model as CASA finds it (`VisModelData::isModelDefined`
/// and `getModelRecord`): its key, the SOURCE row holding it when it is
/// filed under SOURCE, and the record.
#[derive(Clone, Debug, PartialEq)]
pub struct VirtualModelRecord {
    /// The key, `model_<f>…`.
    pub key: String,
    /// The SOURCE row whose `SOURCE_MODEL` holds the record; `None` for a
    /// record among MAIN's keywords.
    pub source_row: Option<usize>,
    /// The model record.
    pub record: RecordValue,
}

/// Field `field`'s virtual model, or `None` when it has none: the pointer
/// `definedmodel_field_<field>` among SOURCE's keywords when SOURCE has
/// rows and holds it, else among MAIN's, naming a key that is defined.
///
/// # Errors
///
/// When the record cannot be read: a key under SOURCE naming no
/// `SOURCE_MODEL` cell, or a MAIN key that holds no record.
pub fn virtual_model(ms: &MeasurementSet, field: i32) -> MsResult<Option<VirtualModelRecord>> {
    let mut state = ModelState::read(ms)?;
    let Some((key, source_row)) = state.defined_for_field(field) else {
        return Ok(None);
    };
    let record = state.record(ms, &key)?;
    Ok(Some(VirtualModelRecord {
        key,
        source_row: source_row.and_then(|row| usize::try_from(row).ok()),
        record,
    }))
}

/// Add `entry` to the virtual model of its fields, as CASA's
/// `VisModelData::putModel` (version 2) does: the record already filed
/// under the entry's key, else the record a SOURCE row in use for the first
/// field's source holds, gains the entry after its others when
/// `incremental` or when it had a record to add to; otherwise (a new
/// record, not incremental) the record is the entry alone, and the images
/// the old record named are deleted. Without `incremental` an entry of
/// the old record covering one of the new combinations is removed first
/// (`addToRec`). The record is then filed as the module states, and every
/// field the entry covers points to it.
///
/// `tclean` adds its models incrementally, each image domain's in turn,
/// after clearing the selected fields' models ([`clear_virtual_model`]).
///
/// SOURCE and then MAIN are written in place under casacore's write lock,
/// each only when it changes, as [`crate::scratch_columns`] writes them.
///
/// # Errors
///
/// [`MsError::InvalidInput`] for an entry that names no field, and when
/// the record to add to is not of version 2: CASA refuses to combine with
/// it ("please clear it before proceeding") and writes nothing, as here.
/// Any error reading the record or FIELD, or locking and writing SOURCE or
/// MAIN.
pub fn put_virtual_model(
    ms: &mut MeasurementSet,
    entry: &VirtualModelEntry,
    incremental: bool,
) -> MsResult<()> {
    let fields = entry.fields();
    let Some(&first) = fields.first() else {
        return Err(invalid("a virtual model entry names no field"));
    };
    let key = model_key(&fields);
    let mut state = ModelState::read(ms)?;
    let mut record = RecordValue::default();
    let mut adds = false;
    if state.is_defined(&key) {
        record = state.record(ms, &key)?;
        if !incremental {
            adds = add_to_record(&mut record, &entry.index_combination)?;
        }
    } else if let Some(held) = state.first_source_row_record(ms, first)?
        && !held.is_empty()
    {
        record = held;
        adds = true;
    }
    if !record.is_empty() && int_field(&record, "version") != Some(2) {
        return Err(invalid(
            "cannot combine with an older virtual model; clear it before proceeding",
        ));
    }
    record.upsert("version", int(2));
    let incremental = incremental || adds;
    let counter = if incremental {
        count(&record, entry.kind)
    } else {
        0
    };
    record.upsert(entry.kind.count_key(), int(counter as i32 + 1));
    record.upsert(
        format!("{}{counter}", entry.kind.prefix()),
        Value::Record(entry.to_record()),
    );
    let mut images = Vec::new();
    if !incremental && state.is_defined(&key) {
        images = record_images(&state.record(ms, &key)?);
    }
    state.put_model_record(ms, &fields, &key, record)?;
    state.write(ms)?;
    delete_images(ms, &images);
    Ok(())
}

/// Clear the virtual models of `fields`, or only their entries'
/// combinations of `spws`, as CASA's `VisModelData::clearModel(ms, field,
/// spw)` does before `tclean` writes a virtual model of that selection:
///
/// - with no field, every field's model ([`prepare_write`] for
///   `MODEL_DATA`, `clearModel(ms)`), whatever `spws` names;
/// - with no spectral window, each named field's model, its record, its
///   key and its pointers;
/// - otherwise each named field's record loses the combinations of a named
///   field and a named window (`removeSpw`); a record left with entries is
///   filed again under its key, and one left with none is removed with its
///   SOURCE pointer (CASA keeps a MAIN pointer then).
///
/// The fields are visited in turn, each seeing what the ones before left,
/// as CASA visits them. `fields` and `spws` are the identifiers CASA's
/// `MSSelection` finds for the `field` and `spw` expressions.
///
/// # Errors
///
/// Any error reading a record or locking and writing SOURCE or MAIN.
pub fn clear_virtual_model(ms: &mut MeasurementSet, fields: &[i32], spws: &[i32]) -> MsResult<()> {
    if fields.is_empty() {
        return prepare_write(ms, VisibilityDataColumn::ModelData, false);
    }
    let mut state = ModelState::read(ms)?;
    let mut images = Vec::new();
    for &field in fields {
        let Some((key, source_row)) = state.defined_for_field(field) else {
            tracing::info!(field, "no virtual model to clear");
            continue;
        };
        let pointer = pointer(field);
        if spws.is_empty() {
            images.extend(record_images(&state.record(ms, &key)?));
            state.remove_record_by_key(&key);
            if source_row.is_some() {
                state.remove_source_keyword(&pointer);
            }
            state.remove_main_keyword(&pointer);
            continue;
        }
        let mut record = state.record(ms, &key)?;
        if remove_spw(&mut record, spws, fields) {
            state.put_record_by_key(&key, record, source_row);
        } else {
            images.extend(record_images(&state.record(ms, &key)?));
            state.remove_record_by_key(&key);
            state.remove_source_keyword(&pointer);
        }
    }
    state.write(ms)?;
    delete_images(ms, &images);
    Ok(())
}

/// The fields and spectral windows CASA's `MSSelection` finds for a
/// `field` and an `spw` expression (`getFieldList`, `getSpwList`), which
/// [`clear_virtual_model`] takes: no fields without a field expression,
/// and no windows without a window expression.
///
/// # Errors
///
/// When an expression cannot be parsed or resolved against `ms`.
pub fn virtual_model_clear_ids(
    ms: &MeasurementSet,
    field: Option<&str>,
    spw: Option<&str>,
) -> MsResult<(Vec<i32>, Vec<i32>)> {
    let selection = MsSelection {
        field: field.map(str::to_string),
        spw: spw.map(str::to_string),
        ..MsSelection::default()
    };
    let compiled = selection.compile(ms)?;
    let fields = if field.is_some() {
        compiled.field_ids.unwrap_or_default()
    } else {
        Vec::new()
    };
    let mut spws = compiled.spw_ids;
    spws.sort_unstable();
    spws.dedup();
    Ok((fields, spws))
}

/// What [`virtual_model_index_combination`] finds in a selection.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct VirtualModelSelection {
    /// The `(FIELD_ID, spectral window, SCAN_NUMBER, STATE_ID)`
    /// combinations, in CASA's order.
    pub index_combination: Vec<[i32; 4]>,
    /// `(FIELD_ID, DATA_DESC_ID, TIME)` of the selected row first in the
    /// imaging iterator's order (field, data description, time): the row
    /// whose buffer CASA's machine last saw when it saved itself.
    pub first_row: Option<(i32, i32, f64)>,
}

/// The `(FIELD_ID, spectral window, SCAN_NUMBER, STATE_ID)` combinations of
/// the rows `selection` selects, as CASA's `MSUtil::getIndexCombination`
/// orders them: by their first row in ascending `TIME × (DATA_DESC_ID + 1)`,
/// each once. `tclean` writes those of its selection
/// (`VisibilityIteratorImpl2::writeModel`).
///
/// The selected rows' scalar columns are read a bounded chunk at a time;
/// the selection's row list (one index per selected row) is held while
/// they are.
///
/// # Errors
///
/// When the selection cannot be resolved or MAIN, DATA_DESCRIPTION cannot
/// be read.
pub fn virtual_model_index_combination(
    ms: &MeasurementSet,
    selection: &MsSelection,
) -> MsResult<VirtualModelSelection> {
    const CHUNK_ROWS: usize = 65_536;
    let rows = selection.compile(ms)?.apply(ms)?;
    let mut first_row: Option<(i32, i32, f64)> = None;
    let description = ms.data_description()?;
    let windows = (0..description.row_count())
        .map(|row| description.spectral_window_id(row))
        .collect::<MsResult<Vec<_>>>()?;
    let main = ms.main_table();
    let mut first: BTreeMap<[i32; 4], f64> = BTreeMap::new();
    for chunk in rows.chunks(CHUNK_ROWS) {
        let ints = |name: &str| -> MsResult<Vec<i32>> {
            main.column_accessor(name)?
                .scalar_cells_owned_for_rows(chunk)?
                .into_iter()
                .map(|cell| match cell {
                    Some(ScalarValue::Int32(value)) => Ok(value),
                    _ => Err(invalid(format!("MAIN {name} is not an Int column"))),
                })
                .collect()
        };
        let times = main
            .column_accessor("TIME")?
            .scalar_cells_owned_for_rows(chunk)?
            .into_iter()
            .map(|cell| match cell {
                Some(ScalarValue::Float64(value)) => Ok(value),
                _ => Err(invalid("MAIN TIME is not a Double column")),
            })
            .collect::<MsResult<Vec<f64>>>()?;
        let (fields, descriptions) = (ints("FIELD_ID")?, ints("DATA_DESC_ID")?);
        let (scans, states) = (ints("SCAN_NUMBER")?, ints("STATE_ID")?);
        for slot in 0..chunk.len() {
            let window = usize::try_from(descriptions[slot])
                .ok()
                .and_then(|ddid| windows.get(ddid).copied())
                .ok_or_else(|| invalid("a row names a data description that does not exist"))?;
            let combination = [fields[slot], window, scans[slot], states[slot]];
            let key = times[slot] * (f64::from(descriptions[slot]) + 1.0);
            first
                .entry(combination)
                .and_modify(|earliest| *earliest = earliest.min(key))
                .or_insert(key);
            let row = (fields[slot], descriptions[slot], times[slot]);
            if first_row.is_none_or(|least| {
                (row.0, row.1)
                    .cmp(&(least.0, least.1))
                    .then(row.2.total_cmp(&least.2))
                    == std::cmp::Ordering::Less
            }) {
                first_row = Some(row);
            }
        }
    }
    let mut combinations: Vec<([i32; 4], f64)> = first.into_iter().collect();
    combinations.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
    Ok(VirtualModelSelection {
        index_combination: combinations.into_iter().map(|(row, _)| row).collect(),
        first_row,
    })
}

/// A new model image's path, where `tclean` writes it: a name CASA's
/// `File::newUniqueName(directory, "FT_MODEL")` would make (`FT_MODEL`, the
/// process id, `_` and a count) in SOURCE's directory, or the
/// MeasurementSet's when it has no SOURCE, made absolute
/// (`SIMapperCollection::saveVirtualModel`). No table of that name exists.
///
/// # Errors
///
/// [`MsError::InvalidInput`] for a MeasurementSet not on disk; the current
/// directory cannot be read for a relative path.
pub fn virtual_model_image_path(ms: &MeasurementSet) -> MsResult<PathBuf> {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let directory = ms
        .subtable(SubtableId::Source)
        .and_then(Table::path)
        .or_else(|| ms.path())
        .ok_or_else(|| invalid("a virtual model needs a MeasurementSet on disk"))?;
    let directory = std::path::absolute(directory).map_err(|error| invalid(error.to_string()))?;
    loop {
        let count = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = directory.join(format!("FT_MODEL{}_{count}", std::process::id()));
        if !path.exists()
            && !path
                .with_file_name(format!("{}_term_0", file_name(&path)))
                .exists()
        {
            return Ok(path);
        }
    }
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default()
}

/// The model key of `fields`, ascending: `model_<f>…`.
fn model_key(fields: &[i32]) -> String {
    fields
        .iter()
        .fold("model".to_string(), |key, field| format!("{key}_{field}"))
}

fn pointer(field: i32) -> String {
    format!("definedmodel_field_{field}")
}

fn int(value: i32) -> Value {
    Value::Scalar(ScalarValue::Int32(value))
}

fn invalid(message: impl Into<String>) -> MsError {
    MsError::InvalidInput(message.into())
}

fn int_field(record: &RecordValue, name: &str) -> Option<i32> {
    match record.get(name) {
        Some(Value::Scalar(ScalarValue::Int32(value))) => Some(*value),
        _ => None,
    }
}

/// The count of `kind`'s entries a record holds, `0` without one.
fn count(record: &RecordValue, kind: VirtualModelKind) -> usize {
    int_field(record, kind.count_key()).map_or(0, |count| usize::try_from(count).unwrap_or(0))
}

/// A `[rows, 4]` `Int` matrix.
fn matrix(rows: &[[i32; 4]]) -> Value {
    Value::Array(ArrayValue::Int32(ArrayD::from_shape_fn(
        IxDyn(&[rows.len(), 4]),
        |index| rows[index[0]][index[1]],
    )))
}

/// The rows of a `[rows, 4]` `Int` matrix.
fn matrix_rows(value: Option<&Value>) -> Option<Vec<[i32; 4]>> {
    let Some(Value::Array(ArrayValue::Int32(array))) = value else {
        return None;
    };
    let shape = array.shape();
    if shape.len() != 2 || shape[1] != 4 {
        return None;
    }
    Some(
        (0..shape[0])
            .map(|row| std::array::from_fn(|column| array[[row, column]]))
            .collect(),
    )
}

/// The images a model record's `FTMachine` entries name
/// (`ft_<k>.container.diskimage`), which CASA deletes with the record
/// (`VisModelData::deleteDiskImage`).
fn record_images(record: &RecordValue) -> Vec<PathBuf> {
    (0..count(record, VirtualModelKind::FtMachine))
        .filter_map(|index| {
            let Some(Value::Record(entry)) = record.get(&format!("ft_{index}")) else {
                return None;
            };
            let Some(Value::Record(container)) = entry.get("container") else {
                return None;
            };
            match container.get("diskimage") {
                Some(Value::Scalar(ScalarValue::String(path))) => Some(PathBuf::from(path)),
                _ => None,
            }
        })
        .collect()
}

/// Delete each of `images` that is an `FT_MODEL*` table in the
/// MeasurementSet's or its SOURCE's directory, at best: one held open,
/// elsewhere or not deletable is logged and left, as CASA leaves a table
/// `TableUtil::canDeleteTable` refuses.
fn delete_images(ms: &MeasurementSet, images: &[PathBuf]) {
    let canonical = |path: &Path| std::fs::canonicalize(path).ok();
    let directories: Vec<PathBuf> = [
        ms.path(),
        ms.subtable(SubtableId::Source).and_then(Table::path),
    ]
    .into_iter()
    .flatten()
    .filter_map(canonical)
    .collect();
    for image in images {
        let within = image
            .file_name()
            .is_some_and(|name| name.to_string_lossy().starts_with("FT_MODEL"))
            && image
                .parent()
                .and_then(canonical)
                .is_some_and(|parent| directories.contains(&parent));
        if !within || !image.join("table.dat").is_file() {
            continue;
        }
        let deleted = match held_open(image) {
            Ok(None) => std::fs::remove_dir_all(image).map_err(|error| error.to_string()),
            Ok(Some(holder)) => Err(format!("it is held open {holder}")),
            Err(error) => Err(error.to_string()),
        };
        if let Err(error) = deleted {
            tracing::warn!(image = %image.display(), %error, "a cleared virtual model's image is left");
        }
    }
}

/// `VisModelData::addToRec` (version 2): remove from `record` every entry
/// covering one of `combinations`, relabelling those left, and say whether
/// the new entry is to be added to what remains: some combination was not
/// covered, or entries remain.
fn add_to_record(record: &mut RecordValue, combinations: &[[i32; 4]]) -> MsResult<bool> {
    if !record.is_empty() && int_field(record, "version") != Some(2) {
        return Err(invalid(
            "cannot combine with an older virtual model; clear it before proceeding",
        ));
    }
    let mut covered = vec![false; combinations.len()];
    let mut remaining = 0;
    for kind in VirtualModelKind::BOTH {
        if record.get(kind.count_key()).is_none() {
            continue;
        }
        let entries = count(record, kind);
        let mut removed = Vec::new();
        for index in 0..entries {
            let name = format!("{}{index}", kind.prefix());
            let rows = match record.get(&name) {
                Some(Value::Record(entry)) => matrix_rows(entry.get("indexcombination")),
                _ => None,
            }
            .ok_or_else(|| invalid(format!("{name} has no indexcombination")))?;
            for (slot, combination) in combinations.iter().enumerate() {
                if rows.contains(combination) {
                    covered[slot] = true;
                    if removed.last() != Some(&name) {
                        removed.push(name.clone());
                    }
                }
            }
        }
        for name in &removed {
            record.remove(name);
        }
        let left = entries - removed.len();
        if !removed.is_empty() {
            record.upsert(kind.count_key(), int(left as i32));
            relabel(record, kind);
        }
        remaining += left;
    }
    Ok(!covered.iter().all(|&covered| covered) || remaining > 0)
}

/// Rename every field whose name contains `kind`'s prefix to that prefix
/// and a running count, in field order (`addToRec`, `removeFTFromRec`).
fn relabel(record: &mut RecordValue, kind: VirtualModelKind) {
    let names: Vec<String> = record
        .fields()
        .iter()
        .map(|field| field.name.clone())
        .filter(|name| name.contains(kind.prefix()))
        .collect();
    for (index, name) in names.iter().enumerate() {
        record.rename_field(name, format!("{}{index}", kind.prefix()));
    }
}

/// `VisModelData::removeSpw`: drop from each entry's combinations those of
/// a field among `fields` and a window among `spws` (version 2), or the
/// windows `spws` names from an older entry of one of `fields`; an entry
/// left with nothing is removed, the others relabelled once the last is.
/// Whether entries remain.
fn remove_spw(record: &mut RecordValue, spws: &[i32], fields: &[i32]) -> bool {
    for kind in VirtualModelKind::BOTH {
        if record.get(kind.count_key()).is_none() {
            continue;
        }
        let mut removed = Vec::new();
        for index in 0..count(record, kind) {
            let name = format!("{}{index}", kind.prefix());
            let Some(Value::Record(entry)) = record.get_mut(&name) else {
                continue;
            };
            let keeps = if entry.get("version").is_some() {
                let rows = matrix_rows(entry.get("indexcombination")).unwrap_or_default();
                let kept: Vec<[i32; 4]> = rows
                    .into_iter()
                    .filter(|row| !(fields.contains(&row[0]) && spws.contains(&row[1])))
                    .collect();
                if !kept.is_empty() {
                    entry.upsert("indexcombination", matrix(&kept));
                }
                !kept.is_empty()
            } else {
                !covers_field(entry, fields) || remove_windows(entry, spws)
            };
            if !keeps {
                removed.push(name);
            }
        }
        let last = removed.len();
        for (position, name) in removed.iter().enumerate() {
            remove_entry(record, kind, name, position + 1 == last);
        }
    }
    count(record, VirtualModelKind::FtMachine) + count(record, VirtualModelKind::ComponentList) > 0
}

/// Whether an older entry covers one of `fields` (all when none is named).
fn covers_field(entry: &RecordValue, fields: &[i32]) -> bool {
    if fields.is_empty() {
        return true;
    }
    match entry.get("fields") {
        Some(Value::Array(ArrayValue::Int32(ids))) => ids.iter().any(|id| fields.contains(id)),
        _ => false,
    }
}

/// `VisModelData::removeSpwFromMachineRec`: drop `spws` from an older
/// entry's `spws`; whether any window remains.
fn remove_windows(entry: &mut RecordValue, spws: &[i32]) -> bool {
    let windows: Vec<i32> = match entry.get("spws") {
        Some(Value::Array(ArrayValue::Int32(ids))) => ids.iter().copied().collect(),
        _ => Vec::new(),
    };
    let kept: Vec<i32> = windows
        .into_iter()
        .filter(|spw| !spws.contains(spw))
        .collect();
    if kept.is_empty() {
        return false;
    }
    entry.upsert(
        "spws",
        Value::Array(ArrayValue::Int32(
            ArrayD::from_shape_vec(IxDyn(&[kept.len()]), kept).expect("one axis"),
        )),
    );
    true
}

/// `VisModelData::removeFTFromRec`: remove entry `name`, decrement its
/// kind's count and, when `relabel`, renumber the entries left.
fn remove_entry(record: &mut RecordValue, kind: VirtualModelKind, name: &str, renumber: bool) {
    if record.remove(name).is_none() {
        return;
    }
    let left = int_field(record, kind.count_key()).unwrap_or(0) - 1;
    record.upsert(kind.count_key(), int(left));
    if renumber {
        relabel(record, kind);
    }
}

/// The keywords and model cells CASA's model functions read and write,
/// changed in memory and written once ([`Self::write`]).
struct ModelState {
    main: RecordValue,
    main_changed: bool,
    /// SOURCE, when the MeasurementSet has one.
    source: Option<SourceModels>,
}

struct SourceModels {
    rows: usize,
    keywords: RecordValue,
    has_column: bool,
    /// Model cells read or written, by row.
    cells: BTreeMap<usize, RecordValue>,
    written: Vec<usize>,
    adds_column: bool,
    changed: bool,
}

impl ModelState {
    fn read(ms: &MeasurementSet) -> MsResult<Self> {
        let source = ms.subtable(SubtableId::Source).map(|table| SourceModels {
            rows: table.row_count(),
            keywords: table.keywords().clone(),
            has_column: table
                .schema()
                .is_some_and(|schema| schema.contains_column(SOURCE_MODEL)),
            cells: BTreeMap::new(),
            written: Vec::new(),
            adds_column: false,
            changed: false,
        });
        Ok(Self {
            main: ms.main_table().keywords().clone(),
            main_changed: false,
            source,
        })
    }

    /// SOURCE, when it has rows: where CASA looks first.
    fn source_with_rows(&self) -> Option<&SourceModels> {
        self.source.as_ref().filter(|source| source.rows > 0)
    }

    /// `isModelDefined(key)`: the key among SOURCE's keywords (with rows)
    /// or MAIN's.
    fn is_defined(&self, key: &str) -> bool {
        self.source_with_rows()
            .is_some_and(|source| source.keywords.get(key).is_some())
            || self.main.get(key).is_some()
    }

    /// `isModelDefined(field)`: the key field `field`'s pointer names and,
    /// under SOURCE, the row the key holds (`-1` when it holds none), when
    /// that key is defined.
    fn defined_for_field(&self, field: i32) -> Option<(String, Option<i32>)> {
        let pointer = pointer(field);
        let (key, row) = match self.source_with_rows() {
            Some(source) if source.keywords.get(&pointer).is_some() => {
                let key = string(&source.keywords, &pointer)?;
                let row = int_field(&source.keywords, &key).unwrap_or(-1);
                (key, Some(row))
            }
            _ => (string(&self.main, &pointer)?, None),
        };
        (!key.is_empty() && self.is_defined(&key)).then_some((key, row))
    }

    /// `getModelRecord(key)`: the `SOURCE_MODEL` cell of the row the key
    /// names under SOURCE, else MAIN's record.
    fn record(&mut self, ms: &MeasurementSet, key: &str) -> MsResult<RecordValue> {
        if let Some(row) = self.source_with_rows().and_then(|source| {
            source
                .keywords
                .get(key)
                .map(|_| int_field(&source.keywords, key))
        }) {
            let row = row
                .and_then(|row| usize::try_from(row).ok())
                .ok_or_else(|| invalid(format!("SOURCE keyword {key} names no row")))?;
            return self.source_cell(ms, row);
        }
        match self.main.get(key) {
            Some(Value::Record(record)) => Ok(record.clone()),
            _ => Err(invalid(format!("MAIN keyword {key} holds no model record"))),
        }
    }

    fn source_cell(&mut self, ms: &MeasurementSet, row: usize) -> MsResult<RecordValue> {
        let source = self
            .source
            .as_mut()
            .ok_or_else(|| MsError::MissingSubtable("SOURCE".to_string()))?;
        if let Some(cell) = source.cells.get(&row) {
            return Ok(cell.clone());
        }
        if !source.has_column || row >= source.rows {
            return Err(invalid(format!(
                "SOURCE row {row} has no {SOURCE_MODEL} cell"
            )));
        }
        let table = ms
            .subtable(SubtableId::Source)
            .ok_or_else(|| MsError::MissingSubtable("SOURCE".to_string()))?;
        let cell = table.record_cell(row, SOURCE_MODEL)?;
        source.cells.insert(row, cell.clone());
        Ok(cell)
    }

    /// `firstSourceRowRecord`: the record in the first SOURCE row of field
    /// `field`'s source, when SOURCE has rows, the source is not -1, a row
    /// has it and an `Int` SOURCE keyword names that row (a model key uses
    /// it); empty when SOURCE has no `SOURCE_MODEL`.
    fn first_source_row_record(
        &mut self,
        ms: &MeasurementSet,
        field: i32,
    ) -> MsResult<Option<RecordValue>> {
        let Some(Some(row)) = self.first_row_of_source(ms, field)? else {
            return Ok(None);
        };
        let source = self.source_with_rows().expect("a source row");
        let used = source
            .keywords
            .fields()
            .iter()
            .any(|keyword| keyword.value == int(row as i32));
        if !used {
            return Ok(None);
        }
        if !source.has_column {
            return Ok(Some(RecordValue::default()));
        }
        self.source_cell(ms, row).map(Some)
    }

    /// The first SOURCE row of field `field`'s source (`MSSourceIndex`):
    /// `None` when SOURCE has no rows or the field's `SOURCE_ID` is -1, and
    /// `Some(None)` when no row has that source.
    fn first_row_of_source(
        &self,
        ms: &MeasurementSet,
        field: i32,
    ) -> MsResult<Option<Option<usize>>> {
        let Some(rows) = self.source_with_rows().map(|source| source.rows) else {
            return Ok(None);
        };
        let row =
            usize::try_from(field).map_err(|_| invalid(format!("field {field} does not exist")))?;
        let source_id = ms.field()?.source_id(row)?;
        if source_id == -1 {
            return Ok(None);
        }
        let ids = ms
            .subtable(SubtableId::Source)
            .ok_or_else(|| MsError::MissingSubtable("SOURCE".to_string()))?
            .column_accessor("SOURCE_ID")?
            .scalar_cells_owned_for_rows(&(0..rows).collect::<Vec<_>>())?;
        Ok(Some(ids.iter().position(
            |id| matches!(id, Some(ScalarValue::Int32(id)) if *id == source_id),
        )))
    }

    /// `putModelRecord`: file `record` under `key` for `fields`, as the
    /// module states, removing a MAIN copy of the key first.
    fn put_model_record(
        &mut self,
        ms: &MeasurementSet,
        fields: &[i32],
        key: &str,
        record: RecordValue,
    ) -> MsResult<()> {
        self.remove_main_keyword(key);
        if let Some(row) = self.first_row_of_source(ms, fields[0])? {
            let row = row.unwrap_or_else(|| {
                tracing::warn!(
                    field = fields[0],
                    "invalid SOURCE_ID found in FIELD; the model is written at SOURCE row 0, \
                     which will be erased: fix the FIELD table before proceeding"
                );
                0
            });
            self.put_record_by_key(key, record, Some(row as i32));
            let source = self.source.as_mut().expect("SOURCE has rows");
            for &field in fields {
                source.keywords.upsert(
                    pointer(field),
                    Value::Scalar(ScalarValue::String(key.to_string())),
                );
            }
            source.changed = true;
            return Ok(());
        }
        self.put_record_by_key(key, record, None);
        for &field in fields {
            self.main.upsert(
                pointer(field),
                Value::Scalar(ScalarValue::String(key.to_string())),
            );
        }
        self.main_changed = true;
        Ok(())
    }

    /// `putRecordByKey`: the record into SOURCE row `row` (adding
    /// `SOURCE_MODEL` when SOURCE lacks it) with the key, moved last, naming
    /// it; else, without such a row, into MAIN's keywords.
    fn put_record_by_key(&mut self, key: &str, record: RecordValue, row: Option<i32>) {
        let row = row.and_then(|row| usize::try_from(row).ok());
        if let (Some(row), Some(source)) = (row, self.source.as_mut())
            && row < source.rows
        {
            if !source.has_column {
                source.has_column = true;
                source.adds_column = true;
            }
            source.keywords.remove(key);
            source.keywords.upsert(key, int(row as i32));
            source.cells.insert(row, record);
            if !source.written.contains(&row) {
                source.written.push(row);
            }
            source.changed = true;
            return;
        }
        self.main.upsert(key, Value::Record(record));
        self.main_changed = true;
    }

    /// `removeRecordByKey`: empty the SOURCE cell the key names and remove
    /// the key from SOURCE and MAIN.
    fn remove_record_by_key(&mut self, key: &str) {
        if let Some(source) = self.source.as_mut().filter(|source| source.rows > 0)
            && let Some(row) = source.keywords.remove(key)
        {
            if let Value::Scalar(ScalarValue::Int32(row)) = row
                && let Ok(row) = usize::try_from(row)
                && row < source.rows
                && source.has_column
            {
                source.cells.insert(row, RecordValue::default());
                if !source.written.contains(&row) {
                    source.written.push(row);
                }
            }
            source.changed = true;
        }
        self.remove_main_keyword(key);
    }

    fn remove_main_keyword(&mut self, name: &str) {
        if self.main.remove(name).is_some() {
            self.main_changed = true;
        }
    }

    fn remove_source_keyword(&mut self, name: &str) {
        if let Some(source) = self.source.as_mut()
            && source.keywords.remove(name).is_some()
        {
            source.changed = true;
        }
    }

    /// Write SOURCE in full and MAIN's metadata, each only when it changed,
    /// under casacore's write lock on each.
    fn write(self, ms: &mut MeasurementSet) -> MsResult<()> {
        if let Some(models) = self.source.filter(|source| source.changed)
            && let Some(table) = ms.subtable_mut(SubtableId::Source)
        {
            change_in_place(table, 0, Write::Full, None, |table| {
                if models.adds_column {
                    table.add_column(ColumnSchema::record(SOURCE_MODEL), None)?;
                }
                for row in &models.written {
                    let cell = models.cells.get(row).cloned().unwrap_or_default();
                    table
                        .cell_accessor_mut(*row, SOURCE_MODEL)
                        .and_then(|mut accessor| accessor.set(Value::Record(cell)))?;
                }
                *table.keywords_mut() = models.keywords.clone();
                Ok(())
            })?;
        }
        if self.main_changed {
            let keywords = self.main;
            change_in_place(ms.main_table_mut(), 0, Write::Metadata, None, |main| {
                *main.keywords_mut() = keywords;
                Ok(())
            })?;
        }
        Ok(())
    }
}

fn string(keywords: &RecordValue, name: &str) -> Option<String> {
    match keywords.get(name) {
        Some(Value::Scalar(ScalarValue::String(value))) => Some(value.clone()),
        _ => None,
    }
}

#[cfg(test)]
#[path = "virtual_model_tests.rs"]
mod tests;
