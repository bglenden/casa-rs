// SPDX-License-Identifier: LGPL-3.0-or-later
//! What CASA clears from a MeasurementSet when it adds a scratch column to
//! it (`VisSetUtil::addScrCols`, CASA 6.7.5), so that a column casa-rs adds
//! is the one CASA reads.
//!
//! - **Cached sort keywords.** CASA's iterators may cache a pre-sorted
//!   reference table of MAIN and record it in MAIN's keywords as
//!   `SORTED_TABLE`, with the columns it is sorted by as `SORT_COLUMNS`
//!   (`MSIter2`). A sorted table made before a column was added does not
//!   see it, so adding `MODEL_DATA` or `CORRECTED_DATA` removes both
//!   keywords (the table itself is left, as CASA leaves it). The MS
//!   definition's own `SORTED_TABLES` is untouched.
//! - **The virtual model.** CASA can hold a field's model as a recipe
//!   instead of a column: `setjy` (`usescratch=False`) and `tclean`
//!   (`savemodel='virtual'`) write one, and CASA's iterator prefers it to
//!   `MODEL_DATA` (`VisibilityIteratorImpl2::visibilityModel`). Adding
//!   `MODEL_DATA` makes the column "the exclusive origin for model
//!   visibilities": it clears every field's virtual model as
//!   `VisModelData::clearModel` does (`remOTFModel`).
//!
//! A field's virtual model is a keyword `definedmodel_field_<id>` naming a
//! model key, in SOURCE's keywords when SOURCE has rows and MAIN's
//! otherwise. Under SOURCE the model key is a keyword holding the SOURCE
//! row whose `SOURCE_MODEL` cell holds the model record; under MAIN it is a
//! keyword holding the record itself. Clearing a field removes both
//! keywords wherever they are and empties the `SOURCE_MODEL` cell, which
//! is what CASA leaves (measured with `setjy` then `delmod` on CASA 6.7.5).

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use casa_types::{RecordValue, ScalarValue, Value};

use crate::ms::MeasurementSet;
use crate::schema::SubtableId;
use crate::schema::main_table::VisibilityDataColumn;
use crate::{MsError, MsResult};

/// MAIN's keywords for a cached pre-sorted reference table (`MSIter2`).
const SORT_KEYWORDS: [&str; 2] = ["SORT_COLUMNS", "SORTED_TABLE"];

/// Clear what CASA clears before it adds scratch column `column`
/// (`MODEL_DATA` or `CORRECTED_DATA`) to `ms`: the cached sort keywords,
/// and for `MODEL_DATA` every field's virtual model, as the module states.
/// Every path that adds either column to an existing MeasurementSet calls
/// it first; a new MeasurementSet holds neither.
///
/// For a MeasurementSet on disk what changes is saved here: MAIN's keywords
/// (its metadata alone) and SOURCE, since the callers' column installs
/// write only the new column's metadata. Model images a cleared model names
/// are deleted where they lie inside the MeasurementSet, with any
/// `FT_MODEL*` table CASA leaves there.
///
/// # Errors
///
/// When SOURCE's model cell cannot be emptied, MAIN's metadata or SOURCE
/// cannot be saved, or a model image cannot be deleted.
pub fn clear_before_adding(ms: &mut MeasurementSet, column: VisibilityDataColumn) -> MsResult<()> {
    let keywords = ms.main_table_mut().keywords_mut();
    let mut main_changed = false;
    for keyword in SORT_KEYWORDS {
        main_changed |= keywords.remove(keyword).is_some();
    }
    if column == VisibilityDataColumn::ModelData {
        main_changed |= clear_virtual_model(ms)?;
    }
    if main_changed && let Some(path) = ms.path().map(Path::to_path_buf) {
        ms.main_table()
            .save_metadata_only(casa_tables::TableOptions::new(path))?;
    }
    Ok(())
}

/// `VisModelData::clearModel`: for every field MAIN names, its model's
/// keywords and record. Whether MAIN's keywords changed.
fn clear_virtual_model(ms: &mut MeasurementSet) -> MsResult<bool> {
    let fields = fields_with_models(ms)?;
    let source_rows = ms
        .subtable(SubtableId::Source)
        .map_or(0, |source| source.row_count());
    let mut images = Vec::new();
    let mut source_changed = false;
    let mut main_changed = false;
    for field in fields {
        let pointer = format!("definedmodel_field_{field}");
        let in_source = source_rows > 0
            && ms
                .subtable(SubtableId::Source)
                .is_some_and(|source| source.keywords().get(&pointer).is_some());
        let model = if in_source {
            ms.subtable(SubtableId::Source)
                .and_then(|source| string(source.keywords(), &pointer))
        } else {
            string(ms.main_table().keywords(), &pointer)
        };
        if let Some(model) = &model {
            let record = model_record(ms, model, source_rows)?;
            if let Some(record) = &record {
                images.extend(disk_images(record));
            }
            if let Some(source) = ms.subtable_mut(SubtableId::Source) {
                let row = match source.keywords().get(model) {
                    Some(Value::Scalar(ScalarValue::Int32(row))) if source_rows > 0 => {
                        usize::try_from(*row).ok()
                    }
                    _ => None,
                };
                let has_model_column = source
                    .schema()
                    .is_some_and(|schema| schema.contains_column("SOURCE_MODEL"));
                if let Some(row) = row.filter(|row| *row < source_rows && has_model_column) {
                    source
                        .cell_accessor_mut(row, "SOURCE_MODEL")
                        .and_then(|mut cell| cell.set(Value::Record(RecordValue::default())))?;
                    source_changed = true;
                }
                source_changed |= source.keywords_mut().remove(model).is_some();
            }
            main_changed |= ms.main_table_mut().keywords_mut().remove(model).is_some();
        }
        if let Some(source) = ms.subtable_mut(SubtableId::Source) {
            source_changed |= source.keywords_mut().remove(&pointer).is_some();
        }
        main_changed |= ms
            .main_table_mut()
            .keywords_mut()
            .remove(&pointer)
            .is_some();
    }
    if let Some(root) = ms.path().map(Path::to_path_buf) {
        if source_changed && let Some(source) = ms.subtable_mut(SubtableId::Source) {
            source.flush()?;
        }
        delete_model_images(&root, &images)?;
    }
    Ok(main_changed)
}

/// MAIN rows read at a time while looking for the fields that have models.
const FIELD_SCAN_ROWS: usize = 65_536;

/// The fields that have a model pointer, in MAIN's or SOURCE's keywords,
/// and rows in MAIN: those `clearModel` reaches, since it visits the fields
/// MAIN names and a field without a pointer has nothing to clear. MAIN's
/// `FIELD_ID` is read only when a pointer exists, a bounded chunk at a
/// time, and no further than to find every pointed field.
fn fields_with_models(ms: &MeasurementSet) -> MsResult<BTreeSet<i32>> {
    let pointed = |keywords: &RecordValue| -> Vec<i32> {
        keywords
            .fields()
            .iter()
            .filter_map(|field| field.name.strip_prefix("definedmodel_field_")?.parse().ok())
            .collect()
    };
    let mut wanted: BTreeSet<i32> = pointed(ms.main_table().keywords()).into_iter().collect();
    if let Some(source) = ms.subtable(SubtableId::Source) {
        wanted.extend(pointed(source.keywords()));
    }
    let mut found = BTreeSet::new();
    if wanted.is_empty() {
        return Ok(found);
    }
    let main = ms.main_table();
    let column = main.column_accessor("FIELD_ID")?;
    let mut start = 0;
    while start < main.row_count() && found.len() < wanted.len() {
        let rows: Vec<usize> = (start..main.row_count().min(start + FIELD_SCAN_ROWS)).collect();
        for cell in column.scalar_cells_owned_for_rows(&rows)? {
            if let Some(ScalarValue::Int32(field)) = cell
                && wanted.contains(&field)
            {
                found.insert(field);
            }
        }
        start += FIELD_SCAN_ROWS;
    }
    Ok(found)
}

fn string(keywords: &RecordValue, name: &str) -> Option<String> {
    match keywords.get(name) {
        Some(Value::Scalar(ScalarValue::String(value))) if !value.is_empty() => Some(value.clone()),
        _ => None,
    }
}

/// `VisModelData::getModelRecord`: the record model key `model` names, in
/// SOURCE's `SOURCE_MODEL` when SOURCE's keywords hold its row, else in
/// MAIN's keywords.
fn model_record(
    ms: &MeasurementSet,
    model: &str,
    source_rows: usize,
) -> MsResult<Option<RecordValue>> {
    if let Some(source) = ms.subtable(SubtableId::Source)
        && source_rows > 0
        && let Some(Value::Scalar(ScalarValue::Int32(row))) = source.keywords().get(model)
    {
        return match usize::try_from(*row) {
            Ok(row) if row < source_rows => Ok(Some(source.record_cell(row, "SOURCE_MODEL")?)),
            _ => Ok(None),
        };
    }
    Ok(match ms.main_table().keywords().get(model) {
        Some(Value::Record(record)) => Some(record.clone()),
        _ => None,
    })
}

/// The model images a virtual model's gridder records name
/// (`VisModelData::deleteDiskImage`: `ft_<k>.container.diskimage`).
fn disk_images(record: &RecordValue) -> Vec<PathBuf> {
    let count = match record.get("numft") {
        Some(Value::Scalar(ScalarValue::Int32(count))) => *count,
        _ => 0,
    };
    (0..count)
        .filter_map(|k| match record.get(&format!("ft_{k}")) {
            Some(Value::Record(ft)) => match ft.get("container") {
                Some(Value::Record(container)) => string(container, "diskimage"),
                _ => None,
            },
            _ => None,
        })
        .map(PathBuf::from)
        .collect()
}

/// Delete `images`, and every `FT_MODEL*` table in the MeasurementSet's
/// directory and its SOURCE's, as `clearModel` does; an image outside the
/// MeasurementSet is left (CASA deletes it wherever it lies).
fn delete_model_images(root: &Path, images: &[PathBuf]) -> MsResult<()> {
    let mut doomed: Vec<PathBuf> = images
        .iter()
        .filter(|image| image.starts_with(root))
        .cloned()
        .collect();
    for directory in [root.to_path_buf(), root.join("SOURCE")] {
        let Ok(entries) = std::fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries.flatten() {
            if entry.file_name().to_string_lossy().starts_with("FT_MODEL") {
                doomed.push(entry.path());
            }
        }
    }
    for table in doomed {
        if table.join("table.dat").is_file() {
            std::fs::remove_dir_all(&table).map_err(|source| {
                MsError::InvalidInput(format!(
                    "cannot delete the cleared model image {}: {source}",
                    table.display()
                ))
            })?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use casa_tables::{ColumnSchema, Table, TableOptions, TableSchema};
    use casa_types::{ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value};

    use super::clear_before_adding;
    use crate::schema::SubtableId;
    use crate::schema::main_table::VisibilityDataColumn;
    use crate::{MeasurementSet, SelectedVisibilityWrite, SelectedVisibilityWriteTargets};

    fn unpack(root: &Path, archive: &str, name: &str) -> PathBuf {
        let archive = Path::new(env!("CARGO_MANIFEST_DIR")).join(archive);
        tar::Archive::new(flate2::read::GzDecoder::new(
            std::fs::File::open(archive).expect("fixture"),
        ))
        .unpack(root)
        .expect("unpack");
        root.join(name)
    }

    /// The trial fixture, whose MAIN names field 0 and which has no SOURCE.
    fn trial(root: &Path) -> PathBuf {
        unpack(
            root,
            "tests/fixtures/ms_trial/ms_trial.ms.tgz",
            "ms_trial.ms",
        )
    }

    fn text(value: &str) -> Value {
        Value::Scalar(ScalarValue::String(value.to_string()))
    }

    /// A component-list model record as `setjy` writes it.
    fn model() -> RecordValue {
        RecordValue::new(vec![
            RecordField::new("numcl", Value::Scalar(ScalarValue::Int32(1))),
            RecordField::new(
                "cl_0",
                Value::Record(RecordValue::new(vec![RecordField::new(
                    "fields",
                    Value::Array(ArrayValue::Int32(
                        ndarray::ArrayD::from_shape_vec(vec![1], vec![0]).expect("shape"),
                    )),
                )])),
            ),
            RecordField::new("version", Value::Scalar(ScalarValue::Float64(2.0))),
        ])
    }

    /// A small table at `path`.
    fn table_at(path: &Path) {
        let mut table = Table::with_schema(
            TableSchema::new(vec![ColumnSchema::scalar("X", PrimitiveType::Int32)])
                .expect("schema"),
        );
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "X",
                Value::Scalar(ScalarValue::Int32(1)),
            )]))
            .expect("row");
        table.save(TableOptions::new(path)).expect("save");
    }

    /// MAIN's keywords as CASA leaves them after a sorted iteration and a
    /// `setjy` into MAIN (an MS without SOURCE rows): the sort keywords and
    /// field 0's model, plus a model for a field MAIN does not name and an
    /// unrelated keyword, which stay.
    fn as_casa_left_it(path: &Path) {
        table_at(&path.join("SORTED_TABLE"));
        let mut ms = MeasurementSet::open(path).expect("open");
        let keywords = ms.main_table_mut().keywords_mut();
        keywords.upsert(
            "SORT_COLUMNS",
            Value::Array(ArrayValue::String(
                ndarray::ArrayD::from_shape_vec(vec![1], vec!["TIME".to_string()]).expect("shape"),
            )),
        );
        keywords.upsert(
            "SORTED_TABLE",
            Value::TableRef("././SORTED_TABLE".to_string()),
        );
        keywords.upsert("definedmodel_field_0", text("model_0"));
        keywords.upsert("model_0", Value::Record(model()));
        keywords.upsert("definedmodel_field_7", text("model_7"));
        keywords.upsert("model_7", Value::Record(model()));
        keywords.upsert("KEEP", text("kept"));
        ms.save().expect("save");
    }

    fn main_keywords(path: &Path) -> Vec<String> {
        let ms = MeasurementSet::open(path).expect("reopen");
        let mut names: Vec<String> = ms
            .main_table()
            .keywords()
            .fields()
            .iter()
            .map(|field| field.name.clone())
            .filter(|name| {
                !SubtableId::ALL_REQUIRED
                    .iter()
                    .chain(SubtableId::ALL_OPTIONAL)
                    .any(|id| id.name() == name)
            })
            .collect();
        names.sort();
        names
    }

    /// Adding `MODEL_DATA` clears the sort keywords and the model of every
    /// field MAIN names (not the sorted table itself, as CASA leaves it);
    /// adding `CORRECTED_DATA` clears only the sort keywords.
    #[test]
    fn scratch_columns_clear_what_casa_clears() {
        for (column, expected) in [
            (
                VisibilityDataColumn::ModelData,
                vec!["KEEP", "MS_VERSION", "definedmodel_field_7", "model_7"],
            ),
            (
                VisibilityDataColumn::CorrectedData,
                vec![
                    "KEEP",
                    "MS_VERSION",
                    "definedmodel_field_0",
                    "definedmodel_field_7",
                    "model_0",
                    "model_7",
                ],
            ),
        ] {
            let root = tempfile::tempdir().expect("root");
            let path = trial(root.path());
            as_casa_left_it(&path);
            let mut ms = MeasurementSet::open(&path).expect("open");
            clear_before_adding(&mut ms, column).expect("clear");
            ms.save().expect("save");
            drop(ms);
            assert_eq!(main_keywords(&path), expected, "{column:?}");
            assert!(path.join("SORTED_TABLE/table.dat").is_file(), "{column:?}");
        }
    }

    /// A field's pointer naming no model is a dangling key, removed as CASA
    /// removes it; a field without a model changes nothing.
    #[test]
    fn a_dangling_model_pointer_is_removed() {
        let root = tempfile::tempdir().expect("root");
        let path = trial(root.path());
        let mut ms = MeasurementSet::open(&path).expect("open");
        ms.main_table_mut()
            .keywords_mut()
            .upsert("definedmodel_field_0", text("model_9"));
        clear_before_adding(&mut ms, VisibilityDataColumn::ModelData).expect("clear");
        assert!(
            ms.main_table()
                .keywords()
                .get("definedmodel_field_0")
                .is_none()
        );
        let before = ms.main_table().keywords().clone();
        clear_before_adding(&mut ms, VisibilityDataColumn::ModelData).expect("again");
        assert_eq!(*ms.main_table().keywords(), before);
    }

    /// A model a gridder recorded names its model image: deleted with the
    /// model when it lies inside the MeasurementSet, with any `FT_MODEL*`
    /// table there, and left when it lies outside.
    #[test]
    fn a_cleared_model_takes_its_images_inside_the_measurement_set() {
        let root = tempfile::tempdir().expect("root");
        let path = trial(root.path());
        let inside = path.join("model_image");
        let outside = root.path().join("outside_image");
        for table in [&inside, &outside, &path.join("FT_MODEL_x")] {
            table_at(table);
        }
        let ft = |image: &Path| {
            Value::Record(RecordValue::new(vec![RecordField::new(
                "container",
                Value::Record(RecordValue::new(vec![RecordField::new(
                    "diskimage",
                    text(&image.display().to_string()),
                )])),
            )]))
        };
        let mut ms = MeasurementSet::open(&path).expect("open");
        let keywords = ms.main_table_mut().keywords_mut();
        keywords.upsert("definedmodel_field_0", text("model_0"));
        keywords.upsert(
            "model_0",
            Value::Record(RecordValue::new(vec![
                RecordField::new("numft", Value::Scalar(ScalarValue::Int32(2))),
                RecordField::new("ft_0", ft(&inside)),
                RecordField::new("ft_1", ft(&outside)),
            ])),
        );
        clear_before_adding(&mut ms, VisibilityDataColumn::ModelData).expect("clear");
        assert!(!inside.exists(), "the image inside");
        assert!(!path.join("FT_MODEL_x").exists(), "the FT_MODEL table");
        assert!(outside.join("table.dat").is_file(), "the image outside");
    }

    /// With SOURCE rows, `setjy` writes the model under SOURCE: its
    /// keywords name the model key and the row whose `SOURCE_MODEL` holds
    /// the record (CASA 6.7.5 on this fixture). Clearing removes both
    /// keywords and empties the cell, keeping the column, as `delmod` leaves
    /// it, and saves SOURCE itself.
    #[test]
    fn a_model_under_source_is_cleared_and_its_cell_emptied() {
        let root = tempfile::tempdir().expect("root");
        let path = unpack(
            root.path(),
            "tests/fixtures/mssel_test_small.ms.tgz",
            "mssel_test_small.ms",
        );
        {
            let mut ms = MeasurementSet::open(&path).expect("open");
            let source = ms.subtable_mut(SubtableId::Source).expect("SOURCE");
            source
                .add_column(ColumnSchema::record("SOURCE_MODEL"), None)
                .expect("SOURCE_MODEL");
            source
                .cell_accessor_mut(0, "SOURCE_MODEL")
                .and_then(|mut cell| cell.set(Value::Record(model())))
                .expect("model");
            let keywords = source.keywords_mut();
            keywords.upsert("definedmodel_field_0", text("model_0"));
            keywords.upsert("model_0", Value::Scalar(ScalarValue::Int32(0)));
            ms.save().expect("save");
        }
        let mut ms = MeasurementSet::open(&path).expect("open");
        clear_before_adding(&mut ms, VisibilityDataColumn::ModelData).expect("clear");
        drop(ms);
        let ms = MeasurementSet::open(&path).expect("reopen");
        let source = ms.subtable(SubtableId::Source).expect("SOURCE");
        for key in ["definedmodel_field_0", "model_0"] {
            assert!(source.keywords().get(key).is_none(), "{key}");
        }
        assert_eq!(
            source.record_cell(0, "SOURCE_MODEL").expect("cell"),
            RecordValue::default()
        );
    }

    /// Imaging's in-place writer, creating `MODEL_DATA`, clears what CASA
    /// clears, on disk.
    #[test]
    fn imaging_s_model_data_write_clears_what_casa_clears() {
        let root = tempfile::tempdir().expect("root");
        let path = trial(root.path());
        as_casa_left_it(&path);
        SelectedVisibilityWrite::begin(&path, SelectedVisibilityWriteTargets::new(true, false))
            .expect("begin")
            .complete()
            .expect("complete");
        assert_eq!(
            main_keywords(&path),
            ["KEEP", "MS_VERSION", "definedmodel_field_7", "model_7"]
        );
    }
}
