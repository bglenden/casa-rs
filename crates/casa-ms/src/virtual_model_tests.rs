// SPDX-License-Identifier: LGPL-3.0-or-later
//! Laws of the virtual model's persistence, against the records CASA 6.7.6
//! writes (`tests/fixtures/virtual_model_casa.tgz`: the SOURCE tables
//! `tclean(savemodel='virtual')` left in `refim_twochan.ms`, MFS and
//! MT-MFS).

use std::path::{Path, PathBuf};

use casa_tables::{Table, TableOptions};
use casa_types::{RecordValue, ScalarValue, Value};

use super::*;
use crate::selection::MsSelection;

fn unpack(root: &Path, archive: &str) {
    let archive = Path::new(env!("CARGO_MANIFEST_DIR")).join(archive);
    tar::Archive::new(flate2::read::GzDecoder::new(
        std::fs::File::open(archive).expect("fixture"),
    ))
    .unpack(root)
    .expect("unpack");
}

/// The trial fixture, whose MAIN names field 0 and which has no SOURCE.
fn trial(root: &Path) -> PathBuf {
    unpack(root, "tests/fixtures/ms_trial/ms_trial.ms.tgz");
    root.join("ms_trial.ms")
}

/// A fixture whose SOURCE has rows.
fn with_source(root: &Path) -> PathBuf {
    unpack(root, "tests/fixtures/mssel_test_small.ms.tgz");
    root.join("mssel_test_small.ms")
}

/// The model record CASA wrote, `"casa_mfs_source"` or
/// `"casa_mtmfs_source"`: row 0's `SOURCE_MODEL`.
fn casa_record(root: &Path, name: &str) -> RecordValue {
    unpack(root, "tests/fixtures/virtual_model_casa.tgz");
    Table::open(TableOptions::new(root.join(name)))
        .expect("CASA's SOURCE")
        .record_cell(0, SOURCE_MODEL)
        .expect("CASA's model record")
}

fn text(value: &str) -> Value {
    Value::Scalar(ScalarValue::String(value.to_string()))
}

/// A gridder entry of `combinations`, its container naming `label`.
fn entry(combinations: &[[i32; 4]], label: &str) -> VirtualModelEntry {
    VirtualModelEntry {
        kind: VirtualModelKind::FtMachine,
        index_combination: combinations.to_vec(),
        channel_selection: vec![[0, 0, 2, 1]],
        container: RecordValue::new(vec![casa_types::RecordField::new("name", text(label))]),
    }
}

fn label(record: &RecordValue, name: &str) -> String {
    let Some(Value::Record(entry)) = record.get(name) else {
        panic!("{name} missing from {record:?}");
    };
    let Some(Value::Record(container)) = entry.get("container") else {
        panic!("{name} has no container");
    };
    match container.get("name") {
        Some(Value::Scalar(ScalarValue::String(label))) => label.clone(),
        other => panic!("{other:?}"),
    }
}

fn names(record: &RecordValue) -> Vec<&str> {
    record
        .fields()
        .iter()
        .map(|field| field.name.as_str())
        .collect()
}

/// casa-rs reads the entries of CASA's records, and writes each back
/// field for field, in CASA's order and types: an `FTMachine` entry of
/// `GridFT` and one of `MultiTermFTNew`.
#[test]
fn casa_records_read_and_write_back_unchanged() {
    for (name, machine) in [
        ("casa_mfs_source", "GridFT"),
        ("casa_mtmfs_source", "MultiTermFTNew"),
    ] {
        let root = tempfile::tempdir().expect("root");
        let record = casa_record(root.path(), name);
        assert_eq!(names(&record), ["version", "numft", "ft_0"], "{name}");
        let entries = VirtualModelEntry::entries(&record).expect("entries");
        assert_eq!(entries.len(), 1, "{name}");
        let read = &entries[0];
        assert_eq!(read.kind, VirtualModelKind::FtMachine);
        assert_eq!(read.index_combination, vec![[0, 0, 0, -1]]);
        assert_eq!(read.channel_selection, vec![[0, 0, 2, 1]]);
        assert_eq!(read.key(), "model_0");
        assert_eq!(label(&record, "ft_0"), machine);
        assert_eq!(
            Some(&Value::Record(read.to_record())),
            record.get("ft_0"),
            "{name}: the entry as casa-rs writes it"
        );
    }
}

/// On a MeasurementSet whose SOURCE has rows, casa-rs files CASA's entry
/// as CASA filed it: SOURCE keywords `model_0` (the row) and
/// `definedmodel_field_0`, and row 0's `SOURCE_MODEL` (the column added)
/// holding CASA's very record; MAIN is untouched. It reads back from disk.
#[test]
fn a_model_is_filed_under_source_as_casa_files_it() {
    let root = tempfile::tempdir().expect("root");
    let record = casa_record(root.path(), "casa_mfs_source");
    let entry = VirtualModelEntry::entries(&record)
        .expect("entries")
        .remove(0);
    let path = with_source(root.path());
    let mut ms = MeasurementSet::open(&path).expect("open");
    let main = ms.main_table().keywords().clone();
    assert!(
        !ms.subtable(SubtableId::Source)
            .and_then(Table::schema)
            .expect("SOURCE")
            .contains_column(SOURCE_MODEL)
    );
    put_virtual_model(&mut ms, &entry, true).expect("put");
    drop(ms);
    let ms = MeasurementSet::open(&path).expect("reopen");
    assert_eq!(*ms.main_table().keywords(), main);
    let source = ms.subtable(SubtableId::Source).expect("SOURCE");
    assert_eq!(source.keywords().get("model_0"), Some(&int(0)));
    assert_eq!(
        source.keywords().get("definedmodel_field_0"),
        Some(&text("model_0"))
    );
    assert_eq!(source.record_cell(0, SOURCE_MODEL).expect("cell"), record);
    let found = virtual_model(&ms, 0).expect("read").expect("a model");
    assert_eq!((found.key.as_str(), found.source_row), ("model_0", Some(0)));
    assert_eq!(found.record, record);
}

/// Without SOURCE rows the record and its pointer are MAIN keywords.
#[test]
fn a_model_without_source_rows_is_filed_in_main() {
    let root = tempfile::tempdir().expect("root");
    let path = trial(root.path());
    let mut ms = MeasurementSet::open(&path).expect("open");
    put_virtual_model(&mut ms, &entry(&[[0, 0, 1, -1]], "a"), true).expect("put");
    drop(ms);
    let ms = MeasurementSet::open(&path).expect("reopen");
    let keywords = ms.main_table().keywords();
    assert_eq!(keywords.get("definedmodel_field_0"), Some(&text("model_0")));
    let Some(Value::Record(record)) = keywords.get("model_0") else {
        panic!("no MAIN record");
    };
    assert_eq!(names(record), ["version", "numft", "ft_0"]);
    let found = virtual_model(&ms, 0).expect("read").expect("a model");
    assert_eq!(found.source_row, None);
    assert_eq!(found.record, *record);
    assert!(virtual_model(&ms, 1).expect("read").is_none());
}

/// Incremental entries append (`tclean`'s image domains); an entry that is
/// not incremental first removes every entry covering one of its
/// combinations and renumbers the rest (`addToRec`), then appends.
#[test]
fn incremental_entries_append_and_a_replacement_drops_covered_ones() {
    let root = tempfile::tempdir().expect("root");
    let path = trial(root.path());
    let mut ms = MeasurementSet::open(&path).expect("open");
    put_virtual_model(&mut ms, &entry(&[[0, 0, 1, -1]], "a"), true).expect("a");
    put_virtual_model(&mut ms, &entry(&[[0, 1, 1, -1]], "b"), true).expect("b");
    let record = virtual_model(&ms, 0).expect("read").expect("model").record;
    assert_eq!(names(&record), ["version", "numft", "ft_0", "ft_1"]);
    assert_eq!(int_field(&record, "numft"), Some(2));
    put_virtual_model(&mut ms, &entry(&[[0, 0, 1, -1]], "c"), false).expect("c");
    let record = virtual_model(&ms, 0).expect("read").expect("model").record;
    assert_eq!(int_field(&record, "numft"), Some(2));
    assert_eq!(
        [label(&record, "ft_0"), label(&record, "ft_1")],
        ["b".to_string(), "c".to_string()]
    );
}

/// A record of an older version is not combined with: nothing is written.
#[test]
fn an_older_record_is_not_combined_with() {
    let root = tempfile::tempdir().expect("root");
    let path = trial(root.path());
    let mut ms = MeasurementSet::open(&path).expect("open");
    let keywords = ms.main_table_mut().keywords_mut();
    keywords.upsert("definedmodel_field_0", text("model_0"));
    keywords.upsert(
        "model_0",
        Value::Record(RecordValue::new(vec![casa_types::RecordField::new(
            "numcl",
            int(1),
        )])),
    );
    ms.save().expect("save");
    let before = ms.main_table().keywords().clone();
    let error = put_virtual_model(&mut ms, &entry(&[[0, 0, 1, -1]], "a"), true)
        .expect_err("an older record");
    assert!(error.to_string().contains("older"), "{error}");
    assert_eq!(*ms.main_table().keywords(), before);
}

/// Clearing a window drops its combinations from the named fields'
/// entries (`removeSpw`); an entry left with none goes, and a record left
/// with no entry is removed with its key, its MAIN pointer kept as CASA
/// keeps it. Clearing a field with no window removes its whole model.
#[test]
fn clearing_windows_and_fields_follows_clear_model() {
    let root = tempfile::tempdir().expect("root");
    let path = trial(root.path());
    let mut ms = MeasurementSet::open(&path).expect("open");
    let both = [[0, 0, 1, -1], [0, 1, 1, -1]];
    put_virtual_model(&mut ms, &entry(&both, "a"), true).expect("a");
    clear_virtual_model(&mut ms, &[0], &[0]).expect("clear spw 0");
    let record = virtual_model(&ms, 0).expect("read").expect("model").record;
    let entries = VirtualModelEntry::entries(&record).expect("entries");
    assert_eq!(entries[0].index_combination, vec![[0, 1, 1, -1]]);
    clear_virtual_model(&mut ms, &[0], &[1]).expect("clear spw 1");
    drop(ms);
    let ms = MeasurementSet::open(&path).expect("reopen");
    assert!(virtual_model(&ms, 0).expect("read").is_none());
    let keywords = ms.main_table().keywords();
    assert!(keywords.get("model_0").is_none());
    assert_eq!(keywords.get("definedmodel_field_0"), Some(&text("model_0")));

    let mut ms = ms;
    put_virtual_model(&mut ms, &entry(&both, "b"), true).expect("b");
    clear_virtual_model(&mut ms, &[0], &[]).expect("clear field 0");
    let keywords = ms.main_table().keywords();
    assert!(keywords.get("model_0").is_none());
    assert!(keywords.get("definedmodel_field_0").is_none());
}

/// Under SOURCE, a cleared model's cell is emptied and both keywords go;
/// clearing with no field clears every model.
#[test]
fn clearing_under_source_empties_the_cell() {
    let root = tempfile::tempdir().expect("root");
    let path = with_source(root.path());
    let mut ms = MeasurementSet::open(&path).expect("open");
    put_virtual_model(&mut ms, &entry(&[[0, 0, 1, -1]], "a"), true).expect("a");
    clear_virtual_model(&mut ms, &[0], &[]).expect("clear");
    drop(ms);
    let ms = MeasurementSet::open(&path).expect("reopen");
    let source = ms.subtable(SubtableId::Source).expect("SOURCE");
    assert!(source.keywords().get("model_0").is_none());
    assert!(source.keywords().get("definedmodel_field_0").is_none());
    assert_eq!(
        source.record_cell(0, SOURCE_MODEL).expect("cell"),
        RecordValue::default()
    );
    let mut ms = ms;
    put_virtual_model(&mut ms, &entry(&[[0, 0, 1, -1]], "b"), true).expect("b");
    clear_virtual_model(&mut ms, &[], &[3]).expect("clear all");
    assert!(virtual_model(&ms, 0).expect("read").is_none());
}

/// The combinations are those of the selected rows, each once, the first
/// that of the row of least `TIME × (DATA_DESC_ID + 1)`; a field
/// selection keeps only its fields'.
#[test]
fn index_combinations_follow_get_index_combination() {
    let root = tempfile::tempdir().expect("root");
    unpack(
        root.path(),
        "tests/fixtures/mssel_test_small_multifield_spw.ms.tgz",
    );
    let path = root.path().join("mssel_test_small_multifield_spw.ms");
    let ms = MeasurementSet::open(&path).expect("open");
    let selected = virtual_model_index_combination(&ms, &MsSelection::default()).expect("all");
    let all = selected.index_combination;
    assert!(!all.is_empty());
    let mut unique = all.clone();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(unique.len(), all.len(), "each combination once");
    let fields: std::collections::BTreeSet<i32> = all.iter().map(|row| row[0]).collect();
    let (first_field, _, _) = selected.first_row.expect("a first row");
    assert_eq!(
        Some(&first_field),
        fields.iter().next(),
        "the least field first"
    );
    let field = *fields.iter().next_back().expect("a field");
    let one = virtual_model_index_combination(&ms, &MsSelection::default().field(&[field]))
        .expect("one field");
    let one = one.index_combination;
    assert!(!one.is_empty() && one.iter().all(|row| row[0] == field));
    let kept: Vec<[i32; 4]> = all.into_iter().filter(|row| row[0] == field).collect();
    assert_eq!(one, kept, "a selection keeps the order");
}

/// The ids `clearModel` takes: none without a field expression, the
/// named fields and windows otherwise.
#[test]
fn clear_ids_are_the_expressions_ids() {
    let root = tempfile::tempdir().expect("root");
    unpack(
        root.path(),
        "tests/fixtures/mssel_test_small_multifield_spw.ms.tgz",
    );
    let path = root.path().join("mssel_test_small_multifield_spw.ms");
    let ms = MeasurementSet::open(&path).expect("open");
    assert_eq!(
        virtual_model_clear_ids(&ms, None, Some("0")).expect("ids"),
        (vec![], vec![0])
    );
    assert_eq!(
        virtual_model_clear_ids(&ms, Some("0"), None).expect("ids"),
        (vec![0], vec![])
    );
    assert_eq!(
        virtual_model_clear_ids(&ms, Some("0"), Some("0:1~2")).expect("ids"),
        (vec![0], vec![0])
    );
}

/// Model images go into SOURCE's directory, each name new.
#[test]
fn model_image_paths_are_new_names_in_source() {
    let root = tempfile::tempdir().expect("root");
    let path = with_source(root.path());
    let ms = MeasurementSet::open(&path).expect("open");
    let first = virtual_model_image_path(&ms).expect("path");
    let second = virtual_model_image_path(&ms).expect("path");
    assert_ne!(first, second);
    assert!(first.is_absolute());
    assert_eq!(
        std::fs::canonicalize(first.parent().expect("parent")).expect("dir"),
        std::fs::canonicalize(path.join("SOURCE")).expect("SOURCE")
    );
    assert!(
        first
            .file_name()
            .expect("name")
            .to_string_lossy()
            .starts_with("FT_MODEL")
    );
}
