// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;
use crate::builder::MeasurementSetBuilder;
use crate::schema::main_table::OptionalMainColumn;
use crate::test_helpers::default_value;
use casa_core::PrimitiveType;
use casa_tables::TableOptions;
use std::fs;

#[test]
fn create_memory_ms() {
    let ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();
    assert_eq!(ms.row_count(), 0);
    assert_eq!(ms.subtable_ids().len(), 12);
    assert!(ms.ms_version().is_some());
    assert!((ms.ms_version().unwrap() - 2.0).abs() < 0.01);
}

#[test]
fn validate_empty_ms() {
    let ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();
    let issues = ms.validate().unwrap();
    assert!(issues.is_empty(), "unexpected issues: {issues:?}");
}

#[test]
fn retained_read_open_holds_main_and_required_subtable_locks() {
    use casa_tables::LockType;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("retained.ms");
    MeasurementSet::create(&path, MeasurementSetBuilder::new()).unwrap();

    let retained = MeasurementSet::open_retained_read(&path).unwrap();

    assert!(retained.main_table().has_lock(LockType::Read));
    for subtable in SubtableId::ALL_REQUIRED {
        assert!(
            retained
                .subtable(*subtable)
                .expect("required subtable")
                .has_lock(LockType::Read),
            "required {subtable:?} subtable must remain read-locked"
        );
    }
}

#[test]
fn validate_detects_missing_column_metadata() {
    let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();
    ms.main_table_mut()
        .set_column_keywords("TIME", RecordValue::default());

    let issues = ms.validate().unwrap();
    assert!(issues.iter().any(|issue| matches!(
        issue,
        ValidationIssue::MissingQuantumUnits {
            table_name,
            column_name
        } if table_name == "MAIN" && column_name == "TIME"
    )));
    assert!(issues.iter().any(|issue| matches!(
        issue,
        ValidationIssue::MissingMeasureInfo {
            table_name,
            column_name
        } if table_name == "MAIN" && column_name == "TIME"
    )));
}

#[test]
fn validate_missing_subtable() {
    let schemas = MeasurementSetBuilder::new().build_schemas().unwrap();
    let main = Table::with_schema(schemas.main);
    let subtables = HashMap::new(); // empty - no subtables
    let issues = validate::validate_subtable_keywords(&main, &subtables);
    assert_eq!(
        issues.len(),
        13 // 12 missing subtables + 1 missing MS_VERSION
    );
}

#[test]
fn typed_subtable_accessors() {
    let ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();
    assert_eq!(ms.antenna().unwrap().row_count(), 0);
    assert_eq!(ms.data_description().unwrap().row_count(), 0);
    assert_eq!(ms.feed().unwrap().row_count(), 0);
    assert_eq!(ms.field().unwrap().row_count(), 0);
    assert_eq!(ms.flag_cmd().unwrap().row_count(), 0);
    assert_eq!(ms.history().unwrap().row_count(), 0);
    assert_eq!(ms.observation().unwrap().row_count(), 0);
    assert_eq!(ms.pointing().unwrap().row_count(), 0);
    assert_eq!(ms.spectral_window().unwrap().row_count(), 0);
    assert_eq!(ms.polarization().unwrap().row_count(), 0);
    assert_eq!(ms.processor().unwrap().row_count(), 0);
    assert_eq!(ms.state().unwrap().row_count(), 0);
}

#[test]
fn optional_subtable_accessors() {
    let mut builder = MeasurementSetBuilder::new();
    for id in SubtableId::ALL_OPTIONAL {
        builder = builder.with_optional_subtable(*id);
    }
    let ms = MeasurementSet::create_memory(builder).unwrap();
    assert_eq!(ms.doppler().unwrap().row_count(), 0);
    assert_eq!(ms.freq_offset().unwrap().row_count(), 0);
    assert_eq!(ms.source().unwrap().row_count(), 0);
    assert_eq!(ms.syscal().unwrap().row_count(), 0);
    assert_eq!(ms.weather().unwrap().row_count(), 0);
}

#[test]
fn casa_like_main_table_bindings_tile_channelized_weight_columns() {
    let ms = MeasurementSet::create_memory(
        MeasurementSetBuilder::new()
            .with_main_column(OptionalMainColumn::Data)
            .with_main_column(OptionalMainColumn::SigmaSpectrum)
            .with_main_column(OptionalMainColumn::WeightSpectrum)
            .with_main_column(OptionalMainColumn::CorrectedWeightSpectrum),
    )
    .unwrap();

    let bindings = measurement_set_main_table_bindings(ms.main_table());
    let data_tile_shape = bindings.get("DATA").unwrap().tile_shape.clone();
    for column in [
        "SIGMA_SPECTRUM",
        "WEIGHT_SPECTRUM",
        "CORRECTED_WEIGHT_SPECTRUM",
    ] {
        let binding = bindings.get(column).expect("channelized weight binding");
        assert_eq!(binding.data_manager, DataManagerKind::TiledShapeStMan);
        assert_eq!(binding.tile_shape, data_tile_shape);
    }
}

#[test]
fn add_antenna_via_ms() {
    let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();
    {
        let mut ant = ms.antenna_mut().unwrap();
        ant.add_antenna(
            "VLA01",
            "N01",
            "GROUND-BASED",
            "ALT-AZ",
            [1.0; 3],
            [0.0; 3],
            25.0,
        )
        .unwrap();
    }
    assert_eq!(ms.antenna().unwrap().row_count(), 1);
    assert_eq!(ms.antenna().unwrap().name(0).unwrap(), "VLA01");
}

#[test]
fn create_and_reopen_on_disk() {
    let dir = tempfile::tempdir().unwrap();
    let ms_path = dir.path().join("test.ms");

    // Create
    {
        let mut ms = MeasurementSet::create(&ms_path, MeasurementSetBuilder::new()).unwrap();
        {
            let mut ant = ms.antenna_mut().unwrap();
            ant.add_antenna(
                "ALMA01",
                "A001",
                "GROUND-BASED",
                "ALT-AZ",
                [2.0; 3],
                [0.0; 3],
                12.0,
            )
            .unwrap();
        }
        ms.save().unwrap();
    }

    // Reopen
    {
        let ms = MeasurementSet::open(&ms_path).unwrap();
        assert_eq!(ms.antenna().unwrap().row_count(), 1);
        assert_eq!(ms.antenna().unwrap().name(0).unwrap(), "ALMA01");
    }
}

#[test]
fn in_place_saves_add_nothing_casacore_would_not_write() {
    let dir = tempfile::tempdir().unwrap();
    let ms_path = dir.path().join("in_place.ms");
    let mut ms = MeasurementSet::create(&ms_path, MeasurementSetBuilder::new()).unwrap();
    ms.save().unwrap();
    let mut ms = MeasurementSet::open(&ms_path).unwrap();
    ms.save().unwrap();
    ms.save_main_table_only().unwrap();

    let entries = fs::read_dir(&ms_path)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    assert!(
        entries.iter().all(|name| !name.contains("casa-rs")),
        "an in-place save left casa-rs files: {entries:?}"
    );
}

/// The save with MAIN column overrides records each table only when it
/// starts writing it: when writing MAIN fails, ANTENNA, with a row
/// pending in memory, publishes nothing and reopens with its persisted
/// rows.
#[cfg(unix)]
#[test]
fn a_failed_main_save_with_column_overrides_publishes_no_unwritten_subtable() {
    let dir = tempfile::tempdir().unwrap();
    let ms_path = dir.path().join("overrides.ms");
    {
        let mut ms = MeasurementSet::create(&ms_path, MeasurementSetBuilder::new()).unwrap();
        for name in ["ALMA01", "ALMA02"] {
            ms.antenna_mut()
                .unwrap()
                .add_antenna(
                    name,
                    "A001",
                    "GROUND-BASED",
                    "ALT-AZ",
                    [2.0; 3],
                    [0.0; 3],
                    12.0,
                )
                .unwrap();
        }
        ms.save().unwrap();
    }
    let antenna = ms_path.join("ANTENNA");
    let published_counter = |table: &Path| {
        let mut table = Table::open_with_lock(
            TableOptions::new(table),
            LockOptions::new(LockMode::UserLocking),
        )
        .unwrap();
        assert!(table.lock(casa_tables::LockType::Read, 1).unwrap());
        table.locked_modify_counter().unwrap()
    };
    let counter_before = published_counter(&antenna);

    let mut ms = MeasurementSet::open(&ms_path).unwrap();
    let antenna_table = ms.subtable_mut(SubtableId::Antenna).unwrap();
    let row = antenna_table.row_accessor().row(0).unwrap().clone();
    antenna_table.add_row(row).unwrap();
    // MAIN's table.dat becomes a directory, which the save cannot write.
    let main_table_dat = ms_path.join("table.dat");
    fs::remove_file(&main_table_dat).unwrap();
    fs::create_dir(&main_table_dat).unwrap();
    fs::write(main_table_dat.join("blocker"), b"").unwrap();
    assert!(
        ms.save_with_column_overrides(&ColumnOverrides::default(), &SubtableColumnOverrides::new())
            .is_err()
    );
    drop(ms);

    assert_eq!(published_counter(&antenna), counter_before);
    assert_eq!(
        Table::open(TableOptions::new(&antenna))
            .unwrap()
            .row_count(),
        2
    );
}

#[test]
fn open_uses_subtable_keyword_paths() {
    let dir = tempfile::tempdir().unwrap();
    let ms_path = dir.path().join("keyword_paths.ms");

    {
        let mut ms = MeasurementSet::create(&ms_path, MeasurementSetBuilder::new()).unwrap();
        ms.antenna_mut()
            .unwrap()
            .add_antenna(
                "ALMA01",
                "A001",
                "GROUND-BASED",
                "ALT-AZ",
                [1.0; 3],
                [0.0; 3],
                12.0,
            )
            .unwrap();
        ms.save().unwrap();
    }

    let relocated = ms_path.join("ANTENNA_RELOCATED");
    fs::rename(ms_path.join("ANTENNA"), &relocated).unwrap();

    {
        let mut main = Table::open(TableOptions::new(&ms_path)).unwrap();
        main.keywords_mut()
            .upsert("ANTENNA", Value::table_ref("././ANTENNA_RELOCATED"));
        main.save(TableOptions::new(&ms_path)).unwrap();
    }

    let reopened = MeasurementSet::open(&ms_path).unwrap();
    assert_eq!(reopened.antenna().unwrap().row_count(), 1);
    assert_eq!(reopened.antenna().unwrap().name(0).unwrap(), "ALMA01");
}

#[test]
fn open_uses_legacy_string_keyword_paths() {
    let dir = tempfile::tempdir().unwrap();
    let ms_path = dir.path().join("legacy_keyword_paths.ms");

    {
        let mut ms = MeasurementSet::create(&ms_path, MeasurementSetBuilder::new()).unwrap();
        ms.antenna_mut()
            .unwrap()
            .add_antenna(
                "ALMA01",
                "A001",
                "GROUND-BASED",
                "ALT-AZ",
                [1.0; 3],
                [0.0; 3],
                12.0,
            )
            .unwrap();
        ms.save().unwrap();
    }

    let relocated = ms_path.join("ANTENNA_RELOCATED");
    fs::rename(ms_path.join("ANTENNA"), &relocated).unwrap();

    {
        let mut main = Table::open(TableOptions::new(&ms_path)).unwrap();
        main.keywords_mut().upsert(
            "ANTENNA",
            Value::Scalar(ScalarValue::String("ANTENNA_RELOCATED".to_string())),
        );
        main.save(TableOptions::new(&ms_path)).unwrap();
    }

    let reopened = MeasurementSet::open(&ms_path).unwrap();
    assert_eq!(reopened.antenna().unwrap().row_count(), 1);
    assert_eq!(reopened.antenna().unwrap().name(0).unwrap(), "ALMA01");
}

#[test]
fn save_as_rewrites_subtable_keywords_relative_to_new_path() {
    let dir = tempfile::tempdir().unwrap();
    let first_path = dir.path().join("first.ms");
    let second_path = dir.path().join("second.ms");

    let mut ms = MeasurementSet::create(&first_path, MeasurementSetBuilder::new()).unwrap();
    ms.save_as(&second_path).unwrap();

    let reopened = Table::open(TableOptions::new(&second_path)).unwrap();
    assert_eq!(
        reopened.keywords().get("ANTENNA"),
        Some(&Value::TableRef("././ANTENNA".to_string()))
    );
    assert_eq!(reopened.info().table_type, "Measurement Set");
}

#[test]
fn save_main_table_only_persists_main_mutations_without_rewriting_subtables() {
    let dir = tempfile::tempdir().unwrap();
    let ms_path = dir.path().join("main-only.ms");

    let mut ms = MeasurementSet::create(&ms_path, MeasurementSetBuilder::new()).unwrap();
    ms.antenna_mut()
        .unwrap()
        .add_antenna(
            "ALMA01",
            "A001",
            "GROUND-BASED",
            "ALT-AZ",
            [1.0; 3],
            [0.0; 3],
            12.0,
        )
        .unwrap();
    ms.save().unwrap();

    let antenna_before = fs::metadata(ms_path.join("ANTENNA"))
        .unwrap()
        .modified()
        .unwrap();

    let schema = ms.main_table().schema().unwrap().clone();
    let fields: Vec<RecordField> = schema
        .columns()
        .iter()
        .map(|col| {
            let value = if col.name() == "FIELD_ID" {
                Value::Scalar(ScalarValue::Int32(1))
            } else {
                default_value(col.name())
            };
            RecordField::new(col.name(), value)
        })
        .collect();
    ms.main_table_mut()
        .add_row(RecordValue::new(fields))
        .unwrap();
    ms.save_main_table_only().unwrap();

    let reopened = MeasurementSet::open(&ms_path).unwrap();
    assert_eq!(reopened.row_count(), 1);
    match reopened
        .main_table()
        .cell_accessor(0, "FIELD_ID")
        .unwrap()
        .scalar()
        .expect("field id cell")
    {
        &ScalarValue::Int32(value) => assert_eq!(value, 1),
        other => panic!(
            "expected FIELD_ID to be Int32, found {:?}",
            other.primitive_type()
        ),
    }
    assert_eq!(reopened.antenna().unwrap().row_count(), 1);

    let antenna_after = fs::metadata(ms_path.join("ANTENNA"))
        .unwrap()
        .modified()
        .unwrap();
    assert_eq!(antenna_before, antenna_after);
}

#[test]
fn validation_fails_on_wrong_column_type() {
    use casa_tables::{ColumnSchema, TableSchema};

    // Create a table with TIME as Int32 instead of Float64
    let bad_schema = TableSchema::new(vec![
        ColumnSchema::scalar("TIME", PrimitiveType::Int32), // wrong type!
    ])
    .unwrap();
    let table = Table::with_schema(bad_schema);

    let issues = validate::validate_columns(
        &table,
        "MAIN",
        &[crate::column_def::ColumnDef {
            name: "TIME",
            data_type: PrimitiveType::Float64,
            column_kind: crate::column_def::ColumnKind::Scalar,
            unit: "s",
            measure_type: None,
            measure_ref: "",
            comment: "",
        }],
    );
    assert_eq!(issues.len(), 1);
    assert!(matches!(issues[0], ValidationIssue::WrongColumnType { .. }));
}
