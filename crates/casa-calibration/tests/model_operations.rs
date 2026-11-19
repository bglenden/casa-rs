// SPDX-License-Identifier: LGPL-3.0-or-later

mod common;

use casa_calibration::{delete_calibration_model, reset_calibration_set};
use casa_ms::column_def::build_table_schema;
use casa_ms::schema::main_table::OptionalMainColumn;
use casa_ms::virtual_model::{put_virtual_model, virtual_model};
use casa_ms::{MeasurementSet, MsSelection, VirtualModelEntry, VirtualModelKind};
use casa_tables::TableWriteLock;
use casa_types::{ArrayValue, Complex32, RecordValue, Value};
use ndarray::{ArrayD, IxDyn};
use tempfile::TempDir;

fn populate_model(ms: &mut MeasurementSet) {
    put_virtual_model(
        ms,
        &VirtualModelEntry {
            kind: VirtualModelKind::ComponentList,
            index_combination: vec![[0, 0, 0, 0], [0, 1, 0, 0]],
            channel_selection: vec![[0, 0, 2, 1], [1, 0, 2, 1]],
            container: RecordValue::default(),
        },
        false,
    )
    .unwrap();
}

fn add_scratch_model(ms: &mut MeasurementSet) {
    let column = OptionalMainColumn::ModelData;
    let schema = build_table_schema(&[*column.column_def()]).unwrap();
    let value = Value::Array(ArrayValue::Complex32(ArrayD::from_elem(
        IxDyn(&[2, 2]),
        Complex32::new(13.0, 7.0),
    )));
    ms.main_table_mut()
        .add_column(schema.column(column.name()).unwrap().clone(), Some(value))
        .unwrap();
    ms.save().unwrap();
}

#[test]
fn selected_scratch_initialization_handles_virtual_models_by_calset() {
    for (old, calset, keeps_virtual) in [(true, 0, true), (true, 1, false), (false, 1, false)] {
        let temp = TempDir::new().unwrap();
        let path = common::create_apply_fixture_ms(temp.path(), true);
        let mut ms = MeasurementSet::open(&path).unwrap();
        add_scratch_model(&mut ms);
        populate_model(&mut ms);
        drop(ms);
        let lock = TableWriteLock::acquire_to_reread(&path, 0).unwrap();
        let mut ms = MeasurementSet::open(&path).unwrap();
        let selection = MsSelection {
            field: Some("0".into()),
            spw: Some("0:1".into()),
            ..MsSelection::default()
        };
        let result = reset_calibration_set(&mut ms, &selection, calset, old, lock);
        if old {
            result.unwrap().release().unwrap();
        } else {
            assert!(
                result
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("initCalSet not implemented")
            );
        }
        let reopened = MeasurementSet::open(&path).unwrap();
        assert_eq!(
            virtual_model(&reopened, 0).unwrap().is_some(),
            keeps_virtual
        );
        let column = reopened.main_table().column_accessor("MODEL_DATA").unwrap();
        let ArrayValue::Complex32(first) = column.array_cell(0).unwrap() else {
            panic!("first model row must be complex");
        };
        let ArrayValue::Complex32(second) = column.array_cell(1).unwrap() else {
            panic!("second model row must be complex");
        };
        let prior = Complex32::new(13.0, 7.0);
        assert_eq!(first[[0, 0]], prior);
        assert_eq!(
            first[[0, 1]],
            if old { Complex32::new(1.0, 0.0) } else { prior }
        );
        assert!(second.iter().all(|&value| value == prior));
    }
}

#[test]
fn deletes_only_the_named_virtual_model_window() {
    let temp = TempDir::new().unwrap();
    let path = common::create_apply_fixture_ms(temp.path(), true);
    let mut ms = MeasurementSet::open(&path).unwrap();
    populate_model(&mut ms);
    let mut lock = TableWriteLock::acquire(&path, 0).unwrap();
    let name = ms.spectral_window().unwrap().name(0).unwrap().to_owned();
    delete_calibration_model(&mut ms, Some("0"), Some(&name), true, false, &mut lock).unwrap();
    lock.release().unwrap();
    let reopened = MeasurementSet::open(&path).unwrap();
    let record = virtual_model(&reopened, 0).unwrap().unwrap().record;
    let entries = VirtualModelEntry::entries(&record).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].index_combination, [[0, 1, 0, 0]]);
    let mut lock = TableWriteLock::acquire(&path, 0).unwrap();
    delete_calibration_model(
        &mut ms,
        Some("false"),
        Some("missing-window"),
        true,
        false,
        &mut lock,
    )
    .unwrap();
    lock.release().unwrap();
    assert!(
        virtual_model(&MeasurementSet::open(&path).unwrap(), 0)
            .unwrap()
            .is_none()
    );
}

#[test]
fn invalid_selectors_leave_both_model_representations_in_place() {
    let temp = TempDir::new().unwrap();
    let path = common::create_apply_fixture_ms(temp.path(), true);
    let mut ms = MeasurementSet::open(&path).unwrap();
    add_scratch_model(&mut ms);
    populate_model(&mut ms);
    drop(ms);
    let mut lock = TableWriteLock::acquire_to_reread(&path, 0).unwrap();
    let mut ms = MeasurementSet::open(&path).unwrap();
    for (field, spw) in [("999", None), ("0", Some("999")), (" ", Some("999"))] {
        assert!(
            delete_calibration_model(&mut ms, Some(field), spw, true, true, &mut lock).is_err()
        );
    }
    lock.release().unwrap();
    let reopened = MeasurementSet::open(&path).unwrap();
    assert!(virtual_model(&reopened, 0).unwrap().is_some());
    assert!(
        reopened
            .main_table()
            .schema()
            .unwrap()
            .contains_column("MODEL_DATA")
    );
}

#[test]
fn a_write_lock_on_another_ms_cannot_clear_models() {
    let first = TempDir::new().unwrap();
    let second = TempDir::new().unwrap();
    let path = common::create_apply_fixture_ms(first.path(), true);
    let other = common::create_apply_fixture_ms(second.path(), true);
    let mut ms = MeasurementSet::open(&path).unwrap();
    add_scratch_model(&mut ms);
    populate_model(&mut ms);
    drop(ms);
    let mut ms = MeasurementSet::open(&path).unwrap();
    let mut lock = TableWriteLock::acquire(&other, 0).unwrap();
    assert!(delete_calibration_model(&mut ms, None, None, true, true, &mut lock).is_err());
    let failure = reset_calibration_set(&mut ms, &MsSelection::default(), 1, false, lock);
    assert!(
        failure
            .err()
            .unwrap()
            .to_string()
            .contains("is not on MAIN")
    );
    let reopened = MeasurementSet::open(&path).unwrap();
    assert!(virtual_model(&reopened, 0).unwrap().is_some());
    assert!(
        reopened
            .main_table()
            .schema()
            .unwrap()
            .contains_column("MODEL_DATA")
    );
    assert!(
        virtual_model(&MeasurementSet::open(&other).unwrap(), 0)
            .unwrap()
            .is_none()
    );
}
