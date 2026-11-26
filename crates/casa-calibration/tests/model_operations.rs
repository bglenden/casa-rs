// SPDX-License-Identifier: LGPL-3.0-or-later

mod common;

use casa_calibration::{delete_calibration_model, reset_calibration_set};
use casa_core::{ArrayValue, Complex32, RecordValue, Value};
use casa_ms::column_def::build_table_schema;
use casa_ms::schema::main_table::OptionalMainColumn;
use casa_ms::virtual_model::{put_virtual_model, virtual_model};
use casa_ms::{MeasurementSet, MsSelection, VirtualModelEntry, VirtualModelKind};
use casa_tables::TableWriteLock;
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

#[test]
fn imager_attachment_keeps_the_opened_ms_and_replaces_uv_selection() {
    use casa_calibration::{AttachmentError, AttachmentMode, AttachmentOptions, ImagerAttachments};

    let temp = TempDir::new().unwrap();
    let first = temp.path().join("first");
    let second = temp.path().join("second");
    std::fs::create_dir(&first).unwrap();
    std::fs::create_dir(&second).unwrap();
    let first = common::create_apply_fixture_ms(&first, true);
    let second = common::create_apply_fixture_ms(&second, true);
    set_fixture_uv_distances(&first);

    let mut object = ImagerAttachments::default();
    object.open(&first, AttachmentOptions::default()).unwrap();
    assert!(matches!(
        object.narrow_uvrange(0.0, 1.0),
        Err(AttachmentError::UvRangeRequiresSelection)
    ));
    assert_eq!(
        object.inputs()[0]
            .resolve()
            .unwrap()
            .row_indices()
            .collect::<Vec<_>>(),
        [0, 1]
    );
    let selected = MsSelection {
        spw: Some("0:0~1^2,1:0~1^2".into()),
        ..MsSelection::default()
    };
    assert!(
        object
            .select(Some(&second), AttachmentOptions::default(), selected)
            .unwrap()
    );
    let [input] = object.inputs() else {
        panic!("explicit open keeps one attachment");
    };
    assert_eq!(input.mode(), AttachmentMode::Open);
    assert_eq!(input.path(), casa_tables::table_cache_name(&first).unwrap());
    let selection = input.resolve().unwrap();
    assert_eq!(selection.row_indices().collect::<Vec<_>>(), [0, 1]);
    assert_eq!(selection.channel_selections[&0].indices(2).unwrap(), [0]);

    let mut distance_hz = 0.0;
    let ms = MeasurementSet::open(&first).unwrap();
    for window in 0..2 {
        let frequencies = ms.spectral_window().unwrap().chan_freq(window).unwrap();
        distance_hz += frequencies.iter().sum::<f64>();
    }
    let cutoff = 50.0 * distance_hz / (4.0 * 299_792_458.0);
    object.narrow_uvrange(0.0, cutoff).unwrap();
    assert_eq!(
        object.inputs()[0]
            .resolve()
            .unwrap()
            .row_indices()
            .collect::<Vec<_>>(),
        [0]
    );

    // A valid range with no matching data keeps the preceding cut.
    object.narrow_uvrange(1e9, 1e10).unwrap();
    assert_eq!(
        object.inputs()[0]
            .resolve()
            .unwrap()
            .row_indices()
            .collect::<Vec<_>>(),
        [0]
    );

    let replacement = MsSelection {
        field: Some("1".into()),
        ..MsSelection::default()
    };
    assert!(
        object
            .select(None, AttachmentOptions::default(), replacement)
            .unwrap()
    );
    assert_eq!(
        object.inputs()[0]
            .resolve()
            .unwrap()
            .row_indices()
            .collect::<Vec<_>>(),
        [1]
    );
    for rejected in [
        MsSelection {
            field: Some("1".into()),
            spw: Some("0".into()),
            ..MsSelection::default()
        },
        MsSelection {
            field: Some("unknown-field".into()),
            ..MsSelection::default()
        },
    ] {
        assert!(
            !object
                .select(None, AttachmentOptions::default(), rejected)
                .unwrap()
        );
        let input = &object.inputs()[0];
        assert!(matches!(
            input.resolve(),
            Err(AttachmentError::RejectedSelection)
        ));
        assert!(matches!(
            input.with_write::<()>(|_, _, _| panic!("stale selection write")),
            Err(AttachmentError::RejectedSelection)
        ));
        let valid = MsSelection {
            field: Some("0".into()),
            spw: Some("0:1".into()),
            ..MsSelection::default()
        };
        assert!(
            object
                .select(Some(&second), AttachmentOptions::default(), valid)
                .unwrap()
        );
        let recovered = object.inputs()[0].resolve().unwrap();
        assert_eq!(recovered.row_indices().collect::<Vec<_>>(), [0]);
        assert_eq!(recovered.channel_selections[&0].indices(2).unwrap(), [1]);
    }
    object.close();
    assert!(object.inputs().is_empty());
    object.open(&second, AttachmentOptions::default()).unwrap();
    assert_eq!(
        object.inputs()[0].path(),
        casa_tables::table_cache_name(&second).unwrap()
    );
    assert_eq!(
        object.inputs()[0]
            .resolve()
            .unwrap()
            .row_indices()
            .collect::<Vec<_>>(),
        [0, 1]
    );
}

#[test]
fn selection_attachments_keep_each_input_and_its_access_flags() {
    use casa_calibration::{AttachmentError, AttachmentMode, AttachmentOptions, ImagerAttachments};

    let temp = TempDir::new().unwrap();
    let first = temp.path().join("first");
    let second = temp.path().join("second");
    std::fs::create_dir(&first).unwrap();
    std::fs::create_dir(&second).unwrap();
    let first = common::create_apply_fixture_ms(&first, true);
    let second = common::create_apply_fixture_ms(&second, true);
    populate_model(&mut MeasurementSet::open(&second).unwrap());

    let mut object = ImagerAttachments::default();
    let writable = AttachmentOptions::default();
    let readonly = AttachmentOptions {
        write_access: false,
        use_scratch: true,
    };
    let first_selection = MsSelection {
        field: Some("0".into()),
        spw: Some("0:1".into()),
        ..MsSelection::default()
    };
    let second_selection = MsSelection {
        field: Some("1".into()),
        spw: Some("1:0".into()),
        ..MsSelection::default()
    };
    assert!(
        object
            .select(Some(&first), writable, first_selection)
            .unwrap()
    );
    assert!(
        object
            .select(Some(&second), readonly, second_selection)
            .unwrap()
    );
    let empty = MsSelection {
        field: Some("1".into()),
        spw: Some("0".into()),
        ..MsSelection::default()
    };
    let mut unattached = ImagerAttachments::default();
    assert!(matches!(
        unattached.select(Some(&second), readonly, empty.clone()),
        Err(AttachmentError::EmptySelection)
    ));
    assert!(unattached.inputs().is_empty());
    assert!(matches!(
        object.select(Some(&second), readonly, empty),
        Err(AttachmentError::EmptySelection)
    ));
    let [first_input, second_input] = object.inputs() else {
        panic!("two selected inputs");
    };
    assert_eq!(first_input.mode(), AttachmentMode::Selection);
    assert_eq!(second_input.mode(), AttachmentMode::Selection);
    assert_eq!(
        first_input.path(),
        casa_tables::table_cache_name(&first).unwrap()
    );
    assert_eq!(
        second_input.path(),
        casa_tables::table_cache_name(&second).unwrap()
    );
    assert_eq!(first_input.options(), writable);
    assert_eq!(second_input.options(), readonly);
    let first_selected = first_input.resolve().unwrap();
    let second_selected = second_input.resolve().unwrap();
    assert_eq!(first_selected.row_indices().collect::<Vec<_>>(), [0]);
    assert_eq!(second_selected.row_indices().collect::<Vec<_>>(), [1]);
    assert_eq!(
        first_selected.channel_selections[&0].indices(2).unwrap(),
        [1]
    );
    assert_eq!(
        second_selected.channel_selections[&1].indices(2).unwrap(),
        [0]
    );
    assert!(matches!(
        second_input.with_write::<()>(|_, _, _| panic!("readonly write")),
        Err(AttachmentError::ReadOnly)
    ));
    assert!(matches!(
        object.writable_inputs(),
        Err(AttachmentError::ReadOnly)
    ));
    let mut reversed = ImagerAttachments::default();
    assert!(
        reversed
            .select(Some(&second), readonly, MsSelection::default())
            .unwrap()
    );
    assert!(
        reversed
            .select(Some(&first), writable, MsSelection::default())
            .unwrap()
    );
    assert!(matches!(
        reversed.writable_inputs(),
        Err(AttachmentError::ReadOnlyInput)
    ));

    let reopened = MeasurementSet::open(&second).unwrap();
    assert!(virtual_model(&reopened, 0).unwrap().is_some());
}

#[test]
fn multi_ms_uvrange_switches_the_active_iterator_and_the_next_selection_restores_it() {
    use casa_calibration::{AttachmentOptions, ImagerAttachments};

    let temp = TempDir::new().unwrap();
    let inputs = ["first", "second", "third"].map(|name| {
        let folder = temp.path().join(name);
        std::fs::create_dir(&folder).unwrap();
        common::create_apply_fixture_ms(&folder, true)
    });
    set_fixture_uv_distances(&inputs[1]);
    let mut object = ImagerAttachments::default();
    let options = AttachmentOptions::default();
    for path in &inputs[..2] {
        assert!(
            object
                .select(Some(path), options, MsSelection::default())
                .unwrap()
        );
    }
    assert_eq!(object.writable_inputs().unwrap().len(), 2);

    // A valid empty cut changes the active MS, with its original rows.
    object.narrow_uvrange(1e9, 1e10).unwrap();
    assert_eq!(object.inputs().len(), 2);
    let [current] = object.writable_inputs().unwrap() else {
        panic!("current MS iterator");
    };
    assert_eq!(
        current.path(),
        casa_tables::table_cache_name(&inputs[1]).unwrap()
    );
    assert_eq!(
        current.resolve().unwrap().row_indices().collect::<Vec<_>>(),
        [0, 1]
    );

    // 170 wavelengths retains the 10 m row and removes the 100 m row.
    object.narrow_uvrange(0.0, 170.0).unwrap();
    assert_eq!(
        object.writable_inputs().unwrap()[0]
            .resolve()
            .unwrap()
            .row_indices()
            .collect::<Vec<_>>(),
        [0]
    );

    assert!(
        object
            .select(Some(&inputs[2]), options, MsSelection::default())
            .unwrap()
    );
    assert_eq!(object.writable_inputs().unwrap().len(), 3);
    assert_eq!(
        object.writable_inputs().unwrap()[1]
            .resolve()
            .unwrap()
            .row_indices()
            .collect::<Vec<_>>(),
        [0, 1]
    );
}

#[cfg(unix)]
#[test]
fn attached_mutation_rereads_selection_with_main_locked() {
    use casa_calibration::{
        AttachmentError, AttachmentMode, AttachmentOptions, CalibrationAttachment,
    };
    use casa_core::ScalarValue;
    use std::process::Command;

    const PROBE_PATH: &str = "CASARS_TEST_CALIBRATION_ATTACHMENT_PROBE";
    if let Some(path) = std::env::var_os(PROBE_PATH) {
        assert!(TableWriteLock::acquire_to_reread(std::path::Path::new(&path), 1).is_err());
        return;
    }

    let temp = TempDir::new().unwrap();
    let path = common::create_apply_fixture_ms(temp.path(), true);
    add_scratch_model(&mut MeasurementSet::open(&path).unwrap());
    let attachment = CalibrationAttachment::attach(
        &path,
        AttachmentMode::Selection,
        AttachmentOptions::default(),
        MsSelection {
            field: Some("0".into()),
            spw: Some("0:0~1^2,1:0~1^2".into()),
            ..MsSelection::default()
        },
    )
    .unwrap();
    assert_eq!(
        attachment
            .resolve()
            .unwrap()
            .row_indices()
            .collect::<Vec<_>>(),
        [0]
    );

    let mut writer = MeasurementSet::open(&path).unwrap();
    for (row, field) in [(0, 1), (1, 0)] {
        writer
            .main_table_mut()
            .cell_accessor_mut(row, "FIELD_ID")
            .unwrap()
            .set(Value::Scalar(ScalarValue::Int32(field)))
            .unwrap();
    }
    writer.save_main_table_only().unwrap();
    drop(writer);

    attachment
        .with_write(|ms, selection, lock| {
            assert_eq!(lock.path(), ms.path().unwrap());
            assert_eq!(selection.row_indices().collect::<Vec<_>>(), [1]);
            let mut child = Command::new(std::env::current_exe().unwrap());
            let result = child
                .args([
                    "--exact",
                    "attached_mutation_rereads_selection_with_main_locked",
                ])
                .env(PROBE_PATH, &path)
                .output()
                .unwrap();
            let output = String::from_utf8_lossy(&result.stdout);
            assert!(
                result.status.success() && output.contains("1 passed"),
                "{output}"
            );

            let row = &selection.selected_rows[0];
            let channels = selection.channel_selections[&row.spectral_window_id].indices(2)?;
            let mut cell = ms
                .main_table()
                .cell_accessor(row.row_index, "MODEL_DATA")?
                .array()?
                .clone();
            let ArrayValue::Complex32(data) = &mut cell else {
                panic!("complex scratch model");
            };
            for channel in channels {
                data[[0, channel]] = Complex32::new(3.0, 0.0);
            }
            ms.main_table_mut()
                .cell_accessor_mut(row.row_index, "MODEL_DATA")?
                .set(Value::Array(cell))?;
            lock.record_write();
            ms.main_table()
                .save(casa_tables::TableOptions::new(lock.path()))?;
            Ok(())
        })
        .unwrap_or_else(|error: AttachmentError| panic!("{error}"));

    let rejected = attachment.with_write::<()>(|_, _, _| Err(AttachmentError::EmptySelection));
    assert!(matches!(rejected, Err(AttachmentError::EmptySelection)));
    TableWriteLock::acquire_to_reread(&path, 1)
        .unwrap()
        .release()
        .unwrap();
    let ms = MeasurementSet::open(&path).unwrap();
    let column = ms.main_table().column_accessor("MODEL_DATA").unwrap();
    let ArrayValue::Complex32(values) = column.array_cell(0).unwrap() else {
        panic!("complex model");
    };
    let prior = Complex32::new(13.0, 7.0);
    assert!(values.iter().all(|value| *value == prior));
    let ArrayValue::Complex32(values) = column.array_cell(1).unwrap() else {
        panic!("complex model");
    };
    assert_eq!(values[[0, 0]], Complex32::new(3.0, 0.0));
    assert_eq!(values[[0, 1]], prior);
    assert_eq!(values[[1, 0]], prior);
}

fn set_fixture_uv_distances(path: &std::path::Path) {
    let mut ms = MeasurementSet::open(path).unwrap();
    for (row, distance) in [(0, 10.0), (1, 100.0)] {
        ms.main_table_mut()
            .cell_accessor_mut(row, "UVW")
            .unwrap()
            .set(Value::Array(ArrayValue::Float64(
                ndarray::arr1(&[distance, 0.0, 0.0]).into_dyn(),
            )))
            .unwrap();
    }
    ms.save().unwrap();
}
