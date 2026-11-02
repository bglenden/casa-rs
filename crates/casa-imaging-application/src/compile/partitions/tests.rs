// SPDX-License-Identifier: LGPL-3.0-or-later
//! Intent selection against real partitions sharing one observing mode.

use casa_imaging_model::UvSelection;
use casa_ms::observation_io::Msv2Observation;
use casa_ms::{MeasurementSet, SubtableId};
use casa_types::{ScalarValue, Value};

use super::*;

fn state_partitions(separate: bool, flagged: bool) -> (tempfile::TempDir, OpenedStore) {
    let root = tempfile::tempdir().unwrap();
    let archive = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../casa-ms/tests/fixtures/ms_trial/ms_trial.ms.tgz");
    tar::Archive::new(flate2::read::GzDecoder::new(
        std::fs::File::open(archive).unwrap(),
    ))
    .unpack(root.path())
    .unwrap();
    let path = root.path().join("ms_trial.ms");
    let mut ms = MeasurementSet::open(&path).unwrap();
    let states = ms.subtable_mut(SubtableId::State).unwrap();
    states
        .cell_accessor_mut(0, "OBS_MODE")
        .unwrap()
        .set(Value::Scalar(ScalarValue::String(
            "TARGET#ON_SOURCE".into(),
        )))
        .unwrap();
    states
        .cell_accessor_mut(0, "FLAG_ROW")
        .unwrap()
        .set(Value::Scalar(ScalarValue::Bool(false)))
        .unwrap();
    let other_state = states.row_count();
    let mut other = states.row_accessor().row(0).unwrap().clone();
    other.upsert("FLAG_ROW", Value::Scalar(ScalarValue::Bool(flagged)));
    states.add_row(other).unwrap();
    let descriptions = ms.subtable_mut(SubtableId::DataDescription).unwrap();
    let other_description = descriptions.row_count();
    let copy = descriptions.row_accessor().row(0).unwrap().clone();
    descriptions.add_row(copy).unwrap();
    let half = ms.row_count() / 2;
    for row in 0..ms.row_count() {
        for (column, value) in [
            ("STATE_ID", if row < half { 0 } else { other_state }),
            (
                "DATA_DESC_ID",
                if separate && row >= half {
                    other_description
                } else {
                    0
                },
            ),
        ] {
            ms.main_table_mut()
                .cell_accessor_mut(row, column)
                .unwrap()
                .set(Value::Scalar(ScalarValue::Int32(value as i32)))
                .unwrap();
        }
    }
    ms.save().unwrap();
    drop(ms);
    let opened = OpenedStore::V2(Box::new(Msv2Observation::open(&path).unwrap()));
    (root, opened)
}

#[test]
fn shared_modes_do_not_select_partitions_of_unselected_or_flagged_states() {
    for (intent, flagged) in [("0", false), ("TARGET*", true)] {
        let (_root, store) = state_partitions(true, flagged);
        let resolved = intents::resolve(intent, store.catalogue()).unwrap();
        let rows = RowSelection::new(IdSelection::All, UvSelection::All, resolved.selection);
        let selected = selected(&store, &WindowChoice::All, (&rows, &resolved.states)).unwrap();
        assert_eq!(selected.len(), 1, "{intent}, flagged {flagged}");
        assert_eq!(
            store.catalogue().partition(selected[0]).states,
            vec![StateId(0)]
        );
    }
}

#[test]
fn a_selection_that_splits_states_inside_one_partition_is_still_refused() {
    let (_root, store) = state_partitions(false, false);
    let resolved = intents::resolve("0", store.catalogue()).unwrap();
    let rows = RowSelection::new(IdSelection::All, UvSelection::All, resolved.selection);
    assert!(matches!(
        selected(&store, &WindowChoice::All, (&rows, &resolved.states)),
        Err(PartitionSelectionError::Unsupported(_))
    ));
}
