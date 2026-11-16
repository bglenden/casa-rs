// SPDX-License-Identifier: LGPL-3.0-or-later
//! `ARRAY_ID` through the MSv2 adapter: the partition key, XRADIO's, has no
//! array, so a partition states the array its rows share, or that they hold
//! several, and the rows' arrays can be read for the latter (#980). CASA's
//! imaging iterator orders its buffers by `ARRAY_ID` first.

use casa_ms::MeasurementSet;
use casa_ms::observation_io::Msv2Observation;
use casa_types::{ScalarValue, Value};

use super::fields::mosaic;

fn field_of(ms: &MeasurementSet, row: usize) -> i32 {
    match ms
        .main_table()
        .cell_accessor(row, "FIELD_ID")
        .unwrap()
        .scalar()
        .unwrap()
    {
        ScalarValue::Int32(field) => *field,
        other => panic!("FIELD_ID is {other:?}"),
    }
}

#[test]
fn a_partition_states_its_one_array_or_that_it_holds_several() {
    let root = tempfile::tempdir().unwrap();
    let (path, _) = mosaic(root.path(), 4);
    let opened = Msv2Observation::open(&path).unwrap();
    let ids = opened.catalogue().partition_ids();
    assert!(!ids.is_empty());
    assert!(ids.iter().all(|id| opened.array_id(*id) == Some(0)));
    drop(opened);

    // Field 2's rows move to array 1; they share their partitions with the
    // other fields' rows.
    let mut ms = MeasurementSet::open(&path).unwrap();
    let rows = ms.row_count();
    let (mut moved, mut kept) = (None, None);
    for row in 0..rows {
        if field_of(&ms, row) == 2 {
            ms.main_table_mut()
                .row_accessor_mut()
                .set_cell(row, "ARRAY_ID", Value::Scalar(ScalarValue::Int32(1)))
                .unwrap();
            moved.get_or_insert(row);
        } else {
            kept.get_or_insert(row);
        }
    }
    ms.save().unwrap();
    drop(ms);
    let (moved, kept) = (moved.unwrap(), kept.unwrap());

    let opened = Msv2Observation::open(&path).unwrap();
    let ids = opened.catalogue().partition_ids();
    assert!(
        ids.iter().any(|id| opened.array_id(*id).is_none()),
        "a partition holding rows of arrays 0 and 1 states no one array"
    );
    let mut arrays = vec![7];
    opened
        .array_ids(&[moved, kept, moved], &mut arrays)
        .unwrap();
    assert_eq!(arrays, [1, 0, 1]);
}
