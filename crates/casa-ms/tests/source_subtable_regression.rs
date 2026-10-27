// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_ms::MeasurementSet;
use casa_test_support::casatestdata_path;
use casa_types::{ArrayValue, Value};

#[test]
fn ngc5921_with_flags_source_subtable_materializes_source_model_records() {
    let Some(ms_path) =
        casatestdata_path("measurementset/vla/ngc5921_with_flags.ms").filter(|path| path.exists())
    else {
        eprintln!("skipping SOURCE regression test: ngc5921_with_flags.ms not available");
        return;
    };

    let ms = MeasurementSet::open(&ms_path).expect("open ngc5921_with_flags.ms");
    let source = ms.source().expect("SOURCE accessor");
    assert!(
        source.row_count() > 0,
        "SOURCE subtable should contain rows"
    );

    let first_row = source.table().row_accessor().row(0).expect("SOURCE row 0");
    assert!(
        matches!(first_row.get("SOURCE_MODEL"), Some(Value::Record(_))),
        "expected SOURCE_MODEL to materialize as a record, got {:?}",
        first_row.get("SOURCE_MODEL")
    );
}

/// casacore stores the empty SOURCE cells of `alma/multiobs.ms` (rows 0 and
/// 2 of `TRANSITION`, `REST_FREQUENCY` and `SYSVEL`; rows 1 and 3 are
/// undefined) with a 0-dimensional shape, an array of no elements (#923).
/// casa-rs reads each as CASA does: empty, not one element.
#[test]
fn multiobs_source_reads_zero_dimensional_cells_as_empty() {
    let Some(ms_path) =
        casatestdata_path("measurementset/alma/multiobs.ms").filter(|path| path.exists())
    else {
        eprintln!("skipping SOURCE regression test: alma/multiobs.ms not available");
        return;
    };

    let ms = MeasurementSet::open(&ms_path).expect("open multiobs.ms");
    let source = ms.source().expect("SOURCE accessor");
    let table = source.table();
    assert_eq!(table.row_count(), 4);
    for column in ["TRANSITION", "REST_FREQUENCY", "SYSVEL"] {
        for row_index in [0, 2] {
            let row = table.row_accessor().row(row_index).expect("SOURCE row");
            let shape = match row.get(column) {
                Some(Value::Array(ArrayValue::String(array))) => array.shape().to_vec(),
                Some(Value::Array(ArrayValue::Float64(array))) => array.shape().to_vec(),
                other => panic!("{column} row {row_index}: an array, not {other:?}"),
            };
            assert_eq!(shape, [0], "{column} row {row_index}");
        }
        for row_index in [1, 3] {
            assert!(
                !table
                    .is_cell_defined(row_index, column)
                    .expect("cell definition"),
                "{column} row {row_index} is undefined"
            );
        }
    }
}
