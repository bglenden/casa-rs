// SPDX-License-Identifier: LGPL-3.0-or-later

use casacore_ms::MeasurementSet;
use casacore_test_support::casatestdata_path;
use casacore_types::Value;

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

    let first_row = source.table().row(0).expect("SOURCE row 0");
    assert!(
        matches!(first_row.get("SOURCE_MODEL"), Some(Value::Record(_))),
        "expected SOURCE_MODEL to materialize as a record, got {:?}",
        first_row.get("SOURCE_MODEL")
    );
}
