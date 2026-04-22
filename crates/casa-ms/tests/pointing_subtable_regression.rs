// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_ms::MeasurementSet;
use casa_test_support::casatestdata_path;
use casa_types::ArrayValue;

fn papersky_mosaic_ms_path() -> Option<std::path::PathBuf> {
    casatestdata_path("measurementset/evla/papersky_mosaic.ms").filter(|path| path.exists())
}

#[test]
fn papersky_mosaic_pointing_direction_materializes_as_array() {
    let Some(ms_path) = papersky_mosaic_ms_path() else {
        eprintln!("skipping POINTING regression test: papersky_mosaic.ms not available");
        return;
    };

    let ms = MeasurementSet::open(&ms_path).expect("open papersky_mosaic.ms");
    let pointing = ms.pointing().expect("POINTING accessor");
    assert!(pointing.row_count() > 0, "POINTING should contain rows");

    let direction = pointing
        .table()
        .cell_accessor(0, "DIRECTION")
        .and_then(|cell| cell.array())
        .expect("POINTING.DIRECTION row 0 should materialize as an array");

    let ArrayValue::Float64(direction) = direction else {
        panic!(
            "POINTING.DIRECTION row 0 should be Float64 array, got {:?}",
            direction.primitive_type()
        );
    };

    assert_eq!(
        direction.shape(),
        &[2, 1],
        "POINTING.DIRECTION row 0 should keep the casacore cell shape",
    );
    let values: Vec<f64> = direction.iter().copied().collect();
    assert!(
        (values[0] + 1.058_214_942_099_811_3).abs() < 1.0e-12,
        "unexpected row 0 longitude: {:?}",
        values
    );
    assert!(
        (values[1] - 0.702_211_407_924_268_5).abs() < 1.0e-12,
        "unexpected row 0 latitude: {:?}",
        values
    );
}
