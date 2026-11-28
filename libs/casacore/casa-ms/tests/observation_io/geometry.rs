// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_core::{ArrayValue, ScalarValue, Value};
use casa_ms::MeasurementSet;
use casa_ms::observation_io::{ObservationError, visit_stored_uvw};

use super::fixture;

#[test]
fn stored_msv2_uvw_visits_flagged_rows_across_an_admitted_batch_boundary() {
    let root = tempfile::tempdir().unwrap();
    let path = fixture(root.path(), "ms_trial.ms");
    let mut ms = MeasurementSet::open(&path).unwrap();
    ms.main_table_mut()
        .cell_accessor_mut(0, "FLAG_ROW")
        .unwrap()
        .set(Value::Scalar(ScalarValue::Bool(true)))
        .unwrap();
    ms.save().unwrap();

    let rows: Vec<_> = (0..ms.main_table().row_count()).collect();
    let expected: Vec<[f64; 3]> = ms
        .main_table()
        .column_accessor("UVW")
        .unwrap()
        .array_cells_owned(&rows)
        .unwrap()
        .into_iter()
        .map(|cell| match cell {
            Some(ArrayValue::Float64(values)) => values.as_slice().unwrap().try_into().unwrap(),
            _ => panic!("a defined UVW triple"),
        })
        .collect();
    drop(ms);

    let mut total = 0;
    visit_stored_uvw(
        &path,
        usize::MAX,
        &mut |bytes| {
            total = total.max(bytes);
            Ok(())
        },
        &mut |_| {},
    )
    .unwrap();

    let mut actual = Vec::new();
    visit_stored_uvw(&path, total - 1, &mut |_| Ok(()), &mut |uvw| {
        actual.push(uvw)
    })
    .unwrap();
    assert_eq!(actual, expected);
    assert!(matches!(
        visit_stored_uvw(
            &path,
            1,
            &mut |_| panic!("unadmitted grant"),
            &mut |_| panic!("unadmitted read")
        ),
        Err(ObservationError::OverBudget { budget: 1, .. })
    ));
}
