// SPDX-License-Identifier: LGPL-3.0-or-later

mod common;

use casa_calibration::{
    ContinuumSubtractionDataColumn, ContinuumSubtractionRequest, continuum_subtract,
};
use casa_ms::ms::MeasurementSet;
use casa_ms::schema::main_table::VisibilityDataColumn;
use casa_ms::selection::MsSelection;
use casa_types::ArrayValue;
use tempfile::TempDir;

use crate::common::create_apply_fixture_ms;

#[test]
fn continuum_subtract_writes_residual_data_ms() {
    let dir = TempDir::new().expect("tempdir");
    let input_ms = create_apply_fixture_ms(dir.path(), true);
    let output_ms = dir.path().join("contsub.ms");

    let report = continuum_subtract(&ContinuumSubtractionRequest {
        input_ms: input_ms.clone(),
        output_ms: output_ms.clone(),
        fit_spw: "0:0~1,1:0~1".to_string(),
        fit_order: 1,
        data_column: ContinuumSubtractionDataColumn::CorrectedData,
        selection: MsSelection::new(),
    })
    .expect("continuum subtract");

    assert_eq!(report.row_count, 2);
    assert_eq!(report.fitted_row_count, 2);
    assert_eq!(report.source_column, "CORRECTED_DATA");
    assert_eq!(report.output_column, "DATA");
    assert_eq!(report.spectral_window_ids, vec![0, 1]);

    let ms = MeasurementSet::open(&output_ms).expect("open output");
    let data_column = ms.data_column(VisibilityDataColumn::Data).expect("DATA");
    let data = data_column.get(0).expect("row data");
    let ArrayValue::Complex32(values) = data else {
        panic!("DATA should be complex");
    };
    assert!(values.iter().all(|value| value.norm() < 1.0e-6));
}

#[test]
fn continuum_subtract_rejects_bad_line_free_channels() {
    let dir = TempDir::new().expect("tempdir");
    let input_ms = create_apply_fixture_ms(dir.path(), true);
    let output_ms = dir.path().join("bad-contsub.ms");

    let error = continuum_subtract(&ContinuumSubtractionRequest {
        input_ms,
        output_ms,
        fit_spw: "0:99".to_string(),
        fit_order: 0,
        data_column: ContinuumSubtractionDataColumn::CorrectedData,
        selection: MsSelection::new(),
    })
    .expect_err("bad fitspw should fail");

    assert!(error.to_string().contains("line-free channel selection"));
    assert!(error.to_string().contains("exceeds spectral window"));
}
