// SPDX-License-Identifier: LGPL-3.0-or-later

//! Field-name selection reaches production imaging (#54/#980).

use casa_core::{ArrayValue, ScalarValue, Value};
use casa_ms::build_table_schema;
use casa_ms::{MeasurementSet, OptionalMainColumn};
use casa_test_support::{IMAGING_NRMS_TOLERANCE, image_nrms};
use ndarray::ArrayD;
use serde_json::json;

use super::fixture::{Component, Observation, PHASE_CENTER_RAD, Product, Setup};

#[test]
fn field_names_patterns_and_mixed_lists_image_the_same_rows_as_ids() {
    let mut setup = Setup::vla_q_band(2);
    setup.fields = vec![PHASE_CENTER_RAD; 2];
    let point = Component {
        offset_px: [12, 8],
        flux_jy: 1.0,
        fwhm_px: None,
        spectral_index: 0.0,
    };
    let observation = Observation::synthesise_setup(setup, &[("point", point)]);
    for (index, ids, names) in [(0, "0", "t1-0"), (1, "1", "t1-1*"), (2, "0,1", "0,t1-1")] {
        let reference = format!("fields-{index}-ids");
        let named = format!("fields-{index}-names");
        let controls = |field, selectdata| {
            json!({
                "field": field, "selectdata": selectdata, "niter": 0, "write_pb": true,
            })
        };
        let (_, a) = observation.image(&reference, controls(ids, true));
        let (_, b) = observation.image(&named, controls(names, false));
        assert_eq!(a.suffixes(), b.suffixes());
        for suffix in a.suffixes() {
            let nrms = image_nrms(
                &observation.scratch(&format!("{reference}{suffix}")),
                &observation.scratch(&format!("{named}{suffix}")),
            )
            .unwrap();
            assert!(nrms <= IMAGING_NRMS_TOLERANCE, "{names} {suffix}: {nrms:e}");
        }
    }
    let error = observation
        .try_image("missing-field", json!({"field":"NO_SUCH_FIELD","niter":0}))
        .err()
        .expect("an unknown field is refused");
    assert!(error.contains("did not match any FIELD rows"), "{error}");
    assert!(!observation.scratch("missing-field.psf").exists());
}

#[test]
fn field_partitions_keep_the_original_spectral_weight_probe() {
    let mut setup = Setup::vla_q_band(2);
    setup.fields = vec![PHASE_CENTER_RAD; 2];
    let point = Component {
        offset_px: [0, 0],
        flux_jy: 1.0,
        fwhm_px: None,
        spectral_index: 0.0,
    };
    let observation = Observation::synthesise_setup(setup, &[("point", point)]);
    let vis = observation.measurement_set();
    let mut ms = MeasurementSet::open(vis).unwrap();
    let schema = build_table_schema(&[
        *OptionalMainColumn::CorrectedData.column_def(),
        *OptionalMainColumn::WeightSpectrum.column_def(),
    ])
    .unwrap();
    for name in ["CORRECTED_DATA", "WEIGHT_SPECTRUM"] {
        ms.main_table_mut()
            .add_column(schema.column(name).unwrap().clone(), None)
            .unwrap();
    }
    let mut later_field = Vec::new();
    for row in 0..ms.row_count() {
        let source = ms.main_table().row_accessor();
        let data = source.array_cell(row, "DATA").unwrap().clone();
        let ScalarValue::Int32(field) = source.scalar_cell(row, "FIELD_ID").unwrap() else {
            panic!("FIELD_ID must be Int32");
        };
        // Make the earlier field sort after its sibling in partition order.
        let field = 1 - *field;
        let mut target = ms.main_table_mut().row_accessor_mut();
        target
            .set_cell(row, "CORRECTED_DATA", Value::Array(data))
            .unwrap();
        target
            .set_cell(row, "FIELD_ID", Value::Scalar(ScalarValue::Int32(field)))
            .unwrap();
        if field == 0 {
            later_field.push(row);
        }
    }
    ms.save().unwrap();
    drop(ms);
    let controls = json!({"field": "0,1", "datacolumn": "corrected", "niter": 0});
    observation.image("scalar", controls.clone());

    let mut ms = MeasurementSet::open(vis).unwrap();
    assert!(!later_field.is_empty() && later_field[0] > 0);
    for row in later_field {
        let shape = ms
            .main_table()
            .row_accessor()
            .array_cell(row, "DATA")
            .unwrap()
            .shape()
            .to_vec();
        let values = ArrayD::from_elem(shape, 51.0);
        ms.main_table_mut()
            .row_accessor_mut()
            .set_cell(
                row,
                "WEIGHT_SPECTRUM",
                Value::Array(ArrayValue::Float32(values)),
            )
            .unwrap();
    }
    ms.save().unwrap();
    drop(ms);
    observation.image("mixed", controls);

    let sumwt = |name: &str| Product::open(&observation.scratch(name)).pixels[[0, 0]];
    let reference = sumwt("scalar.sumwt");
    assert!(reference > 0.0);
    assert_eq!(sumwt("mixed.sumwt"), reference);
}
