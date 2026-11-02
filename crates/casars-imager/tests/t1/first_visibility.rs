// SPDX-License-Identifier: LGPL-3.0-or-later

//! The MFS header's rest frequency comes from the field of CASA's first
//! visibility buffer: `SynthesisImagerVi2` sorts by `ARRAY_ID`,
//! `DATA_DESC_ID`, `FIELD_ID` and `TIME`, and
//! `SynthesisParamsImage::buildCoordinateSystem` reads that buffer's field's
//! SOURCE line (#980, Astra's review of #1043).
//!
//! Two fields share a phase centre and a data description; field 0 names a
//! source with a 1.2 GHz line and field 1 one with 1.4 GHz. MAIN's rows put
//! the fields on opposite arrays, so the first buffer is the field on array
//! 0, whatever its number. The rows of both arrays share one partition (the
//! partition key has no array), so the survey reads their arrays.

use casa_coordinates::CoordinateModel;
use casa_images::PagedImage;
use casa_ms::{MeasurementSet, SubtableId};
use casa_tables::table_measures::{MeasureType, TableMeasDesc};
use casa_tables::{ColumnSchema, RowRange};
use casa_types::{ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value};
use serde_json::json;

use super::fixture::{Component, Observation, PHASE_CENTER_RAD, Setup};

const POINT: Component = Component {
    offset_px: [12, 8],
    flux_jy: 1.0,
    fwhm_px: None,
    spectral_index: 0.0,
};
/// Each field's source's line, by field.
const LINES_HZ: [f64; 2] = [1.2e9, 1.4e9];

/// A two-field observation whose field `f`'s rows carry `ARRAY_ID`
/// `arrays[f]` and whose field `f` names source `f` with line `LINES_HZ[f]`.
fn observation(arrays: [i32; 2]) -> Observation {
    let mut setup = Setup::vla_q_band(2);
    setup.fields = vec![PHASE_CENTER_RAD; 2];
    let observation = Observation::synthesise_setup(setup, &[("point", POINT)]);
    let mut ms = MeasurementSet::open(observation.measurement_set()).unwrap();
    for row in 0..ms.row_count() {
        let field = match ms
            .main_table()
            .cell_accessor(row, "FIELD_ID")
            .unwrap()
            .scalar()
            .unwrap()
        {
            ScalarValue::Int32(field) => *field,
            other => panic!("FIELD_ID is {other:?}"),
        };
        ms.main_table_mut()
            .row_accessor_mut()
            .set_cell(
                row,
                "ARRAY_ID",
                Value::Scalar(ScalarValue::Int32(arrays[field as usize])),
            )
            .unwrap();
    }
    for field in 0..2 {
        ms.field_mut()
            .unwrap()
            .table_mut()
            .fill_column_range(
                "SOURCE_ID",
                RowRange::new(field, field + 1),
                Value::Scalar(ScalarValue::Int32(field as i32)),
            )
            .unwrap();
    }
    let source = ms.add_optional_subtable(SubtableId::Source).unwrap();
    source
        .add_column(
            ColumnSchema::array_variable("REST_FREQUENCY", PrimitiveType::Float64, Some(1)),
            None,
        )
        .unwrap();
    TableMeasDesc::new_fixed("REST_FREQUENCY", MeasureType::Frequency, "LSRK")
        .write(source)
        .unwrap();
    let floats = |values: &[f64]| {
        Value::Array(ArrayValue::Float64(
            ndarray::ArrayD::from_shape_vec(vec![values.len()], values.to_vec()).unwrap(),
        ))
    };
    let int = |value: i32| Value::Scalar(ScalarValue::Int32(value));
    for (field, line_hz) in LINES_HZ.into_iter().enumerate() {
        source
            .add_row(RecordValue::new(vec![
                RecordField::new("CALIBRATION_GROUP", int(-1)),
                RecordField::new("CODE", Value::Scalar(ScalarValue::String(String::new()))),
                RecordField::new("DIRECTION", floats(&PHASE_CENTER_RAD)),
                RecordField::new("INTERVAL", Value::Scalar(ScalarValue::Float64(0.0))),
                RecordField::new(
                    "NAME",
                    Value::Scalar(ScalarValue::String(format!("source{field}"))),
                ),
                RecordField::new("NUM_LINES", int(1)),
                RecordField::new("PROPER_MOTION", floats(&[0.0, 0.0])),
                RecordField::new("SOURCE_ID", int(field as i32)),
                RecordField::new("SPECTRAL_WINDOW_ID", int(0)),
                RecordField::new("TIME", Value::Scalar(ScalarValue::Float64(0.0))),
                RecordField::new("REST_FREQUENCY", floats(&[line_hz])),
            ]))
            .unwrap();
    }
    ms.save().unwrap();
    drop(ms);
    observation
}

/// The rest frequency an MFS image of `observation` records.
fn header_rest_frequency_hz(observation: &Observation, name: &str) -> f64 {
    observation.image(name, json!({"specmode": "mfs", "niter": 0}));
    let image = PagedImage::<f32>::open(observation.scratch(&format!("{name}.psf"))).unwrap();
    let CoordinateModel::Spectral(spectral) = image.coordinates().coordinate(2) else {
        panic!("the MFS image has no spectral coordinate");
    };
    spectral.rest_frequency()
}

#[test]
fn the_mfs_rest_frequency_follows_casas_array_first_buffer_order() {
    // Field 1 on array 0 comes first: CASA records its 1.4 GHz line.
    let opposing = observation([1, 0]);
    assert_eq!(header_rest_frequency_hz(&opposing, "opposing"), LINES_HZ[1]);
    // With the arrays following the fields, field 0 comes first.
    let following = observation([0, 1]);
    assert_eq!(
        header_rest_frequency_hz(&following, "following"),
        LINES_HZ[0]
    );
}
