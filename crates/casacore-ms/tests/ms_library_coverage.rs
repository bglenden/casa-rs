// SPDX-License-Identifier: LGPL-3.0-or-later

use std::fs::File;
use std::path::{Path, PathBuf};

use casacore_ms::column_def::{ColumnDef, ColumnKind, build_table_schema};
use casacore_ms::columns::data_columns::FloatDataColumn;
use casacore_ms::columns::direction_columns::DirectionColumn;
use casacore_ms::columns::exposure_interval::{ExposureColumn, IntervalColumn};
use casacore_ms::columns::frequency_columns::ChanFreqColumn;
use casacore_ms::columns::main_ids;
use casacore_ms::columns::position_columns::AntennaPositionColumn;
use casacore_ms::columns::time_columns::TimeColumn;
use casacore_ms::columns::uvw_column::UvwColumn;
use casacore_ms::{
    ListObsOptions, ListObsOutputFormat, ListObsSummary, MeasurementSet, MeasurementSetBuilder,
    MsError, OptionalMainColumn, SubTable, SubtableId, VisibilityDataColumn,
};
use casacore_tables::{ColumnSchema, Table, TableSchema};
use casacore_types::measures::{EpochRef, PositionRef};
use casacore_types::{
    ArrayD, ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
};
use flate2::read::GzDecoder;
use tar::Archive;
use tempfile::tempdir;

#[test]
fn measurement_set_accessors_cover_real_fixture_columns_and_subtables() {
    let (_temp, ms_path) = unpack_fixture_ms("mssel_test_small.ms.tgz");
    let ms = MeasurementSet::open(&ms_path).expect("open real fixture MS");

    assert_eq!(ms.path(), Some(ms_path.as_path()));
    assert!(ms.ms_version().is_some());
    assert!(ms.row_count() > 1_000);
    assert!(ms.subtable_ids().len() >= 12);
    assert!(ms.main_table().row_count() > 0);

    let antenna = ms.antenna().expect("ANTENNA accessor");
    assert!(antenna.row_count() > 0);
    assert!(!antenna.name(0).unwrap().is_empty());
    assert!(!antenna.station(0).unwrap().is_empty());
    assert!(!antenna.antenna_type(0).unwrap().is_empty());
    assert!(!antenna.mount(0).unwrap().is_empty());
    assert!(antenna.position(0).unwrap().iter().all(|v| v.is_finite()));
    assert!(antenna.offset(0).unwrap().iter().all(|v| v.is_finite()));
    assert!(antenna.dish_diameter(0).unwrap() > 0.0);
    let ant_pos = AntennaPositionColumn::new(antenna.table())
        .get_position(0)
        .expect("antenna position measure");
    assert_eq!(ant_pos.refer(), PositionRef::ITRF);
    assert!(ant_pos.values().iter().all(|v| v.is_finite()));

    let field = ms.field().expect("FIELD accessor");
    assert!(field.row_count() >= 2);
    assert!(!field.name(0).unwrap().is_empty());
    assert!(field.num_poly(0).unwrap() >= 0);
    assert_eq!(field.delay_dir(0).unwrap().shape()[0], 2);
    assert_eq!(field.phase_dir(0).unwrap().shape()[0], 2);
    assert_eq!(field.reference_dir(0).unwrap().shape()[0], 2);
    let phase_dir = DirectionColumn::phase_dir(field.table())
        .get_direction(0)
        .expect("phase direction");
    assert_eq!(
        phase_dir.refer(),
        casacore_types::measures::direction::DirectionRef::J2000
    );
    let (lon, lat) = phase_dir.as_angles();
    assert!(lon.is_finite());
    assert!(lat.is_finite());

    let spw = ms.spectral_window().expect("SPECTRAL_WINDOW accessor");
    assert!(spw.row_count() >= 2);
    let num_chan = spw.num_chan(0).unwrap() as usize;
    assert!(num_chan > 0);
    assert_eq!(spw.chan_freq(0).unwrap().len(), num_chan);
    assert_eq!(spw.chan_width(0).unwrap().len(), num_chan);
    assert_eq!(spw.effective_bw(0).unwrap().len(), num_chan);
    assert_eq!(spw.resolution(0).unwrap().len(), num_chan);
    assert!(spw.ref_frequency(0).unwrap() > 0.0);
    assert!(spw.total_bandwidth(0).unwrap() > 0.0);
    assert!(!spw.name(0).unwrap().is_empty());
    let _freq_group_name = spw.freq_group_name(0).unwrap();
    let chan_freqs = ChanFreqColumn::new(spw.table())
        .get_frequencies(0)
        .expect("channel frequencies");
    assert_eq!(chan_freqs.len(), num_chan);
    assert!(chan_freqs[0].hz() > 0.0);
    assert!(
        ChanFreqColumn::new(spw.table())
            .get_ref_frequency(0)
            .unwrap()
            .hz()
            > 0.0
    );

    let pol = ms.polarization().expect("POLARIZATION accessor");
    assert!(pol.row_count() > 0);
    let num_corr = pol.num_corr(0).unwrap() as usize;
    assert!(num_corr > 0);
    assert_eq!(pol.corr_type(0).unwrap().len(), num_corr);
    assert_eq!(pol.corr_product(0).unwrap().shape()[0], 2);

    let dd = ms.data_description().expect("DATA_DESCRIPTION accessor");
    assert!(dd.row_count() > 0);
    let spw_id = dd.spectral_window_id(0).unwrap();
    let pol_id = dd.polarization_id(0).unwrap();
    assert!(spw_id >= 0 && spw_id < spw.row_count() as i32);
    assert!(pol_id >= 0 && pol_id < pol.row_count() as i32);

    let time = TimeColumn::new(ms.main_table());
    assert_eq!(time.get_epoch(0).unwrap().refer(), EpochRef::UTC);
    assert!(time.get_mjd_seconds(0).unwrap() > 0.0);
    assert!(
        TimeColumn::centroid(ms.main_table())
            .get_mjd_seconds(0)
            .unwrap()
            > 0.0
    );
    assert!(ExposureColumn::new(ms.main_table()).get(0).unwrap() > 0.0);
    assert!(IntervalColumn::new(ms.main_table()).get(0).unwrap() > 0.0);
    assert!(
        UvwColumn::new(ms.main_table())
            .get(0)
            .unwrap()
            .iter()
            .all(|v| v.is_finite())
    );

    let data = ms
        .data_column(VisibilityDataColumn::Data)
        .expect("DATA column present");
    let data_shape = data.shape(0).unwrap();
    assert_eq!(data_shape.len(), 2);
    assert_eq!(data.column_name(), "DATA");
    assert_eq!(ms.flag_column().shape(0).unwrap(), data_shape);
    assert_eq!(ms.flag_column().get(0).unwrap().shape().len(), 2);
    let weight_column = ms.weight_column();
    let weight = weight_column.get(0).unwrap();
    let sigma_column = ms.sigma_column();
    let sigma = sigma_column.get(0).unwrap();
    assert_eq!(weight.shape().len(), 1);
    assert_eq!(sigma.shape().len(), 1);
    let _flag_row = ms.flag_row_column().get(0).unwrap();
    assert!(matches!(
        FloatDataColumn::new(ms.main_table()),
        Err(MsError::ColumnNotPresent(name)) if name == "FLOAT_DATA"
    ));

    let antenna1 = main_ids::antenna1(ms.main_table()).get(0).unwrap();
    let antenna2 = main_ids::antenna2(ms.main_table()).get(0).unwrap();
    let field_id = main_ids::field_id(ms.main_table()).get(0).unwrap();
    let data_desc_id = main_ids::data_desc_id(ms.main_table()).get(0).unwrap();
    let _scan_number = main_ids::scan_number(ms.main_table()).get(0).unwrap();
    let _array_id = main_ids::array_id(ms.main_table()).get(0).unwrap();
    let _observation_id = main_ids::observation_id(ms.main_table()).get(0).unwrap();
    let _processor_id = main_ids::processor_id(ms.main_table()).get(0).unwrap();
    let _state_id = main_ids::state_id(ms.main_table()).get(0).unwrap();
    let _feed1 = main_ids::feed1(ms.main_table()).get(0).unwrap();
    let _feed2 = main_ids::feed2(ms.main_table()).get(0).unwrap();
    assert!(antenna1 >= 0 && antenna1 < antenna.row_count() as i32);
    assert!(antenna2 >= 0 && antenna2 < antenna.row_count() as i32);
    assert!(field_id >= 0 && field_id < field.row_count() as i32);
    assert!(data_desc_id >= 0 && data_desc_id < dd.row_count() as i32);
}

#[test]
fn generic_subtable_wrappers_cover_common_read_write_paths() {
    let mut builder = MeasurementSetBuilder::new()
        .with_main_column(OptionalMainColumn::Data)
        .with_main_column(OptionalMainColumn::CorrectedData)
        .with_main_column(OptionalMainColumn::ModelData);
    for id in SubtableId::ALL_OPTIONAL {
        builder = builder.with_optional_subtable(*id);
    }
    let mut ms = MeasurementSet::create_memory(builder).expect("create memory MS");

    assert!(ms.main_table().schema().unwrap().contains_column("DATA"));
    assert!(
        ms.main_table()
            .schema()
            .unwrap()
            .contains_column("CORRECTED_DATA")
    );
    assert!(
        ms.main_table()
            .schema()
            .unwrap()
            .contains_column("MODEL_DATA")
    );
    assert_eq!(ms.path(), None);
    assert_eq!(ms.subtable_ids().len(), 17);
    assert!(ms.subtable(SubtableId::Antenna).is_some());
    assert!(ms.subtable_mut(SubtableId::Antenna).is_some());

    ms.subtable_mut(SubtableId::Observation)
        .unwrap()
        .add_row(RecordValue::new(vec![
            RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            RecordField::new(
                "LOG",
                Value::Array(ArrayValue::String(
                    ArrayD::from_shape_vec(vec![1], vec!["log".to_string()]).unwrap(),
                )),
            ),
            RecordField::new(
                "OBSERVER",
                Value::Scalar(ScalarValue::String("observer".to_string())),
            ),
            RecordField::new(
                "PROJECT",
                Value::Scalar(ScalarValue::String("project".to_string())),
            ),
            RecordField::new("RELEASE_DATE", Value::Scalar(ScalarValue::Float64(2.0))),
            RecordField::new(
                "SCHEDULE",
                Value::Array(ArrayValue::String(
                    ArrayD::from_shape_vec(vec![1], vec!["sched".to_string()]).unwrap(),
                )),
            ),
            RecordField::new(
                "SCHEDULE_TYPE",
                Value::Scalar(ScalarValue::String("standard".to_string())),
            ),
            RecordField::new(
                "TELESCOPE_NAME",
                Value::Scalar(ScalarValue::String("VLA".to_string())),
            ),
            RecordField::new(
                "TIME_RANGE",
                Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(vec![2], vec![1.0, 2.0]).unwrap(),
                )),
            ),
        ]))
        .unwrap();
    ms.subtable_mut(SubtableId::DataDescription)
        .unwrap()
        .add_row(RecordValue::new(vec![
            RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            RecordField::new("POLARIZATION_ID", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new("SPECTRAL_WINDOW_ID", Value::Scalar(ScalarValue::Int32(1))),
        ]))
        .unwrap();

    {
        let obs = ms.observation().unwrap();
        assert_eq!(obs.row_count(), 1);
        assert_eq!(obs.string(0, "TELESCOPE_NAME").unwrap(), "VLA");
        assert_eq!(obs.f64(0, "RELEASE_DATE").unwrap(), 2.0);
        assert!(!obs.bool(0, "FLAG_ROW").unwrap());
        assert_eq!(obs.array(0, "TIME_RANGE").unwrap().shape(), &[2]);
        assert_eq!(obs.optional_string(0, "MISSING").unwrap(), None);
        assert_eq!(obs.optional_array(0, "MISSING").unwrap(), None);
        assert_eq!(
            <casacore_ms::MsObservation<'_> as SubTable>::id(),
            SubtableId::Observation
        );
        assert!(!obs.table().keywords().fields().is_empty() || obs.table().row_count() == 1);
    }

    {
        let mut obs = ms.observation_mut().unwrap();
        assert_eq!(obs.row_count(), 1);
        assert_eq!(obs.as_ref().string(0, "TELESCOPE_NAME").unwrap(), "VLA");
        obs.set_string(0, "TELESCOPE_NAME", "ALMA").unwrap();
        obs.set_f64(0, "RELEASE_DATE", 3.0).unwrap();
        obs.set_bool(0, "FLAG_ROW", true).unwrap();
        obs.set_array(
            0,
            "TIME_RANGE",
            ArrayValue::Float64(ArrayD::from_shape_vec(vec![2], vec![5.0, 9.0]).unwrap()),
        )
        .unwrap();
        assert_eq!(obs.as_ref().string(0, "TELESCOPE_NAME").unwrap(), "ALMA");
        assert_eq!(obs.as_ref().f64(0, "RELEASE_DATE").unwrap(), 3.0);
        assert!(obs.as_ref().bool(0, "FLAG_ROW").unwrap());
    }

    {
        let dd = ms.data_description().unwrap();
        assert_eq!(dd.row_count(), 1);
        assert_eq!(dd.spectral_window_id(0).unwrap(), 1);
        assert_eq!(dd.polarization_id(0).unwrap(), 0);
        assert!(!dd.flag_row(0).unwrap());
        assert!(
            <casacore_ms::MsDataDescription<'_> as SubTable>::required_columns()
                .iter()
                .any(|col| col.name == "SPECTRAL_WINDOW_ID")
        );
    }

    {
        let mut dd = ms.data_description_mut().unwrap();
        assert_eq!(dd.row_count(), 1);
        dd.set_i32(0, "SPECTRAL_WINDOW_ID", 7).unwrap();
        dd.set_bool(0, "FLAG_ROW", true).unwrap();
        assert_eq!(dd.as_ref().spectral_window_id(0).unwrap(), 7);
        assert!(dd.as_ref().flag_row(0).unwrap());
    }

    assert_eq!(
        ms.data_column(VisibilityDataColumn::Data)
            .unwrap()
            .column_name(),
        "DATA"
    );
    assert_eq!(
        ms.data_column(VisibilityDataColumn::CorrectedData)
            .unwrap()
            .column_name(),
        "CORRECTED_DATA"
    );
    assert_eq!(
        ms.data_column(VisibilityDataColumn::ModelData)
            .unwrap()
            .column_name(),
        "MODEL_DATA"
    );
    assert_eq!(
        ms.data_column_mut(VisibilityDataColumn::Data)
            .unwrap()
            .column_name(),
        "DATA"
    );

    assert_eq!(ms.feed().unwrap().row_count(), 0);
    assert_eq!(ms.flag_cmd().unwrap().row_count(), 0);
    assert_eq!(ms.history().unwrap().row_count(), 0);
    assert_eq!(ms.pointing().unwrap().row_count(), 0);
    assert_eq!(ms.processor().unwrap().row_count(), 0);
    assert_eq!(ms.state().unwrap().row_count(), 0);
    assert_eq!(ms.doppler().unwrap().row_count(), 0);
    assert_eq!(ms.freq_offset().unwrap().row_count(), 0);
    assert_eq!(ms.source().unwrap().row_count(), 0);
    assert_eq!(ms.syscal().unwrap().row_count(), 0);
    assert_eq!(ms.weather().unwrap().row_count(), 0);
}

#[test]
fn listobs_library_summary_renders_real_fixture_with_selection() {
    let (_temp, ms_path) = unpack_fixture_ms("mssel_test_small_multifield_spw.ms.tgz");

    let options = ListObsOptions {
        verbose: false,
        field: Some("NGC4826-F3".to_string()),
        spw: Some("5".to_string()),
        listunfl: true,
        ..ListObsOptions::default()
    };
    assert!(options.has_selection());

    let summary = ListObsSummary::from_path_with_options(&ms_path, &options)
        .expect("listobs summary from path with options");
    assert_eq!(summary.schema_version, 1);
    assert_eq!(summary.options, options);
    assert_eq!(summary.fields.len(), 1);
    assert_eq!(summary.fields[0].name, "NGC4826-F3");
    assert_eq!(summary.spectral_windows.len(), 1);
    assert_eq!(summary.spectral_windows[0].spectral_window_id, 5);
    assert!(summary.measurement_set.row_count > 0);

    let json = summary
        .render(ListObsOutputFormat::Json)
        .expect("render json");
    let parsed: serde_json::Value = serde_json::from_str(&json).expect("parse summary json");
    assert_eq!(parsed["schema_version"], 1);
    assert_eq!(parsed["fields"][0]["name"], "NGC4826-F3");
    assert_eq!(parsed["spectral_windows"][0]["spectral_window_id"], 5);

    let text = summary
        .render(ListObsOutputFormat::Text)
        .expect("render text");
    assert!(text.contains("Fields:"));
    assert!(text.contains("Spectral Windows:"));
    assert!(text.contains("nUnflRows"));
    assert!(!text.contains("ObservationID ="));

    let default_summary = MeasurementSet::open(&ms_path)
        .expect("open MS")
        .listobs_summary()
        .expect("default summary");
    assert!(default_summary.fields.len() >= summary.fields.len());
    assert!(default_summary.measurement_set.row_count >= summary.measurement_set.row_count);
    assert!(
        default_summary
            .render_json_pretty()
            .expect("pretty json")
            .contains("\"schema_version\": 1")
    );
}

#[test]
fn wrapper_mutators_and_generic_type_mismatch_paths_are_exercised() {
    let observation_schema =
        build_table_schema(casacore_ms::schema::observation::REQUIRED_COLUMNS).unwrap();
    let mut observation_table = Table::with_schema(observation_schema);
    observation_table
        .add_row(record_for_defs(
            casacore_ms::schema::observation::REQUIRED_COLUMNS,
            &[
                ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
                (
                    "LOG",
                    Value::Array(ArrayValue::String(
                        ArrayD::from_shape_vec(vec![1], vec!["log".to_string()]).unwrap(),
                    )),
                ),
                (
                    "OBSERVER",
                    Value::Scalar(ScalarValue::String("observer".to_string())),
                ),
                (
                    "PROJECT",
                    Value::Scalar(ScalarValue::String("project".to_string())),
                ),
                ("RELEASE_DATE", Value::Scalar(ScalarValue::Float64(2.0))),
                (
                    "SCHEDULE",
                    Value::Array(ArrayValue::String(
                        ArrayD::from_shape_vec(vec![1], vec!["sched".to_string()]).unwrap(),
                    )),
                ),
                (
                    "SCHEDULE_TYPE",
                    Value::Scalar(ScalarValue::String("standard".to_string())),
                ),
                (
                    "TELESCOPE_NAME",
                    Value::Scalar(ScalarValue::String("VLA".to_string())),
                ),
                (
                    "TIME_RANGE",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![2], vec![1.0, 2.0]).unwrap(),
                    )),
                ),
            ],
        ))
        .unwrap();
    let observation = casacore_ms::MsObservation::new(&observation_table);
    assert!(matches!(
        observation.i32(0, "TELESCOPE_NAME"),
        Err(MsError::ColumnTypeMismatch { expected, .. }) if expected == "Int32"
    ));
    assert!(matches!(
        observation.f64(0, "TELESCOPE_NAME"),
        Err(MsError::ColumnTypeMismatch { expected, .. }) if expected == "Float64"
    ));
    assert!(matches!(
        observation.bool(0, "TELESCOPE_NAME"),
        Err(MsError::ColumnTypeMismatch { expected, .. }) if expected == "Bool"
    ));
    assert!(matches!(
        observation.string(0, "FLAG_ROW"),
        Err(MsError::ColumnTypeMismatch { expected, .. }) if expected == "String"
    ));
    assert_eq!(observation.optional_f64(0, "MISSING").unwrap(), None);
    assert_eq!(observation.optional_bool(0, "MISSING").unwrap(), None);
    assert_eq!(observation.optional_array(0, "MISSING").unwrap(), None);

    let field_schema = build_table_schema(casacore_ms::schema::field::REQUIRED_COLUMNS).unwrap();
    let mut field_table = Table::with_schema(field_schema);
    field_table
        .add_row(record_for_defs(
            casacore_ms::schema::field::REQUIRED_COLUMNS,
            &[
                ("CODE", Value::Scalar(ScalarValue::String("T".to_string()))),
                (
                    "DELAY_DIR",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![2, 1], vec![1.0, 0.5]).unwrap(),
                    )),
                ),
                ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
                (
                    "NAME",
                    Value::Scalar(ScalarValue::String("old".to_string())),
                ),
                ("NUM_POLY", Value::Scalar(ScalarValue::Int32(0))),
                (
                    "PHASE_DIR",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![2, 1], vec![1.0, 0.5]).unwrap(),
                    )),
                ),
                (
                    "REFERENCE_DIR",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![2, 1], vec![1.0, 0.5]).unwrap(),
                    )),
                ),
                ("SOURCE_ID", Value::Scalar(ScalarValue::Int32(0))),
                ("TIME", Value::Scalar(ScalarValue::Float64(10.0))),
            ],
        ))
        .unwrap();
    {
        let mut field = casacore_ms::MsFieldMut::new(&mut field_table);
        field.set_string(0, "NAME", "new").unwrap();
        field.set_string(0, "CODE", "C").unwrap();
        field.set_i32(0, "SOURCE_ID", 7).unwrap();
        field.set_i32(0, "NUM_POLY", 1).unwrap();
        field.set_f64(0, "TIME", 25.0).unwrap();
        field.set_bool(0, "FLAG_ROW", true).unwrap();
        field
            .set_array(
                0,
                "PHASE_DIR",
                ArrayValue::Float64(
                    ArrayD::from_shape_vec(vec![2, 2], vec![1.0, 0.1, 0.5, 0.05]).unwrap(),
                ),
            )
            .unwrap();
        assert_eq!(field.as_ref().name(0).unwrap(), "new");
        assert_eq!(field.as_ref().code(0).unwrap(), "C");
        assert_eq!(field.as_ref().source_id(0).unwrap(), 7);
        assert_eq!(field.as_ref().num_poly(0).unwrap(), 1);
        assert_eq!(field.as_ref().time(0).unwrap(), 25.0);
        assert!(field.as_ref().flag_row(0).unwrap());
    }

    let pol_schema =
        build_table_schema(casacore_ms::schema::polarization::REQUIRED_COLUMNS).unwrap();
    let mut pol_table = Table::with_schema(pol_schema);
    pol_table
        .add_row(record_for_defs(
            casacore_ms::schema::polarization::REQUIRED_COLUMNS,
            &[
                (
                    "CORR_PRODUCT",
                    Value::Array(ArrayValue::Int32(
                        ArrayD::from_shape_vec(vec![2, 2], vec![0, 1, 0, 1]).unwrap(),
                    )),
                ),
                (
                    "CORR_TYPE",
                    Value::Array(ArrayValue::Int32(
                        ArrayD::from_shape_vec(vec![2], vec![9, 12]).unwrap(),
                    )),
                ),
                ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
                ("NUM_CORR", Value::Scalar(ScalarValue::Int32(2))),
            ],
        ))
        .unwrap();
    {
        let mut pol = casacore_ms::MsPolarizationMut::new(&mut pol_table);
        pol.set_i32(0, "NUM_CORR", 4).unwrap();
        pol.set_bool(0, "FLAG_ROW", true).unwrap();
        pol.set_array(
            0,
            "CORR_TYPE",
            ArrayValue::Int32(ArrayD::from_shape_vec(vec![4], vec![9, 10, 11, 12]).unwrap()),
        )
        .unwrap();
        pol.set_array(
            0,
            "CORR_PRODUCT",
            ArrayValue::Int32(
                ArrayD::from_shape_vec(vec![2, 4], vec![0, 0, 1, 1, 0, 1, 0, 1]).unwrap(),
            ),
        )
        .unwrap();
        assert_eq!(pol.as_ref().num_corr(0).unwrap(), 4);
        assert_eq!(pol.as_ref().corr_type(0).unwrap(), vec![9, 10, 11, 12]);
        assert!(pol.as_ref().flag_row(0).unwrap());
    }

    let spw_schema =
        build_table_schema(casacore_ms::schema::spectral_window::REQUIRED_COLUMNS).unwrap();
    let mut spw_table = Table::with_schema(spw_schema);
    spw_table
        .add_row(record_for_defs(
            casacore_ms::schema::spectral_window::REQUIRED_COLUMNS,
            &[
                (
                    "CHAN_FREQ",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![2], vec![1.0e9, 1.1e9]).unwrap(),
                    )),
                ),
                (
                    "CHAN_WIDTH",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![2], vec![1.0e6, 1.0e6]).unwrap(),
                    )),
                ),
                (
                    "EFFECTIVE_BW",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![2], vec![1.0e6, 1.0e6]).unwrap(),
                    )),
                ),
                ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
                ("FREQ_GROUP", Value::Scalar(ScalarValue::Int32(0))),
                (
                    "FREQ_GROUP_NAME",
                    Value::Scalar(ScalarValue::String("group".to_string())),
                ),
                ("IF_CONV_CHAIN", Value::Scalar(ScalarValue::Int32(0))),
                ("MEAS_FREQ_REF", Value::Scalar(ScalarValue::Int32(5))),
                (
                    "NAME",
                    Value::Scalar(ScalarValue::String("spw0".to_string())),
                ),
                ("NET_SIDEBAND", Value::Scalar(ScalarValue::Int32(1))),
                ("NUM_CHAN", Value::Scalar(ScalarValue::Int32(2))),
                ("REF_FREQUENCY", Value::Scalar(ScalarValue::Float64(1.0e9))),
                (
                    "RESOLUTION",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![2], vec![1.0e6, 1.0e6]).unwrap(),
                    )),
                ),
                (
                    "TOTAL_BANDWIDTH",
                    Value::Scalar(ScalarValue::Float64(2.0e6)),
                ),
            ],
        ))
        .unwrap();
    {
        let mut spw = casacore_ms::MsSpectralWindowMut::new(&mut spw_table);
        spw.set_i32(0, "NUM_CHAN", 3).unwrap();
        spw.set_i32(0, "NET_SIDEBAND", -1).unwrap();
        spw.set_i32(0, "FREQ_GROUP", 2).unwrap();
        spw.set_i32(0, "IF_CONV_CHAIN", 4).unwrap();
        spw.set_f64(0, "REF_FREQUENCY", 1.5e9).unwrap();
        spw.set_f64(0, "TOTAL_BANDWIDTH", 3.0e6).unwrap();
        spw.set_bool(0, "FLAG_ROW", true).unwrap();
        spw.set_string(0, "NAME", "spw-updated").unwrap();
        spw.set_string(0, "FREQ_GROUP_NAME", "science").unwrap();
        spw.set_array(
            0,
            "CHAN_FREQ",
            ArrayValue::Float64(
                ArrayD::from_shape_vec(vec![3], vec![1.5e9, 1.5001e9, 1.5002e9]).unwrap(),
            ),
        )
        .unwrap();
        spw.set_array(
            0,
            "CHAN_WIDTH",
            ArrayValue::Float64(ArrayD::from_shape_vec(vec![3], vec![1.0; 3]).unwrap()),
        )
        .unwrap();
        spw.set_array(
            0,
            "EFFECTIVE_BW",
            ArrayValue::Float64(ArrayD::from_shape_vec(vec![3], vec![1.0; 3]).unwrap()),
        )
        .unwrap();
        spw.set_array(
            0,
            "RESOLUTION",
            ArrayValue::Float64(ArrayD::from_shape_vec(vec![3], vec![1.0; 3]).unwrap()),
        )
        .unwrap();
        assert_eq!(spw.as_ref().num_chan(0).unwrap(), 3);
        assert_eq!(spw.as_ref().net_sideband(0).unwrap(), -1);
        assert_eq!(spw.as_ref().freq_group(0).unwrap(), 2);
        assert_eq!(spw.as_ref().freq_group_name(0).unwrap(), "science");
        assert_eq!(spw.as_ref().if_conv_chain(0).unwrap(), 4);
        assert_eq!(spw.as_ref().name(0).unwrap(), "spw-updated");
        assert!(spw.as_ref().flag_row(0).unwrap());
        assert_eq!(spw.as_ref().chan_freq(0).unwrap().len(), 3);
    }

    let antenna_schema =
        build_table_schema(casacore_ms::schema::antenna::REQUIRED_COLUMNS).unwrap();
    let mut antenna_table = Table::with_schema(antenna_schema);
    {
        let mut antenna = casacore_ms::MsAntennaMut::new(&mut antenna_table);
        antenna
            .add_antenna(
                "ANT1",
                "PAD1",
                "GROUND-BASED",
                "ALT-AZ",
                [1.0, 2.0, 3.0],
                [0.0, 0.0, 0.0],
                12.0,
            )
            .unwrap();
        antenna.put_name(0, "ANT2").unwrap();
        antenna.put_station(0, "PAD2").unwrap();
        antenna.put_type(0, "SPACE-BASED").unwrap();
        antenna.put_mount(0, "EQUATORIAL").unwrap();
        antenna.put_position(0, [4.0, 5.0, 6.0]).unwrap();
        antenna.put_offset(0, [0.1, 0.2, 0.3]).unwrap();
        antenna.put_dish_diameter(0, 7.5).unwrap();
        antenna.put_flag_row(0, true).unwrap();
    }
    let antenna = casacore_ms::MsAntenna::new(&antenna_table);
    assert_eq!(antenna.name(0).unwrap(), "ANT2");
    assert_eq!(antenna.station(0).unwrap(), "PAD2");
    assert_eq!(antenna.antenna_type(0).unwrap(), "SPACE-BASED");
    assert_eq!(antenna.mount(0).unwrap(), "EQUATORIAL");
    assert_eq!(antenna.position(0).unwrap(), [4.0, 5.0, 6.0]);
    assert_eq!(antenna.offset(0).unwrap(), [0.1, 0.2, 0.3]);
    assert_eq!(antenna.dish_diameter(0).unwrap(), 7.5);
    assert!(antenna.flag_row(0).unwrap());
    assert_eq!(antenna.mean_orbit(0).unwrap(), None);
    assert_eq!(antenna.orbit_id(0).unwrap(), None);
    assert_eq!(antenna.phased_array_id(0).unwrap(), None);

    let antenna_defs: Vec<ColumnDef> = casacore_ms::schema::antenna::REQUIRED_COLUMNS
        .iter()
        .chain(casacore_ms::schema::antenna::OPTIONAL_COLUMNS.iter())
        .copied()
        .collect();
    let antenna_schema = build_table_schema(&antenna_defs).unwrap();
    let mut optional_antenna_table = Table::with_schema(antenna_schema);
    optional_antenna_table
        .add_row(record_for_defs(
            &antenna_defs,
            &[
                (
                    "NAME",
                    Value::Scalar(ScalarValue::String("ANT3".to_string())),
                ),
                (
                    "STATION",
                    Value::Scalar(ScalarValue::String("PAD3".to_string())),
                ),
                (
                    "TYPE",
                    Value::Scalar(ScalarValue::String("GROUND-BASED".to_string())),
                ),
                (
                    "MOUNT",
                    Value::Scalar(ScalarValue::String("ALT-AZ".to_string())),
                ),
                (
                    "POSITION",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![3], vec![1.0, 2.0, 3.0]).unwrap(),
                    )),
                ),
                (
                    "OFFSET",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![3], vec![0.0, 0.0, 0.0]).unwrap(),
                    )),
                ),
                ("DISH_DIAMETER", Value::Scalar(ScalarValue::Float64(12.0))),
                (
                    "MEAN_ORBIT",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![6], vec![0.0; 6]).unwrap(),
                    )),
                ),
                ("ORBIT_ID", Value::Scalar(ScalarValue::Int32(5))),
                ("PHASED_ARRAY_ID", Value::Scalar(ScalarValue::Int32(9))),
            ],
        ))
        .unwrap();
    let optional_antenna = casacore_ms::MsAntenna::new(&optional_antenna_table);
    assert_eq!(
        optional_antenna.mean_orbit(0).unwrap().unwrap().shape(),
        &[6]
    );
    assert_eq!(optional_antenna.orbit_id(0).unwrap(), Some(5));
    assert_eq!(optional_antenna.phased_array_id(0).unwrap(), Some(9));
}

#[test]
fn validation_and_measure_columns_cover_error_paths() {
    let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();
    ms.main_table_mut().set_column_keywords(
        "TIME",
        RecordValue::new(vec![
            RecordField::new(
                "QuantumUnits",
                Value::Array(ArrayValue::String(
                    ArrayD::from_shape_vec(vec![1], vec!["ms".to_string()]).unwrap(),
                )),
            ),
            RecordField::new(
                "MEASINFO",
                Value::Record(RecordValue::new(vec![RecordField::new(
                    "type",
                    Value::Scalar(ScalarValue::String("DIRECTION".to_string())),
                )])),
            ),
        ]),
    );
    let issues = ms.validate().unwrap();
    assert!(issues.iter().any(|issue| matches!(
        issue,
        casacore_ms::validate::ValidationIssue::WrongQuantumUnits { column_name, .. }
            if column_name == "TIME"
    )));
    assert!(issues.iter().any(|issue| matches!(
        issue,
        casacore_ms::validate::ValidationIssue::WrongMeasureType { column_name, .. }
            if column_name == "TIME"
    )));
    assert!(
        issues
            .iter()
            .map(ToString::to_string)
            .any(|msg| msg.contains("QuantumUnits"))
    );

    let main_schema = TableSchema::new(vec![ColumnSchema::array_variable(
        "UVW",
        PrimitiveType::Float64,
        Some(1),
    )])
    .unwrap();
    let mut main_table = Table::with_schema(main_schema);
    main_table
        .add_row(RecordValue::new(vec![RecordField::new(
            "UVW",
            Value::Array(ArrayValue::Float64(
                ArrayD::from_shape_vec(vec![2], vec![1.0, 2.0]).unwrap(),
            )),
        )]))
        .unwrap();
    assert!(matches!(
        UvwColumn::new(&main_table).get(0),
        Err(MsError::ColumnTypeMismatch { expected, .. }) if expected == "f64[3]"
    ));

    let antenna_schema = TableSchema::new(vec![ColumnSchema::array_variable(
        "POSITION",
        PrimitiveType::Float64,
        Some(1),
    )])
    .unwrap();
    let mut antenna_table = Table::with_schema(antenna_schema);
    antenna_table
        .add_row(RecordValue::new(vec![RecordField::new(
            "POSITION",
            Value::Array(ArrayValue::Float64(
                ArrayD::from_shape_vec(vec![2], vec![1.0, 2.0]).unwrap(),
            )),
        )]))
        .unwrap();
    assert!(matches!(
        AntennaPositionColumn::new(&antenna_table).get_position(0),
        Err(MsError::ColumnTypeMismatch { expected, .. }) if expected == "f64[3]"
    ));

    let antenna_type_schema = TableSchema::new(vec![ColumnSchema::array_variable(
        "POSITION",
        PrimitiveType::Int32,
        Some(1),
    )])
    .unwrap();
    let mut bad_antenna_type_table = Table::with_schema(antenna_type_schema);
    bad_antenna_type_table
        .add_row(RecordValue::new(vec![RecordField::new(
            "POSITION",
            Value::Array(ArrayValue::Int32(
                ArrayD::from_shape_vec(vec![3], vec![1, 2, 3]).unwrap(),
            )),
        )]))
        .unwrap();
    assert!(matches!(
        AntennaPositionColumn::new(&bad_antenna_type_table).get_position(0),
        Err(MsError::ColumnTypeMismatch { expected, .. }) if expected == "Float64 array"
    ));

    let field_schema = TableSchema::new(vec![
        ColumnSchema::array_variable("DELAY_DIR", PrimitiveType::Float64, Some(2)),
        ColumnSchema::array_variable("PHASE_DIR", PrimitiveType::Float64, Some(2)),
        ColumnSchema::array_variable("REFERENCE_DIR", PrimitiveType::Float64, Some(2)),
    ])
    .unwrap();
    let mut bad_field_shape_table = Table::with_schema(field_schema.clone());
    bad_field_shape_table
        .add_row(RecordValue::new(vec![
            RecordField::new(
                "DELAY_DIR",
                Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(vec![3, 1], vec![1.0, 0.5, 0.1]).unwrap(),
                )),
            ),
            RecordField::new(
                "PHASE_DIR",
                Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(vec![3, 1], vec![1.0, 0.5, 0.1]).unwrap(),
                )),
            ),
            RecordField::new(
                "REFERENCE_DIR",
                Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(vec![3, 1], vec![1.0, 0.5, 0.1]).unwrap(),
                )),
            ),
        ]))
        .unwrap();
    assert!(matches!(
        DirectionColumn::delay_dir(&bad_field_shape_table).get_direction(0),
        Err(MsError::ColumnTypeMismatch { expected, .. }) if expected == "shape [2, N]"
    ));
    assert!(matches!(
        DirectionColumn::reference_dir(&bad_field_shape_table).get_direction_at(0, 10.0),
        Err(MsError::ColumnTypeMismatch { expected, .. }) if expected == "shape [2, N]"
    ));

    let field_type_schema = TableSchema::new(vec![
        ColumnSchema::array_variable("DELAY_DIR", PrimitiveType::Int32, Some(2)),
        ColumnSchema::array_variable("PHASE_DIR", PrimitiveType::Int32, Some(2)),
        ColumnSchema::array_variable("REFERENCE_DIR", PrimitiveType::Int32, Some(2)),
    ])
    .unwrap();
    let mut bad_field_type_table = Table::with_schema(field_type_schema);
    bad_field_type_table
        .add_row(RecordValue::new(vec![
            RecordField::new(
                "DELAY_DIR",
                Value::Array(ArrayValue::Int32(
                    ArrayD::from_shape_vec(vec![2, 1], vec![1, 2]).unwrap(),
                )),
            ),
            RecordField::new(
                "PHASE_DIR",
                Value::Array(ArrayValue::Int32(
                    ArrayD::from_shape_vec(vec![2, 1], vec![1, 2]).unwrap(),
                )),
            ),
            RecordField::new(
                "REFERENCE_DIR",
                Value::Array(ArrayValue::Int32(
                    ArrayD::from_shape_vec(vec![2, 1], vec![1, 2]).unwrap(),
                )),
            ),
        ]))
        .unwrap();
    assert!(matches!(
        DirectionColumn::phase_dir(&bad_field_type_table).get_direction(0),
        Err(MsError::ColumnTypeMismatch { expected, .. }) if expected == "Float64 array"
    ));

    let spw_schema =
        build_table_schema(casacore_ms::schema::spectral_window::REQUIRED_COLUMNS).unwrap();
    let mut bad_spw_table = Table::with_schema(spw_schema);
    bad_spw_table
        .add_row(record_for_defs(
            casacore_ms::schema::spectral_window::REQUIRED_COLUMNS,
            &[
                (
                    "CHAN_FREQ",
                    Value::Array(ArrayValue::Int32(
                        ArrayD::from_shape_vec(vec![2], vec![1, 2]).unwrap(),
                    )),
                ),
                ("MEAS_FREQ_REF", Value::Scalar(ScalarValue::Int32(5))),
            ],
        ))
        .unwrap();
    assert!(matches!(
        ChanFreqColumn::new(&bad_spw_table).get_frequencies(0),
        Err(MsError::ColumnTypeMismatch { expected, .. }) if expected == "Float64 array"
    ));
}

fn unpack_fixture_ms(archive_name: &str) -> (tempfile::TempDir, PathBuf) {
    let temp = tempdir().expect("tempdir");
    let archive_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(archive_name);
    let archive_file = File::open(&archive_path).expect("open fixture archive");
    let mut archive = Archive::new(GzDecoder::new(archive_file));
    archive.unpack(temp.path()).expect("unpack fixture archive");

    let ms_dir_name = archive_name
        .strip_suffix(".tgz")
        .expect("fixture archive suffix");
    let ms_path = temp.path().join(ms_dir_name);
    assert!(
        ms_path.is_dir(),
        "expected unpacked MS at {}",
        ms_path.display()
    );
    (temp, ms_path)
}

fn record_for_defs(defs: &[ColumnDef], overrides: &[(&str, Value)]) -> RecordValue {
    let fields = defs
        .iter()
        .map(|def| {
            let value = overrides
                .iter()
                .find_map(|(name, value)| (*name == def.name).then(|| value.clone()))
                .unwrap_or_else(|| default_value_for_def(def));
            RecordField::new(def.name, value)
        })
        .collect();
    RecordValue::new(fields)
}

fn default_value_for_def(def: &ColumnDef) -> Value {
    match def.column_kind {
        ColumnKind::Scalar => Value::Scalar(default_scalar(def.data_type)),
        ColumnKind::FixedArray { shape } => Value::Array(default_array(def.data_type, shape)),
        ColumnKind::VariableArray { ndim } => {
            let shape = vec![1; ndim];
            Value::Array(default_array(def.data_type, &shape))
        }
    }
}

fn default_scalar(data_type: PrimitiveType) -> ScalarValue {
    match data_type {
        PrimitiveType::Bool => ScalarValue::Bool(false),
        PrimitiveType::Int16 => ScalarValue::Int16(0),
        PrimitiveType::Int32 => ScalarValue::Int32(0),
        PrimitiveType::Int64 => ScalarValue::Int64(0),
        PrimitiveType::UInt16 => ScalarValue::UInt16(0),
        PrimitiveType::UInt32 => ScalarValue::UInt32(0),
        PrimitiveType::Float32 => ScalarValue::Float32(0.0),
        PrimitiveType::Float64 => ScalarValue::Float64(0.0),
        PrimitiveType::Complex32 => ScalarValue::Complex32(num_complex::Complex32::new(0.0, 0.0)),
        PrimitiveType::Complex64 => ScalarValue::Complex64(num_complex::Complex64::new(0.0, 0.0)),
        PrimitiveType::String => ScalarValue::String(String::new()),
        PrimitiveType::UInt8 => ScalarValue::UInt8(0),
    }
}

fn default_array(data_type: PrimitiveType, shape: &[usize]) -> ArrayValue {
    let len = shape.iter().product();
    match data_type {
        PrimitiveType::Bool => {
            ArrayValue::Bool(ArrayD::from_shape_vec(shape.to_vec(), vec![false; len]).unwrap())
        }
        PrimitiveType::Int16 => {
            ArrayValue::Int16(ArrayD::from_shape_vec(shape.to_vec(), vec![0; len]).unwrap())
        }
        PrimitiveType::Int32 => {
            ArrayValue::Int32(ArrayD::from_shape_vec(shape.to_vec(), vec![0; len]).unwrap())
        }
        PrimitiveType::Int64 => {
            ArrayValue::Int64(ArrayD::from_shape_vec(shape.to_vec(), vec![0; len]).unwrap())
        }
        PrimitiveType::UInt16 => {
            ArrayValue::UInt16(ArrayD::from_shape_vec(shape.to_vec(), vec![0; len]).unwrap())
        }
        PrimitiveType::UInt32 => {
            ArrayValue::UInt32(ArrayD::from_shape_vec(shape.to_vec(), vec![0; len]).unwrap())
        }
        PrimitiveType::Float32 => {
            ArrayValue::Float32(ArrayD::from_shape_vec(shape.to_vec(), vec![0.0; len]).unwrap())
        }
        PrimitiveType::Float64 => {
            ArrayValue::Float64(ArrayD::from_shape_vec(shape.to_vec(), vec![0.0; len]).unwrap())
        }
        PrimitiveType::Complex32 => ArrayValue::Complex32(
            ArrayD::from_shape_vec(
                shape.to_vec(),
                vec![num_complex::Complex32::new(0.0, 0.0); len],
            )
            .unwrap(),
        ),
        PrimitiveType::Complex64 => ArrayValue::Complex64(
            ArrayD::from_shape_vec(
                shape.to_vec(),
                vec![num_complex::Complex64::new(0.0, 0.0); len],
            )
            .unwrap(),
        ),
        PrimitiveType::String => ArrayValue::String(
            ArrayD::from_shape_vec(shape.to_vec(), vec![String::new(); len]).unwrap(),
        ),
        PrimitiveType::UInt8 => {
            ArrayValue::UInt8(ArrayD::from_shape_vec(shape.to_vec(), vec![0; len]).unwrap())
        }
    }
}
