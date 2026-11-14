// SPDX-License-Identifier: LGPL-3.0-or-later

//! MODEL_DATA and CORRECTED_DATA writes from the final major-cycle pass,
//! through the observation's update session (#932, step 5).

use casa_imaging_application::ImagingOutcome;

use super::*;

/// Run `imaging`, which writes visibilities back through the batch route's
/// update session.
fn execute_writing(imaging: &ImagingRequest) -> ImagingOutcome {
    execute(imaging).expect("the run")
}

/// Channel 1 of the line fixture as a one-channel cube, after a
/// zeroth-order continuum fit to channels 0 and 3, with `overrides`.
fn continuum_subtracted_line(
    measurement_set: &Path,
    image_name: &Path,
    overrides: serde_json::Value,
) -> ImagingRequest {
    let mut values = json!({
        "channel_start": 1,
        "channel_count": 1,
        "spw": "0:1",
        "specmode": "cube",
        "outframe": "TOPO",
        "start": "1",
        "width": "1",
        "fitspw": "0:0;3",
        "fitorder": 0,
    });
    values
        .as_object_mut()
        .expect("line controls")
        .extend(overrides.as_object().expect("overrides").clone());
    request(measurement_set, image_name, values)
}

#[test]
fn application_commits_exact_final_prediction_to_model_data() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = tiny_measurement_set(root.path());
    let image_name = root.path().join("savemodel");
    let imaging = request(
        &measurement_set,
        &image_name,
        json!({ "savemodel": "modelcolumn" }),
    );

    let result = execute_writing(&imaging);
    let visibility = result
        .visibility_products
        .expect("final visibility completion");
    assert_eq!(visibility.sample_count(), 1);

    let reopened = MeasurementSet::open(&measurement_set).expect("reopen saved MODEL_DATA");
    let schema = reopened.main_table().schema().expect("MAIN schema");
    assert!(schema.contains_column("MODEL_DATA"));
    let model_column = reopened
        .data_column(VisibilityDataColumn::ModelData)
        .expect("MODEL_DATA was committed");
    let ArrayValue::Complex32(model) = model_column.get(0).expect("MODEL_DATA row") else {
        panic!("MODEL_DATA row is complex")
    };
    assert!(model[[0, 0]].re.is_finite());
    assert!(model[[0, 0]].im.is_finite());
    assert_ne!(model[[0, 0]], Complex32::new(0.0, 0.0));
    drop(reopened);

    let overwrite = request(
        &measurement_set,
        &root.path().join("savemodel-overwrite"),
        json!({ "savemodel": "modelcolumn" }),
    );
    let overwrite_result = execute_writing(&overwrite);
    assert_eq!(
        overwrite_result
            .visibility_products
            .expect("overwrite visibility completion")
            .sample_count(),
        1,
        "overwrite writes only the selected in-place cell"
    );
}

#[test]
fn continuum_fit_only_channels_are_read_but_not_persisted_as_line_model_data() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = spectral_line_measurement_set(root.path());
    let imaging = continuum_subtracted_line(
        &measurement_set,
        &root.path().join("continuum-subtracted-line"),
        json!({ "savemodel": "modelcolumn" }),
    );

    let result = execute_writing(&imaging);
    assert_eq!(
        result
            .visibility_products
            .expect("final visibility completion")
            .sample_count(),
        4,
        "only the output channel contributes final line predictions"
    );
    let reopened = MeasurementSet::open(&measurement_set).expect("reopen MODEL_DATA");
    let model_column = reopened
        .data_column(VisibilityDataColumn::ModelData)
        .expect("MODEL_DATA");
    let model = model_column.get(0).expect("MODEL_DATA row");
    let ArrayValue::Complex32(model) = model else {
        panic!("MODEL_DATA row is complex")
    };
    for correlation in 0..4 {
        assert_eq!(
            model[[correlation, 0]],
            Complex32::new(9.0, 9.0),
            "fit-only channel must remain untouched"
        );
        assert_ne!(
            model[[correlation, 1]],
            Complex32::new(9.0, 9.0),
            "output channel must receive the final line model"
        );
        assert_eq!(
            model[[correlation, 3]],
            Complex32::new(9.0, 9.0),
            "second fit-only channel must remain untouched"
        );
    }
}

#[test]
fn continuum_residual_persistence_overwrites_only_output_roles_in_the_terminal_pass() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = spectral_line_measurement_set(root.path());
    let before = MeasurementSet::open(&measurement_set).expect("open before persistence");
    let flags_before = before
        .main_table()
        .column_accessor("FLAG")
        .expect("FLAG")
        .get(0)
        .expect("read FLAG")
        .cloned();
    let weights_before = before
        .main_table()
        .column_accessor("WEIGHT")
        .expect("WEIGHT")
        .get(0)
        .expect("read WEIGHT")
        .cloned();
    drop(before);

    let imaging = continuum_subtracted_line(
        &measurement_set,
        &root.path().join("persisted-continuum-residual"),
        json!({ "savemodel": "modelcolumn", "save_continuum_residual": true }),
    );

    let result = execute_writing(&imaging);
    assert_eq!(result.major_cycle_count, 2);
    assert!(
        result.visibility_products.is_some(),
        "the terminal pass completes the combined visibility write"
    );

    let reopened = MeasurementSet::open(&measurement_set).expect("reopen persisted residual");
    let corrected_column = reopened
        .data_column(VisibilityDataColumn::CorrectedData)
        .expect("CORRECTED_DATA");
    let ArrayValue::Complex32(corrected) = corrected_column.get(0).expect("CORRECTED_DATA row")
    else {
        panic!("CORRECTED_DATA is complex")
    };
    for correlation in 0..4 {
        assert_eq!(
            corrected[[correlation, 0]],
            Complex32::new(20.0 + (correlation * 4) as f32, -3.0),
            "fit-only cells remain unchanged"
        );
        assert_eq!(
            corrected[[correlation, 1]],
            Complex32::new(1.0, 0.0),
            "output-role cells receive exact transformed observations"
        );
        assert_eq!(
            corrected[[correlation, 2]],
            Complex32::new(22.0 + (correlation * 4) as f32, -3.0),
            "nonselected cells remain unchanged"
        );
        assert_eq!(
            corrected[[correlation, 3]],
            Complex32::new(23.0 + (correlation * 4) as f32, -3.0),
            "second fit-only cells remain unchanged"
        );
    }
    assert_eq!(
        reopened
            .main_table()
            .column_accessor("FLAG")
            .expect("FLAG")
            .get(0)
            .expect("read FLAG")
            .cloned(),
        flags_before
    );
    assert_eq!(
        reopened
            .main_table()
            .column_accessor("WEIGHT")
            .expect("WEIGHT")
            .get(0)
            .expect("read WEIGHT")
            .cloned(),
        weights_before
    );
    let entries = std::fs::read_dir(&measurement_set)
        .expect("list written MS")
        .map(|entry| {
            entry
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect::<Vec<_>>();
    assert!(
        entries.iter().all(|name| !name.contains("casa-rs")),
        "the visibility write left casa-rs files: {entries:?}"
    );
}

#[test]
fn dirty_continuum_residual_persistence_is_independent_of_model_writeback() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = spectral_line_measurement_set(root.path());
    let imaging = continuum_subtracted_line(
        &measurement_set,
        &root.path().join("dirty-persisted-continuum-residual"),
        json!({ "niter": 0, "save_continuum_residual": true }),
    );
    assert_eq!(imaging.savemodel, casa_imaging_application::SaveModel::None);

    let result = execute_writing(&imaging);
    assert_eq!(result.major_cycle_count, 1);
    assert!(
        result.visibility_products.is_some(),
        "the sole dirty pass completes the residual-only visibility write"
    );

    let reopened = MeasurementSet::open(&measurement_set).expect("reopen residual-only MS");
    let corrected_column = reopened
        .data_column(VisibilityDataColumn::CorrectedData)
        .expect("CORRECTED_DATA");
    let ArrayValue::Complex32(corrected) = corrected_column.get(0).expect("CORRECTED_DATA row")
    else {
        panic!("CORRECTED_DATA is complex")
    };
    for correlation in 0..4 {
        assert_eq!(corrected[[correlation, 1]], Complex32::new(1.0, 0.0));
    }
    let model_column = reopened
        .data_column(VisibilityDataColumn::ModelData)
        .expect("MODEL_DATA");
    let ArrayValue::Complex32(model) = model_column.get(0).expect("MODEL_DATA row") else {
        panic!("MODEL_DATA is complex")
    };
    assert!(
        model.iter().all(|value| *value == Complex32::new(9.0, 9.0)),
        "residual-only persistence leaves MODEL_DATA untouched"
    );
}

#[test]
fn application_replaces_every_selected_model_cell_when_flags_and_correlations_differ() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = flagged_polarized_measurement_set(root.path());
    let imaging = request(
        &measurement_set,
        &root.path().join("polarized-savemodel"),
        json!({ "channel_count": 2, "savemodel": "modelcolumn" }),
    );

    let result = execute_writing(&imaging);
    let visibility = result
        .visibility_products
        .expect("terminal visibility completion");
    assert_eq!(
        visibility.sample_count(),
        8,
        "the sink covers all selected rows, channels, and correlations"
    );

    let reopened = MeasurementSet::open(&measurement_set).expect("reopen MODEL_DATA");
    let model_column = reopened
        .data_column(VisibilityDataColumn::ModelData)
        .expect("MODEL_DATA column");
    let ArrayValue::Complex32(model) = model_column.get(0).expect("MODEL_DATA row") else {
        panic!("MODEL_DATA row is complex")
    };
    let flag_column = reopened
        .main_table()
        .column_accessor("FLAG")
        .expect("FLAG column");
    let Value::Array(ArrayValue::Bool(flags)) = flag_column
        .get(0)
        .expect("read FLAG row")
        .cloned()
        .expect("defined FLAG row")
    else {
        panic!("FLAG row is boolean")
    };
    assert!(
        model.iter().all(|value| *value != Complex32::new(9.0, 9.0)),
        "no selected destination retains its stale pre-run value"
    );
    for channel in 0..2 {
        // GridFT's scalar model prediction suppresses the entire channel
        // when any correlation is flagged.
        let channel_flagged = (0..4).any(|correlation| flags[[correlation, channel]]);
        for correlation in 0..4 {
            let model_value = model[[correlation, channel]];
            let parallel_hand = matches!(correlation, 0 | 3);
            if channel_flagged || !parallel_hand {
                assert_eq!(
                    model_value,
                    Complex32::new(0.0, 0.0),
                    "partially flagged channels and unsupported cross-hands persist as CASA zeros"
                );
            } else {
                assert_ne!(
                    model_value,
                    Complex32::new(0.0, 0.0),
                    "parallel hands of a fully unflagged channel retain the solved Stokes-I model"
                );
            }
        }
    }
}

/// A model saved over four data descriptions, which a UV range thins and,
/// for one, empties: the written partitions' blocks are written each to
/// its own rows, and every row outside the selection keeps its stale
/// `MODEL_DATA`. Baselines (0,1), (0,2) and (1,2) lie within 100 m and the
/// rest beyond (see the fixture's UVW); data description 3's rows (the
/// row number is 3 modulo 4) are moved to 1 km.
#[test]
fn a_model_saved_over_partitions_leaves_what_the_uv_range_excludes() {
    model_saved_over_partitions(0.0);
}

/// The separate parent predictor preserves each selected batch's original
/// row and partition addresses as well.
#[test]
fn final_prediction_preserves_partition_and_row_provenance() {
    model_saved_over_partitions(0.01);
}

fn model_saved_over_partitions(nsigma: f64) {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = four_spw_vla_measurement_set(root.path());
    let mut ms = MeasurementSet::open(&measurement_set).expect("the fixture");
    for row in (3..ms.row_count()).step_by(4) {
        ms.main_table_mut()
            .row_accessor_mut()
            .set_cell(
                row,
                "UVW",
                Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(vec![3], vec![1_000.0, 0.0, 0.0]).expect("UVW"),
                )),
            )
            .expect("move a row beyond the UV range");
    }
    ms.save().expect("save the fixture");
    drop(ms);
    let imaging = request(
        &measurement_set,
        &root.path().join("spanning-savemodel"),
        json!({
            "ddid": null,
            "spw": "0~3",
            "channel_start": null,
            "channel_count": null,
            "uvrange": "0~100m",
            "savemodel": "modelcolumn",
            "nsigma": nsigma,
        }),
    );

    let result = execute_writing(&imaging);
    let reopened = MeasurementSet::open(&measurement_set).expect("reopen MODEL_DATA");
    let model_column = reopened
        .data_column(VisibilityDataColumn::ModelData)
        .expect("MODEL_DATA column");
    let uvw = reopened
        .main_table()
        .column_accessor("UVW")
        .expect("UVW column");
    let mut selected = 0;
    for row in 0..reopened.row_count() {
        let Some(Value::Array(ArrayValue::Float64(uvw))) = uvw.get(row).expect("UVW row").cloned()
        else {
            panic!("UVW row {row} is a defined vector")
        };
        let ArrayValue::Complex32(model) = model_column.get(row).expect("MODEL_DATA row") else {
            panic!("MODEL_DATA row is complex")
        };
        let stale = model
            .iter()
            .filter(|value| **value == Complex32::new(9.0, 9.0));
        if uvw[[0]].hypot(uvw[[1]]) <= 100.0 {
            selected += 1;
            assert_eq!(stale.count(), 0, "row {row} is selected and written");
        } else {
            assert_eq!(stale.count(), model.len(), "row {row} is excluded and kept");
        }
    }
    assert_eq!(
        selected, 8,
        "two rows each of data descriptions 0 and 2, four of 1, none of 3"
    );
    assert_eq!(
        result
            .visibility_products
            .expect("terminal visibility completion")
            .sample_count(),
        selected * 8 * 4,
        "every channel and correlation of each selected row is written"
    );
}

/// The final parent prediction must retain the channels used for a
/// continuum fit and persist only output roles, including when `spw` names
/// channels that are neither fit nor output roles.
#[test]
fn final_model_prediction_preserves_continuum_fit_and_residual_writes() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    for spw in ["0:1", "0"] {
        let root = tempfile::tempdir().expect("test root");
        let measurement_set = spectral_line_measurement_set(root.path());
        let reference_root = root.path().join("reference");
        std::fs::create_dir(&reference_root).expect("reference directory");
        let reference_ms = spectral_line_measurement_set(&reference_root);
        let reference_request = continuum_subtracted_line(
            &reference_ms,
            &reference_root.join("ordinary-corrected-write"),
            json!({ "spw": spw, "save_continuum_residual": true, "nsigma": 0.01 }),
        );
        execute_writing(&reference_request);
        let reference = MeasurementSet::open(&reference_ms).expect("reference MS");
        let reference_column = reference
            .data_column(VisibilityDataColumn::CorrectedData)
            .expect("reference CORRECTED_DATA");
        let ArrayValue::Complex32(reference_corrected) =
            reference_column.get(0).expect("reference row")
        else {
            panic!("reference CORRECTED_DATA is complex")
        };
        let reference_corrected = reference_corrected.to_owned();
        drop(reference);
        let imaging = continuum_subtracted_line(
            &measurement_set,
            &root.path().join("final-prediction-continuum"),
            json!({ "spw": spw, "savemodel": "modelcolumn", "save_continuum_residual": true, "nsigma": 0.01 }),
        );
        execute_writing(&imaging);
        let reopened = MeasurementSet::open(&measurement_set).expect("reopen residual");
        let column = reopened
            .data_column(VisibilityDataColumn::CorrectedData)
            .expect("CORRECTED_DATA");
        let ArrayValue::Complex32(corrected) = column.get(0).expect("CORRECTED_DATA row") else {
            panic!("CORRECTED_DATA is complex")
        };
        let model_column = reopened
            .data_column(VisibilityDataColumn::ModelData)
            .expect("MODEL_DATA");
        let ArrayValue::Complex32(model) = model_column.get(0).expect("MODEL_DATA row") else {
            panic!("MODEL_DATA is complex")
        };
        for correlation in 0..4 {
            assert_eq!(
                corrected[[correlation, 1]],
                Complex32::new(1.0, 0.0),
                "{spw}: output role"
            );
            assert_ne!(
                model[[correlation, 1]],
                Complex32::new(9.0, 9.0),
                "{spw}: model output"
            );
            for channel in 0..4 {
                assert_eq!(
                    corrected[[correlation, channel]],
                    reference_corrected[[correlation, channel]],
                    "{spw}: corrected output agrees with an ordinary tuned write, channel {channel}"
                );
                if spw == "0:1" && channel != 1 {
                    assert_eq!(
                        corrected[[correlation, channel]],
                        Complex32::new(20.0 + (correlation * 4 + channel) as f32, -3.0),
                        "{spw}: non-output corrected channel {channel}"
                    );
                    assert_eq!(
                        model[[correlation, channel]],
                        Complex32::new(9.0, 9.0),
                        "{spw}: non-output model channel {channel}"
                    );
                }
            }
        }
    }
}

/// If the separate predictor cannot hold every plane, refuse before the
/// corrected-data traversal can change the observation.
#[test]
fn refused_final_prediction_leaves_both_visibility_columns_unchanged() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = spectral_line_many_row_measurement_set(root.path());
    let imaging = continuum_subtracted_line(
        &measurement_set,
        &root.path().join("refused-prediction"),
        json!({
            "imsize": 256, "channel_count": 2, "niter": 1, "nsigma": 0.01,
            "savemodel": "modelcolumn", "save_continuum_residual": true,
        }),
    );
    let context = RunContext {
        host: HostResources {
            threads: 64,
            performance_cores: 64,
            available_memory: 1 << 40,
            metal: false,
        },
        ..context_with(ResourcePolicy::Explicit {
            workers: 4,
            memory: compile_resident() + (35_000 << 10),
        })
    };
    let Err(error) = casa_imaging_application::execute(&imaging, context) else {
        panic!("the predictor must refuse when it cannot fit all planes");
    };
    assert!(
        error
            .to_string()
            .contains("writing visibilities needs every plane resident"),
        "{error}"
    );
    let reopened = MeasurementSet::open(&measurement_set).expect("reopen the observation");
    for (column, expected) in [
        (
            VisibilityDataColumn::CorrectedData,
            Complex32::new(21.0, -3.0),
        ),
        (VisibilityDataColumn::ModelData, Complex32::new(9.0, 9.0)),
    ] {
        let data = reopened.data_column(column).expect("visibility column");
        let ArrayValue::Complex32(row) = data.get(0).expect("first row") else {
            panic!("visibility row is complex");
        };
        assert_eq!(row[[0, 1]], expected, "{column:?} changed before refusal");
    }
}

/// A per-channel density sees the line output's physical singleton, just as
/// a separately continuum-subtracted observation does, not the fit union.
#[test]
fn briggs_density_ignores_continuum_fit_only_channels() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = spectral_line_measurement_set(root.path());
    let fitted_name = root.path().join("fitted-density");
    let controls = json!({
        "channel_start": null, "channel_count": 2, "specmode": "cube", "outframe": "TOPO",
        "spw": "0:1", "start": "44.00125GHz", "width": "1MHz", "niter": 0,
        "fitspw": "0:0;3", "fitorder": 0, "weighting": "briggs", "robust": 0.5,
        "perchanweightdensity": true, "save_continuum_residual": true,
    });
    execute(&request(&measurement_set, &fitted_name, controls.clone()))
        .unwrap_or_else(|error| panic!("fitted imaging: {error:?}"));
    let reference_name = root.path().join("materialized-density");
    let mut materialized = controls;
    materialized["fitspw"] = serde_json::Value::Null;
    materialized["save_continuum_residual"] = json!(false);
    materialized["datacolumn"] = json!("corrected");
    execute(&request(&measurement_set, &reference_name, materialized))
        .unwrap_or_else(|error| panic!("materialized imaging: {error:?}"));
    let read = |base: &Path, suffix: &str| {
        let image = PagedImage::<f32>::open(PathBuf::from(format!("{}{suffix}", base.display())))
            .expect("image product");
        image
            .get_slice(&vec![0; image.shape().len()], image.shape())
            .expect("pixels")
    };
    assert!(
        read(&reference_name, ".sumwt")
            .iter()
            .any(|weight| *weight > 0.0),
        "the line contributes density"
    );
    for suffix in [".sumwt", ".residual"] {
        assert_eq!(
            read(&fitted_name, suffix),
            read(&reference_name, suffix),
            "{suffix}"
        );
    }
}

/// A nonconstant multi-plane model predicted after continuum fitting agrees
/// with the same fit materialized in a physically output-only observation.
#[test]
fn final_continuum_prediction_matches_a_compact_materialized_spectrum() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let original_root = root.path().join("original");
    let compact_root = root.path().join("compact");
    std::fs::create_dir(&original_root).unwrap();
    std::fs::create_dir(&compact_root).unwrap();
    let original = line_model_measurement_set(&original_root, 40);
    let compact = line_model_measurement_set(&compact_root, 11);
    // The parent row begins with a quarter-MHz gap, while its compact
    // output channels are one MHz apart. A wider parent row would refine
    // the prediction grid fourfold and change the predicted line values.
    let mut irregular = MeasurementSet::open(&original).unwrap();
    let mut frequencies = (0..40)
        .map(|channel| 44.0e9 + f64::from(channel) * 1.0e6)
        .collect::<Vec<_>>();
    frequencies[1] = 44.00025e9;
    irregular
        .subtable_mut(SubtableId::SpectralWindow)
        .unwrap()
        .row_accessor_mut()
        .set_cell(
            0,
            "CHAN_FREQ",
            Value::Array(ArrayValue::Float64(
                ArrayD::from_shape_vec(vec![40], frequencies).unwrap(),
            )),
        )
        .unwrap();
    irregular.save().unwrap();
    drop(irregular);
    let raw = MeasurementSet::open(&original).unwrap();
    let data = raw.data_column(VisibilityDataColumn::Data).unwrap();
    let ArrayValue::Complex32(raw_values) = data.get(0).unwrap() else {
        panic!("DATA")
    };
    let mut corrected = ArrayD::zeros(vec![4, 11]);
    for correlation in 0..4 {
        let continuum = (raw_values[[correlation, 0]] + raw_values[[correlation, 39]]) / 2.0;
        for channel in 0..11 {
            corrected[[correlation, channel]] = raw_values[[correlation, channel + 16]] - continuum;
        }
    }
    drop(raw);
    let mut reference = MeasurementSet::open(&compact).unwrap();
    reference
        .main_table_mut()
        .row_accessor_mut()
        .set_cell(0, "DATA", Value::Array(ArrayValue::Complex32(corrected)))
        .unwrap();
    let frequencies = (16..27)
        .map(|channel| 44.0e9 + f64::from(channel) * 1.0e6)
        .collect();
    let window = reference.subtable_mut(SubtableId::SpectralWindow).unwrap();
    window
        .row_accessor_mut()
        .set_cell(0, "REF_FREQUENCY", float(44.016e9))
        .unwrap();
    window
        .row_accessor_mut()
        .set_cell(
            0,
            "CHAN_FREQ",
            Value::Array(ArrayValue::Float64(
                ArrayD::from_shape_vec(vec![11], frequencies).unwrap(),
            )),
        )
        .unwrap();
    reference.save().unwrap();
    drop(reference);
    let controls = json!({
        "channel_start": null, "channel_count": 2, "specmode": "cube", "outframe": "TOPO",
        "spw": "0", "start": "44.02025GHz", "width": "1MHz", "niter": 5,
        "nsigma": 0.01, "savemodel": "modelcolumn", "fitspw": "0:0;39", "fitorder": 0,
    });
    let fitted = request(&original, &original_root.join("image"), controls.clone());
    assert_eq!(
        casa_imaging_application::source_law::gridding_layouts(&fitted, &context()).unwrap(),
        [(16..27).collect::<Vec<_>>()],
    );
    execute_writing(&fitted);
    let mut reference_controls = controls;
    reference_controls["fitspw"] = serde_json::Value::Null;
    let reference = request(&compact, &compact_root.join("image"), reference_controls);
    execute_writing(&reference);
    let original = MeasurementSet::open(&original).unwrap();
    let compact = MeasurementSet::open(&compact).unwrap();
    let original_model = original
        .data_column(VisibilityDataColumn::ModelData)
        .unwrap();
    let compact_model = compact
        .data_column(VisibilityDataColumn::ModelData)
        .unwrap();
    let ArrayValue::Complex32(original_model) = original_model.get(0).unwrap() else {
        panic!("MODEL_DATA")
    };
    let ArrayValue::Complex32(compact_model) = compact_model.get(0).unwrap() else {
        panic!("MODEL_DATA")
    };
    for correlation in 0..4 {
        for channel in 0..11 {
            let actual = original_model[[correlation, channel + 16]];
            let expected = compact_model[[correlation, channel]];
            assert!(
                (actual - expected).norm() <= 1.0e-5 * expected.norm().max(1.0),
                "{correlation}/{channel}: {actual} != {expected}"
            );
        }
        for channel in (0..16).chain(27..40) {
            assert_eq!(
                original_model[[correlation, channel]],
                Complex32::new(9.0, 9.0)
            );
        }
    }
    assert!(
        (compact_model[[0, 4]] - compact_model[[0, 5]]).norm() > 1.0e-3,
        "the prediction varies over the output spectrum"
    );
}
