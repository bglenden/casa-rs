// SPDX-License-Identifier: LGPL-3.0-or-later

//! MODEL_DATA and CORRECTED_DATA writes from the final major-cycle pass.

use super::*;

#[test]
fn application_commits_exact_final_prediction_to_model_data() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = tiny_measurement_set(root.path());
    let image_name = root.path().join("savemodel");
    let mut imaging = request(
        measurement_set.clone(),
        image_name,
        ContinuumAlgorithm::Hogbom,
    );
    imaging.save_model_column = true;
    imaging.task_requirements = vec![TaskRequirement::SerialCpu];
    imaging.resource_policy = casa_imaging_runtime::ResourcePolicy::Balanced;

    let result = execute_continuum(imaging).expect("native save-model application execution");
    let visibility = result
        .outcome
        .output
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

    let mut overwrite = request(
        measurement_set,
        root.path().join("savemodel-overwrite"),
        ContinuumAlgorithm::Hogbom,
    );
    overwrite.save_model_column = true;
    let overwrite_result =
        execute_continuum(overwrite).expect("native in-place MODEL_DATA overwrite");
    assert_eq!(
        overwrite_result
            .outcome
            .output
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
    let mut imaging = request(
        measurement_set.clone(),
        root.path().join("continuum-subtracted-line"),
        ContinuumAlgorithm::Hogbom,
    );
    imaging.channel_start = Some(1);
    imaging.channel_count = Some(1);
    imaging.spectral_window = Some("0:1".to_string());
    let axis = CubeAxisConfig {
        outframe: FrequencyRef::TOPO,
        start: Some(CubeAxisValue::Channel(1)),
        width: Some(CubeAxisValue::Channel(1)),
        ..CubeAxisConfig::default()
    };
    imaging.spectral_mode = SpectralImagingMode::Cube {
        axis,
        output_channels: Some(1),
    };
    imaging.continuum_subtraction = Some(VisibilityContinuumSubtraction {
        fit_spw: "0:0;3".to_string(),
        fit_order: 0,
    });
    imaging.save_model_column = true;

    let result = execute_continuum(imaging).expect("line-only MODEL_DATA write");
    assert_eq!(
        result
            .outcome
            .output
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

    let mut imaging = request(
        measurement_set.clone(),
        root.path().join("persisted-continuum-residual"),
        ContinuumAlgorithm::Hogbom,
    );
    imaging.channel_start = Some(1);
    imaging.channel_count = Some(1);
    imaging.spectral_window = Some("0:1".to_string());
    imaging.spectral_mode = SpectralImagingMode::Cube {
        axis: CubeAxisConfig {
            outframe: FrequencyRef::TOPO,
            start: Some(CubeAxisValue::Channel(1)),
            width: Some(CubeAxisValue::Channel(1)),
            ..CubeAxisConfig::default()
        },
        output_channels: Some(1),
    };
    imaging.continuum_subtraction = Some(VisibilityContinuumSubtraction {
        fit_spw: "0:0;3".to_string(),
        fit_order: 0,
    });
    imaging.save_model_column = true;
    imaging.save_continuum_residual = true;

    let result = execute_continuum(imaging).expect("persist continuum residual");
    assert_eq!(result.outcome.output.major_cycle_count, 2);
    assert!(
        result.outcome.output.visibility_products.is_some(),
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
    assert!(!measurement_set.join(".casa-rs-write-incomplete").exists());
}

#[test]
fn dirty_continuum_residual_persistence_is_independent_of_model_writeback() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = spectral_line_measurement_set(root.path());
    let mut imaging = request(
        measurement_set.clone(),
        root.path().join("dirty-persisted-continuum-residual"),
        ContinuumAlgorithm::Dirty,
    );
    imaging.channel_start = Some(1);
    imaging.channel_count = Some(1);
    imaging.spectral_window = Some("0:1".to_string());
    imaging.spectral_mode = SpectralImagingMode::Cube {
        axis: CubeAxisConfig {
            outframe: FrequencyRef::TOPO,
            start: Some(CubeAxisValue::Channel(1)),
            width: Some(CubeAxisValue::Channel(1)),
            ..CubeAxisConfig::default()
        },
        output_channels: Some(1),
    };
    imaging.continuum_subtraction = Some(VisibilityContinuumSubtraction {
        fit_spw: "0:0;3".to_string(),
        fit_order: 0,
    });
    imaging.save_continuum_residual = true;
    assert!(!imaging.save_model_column);

    let result = execute_continuum(imaging).expect("dirty residual-only persistence");
    assert_eq!(result.outcome.output.major_cycle_count, 1);
    assert!(
        result.outcome.output.visibility_products.is_some(),
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
    let mut imaging = request(
        measurement_set.clone(),
        root.path().join("polarized-savemodel"),
        ContinuumAlgorithm::Hogbom,
    );
    imaging.channel_count = Some(2);
    imaging.save_model_column = true;

    let result = execute_continuum(imaging).expect("partially flagged MODEL_DATA write");
    let visibility = result
        .outcome
        .output
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
    for correlation in 0..4 {
        for channel in 0..2 {
            let model_value = model[[correlation, channel]];
            let parallel_hand = matches!(correlation, 0 | 3);
            if flags[[correlation, channel]] || !parallel_hand {
                assert_eq!(
                    model_value,
                    Complex32::new(0.0, 0.0),
                    "flagged and unsupported cross-hand predictions persist as CASA zeros"
                );
            } else {
                assert_ne!(
                    model_value,
                    Complex32::new(0.0, 0.0),
                    "unflagged parallel-hand predictions retain the solved Stokes-I model"
                );
            }
        }
    }
}
