// SPDX-License-Identifier: LGPL-3.0-or-later
//! Row selections by UV distance and intent, on both observation routes,
//! and what the observation-batch route does not read yet.

use super::*;

/// A λ or intent selection that leaves data descriptions without a row
/// drops them with their spectral windows, and images exactly what an
/// explicit selection of the remaining windows images. Spectral windows
/// 0 to 3 sit at 0.01, 0.02, 0.04 and 0.08 m, so every 100 m baseline is
/// 10,000, 5,000, 2,500 and 1,250 λ long; the target intent holds the rows
/// of data descriptions 0 and 1 (DATA_DESC_ID is the row number modulo
/// 4), the phase calibrator those of 2 and 3; state 0 selects them by
/// number as by intent.
#[test]
fn selections_that_empty_data_descriptions_image_the_remaining_rows() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let path = four_spw_vla_measurement_set(root.path());
    let mut ms = MeasurementSet::open(&path).unwrap();
    for (spw, wavelength_m) in [0.01, 0.02, 0.04, 0.08].into_iter().enumerate() {
        let frequency_hz = 299_792_458.0 / wavelength_m;
        let channels = (0..8)
            .map(|channel| frequency_hz + f64::from(channel) * 1.0e6)
            .collect();
        let spectral_window = ms.subtable_mut(SubtableId::SpectralWindow).unwrap();
        spectral_window
            .row_accessor_mut()
            .set_cell(spw, "REF_FREQUENCY", float(frequency_hz))
            .unwrap();
        spectral_window
            .row_accessor_mut()
            .set_cell(
                spw,
                "CHAN_FREQ",
                Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(vec![8], channels).unwrap(),
                )),
            )
            .unwrap();
    }
    for mode in ["OBSERVE_TARGET#ON_SOURCE", "CALIBRATE_PHASE#ON_SOURCE"] {
        ms.subtable_mut(SubtableId::State)
            .unwrap()
            .add_row(required_row(
                schema::state::REQUIRED_COLUMNS,
                &[("OBS_MODE", string(mode))],
            ))
            .unwrap();
    }
    for row in 0..ms.row_count() {
        let angle = row as f64 * 0.25;
        let uvw = vec![100.0 * angle.cos(), 100.0 * angle.sin(), 0.0];
        ms.main_table_mut()
            .row_accessor_mut()
            .set_cell(
                row,
                "UVW",
                Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(vec![3], uvw).unwrap(),
                )),
            )
            .unwrap();
        ms.main_table_mut()
            .row_accessor_mut()
            .set_cell(row, "STATE_ID", int(i32::from(row % 4 >= 2)))
            .unwrap();
    }
    ms.save().unwrap();
    drop(ms);

    let image_on = |name: &str, selection: &serde_json::Value, route: SourceRoute| {
        let image_name = root.path().join(name);
        let mut overrides = json!({ "niter": 0, "ddid": null });
        overrides
            .as_object_mut()
            .unwrap()
            .extend(selection.as_object().unwrap().clone());
        let mut imaging = request(&path, &image_name, overrides);
        imaging.source_route = route;
        execute(&imaging).map(|outcome| (image_name, outcome))
    };
    // Each selection imaged on the observation-batch route too (#932 step
    // 4), which reads its partitions by data description and intent and
    // tests the UV distance per sample, gives the same products.
    let image = |name: &str, selection: serde_json::Value| {
        let imaged = image_on(name, &selection, SourceRoute::SelectedRows)?;
        let (batches, _) = image_on(
            &format!("{name}-batches"),
            &selection,
            SourceRoute::ObservationBatches,
        )
        .unwrap_or_else(|error| panic!("{name} on batches: {error}"));
        for suffix in [".psf", ".residual", ".sumwt"] {
            let nrms = route_law::image_nrms(
                &PathBuf::from(format!("{}{suffix}", imaged.0.display())),
                &PathBuf::from(format!("{}{suffix}", batches.display())),
            )
            .expect("both routes' products");
            assert!(
                nrms <= route_law::TOLERANCE,
                "{name}{suffix}: the routes differ by NRMS {nrms:.3e}"
            );
        }
        Ok(imaged)
    };
    let plane = |image_name: &Path, suffix: &str| {
        product_plane(image_name, suffix)
            .iter()
            .copied()
            .collect::<Vec<_>>()
    };
    let (windows, _) = image("windows", json!({ "spw": "0,1" })).expect("windows 0 and 1");
    for (name, selection) in [
        ("lambda", json!({ "uvrange": ">4000lambda" })),
        ("intent", json!({ "intent": "OBSERVE_TARGET*" })),
        ("intent by state", json!({ "intent": "0" })),
    ] {
        let (image_name, outcome) =
            image(name, selection).unwrap_or_else(|error| panic!("{name}: {error}"));
        let selected = outcome.problem.observation().sources()[0].selection();
        assert_eq!(selected.rows().selected_row_count(), 12, "{name}");
        assert_eq!(
            selected
                .data_descriptions()
                .iter()
                .map(|description| (
                    description.data_description_id(),
                    description.spectral_window_id()
                ))
                .collect::<Vec<_>>(),
            [(0, 0), (1, 1)],
            "{name}"
        );
        for suffix in [".psf", ".residual", ".sumwt"] {
            assert_real_agreement(&plane(&windows, suffix), &plane(&image_name, suffix));
        }
    }

    // Each selector alone selects rows; together they select none.
    let error = image(
        "none",
        json!({ "uvrange": ">4000lambda", "intent": "CALIBRATE_PHASE*" }),
    )
    .err()
    .expect("an empty selection");
    assert!(
        matches!(
            error,
            ApplicationDispatchError::Preparation(PrepareError::NoSelectedRows)
        ),
        "{error}"
    );

    // The MSv2 adapter partitions rows by their state's intents, so a
    // selection of one of two states listing the same intents is refused
    // on the batch route: their rows share partitions.
    let mut ms = MeasurementSet::open(&path).unwrap();
    ms.subtable_mut(SubtableId::State)
        .unwrap()
        .add_row(required_row(
            schema::state::REQUIRED_COLUMNS,
            &[("OBS_MODE", string("OBSERVE_TARGET#ON_SOURCE"))],
        ))
        .unwrap();
    ms.save().unwrap();
    drop(ms);
    let error = image_on(
        "shared-intents",
        &json!({ "intent": "0" }),
        SourceRoute::ObservationBatches,
    )
    .err()
    .expect("a state sharing its intents with an unselected one");
    assert!(error.to_string().contains("same intents"), "{error}");
}

/// The observation-batch route (#932, step 4) refuses what it does not
/// read yet, before anything is imaged: a run that writes the model column
/// (step 5), and a field that follows an ephemeris (#942), whose
/// `PHASE_DIR` batch geometry would read as a direction.
#[test]
fn the_observation_batch_route_refuses_what_it_does_not_read_yet() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    let path = tiny_measurement_set(root.path());
    let on_batches = |name: &str, controls: serde_json::Value| {
        let mut imaging = request(&path, &root.path().join(name), controls);
        imaging.source_route = SourceRoute::ObservationBatches;
        imaging
    };
    let writes = on_batches("writes", json!({ "niter": 10, "savemodel": "modelcolumn" }));
    let error = execute(&writes).err().expect("a model column write");
    assert!(error.to_string().contains("step 5"), "{error}");
    execute(&on_batches("reads", json!({ "niter": 0 }))).expect("the batch route images the field");
    let mut ms = MeasurementSet::open(&path).unwrap();
    ms.subtable_mut(SubtableId::Field)
        .unwrap()
        .add_column(
            casa_tables::ColumnSchema::scalar("EPHEMERIS_ID", PrimitiveType::Int32),
            Some(Value::Scalar(ScalarValue::Int32(0))),
        )
        .unwrap();
    ms.save().unwrap();
    drop(ms);
    let error = execute(&on_batches("moving", json!({ "niter": 0 })))
        .err()
        .expect("an ephemeris field");
    assert!(error.to_string().contains("#942"), "{error}");
}
