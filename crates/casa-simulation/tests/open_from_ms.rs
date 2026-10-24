// SPDX-License-Identifier: LGPL-3.0-or-later
//! `sm.openfromms` (#841 Phase 2): a session on a MeasurementSet that
//! exists, whose `predict` and `corrupt` rewrite its rows at `close`.
//!
//! The laws are two-stage: one session observes, takes DATA steps and
//! closes; another observes and closes, then `openfromms` takes the same
//! steps and closes. On the CPU both hold the same DATA, bit for bit: what
//! `openfromms` reads back (antennas, telescope, channels, correlations,
//! fields, phase centres, rows) is all the steps take. Agreement with CASA
//! is the `openfromms` scenarios' (`tools/simulation`).

mod predict_support;

use std::path::Path;

use casa_simulation::{
    CasaOutcome, NoiseSpec, PredictionOptions, Simulator, SimulatorOptions, SimulatorResult,
};
use casa_tables::{Table, TableOptions};
use casa_types::measures::{EpochRef, MEpoch};
use casa_types::{ArrayValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};
use predict_support::{
    CHANNELS, REFERENCE_MJD, bits, data, extended_model, predict_spec, read_main, resources, scan,
    session, session_with_dishes, write_point_list,
};

/// The options [`session`] opens with.
fn options() -> SimulatorOptions {
    SimulatorOptions {
        measures: casa_test_support::deterministic_measures_provider(),
        resources: resources(2, 2 << 30),
        now: MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC),
    }
}

/// An eight-antenna ALMA session observing the model's centre and then the
/// `offset` field.
fn observed(path: &Path) -> Simulator {
    let mut sm = session(path, "ALMA", "XX YY", resources(2, 2 << 30));
    sm.observe(&scan("target", 0.0, 60.0)).expect("observe");
    sm.observe(&scan("offset", 60.0, 120.0)).expect("observe");
    sm
}

fn simple_noise(sigma_jy: f64) -> NoiseSpec {
    NoiseSpec::simple(sigma_jy)
}

/// `sm.setnoise(simplenoise=…)` then `sm.corrupt()`.
fn corrupt(sm: &mut Simulator) -> SimulatorResult<()> {
    sm.set_noise(&simple_noise(0.25))?;
    sm.corrupt()
}

/// The MeasurementSet `make` observes in `directory/one.ms`, closed with
/// `first` and `then` taken in one session, and in `directory/two.ms`,
/// closed with `first` and then reopened with `openfromms` to take `then`.
fn two_stage(
    directory: &Path,
    make: &dyn Fn(&Path) -> Simulator,
    first: &dyn Fn(&mut Simulator) -> SimulatorResult<()>,
    then: &dyn Fn(&mut Simulator) -> SimulatorResult<()>,
) -> (Vec<predict_support::Row>, Vec<predict_support::Row>) {
    let one = directory.join("one.ms");
    let mut sm = make(&one);
    first(&mut sm).expect("the first steps");
    then(&mut sm).expect("the later steps");
    sm.close().expect("close");
    let two = directory.join("two.ms");
    let mut sm = make(&two);
    first(&mut sm).expect("the first steps");
    sm.close().expect("close");
    let mut sm = Simulator::open_from_ms(&two, options()).expect("openfromms");
    then(&mut sm).expect("the later steps after openfromms");
    sm.close().expect("close the opened MeasurementSet");
    (read_main(&one).0, read_main(&two).0)
}

fn nothing(_: &mut Simulator) -> SimulatorResult<()> {
    Ok(())
}

/// A `predict` and a `corrupt` after `openfromms` write the DATA they write
/// in the session that observed.
#[test]
fn predict_and_corrupt_after_openfromms_write_the_same_data() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let model = extended_model(directory.path(), "model.image");
    let steps = |sm: &mut Simulator| {
        sm.predict(&predict_spec(&model, false))?;
        corrupt(sm)
    };
    let (one, two) = two_stage(directory.path(), &observed, &nothing, &steps);
    assert!(
        one.iter()
            .any(|row| row.data.iter().any(|value| value.norm() > 0.0))
    );
    assert_eq!(bits(&data(&one)), bits(&data(&two)));
}

/// A `corrupt` after `openfromms` adds its noise to the DATA on disk: a
/// prediction closed first, or the zeros `observe` wrote.
#[test]
fn a_corrupt_after_openfromms_adds_to_the_data_on_disk() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let model = extended_model(directory.path(), "model.image");
    let predict = |sm: &mut Simulator| sm.predict(&predict_spec(&model, false));
    let (one, two) = two_stage(directory.path(), &observed, &predict, &corrupt);
    assert_eq!(bits(&data(&one)), bits(&data(&two)));
    let noise_only = tempfile::tempdir().expect("temporary directory");
    let (one, two) = two_stage(noise_only.path(), &observed, &nothing, &corrupt);
    assert_eq!(bits(&data(&one)), bits(&data(&two)));
}

/// A replacing `predict` after `openfromms` replaces the DATA on disk, and
/// an incremental one adds to it, to rounding: one session sums the
/// predictions within its predictor.
#[test]
fn a_predict_after_openfromms_replaces_or_adds_to_the_data_on_disk() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let model = &extended_model(directory.path(), "model.image");
    let predict =
        |incremental| move |sm: &mut Simulator| sm.predict(&predict_spec(model, incremental));
    let (one, two) = two_stage(directory.path(), &observed, &corrupt, &predict(false));
    assert_eq!(bits(&data(&one)), bits(&data(&two)));
    let incremental = tempfile::tempdir().expect("temporary directory");
    let (one, two) = two_stage(
        incremental.path(),
        &observed,
        &predict(false),
        &predict(true),
    );
    let nrms = normalized_rms(&two, &one);
    assert!(nrms < 1e-6, "NRMS {nrms}");
}

/// The RMS of `found − expected`'s DATA over the RMS of `expected`'s,
/// which must have power.
fn normalized_rms(found: &[predict_support::Row], expected: &[predict_support::Row]) -> f64 {
    let (mut difference, mut norm) = (0.0_f64, 0.0_f64);
    for (found, expected) in found.iter().zip(expected) {
        for (x, y) in found.data.iter().zip(&expected.data) {
            difference += f64::from((x - y).norm_sqr());
            norm += f64::from(y.norm_sqr());
        }
    }
    assert!(norm > 0.0, "the expected DATA has power");
    (difference / norm).sqrt()
}

/// The `wproject` machine, and the heterogeneous mosaic machine, predict
/// after `openfromms` as in the session that observed.
#[test]
fn wproject_and_the_mosaic_predict_after_openfromms_as_before() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let model = &extended_model(directory.path(), "model.image");
    let with = |ftmachine: &'static str| {
        move |sm: &mut Simulator| {
            sm.set_options(&PredictionOptions {
                ftmachine: ftmachine.to_string(),
                w_planes: 4,
                ..PredictionOptions::default()
            })?;
            sm.predict(&predict_spec(model, false))
        }
    };
    let wproject = directory.path().join("wproject");
    std::fs::create_dir(&wproject).expect("directory");
    let (one, two) = two_stage(&wproject, &observed, &nothing, &with("wproject"));
    assert_eq!(bits(&data(&one)), bits(&data(&two)));

    let heterogeneous = |path: &Path| {
        let mut sm = session_with_dishes(
            path,
            "ALMA",
            "XX YY",
            CHANNELS,
            &[12.0, 7.0, 12.0, 7.0, 12.0, 7.0, 12.0, 7.0],
            resources(2, 2 << 30),
        );
        sm.observe(&scan("target", 0.0, 60.0)).expect("observe");
        sm.observe(&scan("offset", 60.0, 120.0)).expect("observe");
        sm
    };
    let mosaic = directory.path().join("mosaic");
    std::fs::create_dir(&mosaic).expect("directory");
    let (one, two) = two_stage(&mosaic, &heterogeneous, &nothing, &with("mosaic"));
    assert_eq!(bits(&data(&one)), bits(&data(&two)));
}

/// What adds to the MeasurementSet is not yet implemented after
/// `openfromms`, and a session with no DATA step leaves it as it was.
#[test]
fn openfromms_refuses_additions_and_writes_nothing_without_a_step() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("observed.ms");
    observed(&path).close().expect("close");
    let before = read_main(&path).0;
    let mut sm = Simulator::open_from_ms(&path, options()).expect("openfromms");
    assert_eq!(sm.scan_count(), 2, "one scan per field");
    let refused = sm
        .observe(&scan("target", 120.0, 180.0))
        .expect_err("observe");
    assert_eq!(refused.outcome(), CasaOutcome::NotYetImplemented);
    assert!(
        refused.message().contains("observe after openfromms"),
        "{refused}"
    );
    sm.close().expect("close");
    assert_eq!(bits(&data(&read_main(&path).0)), bits(&data(&before)));
    let missing = Simulator::open_from_ms(directory.path().join("missing.ms"), options())
        .err()
        .expect("no MeasurementSet");
    assert_eq!(missing.outcome(), CasaOutcome::Raises);
}

/// The session holds the MeasurementSet `openfromms` opened by its
/// absolute path, as CASA's simulator holds its table and names it: `close`
/// patches that MeasurementSet whatever the working directory is by then
/// (#886 review).
#[test]
fn openfromms_holds_the_measurement_set_by_its_absolute_path() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("observed.ms");
    observed(&path).close().expect("close");
    // `path` from the working directory, which the tests share and so keep.
    let here = std::env::current_dir().expect("the working directory");
    let relative = std::iter::repeat_n("..", here.components().count() - 1)
        .collect::<std::path::PathBuf>()
        .join(path.strip_prefix("/").expect("an absolute path"));
    assert!(relative.is_relative());
    let mut sm = Simulator::open_from_ms(&relative, options()).expect("openfromms");
    assert!(sm.name().is_absolute(), "{}", sm.name().display());
    assert_eq!(
        std::fs::canonicalize(sm.name()).expect("the session's MeasurementSet"),
        std::fs::canonicalize(&path).expect("the MeasurementSet")
    );
    corrupt(&mut sm).expect("corrupt");
    sm.close().expect("close");
    assert!(
        read_main(&path)
            .0
            .iter()
            .any(|row| row.data.iter().any(|value| value.norm() > 0.0)),
        "the opened MeasurementSet is patched"
    );
}

/// Rewrite `path`'s `subtable` with `rewrite(table, row)` for each row, and
/// save it.
fn rewrite_subtable(path: &Path, subtable: &str, rewrite: &dyn Fn(&mut Table, usize)) {
    let mut table = Table::open(TableOptions::new(path.join(subtable))).expect(subtable);
    for row in 0..table.row_count() {
        rewrite(&mut table, row);
    }
    table.flush().expect("save the subtable");
}

/// Set `table`'s `column` of row `row` to `value`.
fn set_cell(table: &mut Table, row: usize, column: &str, value: Value) {
    table
        .cell_accessor_mut(row, column)
        .and_then(|mut cell| cell.set(value))
        .expect(column);
}

/// A POINTING row is matched to the scans by the times casacore's
/// `MSPointingColumns::pointingIndex` takes it as valid for (#886 review):
/// within half its INTERVAL of its TIME; with INTERVAL 0, within half the
/// gap to its antenna's next row at another TIME, or at every time when
/// there is none; with a negative INTERVAL, at every time. A beam is refused
/// when a row pointing away from the field is valid during the scan, which
/// has two integrations, at `t1` and 10 s later.
#[test]
fn pointing_rows_apply_for_the_times_casacore_takes_them_as_valid() {
    use casa_simulation::{PredictSpec, VoltagePatternSpec};
    let directory = tempfile::tempdir().expect("temporary directory");
    let point = &directory.path().join("point.cl");
    write_point_list(point, predict_support::CENTRE_RAD, 1.0);
    let away = Value::Array(ArrayValue::Float64(
        ArrayD::from_shape_vec(
            IxDyn(&[2, 1]).f(),
            vec![
                predict_support::CENTRE_RAD[0] + 1.0e-5,
                predict_support::CENTRE_RAD[1],
            ],
        )
        .expect("a direction"),
    ));
    // The first integration's rows of each antenna point away, at `t1` and
    // `offset_s` with `interval_s`; when `both`, the second's too, at the
    // same time.
    let refused = |name: &str, offset_s: f64, interval_s: f64, both: bool| {
        let path = directory.path().join(name);
        let mut sm = session(&path, "ALMA", "XX YY", resources(2, 2 << 30));
        sm.observe(&scan("target", 0.0, 20.0)).expect("observe");
        sm.close().expect("close");
        let time = |table: &Table, row| match table
            .column_accessor("TIME")
            .and_then(|column| column.scalar_cell(row).cloned())
        {
            Ok(ScalarValue::Float64(time_s)) => time_s,
            other => panic!("POINTING TIME: {other:?}"),
        };
        let first = Table::open(TableOptions::new(path.join("POINTING"))).expect("POINTING");
        let t1 = (0..first.row_count())
            .map(|row| time(&first, row))
            .fold(f64::INFINITY, f64::min);
        drop(first);
        rewrite_subtable(&path, "POINTING", &|table, row| {
            if time(table, row) == t1 || both {
                let scalar = |value| Value::Scalar(ScalarValue::Float64(value));
                set_cell(table, row, "TIME", scalar(t1 + offset_s));
                set_cell(table, row, "INTERVAL", scalar(interval_s));
                set_cell(table, row, "DIRECTION", away.clone());
            }
        });
        let mut sm = Simulator::open_from_ms(&path, options()).expect("openfromms");
        sm.set_vp(&VoltagePatternSpec::default()).expect("setvp");
        match sm.predict(&PredictSpec {
            images: Vec::new(),
            component_list: Some(point.clone()),
            incremental: false,
        }) {
            Ok(()) => false,
            Err(refused) => {
                assert_eq!(refused.outcome(), CasaOutcome::NotYetImplemented);
                assert!(
                    refused.message().contains("POINTING points scan"),
                    "{refused}"
                );
                true
            }
        }
    };
    // Valid from t1 − 6 s to t1 + 4 s, and to t1 − 1 s.
    assert!(refused("covering.ms", -1.0, 10.0, false));
    assert!(!refused("before.ms", -6.0, 10.0, false));
    // INTERVAL 0: the next row, at t1 + 10 s, bounds the row to half the
    // gap: from t1 − 17 s to t1 + 1 s, and to t1 − 1 s.
    assert!(refused("bounded-covering.ms", -8.0, 0.0, false));
    assert!(!refused("bounded-before.ms", -12.0, 0.0, false));
    // Every row of an antenna at one time with INTERVAL 0, and a negative
    // INTERVAL: valid at every time.
    assert!(refused("unbounded.ms", -100.0, 0.0, true));
    assert!(refused("negative.ms", -100.0, -1.0, false));
}

/// Spectral window 0's REF_FREQUENCY picks the VLA's beam after
/// `openfromms`, as `VPSkyJones` picks it, whatever the channels are (#886
/// review). With the channels' 230 GHz, outside the VLA's bands, it is
/// VLA_NVSS, as in the session that observed; set to 40 GHz, in the Q band,
/// the VLA's common pattern predicts other DATA for a source off the axis.
#[test]
fn the_reference_frequency_picks_the_beam_after_openfromms() {
    use casa_simulation::{PredictSpec, VoltagePatternSpec};
    let directory = tempfile::tempdir().expect("temporary directory");
    let list = &directory.path().join("off-axis.cl");
    let centre = predict_support::CENTRE_RAD;
    write_point_list(list, [centre[0], centre[1] + 2.0e-5], 1.0);
    let vla = |path: &Path| {
        let mut sm = session(path, "VLA", "RR LL", resources(2, 2 << 30));
        sm.observe(&scan("target", 0.0, 60.0)).expect("observe");
        sm
    };
    let predict = move |sm: &mut Simulator| {
        sm.set_vp(&VoltagePatternSpec {
            do_squint: false,
            ..VoltagePatternSpec::default()
        })?;
        sm.predict(&PredictSpec {
            images: Vec::new(),
            component_list: Some(list.clone()),
            incremental: false,
        })
    };
    let (one, two) = two_stage(directory.path(), &vla, &nothing, &predict);
    assert_eq!(bits(&data(&one)), bits(&data(&two)));
    let q_band = directory.path().join("q-band.ms");
    vla(&q_band).close().expect("close");
    rewrite_subtable(&q_band, "SPECTRAL_WINDOW", &|table, row| {
        set_cell(
            table,
            row,
            "REF_FREQUENCY",
            Value::Scalar(ScalarValue::Float64(40.0e9)),
        );
    });
    let mut sm = Simulator::open_from_ms(&q_band, options()).expect("openfromms");
    predict(&mut sm).expect("predict");
    sm.close().expect("close");
    let nrms = normalized_rms(&read_main(&q_band).0, &one);
    assert!(
        nrms > 1e-4,
        "the Q band's pattern predicts other DATA: NRMS {nrms}"
    );
}

fn write_points(path: &Path) {
    use casa_components::{
        ComponentList, ComponentShape, Flux, SkyComponent, SpectralKind, SpectralModel,
    };
    use casa_types::measures::direction::{DirectionRef, MDirection};
    use casa_types::measures::frequency::{FrequencyRef, MFrequency};
    let mut list = ComponentList::new();
    for (north_rad, flux) in [(0.0, 1.0), (1.5e-5, 0.5)] {
        list.push(SkyComponent::new(
            Flux::stokes_jy(flux, 0.0, 0.0, 0.0),
            ComponentShape::point(MDirection::from_angles(
                predict_support::CENTRE_RAD[0],
                predict_support::CENTRE_RAD[1] + north_rad,
                DirectionRef::J2000,
            )),
            SpectralModel::new(
                MFrequency::new(predict_support::FREQUENCY_HZ, FrequencyRef::LSRK),
                SpectralKind::Constant,
            ),
        ));
    }
    list.write_table(path).expect("write the list");
}

/// A model image and a component list under the primary beam, and total
/// power, predict after `openfromms` as in the session that observed.
#[test]
fn components_under_the_beam_and_total_power_predict_after_openfromms_as_before() {
    use casa_simulation::{PredictSpec, VoltagePatternSpec};
    let directory = tempfile::tempdir().expect("temporary directory");
    let model = &extended_model(directory.path(), "model.image");
    let points = &directory.path().join("points.cl");
    write_points(points);
    let both = move |sm: &mut Simulator| {
        sm.set_vp(&VoltagePatternSpec::default())?;
        sm.predict(&PredictSpec {
            images: vec![model.clone()],
            component_list: Some(points.clone()),
            incremental: false,
        })?;
        corrupt(sm)
    };
    let components = directory.path().join("components");
    std::fs::create_dir(&components).expect("directory");
    let (one, two) = two_stage(&components, &observed, &nothing, &both);
    assert_eq!(bits(&data(&one)), bits(&data(&two)));

    let total_power = move |sm: &mut Simulator| {
        sm.set_options(&PredictionOptions {
            ftmachine: "sd".to_string(),
            grid_function: "PB".to_string(),
            ..PredictionOptions::default()
        })?;
        sm.predict(&predict_spec(model, false))
    };
    let single_dish = directory.path().join("sd");
    std::fs::create_dir(&single_dish).expect("directory");
    let (one, two) = two_stage(&single_dish, &observed, &nothing, &total_power);
    assert!(
        one.iter()
            .any(|row| row.data.iter().any(|value| value.norm() > 0.0))
    );
    assert_eq!(bits(&data(&one)), bits(&data(&two)));
}

/// A pointing `observemany` moved off its field keeps its field's FIELD
/// row, and is phased there, as CASA's `VisBuffer` phases it, both in the
/// session that observed it and after `openfromms` (#884): a source at the
/// field's direction predicts zero phase in both pointings, as it does in
/// CASA 6.7.6 (measured), although the moved pointing's UVW were computed
/// at the moved direction. A primary beam after `openfromms`, centred on
/// POINTING's direction, is refused.
#[test]
fn a_moved_pointing_is_phased_on_its_field_after_openfromms() {
    use casa_simulation::{ObserveManySpec, Pointing, PredictSpec, VoltagePatternSpec};
    use casa_types::measures::direction::{DirectionRef, MDirection};
    let directory = tempfile::tempdir().expect("temporary directory");
    let point = &directory.path().join("point.cl");
    write_point_list(point, predict_support::CENTRE_RAD, 1.0);
    let moved = |path: &Path| {
        let mut sm = session(path, "ALMA", "XX YY", resources(2, 2 << 30));
        let base = scan("target", 0.0, 20.0);
        let direction = MDirection::from_angles(
            predict_support::CENTRE_RAD[0] + 1.0e-5,
            predict_support::CENTRE_RAD[1],
            DirectionRef::J2000,
        );
        sm.observe_many(&ObserveManySpec {
            pointings: vec![
                Pointing {
                    source: "target".to_string(),
                    start_s: 0.0,
                    stop_s: 20.0,
                    direction: None,
                },
                Pointing {
                    source: "target".to_string(),
                    start_s: 20.0,
                    stop_s: 40.0,
                    direction: Some(direction),
                },
            ],
            spectral_window: base.spectral_window,
            add_observation: false,
            state: base.state,
            observer: base.observer,
            project: base.project,
        })
        .expect("observemany");
        sm
    };
    let predict = move |sm: &mut Simulator| {
        sm.predict(&PredictSpec {
            images: Vec::new(),
            component_list: Some(point.clone()),
            incremental: false,
        })
    };
    let (one, two) = two_stage(directory.path(), &moved, &nothing, &predict);
    assert_eq!(bits(&data(&one)), bits(&data(&two)));
    for row in &two {
        for value in &row.data {
            assert!((value.re - 1.0).abs() < 1e-6, "{value}");
            assert!(value.im.abs() < 1e-6, "{value}");
        }
    }

    let mut sm =
        Simulator::open_from_ms(directory.path().join("two.ms"), options()).expect("openfromms");
    sm.set_vp(&VoltagePatternSpec::default()).expect("setvp");
    let refused = predict(&mut sm).expect_err("the beam needs POINTING at the phase centres");
    assert_eq!(refused.outcome(), CasaOutcome::NotYetImplemented);
    assert!(
        refused.message().contains("POINTING points scan"),
        "{refused}"
    );
}

/// Install MODEL_DATA and CORRECTED_DATA in `path`'s MAIN as copies of
/// DATA, as CASA's `VisSet` adds its scratch columns.
fn add_scratch_columns(path: &Path) {
    use casa_ms::{
        MeasurementSet, MeasurementSetColumnStorage, MeasurementSetColumnWriteMode,
        MeasurementSetWriteColumnPlan, MeasurementSetWritePlan, MeasurementSetWriteResources,
        MeasurementSetWriteSession,
    };
    let mut ms = MeasurementSet::open(path).expect("open");
    let columns = ["MODEL_DATA", "CORRECTED_DATA"];
    for name in columns {
        ms.main_table_mut()
            .add_column(
                casa_tables::ColumnSchema::array_variable(
                    name,
                    casa_types::PrimitiveType::Complex32,
                    Some(2),
                ),
                None,
            )
            .expect("add the column to the schema");
    }
    let plan = MeasurementSetWritePlan::selected_row_mutation(
        vec![0],
        columns
            .iter()
            .map(|name| MeasurementSetWriteColumnPlan {
                name: (*name).to_string(),
                bytes_per_row: 512,
                mode: MeasurementSetColumnWriteMode::Create,
                storage_manager: MeasurementSetColumnStorage::TiledShape,
                tile_shape: None,
                create_source_column: Some("DATA".to_string()),
            })
            .collect(),
        MeasurementSetWriteResources {
            available_bytes: 1024,
            maximum_live_batches: 1,
            tiled_column_buffer_bytes: 0,
        },
    )
    .expect("plan");
    drop(MeasurementSetWriteSession::start_selected_row_mutation(&mut ms, plan).expect("install"));
}

/// MAIN's `column` of `path`, each cell `[channel][correlation]`.
fn read_column(path: &Path, column: &str) -> Vec<Vec<casa_types::Complex32>> {
    use casa_ms::MeasurementSet;
    use casa_ms::columns::data_columns::DataColumn;
    let ms = MeasurementSet::open(path).expect("open");
    let main = ms.main_table();
    let cells = match column {
        "MODEL_DATA" => DataColumn::model_data(main),
        "CORRECTED_DATA" => DataColumn::corrected_data(main),
        _ => DataColumn::data(main),
    }
    .expect("the column");
    (0..ms.row_count())
        .map(|row| {
            let ArrayValue::Complex32(cell) = cells.get(row).expect("a cell") else {
                panic!("{column} is not Complex32");
            };
            cell.t().iter().copied().collect()
        })
        .collect()
}

/// MODEL_DATA and CORRECTED_DATA, where MAIN has them, are left as CASA's
/// steps leave them: CORRECTED_DATA the new DATA after any step, MODEL_DATA
/// 1 after a `predict` and as it was after a `corrupt`. A MeasurementSet
/// without them gets none.
#[test]
fn the_scratch_columns_follow_data_and_none_is_created() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let model = &extended_model(directory.path(), "model.image");
    let reopened = |name: &str, steps: &dyn Fn(&mut Simulator) -> SimulatorResult<()>| {
        let path = directory.path().join(name);
        let mut sm = observed(&path);
        sm.predict(&predict_spec(model, false)).expect("predict");
        sm.close().expect("close");
        add_scratch_columns(&path);
        let mut sm = Simulator::open_from_ms(&path, options()).expect("openfromms");
        steps(&mut sm).expect("steps");
        sm.close().expect("close");
        path
    };
    let predicted = reopened("predicted.ms", &|sm| {
        sm.predict(&predict_spec(model, false))?;
        corrupt(sm)
    });
    let data = read_column(&predicted, "DATA");
    assert_eq!(
        bits(&read_column(&predicted, "CORRECTED_DATA")),
        bits(&data)
    );
    let one = casa_types::Complex32::new(1.0, 0.0);
    assert!(
        read_column(&predicted, "MODEL_DATA")
            .iter()
            .flatten()
            .all(|value| *value == one)
    );

    let corrupted = reopened("corrupted.ms", &corrupt);
    let data = read_column(&corrupted, "DATA");
    assert_eq!(
        bits(&read_column(&corrupted, "CORRECTED_DATA")),
        bits(&data)
    );
    // MODEL_DATA is still the copy of the DATA predicted before.
    let predicted_first = directory.path().join("first.ms");
    let mut sm = observed(&predicted_first);
    sm.predict(&predict_spec(model, false)).expect("predict");
    sm.close().expect("close");
    assert_eq!(
        bits(&read_column(&corrupted, "MODEL_DATA")),
        bits(&read_column(&predicted_first, "DATA"))
    );

    let path = directory.path().join("bare.ms");
    observed(&path).close().expect("close");
    let mut sm = Simulator::open_from_ms(&path, options()).expect("openfromms");
    corrupt(&mut sm).expect("corrupt");
    sm.close().expect("close");
    let ms = casa_ms::MeasurementSet::open(&path).expect("open");
    let schema = ms.main_table().schema().expect("schema");
    assert!(!schema.contains_column("MODEL_DATA") && !schema.contains_column("CORRECTED_DATA"));
}

/// A row with a flagged sample but no FLAG_ROW is refused before anything
/// is written: the MeasurementSet is left as it was.
#[test]
fn a_partly_flagged_row_is_refused_before_anything_is_written() {
    use casa_ms::{
        MeasurementSet, MeasurementSetColumnStorage, MeasurementSetColumnWriteMode,
        MeasurementSetMutationBatch, MeasurementSetMutationColumnBatch,
        MeasurementSetMutationColumnValues, MeasurementSetWriteColumnPlan, MeasurementSetWritePlan,
        MeasurementSetWriteResources, MeasurementSetWriteSession,
    };
    let directory = tempfile::tempdir().expect("temporary directory");
    let model = &extended_model(directory.path(), "model.image");
    let path = directory.path().join("flagged.ms");
    let mut sm = observed(&path);
    sm.predict(&predict_spec(model, false)).expect("predict");
    sm.close().expect("close");
    // Flag one sample of row 3, not the row.
    let mut ms = MeasurementSet::open(&path).expect("open");
    let plan = MeasurementSetWritePlan::selected_row_mutation(
        vec![3],
        vec![MeasurementSetWriteColumnPlan {
            name: "FLAG".to_string(),
            bytes_per_row: 2 * CHANNELS,
            mode: MeasurementSetColumnWriteMode::Replace,
            storage_manager: MeasurementSetColumnStorage::Persisted,
            tile_shape: None,
            create_source_column: None,
        }],
        MeasurementSetWriteResources {
            available_bytes: 1024,
            maximum_live_batches: 1,
            tiled_column_buffer_bytes: 0,
        },
    )
    .expect("plan");
    let mut session =
        MeasurementSetWriteSession::start_selected_row_mutation(&mut ms, plan).expect("start");
    let mut flags = vec![false; 2 * CHANNELS];
    flags[1] = true;
    session
        .write_mutation_batch(
            &mut ms,
            MeasurementSetMutationBatch {
                row_indices: vec![3],
                columns: vec![MeasurementSetMutationColumnBatch {
                    name: "FLAG".to_string(),
                    values: MeasurementSetMutationColumnValues::Arrays(vec![ArrayValue::Bool(
                        ArrayD::from_shape_vec(IxDyn(&[2, CHANNELS]).f(), flags)
                            .expect("FLAG cell"),
                    )]),
                }],
            },
        )
        .expect("flag a sample");
    session.finish_mutation().expect("finish");
    drop(ms);

    let before = read_column(&path, "DATA");
    let mut sm = Simulator::open_from_ms(&path, options()).expect("openfromms");
    corrupt(&mut sm).expect("corrupt");
    let refused = sm.close().expect_err("refused at close");
    assert_eq!(refused.outcome(), CasaOutcome::NotYetImplemented);
    assert!(refused.message().contains("MAIN row 3"), "{refused}");
    assert_eq!(bits(&read_column(&path, "DATA")), bits(&before));
}
