// SPDX-License-Identifier: LGPL-3.0-or-later
//! Creating a MeasurementSet from observations (#937 PR B): a plan checked
//! before anything is written, then written. Converting into an MSv2 is a
//! plan over one input, so the converter's laws (`convert.rs`) are the
//! one-input laws; these add what a plan says of itself.

use std::path::{Path, PathBuf};

use casa_ms::observation_io::convert::{ConversionOptions, to_msv2};
use casa_ms::observation_io::creation::{
    CreationInput, CreationOptions, CreationPlan, Destination, HistoryRow, Loss, LossKind,
    PaddingRows,
};
use casa_ms::observation_io::{Msv4Codec, OpenOptions, open};
use casa_ms::{MeasurementSet, SubtableId};
use casa_observation::{Catalogue, MergeTolerance, ObservationError, SourceWindow, WindowId};
use casa_tables::table_measures::{MeasureType, TableMeasDesc};
use casa_tables::{ColumnSchema, Table};
use casa_types::measures::MEpoch;
use casa_types::measures::direction::MDirection;
use casa_types::{ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value};
use ndarray::ArrayD;

use super::convert::rows_and_sigmas;
use super::fixture;

/// Every loss the trial fixture takes into an MSv2.
const ACCEPT: [LossKind; 2] = [LossKind::Sigma, LossKind::Tables];

fn options() -> CreationOptions {
    CreationOptions {
        data_groups: None,
        accept: ACCEPT.to_vec(),
        batch_bytes: 1 << 20,
        padding: PaddingRows::Flagged,
        tolerance: MergeTolerance::default(),
        history: Vec::new(),
    }
}

/// The catalogue of the store at `path`, printed whole for comparison.
fn catalogue(path: &Path) -> String {
    let observation = open(path, &OpenOptions::default()).expect("open");
    format!("{:?}", observation.catalogue())
}

/// A plan over one input writes the MeasurementSet `to_msv2` writes, and
/// says before writing every loss its report then lists.
#[test]
fn a_plan_over_one_input_writes_what_converting_writes() {
    let root = tempfile::tempdir().expect("root");
    let v2 = fixture(root.path(), "ms_trial.ms");
    let (planned, converted) = (
        root.path().join("planned.ms"),
        root.path().join("converted.ms"),
    );
    let plan = CreationPlan::new(&[CreationInput::new(&v2)], &options()).expect("plan");
    let losses = plan.losses().to_vec();
    assert!(losses.contains(&Loss::SigmaDerived), "{losses:?}");
    assert!(!planned.exists(), "a plan writes nothing");
    let report = plan.write(&planned, Destination::New).expect("write");
    assert_eq!(report.losses, losses);
    let conversion = to_msv2(
        &v2,
        &converted,
        &ConversionOptions {
            data_groups: None,
            accept: ACCEPT.to_vec(),
            batch_bytes: 1 << 20,
            workers: 1,
            chunks: [0; 4],
            codec: Msv4Codec::BloscLz4,
            padding: PaddingRows::Flagged,
            creator: "casa-rs".to_string(),
            creation_date: "2026-10-29T00:00:00+00:00".to_string(),
        },
    )
    .expect("convert");
    assert_eq!(
        (report.partitions, report.samples, report.rows),
        (conversion.partitions, conversion.samples, conversion.rows)
    );
    assert_eq!(rows_and_sigmas(&planned), rows_and_sigmas(&converted));
    assert_eq!(catalogue(&planned), catalogue(&converted));
}

/// An input's weight scale multiplies every `WEIGHT`, and `SIGMA` is derived
/// from the scaled weight: `1/√(W·w)`.
#[test]
fn weights_are_scaled_and_sigmas_follow_them() {
    let root = tempfile::tempdir().expect("root");
    let v2 = fixture(root.path(), "ms_trial.ms");
    let write = |name: &str, weight_scale: f64| {
        let path = root.path().join(name);
        let input = CreationInput {
            path: v2.clone(),
            weight_scale,
        };
        CreationPlan::new(&[input], &options())
            .and_then(|plan| plan.write(&path, Destination::New))
            .expect("write");
        rows_and_sigmas(&path)
    };
    let (rows, unscaled) = write("unscaled.ms", 1.0);
    let (scaled_rows, scaled) = write("scaled.ms", 4.0);
    assert_eq!(rows, scaled_rows);
    assert!(!unscaled.is_empty());
    for ((weight, sigma), (scaled_weight, scaled_sigma)) in unscaled.iter().zip(&scaled) {
        assert_eq!(scaled_weight.to_bits(), (weight * 4.0).to_bits());
        if *weight > 0.0 {
            assert_eq!(
                scaled_sigma.to_bits(),
                (1.0 / (weight * 4.0).sqrt()).to_bits()
            );
            assert!((scaled_sigma - sigma / 2.0).abs() <= f32::EPSILON * sigma);
        } else {
            assert_eq!(
                *scaled_sigma, -1.0,
                "no sigma where the weight is not positive"
            );
        }
    }
}

/// A weight scale that is not finite and positive is refused by the plan.
#[test]
fn weight_scales_must_be_finite_and_positive() {
    let root = tempfile::tempdir().expect("root");
    let v2 = fixture(root.path(), "ms_trial.ms");
    for weight_scale in [0.0, -1.0, f64::NAN, f64::INFINITY, 1e300] {
        let input = CreationInput {
            path: v2.clone(),
            weight_scale,
        };
        assert!(
            matches!(
                CreationPlan::new(&[input], &options()),
                Err(ObservationError::InvalidRequest { .. })
            ),
            "{weight_scale}"
        );
    }
    assert!(matches!(
        CreationPlan::new(&[], &options()),
        Err(ObservationError::InvalidRequest { .. })
    ));
}

/// A new MeasurementSet is never written over an existing path, which is
/// left as it was.
#[test]
fn an_existing_destination_is_refused_and_kept() {
    let root = tempfile::tempdir().expect("root");
    let v2 = fixture(root.path(), "ms_trial.ms");
    let destination = root.path().join("taken.ms");
    std::fs::create_dir(&destination).expect("taken");
    std::fs::write(destination.join("keep"), b"kept").expect("keep");
    let plan = CreationPlan::new(&[CreationInput::new(&v2)], &options()).expect("plan");
    assert!(plan.write(&destination, Destination::New).is_err());
    assert_eq!(
        std::fs::read(destination.join("keep")).expect("kept"),
        b"kept"
    );
    assert_eq!(std::fs::read_dir(&destination).expect("dir").count(), 1);
}

fn int(value: i32) -> Value {
    Value::Scalar(ScalarValue::Int32(value))
}

fn float(value: f64) -> Value {
    Value::Scalar(ScalarValue::Float64(value))
}

fn text(value: &str) -> Value {
    Value::Scalar(ScalarValue::String(value.to_string()))
}

fn floats(values: &[f64], shape: Vec<usize>) -> Value {
    Value::Array(ArrayValue::Float64(
        ArrayD::from_shape_vec(shape, values.to_vec()).expect("shape"),
    ))
}

fn texts(values: &[&str]) -> Value {
    Value::Array(ArrayValue::String(
        ArrayD::from_shape_vec(
            vec![values.len()],
            values.iter().map(ToString::to_string).collect(),
        )
        .expect("shape"),
    ))
}

fn set(table: &mut Table, row: usize, cells: &[(&str, Value)]) {
    for (column, value) in cells {
        table
            .row_accessor_mut()
            .set_cell(row, column, value.clone())
            .unwrap_or_else(|error| panic!("{column}: {error}"));
    }
}

fn subtable(ms: &mut MeasurementSet, id: SubtableId) -> &mut Table {
    ms.subtable_mut(id).unwrap_or_else(|| panic!("{id:?}"))
}

/// The row of `table` (`SOURCE`) for source `id` in window `window`.
fn source_row(table: &Table, id: i32, window: i32) -> usize {
    let rows = table.row_accessor();
    (0..rows.row_count())
        .find(|row| {
            let record = rows.row(*row).expect("row");
            record.get("SOURCE_ID") == Some(&int(id))
                && record.get("SPECTRAL_WINDOW_ID") == Some(&int(window))
        })
        .expect("a SOURCE row")
}

/// Losses the CASA test MS takes into an MSv2: its SIGMA, row flags,
/// ASDM and other subtables, and the columns the catalogue does not carry.
const CASA_LOSSES: [LossKind; 6] = [
    LossKind::Sigma,
    LossKind::RowFlags,
    LossKind::FlagCategories,
    LossKind::DataGroups,
    LossKind::Tables,
    LossKind::Columns,
];

/// CASA's ALMA test MS (`mssel_test_small.ms`, its partitions in windows 1
/// and 2; window 0, the WVR's, holds no rows), its `SOURCE` given lines:
/// source 0's record in window 1 one line, and source 1 a record for every
/// window of two lines. Its first state, an antenna, a field's reference
/// centre and the session's release date are given values no default has.
fn stated_msv2(root: &Path) -> PathBuf {
    let archive =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/mssel_test_small.ms.tgz");
    tar::Archive::new(flate2::read::GzDecoder::new(
        std::fs::File::open(archive).expect("fixture"),
    ))
    .unpack(root)
    .expect("unpack");
    let path = root.join("mssel_test_small.ms");
    let mut ms = MeasurementSet::open(&path).expect("ms");
    set(
        subtable(&mut ms, SubtableId::State),
        0,
        &[("CAL", float(1.5)), ("LOAD", float(290.0))],
    );
    set(
        subtable(&mut ms, SubtableId::Field),
        1,
        &[("REFERENCE_DIR", floats(&[3.7, -1.3], vec![2, 1]))],
    );
    set(
        subtable(&mut ms, SubtableId::Observation),
        0,
        &[("RELEASE_DATE", float(5.0e9))],
    );
    set(
        subtable(&mut ms, SubtableId::Antenna),
        1,
        &[("FLAG_ROW", Value::Scalar(ScalarValue::Bool(true)))],
    );
    let source = subtable(&mut ms, SubtableId::Source);
    for (name, data_type) in [
        ("REST_FREQUENCY", PrimitiveType::Float64),
        ("SYSVEL", PrimitiveType::Float64),
        ("TRANSITION", PrimitiveType::String),
    ] {
        if !source
            .schema()
            .is_some_and(|schema| schema.contains_column(name))
        {
            source
                .add_column(ColumnSchema::array_variable(name, data_type, Some(1)), None)
                .expect("line column");
        }
    }
    TableMeasDesc::new_fixed("REST_FREQUENCY", MeasureType::Frequency, "BARY")
        .write(source)
        .expect("REST_FREQUENCY MEASINFO");
    TableMeasDesc::new_fixed("SYSVEL", MeasureType::RadialVelocity, "LSRK")
        .write(source)
        .expect("SYSVEL MEASINFO");
    let row = source_row(source, 0, 1);
    set(
        source,
        row,
        &[
            ("CODE", text("S")),
            ("CALIBRATION_GROUP", int(2)),
            ("NUM_LINES", int(1)),
            ("REST_FREQUENCY", floats(&[230.5e9], vec![1])),
            ("SYSVEL", floats(&[1500.0], vec![1])),
            ("TRANSITION", texts(&["CO 2-1"])),
        ],
    );
    let row = source_row(source, 1, 1);
    let mut every_window: Vec<RecordField> = source
        .row_accessor()
        .row(row)
        .expect("row")
        .fields()
        .iter()
        .filter(|field| !["REST_FREQUENCY", "SYSVEL", "TRANSITION"].contains(&field.name.as_str()))
        .cloned()
        .collect();
    for field in &mut every_window {
        match field.name.as_str() {
            "SPECTRAL_WINDOW_ID" => field.value = int(-1),
            "NUM_LINES" => field.value = int(2),
            _ => {}
        }
    }
    every_window.push(RecordField::new(
        "REST_FREQUENCY",
        floats(&[230.5e9, 220.4e9], vec![2]),
    ));
    every_window.push(RecordField::new(
        "TRANSITION",
        texts(&["CO 2-1", "13CO 2-1"]),
    ));
    source
        .add_row(RecordValue::new(every_window))
        .expect("an every-window record");
    ms.save().expect("save");
    path
}

/// The descriptive members of `catalogue` creation writes, printed for
/// comparison, with each window as `window` renumbers it (`None` for one
/// not written): epochs as whole MJD seconds and directions to a picoradian,
/// which their writing keeps to within rounding, and each partition with
/// its first state.
fn described(catalogue: &Catalogue, window: &dyn Fn(WindowId) -> Option<WindowId>) -> Vec<String> {
    let seconds = |epoch: &MEpoch| (epoch.value().as_mjd() * 86_400.0).round();
    let angles = |centre: &Option<MDirection>| {
        centre.as_ref().map(|centre| {
            let (longitude, latitude) = centre.as_angles();
            ((longitude * 1e12).round(), (latitude * 1e12).round())
        })
    };
    let mut described = vec![
        format!("{:?}", catalogue.states),
        format!("{:?}", catalogue.processors),
    ];
    for (index, entry) in catalogue.windows.iter().enumerate() {
        if let Some(id) = window(WindowId(index as u32)) {
            described.push(format!("{id:?} {entry:?}"));
        }
    }
    for entry in &catalogue.partitions {
        described.push(format!(
            "{:?} {:?} {:?} {:?}",
            window(entry.window),
            entry.states.first(),
            entry.processor,
            entry.intents
        ));
    }
    for antenna in &catalogue.antennas {
        described.push(format!("{} {:?}", antenna.name, antenna.flagged));
    }
    for field in &catalogue.fields {
        described.push(format!(
            "{} {:?} {:?} {:?} {:?} {:?}",
            field.name,
            field.code,
            field.flagged,
            field.source,
            angles(&field.delay_centre),
            angles(&field.reference_centre),
        ));
    }
    for observation in &catalogue.observations {
        described.push(format!(
            "{:?} {:?} {:?}",
            observation.flagged,
            observation.release_date.as_ref().map(seconds),
            observation
                .time_range
                .as_ref()
                .map(|range| range.each_ref().map(seconds)),
        ));
    }
    for source in &catalogue.sources {
        for record in &source.records {
            let written = match record.window {
                SourceWindow::All => Some(SourceWindow::All),
                SourceWindow::Window(id) => window(id).map(SourceWindow::Window),
            };
            let Some(written) = written else { continue };
            described.push(format!(
                "{} {written:?} {:?} {:?} {:?} {:?} {} {:?} {:?} {:?} {:?} {:?}",
                source.name,
                record.code,
                record.calibration_group,
                angles(&record.direction),
                record.proper_motion_rad_s,
                record.num_lines,
                record.transitions,
                record.rest_frequencies,
                record.systemic_velocities,
                record.interval_s,
                record.time.as_ref().map(seconds),
            ));
        }
    }
    described
}

/// Every descriptive member an MSv2 states survives creation: its states,
/// processors, windows, fields' codes, centres and flags, the session's
/// release date and extent, antennas' flags, and each logical source's
/// records with their lines, the every-window record included; nothing is
/// written as a default. What an MSv2 cannot hold is a loss: the WVR
/// window, which no partition uses and whose channels no partition holds,
/// is not written, so the others are renumbered and its source records go;
/// and rows of one partition using two states are written with the first.
#[test]
fn an_msv2s_descriptive_metadata_survives_creation() {
    let root = tempfile::tempdir().expect("root");
    let stated = stated_msv2(root.path());
    let created = root.path().join("created.ms");
    let options = CreationOptions {
        accept: CASA_LOSSES.to_vec(),
        ..options()
    };
    let report = CreationPlan::new(&[CreationInput::new(&stated)], &options)
        .and_then(|plan| plan.write(&created, Destination::New))
        .expect("create");
    assert!(
        !report
            .losses
            .iter()
            .any(|loss| matches!(loss, Loss::Defaulted(_))),
        "{:?}",
        report.losses
    );
    for loss in [
        Loss::Table("SPECTRAL_WINDOW rows no partition uses (1)".to_string()),
        Loss::Table("SOURCE records of spectral windows not written (2)".to_string()),
        Loss::Column(
            "STATE_ID of rows using several states in one partition (each is written with the \
             first)"
                .to_string(),
        ),
    ] {
        assert!(
            report.losses.contains(&loss),
            "{loss:?}: {:?}",
            report.losses
        );
    }
    let open = |path: &Path| open(path, &OpenOptions::default()).expect("open");
    let (input, output) = (open(&stated), open(&created));
    let (input, output) = (input.catalogue(), output.catalogue());
    assert_eq!(input.sources.len(), 2);
    assert_eq!(input.sources[1].records.len(), 4, "{:?}", input.sources);
    assert!(input.partitions.iter().any(|entry| entry.states.len() == 2));
    assert!(
        output
            .partitions
            .iter()
            .all(|entry| entry.states.len() == 1)
    );
    let renumbered = |id: WindowId| id.0.checked_sub(1).map(WindowId);
    let (written, stated) = (described(output, &Some), described(input, &renumbered));
    for (written, stated) in written.iter().zip(&stated) {
        assert_eq!(written, stated);
    }
    assert_eq!(written.len(), stated.len());
}

/// A partition's content as reopening states it, for comparison with
/// what a plan said it would be.
fn partitions(catalogue: &Catalogue) -> Vec<String> {
    catalogue
        .partitions
        .iter()
        .map(|entry| {
            format!(
                "{:?} {} {:?} {:?} {:?} {:?} {:?} {:?} {:?} {:?} {:?}",
                entry.window,
                entry.spectral_window,
                entry.correlations,
                entry.intents,
                entry.observation,
                entry.fields,
                entry.scans,
                entry.baselines,
                entry.states,
                entry.processor,
                entry.frequencies_hz,
            )
        })
        .collect()
}

/// The trial MSv2, its times in three scans, split by time into two inputs
/// (scan 2 in both) is created whole again: its partitions combine into
/// one, every acquisition is the original's sample for sample, scans
/// unchanged since the session keeps its ID, and reopening gives the
/// entities and partitions the plan said it would.
#[test]
fn a_split_observation_is_created_whole_again() {
    let root = tempfile::tempdir().expect("root");
    let original = fixture(root.path(), "ms_trial.ms");
    super::three_scans(&original);
    let halves: Vec<PathBuf> = ["early", "late"]
        .iter()
        .zip([60..120, 0..60])
        .map(|(name, removed)| {
            let half = fixture(&root.path().join(name), "ms_trial.ms");
            super::three_scans(&half);
            let mut ms = MeasurementSet::open(&half).expect("half");
            ms.main_table_mut()
                .remove_rows(&removed.collect::<Vec<usize>>())
                .expect("remove");
            ms.save().expect("save");
            half
        })
        .collect();
    let whole = root.path().join("whole.ms");
    let options = CreationOptions {
        padding: PaddingRows::Omitted,
        history: vec![HistoryRow {
            time_s: 5.0e9,
            observation: -1,
            message: "taskname=concat".to_string(),
            priority: "NORMAL".to_string(),
            origin: "concat".to_string(),
            application: "casa-rs".to_string(),
            parameters: vec!["vis=['early', 'late']".to_string()],
            command: Vec::new(),
        }],
        ..options()
    };
    let inputs: Vec<CreationInput> = halves.iter().map(CreationInput::new).collect();
    let plan = CreationPlan::new(&inputs, &options).expect("plan");
    let promised = plan.output().clone();
    assert!(plan.notices().is_empty(), "{:?}", plan.notices());
    let report = plan.write(&whole, Destination::New).expect("write");
    assert_eq!(report.partitions, 2, "one partition of each input");
    let id = casa_observation::PartitionId(0);
    let reopened = open(&whole, &OpenOptions::default()).expect("reopen");
    assert_eq!(
        reopened.catalogue().partitions.len(),
        1,
        "the halves combine"
    );
    assert_eq!(
        super::convert::acquisitions(&whole, id, "base"),
        super::convert::acquisitions(&original, id, "base"),
        "every acquisition, with its scan"
    );
    assert_eq!(partitions(reopened.catalogue()), partitions(&promised));
    let every = |id: WindowId| Some(id);
    assert_eq!(
        described(reopened.catalogue(), &every),
        described(&promised, &every)
    );
    let mut ms = MeasurementSet::open(&whole).expect("ms");
    let history = subtable(&mut ms, SubtableId::History).row_accessor();
    assert_eq!(history.row_count(), 1, "the task's row, not the inputs'");
    assert_eq!(
        history.row(0).expect("row").get("ORIGIN"),
        Some(&text("concat"))
    );
}

/// The trial MSv2 unpacked as `name`, its times in three scans, with the
/// rows `removed` taken out and then `edit` applied.
fn edited(
    root: &Path,
    name: &str,
    removed: std::ops::Range<usize>,
    edit: impl FnOnce(&mut MeasurementSet),
) -> PathBuf {
    let path = fixture(&root.join(name), "ms_trial.ms");
    super::three_scans(&path);
    let mut ms = MeasurementSet::open(&path).expect("ms");
    ms.main_table_mut()
        .remove_rows(&removed.collect::<Vec<usize>>())
        .expect("remove");
    edit(&mut ms);
    ms.save().expect("save");
    path
}

/// Plan `inputs` with padding omitted.
fn plan_of(inputs: &[&Path]) -> Result<CreationPlan, ObservationError> {
    let inputs: Vec<CreationInput> = inputs.iter().map(CreationInput::new).collect();
    CreationPlan::new(
        &inputs,
        &CreationOptions {
            padding: PaddingRows::Omitted,
            ..options()
        },
    )
}

fn main_ints(path: &Path, column: &str) -> Vec<i32> {
    let ms = MeasurementSet::open(path).expect("ms");
    let main = ms.main_table();
    (0..main.row_count())
        .map(
            |row| match main.row_accessor().row(row).expect("row").get(column) {
                Some(Value::Scalar(ScalarValue::Int32(value))) => *value,
                other => panic!("{column}: {other:?}"),
            },
        )
        .collect()
}

/// What a reopening could not hold is refused before anything is written:
/// a time and baseline both inputs acquire; the halves' intervals
/// differing; flagged padding where partitions combine.
#[test]
fn what_a_reopening_cannot_hold_is_refused() {
    let root = tempfile::tempdir().expect("root");
    let early = edited(root.path(), "early", 70..120, |_| {});
    let late = edited(root.path(), "late", 0..60, |_| {});
    let refused = plan_of(&[&early, &late]);
    assert!(
        matches!(&refused, Err(ObservationError::InvalidRequest { reason }) if reason.contains("also acquire")),
        "time 6 is in both: {}",
        refused
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default()
    );
    let early = edited(root.path(), "early-clean", 60..120, |_| {});
    let longer = edited(root.path(), "longer", 0..60, |ms| {
        ms.main_table_mut()
            .fill_column_range(
                "INTERVAL",
                casa_tables::RowRange::new(0, 60),
                Value::Scalar(ScalarValue::Float64(20.0)),
            )
            .expect("interval");
    });
    assert!(matches!(
        plan_of(&[&early, &longer]),
        Err(ObservationError::Unsupported { what }) if what.contains("intervals")
    ));
    let late = edited(root.path(), "late-clean", 0..60, |_| {});
    let flagged = CreationPlan::new(
        &[CreationInput::new(&early), CreationInput::new(&late)],
        &options(),
    );
    assert!(matches!(
        flagged,
        Err(ObservationError::InvalidRequest { .. })
    ));
}

/// One `OBS_MODE` whose state differs by its `CAL` combines into one
/// partition, each input's rows keeping its own state; a processor stated
/// by one input but not the other, under one key, is refused.
#[test]
fn states_combine_and_processors_must_agree() {
    let root = tempfile::tempdir().expect("root");
    let early = edited(root.path(), "early", 60..120, |_| {});
    let hotter = edited(root.path(), "hotter", 0..60, |ms| {
        set(subtable(ms, SubtableId::State), 0, &[("CAL", float(5.0))]);
    });
    let whole = root.path().join("whole.ms");
    plan_of(&[&early, &hotter])
        .and_then(|plan| plan.write(&whole, Destination::New))
        .expect("write");
    let reopened = open(&whole, &OpenOptions::default()).expect("reopen");
    let catalogue = reopened.catalogue();
    assert_eq!(catalogue.partitions.len(), 1);
    assert_eq!(catalogue.states.len(), 2);
    assert_eq!(catalogue.partitions[0].states.len(), 2);
    let states = main_ints(&whole, "STATE_ID");
    assert_eq!(&states[..60], &[0; 60]);
    assert_eq!(&states[60..], &[1; 60]);
    let processed = edited(root.path(), "processed", 0..60, |ms| {
        let table = subtable(ms, SubtableId::Processor);
        let row = casa_types::RecordValue::new(vec![
            RecordField::new("TYPE", text("CORRELATOR")),
            RecordField::new("SUB_TYPE", text("")),
            RecordField::new("TYPE_ID", int(-1)),
            RecordField::new("MODE_ID", int(-1)),
            RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
        ]);
        table.add_row(row).expect("processor");
    });
    assert!(matches!(
        plan_of(&[&early, &processed]),
        Err(ObservationError::Unsupported { what }) if what.contains("processors")
    ));
}

/// A second observing session's scans follow the first's, CASA's default
/// offset (its largest scan plus 1, less the incoming smallest), and its
/// partition is its own; each input's weights take its own scale.
#[test]
fn a_new_session_is_offset_and_weights_take_their_inputs_scale() {
    let root = tempfile::tempdir().expect("root");
    let early = edited(root.path(), "early", 60..120, |_| {});
    let other = edited(root.path(), "other", 0..60, |ms| {
        set(
            subtable(ms, SubtableId::Observation),
            0,
            &[("PROJECT", text("another"))],
        );
    });
    let whole = root.path().join("whole.ms");
    let inputs = [
        CreationInput::new(&early),
        CreationInput {
            path: other.clone(),
            weight_scale: 3.0,
        },
    ];
    let plan = CreationPlan::new(
        &inputs,
        &CreationOptions {
            padding: PaddingRows::Omitted,
            ..options()
        },
    )
    .expect("plan");
    plan.write(&whole, Destination::New).expect("write");
    let reopened = open(&whole, &OpenOptions::default()).expect("reopen");
    assert_eq!(reopened.catalogue().observations.len(), 2);
    assert_eq!(reopened.catalogue().partitions.len(), 2);
    let scans = main_ints(&whole, "SCAN_NUMBER");
    assert_eq!(
        (scans[..60].iter().max(), scans[60..].iter().min()),
        (Some(&2), Some(&3)),
        "early holds scans 1 and 2; the other's 2 and 3 become 3 and 4"
    );
    assert_eq!(scans[60..].iter().max(), Some(&4));
    assert_eq!(&main_ints(&whole, "OBSERVATION_ID")[60..], &[1; 60]);
    let (_, written) = rows_and_sigmas(&whole);
    let (_, own) = rows_and_sigmas(&other);
    let correlations = written.len() / 120;
    for ((weight, _), (stated, _)) in written[60 * correlations..].iter().zip(&own) {
        assert_eq!(weight.to_bits(), (stated * 3.0).to_bits());
    }
}
