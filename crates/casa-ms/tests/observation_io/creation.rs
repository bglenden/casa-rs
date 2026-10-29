// SPDX-License-Identifier: LGPL-3.0-or-later
//! Creating a MeasurementSet from observations (#937 PR B): a plan checked
//! before anything is written, then written. Converting into an MSv2 is a
//! plan over one input, so the converter's laws (`convert.rs`) are the
//! one-input laws; these add what a plan says of itself.

use std::collections::{BTreeMap, BTreeSet};
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
use casa_types::measures::position::MPosition;
use casa_types::{ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value};
use ndarray::ArrayD;

use super::convert::rows_and_sigmas;
use super::fixture;

/// Every loss the trial fixture takes into an MSv2: its SIGMA, its
/// HISTORY and its `OBSERVATION LOG` and `SCHEDULE_TYPE`.
const ACCEPT: [LossKind; 3] = [LossKind::Sigma, LossKind::Tables, LossKind::Columns];

fn options() -> CreationOptions {
    CreationOptions {
        data_groups: None,
        accept: ACCEPT.to_vec(),
        batch_bytes: 1 << 20,
        memory_bytes: 1 << 30,
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
    let plan = CreationPlan::new(
        &[CreationInput::new(&v2)],
        &options(),
        Destination::New(planned.clone()),
    )
    .expect("plan");
    let losses = plan.losses().to_vec();
    assert!(
        !losses.iter().any(|loss| loss.kind() == LossKind::Sigma),
        "the fixture's SIGMA is 1/√WEIGHT, so deriving it loses nothing: {losses:?}"
    );
    assert!(!planned.exists(), "a plan writes nothing");
    let report = plan.write().expect("write");
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
        CreationPlan::new(&[input], &options(), Destination::New(path.clone()))
            .and_then(CreationPlan::write)
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

/// Deriving SIGMA from the weights is a loss only where an input's stored
/// SIGMA is not already `1/√WEIGHT` to within one unit in the last place.
#[test]
fn sigma_is_lost_only_where_it_does_not_follow_the_weight() {
    let root = tempfile::tempdir().expect("root");
    let v2 = fixture(root.path(), "ms_trial.ms");
    let mut ms = MeasurementSet::open(&v2).expect("ms");
    let main = ms.main_table();
    let cell = |name: &str, row: usize| match main
        .column_accessor(name)
        .and_then(|column| column.array_cells_owned(&[row]))
        .expect(name)
        .pop()
        .flatten()
    {
        Some(ArrayValue::Float32(values)) => values,
        other => panic!("{name}: {other:?}"),
    };
    let row = (0..main.row_count())
        .find(|row| cell("WEIGHT", *row)[0] > 0.0)
        .expect("a row of positive weight");
    let weight = cell("WEIGHT", row)[0];
    let mut sigma = cell("SIGMA", row);
    let derived = 1.0 / weight.sqrt();
    let mut losses = |ms: &mut MeasurementSet, value: f32| {
        sigma[0] = value;
        let table = ms.main_table_mut();
        set(
            table,
            row,
            &[("SIGMA", Value::Array(ArrayValue::Float32(sigma.clone())))],
        );
        ms.save().expect("save");
        CreationPlan::new(
            &[CreationInput::new(&v2)],
            &options(),
            Destination::New(root.path().join("unwritten.ms")),
        )
        .expect("plan")
        .losses()
        .iter()
        .filter(|loss| loss.kind() == LossKind::Sigma)
        .cloned()
        .collect::<Vec<_>>()
    };
    assert!(losses(&mut ms, f32::from_bits(derived.to_bits() + 1)).is_empty());
    assert_eq!(
        losses(&mut ms, 2.0 * derived),
        [Loss::Sigma, Loss::SigmaDerived]
    );
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
                CreationPlan::new(&[input], &options(), unused(root.path())),
                Err(ObservationError::InvalidRequest { .. })
            ),
            "{weight_scale}"
        );
    }
    assert!(matches!(
        CreationPlan::new(&[], &options(), unused(root.path())),
        Err(ObservationError::InvalidRequest { .. })
    ));
}

/// A new destination in `root` that nothing is written to.
fn unused(root: &Path) -> Destination {
    Destination::New(root.join("unused.ms"))
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
    assert!(
        CreationPlan::new(
            &[CreationInput::new(&v2)],
            &options(),
            Destination::New(destination.clone())
        )
        .is_err(),
        "refused when planned"
    );
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
    // A scalar and a fixed-shape optional column, which the writer adds
    // with their defaults.
    for (column, default) in [
        (
            ColumnSchema::scalar("PULSAR_ID", PrimitiveType::Int32),
            int(-1),
        ),
        (
            ColumnSchema::array_fixed("POSITION", PrimitiveType::Float64, vec![3]),
            floats(&[0.0; 3], vec![3]),
        ),
    ] {
        if !source
            .schema()
            .is_some_and(|schema| schema.contains_column(column.name()))
        {
            source.add_column(column, Some(default)).expect("column");
        }
    }
    for row in 0..source.row_count() {
        set(
            source,
            row,
            &[
                ("PULSAR_ID", int(7)),
                ("POSITION", floats(&[6.0e6, 1.0e5, 2.0e5], vec![3])),
            ],
        );
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
                "{} {written:?} {:?} {:?} {:?} {:?} {} {:?} {:?} {:?} {:?} {:?} {:?} {:?}",
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
                record.position.as_ref().map(MPosition::as_itrf),
                record.pulsar_id,
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
/// is not written, so the others are renumbered and its source records go.
/// Rows of one partition using two states keep their own.
#[test]
fn an_msv2s_descriptive_metadata_survives_creation() {
    let root = tempfile::tempdir().expect("root");
    let stated = stated_msv2(root.path());
    let created = root.path().join("created.ms");
    let options = CreationOptions {
        accept: CASA_LOSSES.to_vec(),
        ..options()
    };
    let report = CreationPlan::new(
        &[CreationInput::new(&stated)],
        &options,
        Destination::New(created.clone()),
    )
    .and_then(CreationPlan::write)
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
    ] {
        assert!(
            report.losses.contains(&loss),
            "{loss:?}: {:?}",
            report.losses
        );
    }
    assert!(
        !report
            .losses
            .iter()
            .any(|loss| matches!(loss, Loss::Column(column) if column.contains("STATE_ID"))),
        "{:?}",
        report.losses
    );
    let open = |path: &Path| open(path, &OpenOptions::default()).expect("open");
    let (input, output) = (open(&stated), open(&created));
    let (input, output) = (input.catalogue(), output.catalogue());
    assert_eq!(input.sources.len(), 2);
    assert_eq!(input.sources[1].records.len(), 4, "{:?}", input.sources);
    let states = |catalogue: &Catalogue| -> Vec<usize> {
        let mut states: Vec<usize> = catalogue
            .partitions
            .iter()
            .map(|entry| entry.states.len())
            .collect();
        states.sort_unstable();
        states
    };
    assert!(states(input).contains(&2));
    assert_eq!(states(output), states(input));
    states_kept(&stated, &created);
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
    let plan = CreationPlan::new(&inputs, &options, Destination::New(whole.clone())).expect("plan");
    let promised = plan.output().clone();
    assert!(plan.notices().is_empty(), "{:?}", plan.notices());
    let report = plan.write().expect("write");
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

/// Every ID in `catalogue` names an entry of its list.
fn references_hold(catalogue: &Catalogue) {
    for entry in &catalogue.partitions {
        assert!(
            (entry.window.0 as usize) < catalogue.windows.len(),
            "window"
        );
        assert!(
            (entry.observation.0 as usize) < catalogue.observations.len(),
            "observation"
        );
        for state in &entry.states {
            assert!(
                (state.0 as usize) < catalogue.states.len(),
                "state {state:?}"
            );
        }
        for field in &entry.fields {
            assert!((field.0 as usize) < catalogue.fields.len(), "field");
        }
        for scan in &entry.scans {
            assert!((scan.0 as usize) < catalogue.scans.len(), "scan");
        }
        if let Some(processor) = entry.processor {
            assert!(
                (processor.0 as usize) < catalogue.processors.len(),
                "processor"
            );
        }
    }
    for field in &catalogue.fields {
        if let Some(source) = field.source {
            assert!((source.0 as usize) < catalogue.sources.len(), "source");
        }
    }
}

/// What a plan says reopening its MeasurementSet gives (`output()`) is what
/// reopening it gives, whole: from an MSv2, and from an MSv4 whose
/// partitions state intents and no states, whose STATE rows and MSv2
/// defaults are written. Every reference holds, and intent selection agrees.
#[test]
fn the_planned_catalogue_is_the_reopened_one() {
    let root = tempfile::tempdir().expect("root");
    let every_loss = [
        LossKind::Sigma,
        LossKind::RowFlags,
        LossKind::FlagCategories,
        LossKind::DataGroups,
        LossKind::Tables,
        LossKind::Columns,
        LossKind::Defaulted,
    ];
    for store in ["ms_trial.ms", "ms_trial.ps.zarr"] {
        let input = fixture(&root.path().join(store), store);
        let destination = root.path().join(format!("{store}.created.ms"));
        let plan = CreationPlan::new(
            &[CreationInput::new(&input)],
            &CreationOptions {
                accept: every_loss.to_vec(),
                ..options()
            },
            Destination::New(destination.clone()),
        )
        .expect("plan");
        let promised = plan.output().clone();
        plan.write().expect("write");
        let reopened = open(&destination, &OpenOptions::default()).expect("reopen");
        let reopened = reopened.catalogue();
        references_hold(&promised);
        let lists = |catalogue: &Catalogue| {
            [
                ("antennas", format!("{:#?}", catalogue.antennas)),
                ("fields", format!("{:#?}", catalogue.fields)),
                ("scans", format!("{:#?}", catalogue.scans)),
                ("observations", format!("{:#?}", catalogue.observations)),
                ("partitions", format!("{:#?}", catalogue.partitions)),
                ("windows", format!("{:#?}", catalogue.windows)),
                ("sources", format!("{:#?}", catalogue.sources)),
                ("states", format!("{:#?}", catalogue.states)),
                ("processors", format!("{:#?}", catalogue.processors)),
            ]
        };
        for ((list, planned), (_, read)) in lists(&promised).into_iter().zip(lists(reopened)) {
            let differs = planned
                .lines()
                .zip(read.lines())
                .enumerate()
                .find(|(_, (a, b))| a != b);
            assert!(
                differs.is_none() && planned.lines().count() == read.lines().count(),
                "{store}: {list} differ at line {differs:?}"
            );
        }
        let intents: BTreeSet<&String> = reopened
            .partitions
            .iter()
            .flat_map(|entry| &entry.intents)
            .collect();
        assert!(!intents.is_empty(), "{store} states intents");
        for intent in intents {
            assert_eq!(
                promised.partitions_with_intent(intent),
                reopened.partitions_with_intent(intent),
                "{store}: {intent}"
            );
            assert!(
                !reopened
                    .partitions_with_intent(intent)
                    .expect("selected")
                    .is_empty(),
                "{store}: {intent} selects its partition"
            );
        }
    }
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

/// Plan `inputs` into `destination` with padding omitted.
fn plan_of(inputs: &[&Path], destination: Destination) -> Result<CreationPlan, ObservationError> {
    let inputs: Vec<CreationInput> = inputs.iter().map(CreationInput::new).collect();
    CreationPlan::new(
        &inputs,
        &CreationOptions {
            padding: PaddingRows::Omitted,
            ..options()
        },
        destination,
    )
}

/// MAIN's `STATE_ID`s at each time (milliseconds) and baseline.
fn states_by_cell(path: &Path) -> BTreeMap<(i64, i32, i32), BTreeSet<i32>> {
    let ms = MeasurementSet::open(path).expect("ms");
    let main = ms.main_table();
    let mut states = BTreeMap::<_, BTreeSet<i32>>::new();
    for row in 0..main.row_count() {
        let record = main.row_accessor().row(row).expect("row");
        let int = |column: &str| match record.get(column) {
            Some(Value::Scalar(ScalarValue::Int32(value))) => *value,
            other => panic!("{column}: {other:?}"),
        };
        let Some(Value::Scalar(ScalarValue::Float64(time))) = record.get("TIME") else {
            panic!("TIME");
        };
        let cell = (
            (time * 1e3).round() as i64,
            int("ANTENNA1"),
            int("ANTENNA2"),
        );
        states.entry(cell).or_default().insert(int("STATE_ID"));
    }
    states
}

/// Every row of `input` is written with its own `STATE_ID` in `created`,
/// whose `STATE` keeps the input's order.
fn states_kept(input: &Path, created: &Path) {
    let written = states_by_cell(created);
    for (cell, states) in states_by_cell(input) {
        assert_eq!(written.get(&cell), Some(&states), "{cell:?}");
    }
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
    let refused = plan_of(&[&early, &late], unused(root.path()));
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
        plan_of(&[&early, &longer], unused(root.path())),
        Err(ObservationError::Unsupported { what }) if what.contains("intervals")
    ));
    let late = edited(root.path(), "late-clean", 0..60, |_| {});
    let flagged = CreationPlan::new(
        &[CreationInput::new(&early), CreationInput::new(&late)],
        &options(),
        unused(root.path()),
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
    plan_of(&[&early, &hotter], Destination::New(whole.clone()))
        .and_then(CreationPlan::write)
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
        plan_of(&[&early, &processed], unused(root.path())),
        Err(ObservationError::Unsupported { what }) if what.contains("processors")
    ));
}

/// Rows of one input partition using two states of one `OBS_MODE` (`CAL`
/// 0 and 5 K, alternating within each time) keep their own `STATE_ID`, and
/// nothing is lost: the partition reopens whole with both states, and a
/// padded row takes the state of its time's first row.
#[test]
fn rows_of_one_partition_keep_their_own_states() {
    let root = tempfile::tempdir().expect("root");
    // Two rows removed leave their cells padding.
    let input = edited(root.path(), "two-states", 7..9, |ms| {
        let state = subtable(ms, SubtableId::State);
        let hotter = state.row_accessor().row(0).expect("state").clone();
        state.add_row(hotter).expect("a second state");
        set(state, 1, &[("CAL", float(5.0))]);
        let main = ms.main_table_mut();
        for row in (1..main.row_count()).step_by(2) {
            set(main, row, &[("STATE_ID", int(1))]);
        }
    });
    // Small batches (of an integration at least, and of an antenna's
    // pointing read), so the rows' states are read batch by batch.
    let inputs = [CreationInput::new(&input)];
    let small = CreationOptions {
        batch_bytes: 8 << 10,
        ..options()
    };
    let batch_bytes = match CreationPlan::new(&inputs, &small, unused(root.path())) {
        Err(ObservationError::OverBudget { required, .. }) => required,
        other => other.map(|_| small.batch_bytes).expect("a plan"),
    };
    let created = root.path().join("created.ms");
    let report = CreationPlan::new(
        &inputs,
        &CreationOptions {
            batch_bytes,
            ..options()
        },
        Destination::New(created.clone()),
    )
    .and_then(CreationPlan::write)
    .expect("create");
    assert!(report.batches > 1, "{}", report.batches);
    assert!(
        !report
            .losses
            .iter()
            .any(|loss| matches!(loss, Loss::Column(column) if column.contains("STATE_ID"))),
        "{:?}",
        report.losses
    );
    let reopened = open(&created, &OpenOptions::default()).expect("reopen");
    let catalogue = reopened.catalogue();
    assert_eq!(catalogue.partitions.len(), 1);
    assert_eq!(catalogue.partitions[0].states.len(), 2);
    states_kept(&input, &created);
    let (stated, written) = (states_by_cell(&input), states_by_cell(&created));
    let padded: Vec<_> = written
        .iter()
        .filter(|(cell, _)| !stated.contains_key(cell))
        .collect();
    assert_eq!(padded.len(), 2, "{padded:?}");
    for ((time, _, _), states) in padded {
        let first = stated
            .iter()
            .find(|(cell, _)| cell.0 == *time)
            .map(|(_, states)| states);
        assert_eq!(Some(states), first, "{time}");
    }
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
        Destination::New(whole.clone()),
    )
    .expect("plan");
    plan.write().expect("write");
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

/// A subtable column the catalogue does not carry is a declared loss when
/// a row holds a value other than the one the writer writes: an antenna's
/// offset is, its type `GROUND-BASED` is not; so is an observation's
/// `SCHEDULE` the writer does not rebuild, a polarization's or data
/// description's `FLAG_ROW` set (written false), and a column outside the
/// schema whatever it holds; refused until accepted.
#[test]
fn uncarried_subtable_columns_are_declared() {
    let root = tempfile::tempdir().expect("root");
    let flagged = Value::Scalar(ScalarValue::Bool(true));
    let path = edited(root.path(), "offset", 0..0, |ms| {
        set(
            subtable(ms, SubtableId::Antenna),
            0,
            &[
                ("OFFSET", floats(&[0.0, 0.0, 1.5], vec![3])),
                ("TYPE", text("GROUND-BASED")),
            ],
        );
        subtable(ms, SubtableId::Antenna)
            .add_column(
                ColumnSchema::scalar("EXTRA", PrimitiveType::Int32),
                Some(int(0)),
            )
            .expect("a column outside the schema, all zero");
        set(
            subtable(ms, SubtableId::Observation),
            0,
            &[("SCHEDULE", texts(&["science schedule"]))],
        );
        set(
            subtable(ms, SubtableId::Polarization),
            0,
            &[("FLAG_ROW", flagged.clone())],
        );
        set(
            subtable(ms, SubtableId::DataDescription),
            0,
            &[("FLAG_ROW", flagged.clone())],
        );
    });
    let plan = CreationPlan::new(
        &[CreationInput::new(&path)],
        &options(),
        unused(root.path()),
    )
    .expect("plan");
    let columns: Vec<&Loss> = plan
        .losses()
        .iter()
        .filter(|loss| matches!(loss, Loss::Column(_)))
        .collect();
    let expected: Vec<Loss> = [
        "ANTENNA EXTRA (outside the MSv2 schema)",
        "ANTENNA OFFSET",
        "DATA_DESCRIPTION FLAG_ROW",
        "OBSERVATION LOG",
        "OBSERVATION SCHEDULE",
        "OBSERVATION SCHEDULE_TYPE",
        "POLARIZATION FLAG_ROW",
    ]
    .map(|column| Loss::Column(column.to_string()))
    .to_vec();
    assert_eq!(columns, expected.iter().collect::<Vec<_>>());
    let strict = CreationOptions {
        accept: vec![LossKind::Sigma, LossKind::Tables],
        ..options()
    };
    let refused = CreationPlan::new(&[CreationInput::new(&path)], &strict, unused(root.path()));
    assert!(
        matches!(&refused, Err(ObservationError::InvalidRequest { reason }) if reason.contains("ANTENNA EXTRA")),
        "{}",
        refused
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default()
    );
}

/// The name of this process's stage of `purpose` for a MeasurementSet named
/// `name`, at `attempt`.
fn stage_name(name: &str, purpose: &str, attempt: usize) -> String {
    format!(".{name}.casa-rs-{purpose}-{attempt}-{}", std::process::id())
}

/// MAIN's storage-manager files of the MeasurementSet at `path`.
fn payload_files(path: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(path)
        .expect("dir")
        .map(|entry| entry.expect("entry").path())
        .filter(|file| {
            file.file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with("table.f"))
        })
        .collect()
}

/// Inputs whose POINTING tables are each in one frame, but not the same
/// one, are refused when the plan is made, not while it writes: one
/// POINTING holds one frame.
#[test]
fn pointing_in_different_frames_is_refused_by_the_plan() {
    let root = tempfile::tempdir().expect("root");
    let early = edited(root.path(), "early", 60..120, |_| {});
    let late = edited(root.path(), "late", 0..60, |ms| {
        let pointing = ms.subtable_mut(SubtableId::Pointing).expect("POINTING");
        TableMeasDesc::new_fixed("DIRECTION", MeasureType::Direction, "B1950")
            .write(pointing)
            .expect("B1950 directions");
    });
    let destination = root.path().join("whole.ms");
    let refused = plan_of(&[&early, &late], Destination::New(destination.clone()));
    assert!(
        matches!(&refused, Err(ObservationError::Unsupported { what }) if what.contains("frames")),
        "{:?}",
        refused.err()
    );
    assert!(!destination.exists());
    assert_eq!(
        others(root.path(), &["early", "late"]),
        Vec::<String>::new()
    );
}

/// A new MeasurementSet passes over what occupies its stage's first name,
/// which is kept, and is not published over a destination that appeared
/// after the plan was made: that is kept, and the stage removed.
#[test]
fn a_new_measurement_set_keeps_what_it_did_not_make() {
    let root = tempfile::tempdir().expect("root");
    let v2 = fixture(root.path(), "ms_trial.ms");
    let destination = root.path().join("created.ms");
    let occupied = root.path().join(stage_name("created.ms", "creating", 0));
    std::fs::create_dir(&occupied).expect("a directory");
    std::fs::write(occupied.join("sentinel"), b"kept").expect("sentinel");
    let plan = || {
        CreationPlan::new(
            &[CreationInput::new(&v2)],
            &options(),
            Destination::New(destination.clone()),
        )
        .expect("plan")
    };
    plan().write().expect("created");
    assert!(MeasurementSet::open(&destination).is_ok());
    assert_eq!(
        std::fs::read(occupied.join("sentinel")).expect("kept"),
        b"kept"
    );
    let entries = |root: &Path| {
        let mut names = others(root, &["ms_trial.ms", "created.ms"]);
        names.sort();
        names
    };
    assert_eq!(
        entries(root.path()),
        [stage_name("created.ms", "creating", 0)]
    );

    std::fs::remove_dir_all(&destination).expect("remove");
    let pending = plan();
    std::fs::create_dir(&destination).expect("appeared");
    std::fs::write(destination.join("keep"), b"theirs").expect("theirs");
    assert!(pending.write().is_err(), "not published over it");
    assert_eq!(
        std::fs::read(destination.join("keep")).expect("kept"),
        b"theirs"
    );
    assert_eq!(
        entries(root.path()),
        [stage_name("created.ms", "creating", 0)]
    );
}

/// The entries of `directory` other than `kept`, by name.
fn others(directory: &Path, kept: &[&str]) -> Vec<String> {
    std::fs::read_dir(directory)
        .expect("dir")
        .map(|entry| {
            entry
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .filter(|name| !kept.contains(&name.as_str()))
        .collect()
}

/// Replacing appends to an existing MeasurementSet, the first input, as
/// CASA's `concat` appends to `concatvis`: the late half's rows join the
/// early half's in its place, and nothing is left beside it.
#[test]
fn replacing_appends_to_an_existing_measurement_set() {
    let root = tempfile::tempdir().expect("root");
    let early = edited(root.path(), "early", 60..120, |_| {});
    let late = edited(root.path(), "late", 0..60, |_| {});
    let report = plan_of(&[&early, &late], Destination::Replace(early.clone()))
        .and_then(CreationPlan::write)
        .expect("replace");
    assert_eq!(report.leftover, None);
    let reopened = open(&early, &OpenOptions::default()).expect("reopen");
    assert_eq!(reopened.catalogue().partitions.len(), 1);
    assert_eq!(main_ints(&early, "ANTENNA1").len(), 120);
    let parent = early.parent().expect("parent");
    assert_eq!(others(parent, &["ms_trial.ms"]), Vec::<String>::new());
}

/// What cannot be replaced is refused when planned: a path that is no
/// MeasurementSet, and one another handle holds for writing.
#[test]
fn replace_refuses_what_it_cannot_replace() {
    let root = tempfile::tempdir().expect("root");
    let early = edited(root.path(), "early", 60..120, |_| {});
    let late = edited(root.path(), "late", 0..60, |_| {});
    let nothing = root.path().join("nothing.ms");
    std::fs::create_dir(&nothing).expect("nothing");
    assert!(matches!(
        plan_of(&[&early, &late], Destination::Replace(nothing)),
        Err(ObservationError::InvalidRequest { .. })
    ));
    let held = casa_tables::TableWriteLock::acquire(&early, 1).expect("held");
    let refused = plan_of(&[&early, &late], Destination::Replace(early.clone()));
    assert!(
        matches!(&refused, Err(ObservationError::InvalidRequest { reason }) if reason.contains("held")),
        "{}",
        refused
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default()
    );
    held.release().expect("release");
}

/// A replacement that fails before the exchange leaves the MeasurementSet
/// as it was, and its stage removed; an old copy that cannot be deleted
/// after the exchange is reported, the new MeasurementSet in place.
#[test]
fn a_failed_replacement_leaves_the_measurement_set_as_it_was() {
    use std::os::unix::fs::PermissionsExt;

    let root = tempfile::tempdir().expect("root");
    let early = edited(root.path(), "early", 60..120, |_| {});
    let late = edited(root.path(), "late", 0..60, |_| {});
    let parent = early.parent().expect("parent");
    // What occupies the stage's first names, a file and a nonempty
    // directory, is passed over and kept.
    let (file, directory) = (
        stage_name("ms_trial.ms", "replacing", 0),
        stage_name("ms_trial.ms", "replacing", 1),
    );
    std::fs::write(parent.join(&file), b"in the way").expect("a file");
    std::fs::create_dir(parent.join(&directory)).expect("a directory");
    std::fs::write(parent.join(&directory).join("sentinel"), b"kept").expect("sentinel");
    let kept = ["ms_trial.ms", file.as_str(), directory.as_str()];
    // A write that fails part way: the late input's payload unreadable.
    let plan = plan_of(&[&early, &late], Destination::Replace(early.clone())).expect("plan");
    let payload = payload_files(&late);
    for path in &payload {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o000)).expect("hide");
    }
    let failed = plan.write();
    for path in &payload {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).expect("show");
    }
    assert!(failed.is_err(), "{:?}", failed.ok());
    assert_eq!(main_ints(&early, "ANTENNA1").len(), 60, "as it was");
    assert_eq!(
        others(parent, &kept),
        Vec::<String>::new(),
        "its stage removed"
    );
    assert_eq!(
        std::fs::read(parent.join(&directory).join("sentinel")).expect("kept"),
        b"kept"
    );
    assert!(parent.join(&file).is_file());
    std::fs::remove_file(parent.join(&file)).expect("tidy");
    std::fs::remove_dir_all(parent.join(&directory)).expect("tidy");

    let plan = plan_of(&[&early, &late], Destination::Replace(early.clone())).expect("plan");
    let pinned = early.join("ANTENNA");
    std::fs::set_permissions(&pinned, std::fs::Permissions::from_mode(0o500)).expect("pin");
    let report = plan.write().expect("replace");
    let leftover = report.leftover.expect("the old copy is reported");
    assert_eq!(
        main_ints(&early, "ANTENNA1").len(),
        120,
        "the new one in place"
    );
    std::fs::set_permissions(
        leftover.join("ANTENNA"),
        std::fs::Permissions::from_mode(0o700),
    )
    .expect("unpin");
}
