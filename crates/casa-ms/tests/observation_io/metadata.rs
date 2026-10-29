// SPDX-License-Identifier: LGPL-3.0-or-later
//! The catalogue's descriptive metadata (#937) as each trial store states
//! it: the MSv2's every `SPECTRAL_WINDOW`, `STATE` and `PROCESSOR` row and
//! `OBSERVATION`'s extent, flags and release date; the MSv4 XRADIO wrote
//! from it, which states a window's reference frequency, a processor's type
//! and subtype and a release date, and leaves the rest unstated. The two
//! still bind.

use std::path::Path;

use casa_ms::observation_io::{OpenOptions, open};
use casa_ms::{MeasurementSet, SubtableId};
use casa_observation::{
    BindingTolerance, IntentError, ObservationError, PartitionId, ProcessorId, StateId, WindowId,
};
use casa_tables::ColumnSchema;
use casa_types::measures::frequency::FrequencyRef;
use casa_types::{ArrayValue, PrimitiveType, RecordValue, ScalarValue, Value};
use ndarray::ArrayD;

use super::fixture;

/// Partitions selected by `intent` in the MSv2 at `path`, through its
/// adapter.
fn select(path: &Path, intent: &str) -> Result<Vec<PartitionId>, IntentError> {
    open(path, &OpenOptions::default())
        .expect("msv2")
        .catalogue()
        .partitions_with_intent(intent)
}

/// Write `mode` as the `OBS_MODE` of the MSv2 at `path`'s first STATE row.
fn set_obs_mode(path: &Path, mode: &str) {
    let mut ms = MeasurementSet::open(path).expect("ms");
    ms.subtable_mut(SubtableId::State)
        .expect("STATE")
        .row_accessor_mut()
        .set_cell(
            0,
            "OBS_MODE",
            Value::Scalar(ScalarValue::String(mode.to_string())),
        )
        .expect("OBS_MODE");
    ms.save().expect("save");
}

/// Replace `table`'s `column` in the MSv2 at `path` with an array column of
/// any shape, every cell undefined, then set `cells`.
fn reshape(path: &Path, table: SubtableId, column: &str, cells: &[(usize, ArrayD<f64>)]) {
    let mut ms = MeasurementSet::open(path).expect("ms");
    let subtable = ms.subtable_mut(table).expect("subtable");
    subtable.remove_column(column).expect("remove the column");
    subtable
        .add_column(
            ColumnSchema::array_variable(column, PrimitiveType::Float64, None),
            None,
        )
        .expect("an array column of any shape");
    for (row, cell) in cells {
        subtable
            .row_accessor_mut()
            .set_cell(
                *row,
                column,
                Value::Array(ArrayValue::Float64(cell.clone())),
            )
            .expect("cell");
    }
    ms.save().expect("save");
}

/// The reason opening the MSv2 at `path` is refused.
fn refusal(path: &Path) -> String {
    match open(path, &OpenOptions::default()) {
        Err(ObservationError::InvalidStore { reason }) => reason,
        Err(other) => panic!("refused otherwise: {other}"),
        Ok(_) => panic!("opened"),
    }
}

/// A field's delay or reference centre that the row leaves undefined is
/// unstated (`None`), and the opening reads everything else.
#[test]
fn undefined_field_centres_are_unstated() {
    for (column, other) in [
        ("DELAY_DIR", "REFERENCE_DIR"),
        ("REFERENCE_DIR", "DELAY_DIR"),
    ] {
        let root = tempfile::tempdir().expect("root");
        let path = fixture(root.path(), "ms_trial.ms");
        reshape(&path, SubtableId::Field, column, &[]);
        let opened = open(&path, &OpenOptions::default()).expect("an undefined centre opens");
        let field = &opened.catalogue().fields[0];
        let (undefined, stated) = match column {
            "DELAY_DIR" => (&field.delay_centre, &field.reference_centre),
            _ => (&field.reference_centre, &field.delay_centre),
        };
        assert!(undefined.is_none(), "{column} undefined");
        assert!(stated.is_some(), "{other} still stated");
    }
}

/// A defined cell of another shape than its definition's refuses the
/// opening, naming its column: a window's per-channel array of fewer or
/// more values than `NUM_CHAN`, or of two axes; a FIELD direction without a
/// constant term, or of more terms than `NUM_POLY` gives.
#[test]
fn metadata_cells_of_the_wrong_shape_are_refused() {
    let vector = |values: usize| ArrayD::from_elem(vec![values], 1.0e6);
    for (table, column, cell) in [
        (SubtableId::SpectralWindow, "EFFECTIVE_BW", vector(7)),
        (SubtableId::SpectralWindow, "EFFECTIVE_BW", vector(9)),
        (
            SubtableId::SpectralWindow,
            "RESOLUTION",
            ArrayD::from_elem(vec![2, 4], 1.0e6),
        ),
        (SubtableId::Field, "DELAY_DIR", ArrayD::zeros(vec![2, 0])),
        (SubtableId::Field, "DELAY_DIR", ArrayD::zeros(vec![2, 2])),
    ] {
        let root = tempfile::tempdir().expect("root");
        let path = fixture(root.path(), "ms_trial.ms");
        let shape = cell.shape().to_vec();
        reshape(&path, table, column, &[(0, cell)]);
        let reason = refusal(&path);
        assert!(reason.contains(column), "{column} {shape:?}: {reason}");
    }
}

/// Intent selection through the MSv2 adapter matches each STATE row's
/// stored `OBS_MODE` as CASA's literal matcher does
/// (`MSStateIndex::matchStateObsMode`): split at commas, each element
/// untrimmed. Two STATE rows of one `OBS_MODE`, one flagged, make one
/// partition, whose selection is refused.
#[test]
fn msv2_intent_selection_matches_the_stored_obs_mode_literally() {
    let root = tempfile::tempdir().expect("root");
    let path = fixture(root.path(), "ms_trial.ms");
    let all = Ok(vec![PartitionId(0)]);
    set_obs_mode(&path, "A, B");
    assert_eq!(select(&path, "A"), all);
    assert_eq!(select(&path, "B"), Ok(Vec::new()), "\" B\" is not \"B\"");
    assert_eq!(select(&path, " B"), all);
    set_obs_mode(&path, " A,B");
    assert_eq!(select(&path, "A"), Ok(Vec::new()));
    assert_eq!(select(&path, "B"), all);

    let mut ms = MeasurementSet::open(&path).expect("ms");
    let state = ms.subtable_mut(SubtableId::State).expect("STATE");
    let mut fields = state.row_accessor().row(0).expect("row").fields().to_vec();
    for field in &mut fields {
        if field.name == "FLAG_ROW" {
            field.value = Value::Scalar(ScalarValue::Bool(true));
        }
    }
    state
        .add_row(RecordValue::new(fields))
        .expect("a flagged state");
    let rows = ms.row_count();
    ms.main_table_mut()
        .fill_column_range(
            "STATE_ID",
            casa_tables::RowRange::new(0, rows / 2),
            Value::Scalar(ScalarValue::Int32(1)),
        )
        .expect("half the rows in the flagged state");
    ms.save().expect("save");
    drop(ms);
    let opened = open(&path, &OpenOptions::default()).expect("msv2");
    assert_eq!(opened.catalogue().partitions.len(), 1, "one OBS_MODE");
    assert_eq!(
        opened.catalogue().partitions[0].states,
        [StateId(0), StateId(1)]
    );
    assert!(matches!(
        select(&path, "B"),
        Err(IntentError::MixedStateFlags { .. })
    ));
}

#[test]
fn each_store_states_its_windows_states_processors_and_observations() {
    let root = tempfile::tempdir().expect("root");
    let msv2 = open(fixture(root.path(), "ms_trial.ms"), &OpenOptions::default()).expect("msv2");
    let msv4 = open(
        fixture(root.path(), "ms_trial.ps.zarr"),
        &OpenOptions::default(),
    )
    .expect("msv4");
    let (ours, theirs) = (msv2.catalogue(), msv4.catalogue());

    // The MSv2's one SPECTRAL_WINDOW row, every member stated.
    assert_eq!(ours.windows.len(), 1);
    let window = ours.window(ours.partitions[0].window);
    let reference = window.reference_frequency.as_ref().expect("REF_FREQUENCY");
    assert_eq!(
        (reference.hz(), reference.refer()),
        (44.512e9, FrequencyRef::LSRK)
    );
    assert_eq!(window.total_bandwidth_hz, Some(1.024e9));
    assert_eq!((window.if_chain, window.net_sideband), (Some(0), Some(1)));
    assert_eq!(window.frequency_group_name.as_deref(), Some(""));
    assert_eq!(
        window.effective_bandwidths_hz.as_ref().map(Vec::len),
        Some(8)
    );
    assert_eq!(window.flagged, Some(false));
    // Its one STATE row, which every partition's rows use; an empty
    // PROCESSOR whose MAIN rows name PROCESSOR_ID 0 gives no processor.
    assert_eq!(ours.states.len(), 1);
    let state = &ours.states[0];
    assert_eq!(state.obs_mode, "TARGET.ON_SOURCE");
    assert_eq!(
        (
            state.cal_k,
            state.load_k,
            state.signal,
            state.reference,
            state.sub_scan
        ),
        (Some(0.0), Some(0.0), Some(true), Some(false), Some(0))
    );
    assert!(ours.processors.is_empty());
    for partition in &ours.partitions {
        assert_eq!(partition.window, WindowId(0));
        assert_eq!(partition.states, [StateId(0)]);
        assert_eq!(partition.processor, None);
    }
    let observation = &ours.observations[0];
    let range = observation.time_range.as_ref().expect("TIME_RANGE");
    assert!(range[0].value().as_mjd() <= range[1].value().as_mjd());
    assert_eq!(
        observation
            .release_date
            .as_ref()
            .map(|date| date.value().as_mjd()),
        Some(0.0)
    );
    assert_eq!(observation.flagged, Some(false));
    // Without a SOURCE table there are no sources.
    assert!(ours.sources.is_empty());
    assert!(ours.fields.iter().all(|field| field.source.is_none()));

    // XRADIO's conversion: the reference frequency, a processor of empty
    // type and subtype, the release date; no states, no extent, no flags.
    assert_eq!(theirs.windows.len(), 1);
    let window = theirs.window(theirs.partitions[0].window);
    let reference = window
        .reference_frequency
        .as_ref()
        .expect("reference_frequency");
    assert_eq!(
        (reference.hz(), reference.refer()),
        (44.512e9, FrequencyRef::LSRK)
    );
    assert_eq!(
        (window.total_bandwidth_hz, window.if_chain, window.flagged),
        (None, None, None)
    );
    assert_eq!(theirs.processors.len(), 1);
    assert_eq!(
        (
            theirs.processors[0].kind.as_deref(),
            theirs.processors[0].sub_type.as_deref()
        ),
        (Some(""), Some(""))
    );
    assert_eq!(theirs.partitions[0].processor, Some(ProcessorId(0)));
    assert!(theirs.states.is_empty() && theirs.partitions[0].states.is_empty());
    let observation = &theirs.observations[0];
    assert!(
        observation.time_range.is_none(),
        "never derived from the samples"
    );
    assert_eq!(
        observation
            .release_date
            .as_ref()
            .map(|date| date.value().as_mjd()),
        Some(0.0)
    );

    // What one store leaves unstated does not keep them from binding.
    let binding = ours
        .bind_to(theirs, &BindingTolerance::EXACT)
        .expect("the MSv2 binds to its conversion");
    assert_eq!(binding.partitions, [PartitionId(0)]);
    // Intent selection reads the MSv2's one unflagged state.
    assert_eq!(
        ours.partitions_with_intent("TARGET.ON_SOURCE")
            .expect("selection"),
        [PartitionId(0)]
    );
}
