// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `FIELD`, `OBSERVATION`, `SPECTRAL_WINDOW`, `STATE` and `PROCESSOR`
//! rows of an MSv2 a plan creates, each from its catalogue entry. A member
//! the catalogue leaves unstated is written as MSv2's default and noted
//! ([`Notes`]); a value an MSv2 cannot hold as stated (a time in another
//! scale than UTC, a field direction in another frame than J2000) is
//! refused.

use casa_observation::{Catalogue, ObservationError, PartitionEntry};
use casa_tables::ColumnSchema;
use casa_tables::table_measures::{MeasureType, TableMeasDesc};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::frequency::FrequencyRef;
use casa_types::measures::{EpochRef, MEpoch};

use super::Loss;
use super::msv2_sources::{self, NamedSources};
use super::msv2_tables::Notes;
use crate::MeasurementSet;
use crate::observation_io::update::{request_error, storage};
use crate::schema::{self, SubtableId, optional_columns};
use crate::subtable_rows::{b, f, f64_array, i, row_from_defs, s, string_array, subtable_mut};

/// Seconds per day, for MSv2 times (MJD seconds).
const SECONDS_PER_DAY: f64 = 86_400.0;

/// `epoch` as an MSv2 subtable time: MJD seconds in UTC, the scale the
/// columns this writer writes declare.
pub(super) fn utc_seconds(epoch: &MEpoch) -> Result<f64, ObservationError> {
    if epoch.refer() != EpochRef::UTC {
        return Err(ObservationError::Unsupported {
            what: "writing MSv2 subtable times in another scale than UTC",
        });
    }
    let value = epoch.value();
    Ok((value.day() + value.frac()) * SECONDS_PER_DAY)
}

/// `direction`'s angles, which an MSv2 field holds in J2000 only.
fn j2000(direction: &MDirection) -> Result<[f64; 2], ObservationError> {
    if direction.refer() != DirectionRef::J2000 {
        return Err(ObservationError::Unsupported {
            what: "writing MSv2 field directions in another frame than J2000",
        });
    }
    let (longitude, latitude) = direction.as_angles();
    Ok([longitude, latitude])
}

/// One `FIELD` row per catalogue field, its source named by `SOURCE_ID`
/// as [`msv2_sources`] writes them (`named`); `time`, the data's first, is
/// the `TIME` of a field that states none.
pub(super) fn fields(
    ms: &mut MeasurementSet,
    catalogue: &Catalogue,
    (named, time): (&NamedSources, f64),
    notes: &mut Notes,
) -> Result<(), ObservationError> {
    let table = subtable_mut(ms, SubtableId::Field).map_err(storage)?;
    for field in &catalogue.fields {
        let phase = &field.phase_centre;
        let delay = notes.or(field.delay_centre.as_ref(), "FIELD DELAY_DIR", || phase);
        let reference = notes.or(
            field.reference_centre.as_ref(),
            "FIELD REFERENCE_DIR",
            || phase,
        );
        let direction = |centre| j2000(centre).map(|angles| f64_array(&angles, vec![2, 1]));
        let epoch = field
            .reference_epoch
            .as_ref()
            .map(utc_seconds)
            .transpose()?;
        let row = row_from_defs(
            schema::field::REQUIRED_COLUMNS,
            &[
                ("NAME", s(&field.name)),
                (
                    "CODE",
                    s(&notes.or(field.code.clone(), "FIELD CODE", String::new)),
                ),
                ("NUM_POLY", i(0)),
                ("DELAY_DIR", direction(delay)?),
                ("PHASE_DIR", direction(phase)?),
                ("REFERENCE_DIR", direction(reference)?),
                (
                    "SOURCE_ID",
                    i(msv2_sources::field_source(catalogue, field, named)),
                ),
                ("TIME", f(notes.or(epoch, "FIELD TIME", || time))),
                (
                    "FLAG_ROW",
                    b(notes.or(field.flagged, "FIELD FLAG_ROW", || false)),
                ),
            ],
        );
        table.add_row(row).map_err(storage)?;
    }
    Ok(())
}

/// One `OBSERVATION` row per session; a session that states no extent
/// spans the times its partitions span (`ranges`, by session), edge to
/// edge.
pub(super) fn observations(
    ms: &mut MeasurementSet,
    catalogue: &Catalogue,
    ranges: &[[f64; 2]],
    notes: &mut Notes,
) -> Result<(), ObservationError> {
    let table = subtable_mut(ms, SubtableId::Observation).map_err(storage)?;
    for (observation, span) in catalogue.observations.iter().zip(ranges) {
        // The archive identifiers as importasdm writes them, which is how
        // the MSv2 adapter and XRADIO's converter read them back.
        let schedule = match (&observation.scheduling_block, &observation.execution_block) {
            (Some(scheduling), Some(execution)) => vec![
                format!("[SchedulingBlock {scheduling},"),
                format!("ExecBlock {execution}]"),
            ],
            _ => Vec::new(),
        };
        let schedule: Vec<&str> = schedule.iter().map(String::as_str).collect();
        let time_range = observation
            .time_range
            .as_ref()
            .map(|[start, end]| Ok::<_, ObservationError>([utc_seconds(start)?, utc_seconds(end)?]))
            .transpose()?;
        let time_range = notes.or(time_range, "OBSERVATION TIME_RANGE", || *span);
        let release = observation
            .release_date
            .as_ref()
            .map(utc_seconds)
            .transpose()?;
        let row = row_from_defs(
            schema::observation::REQUIRED_COLUMNS,
            &[
                (
                    "FLAG_ROW",
                    b(notes.or(observation.flagged, "OBSERVATION FLAG_ROW", || false)),
                ),
                ("LOG", string_array(&[], vec![0])),
                ("OBSERVER", s(&observation.observers.join(", "))),
                ("PROJECT", s(&observation.project)),
                (
                    "RELEASE_DATE",
                    f(notes.or(release, "OBSERVATION RELEASE_DATE", || 0.0)),
                ),
                ("SCHEDULE", string_array(&schedule, vec![schedule.len()])),
                ("SCHEDULE_TYPE", s("")),
                ("TELESCOPE_NAME", s(&observation.telescope)),
                ("TIME_RANGE", f64_array(&time_range, vec![2])),
            ],
        );
        table.add_row(row).map_err(storage)?;
    }
    Ok(())
}

/// Whether two partitions of one window hold its channels alike: the same
/// frame, frequencies and widths, bit for bit.
fn same_channels(a: &PartitionEntry, b: &PartitionEntry) -> bool {
    let bits = |values: &[f64]| values.iter().map(|v| v.to_bits()).collect::<Vec<_>>();
    a.frequency_frame == b.frequency_frame
        && bits(&a.frequencies_hz) == bits(&b.frequencies_hz)
        && bits(&a.channel_widths_hz) == bits(&b.channel_widths_hz)
}

/// One `SPECTRAL_WINDOW` row per catalogue window a partition uses, with
/// the channels its partitions hold, and the row of each window: `None`
/// for a window no partition uses, which has no channels to write and is a
/// loss.
pub(super) fn spectral_windows(
    ms: &mut MeasurementSet,
    catalogue: &Catalogue,
    notes: &mut Notes,
) -> Result<Vec<Option<i32>>, ObservationError> {
    let table = subtable_mut(ms, SubtableId::SpectralWindow).map_err(storage)?;
    let mut rows = Vec::with_capacity(catalogue.windows.len());
    let mut unused = 0;
    for (index, window) in catalogue.windows.iter().enumerate() {
        let mut partitions = catalogue
            .partitions
            .iter()
            .filter(|entry| entry.window.0 as usize == index);
        let Some(first) = partitions.next() else {
            unused += 1;
            rows.push(None);
            continue;
        };
        if partitions.any(|entry| !same_channels(entry, first)) {
            return Err(ObservationError::Unsupported {
                what: "writing one MSv2 spectral window for partitions with different channels",
            });
        }
        let channels = first.frequencies_hz.len();
        let widths = &first.channel_widths_hz;
        let magnitudes: Vec<f64> = widths.iter().map(|width| width.abs()).collect();
        let reference = match &window.reference_frequency {
            Some(frequency) if frequency.refer() == first.frequency_frame => Some(frequency.hz()),
            Some(_) => {
                return Err(ObservationError::Unsupported {
                    what: "writing an MSv2 reference frequency in another frame than its channels",
                });
            }
            None => None,
        };
        let mut per_channel = |values: &Option<Vec<f64>>, column: &str| match values {
            Some(values) if values.len() == channels => Ok(values.clone()),
            Some(_) => Err(request_error(format!(
                "{column} of window {} holds other than its {channels} channels",
                window.name
            ))),
            None => Ok(notes.or(None, column, || magnitudes.clone())),
        };
        let effective = per_channel(
            &window.effective_bandwidths_hz,
            "SPECTRAL_WINDOW EFFECTIVE_BW",
        )?;
        let resolution = per_channel(&window.resolutions_hz, "SPECTRAL_WINDOW RESOLUTION")?;
        let row = row_from_defs(
            schema::spectral_window::REQUIRED_COLUMNS,
            &[
                ("NUM_CHAN", i(channels as i32)),
                ("NAME", s(&window.name)),
                (
                    "REF_FREQUENCY",
                    f(notes.or(reference, "SPECTRAL_WINDOW REF_FREQUENCY", || {
                        first.frequencies_hz[0]
                    })),
                ),
                (
                    "TOTAL_BANDWIDTH",
                    f(notes.or(
                        window.total_bandwidth_hz,
                        "SPECTRAL_WINDOW TOTAL_BANDWIDTH",
                        || magnitudes.iter().sum(),
                    )),
                ),
                (
                    "CHAN_FREQ",
                    f64_array(&first.frequencies_hz, vec![channels]),
                ),
                ("CHAN_WIDTH", f64_array(widths, vec![channels])),
                ("EFFECTIVE_BW", f64_array(&effective, vec![channels])),
                ("RESOLUTION", f64_array(&resolution, vec![channels])),
                ("MEAS_FREQ_REF", i(first.frequency_frame.casacore_code())),
                (
                    "NET_SIDEBAND",
                    i(
                        notes.or(window.net_sideband, "SPECTRAL_WINDOW NET_SIDEBAND", || {
                            if widths[0] >= 0.0 { 1 } else { -1 }
                        }),
                    ),
                ),
                (
                    "FREQ_GROUP",
                    i(notes.or(window.frequency_group, "SPECTRAL_WINDOW FREQ_GROUP", || 0)),
                ),
                (
                    "FREQ_GROUP_NAME",
                    s(&notes.or(
                        window.frequency_group_name.clone(),
                        "SPECTRAL_WINDOW FREQ_GROUP_NAME",
                        String::new,
                    )),
                ),
                (
                    "IF_CONV_CHAIN",
                    i(notes.or(window.if_chain, "SPECTRAL_WINDOW IF_CONV_CHAIN", || 0)),
                ),
                (
                    "FLAG_ROW",
                    b(notes.or(window.flagged, "SPECTRAL_WINDOW FLAG_ROW", || false)),
                ),
            ],
        );
        table.add_row(row).map_err(storage)?;
        rows.push(Some(table.row_count() as i32 - 1));
    }
    if unused > 0 {
        notes.lost(Loss::Table(format!(
            "SPECTRAL_WINDOW rows no partition uses ({unused})"
        )));
    }
    for column in ["CHAN_FREQ", "REF_FREQUENCY"] {
        TableMeasDesc::new_variable_int(
            column,
            MeasureType::Frequency,
            "MEAS_FREQ_REF",
            FrequencyRef::ALL
                .iter()
                .map(|refer| refer.as_str().to_owned())
                .collect(),
            FrequencyRef::ALL
                .iter()
                .map(|refer| refer.casacore_code())
                .collect(),
        )
        .and_then(|description| description.write(table))
        .map_err(storage)?;
    }
    Ok(rows)
}

/// What the `STATE` rows written say of each partition, in catalogue
/// order: its `STATE_ID`, -1 for rows without a state, and the `OBS_MODE`
/// the MSv2 reader keys its rows by.
pub(super) struct States {
    pub(super) ids: Vec<i32>,
    pub(super) modes: Vec<String>,
}

/// One `STATE` row per catalogue state, then one per `OBS_MODE` of the
/// partitions that state intents but no state (an MSv4's), their other
/// columns MSv2's defaults. A partition whose rows use several states is
/// written with the first, a loss: a batch does not say which row used
/// which.
pub(super) fn states(
    ms: &mut MeasurementSet,
    catalogue: &Catalogue,
    notes: &mut Notes,
) -> Result<States, ObservationError> {
    let table = subtable_mut(ms, SubtableId::State).map_err(storage)?;
    for state in &catalogue.states {
        let row = row_from_defs(
            schema::state::REQUIRED_COLUMNS,
            &[
                ("OBS_MODE", s(&state.obs_mode)),
                ("CAL", f(notes.or(state.cal_k, "STATE CAL", || 0.0))),
                ("LOAD", f(notes.or(state.load_k, "STATE LOAD", || 0.0))),
                ("SIG", b(notes.or(state.signal, "STATE SIG", || true))),
                ("REF", b(notes.or(state.reference, "STATE REF", || false))),
                (
                    "SUB_SCAN",
                    i(notes.or(state.sub_scan, "STATE SUB_SCAN", || 0)),
                ),
                (
                    "FLAG_ROW",
                    b(notes.or(state.flagged, "STATE FLAG_ROW", || false)),
                ),
            ],
        );
        table.add_row(row).map_err(storage)?;
    }
    let mut stated: Vec<String> = Vec::new();
    let mut states = States {
        ids: Vec::with_capacity(catalogue.partitions.len()),
        modes: Vec::with_capacity(catalogue.partitions.len()),
    };
    for entry in &catalogue.partitions {
        let (id, mode) = match entry.states.as_slice() {
            [] if entry.intents.is_empty() => (-1, String::new()),
            [] => {
                let mode = entry.intents.join(",");
                let index = match stated.iter().position(|known| *known == mode) {
                    Some(index) => index,
                    None => {
                        let row = row_from_defs(
                            schema::state::REQUIRED_COLUMNS,
                            &[
                                ("OBS_MODE", s(&mode)),
                                ("SIG", b(true)),
                                ("REF", b(false)),
                                ("FLAG_ROW", b(false)),
                            ],
                        );
                        table.add_row(row).map_err(storage)?;
                        notes.defaulted("STATE CAL, LOAD, SIG, REF, SUB_SCAN and FLAG_ROW");
                        stated.push(mode.clone());
                        stated.len() - 1
                    }
                };
                ((catalogue.states.len() + index) as i32, mode)
            }
            [first, rest @ ..] => {
                if !rest.is_empty() {
                    notes.lost(Loss::Column(
                        "STATE_ID of rows using several states in one partition (each is \
                         written with the first)"
                            .to_string(),
                    ));
                }
                let state = catalogue.states.get(first.0 as usize).ok_or_else(|| {
                    request_error("a partition names a state the catalogue lacks")
                })?;
                (first.0 as i32, state.obs_mode.clone())
            }
        };
        states.ids.push(id);
        states.modes.push(mode);
    }
    Ok(states)
}

/// One `PROCESSOR` row per catalogue processor, `PASS_ID` among them when
/// some processor states one, and the `PROCESSOR_ID` of each partition: -1
/// for none, as for a store whose `PROCESSOR_ID` names no row.
pub(super) fn processors(
    ms: &mut MeasurementSet,
    catalogue: &Catalogue,
    notes: &mut Notes,
) -> Result<Vec<i32>, ObservationError> {
    let table = subtable_mut(ms, SubtableId::Processor).map_err(storage)?;
    let passes = catalogue
        .processors
        .iter()
        .any(|processor| processor.pass_id.is_some());
    if passes {
        let definition = optional_columns(SubtableId::Processor)
            .iter()
            .find(|definition| definition.name == "PASS_ID")
            .expect("PROCESSOR's PASS_ID");
        table
            .add_column(
                ColumnSchema::scalar(definition.name, definition.data_type),
                None,
            )
            .map_err(storage)?;
    }
    for processor in &catalogue.processors {
        let row = [
            (
                "TYPE",
                s(&notes.or(processor.kind.clone(), "PROCESSOR TYPE", String::new)),
            ),
            (
                "SUB_TYPE",
                s(&notes.or(
                    processor.sub_type.clone(),
                    "PROCESSOR SUB_TYPE",
                    String::new,
                )),
            ),
            (
                "TYPE_ID",
                i(notes.or(processor.type_id, "PROCESSOR TYPE_ID", || -1)),
            ),
            (
                "MODE_ID",
                i(notes.or(processor.mode_id, "PROCESSOR MODE_ID", || -1)),
            ),
            (
                "FLAG_ROW",
                b(notes.or(processor.flagged, "PROCESSOR FLAG_ROW", || false)),
            ),
        ];
        let mut record = row_from_defs(schema::processor::REQUIRED_COLUMNS, &row);
        if passes {
            let pass = notes.or(processor.pass_id, "PROCESSOR PASS_ID", || -1);
            record.push(casa_types::RecordField::new("PASS_ID", i(pass)));
        }
        table.add_row(record).map_err(storage)?;
    }
    Ok(catalogue
        .partitions
        .iter()
        .map(|entry| entry.processor.map_or(-1, |processor| processor.0 as i32))
        .collect())
}
