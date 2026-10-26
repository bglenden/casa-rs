// SPDX-License-Identifier: LGPL-3.0-or-later
//! The subtables of an MSv2 a conversion creates, from the source's
//! catalogue: one antenna and feed per catalogue antenna, one field per
//! field and one source per source name, one observation per session, and
//! spectral windows, polarizations, data descriptions and states shared by
//! the partitions whose content is the same, so long as no two partitions
//! share a data description, state and observation (an MSv2 would read
//! them back as one). The layout says where each partition's rows point.

use casa_observation::{Catalogue, Correlation, ObservationError, PartitionEntry};
use casa_tables::table_measures::{MeasureType, TableMeasDesc};
use casa_types::measures::direction::DirectionRef;
use casa_types::measures::frequency::FrequencyRef;
use casa_types::quanta::Unit;
use num_complex::Complex32;

use crate::observation_io::update::{request_error, storage};
use crate::schema::{self, SubtableId};
use crate::subtable_rows::{
    b, complex_array, f, f64_array, i, i32_array, row_from_defs, s, string_array, subtable_mut,
};
use crate::{MeasurementSet, MeasurementSetBuilder, OptionalMainColumn};

/// The MeasurementSet to create for `catalogue` with the visibility
/// `columns`: a `SOURCE` subtable when some field names its source.
pub(super) fn builder(
    catalogue: &Catalogue,
    columns: &[OptionalMainColumn],
) -> MeasurementSetBuilder {
    let builder = columns
        .iter()
        .fold(MeasurementSetBuilder::new(), |builder, column| {
            builder.with_main_column(*column)
        });
    if catalogue
        .fields
        .iter()
        .any(|field| field.source_name.is_some())
    {
        builder.with_optional_subtable(SubtableId::Source)
    } else {
        builder
    }
}

/// Where each partition's rows point, in catalogue partition order.
pub(super) struct Layout {
    /// `DATA_DESC_ID` of each partition.
    pub(super) data_description: Vec<i32>,
    /// `STATE_ID` of each partition.
    pub(super) state: Vec<i32>,
    /// `SCAN_NUMBER` of each catalogue scan.
    pub(super) scan_number: Vec<i32>,
}

/// Fill `ms`'s subtables from `catalogue`, whose data span `time_range`
/// (MJD seconds).
pub(super) fn populate(
    ms: &mut MeasurementSet,
    catalogue: &Catalogue,
    time_range: [f64; 2],
) -> Result<Layout, ObservationError> {
    antennas(ms, catalogue, time_range)?;
    fields(ms, catalogue, time_range)?;
    observations(ms, catalogue, time_range)?;
    let scan_number = catalogue
        .scans
        .iter()
        .map(|scan| {
            scan.number.ok_or_else(|| {
                request_error(format!(
                    "scan {} has no casacore scan number to write as SCAN_NUMBER",
                    scan.name
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut windows: Vec<&PartitionEntry> = Vec::new();
    let mut polarizations: Vec<&[Correlation]> = Vec::new();
    let mut descriptions: Vec<(usize, usize)> = Vec::new();
    let mut states: Vec<&[String]> = Vec::new();
    let mut keys: Vec<(usize, usize, u32)> = Vec::new();
    let mut layout = Layout {
        data_description: Vec::new(),
        state: Vec::new(),
        scan_number,
    };
    for entry in &catalogue.partitions {
        let window = position_or_push(&mut windows, entry, |known| same_window(known, entry));
        let polarization = position_or_push(&mut polarizations, &entry.correlations, |known| {
            *known == entry.correlations.as_slice()
        });
        let state = position_or_push(&mut states, &entry.intents, |known| {
            *known == entry.intents.as_slice()
        });
        // Partitions an MSv2 keys alike (data description, state,
        // observation) would read back as one: a later one gets a data
        // description of its own.
        let key = |description| (description, state, entry.observation.0);
        let description = (0..descriptions.len())
            .find(|index| {
                descriptions[*index] == (window, polarization) && !keys.contains(&key(*index))
            })
            .unwrap_or_else(|| {
                descriptions.push((window, polarization));
                descriptions.len() - 1
            });
        keys.push(key(description));
        layout.data_description.push(description as i32);
        layout.state.push(state as i32);
    }
    spectral_windows(ms, &windows)?;
    for correlations in &polarizations {
        polarization(ms, correlations)?;
    }
    for (window, polarization) in &descriptions {
        subtable_mut(ms, SubtableId::DataDescription)
            .and_then(|table| {
                table
                    .add_row(row_from_defs(
                        schema::data_description::REQUIRED_COLUMNS,
                        &[
                            ("SPECTRAL_WINDOW_ID", i(*window as i32)),
                            ("POLARIZATION_ID", i(*polarization as i32)),
                            ("FLAG_ROW", b(false)),
                        ],
                    ))
                    .map_err(Into::into)
            })
            .map_err(storage)?;
    }
    for intents in &states {
        subtable_mut(ms, SubtableId::State)
            .and_then(|table| {
                table
                    .add_row(row_from_defs(
                        schema::state::REQUIRED_COLUMNS,
                        &[
                            ("OBS_MODE", s(&intents.join(","))),
                            ("SIG", b(true)),
                            ("REF", b(false)),
                            ("FLAG_ROW", b(false)),
                        ],
                    ))
                    .map_err(Into::into)
            })
            .map_err(storage)?;
    }
    subtable_mut(ms, SubtableId::Processor)
        .and_then(|table| {
            table
                .add_row(row_from_defs(
                    schema::processor::REQUIRED_COLUMNS,
                    &[
                        ("TYPE", s("CORRELATOR")),
                        ("TYPE_ID", i(-1)),
                        ("MODE_ID", i(-1)),
                        ("FLAG_ROW", b(false)),
                    ],
                ))
                .map_err(Into::into)
        })
        .map_err(storage)?;
    Ok(layout)
}

/// The index of the first of `known` that `is` matches, after pushing
/// `item` when none does.
fn position_or_push<T: Copy>(known: &mut Vec<T>, item: T, is: impl Fn(&T) -> bool) -> usize {
    known.iter().position(is).unwrap_or_else(|| {
        known.push(item);
        known.len() - 1
    })
}

/// Whether two partitions have one spectral window: the same name,
/// channels (bit for bit) and frame.
fn same_window(a: &PartitionEntry, b: &PartitionEntry) -> bool {
    let bits = |values: &[f64]| values.iter().map(|v| v.to_bits()).collect::<Vec<_>>();
    a.spectral_window == b.spectral_window
        && a.frequency_frame == b.frequency_frame
        && bits(&a.frequencies_hz) == bits(&b.frequencies_hz)
        && bits(&a.channel_widths_hz) == bits(&b.channel_widths_hz)
}

fn antennas(
    ms: &mut MeasurementSet,
    catalogue: &Catalogue,
    time_range: [f64; 2],
) -> Result<(), ObservationError> {
    let metres = Unit::new("m").expect("metres");
    let radians = Unit::new("rad").expect("radians");
    for antenna in &catalogue.antennas {
        let diameter = antenna
            .dish_diameter
            .get_value_in(&metres)
            .map_err(|_| request_error("a dish diameter is not a length"))?;
        let position = antenna.position.values();
        ms.antenna_mut()
            .and_then(|mut table| {
                table.add_antenna(
                    &antenna.name,
                    &antenna.station,
                    "GROUND-BASED",
                    &antenna.mount,
                    [position[0], position[1], position[2]],
                    [0.0; 3],
                    diameter,
                )
            })
            .map_err(storage)?;
    }
    for (index, antenna) in catalogue.antennas.iter().enumerate() {
        let receptors = antenna.receptors.len();
        let angles = antenna
            .receptor_angles
            .iter()
            .map(|angle| {
                angle
                    .get_value_in(&radians)
                    .map_err(|_| request_error("a receptor angle is not an angle"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let types: Vec<&str> = antenna.receptors.iter().map(String::as_str).collect();
        let identity: Vec<Complex32> = (0..receptors * receptors)
            .map(|k| {
                if k % (receptors + 1) == 0 {
                    Complex32::new(1.0, 0.0)
                } else {
                    Complex32::new(0.0, 0.0)
                }
            })
            .collect();
        let row = row_from_defs(
            schema::feed::REQUIRED_COLUMNS,
            &[
                ("ANTENNA_ID", i(index as i32)),
                ("BEAM_ID", i(-1)),
                (
                    "BEAM_OFFSET",
                    f64_array(&vec![0.0; 2 * receptors], vec![2, receptors]),
                ),
                ("FEED_ID", i(0)),
                ("INTERVAL", f(time_range[1] - time_range[0])),
                ("NUM_RECEPTORS", i(receptors as i32)),
                (
                    "POL_RESPONSE",
                    complex_array(&identity, vec![receptors, receptors]),
                ),
                ("POLARIZATION_TYPE", string_array(&types, vec![receptors])),
                ("POSITION", f64_array(&[0.0; 3], vec![3])),
                ("RECEPTOR_ANGLE", f64_array(&angles, vec![receptors])),
                ("SPECTRAL_WINDOW_ID", i(-1)),
                ("TIME", f(0.5 * (time_range[0] + time_range[1]))),
            ],
        );
        subtable_mut(ms, SubtableId::Feed)
            .and_then(|table| table.add_row(row).map_err(Into::into))
            .map_err(storage)?;
    }
    Ok(())
}

fn fields(
    ms: &mut MeasurementSet,
    catalogue: &Catalogue,
    time_range: [f64; 2],
) -> Result<(), ObservationError> {
    let time = time_range[0];
    let mut sources: Vec<(&str, [f64; 2])> = Vec::new();
    for field in &catalogue.fields {
        if field.phase_centre.refer() != DirectionRef::J2000 {
            return Err(ObservationError::Unsupported {
                what: "writing MSv2 fields in another frame than J2000",
            });
        }
        let (longitude, latitude) = field.phase_centre.as_angles();
        let source = field.source_name.as_deref().map_or(-1, |name| {
            position_or_push(&mut sources, (name, [longitude, latitude]), |known| {
                known.0 == name
            }) as i32
        });
        let direction = f64_array(&[longitude, latitude], vec![2, 1]);
        let row = row_from_defs(
            schema::field::REQUIRED_COLUMNS,
            &[
                ("NAME", s(&field.name)),
                ("CODE", s("")),
                ("NUM_POLY", i(0)),
                ("DELAY_DIR", direction.clone()),
                ("PHASE_DIR", direction.clone()),
                ("REFERENCE_DIR", direction),
                ("SOURCE_ID", i(source)),
                ("TIME", f(time)),
                ("FLAG_ROW", b(false)),
            ],
        );
        subtable_mut(ms, SubtableId::Field)
            .and_then(|table| table.add_row(row).map_err(Into::into))
            .map_err(storage)?;
    }
    // One SOURCE row per source name, for every spectral window, at the
    // direction of its first field: what the catalogue says of a source.
    for (id, (name, direction)) in sources.iter().enumerate() {
        let row = row_from_defs(
            schema::source::REQUIRED_COLUMNS,
            &[
                ("CALIBRATION_GROUP", i(-1)),
                ("CODE", s("")),
                ("DIRECTION", f64_array(direction, vec![2])),
                ("INTERVAL", f(time_range[1] - time_range[0])),
                ("NAME", s(name)),
                ("NUM_LINES", i(0)),
                ("PROPER_MOTION", f64_array(&[0.0, 0.0], vec![2])),
                ("SOURCE_ID", i(id as i32)),
                ("SPECTRAL_WINDOW_ID", i(-1)),
                ("TIME", f(0.5 * (time_range[0] + time_range[1]))),
            ],
        );
        subtable_mut(ms, SubtableId::Source)
            .and_then(|table| table.add_row(row).map_err(Into::into))
            .map_err(storage)?;
    }
    Ok(())
}

fn observations(
    ms: &mut MeasurementSet,
    catalogue: &Catalogue,
    time_range: [f64; 2],
) -> Result<(), ObservationError> {
    for observation in &catalogue.observations {
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
        let row = row_from_defs(
            schema::observation::REQUIRED_COLUMNS,
            &[
                ("FLAG_ROW", b(false)),
                ("LOG", string_array(&[], vec![0])),
                ("OBSERVER", s(&observation.observers.join(", "))),
                ("PROJECT", s(&observation.project)),
                ("RELEASE_DATE", f(0.0)),
                ("SCHEDULE", string_array(&schedule, vec![schedule.len()])),
                ("SCHEDULE_TYPE", s("")),
                ("TELESCOPE_NAME", s(&observation.telescope)),
                ("TIME_RANGE", f64_array(&time_range, vec![2])),
            ],
        );
        subtable_mut(ms, SubtableId::Observation)
            .and_then(|table| table.add_row(row).map_err(Into::into))
            .map_err(storage)?;
    }
    Ok(())
}

fn spectral_windows(
    ms: &mut MeasurementSet,
    windows: &[&PartitionEntry],
) -> Result<(), ObservationError> {
    let table = subtable_mut(ms, SubtableId::SpectralWindow).map_err(storage)?;
    for entry in windows {
        let channels = entry.frequencies_hz.len();
        let widths = &entry.channel_widths_hz;
        let magnitudes: Vec<f64> = widths.iter().map(|width| width.abs()).collect();
        table
            .add_row(row_from_defs(
                schema::spectral_window::REQUIRED_COLUMNS,
                &[
                    ("NUM_CHAN", i(channels as i32)),
                    ("NAME", s(&entry.spectral_window)),
                    ("REF_FREQUENCY", f(entry.frequencies_hz[0])),
                    ("TOTAL_BANDWIDTH", f(magnitudes.iter().sum())),
                    (
                        "CHAN_FREQ",
                        f64_array(&entry.frequencies_hz, vec![channels]),
                    ),
                    ("CHAN_WIDTH", f64_array(widths, vec![channels])),
                    ("EFFECTIVE_BW", f64_array(&magnitudes, vec![channels])),
                    ("RESOLUTION", f64_array(&magnitudes, vec![channels])),
                    ("MEAS_FREQ_REF", i(entry.frequency_frame.casacore_code())),
                    ("NET_SIDEBAND", i(if widths[0] >= 0.0 { 1 } else { -1 })),
                    ("FREQ_GROUP", i(0)),
                    ("FREQ_GROUP_NAME", s("")),
                    ("IF_CONV_CHAIN", i(0)),
                    ("FLAG_ROW", b(false)),
                ],
            ))
            .map_err(storage)?;
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
    Ok(())
}

/// The receptor pair of a correlation product of a two-receptor feed.
fn receptors(correlation: Correlation) -> Result<[i32; 2], ObservationError> {
    use Correlation::{LL, LR, RL, RR, XX, XY, YX, YY};
    Ok(match correlation {
        RR | XX => [0, 0],
        RL | XY => [0, 1],
        LR | YX => [1, 0],
        LL | YY => [1, 1],
        _ => {
            return Err(ObservationError::Unsupported {
                what: "writing MSv2 correlation products other than a feed pair's",
            });
        }
    })
}

fn polarization(
    ms: &mut MeasurementSet,
    correlations: &[Correlation],
) -> Result<(), ObservationError> {
    let count = correlations.len();
    let types: Vec<i32> = correlations
        .iter()
        .map(|correlation| correlation.casacore_code())
        .collect();
    let mut products = vec![0; 2 * count];
    for (index, correlation) in correlations.iter().enumerate() {
        let [first, second] = receptors(*correlation)?;
        products[index] = first;
        products[count + index] = second;
    }
    let row = row_from_defs(
        schema::polarization::REQUIRED_COLUMNS,
        &[
            ("NUM_CORR", i(count as i32)),
            ("CORR_TYPE", i32_array(&types, vec![count])),
            ("CORR_PRODUCT", i32_array(&products, vec![2, count])),
            ("FLAG_ROW", b(false)),
        ],
    );
    subtable_mut(ms, SubtableId::Polarization)
        .and_then(|table| table.add_row(row).map_err(Into::into))
        .map_err(storage)
}

#[cfg(test)]
mod tests {
    use super::position_or_push;

    #[test]
    fn equal_content_shares_one_row() {
        let mut known = Vec::new();
        assert_eq!(position_or_push(&mut known, 3, |k| *k == 3), 0);
        assert_eq!(position_or_push(&mut known, 5, |k| *k == 5), 1);
        assert_eq!(position_or_push(&mut known, 3, |k| *k == 3), 0);
        assert_eq!(known, [3, 5]);
    }
}
