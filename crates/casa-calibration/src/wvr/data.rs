// SPDX-License-Identifier: LGPL-3.0-or-later
//! Group the MeasurementSet's WVR data by time and antenna.

use casa_core::{ArrayValue, ScalarValue};
use casa_ms::{MeasurementSet, MsError};
use casa_tables::{RequiredScalarColumnValues, Table};

use crate::constants::{COL_ANTENNA1, COL_ANTENNA2, COL_FIELD_ID, COL_FLAG, COL_TIME};

pub(super) type Temperatures = [f64; 4];

pub(super) struct RadiometerIntegration {
    pub time_seconds: f64,
    pub field_id: i32,
    pub source_id: i32,
    pub state_id: i32,
    pub temperatures: Vec<Option<Temperatures>>,
}

#[derive(Clone, Copy)]
pub(super) struct Observation {
    pub time_seconds: f64,
    pub field_id: i32,
    pub source_id: i32,
    pub window_id: i32,
    pub repeated: bool,
}

pub(super) struct RadiometerSeries {
    pub integrations: Vec<RadiometerIntegration>,
    pub sample_counts: Vec<usize>,
    pub valid_counts: Vec<usize>,
    pub fitted_antennas: Vec<bool>,
    pub observations: Vec<Observation>,
    pub statistic_window: Option<i32>,
}

impl RadiometerSeries {
    pub fn read(
        ms: &MeasurementSet,
        wvr_windows: &[i32],
        offsets_table: Option<&Table>,
    ) -> Result<Self, MsError> {
        let main = ms.main_table();
        let antenna_count = ms.antenna()?.row_count();
        let windows = ms.spectral_window()?;
        let data_descriptions = ms.data_description()?;
        // The loader follows the selected four-channel windows through DATA_DESCRIPTION.
        let mut descriptions = vec![None; windows.row_count()];
        let mut window_of_description = Vec::with_capacity(data_descriptions.row_count());
        for desc in 0..data_descriptions.row_count() {
            let window = data_descriptions.spectral_window_id(desc)?;
            window_of_description.push(window);
            let index = usize::try_from(window)
                .ok()
                .filter(|&index| index < windows.row_count())
                .ok_or_else(|| invalid("WVR input has an invalid spectral-window reference"))?;
            if windows.num_chan(index)? == 4
                && (wvr_windows.is_empty() || wvr_windows.contains(&window))
            {
                descriptions[index].get_or_insert(desc as i32);
            }
        }
        let statistic_window = wvr_windows.first().copied().or_else(|| {
            descriptions
                .iter()
                .position(Option::is_some)
                .map(|id| id as i32)
        });
        let descriptions: Vec<_> = descriptions.into_iter().flatten().collect();
        let primary = descriptions
            .iter()
            .min()
            .copied()
            .ok_or_else(|| invalid("No four-channel WVR spectral windows selected"))?;
        let metadata = main.required_scalar_columns_owned(&[
            COL_TIME,
            COL_FIELD_ID,
            COL_ANTENNA1,
            COL_ANTENNA2,
            "DATA_DESC_ID",
            "STATE_ID",
        ])?;
        let RequiredScalarColumnValues::Float64(times) = &metadata[COL_TIME] else {
            return Err(invalid("MeasurementSet TIME is not Float64"));
        };
        let ints = |name| match &metadata[name] {
            RequiredScalarColumnValues::Int32(values) => Ok(values.as_slice()),
            _ => Err(invalid(format!("MeasurementSet {name} is not Int32"))),
        };
        let fields = ints(COL_FIELD_ID)?;
        let antennas = ints(COL_ANTENNA1)?;
        let antenna2 = ints(COL_ANTENNA2)?;
        let descs = ints("DATA_DESC_ID")?;
        let states = ints("STATE_ID")?;
        let mut sorted: Vec<_> = (0..main.row_count())
            .filter(|&row| times[row].is_finite())
            .collect();
        sorted.sort_by(|&left, &right| times[left].total_cmp(&times[right]));
        let (observations, rows) = read_observations(
            ms,
            &sorted,
            times,
            fields,
            descs,
            &descriptions,
            &window_of_description,
        )?;
        let fitted_antennas = wvr_antennas(ms, &rows, wvr_windows, antennas)?;

        let mut offsets = vec![None; rows.len()];
        if let Some(table) = offsets_table {
            let samples = read_offsets(table)?;
            let selected = rows
                .iter()
                .enumerate()
                .filter(|&(_, &row)| antennas[row] == antenna2[row]);
            if !samples.is_empty() && selected.clone().count() != samples.len() {
                return Err(invalid(
                    "Provided Offsets Table number of rows does not match the number of WVR data rows in MS",
                ));
            }
            for ((index, &row), (time, antenna, values)) in selected.zip(samples) {
                if time == times[row] && antenna == antennas[row] {
                    offsets[index] = Some(values);
                }
            }
        }
        let field_table = ms.field()?;
        let mut series = Self {
            integrations: Vec::new(),
            sample_counts: vec![0; antenna_count],
            valid_counts: vec![0; antenna_count],
            fitted_antennas,
            observations,
            statistic_window,
        };
        let rows: Vec<_> = rows.into_iter().zip(offsets).collect();
        for group in rows.chunk_by(|left, right| times[left.0] == times[right.0]) {
            let Some(&(first, _)) = group.iter().find(|entry| descs[entry.0] == primary) else {
                continue;
            };
            let mut unflagged = false;
            for &(row, _) in group.iter().filter(|entry| descs[entry.0] == primary) {
                unflagged |= all_unflagged(main, row)?;
                if unflagged {
                    break;
                }
            }
            if !unflagged {
                continue;
            }
            let mut integration = RadiometerIntegration {
                time_seconds: times[first],
                field_id: fields[first],
                source_id: field_table.source_id(fields[first] as usize)?,
                state_id: states[first],
                temperatures: vec![None; antenna_count],
            };
            for &(row, offset) in group
                .iter()
                .filter(|entry| antennas[entry.0] == antenna2[entry.0])
            {
                let antenna = usize::try_from(antennas[row])
                    .ok()
                    .filter(|&id| id < antenna_count)
                    .ok_or_else(|| invalid("WVR data refer to an unknown antenna"))?;
                series.sample_counts[antenna] += 1;
                let reading = temperatures(main, row, offset.unwrap_or_default())?;
                series.valid_counts[antenna] += usize::from(reading.is_some());
                integration.temperatures[antenna] = reading;
            }
            series.integrations.push(integration);
        }
        Ok(series)
    }
}

fn read_observations(
    ms: &MeasurementSet,
    sorted: &[usize],
    times: &[f64],
    fields: &[i32],
    descriptions: &[i32],
    selected_descriptions: &[i32],
    window_of_description: &[i32],
) -> Result<(Vec<Observation>, Vec<usize>), MsError> {
    let field_table = ms.field()?;
    let sources = (0..field_table.row_count())
        .map(|field| field_table.source_id(field))
        .collect::<Result<Vec<_>, _>>()?;
    let mut observations = Vec::<Observation>::new();
    let mut selected = Vec::new();
    for &row in sorted {
        let field_id = fields[row];
        let source_id = *sources
            .get(field_id as usize)
            .ok_or_else(|| invalid("WVR observation has an unknown FIELD_ID"))?;
        let description_id = descriptions[row];
        let window_id = *window_of_description
            .get(description_id as usize)
            .ok_or_else(|| invalid("WVR observation has an unknown DATA_DESC_ID"))?;
        if selected_descriptions.contains(&description_id) {
            selected.push(row);
        }
        let time_seconds = times[row];
        if let Some(previous) = observations.last_mut().filter(|previous| {
            previous.time_seconds == time_seconds
                && previous.field_id == field_id
                && previous.window_id == window_id
        }) {
            previous.repeated = true;
        } else {
            observations.push(Observation {
                time_seconds,
                field_id,
                source_id,
                window_id,
                repeated: false,
            });
        }
    }
    Ok((observations, selected))
}

fn wvr_antennas(
    ms: &MeasurementSet,
    selected: &[usize],
    windows: &[i32],
    antennas: &[i32],
) -> Result<Vec<bool>, MsError> {
    let mut present = vec![false; ms.antenna()?.row_count()];
    let feed = ms.feed()?;
    let spw = ms.spectral_window()?;
    for row in 0..feed.row_count() {
        let id = feed.i32(row, "SPECTRAL_WINDOW_ID")?;
        let selected = if windows.is_empty() {
            id >= 0 && (id as usize) < spw.row_count() && spw.num_chan(id as usize)? == 4
        } else {
            windows.contains(&id)
        };
        if selected && let Some(value) = present.get_mut(feed.i32(row, "ANTENNA_ID")? as usize) {
            *value = true;
        }
    }
    if !present.iter().any(|&value| value) {
        for &row in selected {
            if let Some(value) = present.get_mut(antennas[row] as usize) {
                *value = true;
            }
        }
    }
    Ok(present)
}

fn all_unflagged(main: &Table, row: usize) -> Result<bool, MsError> {
    let ArrayValue::Bool(flags) = main.cell_accessor(row, COL_FLAG)?.array()? else {
        return Err(invalid("MeasurementSet FLAG is not a Boolean array"));
    };
    Ok(!flags.iter().any(|&flag| flag))
}

fn temperatures(
    main: &Table,
    row: usize,
    offset: Temperatures,
) -> Result<Option<Temperatures>, MsError> {
    if !all_unflagged(main, row)? {
        return Ok(None);
    }
    let ArrayValue::Complex32(data) = main.cell_accessor(row, "DATA")?.array()? else {
        return Err(invalid("WVR DATA is not a Complex32 array"));
    };
    let [correlations, channels] = data.shape() else {
        return Err(invalid("WVR DATA must have correlation and channel axes"));
    };
    if *correlations == 0 || correlations * channels < 4 {
        return Err(invalid("WVR DATA must contain four temperatures"));
    }
    // The first four casacore cells are read in storage order.
    let values = std::array::from_fn(|slot| {
        f64::from(data[[slot % correlations, slot / correlations]].re) - offset[slot]
    });
    Ok(values
        .iter()
        .all(|&value| 2.7 < value && value < 300.0)
        .then_some(values))
}

fn read_offsets(table: &Table) -> Result<Vec<(f64, i32, Temperatures)>, MsError> {
    let mut offsets = Vec::with_capacity(table.row_count());
    for row in 0..table.row_count() {
        let ScalarValue::Float64(time) = table.cell_accessor(row, "TIME")?.scalar()? else {
            return Err(invalid("Offsets Table TIME must be Float64"));
        };
        let ScalarValue::Int32(antenna) = table.cell_accessor(row, "ANTENNA")?.scalar()? else {
            return Err(invalid("Offsets Table ANTENNA must be Int32"));
        };
        let ArrayValue::Float64(values) = table.cell_accessor(row, "OFFSETS")?.array()? else {
            return Err(invalid("Offsets Table OFFSETS must be Float64"));
        };
        if values.shape() != [4] {
            return Err(invalid("Offsets Table requires four offsets per row"));
        }
        let values = std::array::from_fn(|channel| values[[channel]]);
        offsets.push((*time, *antenna, values));
    }
    offsets.sort_by(|left, right| left.0.total_cmp(&right.0));
    Ok(offsets)
}

fn invalid(message: impl Into<String>) -> MsError {
    MsError::InvalidInput(message.into())
}
