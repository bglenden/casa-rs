// SPDX-License-Identifier: LGPL-3.0-or-later
//! Position-switch sky spectra from autocorrelation OFF integrations.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use casa_ms::MeasurementSet;
use casa_tables::{RequiredScalarColumnValues, Table};
use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, Axis, IxDyn, Zip};

use crate::CalibrationOutputMode;
use crate::constants::*;
use crate::session::scan_timing::ScanTiming;
use crate::table_maintenance::write_standard_calibration_subtables;
use crate::writer::{
    CalibrationTableDescriptor, CalibrationTableWriter, new_calibration_table_schema,
};

pub(super) const SUBTYPE: &str = "SDSKY_PS";

/// CASA groups position-switch reference rows by these columns even when
/// there is a gap between integrations belonging to the same group.
#[derive(Ord, PartialOrd, Eq, PartialEq)]
struct ReferenceGroup {
    observation: i32,
    processor: i32,
    field: i32,
    antenna: i32,
    feed: i32,
    data_description: i32,
    scan: i32,
    state: i32,
}

#[derive(Default)]
struct Integrations {
    count: usize,
    time_sum: f64,
    exposure_sum: f64,
}

impl Integrations {
    fn add(&mut self, time: f64, exposure: f64) {
        self.count += 1;
        self.time_sum += time;
        self.exposure_sum += exposure;
    }
}

struct SkySpectrum {
    spw: usize,
    signal: ArrayD<f32>,
    exposure: ArrayD<f32>,
    all: Integrations,
    usable: Integrations,
}

impl SkySpectrum {
    fn new(spw: usize, template: &ArrayD<f32>) -> Self {
        Self {
            spw,
            signal: ArrayD::zeros(template.raw_dim()),
            exposure: ArrayD::zeros(template.raw_dim()),
            all: Integrations::default(),
            usable: Integrations::default(),
        }
    }

    fn add(
        &mut self,
        data: &ArrayD<f32>,
        flags: &ArrayD<bool>,
        time: f64,
        exposure: f64,
    ) -> Result<(), String> {
        if self.signal.shape() != data.shape()
            || data.shape() != flags.shape()
            || data.ndim() != 2
            || !(1..=2).contains(&data.shape()[0])
        {
            return Err("Position-switch data and FLAG shapes disagree.".into());
        }
        self.all.add(time, exposure);
        let mut has_usable_channel = false;
        Zip::from(&mut self.signal)
            .and(&mut self.exposure)
            .and(data)
            .and(flags)
            .for_each(|signal, weight, &power, &flagged| {
                if !flagged {
                    *signal += exposure as f32 * power;
                    *weight += exposure as f32;
                    has_usable_channel = true;
                }
            });
        if has_usable_channel {
            self.usable.add(time, exposure);
        }
        Ok(())
    }

    fn finish(mut self, antenna: i32, scans: &ScanTiming, channel_width: f64) -> RecordValue {
        let stamps = if self.usable.count > 0 {
            &self.usable
        } else {
            &self.all
        };
        let time = stamps.time_sum / stamps.count as f64;
        let interval = stamps.exposure_sum;
        let scan = scans.at(time);
        Zip::from(&mut self.signal)
            .and(&self.exposure)
            .for_each(|signal, &exposure| {
                if exposure != 0. {
                    *signal /= exposure;
                }
            });
        let input_shape = self.signal.raw_dim();
        let signal = two_receptors(self.signal, 0.);
        let shape = signal.raw_dim();
        let flags = two_receptors(self.exposure.mapv(|exposure| exposure == 0.), true);
        let error = two_receptors(ArrayD::from_elem(input_shape.clone(), 0.1), 0.);
        let snr = two_receptors(ArrayD::from_elem(input_shape, 1.), 0.);
        let integer =
            |name, value| RecordField::new(name, Value::Scalar(ScalarValue::Int32(value)));
        let number =
            |name, value| RecordField::new(name, Value::Scalar(ScalarValue::Float64(value)));
        let array = |name, array| RecordField::new(name, Value::Array(array));
        RecordValue::new(vec![
            number(COL_TIME, time),
            number(COL_INTERVAL, interval),
            integer(COL_ANTENNA1, antenna),
            integer(COL_ANTENNA2, antenna),
            integer(COL_FIELD_ID, scan.field),
            integer(COL_SCAN_NUMBER, scan.number),
            integer(COL_OBSERVATION_ID, scan.observation),
            integer(COL_SPECTRAL_WINDOW_ID, self.spw as i32),
            array(COL_FPARAM, ArrayValue::Float32(signal)),
            array(COL_FLAG, ArrayValue::Bool(flags)),
            array(COL_PARAMERR, ArrayValue::Float32(error)),
            array(COL_SNR, ArrayValue::Float32(snr)),
            array(
                COL_WEIGHT,
                ArrayValue::Float32(ArrayD::from_elem(
                    shape,
                    (channel_width.abs().trunc() * interval) as f32,
                )),
            ),
        ])
    }
}

fn two_receptors<T: Clone>(one_or_two: ArrayD<T>, fill: T) -> ArrayD<T> {
    if one_or_two.shape()[0] == 2 {
        return one_or_two;
    }
    let mut two = ArrayD::from_elem(IxDyn(&[2, one_or_two.shape()[1]]), fill);
    two.index_axis_mut(Axis(0), 0)
        .assign(&one_or_two.index_axis(Axis(0), 0));
    two
}

pub(super) fn generate(
    ms: &MeasurementSet,
    rows: &[usize],
    destination: &Path,
    overwrite: bool,
) -> Result<usize, String> {
    let solutions = solve(ms, rows)?;
    let scans = ScanTiming::read(ms.main_table())?;
    let spectral = ms.spectral_window().map_err(error_text)?;
    let mode = if overwrite {
        CalibrationOutputMode::Replace
    } else {
        CalibrationOutputMode::Create
    };
    let mut writer = CalibrationTableWriter::with_mode(
        ms,
        CalibrationTableDescriptor {
            output: destination,
            schema: new_calibration_table_schema(false),
            subtype: SUBTYPE,
            parameter_type: Some("Float"),
            measurement_set_name: ms
                .path()
                .map_or_else(|| "none".into(), |path| path.display().to_string()),
            include_polarization_basis: true,
            time_extra_precision_column: None,
        },
        mode,
    )
    .map_err(error_text)?;
    let rows = solutions.len();
    let present_windows = solutions
        .values()
        .map(|spectrum| spectrum.spw as i32)
        .collect::<BTreeSet<_>>();
    let mut channel_widths = BTreeMap::new();
    for (group, spectrum) in solutions {
        let width = match channel_widths.entry(spectrum.spw) {
            std::collections::btree_map::Entry::Occupied(entry) => *entry.get(),
            std::collections::btree_map::Entry::Vacant(entry) => {
                let width = spectral
                    .chan_width(spectrum.spw)
                    .map_err(error_text)?
                    .first()
                    .copied()
                    .ok_or("Missing channel width for position-switch calibration.")?;
                *entry.insert(width)
            }
        };
        writer
            .append(spectrum.finish(group.antenna, &scans, width))
            .map_err(error_text)?;
    }
    write_standard_calibration_subtables(ms, writer.output_path(), None)?;
    crate::gencal::write_spectral_axis(
        &writer.output_path().join("SPECTRAL_WINDOW"),
        &present_windows,
        false,
    )
    .map_err(error_text)?;
    writer.save_main().map_err(error_text)?;
    Ok(rows)
}

fn solve(
    ms: &MeasurementSet,
    rows: &[usize],
) -> Result<BTreeMap<ReferenceGroup, SkySpectrum>, String> {
    let main = ms.main_table();
    let columns = main
        .required_scalar_columns_owned_for_rows(
            &[
                COL_OBSERVATION_ID,
                "PROCESSOR_ID",
                COL_FIELD_ID,
                COL_ANTENNA1,
                COL_ANTENNA2,
                "FEED1",
                "FEED2",
                "DATA_DESC_ID",
                COL_SCAN_NUMBER,
                "STATE_ID",
                COL_TIME,
                "EXPOSURE",
            ],
            rows,
        )
        .map_err(error_text)?;
    let ids = |column: &str| match &columns[column] {
        RequiredScalarColumnValues::Int32(values) => Ok(values.as_slice()),
        _ => Err(format!("MeasurementSet {column} is not Int32.")),
    };
    let [
        observation,
        processor,
        field,
        antenna,
        other_antenna,
        feed,
        other_feed,
        description,
        scan,
        state,
    ] = [
        ids(COL_OBSERVATION_ID)?,
        ids("PROCESSOR_ID")?,
        ids(COL_FIELD_ID)?,
        ids(COL_ANTENNA1)?,
        ids(COL_ANTENNA2)?,
        ids("FEED1")?,
        ids("FEED2")?,
        ids("DATA_DESC_ID")?,
        ids(COL_SCAN_NUMBER)?,
        ids("STATE_ID")?,
    ];
    let (
        RequiredScalarColumnValues::Float64(times),
        RequiredScalarColumnValues::Float64(exposures),
    ) = (&columns[COL_TIME], &columns["EXPOSURE"])
    else {
        return Err("MeasurementSet TIME or EXPOSURE is not Float64.".into());
    };
    let (state_table, observations, descriptions, spectral) = (
        ms.state().map_err(error_text)?,
        ms.observation().map_err(error_text)?,
        ms.data_description().map_err(error_text)?,
        ms.spectral_window().map_err(error_text)?,
    );
    let off_state = (0..state_table.row_count())
        .map(|row| {
            state_table.string(row, "OBS_MODE").map(|mode| {
                mode.to_ascii_uppercase()
                    .starts_with("OBSERVE_TARGET#OFF_SOURCE")
            })
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(error_text)?;
    let alma = (0..observations.row_count())
        .map(|row| {
            observations
                .string(row, "TELESCOPE_NAME")
                .map(|name| name == "ALMA")
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(error_text)?;
    let windows = (0..descriptions.row_count())
        .map(|row| descriptions.spectral_window_id(row))
        .collect::<Result<Vec<_>, _>>()
        .map_err(error_text)?;
    let wvr = (0..spectral.row_count())
        .map(|row| spectral.num_chan(row).map(|channels| channels == 4))
        .collect::<Result<Vec<_>, _>>()
        .map_err(error_text)?;
    let data_column = if main
        .schema()
        .is_some_and(|schema| schema.contains_column("FLOAT_DATA"))
    {
        "FLOAT_DATA"
    } else {
        "DATA"
    };
    let mut solutions = BTreeMap::new();
    for (index, &row) in rows.iter().enumerate() {
        if antenna[index] != other_antenna[index]
            || feed[index] != other_feed[index]
            || off_state.get(state[index] as usize) != Some(&true)
        {
            continue;
        }
        let spw = *windows
            .get(description[index] as usize)
            .ok_or("Invalid DATA_DESC_ID in reference data.")? as usize;
        let water_vapour = *wvr.get(spw).ok_or("Invalid reference spectral window.")?;
        if water_vapour && alma.get(observation[index] as usize) == Some(&true) {
            continue;
        }
        let group = ReferenceGroup {
            observation: observation[index],
            processor: processor[index],
            field: field[index],
            antenna: antenna[index],
            feed: feed[index],
            data_description: description[index],
            scan: scan[index],
            state: state[index],
        };
        let power = spectrum(main, row, data_column)?;
        let ArrayValue::Bool(flags) = main
            .cell_accessor(row, "FLAG")
            .and_then(|cell| cell.array())
            .map_err(error_text)?
        else {
            return Err("MeasurementSet FLAG is not Boolean.".into());
        };
        let time = times[index];
        let exposure = exposures[index];
        if !time.is_finite() || !exposure.is_finite() {
            return Err("Invalid reference integration time or exposure.".into());
        }
        solutions
            .entry(group)
            .or_insert_with(|| SkySpectrum::new(spw, &power))
            .add(&power, flags, time, exposure)?;
    }
    if solutions.is_empty() {
        return Err("No reference integration found in user-selected data. Please double-check your data selection criteria.".into());
    }
    Ok(solutions)
}

fn spectrum<'a>(main: &'a Table, row: usize, column: &str) -> Result<Cow<'a, ArrayD<f32>>, String> {
    let value = main
        .cell_accessor(row, column)
        .and_then(|cell| cell.array())
        .map_err(error_text)?;
    match value {
        ArrayValue::Float32(data) => Ok(Cow::Borrowed(data)),
        ArrayValue::Complex32(data) => Ok(Cow::Owned(data.mapv(|value| value.re))),
        _ => Err(format!(
            "MeasurementSet {column} is not single-precision visibility data."
        )),
    }
}

fn error_text(error: impl std::fmt::Display) -> String {
    error.to_string()
}
