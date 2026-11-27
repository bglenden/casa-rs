// SPDX-License-Identifier: LGPL-3.0-or-later
//! Cumulative antenna gain tables on existing or MeasurementSet-derived time grids.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use casa_core::{ArrayValue, Complex32, RecordField, RecordValue, ScalarValue, Value};
use casa_ms::{MeasurementSet, MsSelection};
use casa_tables::{Table, TableOptions, TableWriteLock};
use ndarray::{ArrayD, IxDyn};

use crate::constants::{
    COL_ANTENNA1, COL_ANTENNA2, COL_CPARAM, COL_FIELD_ID, COL_FLAG, COL_INTERVAL,
    COL_OBSERVATION_ID, COL_PARAMERR, COL_SCAN_NUMBER, COL_SNR, COL_SPECTRAL_WINDOW_ID, COL_TIME,
};
use crate::execute::{LoadedCalibrationTable, load_interpolated_calibration_table};
use crate::lookup::{ComplexSolution, TimeQuery, spectral_window_path};
use crate::lookup_frequency::{relative_axis, relative_axis_at_center};
use crate::solution_edit::{edit_calibration_table, error_text, update_spectral_window_flags};
use crate::solve::grouping::collect_selected_rows;
use crate::table_maintenance::{prepare_empty_calibration_table, repair_observation_metadata};
use crate::writer::SingleChannelFrequency;
use crate::{
    apply::{
        ApplyCalibrationTablePlan, ApplyCalibrationTableSpec, ApplyInterpolationMode,
        GainFieldSelector,
    },
    plan,
    table::summarize_table,
};

/// Tables, selectors, and sampling for CASA's `calibrater.accumulate`.
pub struct CalibrationAccumulationRequest {
    /// Existing cumulative input when `interval_seconds` is negative.
    pub table_in: PathBuf,
    /// Incremental antenna calibration to multiply into the cumulative table.
    pub incremental: PathBuf,
    /// Result table. An empty path uses `table_in`.
    pub table_out: PathBuf,
    /// Cumulative fields to modify. Empty selects every field.
    pub field: String,
    /// Incremental fields to interpolate. Empty uses all solution fields.
    pub calibration_field: String,
    /// CASA time and frequency interpolation expression.
    pub interpolation: String,
    /// Negative reuses `table_in`. Zero or positive builds a cumulative time grid.
    pub interval_seconds: f64,
    /// Map from cumulative spectral windows to incremental ones. Negative is identity.
    pub spectral_window_map: Vec<i32>,
}

/// Apply incremental calibration to an existing cumulative table or to a new
/// time grid on the MeasurementSet.
///
/// # Errors
/// Returns a description of selection, table, interpolation, or output errors.
pub fn accumulate_calibration(
    ms: &MeasurementSet,
    selection: &MsSelection,
    request: &CalibrationAccumulationRequest,
) -> Result<(), String> {
    if !request.interval_seconds.is_finite() {
        return Err("accumulation time must be finite".into());
    }
    let destination = if request.table_out.as_os_str().is_empty() {
        &request.table_in
    } else {
        &request.table_out
    };
    if destination.as_os_str().is_empty() {
        return Err("an accumulation output table is required".into());
    }
    let fields = ms
        .resolve_field_selector(&request.field)
        .map_err(error_text)?;
    let summary = summarize_table(&request.incremental).map_err(error_text)?;
    let interpolation = ApplyInterpolationMode::parse_casa(&request.interpolation)?;
    let mut spec = ApplyCalibrationTableSpec::new(&request.incremental);
    let spectrum = ms.spectral_window().map_err(error_text)?;
    let window_count = spectrum.row_count();
    if request.spectral_window_map.len() > window_count {
        return Err("spwmap has more entries than the MeasurementSet has spectral windows".into());
    }
    spec.spwmap.clone_from(&request.spectral_window_map);
    spec.interp = interpolation;
    spec.gainfield = match request.calibration_field.as_str() {
        "" => None,
        "nearest" => Some(GainFieldSelector::Nearest),
        selection => Some(GainFieldSelector::Expression(selection.to_string())),
    };
    let ms_fields = (0..ms.field().map_err(error_text)?.row_count())
        .map(|id| i32::try_from(id).map_err(error_text))
        .collect::<Result<Vec<_>, _>>()?;
    let (resolved_gainfields, resolved_nearest_gainfields) =
        plan::resolve_gainfield(ms, &spec, &summary, &ms_fields).map_err(error_text)?;
    let nearest = resolved_nearest_gainfields
        .iter()
        .map(|mapping| {
            (
                mapping.measurement_set_field_id,
                mapping.calibration_field_id,
            )
        })
        .collect::<BTreeMap<_, _>>();
    let subtype = summary.table_subtype.clone();
    let (receptors, channelized) = match subtype.as_str() {
        "G Jones" => (2, false),
        "T Jones" => (1, false),
        "B Jones" => (2, true),
        "Tf Jones" => (1, true),
        _ => {
            return Err(format!(
                "calibration type {subtype} does not support accumulation"
            ));
        }
    };
    let plan = ApplyCalibrationTablePlan {
        applicable_row_indices: None,
        unavailable_row_indices: Vec::new(),
        spec,
        applicable_selected_row_count: 0,
        summary,
        resolved_gainfields,
        resolved_nearest_gainfields,
        spw_mapping: Vec::new(),
        calibration_spectral_windows: Vec::new(),
        interp: interpolation,
        calwt: false,
    };
    let lookup = load_interpolated_calibration_table(&plan, None, ms).map_err(error_text)?;
    let spectral = spectral_window_path(&request.incremental)
        .map_err(error_text)?
        .ok_or("incremental calibration table has no spectral windows")?;
    let data_centers = (0..window_count)
        .map(|row| {
            let channels = spectrum.chan_freq(row).map_err(error_text)?;
            Ok(channels.iter().sum::<f64>() / channels.len() as f64)
        })
        .collect::<Result<Vec<_>, String>>()?;
    let increment = Increment {
        available_windows: lookup.interpolation_windows(),
        data_centers,
        lookup,
        frequencies: table_frequencies(&spectral)?,
        fields: &fields,
        nearest,
        spec: plan.spec,
        interpolation,
    };
    if request.interval_seconds < 0.0 {
        return edit_calibration_table(&request.table_in, destination, |table, stage| {
            if table.info().sub_type != subtype {
                return Err("cumulative and incremental calibration table types differ".into());
            }
            repair_observation_metadata(table, stage)?;
            increment.apply(table, stage)
        });
    }

    let mut writer = prepare_empty_calibration_table(
        ms,
        destination,
        "Complex",
        &subtype,
        (!channelized).then_some(SingleChannelFrequency::Mean),
    )?;
    let _destination_lock = if destination.exists() {
        Some(TableWriteLock::acquire_to_reread(destination, 1).map_err(error_text)?)
    } else {
        None
    };
    let stage = writer.output_path().to_path_buf();
    populate_time_grid(
        ms,
        selection,
        request.interval_seconds,
        receptors,
        channelized,
        &mut writer,
    )?;
    increment.apply(writer.table_mut(), &stage)?;
    update_spectral_window_flags(writer.table_mut(), &stage)?;
    writer.save_main().map_err(error_text)
}

struct Increment<'a> {
    lookup: LoadedCalibrationTable,
    available_windows: Vec<i32>,
    data_centers: Vec<f64>,
    nearest: BTreeMap<i32, i32>,
    frequencies: Vec<Vec<f64>>,
    fields: &'a [i32],
    spec: ApplyCalibrationTableSpec,
    interpolation: ApplyInterpolationMode,
}

impl Increment<'_> {
    fn apply(&self, table: &mut Table, path: &Path) -> Result<(), String> {
        let mut groups = BTreeMap::<_, Vec<_>>::new();
        for row in 0..table.row_count() {
            let window = read_i32(table, row, COL_SPECTRAL_WINDOW_ID)?;
            let time = read_time(table, row)?;
            let field = read_i32(table, row, COL_FIELD_ID)?;
            groups
                .entry((window, time.to_bits(), field))
                .or_default()
                .push(row);
        }
        let output_frequencies = table_frequencies(&path.join("SPECTRAL_WINDOW"))?;
        let selected = groups
            .keys()
            .map(|&(window, _, _)| window)
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let mapping = plan::resolve_spw_mapping(
            &self.spec,
            &self.available_windows,
            self.frequencies.len(),
            &selected,
        )
        .map_err(error_text)?
        .into_iter()
        .map(|mapping| (mapping.data_spw_id, mapping.calibration_spw_id))
        .collect::<BTreeMap<_, _>>();
        for ((window, time, field), rows) in groups {
            if !self.fields.is_empty() && self.fields.binary_search(&field).is_err() {
                continue;
            }
            let target = usize::try_from(window).map_err(error_text)?;
            let mapped = mapping[&window];
            if self.available_windows.binary_search(&mapped).is_err() {
                continue;
            }
            let source = usize::try_from(mapped).map_err(error_text)?;
            let axis = output_frequencies
                .get(target)
                .ok_or("cumulative spectral window is missing")?;
            let data_center = *self
                .data_centers
                .get(target)
                .ok_or("cumulative calibration window is absent from the MeasurementSet")?;
            for row in rows {
                let antenna = read_i32(table, row, COL_ANTENNA1)?;
                let observation = read_i32(table, row, COL_OBSERVATION_ID)?;
                let scan = read_i32(table, row, COL_SCAN_NUMBER)?;
                let calfield = self.nearest.get(&field).copied().unwrap_or(-1);
                let correction = self
                    .lookup
                    .complex_solution_for_field(
                        calfield,
                        TimeQuery {
                            observation,
                            scan,
                            window: mapped,
                            antenna,
                            time: f64::from_bits(time),
                            frequency_hz: Some(data_center),
                        },
                    )
                    .map_err(error_text)?;
                let correction = correction
                    .map(|corr| self.resample(corr, source, axis, data_center))
                    .transpose()?;
                multiply_row(table, row, correction)?;
            }
        }
        Ok(())
    }

    fn resample(
        &self,
        solution: ComplexSolution,
        source: usize,
        output: &[f64],
        data_center: f64,
    ) -> Result<ComplexSolution, String> {
        if solution.channels == 1 {
            return Ok(solution);
        }
        let input = self
            .frequencies
            .get(source)
            .ok_or("incremental spectral window is missing")?;

        let (relative_input, relative_output);
        let (input, output) = if matches!(
            self.interpolation,
            ApplyInterpolationMode::Casa {
                relative_frequency: true,
                ..
            }
        ) {
            relative_input = relative_axis(input);
            relative_output = relative_axis_at_center(output, data_center);
            (relative_input.as_slice(), relative_output.as_slice())
        } else {
            (input.as_slice(), output)
        };
        if input == output {
            return Ok(solution);
        }
        solution
            .resample_frequency(input, output, self.interpolation.frequency_mode())
            .map_err(error_text)
    }
}

fn populate_time_grid(
    ms: &MeasurementSet,
    selection: &MsSelection,
    interval: f64,
    receptors: usize,
    channelized: bool,
    writer: &mut crate::writer::CalibrationTableWriter<'_>,
) -> Result<(), String> {
    let whole_ms = MsSelection::default();
    let mut selected =
        collect_selected_rows(ms, if interval > 0.0 { &whole_ms } else { selection }, true)
            .map_err(error_text)?;
    if selected.iter().any(|row| !row.time_seconds.is_finite()) {
        return Err("accumulation requires finite MeasurementSet times".into());
    }
    selected.sort_by(|a, b| {
        let group = (a.field_id, a.data_desc_id).cmp(&(b.field_id, b.data_desc_id));
        let time = a.time_seconds.total_cmp(&b.time_seconds);
        a.array_id.cmp(&b.array_id).then(if interval > 0.0 {
            time.then(group)
        } else {
            group.then(time)
        })
    });
    let spectrum = ms.spectral_window().map_err(error_text)?;
    let antennas = ms.antenna().map_err(error_text)?.row_count();
    for group in selected.chunk_by(|a, b| {
        a.array_id == b.array_id && a.field_id == b.field_id && a.data_desc_id == b.data_desc_id
    }) {
        let channels = if channelized {
            usize::try_from(
                spectrum
                    .num_chan(group[0].data_spw_id as usize)
                    .map_err(error_text)?,
            )
            .map_err(error_text)?
        } else {
            1
        };
        let mut start = 0;
        while start < group.len() {
            let first = &group[start];
            let count = if interval == 0.0 {
                group.len() - start
            } else {
                // MSIter places the first sample ten milliseconds into
                // each interval so floating-point boundary comparisons agree.
                let origin = first.time_seconds - 0.01;
                let bin = |time: f64| ((time - origin) / interval).floor();
                let first_bin = bin(first.time_seconds);
                group[start..].partition_point(|row| {
                    row.time_seconds == first.time_seconds
                        || (interval >= 2.0 * f64::MIN_POSITIVE
                            && bin(row.time_seconds) == first_bin)
                })
            };
            let mut samples = group[start..start + count]
                .iter()
                .map(|row| row.time_seconds)
                .peekable();
            let mut sum = 0.0;
            let mut times = 0;
            while let Some(time) = samples.next() {
                while samples.peek() == Some(&time) {
                    samples.next();
                }
                sum += time;
                times += 1;
            }
            let time = sum / f64::from(times);
            let shape = IxDyn(&[receptors, channels]);
            for antenna in 0..antennas {
                let row = RecordValue::new(vec![
                    RecordField::new(COL_TIME, Value::Scalar(ScalarValue::Float64(time))),
                    RecordField::new(COL_INTERVAL, Value::Scalar(ScalarValue::Float64(0.0))),
                    id(COL_FIELD_ID, first.field_id),
                    id(COL_SPECTRAL_WINDOW_ID, first.data_spw_id),
                    id(COL_ANTENNA1, i32::try_from(antenna).map_err(error_text)?),
                    id(COL_ANTENNA2, -1),
                    id(COL_SCAN_NUMBER, first.scan_number),
                    id(COL_OBSERVATION_ID, first.observation_id),
                    RecordField::new(
                        COL_CPARAM,
                        Value::Array(ArrayValue::Complex32(ArrayD::from_elem(
                            shape.clone(),
                            Complex32::new(1.0, 0.0),
                        ))),
                    ),
                    RecordField::new(
                        COL_FLAG,
                        Value::Array(ArrayValue::Bool(ArrayD::from_elem(shape.clone(), false))),
                    ),
                    RecordField::new(
                        COL_PARAMERR,
                        Value::Array(ArrayValue::Float32(ArrayD::zeros(shape.clone()))),
                    ),
                    RecordField::new(
                        COL_SNR,
                        Value::Array(ArrayValue::Float32(ArrayD::from_elem(shape.clone(), 1.0))),
                    ),
                ]);
                writer.append(row).map_err(error_text)?;
            }
            start += count;
        }
    }
    Ok(())
}

fn id(name: &str, value: i32) -> RecordField {
    RecordField::new(name, Value::Scalar(ScalarValue::Int32(value)))
}

fn table_frequencies(path: &Path) -> Result<Vec<Vec<f64>>, String> {
    let table = Table::open(TableOptions::new(path)).map_err(error_text)?;
    let spectrum = casa_ms::subtables::MsSpectralWindow::new(&table);
    (0..table.row_count())
        .map(|row| spectrum.chan_freq(row).map_err(error_text))
        .collect()
}

fn read_i32(table: &Table, row: usize, name: &str) -> Result<i32, String> {
    match table
        .cell_accessor(row, name)
        .and_then(|cell| cell.scalar())
        .map_err(error_text)?
    {
        ScalarValue::Int32(value) => Ok(*value),
        _ => Err(format!("invalid calibration table {name} at row {row}")),
    }
}

fn read_time(table: &Table, row: usize) -> Result<f64, String> {
    match table
        .cell_accessor(row, COL_TIME)
        .and_then(|cell| cell.scalar())
        .map_err(error_text)?
    {
        ScalarValue::Float64(value) => Ok(*value),
        _ => Err(format!("invalid calibration table TIME at row {row}")),
    }
}

fn multiply_row(
    table: &mut Table,
    row: usize,
    update: Option<ComplexSolution>,
) -> Result<(), String> {
    let read = |column| {
        table
            .cell_accessor(row, column)
            .and_then(|cell| cell.array())
            .cloned()
            .map_err(error_text)
    };
    let ArrayValue::Bool(mut flags) = read(COL_FLAG)? else {
        return Err("calibration table does not have boolean FLAG cells".into());
    };
    let ArrayValue::Complex32(mut values) = read(COL_CPARAM)? else {
        return Err("calibration table does not have complex CPARAM cells".into());
    };
    if values.shape() != flags.shape() || values.ndim() != 2 {
        return Err("calibration solution and flag arrays disagree".into());
    }
    if let Some(update) = update {
        if update.receptors != values.shape()[0]
            || (update.channels != 1 && update.channels != values.shape()[1])
        {
            return Err("cumulative and incremental solution shapes disagree".into());
        }
        for receptor in 0..values.shape()[0] {
            for channel in 0..values.shape()[1] {
                let index = receptor * update.channels + channel.min(update.channels - 1);
                let cell = [receptor, channel];
                flags[cell] |= update.flags[index];
                if !flags[cell] {
                    values[cell] *= update.values[index];
                }
            }
        }
    } else {
        flags.fill(true);
    }
    let mut writer = table
        .row_accessor_mut()
        .prepare(&[COL_CPARAM, COL_FLAG])
        .map_err(error_text)?;
    writer.seek(row).map_err(error_text)?;
    writer
        .set_value_at(0, Value::Array(ArrayValue::Complex32(values)))
        .map_err(error_text)?;
    writer
        .set_value_at(1, Value::Array(ArrayValue::Bool(flags)))
        .map_err(error_text)?;
    writer.flush().map_err(error_text)
}
