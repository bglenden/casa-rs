// SPDX-License-Identifier: LGPL-3.0-or-later
//! The double-circle single-dish gain solver.

mod footprint;
mod normalization;
mod spectral;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use casa_core::{ArrayValue, Complex32, RecordField, RecordValue, ScalarValue, Value};
use casa_ms::{MeasurementSet, ResolvedMsSelection};
use ndarray::{ArrayD, Axis, IxDyn};

use super::edges::{self, Integration};
use crate::constants::*;
use crate::execute::SolvePriorApply;
use crate::solve_stats::{IntervalStages, SolveStats, SolveStatsBuilder, ToolSolveActivity};
use crate::table_maintenance::write_standard_calibration_subtables;
use crate::writer::{
    CalibrationTableDescriptor, CalibrationTableWriter, new_calibration_table_schema,
};
use crate::{CalibrationOutputMode, GainSolveMode};
use normalization::GainSample;

const SUBTYPE: &str = "SDGAIN_OTFD";

/// Controls for double-circle single-dish gain solving.
#[derive(Clone, Copy)]
pub struct Controls {
    /// Central reference region radius in radians, if fixed.
    pub radius: Option<f64>,
    /// Smooth the reference gain timeline before taking ratios.
    pub smooth: bool,
    /// Amplitude/phase solve mode.
    pub solve_mode: GainSolveMode,
    /// Track per-correlation flags independently.
    pub correlation_dependent_flags: bool,
}

/// Error from double-circle gain generation, distinguishing `spw` selection errors.
#[derive(Debug)]
pub enum GainGenerateError {
    /// Spectral window selection error (`at("spw")`).
    Spw(String),
    /// General solve error.
    Other(String),
}

impl GainGenerateError {
    /// Human-readable error message.
    pub fn into_message(self) -> String {
        match self {
            Self::Spw(msg) | Self::Other(msg) => msg,
        }
    }
}

/// Generate a double-circle single-dish gain calibration table.
pub fn generate(
    ms: &MeasurementSet,
    selection: &ResolvedMsSelection,
    priors: Option<&SolvePriorApply>,
    output: &Path,
    mode: CalibrationOutputMode,
    controls: Controls,
    activity: Option<&mut ToolSolveActivity>,
) -> Result<(Option<usize>, Option<SolveStats>), GainGenerateError> {
    let integrations = selected_integrations(ms, selection).map_err(|error| {
        tracing::error!(%error, "Single-dish gain solve failed");
        GainGenerateError::Other("Error in Calibrater::solve.".into())
    })?;
    let references = footprint::select(ms, &integrations, controls.radius).map_err(|error| {
        tracing::error!(%error, "Single-dish gain solve failed");
        GainGenerateError::Other("Error in Calibrater::solve.".into())
    })?;
    let channels =
        spectral::channels(ms, selection, &references).map_err(GainGenerateError::Spw)?;
    let mut intervals = activity.as_ref().map(|_| GainIntervals::default());
    let samples = solve(
        ms,
        references,
        priors,
        controls,
        &channels,
        intervals.as_mut(),
    )
    .map_err(|error| {
        tracing::error!(%error, "Single-dish gain solve failed");
        GainGenerateError::Other("Error in Calibrater::solve.".into())
    })?;
    let stats = intervals
        .map(|intervals| intervals.finish(ms, activity))
        .transpose()
        .map_err(|error| {
            tracing::error!(%error, "Single-dish gain solve failed");
            GainGenerateError::Other("Error in Calibrater::solve.".into())
        })?;
    let count = samples
        .map(|samples| write(ms, output, mode, samples, &channels))
        .transpose()
        .map_err(|error| {
            tracing::error!(%error, "Single-dish gain solve failed");
            GainGenerateError::Other("Error in Calibrater::solve.".into())
        })?;
    Ok((count, stats))
}

fn selected_integrations(
    ms: &MeasurementSet,
    selection: &ResolvedMsSelection,
) -> Result<Vec<Integration>, String> {
    let descriptions = ms.data_description().map_err(error_text)?;
    let windows = (0..descriptions.row_count())
        .map(|row| descriptions.spectral_window_id(row).map(|id| id as usize))
        .collect::<Result<Vec<_>, _>>()
        .map_err(error_text)?;
    let rows = selection.row_indices().collect::<Vec<_>>();
    edges::read_integrations(ms, &rows, &windows)
}

fn solve<'a>(
    ms: &MeasurementSet,
    references: Vec<&'a Integration>,
    priors: Option<&SolvePriorApply>,
    controls: Controls,
    channels: &BTreeMap<usize, usize>,
    mut intervals: Option<&mut GainIntervals>,
) -> Result<Option<Vec<GainSample<'a>>>, String> {
    let mut groups = BTreeMap::<_, Vec<_>>::new();
    for row in references {
        groups
            .entry((row.window, row.field, row.antenna))
            .or_default()
            .push(row);
    }
    let spectral = ms.spectral_window().map_err(error_text)?;
    let antennas = ms.antenna().map_err(error_text)?;
    let descriptions = ms.data_description().map_err(error_text)?;
    let pol = ms.polarization().map_err(error_text)?;
    let correlations = (0..descriptions.row_count())
        .map(|row| {
            descriptions
                .polarization_id(row)
                .and_then(|id| pol.corr_type(id as usize))
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(error_text)?;
    let mut results = Vec::new();
    let mut has_solving_data = false;
    for ((window, _, antenna), mut group) in groups {
        group.sort_by(|first, second| first.time.total_cmp(&second.time));
        let channel = channels[&window];
        let frequency = spectral.chan_freq(window).map_err(error_text)?[channel];
        let radius = normalization::radius(
            controls.radius,
            &[frequency],
            antennas
                .dish_diameter(antenna as usize)
                .map_err(error_text)?,
        )?;
        let mut samples = Vec::new();
        for row in group {
            let sample = read_sample(ms, row, priors, &correlations, channel, controls)?;
            if let Some(intervals) = intervals.as_deref_mut() {
                intervals.observe(row, sample.as_ref());
            }
            samples.extend(sample);
        }
        has_solving_data |= !samples.is_empty();
        if samples.len() < 100 {
            continue;
        }
        normalization::normalize(&mut samples, radius, controls.smooth)?;
        results.extend(samples);
    }
    results.sort_by(|first, second| {
        let left = first.integration;
        let right = second.integration;
        left.description
            .cmp(&right.description)
            .then_with(|| left.time.total_cmp(&right.time))
            .then_with(|| left.antenna.cmp(&right.antenna))
            .then_with(|| left.field.cmp(&right.field))
    });
    Ok(has_solving_data.then_some(results))
}

type IntegrationKey = (i32, i32, i32, i32, u64);

#[derive(Default)]
struct GainIntervals(BTreeMap<IntegrationKey, [BTreeSet<i32>; 2]>);

impl GainIntervals {
    fn observe(&mut self, row: &Integration, sample: Option<&GainSample<'_>>) {
        let key = (
            row.window as i32,
            row.observation,
            row.scan,
            row.field,
            row.time.to_bits(),
        );
        let hands = self.0.entry(key).or_default();
        if let Some(sample) = sample {
            for (index, hand) in hands.iter_mut().take(sample.data.shape()[0]).enumerate() {
                if !sample.flags[[index, 0]] {
                    hand.insert(row.antenna);
                }
            }
        }
    }

    fn finish(
        self,
        ms: &MeasurementSet,
        activity: Option<&mut ToolSolveActivity>,
    ) -> Result<SolveStats, String> {
        let antenna_count = ms.antenna().map_err(error_text)?.row_count() as i32;
        let window_count = ms.spectral_window().map_err(error_text)?.row_count() as i32;
        let mut stats = SolveStatsBuilder::new(2, 0..window_count, &(0..antenna_count).collect());
        let no_cross_correlations = BTreeSet::new();
        for ((spw_id, _, _, _, _), hands) in self.0 {
            let attempted = hands.iter().any(|hand| !hand.is_empty());
            stats.interval(&IntervalStages {
                spw_id,
                antennas: &no_cross_correlations,
                expected_window: false,
                attempted_interval: Some(attempted),
                with_data: hands.into(),
                constrained: BTreeSet::new(),
            });
            stats.record_success(spw_id, attempted);
        }
        stats.track_activity(activity);
        Ok(stats.finish())
    }
}

fn read_sample<'a>(
    ms: &MeasurementSet,
    row: &'a Integration,
    priors: Option<&SolvePriorApply>,
    correlations: &[Vec<i32>],
    channel: usize,
    controls: Controls,
) -> Result<Option<GainSample<'a>>, String> {
    let main = ms.main_table();
    let source = crate::model_columns::observed_column(ms);
    let data = main
        .cell_accessor(row.row, source)
        .and_then(|cell| cell.array())
        .map_err(error_text)?;
    let mut flags = main
        .cell_accessor(row.row, "FLAG")
        .and_then(|cell| cell.array())
        .map_err(error_text)?
        .clone();
    if row.flagged
        && let ArrayValue::Bool(flags) = &mut flags
    {
        flags.fill(true);
    }
    let types = correlations
        .get(row.description as usize)
        .ok_or("Invalid DATA_DESC_ID")?;
    let (data, flags) = if let Some(priors) = priors {
        let applied = priors
            .correct_single_dish(row.row, types, data, flags)
            .map_err(error_text)?
            .ok_or("Prior application omitted a selected gain integration.")?;
        (applied.corrected_data, applied.flags)
    } else {
        (data.clone(), flags)
    };
    let data = match data {
        ArrayValue::Float32(values) => values.mapv(|value| Complex32::new(value, 0.)),
        ArrayValue::Complex32(values) => values,
        _ => return Err("Invalid single-dish visibility shape.".into()),
    };
    let ArrayValue::Bool(flags) = flags else {
        return Err("FLAG is not Boolean.".into());
    };
    if data.ndim() != 2 || data.shape() != flags.shape() || channel >= data.shape()[1] {
        return Err("Visibility data and flags do not cover the selected gain channel.".into());
    }
    let (mut data, flags) = if data.shape()[1] == 1 {
        (data, flags)
    } else {
        (
            data.select(Axis(1), &[channel]),
            flags.select(Axis(1), &[channel]),
        )
    };
    let usable = if controls.correlation_dependent_flags {
        flags.iter().any(|&flagged| !flagged)
    } else {
        flags.iter().all(|&flagged| !flagged)
    };
    if !usable {
        return Ok(None);
    }
    for (value, &flagged) in data.iter_mut().zip(flags.iter()) {
        if flagged {
            *value = Complex32::default();
        } else {
            match controls.solve_mode {
                GainSolveMode::Amplitude => *value = Complex32::new(value.norm(), 0.),
                GainSolveMode::Phase => {
                    let magnitude = value.norm();
                    if magnitude > 0. {
                        *value /= magnitude;
                    }
                }
                GainSolveMode::AmplitudePhase => {}
            }
        }
    }
    Ok(Some(GainSample {
        integration: row,
        data,
        flags,
    }))
}

fn write(
    ms: &MeasurementSet,
    output: &Path,
    mode: CalibrationOutputMode,
    samples: Vec<GainSample<'_>>,
    channels: &BTreeMap<usize, usize>,
) -> Result<usize, String> {
    let count = samples.len();
    let destination = crate::output::CalibrationOutput::prepare_single_dish_solve(output, mode)
        .map_err(error_text)?;
    let mut writer = CalibrationTableWriter::staged(
        ms,
        CalibrationTableDescriptor {
            output,
            schema: new_calibration_table_schema(true),
            subtype: SUBTYPE,
            parameter_type: Some("Complex"),
            measurement_set_name: ms
                .path()
                .map_or_else(|| "none".into(), |path| path.display().to_string()),
            include_polarization_basis: true,
            time_extra_precision_column: None,
        },
        destination,
        mode,
    );
    let mut windows = BTreeSet::new();
    for sample in samples {
        windows.insert(sample.integration.window);
        writer.append(gain_record(sample)?).map_err(error_text)?;
    }
    write_standard_calibration_subtables(ms, writer.output_path(), None)?;
    spectral::write_axis(
        &writer.output_path().join("SPECTRAL_WINDOW"),
        channels,
        &windows,
    )?;
    writer.save_main().map_err(error_text)?;
    Ok(count)
}

fn gain_record(sample: GainSample<'_>) -> Result<RecordValue, String> {
    let row = sample.integration;
    let shape = sample.data.shape();
    if shape.len() != 2 || !(1..=2).contains(&shape[0]) {
        return Err("Double-circle gain needs one or two receptors.".into());
    }
    let (receptors, channels) = (shape[0], shape[1]);
    let dimensions = IxDyn(&[2, channels]);
    let mut values = ArrayD::zeros(dimensions.clone());
    let mut flags = ArrayD::from_elem(dimensions.clone(), true);
    let mut error = ArrayD::zeros(dimensions.clone());
    let mut snr = ArrayD::zeros(dimensions);
    for receptor in 0..receptors {
        values
            .index_axis_mut(Axis(0), receptor)
            .assign(&sample.data.index_axis(Axis(0), receptor));
        flags
            .index_axis_mut(Axis(0), receptor)
            .assign(&sample.flags.index_axis(Axis(0), receptor));
        error.index_axis_mut(Axis(0), receptor).fill(0.1);
        snr.index_axis_mut(Axis(0), receptor).fill(1.);
    }
    let int = |name, value| RecordField::new(name, Value::Scalar(ScalarValue::Int32(value)));
    let float = |name, value| RecordField::new(name, Value::Scalar(ScalarValue::Float64(value)));
    let array = |name, value| RecordField::new(name, Value::Array(value));
    Ok(RecordValue::new(vec![
        float(COL_TIME, row.time),
        float(COL_INTERVAL, 0.),
        int(COL_ANTENNA1, row.antenna),
        int(COL_ANTENNA2, -1),
        int(COL_FIELD_ID, row.field),
        int(COL_SCAN_NUMBER, row.scan),
        int(COL_OBSERVATION_ID, row.observation),
        int(COL_SPECTRAL_WINDOW_ID, row.window as i32),
        array(COL_CPARAM, ArrayValue::Complex32(values)),
        array(COL_FLAG, ArrayValue::Bool(flags)),
        array(COL_PARAMERR, ArrayValue::Float32(error)),
        array(COL_SNR, ArrayValue::Float32(snr)),
    ]))
}

fn error_text(error: impl std::fmt::Display) -> String {
    error.to_string()
}
