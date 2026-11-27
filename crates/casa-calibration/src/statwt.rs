// SPDX-License-Identifier: LGPL-3.0-or-later
//! Estimate visibility weights from the scatter within baseline and data bins.

mod channels;
mod classic;
mod grouping;
mod legacy;
mod output;
mod robust;
mod storage;

use std::collections::BTreeMap;
use std::path::PathBuf;

use casa_core::quanta::{Quantity, Unit};
use casa_core::{ArrayValue, Complex32, ScalarValue, Value};
use casa_ms::{MsError, MsResult, ResolvedMsSelectionRow};
use casa_tables::{Table, TableWriteLock};
use ndarray::{ArrayD, Axis, IxDyn, ShapeBuilder};

use channels::Channels;
use classic::{Moments, VisibilityMoments};
pub use legacy::{LegacyStatWtOptions, reweight_legacy};
use output::Output;
pub use robust::StatWtAlgorithm;
pub use storage::StatWtMs;

use crate::solve::grouping::{VisibilityModels, correlation_types_for_ddid};

/// Main-table data column contributing samples to a classical weight estimate.
#[derive(Clone, Copy)]
pub enum StatWtDataColumn {
    /// Read the input visibilities from DATA.
    Data,
    /// Read the calibrated visibilities from CORRECTED_DATA.
    Corrected,
    /// Subtract MODEL_DATA from DATA.
    ResidualData,
    /// Subtract MODEL_DATA from CORRECTED_DATA.
    ResidualCorrected,
}

impl StatWtDataColumn {
    /// Interpret the case-insensitive CASA data-column selector.
    pub fn parse(value: &str) -> MsResult<Self> {
        let value = value.to_ascii_lowercase();
        if value.starts_with('d') {
            Ok(Self::Data)
        } else if value.starts_with('c') {
            Ok(Self::Corrected)
        } else if value.starts_with("residual_") {
            Ok(Self::ResidualData)
        } else if value.starts_with("residual") {
            Ok(Self::ResidualCorrected)
        } else {
            Err(MsError::InvalidInput(format!(
                "unknown statwt data column {value:?}"
            )))
        }
    }

    fn visibility_column(self) -> &'static str {
        match self {
            Self::Data | Self::ResidualData => "DATA",
            Self::Corrected | Self::ResidualCorrected => "CORRECTED_DATA",
        }
    }

    fn residual(self) -> bool {
        matches!(self, Self::ResidualData | Self::ResidualCorrected)
    }

    fn raw(self) -> bool {
        matches!(self, Self::Data | Self::ResidualData)
    }
}

/// How the task sets the width of a statistical time bin.
#[derive(Clone, Copy)]
pub enum StatWtTimeBin {
    /// Count distinct timestamps in the selected time series.
    Timestamps(usize),
    /// Bin using the elapsed time in seconds.
    Seconds(f64),
}

impl StatWtTimeBin {
    /// Read a positive duration with time units.
    pub fn duration(value: &str) -> MsResult<Self> {
        physical_width(value, "s", "timebin").map(Self::Seconds)
    }
}

/// Boundaries and correlations to combine within a time bin.
#[derive(Clone, Copy, Default)]
pub struct StatWtCombine {
    /// Combine all visibility correlations.
    pub correlations: bool,
    /// Cross scan boundaries.
    pub scans: bool,
    /// Cross observational state boundaries.
    pub states: bool,
    /// Cross field boundaries.
    pub fields: bool,
}

impl StatWtCombine {
    /// Parse the CASA combination selector.
    pub fn parse(value: &str) -> Self {
        let folded = value.to_ascii_lowercase();
        Self {
            correlations: value.contains("corr"),
            scans: folded.contains("scan"),
            states: folded.contains("state"),
            fields: folded.contains("field"),
        }
    }
}

/// How channels within a spectral window contribute to each estimate.
#[derive(Clone, Copy)]
pub enum StatWtChannelBin {
    /// One bin spans the spectral window.
    Window,
    /// Bin a fixed count of adjacent channels.
    Channels(usize),
    /// Bin channels up to a frequency difference from the first channel.
    Hertz(f64),
}

impl StatWtChannelBin {
    /// Read the all-channel marker or a positive quantity with frequency units.
    pub fn parse(value: &str) -> MsResult<Self> {
        if value == "spw" {
            Ok(Self::Window)
        } else {
            physical_width(value, "Hz", "chanbin").map(Self::Hertz)
        }
    }
}

fn physical_width(value: &str, unit: &str, argument: &str) -> MsResult<f64> {
    let invalid = || MsError::InvalidInput(format!("invalid statwt {argument} {value:?}"));
    let unit = Unit::new(unit).expect("known unit");
    let quantity: Quantity = value.parse().map_err(|_| invalid())?;
    let width = quantity.get_value_in(&unit).map_err(|_| invalid())?;
    if width <= 0.0 || !width.is_finite() {
        return Err(invalid());
    }
    Ok(width)
}

/// Options for visibility weight estimation.
#[derive(Clone)]
pub struct StatWtOptions {
    /// The main-table data to estimate weights from.
    pub data_column: StatWtDataColumn,
    /// Estimate classical moments or use a robust algorithm.
    pub algorithm: StatWtAlgorithm,
    /// The minimum number of unflagged samples in a bin.
    pub minimum_samples: usize,
    /// Combine correlations or data across selected boundaries.
    pub combine: StatWtCombine,
    /// The number of timestamps or seconds per time bin.
    pub time_bin: StatWtTimeBin,
    /// Use a centered window for each timestamp.
    pub sliding: bool,
    /// Channel bins used to estimate and apply each weight.
    pub channel_bin: StatWtChannelBin,
    /// CASA spectral-window and channel expression restricting the fit.
    pub fit_spw: String,
    /// Use the complement of the fit channel selection within named windows.
    pub exclude_channels: bool,
    /// Optional inclusive bounds on acceptable weights.
    pub weight_range: Option<[f64; 2]>,
}

/// Interpret CASA's optional inclusive `wtrange`.
pub fn statwt_weight_range(bounds: &[f64]) -> MsResult<Option<[f64; 2]>> {
    match bounds {
        [] => Ok(None),
        &[left, right] if left >= 0.0 && right >= 0.0 && left != right => {
            Ok(Some([left.min(right), left.max(right)]))
        }
        _ => Err(MsError::InvalidInput(
            "wtrange requires two distinct nonnegative bounds".into(),
        )),
    }
}

/// Statistical summary of the estimated weights over previously unflagged data.
#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize)]
pub struct StatWtSummary {
    /// Mean of the estimated weights, including zeros for insufficient samples.
    pub mean: f64,
    /// Sample variance of the estimated weights.
    pub variance: f64,
}

/// Estimate scalar and spectral weights in block or sliding time bins.
///
/// The caller supplies the MeasurementSet selection held by either the task
/// or the existing MS tool. Pass a MAIN write lock to persist weights and
/// flags. Without a write lock this reports the same statistics in preview.
pub fn estimate_statwt(
    ms: &mut StatWtMs,
    selection: &[ResolvedMsSelectionRow],
    options: &StatWtOptions,
    mut locks: Option<&mut BTreeMap<PathBuf, TableWriteLock>>,
) -> MsResult<StatWtSummary> {
    if selection.is_empty() {
        return Err(MsError::InvalidInput(
            "MeasurementSet has zero selected rows".into(),
        ));
    }
    if options.minimum_samples < 2 {
        return Err(MsError::InvalidInput("minsamp must be at least 2".into()));
    }
    if matches!(options.time_bin, StatWtTimeBin::Timestamps(0))
        || matches!(options.time_bin, StatWtTimeBin::Seconds(value) if value <= 0.0 || !value.is_finite())
    {
        return Err(MsError::InvalidInput(
            "statwt time bin must be positive".into(),
        ));
    }
    if matches!(options.channel_bin, StatWtChannelBin::Channels(0))
        || matches!(options.channel_bin, StatWtChannelBin::Hertz(value) if value <= 0.0 || !value.is_finite())
    {
        return Err(MsError::InvalidInput(
            "statwt channel bin must be positive".into(),
        ));
    }
    let channel_plans = channels::prepare(ms.metadata(), selection, options)?;
    let models = options
        .data_column
        .residual()
        .then(|| ms.models(selection))
        .transpose()?;
    let groups = grouping::groups(
        ms,
        selection,
        options.combine,
        options.time_bin,
        options.sliding,
    )?;
    let mut summary = Moments::default();
    let mut pending = Vec::new();
    let mut pending_bytes = 0;
    let output = ms.output(selection, options, locks.as_deref_mut())?;
    let columns = output.columns();
    let polarizations = ms.metadata().polarization()?;
    let correlation_count = (0..polarizations.row_count())
        .map(|row| polarizations.num_corr(row).map(|count| count as usize))
        .collect::<MsResult<Vec<_>>>()?;
    let cell_bytes =
        size_of::<Complex32>() * (1 + usize::from(models.is_some())) + size_of::<bool>();
    let mut remaining = groups.as_slice();
    while !remaining.is_empty() {
        let mut bytes = 0;
        let mut count = 0;
        for group in remaining {
            let row = group.rows[0];
            let channels = channel_plans[&row.spectral_window_id].fit.len();
            let correlations = correlation_count
                .get(row.polarization_id as usize)
                .copied()
                .ok_or_else(|| MsError::InvalidInput("invalid POLARIZATION_ID".into()))?;
            let group_bytes = group
                .rows
                .len()
                .saturating_mul(128 + channels * correlations * cell_bytes);
            if count > 0 && bytes + group_bytes > casa_ms::TARGET_BATCH_BYTES {
                break;
            }
            bytes = bytes.saturating_add(group_bytes);
            count += 1;
        }
        let (batch, rest) = remaining.split_at(count);
        remaining = rest;
        let rows = batch
            .iter()
            .flat_map(|group| group.rows.iter().copied())
            .collect::<Vec<_>>();
        let samples = read_samples(ms, options.data_column, models.as_deref(), &rows)?;
        let mut offset = 0;
        for group in batch {
            let channel_plan = &channel_plans[&group.rows[0].spectral_window_id];
            let samples = &samples[offset..offset + group.rows.len()];
            offset += samples.len();
            let shape = samples[0].values.shape();
            if shape[1] != channel_plan.fit.len()
                || samples.iter().any(|sample| sample.values.shape() != shape)
            {
                return Err(MsError::InvalidInput(
                    "visibility shape disagrees with spectral window".into(),
                ));
            }
            let estimate = |range: std::ops::Range<usize>| {
                estimate_variances(
                    &samples[range.clone()],
                    &group.exposures[range],
                    channel_plan,
                    options,
                )
            };
            let mut previous_window = 0..0;
            let mut variances = Vec::new();
            let mut weights = Vec::new();
            for (index, (&row, sample)) in group.rows.iter().zip(samples).enumerate() {
                let window = group.window(index, options.time_bin, options.sliding);
                if window != previous_window {
                    variances = estimate(window.clone());
                    previous_window = window;
                }
                let exposure = group.exposures[index];
                weights.clear();
                weights.extend(variances.iter().map(|&variance| {
                    if variance == 0.0 {
                        0.0
                    } else {
                        (exposure / variance) as f32
                    }
                }));
                add_summary(
                    &mut summary,
                    sample,
                    &weights,
                    channel_plan,
                    output.spectral,
                    options.combine.correlations,
                );
                if let Some(locks) = &mut locks {
                    let use_duration_scalar = !output.spectral
                        && matches!(options.time_bin, StatWtTimeBin::Seconds(_))
                        && !options.sliding;
                    let update = RowUpdate::new(
                        row.row_index,
                        sample,
                        &weights,
                        channel_plan,
                        &output,
                        options,
                        use_duration_scalar.then_some((variances.as_slice(), exposure)),
                    );
                    pending_bytes += update.flags.len()
                        + update
                            .values
                            .iter()
                            .map(|value| value.len() * size_of::<f32>())
                            .sum::<usize>();
                    pending.push(update);
                    if pending_bytes >= casa_ms::TARGET_BATCH_BYTES || pending.len() == 65_536 {
                        ms.persist(&columns, &mut pending, locks)?;
                        pending_bytes = 0;
                    }
                }
            }
        }
    }
    if let Some(locks) = locks {
        ms.persist(&columns, &mut pending, locks)?;
    }
    Ok(StatWtSummary {
        mean: summary.mean(),
        variance: summary.sample_variance(),
    })
}

fn add_summary(
    summary: &mut Moments,
    sample: &Samples,
    weights: &[f32],
    channels: &Channels,
    spectral: bool,
    combine: bool,
) {
    let correlations = sample.flags.len_of(Axis(0));
    for (bin, bounds) in channels.bins.iter().enumerate() {
        let count = |correlation| {
            bounds
                .clone()
                .filter(|&channel| !sample.flags[[correlation, channel]])
                .count()
        };
        for (correlation, &weight) in weights[bin * correlations..][..correlations]
            .iter()
            .enumerate()
        {
            let unflagged = if !spectral && combine {
                if correlation != 0 {
                    continue;
                }
                usize::from((0..correlations).any(|corr| count(corr) != 0))
            } else if spectral {
                count(correlation)
            } else {
                usize::from(count(correlation) != 0)
            };
            summary.add_repeated(f64::from(weight), unflagged);
        }
    }
}

struct RowUpdate {
    row: usize,
    values: Vec<ArrayValue>,
    flags: ArrayValue,
    row_flagged: bool,
}

impl RowUpdate {
    fn new(
        row: usize,
        sample: &Samples,
        weights: &[f32],
        channels: &Channels,
        output: &Output,
        options: &StatWtOptions,
        duration_scalar: Option<(&[f64], f64)>,
    ) -> Self {
        let mut flags = sample.flags.clone();
        let correlations = flags.len_of(Axis(0));
        let mut spectrum = output
            .spectral
            .then(|| ArrayD::zeros(IxDyn(flags.shape()).f()));
        for (bin, bounds) in channels.bins.iter().enumerate() {
            for correlation in 0..correlations {
                let weight = weights[bin * correlations + correlation];
                let rejected = weight == 0.0
                    || options.weight_range.is_some_and(|[low, high]| {
                        f64::from(weight) < low || f64::from(weight) > high
                    });
                for channel in bounds.clone() {
                    flags[[correlation, channel]] |= rejected;
                    if let Some(spectrum) = &mut spectrum {
                        spectrum[[correlation, channel]] = if rejected { 0.0 } else { weight };
                    }
                }
            }
        }
        let scalar_weights = if let Some(spectrum) = &spectrum {
            spectral_medians(spectrum, &flags, options.combine.correlations)
        } else if let Some((variances, exposure)) = duration_scalar {
            // The duration-block scalar path divides exposure by the bin variance directly.
            variances
                .iter()
                .map(|value| (exposure / value) as f32)
                .collect()
        } else {
            weights.to_vec()
        };
        Self {
            row,
            values: output.cells(&scalar_weights, spectrum.as_ref()),
            row_flagged: flags.iter().all(|&flag| flag),
            flags: ArrayValue::Bool(flags),
        }
    }
}

fn spectral_medians(spectrum: &ArrayD<f32>, flags: &ArrayD<bool>, combine: bool) -> Vec<f32> {
    let correlations = flags.len_of(Axis(0));
    let channels = flags.len_of(Axis(1));
    let mut values = Vec::with_capacity(channels);
    let mut median = |selected: std::ops::Range<usize>| {
        values.clear();
        for correlation in selected {
            values.extend((0..channels).filter_map(|channel| {
                (!flags[[correlation, channel]]).then_some(spectrum[[correlation, channel]])
            }));
        }
        let midpoint = values.len() / 2;
        let even = values.len() % 2 == 0;
        if values.is_empty() {
            return 0.0;
        }
        let (lower, middle, _) = values.select_nth_unstable_by(midpoint, f32::total_cmp);
        if even {
            (0.5 * (f64::from(*middle)
                + f64::from(*lower.iter().max_by(|a, b| a.total_cmp(b)).unwrap())))
                as f32
        } else {
            *middle
        }
    };
    if combine {
        vec![median(0..correlations); correlations]
    } else {
        (0..correlations)
            .map(|correlation| median(correlation..correlation + 1))
            .collect()
    }
}

fn persist_updates(
    table: &mut Table,
    weight_columns: &[&str],
    pending: &mut Vec<RowUpdate>,
    lock: &mut TableWriteLock,
) -> MsResult<()> {
    if pending.is_empty() {
        return Ok(());
    }
    let mut columns = weight_columns.to_vec();
    columns.extend(["FLAG", "FLAG_ROW"]);
    let rows: Vec<_> = pending.iter().map(|update| update.row).collect();
    let mut writer = table.row_accessor_mut().prepare(&columns)?;
    for update in pending.drain(..) {
        writer.seek(update.row)?;
        for (index, value) in update.values.into_iter().enumerate() {
            writer.set_value_at(index, Value::Array(value))?;
        }
        writer.set_value_at(weight_columns.len(), Value::Array(update.flags))?;
        writer.set_value_at(
            weight_columns.len() + 1,
            Value::Scalar(ScalarValue::Bool(update.row_flagged)),
        )?;
    }
    writer.flush()?;
    drop(writer);
    lock.record_write();
    table.prepare_write().save_selected_rows(&columns, &rows)?;
    table.discard_persisted_cell_updates(&columns, &rows);
    Ok(())
}

struct Samples {
    values: ArrayD<Complex32>,
    flags: ArrayD<bool>,
}

fn read_samples(
    ms: &StatWtMs,
    column: StatWtDataColumn,
    models: Option<&[VisibilityModels]>,
    integration: &[&ResolvedMsSelectionRow],
) -> MsResult<Vec<Samples>> {
    let indices = integration
        .iter()
        .map(|row| row.row_index)
        .collect::<Vec<_>>();
    let read = |name| ms.array_cells(name, &indices);
    let data = read(column.visibility_column())?;
    let flags = read("FLAG")?;
    let scratch = models
        .is_some_and(|models| {
            integration
                .iter()
                .any(|row| models[ms.by_row(row.row_index).0].reads_column(row.field_id))
        })
        .then(|| read("MODEL_DATA"))
        .transpose()?;
    let mut samples = Vec::with_capacity(integration.len());
    for (index, (values, flags)) in data.into_iter().zip(flags).enumerate() {
        let row = integration[index];
        let (Some(ArrayValue::Complex32(mut values)), Some(ArrayValue::Bool(flags))) =
            (values, flags)
        else {
            return Err(MsError::InvalidInput(format!(
                "visibility or flag cell is undefined at row {}",
                row.row_index
            )));
        };
        if values.ndim() != 2 || values.shape() != flags.shape() {
            return Err(MsError::InvalidInput(format!(
                "visibility and flag shapes disagree at row {}",
                row.row_index
            )));
        }
        let model = if let Some(models) = models {
            let (part, physical_ms, physical_row) = ms.by_row(row.row_index);
            let model = &models[part];
            if model.reads_column(row.field_id) {
                match &scratch.as_ref().expect("read scratch model for its field")[index] {
                    Some(ArrayValue::Complex32(model)) => Some(std::borrow::Cow::Borrowed(model)),
                    _ => {
                        return Err(MsError::InvalidInput(format!(
                            "MODEL_DATA is undefined at row {}",
                            row.row_index
                        )));
                    }
                }
            } else {
                let codes = correlation_types_for_ddid(physical_ms, row.data_desc_id)
                    .map_err(|error| MsError::InvalidInput(error.to_string()))?;
                model
                    .predict(physical_ms, physical_row, &codes, &flags)
                    .map_err(|error| MsError::InvalidInput(error.to_string()))?
                    .map(std::borrow::Cow::Owned)
            }
        } else {
            None
        };
        if let Some(model) = model {
            if model.shape() != values.shape() {
                return Err(MsError::InvalidInput(format!(
                    "MODEL_DATA has a different shape at row {}",
                    row.row_index
                )));
            }
            values -= model.as_ref();
        }
        samples.push(Samples { values, flags });
    }
    Ok(samples)
}

fn estimate_variances(
    samples: &[Samples],
    exposures: &[f64],
    channels: &Channels,
    options: &StatWtOptions,
) -> Vec<f64> {
    let correlations = samples[0].values.len_of(Axis(0));
    let combine = options.combine.correlations;
    let mut variances = Vec::with_capacity(channels.bins.len() * correlations);
    let mut estimates = vec![VisibilityMoments::default(); if combine { 1 } else { correlations }];
    let mut counts = vec![0usize; estimates.len()];
    let mut robust_samples =
        (!options.algorithm.uses_classical_moments()).then(|| vec![Vec::new(); estimates.len()]);
    for bounds in &channels.bins {
        estimates.fill(VisibilityMoments::default());
        counts.fill(0);
        if let Some(samples) = &mut robust_samples {
            for sample in samples {
                sample.clear();
            }
        }
        for (row, &exposure) in samples.iter().zip(exposures) {
            for channel in bounds.clone().filter(|&channel| channels.fit[channel]) {
                for correlation in 0..correlations {
                    let bin = if combine { 0 } else { correlation };
                    if !row.flags[[correlation, channel]] {
                        counts[bin] += 1;
                        let visibility = row.values[[correlation, channel]];
                        if let Some(samples) = &mut robust_samples {
                            samples[bin].push((visibility, exposure));
                        } else {
                            estimates[bin].add(visibility, exposure);
                        }
                    }
                }
            }
        }
        let bin_start = variances.len();
        variances.extend((0..estimates.len()).map(|correlation| {
            if counts[correlation] < options.minimum_samples {
                0.0
            } else if let Some(samples) = &robust_samples {
                options.algorithm.variance(&samples[correlation])
            } else {
                estimates[correlation].variance()
            }
        }));
        if combine {
            variances.resize(bin_start + correlations, variances[bin_start]);
        }
    }
    variances
}
