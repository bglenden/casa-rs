// SPDX-License-Identifier: LGPL-3.0-or-later
//! One batch packed into a native block: each selected sample's geometry,
//! values, weights and flags, copied once.

use casa_coordinates::DirectionCoordinate;
use casa_imaging_model::{
    AntennaResponseClass, BatchGeometry, CompiledProblem, ContinuumFitRule,
    DirectionCoordinateSpec, FiniteValuePolicy, FrequencyBands, GeometryPlan,
    SelectedAntennaResponses, SkyDirection,
};
use casa_imaging_operator::RowContext;
use casa_imaging_runtime::pass::{
    BlockShape, DomainProjection, NativeBlock, NativeRowHeader, RowAddress, SourceError,
};
use casa_observation::{Correlation, PartitionEntry, VisibilityBatch};
use casa_types::measures::MjdHighPrec;
use num_complex::Complex32;

use super::keys::StoreKeys;
use super::selection::{PartitionSelection, SelectedPartition};
use crate::imaging::continuum::{ContinuumRow, flag_unimaged_channels, subtract_continuum};

/// The block layout of one partition's batches in one wave: which of a
/// batch's channels and correlations a block holds.
#[derive(Debug)]
pub(super) struct Layout {
    /// The batch channel of each channel a block holds.
    channels: Vec<usize>,
    /// Their native indices, the block's channel indices.
    channel_indices: Vec<u32>,
    /// Their stored frequencies, the continuum fit's abscissae.
    native_hz: Vec<f64>,
    /// Half their stored widths, the extent a frequency band selects
    /// them by.
    half_widths_hz: Vec<f64>,
    /// The routed products, in the operator's order.
    correlations: Vec<Correlation>,
    /// Their stored indices, the block's correlation indices.
    correlation_indices: Vec<u32>,
    /// Whether each enters Stokes I.
    parallel: Vec<bool>,
}

impl Layout {
    /// The layout of `partition`'s native channels `channels`, read as the
    /// native channels from the first of them.
    pub(super) fn new(
        partition: &SelectedPartition,
        entry: &PartitionEntry,
        channels: &[u32],
    ) -> Result<Self, SourceError> {
        let channel_indices = channels.to_vec();
        let first = channel_indices[0] as usize;
        let correlation_indices = partition
            .correlations
            .iter()
            .map(|product| {
                entry
                    .correlation_index(*product)
                    .map(|index| index as u32)
                    .ok_or_else(|| format!("partition lacks the routed product {product:?}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            channels: channel_indices
                .iter()
                .map(|channel| *channel as usize - first)
                .collect(),
            native_hz: channel_indices
                .iter()
                .map(|channel| entry.frequencies_hz[*channel as usize])
                .collect(),
            half_widths_hz: channel_indices
                .iter()
                .map(|channel| entry.channel_widths_hz[*channel as usize].abs() / 2.0)
                .collect(),
            channel_indices,
            parallel: partition
                .correlations
                .iter()
                .map(|product| product.contributes_to_stokes_i())
                .collect(),
            correlations: partition.correlations.clone(),
            correlation_indices,
        })
    }

    /// Keep a time's selected channels while preserving offsets into the
    /// original batch's channel span.
    fn compact(&self, selected: &[bool]) -> Option<Self> {
        assert_eq!(selected.len(), self.channels.len());
        let count = selected.iter().filter(|keep| **keep).count();
        if count == 0 {
            return None;
        }
        let mut compact = Self {
            channels: Vec::with_capacity(count),
            channel_indices: Vec::with_capacity(count),
            native_hz: Vec::with_capacity(count),
            half_widths_hz: Vec::with_capacity(count),
            correlations: self.correlations.clone(),
            correlation_indices: self.correlation_indices.clone(),
            parallel: self.parallel.clone(),
        };
        for (index, keep) in selected.iter().enumerate() {
            if *keep {
                compact.channels.push(self.channels[index]);
                compact.channel_indices.push(self.channel_indices[index]);
                compact.native_hz.push(self.native_hz[index]);
                compact.half_widths_hz.push(self.half_widths_hz[index]);
            }
        }
        Some(compact)
    }

    /// Heap bound for a compacted layout beside the original layout.
    fn compacted_bound(shape: BlockShape) -> usize {
        2 * (shape.channels() * (size_of::<usize>() + size_of::<u32>() + 2 * size_of::<f64>())
            + shape.correlations
                * (size_of::<Correlation>() + size_of::<u32>() + size_of::<bool>()))
    }

    /// The native channels a binding reads: from the first channel the
    /// block holds to the last.
    pub(super) fn native_channels(&self) -> std::ops::Range<usize> {
        let first = self.channel_indices[0] as usize;
        first
            ..*self
                .channel_indices
                .last()
                .expect("a layout holds a channel") as usize
                + 1
    }
}

/// Which samples of `batch` the selection images, into `keep`: those of a
/// selected scan and baseline that are no padding (every flag set and every
/// weight not finite, as the MSv2 adapter pads and XRADIO's NaN cells read)
/// and whose UV distance it selects. Fields and intents were selected by the
/// request.
pub(super) fn select_samples(
    batch: &VisibilityBatch<'_>,
    selection: &PartitionSelection,
    partition: &SelectedPartition,
    keep: &mut Vec<bool>,
) -> Result<(), SourceError> {
    let flags = batch.group.flags.ok_or("the batch carries no flags")?;
    let weights = batch.group.weights.ok_or("the batch carries no weights")?;
    let uvw = batch.group.uvw.ok_or("the batch carries no UVW")?;
    keep.clear();
    let baselines = batch.samples_per_time();
    for time in 0..batch.times() {
        let scan_selected = selection.time_holds(batch, time)?;
        keep.extend((time * baselines..(time + 1) * baselines).map(|sample| {
            if !scan_selected || !selection.baseline_holds(batch.baselines[sample % baselines]) {
                return false;
            }
            let flags = flags.index_axis(ndarray::Axis(0), sample);
            let weights = weights.index_axis(ndarray::Axis(0), sample);
            let padding =
                flags.iter().all(|flag| *flag) && weights.iter().all(|weight| !weight.is_finite());
            !padding && selection.uv_holds(partition, uvw.metres_at(sample))
        }));
    }
    Ok(())
}

/// What packing reads besides the batch: the problem and the per-chart
/// laws, fixed for a run.
pub(super) struct Packer<'p> {
    pub(super) problem: &'p CompiledProblem,
    /// Each store's geometry, in store order: what converts a window's
    /// frequency bands to a time's rows.
    pub(super) plans: Vec<&'p GeometryPlan>,
    /// Projections per row: one per chart.
    pub(super) charts: usize,
    /// Each chart's direction law, and the coordinate that places a
    /// pointing on it, when a kernel set ramps each row to its pointing;
    /// empty otherwise.
    pub(super) pointing: Vec<(DirectionCoordinateSpec, DirectionCoordinate)>,
    /// The keys of the route's stores: the aperture classes a row's
    /// antenna types index, and the window and field keys its context
    /// carries.
    pub(super) keys: StoreKeys,
}

/// Buffers packing reuses from batch to batch.
#[derive(Debug, Default)]
pub(super) struct PackScratch {
    projections: Vec<DomainProjection>,
    frequencies_hz: Vec<f64>,
    /// Whether the window's frequency bands select each channel at the time
    /// packed; empty when every channel is read.
    in_band: Vec<bool>,
    /// The parent selection at a prediction's time.
    in_prediction: Vec<bool>,
    /// What a write limited by the bands keeps of `in_band`,
    /// `[time][channel]`, for the block's provenance, which takes it; empty
    /// when every channel is written.
    pub(super) in_band_by_time: Vec<bool>,
    /// Each routed product's place along the batch's correlations.
    routed: Vec<usize>,
    /// One sample's weight per delivered product, for weights stored per
    /// correlation.
    row_weights: Vec<f32>,
    /// Full fit/output union for one row, before the output is compacted.
    fit_values: Vec<Complex32>,
    fit_weights: Vec<f32>,
    fit_flags: Vec<bool>,
    /// Each compact output channel's ordinal in that union.
    fit_positions: Vec<usize>,
}

impl PackScratch {
    /// At most what the buffers come to hold for blocks of `layout`'s rows,
    /// each growing at most to twice what it needs.
    pub(super) fn bound(layout: BlockShape) -> usize {
        2 * (layout.domains * size_of::<DomainProjection>()
            + layout.channels() * (size_of::<f64>() + 2 * size_of::<bool>())
            + layout.correlations * (size_of::<usize>() + size_of::<f32>()))
            + Layout::compacted_bound(layout)
            + Self::fit_bound(layout)
    }

    /// The reusable full-spectrum fit row and compact-channel mapping.
    fn fit_bound(layout: BlockShape) -> usize {
        2 * (layout.channels()
            * layout.correlations
            * (size_of::<Complex32>() + size_of::<f32>() + size_of::<bool>())
            + layout.channels() * size_of::<usize>())
    }
}

/// One batch's arrays as flat slices, `[sample][channel][correlation]`.
struct Arrays<'b> {
    values: Option<&'b [Complex32]>,
    flags: &'b [bool],
    /// Per cell, or `None` for weights stored per correlation.
    weights: Option<&'b [f32]>,
    weight_view: ndarray::ArrayView3<'b, f32>,
    channels: usize,
    correlations: usize,
}

impl<'b> Arrays<'b> {
    fn new(batch: &VisibilityBatch<'b>, values: bool) -> Result<Self, SourceError> {
        let standard = "a batch array is not in standard order";
        let flags = batch.group.flags.ok_or("the batch carries no flags")?;
        let weights = batch.group.weights.ok_or("the batch carries no weights")?;
        let per_correlation = batch.channel_count() > 1 && weights.strides()[1] == 0;
        Ok(Self {
            values: values
                .then(|| {
                    batch
                        .group
                        .values
                        .ok_or("the batch carries no visibilities")?
                        .to_slice()
                        .ok_or(standard)
                })
                .transpose()?,
            flags: flags.to_slice().ok_or(standard)?,
            weights: if per_correlation {
                None
            } else {
                Some(weights.to_slice().ok_or(standard)?)
            },
            weight_view: weights,
            channels: batch.channel_count(),
            correlations: batch.correlations.len(),
        })
    }
}

/// Recover the stored channel frequency from the resampler's frame. Each
/// conversion hop scales every channel linearly. At most eight hops per
/// channel, one division and one multiplication bound the relative error by
/// gamma_18 (about 2e-15) for positive, normal, finite intermediates; a cell
/// can differ only at a rounding-near tie.
fn stored_frequency_ratio(
    batch: &VisibilityBatch<'_>,
    layout: &Layout,
    scratch: &PackScratch,
) -> f64 {
    layout
        .channels
        .first()
        .zip(scratch.frequencies_hz.first())
        .map_or(1.0, |(channel, frequency)| {
            batch.frequency.hz()[*channel] / frequency
        })
}

impl Packer<'_> {
    /// The physically selected channels at a batch's sole time. An empty
    /// selection contributes no block or write.
    pub(super) fn time_layout(
        &self,
        (batch, store, layout): (&VisibilityBatch<'_>, usize, &Layout),
        geometry: &BatchGeometry,
        bands: Option<&FrequencyBands>,
        rule: Option<&ContinuumFitRule>,
        scratch: &mut PackScratch,
    ) -> Result<Option<Layout>, SourceError> {
        if batch.times() != 1 {
            return Err("a compact batch must hold one time".into());
        }
        let frequencies = geometry.frequencies_hz(0);
        scratch.frequencies_hz.clear();
        scratch
            .frequencies_hz
            .extend(layout.channels.iter().map(|channel| frequencies[*channel]));
        scratch.in_prediction.clear();
        if let Some(bands) = bands {
            let factor = self.plans[store].frequency_factor(
                bands.frame().frequency_ref(),
                batch.field[0],
                batch.time.seconds_since(0, MjdHighPrec::new(0.0, 0.0)),
                batch.time.reference(),
            )?;
            in_band(
                bands,
                factor,
                layout,
                &scratch.frequencies_hz,
                &mut scratch.in_prediction,
            );
        } else {
            scratch.in_prediction.resize(layout.channels.len(), true);
        }
        if let Some(rule) = rule {
            for (channel, selected) in layout
                .channel_indices
                .iter()
                .zip(&mut scratch.in_prediction)
            {
                *selected &= rule
                    .channel_use(*channel)
                    .is_some_and(|role| role.contributes_to_output());
            }
        }
        Ok(layout.compact(&scratch.in_prediction))
    }

    /// Prepare one time's physical frequencies and selection masks.
    fn prepare_time(
        &self,
        (batch, store, partition, layout): (
            &VisibilityBatch<'_>,
            usize,
            &SelectedPartition,
            &Layout,
        ),
        geometry: &BatchGeometry,
        time: usize,
        (bands, prediction, keep_in_band): (Option<&FrequencyBands>, bool, bool),
        scratch: &mut PackScratch,
    ) -> Result<(), SourceError> {
        let origin = MjdHighPrec::new(0.0, 0.0);
        let frequencies = geometry.frequencies_hz(time);
        scratch.frequencies_hz.clear();
        scratch
            .frequencies_hz
            .extend(layout.channels.iter().map(|channel| frequencies[*channel]));
        scratch.in_band.clear();
        if let Some(bands) = bands {
            let factor = self.plans[store].frequency_factor(
                bands.frame().frequency_ref(),
                batch.field[time],
                batch.time.seconds_since(time, origin),
                batch.time.reference(),
            )?;
            in_band(
                bands,
                factor,
                layout,
                &scratch.frequencies_hz,
                &mut scratch.in_band,
            );
        }
        if prediction {
            if bands.is_none() {
                scratch.in_band.resize(layout.channels.len(), true);
            }
            for (channel, selected) in layout.channel_indices.iter().zip(&mut scratch.in_band) {
                *selected &= partition.channels.binary_search(channel).is_ok();
            }
            scratch.in_prediction.clear();
            if let Some(bands) = partition.prediction_bands.as_ref() {
                let factor = self.plans[store].frequency_factor(
                    bands.frame().frequency_ref(),
                    batch.field[time],
                    batch.time.seconds_since(time, origin),
                    batch.time.reference(),
                )?;
                in_band(
                    bands,
                    factor,
                    layout,
                    &scratch.frequencies_hz,
                    &mut scratch.in_prediction,
                );
            } else {
                scratch.in_prediction.resize(layout.channels.len(), true);
            }
        }
        if keep_in_band {
            let selected = if prediction {
                &scratch.in_prediction
            } else {
                &scratch.in_band
            };
            scratch.in_band_by_time[time * layout.channels.len()..][..layout.channels.len()]
                .copy_from_slice(selected);
        }
        Ok(())
    }

    /// Pack `batch`'s samples `keep` names into `out` under `layout`, with
    /// `geometry` applied to those samples; the rows are keyed as store
    /// `store`'s ([`StoreKeys`]). Unread visibilities are zero.
    /// A non-finite input fails the run under [`FiniteValuePolicy`], naming
    /// the sample.
    pub(super) fn pack(
        &self,
        (batch, store, partition, layout, input_layout): (
            &VisibilityBatch<'_>,
            usize,
            &SelectedPartition,
            &Layout,
            &Layout,
        ),
        geometry: &BatchGeometry,
        keep: &[bool],
        (values, writing, prediction): (bool, bool, bool),
        scratch: &mut PackScratch,
        out: &mut NativeBlock,
    ) -> Result<(), SourceError> {
        out.reset(
            self.charts,
            &layout.channel_indices,
            &layout.correlation_indices,
        );
        let arrays = Arrays::new(batch, values)?;
        scratch.routed.clear();
        for product in &layout.correlations {
            scratch.routed.push(
                batch
                    .correlations
                    .iter()
                    .position(|delivered| delivered == product)
                    .ok_or("the batch lacks a routed product")?,
            );
        }
        let reject_flagged = matches!(
            self.problem.numerics().finite_values(),
            FiniteValuePolicy::RejectAll
        );
        let per_time = batch.samples_per_time();
        let origin = MjdHighPrec::new(0.0, 0.0);
        let uvw = batch.group.uvw.ok_or("the batch carries no UVW")?;
        // A pass reads a time's channels as CASA's frequency selection does:
        // tuned to the image for one that images, and for a density over the
        // whole selection its untuned bands. The per-plane density's layout
        // is already compacted to the time's imaging bands.
        let bands = if values {
            partition.frequency_bands.as_ref()
        } else {
            partition.density_bands.as_ref()
        };
        let prediction = prediction && partition.prediction_channels.is_some();
        let write_bands = if prediction {
            partition.prediction_bands.as_ref()
        } else {
            bands
        };
        let keep_in_band = writing && write_bands.is_some();
        scratch.in_band_by_time.clear();
        if keep_in_band {
            let cells = batch.samples() / per_time * layout.channels.len();
            scratch.in_band_by_time.resize(cells, false);
        }
        let compacted = !std::ptr::eq(layout, input_layout);
        scratch.fit_positions.clear();
        if compacted && values && !prediction && self.problem.visibility_transform().is_some() {
            for channel in &layout.channel_indices {
                scratch.fit_positions.push(
                    input_layout
                        .channel_indices
                        .binary_search(channel)
                        .map_err(|_| "a compact channel is absent from the fit selection")?,
                );
            }
        }
        let (mut packed_time, mut native_frequency_ratio) = (None, 1.0);
        for sample in (0..batch.samples()).filter(|sample| keep[*sample]) {
            let time = sample / per_time;
            if packed_time != Some(time) {
                self.prepare_time(
                    (batch, store, partition, layout),
                    geometry,
                    time,
                    (bands, prediction, keep_in_band),
                    scratch,
                )?;
                native_frequency_ratio = stored_frequency_ratio(batch, layout, scratch);
                packed_time = Some(time);
            }
            let row = Row {
                batch,
                partition,
                sample,
                time_s: batch.time.seconds_since(time, origin),
            };
            self.project(geometry, sample, &mut scratch.projections)?;
            let nonfinite_uvw = !scratch.projections.iter().all(|projection| {
                projection.uvw_m.iter().all(|value| value.is_finite())
                    && projection.phase_shift_m.is_finite()
            });
            if arrays.weights.is_none() {
                scratch.row_weights.clear();
                scratch.row_weights.extend(
                    (0..arrays.correlations)
                        .map(|product| arrays.weight_view[[sample, 0, product]]),
                );
            }
            let flag_layout =
                if values && !prediction && self.problem.visibility_transform().is_some() {
                    input_layout
                } else {
                    layout
                };
            let flagged = flag_layout.channels.iter().all(|channel| {
                scratch.routed.iter().all(|product| {
                    arrays.flags
                        [(sample * arrays.channels + channel) * arrays.correlations + product]
                })
            });
            let baseline = batch.baselines[sample % per_time];
            let field = batch.field[time];
            let header = NativeRowHeader {
                row_flag: flagged || nonfinite_uvw,
                context: RowContext {
                    native_frequency_ratio,
                    original_uvw_m: Some(uvw.metres_at(sample)),
                    time_s: row.time_s,
                    antennas: [baseline.antenna1.0, baseline.antenna2.0],
                    antenna_types: antenna_types(
                        geometry.antenna_responses(sample),
                        &self.keys.dish_classes,
                    ),
                    parallactic_angle_rad: kernel_parallactic_angles(geometry, sample, time),
                    field: self.keys.field(store, field.0),
                    spectral_window: self.keys.window(store, partition.spectral_window),
                },
                address: RowAddress::UNADDRESSED,
            };
            let rule = self
                .problem
                .visibility_transform()
                .and_then(|transform| transform.rule(field.0 as i32, partition.spectral_window));
            let prefit = compacted && values && !prediction && rule.is_some();
            if prefit {
                fit_sample(
                    &row,
                    input_layout,
                    &arrays,
                    rule.expect("a compact fit has a rule"),
                    scratch,
                    (flagged, reject_flagged),
                )?;
            }
            let row_weights = &scratch.row_weights;
            let routed = &scratch.routed;
            let in_band = &scratch.in_band;
            let fit_values = &scratch.fit_values;
            let fit_weights = &scratch.fit_weights;
            let fit_flags = &scratch.fit_flags;
            let fit_positions = &scratch.fit_positions;
            out.push_row_with(
                header,
                &scratch.projections,
                &scratch.frequencies_hz,
                |values_out, weights_out, flags_out| {
                    if prefit {
                        for (output, input) in fit_positions.iter().copied().enumerate() {
                            let output = output * routed.len();
                            let input = input * routed.len();
                            values_out[output..output + routed.len()]
                                .copy_from_slice(&fit_values[input..input + routed.len()]);
                            weights_out[output..output + routed.len()]
                                .copy_from_slice(&fit_weights[input..input + routed.len()]);
                            flags_out[output..output + routed.len()]
                                .copy_from_slice(&fit_flags[input..input + routed.len()]);
                        }
                    } else {
                        fill_samples(
                            &row,
                            (&arrays, row_weights, routed, layout),
                            (values_out, weights_out, flags_out),
                            reject_flagged,
                        )?;
                    }
                    // A channel the time's bands miss is not read: no
                    // sample of it, or interpolated from it, is imaged.
                    if !prediction {
                        for (cells, _) in flags_out
                            .chunks_mut(routed.len())
                            .zip(in_band)
                            .filter(|(_, selected)| !**selected)
                        {
                            cells.fill(true);
                        }
                    }
                    // A row's uvw matters only if one of its samples is imaged.
                    let imaged = !flagged && flags_out.iter().any(|flag| !*flag);
                    if nonfinite_uvw && (reject_flagged || imaged) {
                        return Err(nonfinite_input("uvw", &row.at(None)));
                    }
                    // A model-only parent traversal does not fit or alter
                    // continuum data; its writer applies output roles.
                    if prefit || (prediction && !values) {
                        return Ok(());
                    }
                    let Some(rule) = rule else {
                        return Ok(());
                    };
                    let row = ContinuumRow {
                        channels: &layout.channel_indices,
                        frequencies_hz: &layout.native_hz,
                        parallel: &layout.parallel,
                        real: false,
                        row_flag: flagged,
                    };
                    if values {
                        subtract_continuum(rule, &row, values_out, weights_out, flags_out)
                    } else {
                        flag_unimaged_channels(rule, &row, flags_out)
                    }
                },
            )?;
            if prediction {
                out.separate_last_row_selection(&scratch.in_band, &scratch.in_prediction);
            }
        }
        Ok(())
    }

    /// Sample `sample`'s projection on every chart into `projections`.
    fn project(
        &self,
        geometry: &BatchGeometry,
        sample: usize,
        projections: &mut Vec<DomainProjection>,
    ) -> Result<(), SourceError> {
        projections.clear();
        for chart in 0..self.charts {
            let projection = geometry.projection(sample, chart);
            let pointing_offset_rad = match self.pointing.get(chart) {
                Some(direction) => pointing_offset(
                    direction,
                    geometry.aw_pointing_pixel(sample, chart),
                    geometry
                        .chart_pointing(sample, chart)
                        .unwrap_or(geometry.pointing(sample).antenna1),
                )?,
                None => [0.0; 2],
            };
            projections.push(DomainProjection {
                uvw_m: projection.uvw_m,
                phase_shift_m: projection.phase_shift_m,
                pointing_offset_rad,
            });
        }
        Ok(())
    }
}

/// Fit one full input row before its output is compacted.
fn fit_sample(
    row: &Row<'_, '_>,
    input_layout: &Layout,
    arrays: &Arrays<'_>,
    rule: &ContinuumFitRule,
    scratch: &mut PackScratch,
    (flagged, reject_flagged): (bool, bool),
) -> Result<(), SourceError> {
    let cells = input_layout.channels.len() * scratch.routed.len();
    scratch.fit_values.resize(cells, Complex32::default());
    scratch.fit_weights.resize(cells, 0.0);
    scratch.fit_flags.resize(cells, false);
    fill_samples(
        row,
        (arrays, &scratch.row_weights, &scratch.routed, input_layout),
        (
            &mut scratch.fit_values,
            &mut scratch.fit_weights,
            &mut scratch.fit_flags,
        ),
        reject_flagged,
    )?;
    let fit_row = ContinuumRow {
        channels: &input_layout.channel_indices,
        frequencies_hz: &input_layout.native_hz,
        parallel: &input_layout.parallel,
        real: false,
        row_flag: flagged,
    };
    subtract_continuum(
        rule,
        &fit_row,
        &mut scratch.fit_values,
        &scratch.fit_weights,
        &mut scratch.fit_flags,
    )?;
    Ok(())
}

/// Whether `bands` select each of `layout`'s channels at one time, into
/// `out`: the channel's extent, its centre `frequencies_hz` in the rows'
/// frame less and plus half its width scaled as its centre is, meets a
/// band scaled by `factor`, the bands' frame's factor to the rows' frame
/// at the time ([`GeometryPlan::frequency_factor`]). CASA converts the
/// bands to the window's frame instead and compares stored extents
/// (`VisibilityIteratorImpl2::makeChannelSelectorF`); both conversions are
/// one Doppler factor, so the comparisons agree.
fn in_band(
    bands: &FrequencyBands,
    factor: f64,
    layout: &Layout,
    frequencies_hz: &[f64],
    out: &mut Vec<bool>,
) {
    out.extend(
        frequencies_hz
            .iter()
            .zip(&layout.native_hz)
            .zip(&layout.half_widths_hz)
            .map(|((centre, native), half_width)| {
                let half_width = half_width * centre / native;
                bands.meets(factor, [centre - half_width, centre + half_width])
            }),
    );
}

/// One sample being packed, for naming it in an error.
struct Row<'r, 'b> {
    batch: &'r VisibilityBatch<'b>,
    partition: &'r SelectedPartition,
    sample: usize,
    /// Its time, in seconds from MJD 0 on the partition's scale.
    time_s: f64,
}

impl Row<'_, '_> {
    /// Where the sample is: its window, time, baseline and, for one value,
    /// its channel and correlation.
    fn at(&self, cell: Option<(u32, Correlation)>) -> String {
        let baseline = self.batch.baselines[self.sample % self.batch.samples_per_time()];
        let mut at = format!(
            "spectral window {} at {:.3} s (MJD, {:?}), baseline {}–{}",
            self.partition.spectral_window,
            self.time_s,
            self.batch.time.reference(),
            baseline.antenna1.0,
            baseline.antenna2.0,
        );
        if let Some((channel, correlation)) = cell {
            at.push_str(&format!(", channel {channel}, correlation {correlation:?}"));
        }
        at
    }
}

/// The error for a non-finite `what` at `at`, naming the remedy.
fn nonfinite_input(what: &str, at: &str) -> SourceError {
    format!(
        "an unflagged {what} at {at} is not finite; CASA would image it as NaN everywhere. \
         Flag the non-finite data (CASA `flagdata`) and run again"
    )
    .into()
}

/// Write one sample's selected values, weights and flags in channel and
/// correlation order. A non-finite value or weight in an unflagged sample
/// fails the row; in a flagged one it is ignored, unless `reject_flagged`;
/// either way its flag is set.
fn fill_samples(
    row: &Row<'_, '_>,
    (arrays, row_weights, routed, layout): (&Arrays<'_>, &[f32], &[usize], &Layout),
    (values, weights, flags): (&mut [Complex32], &mut [f32], &mut [bool]),
    reject_flagged: bool,
) -> Result<(), SourceError> {
    let stride = arrays.correlations;
    let base = row.sample * arrays.channels * stride;
    let mut cell = 0;
    for (ordinal, channel) in layout.channels.iter().enumerate() {
        let start = base + channel * stride;
        for (k, product) in routed.iter().enumerate() {
            let index = start + product;
            let sample = arrays
                .values
                .map_or(Complex32::default(), |values| values[index]);
            let weight = arrays
                .weights
                .map_or_else(|| row_weights[*product], |weights| weights[index]);
            let flagged = arrays.flags[index];
            let finite_value = sample.re.is_finite() && sample.im.is_finite();
            let nonfinite = !(finite_value && weight.is_finite());
            if nonfinite && (reject_flagged || !flagged) {
                return Err(nonfinite_input(
                    if finite_value { "weight" } else { "visibility" },
                    &row.at(Some((
                        layout.channel_indices[ordinal],
                        layout.correlations[k],
                    ))),
                ));
            }
            values[cell] = sample;
            weights[cell] = weight;
            flags[cell] = flagged || nonfinite;
            cell += 1;
        }
    }
    Ok(())
}

/// The mosaic dish index of each antenna of a row: its aperture class's
/// position in the selection's `classes`, the order
/// [`crate::imaging::measurement::domain_operator`] lists the dishes in; a row
/// without response classes (every problem without an instrument model)
/// is on the first dish.
fn kernel_parallactic_angles(geometry: &BatchGeometry, sample: usize, time: usize) -> [f64; 2] {
    geometry.aw_parallactic_angle(time).map_or_else(
        || geometry.parallactic_angles(sample).unwrap_or([0.0; 2]),
        |pa| [-f64::from(pa); 2],
    )
}

fn antenna_types(
    responses: Option<SelectedAntennaResponses>,
    classes: &[AntennaResponseClass],
) -> [u8; 2] {
    let index = |class: AntennaResponseClass| {
        classes
            .iter()
            .position(|known| *known == class)
            .map_or(0, |index| u8::try_from(index).expect("few dish classes"))
    };
    responses.map_or([0, 0], |responses| {
        [index(responses.antenna1), index(responses.antenna2)]
    })
}

/// The offset of a row's pointing from `direction`'s reference pixel as
/// direction cosines along the image axes: the A-projection pointing pixel
/// when the selection evaluated one, else the antenna-1 pointing direction
/// on the image plane (`SimplePBConvFunc::findConvFunction` takes the
/// buffer's first `direction1`; under tclean `usepointing=False` the
/// selection evaluates it as the field's phase-tracking centre), placed in
/// the chart's frame and less a moving source's shift when the geometry
/// placed it ([`BatchGeometry::chart_pointing`]).
fn pointing_offset(
    (direction, coordinate): &(DirectionCoordinateSpec, DirectionCoordinate),
    aw_pixel: Option<[f64; 2]>,
    pointing: SkyDirection,
) -> Result<[f64; 2], SourceError> {
    let pixel = match aw_pixel {
        Some(pixel) => pixel,
        None => {
            if pointing.frame() != direction.reference_direction().frame() {
                return Err("a row's pointing direction is not in the image frame".into());
            }
            coordinate.world_to_pixel([pointing.longitude_rad(), pointing.latitude_rad()])?
        }
    };
    let reference = direction.reference_pixel();
    let increment = direction.increment_rad();
    Ok([
        (pixel[0] - reference[0]) * increment[0],
        (pixel[1] - reference[1]) * increment[1],
    ])
}

#[cfg(test)]
mod compact_layout_tests {
    use super::*;

    /// A compact row addresses the original batch span, including gaps
    /// before and between its channels; the write uses the same indices.
    #[test]
    fn compacting_channels_preserves_batch_offsets_and_admission() {
        let layout = Layout {
            channels: vec![0, 2, 5, 9],
            channel_indices: vec![10, 12, 15, 19],
            native_hz: vec![100.0, 102.0, 105.0, 109.0],
            half_widths_hz: vec![0.1, 0.2, 0.3, 0.4],
            correlations: vec![Correlation::XX, Correlation::YY],
            correlation_indices: vec![1, 3],
            parallel: vec![true, true],
        };
        let selected = layout
            .compact(&[false, true, false, true])
            .expect("two channels");
        assert_eq!(selected.channels, [2, 9]);
        assert_eq!(selected.channel_indices, [12, 19]);
        assert_eq!(selected.native_hz, [102.0, 109.0]);
        assert_eq!(selected.half_widths_hz, [0.2, 0.4]);
        assert_eq!(selected.correlation_indices, [1, 3]);
        assert!(layout.compact(&[false; 4]).is_none());
        let allocated = selected.channels.capacity() * size_of::<usize>()
            + selected.channel_indices.capacity() * size_of::<u32>()
            + (selected.native_hz.capacity() + selected.half_widths_hz.capacity())
                * size_of::<f64>()
            + selected.correlations.capacity() * size_of::<Correlation>()
            + selected.correlation_indices.capacity() * size_of::<u32>()
            + selected.parallel.capacity() * size_of::<bool>();
        let shape = BlockShape {
            spectrum: casa_imaging_operator::RowSpectrum::evenly_spaced(4),
            correlations: 2,
            ..BlockShape::default()
        };
        assert!(
            allocated <= Layout::compacted_bound(shape),
            "{allocated} bytes exceed compact layout admission"
        );
    }
    /// The fit buffers retain capacity as rows of different sizes arrive;
    /// their admitted bound covers the full input union and mapping.
    #[test]
    fn compacted_fit_scratch_stays_within_its_admission() {
        let mut scratch = PackScratch::default();
        for channels in [1, 17, 33, 65] {
            let cells = channels * 4;
            scratch.fit_values.resize(cells, Complex32::default());
            scratch.fit_weights.resize(cells, 0.0);
            scratch.fit_flags.resize(cells, false);
            scratch.fit_positions.clear();
            scratch.fit_positions.extend(0..channels);
        }
        let held = scratch.fit_values.capacity() * size_of::<Complex32>()
            + scratch.fit_weights.capacity() * size_of::<f32>()
            + scratch.fit_flags.capacity() * size_of::<bool>()
            + scratch.fit_positions.capacity() * size_of::<usize>();
        let shape = BlockShape {
            spectrum: casa_imaging_operator::RowSpectrum::evenly_spaced(65),
            correlations: 4,
            ..BlockShape::default()
        };
        assert!(
            held <= PackScratch::fit_bound(shape),
            "{held} bytes exceed fit scratch admission"
        );
    }
}
