// SPDX-License-Identifier: LGPL-3.0-or-later
//! One batch packed into a native block: each selected sample's geometry,
//! values, weights and flags, copied once.

use casa_imaging_deconvolution::direction_world_to_pixel;
use casa_imaging_model::{
    AntennaResponseClass, BatchGeometry, CompiledProblem, DirectionCoordinateSpec,
    FiniteValuePolicy, SelectedAntennaResponses, SkyDirection,
};
use casa_imaging_operator::RowContext;
use casa_imaging_runtime::pass::{
    BlockShape, DomainProjection, NativeBlock, NativeRowHeader, RowAddress, SourceError,
};
use casa_observation::{Correlation, PartitionEntry, VisibilityBatch};
use casa_types::measures::MjdHighPrec;
use num_complex::Complex32;

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
    /// The routed products, in the operator's order.
    correlations: Vec<Correlation>,
    /// Their stored indices, the block's correlation indices.
    correlation_indices: Vec<u32>,
    /// Whether each enters Stokes I.
    parallel: Vec<bool>,
}

impl Layout {
    /// The layout of `partition`'s selected channels `ordinals` (indices
    /// into its selected channels), read as the native channels from the
    /// first of them.
    pub(super) fn new(
        partition: &SelectedPartition,
        entry: &PartitionEntry,
        ordinals: std::ops::Range<usize>,
    ) -> Result<Self, SourceError> {
        let channel_indices = partition.channels[ordinals].to_vec();
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

/// Which samples of `batch` the selection images, into `keep`: those that
/// are no padding (every flag set and every weight not finite, as the MSv2
/// adapter pads and XRADIO's NaN cells read) and whose UV distance it
/// selects. Fields and intents were selected by the request.
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
        let scan_selected = selection.scan_holds(batch, time)?;
        keep.extend((time * baselines..(time + 1) * baselines).map(|sample| {
            let flags = flags.index_axis(ndarray::Axis(0), sample);
            let weights = weights.index_axis(ndarray::Axis(0), sample);
            let padding =
                flags.iter().all(|flag| *flag) && weights.iter().all(|weight| !weight.is_finite());
            scan_selected && !padding && selection.uv_holds(partition, uvw.metres_at(sample))
        }));
    }
    Ok(())
}

/// What packing reads besides the batch: the problem and the per-chart
/// laws, fixed for a run.
pub(super) struct Packer<'p> {
    pub(super) problem: &'p CompiledProblem,
    /// Projections per row: one per chart.
    pub(super) charts: usize,
    /// Each chart's direction law when a kernel set ramps each row to its
    /// pointing; empty otherwise.
    pub(super) pointing: Vec<DirectionCoordinateSpec>,
    /// The aperture classes in dish order.
    pub(super) dish_classes: Vec<AntennaResponseClass>,
}

/// Buffers packing reuses from batch to batch.
#[derive(Debug, Default)]
pub(super) struct PackScratch {
    projections: Vec<DomainProjection>,
    frequencies_hz: Vec<f64>,
    /// Each routed product's place along the batch's correlations.
    routed: Vec<usize>,
    /// One sample's weight per delivered product, for weights stored per
    /// correlation.
    row_weights: Vec<f32>,
}

impl PackScratch {
    /// At most what the buffers come to hold for blocks of `layout`'s rows,
    /// each growing at most to twice what it needs.
    pub(super) fn bound(layout: BlockShape) -> usize {
        2 * (layout.domains * size_of::<DomainProjection>()
            + layout.channels() * size_of::<f64>()
            + layout.correlations * (size_of::<usize>() + size_of::<f32>()))
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

impl Packer<'_> {
    /// Pack `batch`'s samples `keep` names into `out` under `layout`, with
    /// `geometry` applied to those samples. Unread visibilities are zero.
    /// A non-finite input fails the run under [`FiniteValuePolicy`], naming
    /// the sample.
    pub(super) fn pack(
        &self,
        (batch, partition, layout): (&VisibilityBatch<'_>, &SelectedPartition, &Layout),
        geometry: &BatchGeometry,
        keep: &[bool],
        values: bool,
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
        let mut packed_time = None;
        for sample in (0..batch.samples()).filter(|sample| keep[*sample]) {
            let time = sample / per_time;
            if packed_time != Some(time) {
                let frequencies = geometry.frequencies_hz(time);
                scratch.frequencies_hz.clear();
                scratch
                    .frequencies_hz
                    .extend(layout.channels.iter().map(|channel| frequencies[*channel]));
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
            let flagged = layout.channels.iter().all(|channel| {
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
                    original_uvw_m: Some(uvw.metres_at(sample)),
                    time_s: row.time_s,
                    antennas: [baseline.antenna1.0, baseline.antenna2.0],
                    antenna_types: antenna_types(
                        geometry.antenna_responses(sample),
                        &self.dish_classes,
                    ),
                    parallactic_angle_rad: geometry.parallactic_angles(sample).unwrap_or([0.0; 2]),
                    field: field.0,
                    spectral_window: partition.spectral_window,
                },
                address: RowAddress::UNADDRESSED,
            };
            let rule = self
                .problem
                .visibility_transform()
                .and_then(|transform| transform.rule(field.0 as i32, partition.spectral_window));
            let row_weights = &scratch.row_weights;
            let routed = &scratch.routed;
            out.push_row_with(
                header,
                &scratch.projections,
                &scratch.frequencies_hz,
                |values_out, weights_out, flags_out| {
                    fill_samples(
                        &row,
                        (&arrays, row_weights, routed, layout),
                        (values_out, weights_out, flags_out),
                        reject_flagged,
                    )?;
                    // A row's uvw matters only if one of its samples is imaged.
                    let imaged = !flagged && flags_out.iter().any(|flag| !*flag);
                    if nonfinite_uvw && (reject_flagged || imaged) {
                        return Err(nonfinite_input("uvw", &row.at(None)));
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
                    *direction,
                    geometry.aw_pointing_pixel(sample, chart),
                    geometry.pointing(sample).antenna1,
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
/// selection evaluates it as the field's phase-tracking centre).
fn pointing_offset(
    direction: DirectionCoordinateSpec,
    aw_pixel: Option<[f64; 2]>,
    pointing: SkyDirection,
) -> Result<[f64; 2], SourceError> {
    let pixel = match aw_pixel {
        Some(pixel) => pixel,
        None => {
            if pointing.frame() != direction.reference_direction().frame() {
                return Err("a row's pointing direction is not in the image frame".into());
            }
            direction_world_to_pixel(
                direction,
                [pointing.longitude_rad(), pointing.latitude_rad()],
            )?
        }
    };
    let reference = direction.reference_pixel();
    let increment = direction.increment_rad();
    Ok([
        (pixel[0] - reference[0]) * increment[0],
        (pixel[1] - reference[1]) * increment[1],
    ])
}
