// SPDX-License-Identifier: LGPL-3.0-or-later
//! MAIN's rows of an MSv2 a plan creates: one per sample, padding a flagged
//! row, or one per acquisition with padding omitted, input after input and
//! in batch order. A sample's array cells go to the variable-shape creation
//! writers as they are read; its keys are gathered per batch and pushed to
//! the scalar columns a column at a time. Each input's identities are
//! written as the output's through its [`RowMap`].

use std::collections::BTreeMap;
use std::path::Path;

use casa_observation::{Catalogue, ObservationError, VisibilityBatch};
use casa_tables::{ColumnOverrides, StreamedScalarValues, StreamedTiledShapeValueType};
use casa_types::ScalarValue;
use casa_types::measures::MjdHighPrec;
use ndarray::{ArrayView2, ArrayView3, Axis};
use num_complex::Complex32;

use super::PaddingRows;
use super::msv2_tables::Layout;
use crate::observation_io::update::{request_error, storage};
use crate::write_session::{
    MeasurementSetArrayColumnPlan, MeasurementSetArrayShapePlan, MeasurementSetColumnStorage,
    MeasurementSetWritePlan, MeasurementSetWriteResources, MeasurementSetWriteSession,
    ScalarColumnId, standard_main_scalar_column_plans,
};
use crate::{MeasurementSet, initial_model_visibility};

/// The array columns every created MSv2 holds besides its data columns.
const ARRAY_COLUMNS: [&str; 5] = ["FLAG", "FLAG_CATEGORY", "UVW", "WEIGHT", "SIGMA"];

/// The rows each output partition will hold, from its inputs' grids.
pub(super) struct Occupancy {
    /// Rows of each output partition: its inputs' grid cells, or with
    /// padding omitted their acquisitions (samples that are not padding).
    pub(super) rows: Vec<usize>,
    /// Each output partition's grid interval, a padded row's `INTERVAL`.
    pub(super) intervals: Vec<f64>,
    /// The times the grids span.
    pub(super) spans: Spans,
}

/// The times an observation's grids span, which its subtables' times
/// state.
pub(super) struct Spans {
    /// The first and last time of any grid, MJD seconds.
    pub(super) time_range: [f64; 2],
    /// Each session's grids' span, by session: from the start of the
    /// first integration to the end of the last.
    pub(super) observation_ranges: Vec<[f64; 2]>,
}

/// Where a data column's values come from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ColumnSource {
    /// The read data group of this index.
    Group(usize),
    /// CASA's initial model (`VisSetUtil::initScrCols`): 1 on the parallel
    /// hands and 0 on the cross hands, as `initial_model_visibility` gives.
    InitialModel,
}

/// Where one input partition's rows go, and what they are written with.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct Placement {
    /// The output partition.
    pub(super) output: usize,
    /// `STATE_ID`: a merged state, -1 for none, or `None` for the output
    /// partition's state of intents, written for partitions that state
    /// intents but no state.
    pub(super) state: Option<i32>,
    /// `PROCESSOR_ID`, -1 for none.
    pub(super) processor: i32,
    /// Added to the input's scan numbers.
    pub(super) scan_offset: i32,
    /// The data groups read, the first with every role.
    pub(super) groups: Vec<String>,
    /// Each data column's source, in data-column order.
    pub(super) columns: Vec<ColumnSource>,
}

/// One input's identities in the output, and its weights' scale.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct RowMap {
    /// `ANTENNA` row of each of the input's antennas.
    pub(super) antennas: Vec<i32>,
    /// `FIELD` row of each of the input's fields.
    pub(super) fields: Vec<i32>,
    /// `SCAN_NUMBER` of each of the input's scans, before its partition's
    /// offset.
    pub(super) scans: Vec<i32>,
    /// Each of the input's partitions.
    pub(super) partitions: Vec<Placement>,
    /// The factor every acquisition's weights are multiplied by.
    pub(super) weight_scale: f32,
}

/// Whether a sample's weights (`[channel, correlation]`) are an
/// acquisition's rather than padding's, whose every weight is non-finite.
///
/// # Errors
///
/// An acquisition whose weights vary over channels: an MSv2 `WEIGHT` holds
/// one per correlation, and the conversion writes no `WEIGHT_SPECTRUM`.
pub(super) fn acquired(weights: ArrayView2<'_, f32>) -> Result<bool, ObservationError> {
    let Some(first) = weights.outer_iter().next() else {
        return Ok(false);
    };
    if weights.iter().all(|weight| !weight.is_finite()) {
        return Ok(false);
    }
    let varies = weights.outer_iter().skip(1).any(|channel| {
        channel
            .iter()
            .zip(first.iter())
            .any(|(weight, first)| weight.to_bits() != first.to_bits())
    });
    if varies {
        return Err(ObservationError::Unsupported {
            what: "converting weights that vary over channels into an MSv2 (WEIGHT_SPECTRUM)",
        });
    }
    Ok(true)
}

/// CASA's `SIGMA` for a weight, as `mstransform` derives it
/// (`weightToSigma`): `1/√w`, and -1 where `w` is not positive.
fn sigma(weight: f32) -> f32 {
    if weight > f32::MIN_POSITIVE {
        1.0 / weight.sqrt()
    } else {
        -1.0
    }
}

/// One sample's cells as a batch holds them: each data column's values
/// (`None` for the initial model), the flags and the weights.
type Cells<'a, 'b> = (
    &'a [Option<ArrayView3<'b, Complex32>>],
    ArrayView3<'b, bool>,
    ArrayView2<'b, f32>,
);

/// The scalar columns, resolved once.
struct Scalars {
    antenna1: ScalarColumnId,
    antenna2: ScalarColumnId,
    field: ScalarColumnId,
    scan: ScalarColumnId,
    time: ScalarColumnId,
    centroid: ScalarColumnId,
    interval: ScalarColumnId,
    exposure: ScalarColumnId,
    /// Columns constant over an input partition: `ARRAY_ID`,
    /// `DATA_DESC_ID`, `FEED1`, `FEED2`, `FLAG_ROW`, `OBSERVATION_ID`,
    /// `PROCESSOR_ID`, `STATE_ID`.
    constant: [ScalarColumnId; 8],
}

/// One batch's keys, gathered for the scalar columns.
#[derive(Default)]
struct Keys {
    antenna1: Vec<i32>,
    antenna2: Vec<i32>,
    field: Vec<i32>,
    scan: Vec<i32>,
    time: Vec<f64>,
    centroid: Vec<f64>,
    interval: Vec<f64>,
    exposure: Vec<f64>,
}

impl Keys {
    fn clear(&mut self) {
        self.antenna1.clear();
        self.antenna2.clear();
        self.field.clear();
        self.scan.clear();
        self.time.clear();
        self.centroid.clear();
        self.interval.clear();
        self.exposure.clear();
    }
}

/// Cells copied when a view is not contiguous: an MSv2 source's weights
/// (one per correlation, a zero stride along channels) and any strided
/// payload; and a batch's initial-model cell.
#[derive(Default)]
struct Scratch {
    values: Vec<Complex32>,
    flags: Vec<bool>,
    weights: Vec<f32>,
    sigmas: Vec<f32>,
    model: Vec<Complex32>,
}

/// What every row of an output partition shares.
struct Output {
    observation: i32,
    feeds: [i32; 2],
    /// The grid interval, a padded row's `INTERVAL`.
    interval: f64,
}

/// The creation session of a created MSv2's MAIN.
pub(super) struct MainRows {
    session: MeasurementSetWriteSession,
    layout: Layout,
    outputs: Vec<Output>,
    padding: PaddingRows,
    data_columns: Vec<&'static str>,
    scalars: Scalars,
    planned: usize,
    rows: usize,
    keys: Keys,
    scratch: Scratch,
}

impl MainRows {
    /// Plan and start MAIN's writers at `staging` for the `occupancy` of
    /// `catalogue`'s (output) partitions, with `data_columns` (`DATA`,
    /// `CORRECTED_DATA`, `MODEL_DATA`), the subtables' `layout` and the
    /// `padding` policy the occupancy was counted under.
    pub(super) fn start(
        staging: &Path,
        catalogue: &Catalogue,
        data_columns: Vec<&'static str>,
        occupancy: &Occupancy,
        layout: Layout,
        padding: PaddingRows,
    ) -> Result<Self, ObservationError> {
        let planned = occupancy.rows.iter().sum();
        let plan = plan(catalogue, &data_columns, &occupancy.rows, planned)?;
        let session = MeasurementSetWriteSession::start(staging, plan).map_err(storage)?;
        let id = |name: &str| session.scalar_column(name).map_err(storage);
        let scalars = Scalars {
            antenna1: id("ANTENNA1")?,
            antenna2: id("ANTENNA2")?,
            field: id("FIELD_ID")?,
            scan: id("SCAN_NUMBER")?,
            time: id("TIME")?,
            centroid: id("TIME_CENTROID")?,
            interval: id("INTERVAL")?,
            exposure: id("EXPOSURE")?,
            constant: [
                id("ARRAY_ID")?,
                id("DATA_DESC_ID")?,
                id("FEED1")?,
                id("FEED2")?,
                id("FLAG_ROW")?,
                id("OBSERVATION_ID")?,
                id("PROCESSOR_ID")?,
                id("STATE_ID")?,
            ],
        };
        let outputs = catalogue
            .partitions
            .iter()
            .zip(&occupancy.intervals)
            .map(|(entry, interval)| {
                let [feed1, feed2] = entry.feeds.unwrap_or([0, 0]);
                Output {
                    observation: entry.observation.0 as i32,
                    feeds: [feed1 as i32, feed2 as i32],
                    interval: *interval,
                }
            })
            .collect();
        Ok(Self {
            session,
            layout,
            outputs,
            padding,
            data_columns,
            scalars,
            planned,
            rows: 0,
            keys: Keys::default(),
            scratch: Scratch::default(),
        })
    }

    /// Write a row for every sample of `batches` (every acquisition, with
    /// padding omitted), one batch per group the partition reads, of the
    /// same times, the first with every role; the input's identities
    /// written as `map` gives them.
    pub(super) fn push(
        &mut self,
        map: &RowMap,
        batches: &[VisibilityBatch<'_>],
    ) -> Result<(), ObservationError> {
        let batch = &batches[0];
        let missing = |role: &str| request_error(format!("a converted batch carries its {role}"));
        let weights = batch.group.weights.ok_or_else(|| missing("weights"))?;
        let flags = batch.group.flags.ok_or_else(|| missing("flags"))?;
        let uvw = batch.group.uvw.ok_or_else(|| missing("UVW"))?;
        let centroid = batch
            .timing
            .centroid
            .ok_or_else(|| missing("time centroids"))?;
        if batches.iter().any(|each| {
            each.partition != batch.partition
                || each.samples() != batch.samples()
                || each.time.offsets_s() != batch.time.offsets_s()
        }) {
            return Err(request_error(
                "a further group's batch is not the first's partition and times",
            ));
        }
        let placement = &map.partitions[batch.partition.0 as usize];
        let values = placement
            .columns
            .iter()
            .map(|source| match source {
                ColumnSource::Group(index) => batches[*index]
                    .group
                    .values
                    .map(Some)
                    .ok_or_else(|| missing("visibilities")),
                ColumnSource::InitialModel => Ok(None),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let shape = [batch.correlations.len(), batch.channel_count()];
        if placement.columns.contains(&ColumnSource::InitialModel) {
            self.scratch.model.clear();
            for _ in 0..shape[1] {
                self.scratch.model.extend(
                    batch
                        .correlations
                        .iter()
                        .map(|correlation| initial_model_visibility(correlation.casacore_code())),
                );
            }
        }
        let origin = MjdHighPrec::new(0.0, 0.0);
        let interval = self.outputs[placement.output].interval;
        self.keys.clear();
        for sample in 0..batch.samples() {
            let sample_weights = weights.index_axis(Axis(0), sample);
            let acquisition = acquired(sample_weights)?;
            if !acquisition && self.padding == PaddingRows::Omitted {
                continue;
            }
            self.push_cells(
                sample,
                shape,
                (&values, flags, sample_weights),
                (acquisition, map.weight_scale),
            )?;
            self.session
                .push_f64_row("UVW", &[3], &uvw.metres_at(sample))
                .map_err(storage)?;
            let coordinate = batch.sample(sample);
            let scan = batch.scan(sample).ok_or_else(|| missing("scans"))?;
            let time = batch
                .time
                .seconds_since(sample / batch.samples_per_time(), origin);
            let keys = &mut self.keys;
            keys.antenna1
                .push(map.antennas[coordinate.baseline.antenna1.0 as usize]);
            keys.antenna2
                .push(map.antennas[coordinate.baseline.antenna2.0 as usize]);
            keys.field.push(map.fields[coordinate.field.0 as usize]);
            keys.scan
                .push(map.scans[scan.0 as usize] + placement.scan_offset);
            keys.time.push(time);
            if acquisition {
                keys.centroid.push(centroid.seconds_since(sample, origin));
                keys.interval.push(
                    batch
                        .interval_s(sample)
                        .ok_or_else(|| missing("intervals"))?,
                );
                keys.exposure.push(
                    batch
                        .exposure_s(sample)
                        .ok_or_else(|| missing("exposures"))?,
                );
            } else {
                // A padded row's keys stay valid: nothing was integrated,
                // over the grid's interval, centred on its time.
                keys.centroid.push(time);
                keys.interval.push(interval);
                keys.exposure.push(0.0);
            }
        }
        self.push_keys(placement)
    }

    /// One sample's visibility, flag, weight and sigma cells; padding's
    /// flagged throughout with NaN weights, padding's one mark. An
    /// acquisition's weights are multiplied by `scale`, and its sigmas
    /// derived from the scaled weights, so `SIGMA` is `1/√(W·w)`.
    fn push_cells(
        &mut self,
        sample: usize,
        shape: [usize; 2],
        (values, flags, weights): Cells<'_, '_>,
        (acquisition, scale): (bool, f32),
    ) -> Result<(), ObservationError> {
        // A sample's `[channel, correlation]` cells, correlation fastest,
        // are an MSv2 cell of shape `[correlation, channel]` as casacore
        // stores it.
        let scratch = &mut self.scratch;
        for (column, values) in self.data_columns.iter().zip(values) {
            let cell = match values {
                Some(values) => contiguous(values.index_axis(Axis(0), sample), &mut scratch.values),
                None => &scratch.model,
            };
            self.session
                .push_complex32_row(column, &shape, cell)
                .map_err(storage)?;
        }
        let cell = if acquisition {
            contiguous(flags.index_axis(Axis(0), sample), &mut scratch.flags)
        } else {
            scratch.flags.clear();
            scratch.flags.resize(shape[0] * shape[1], true);
            &scratch.flags
        };
        self.session
            .push_bool_row("FLAG", &shape, cell)
            .map_err(storage)?;
        self.session
            .push_undefined_row("FLAG_CATEGORY")
            .map_err(storage)?;
        scratch.weights.clear();
        if acquisition {
            scratch
                .weights
                .extend(weights.row(0).iter().map(|weight| weight * scale));
        } else {
            scratch.weights.resize(shape[0], f32::NAN);
        }
        scratch.sigmas.clear();
        scratch
            .sigmas
            .extend(scratch.weights.iter().map(|weight| sigma(*weight)));
        let correlations = [shape[0]];
        self.session
            .push_f32_row("WEIGHT", &correlations, &scratch.weights)
            .map_err(storage)?;
        self.session
            .push_f32_row("SIGMA", &correlations, &scratch.sigmas)
            .map_err(storage)
    }

    /// The batch's gathered keys, and its input partition's constant ones.
    fn push_keys(&mut self, placement: &Placement) -> Result<(), ObservationError> {
        let keys = &self.keys;
        let count = keys.antenna1.len();
        let scalars = &self.scalars;
        for (column, values) in [
            (
                scalars.antenna1,
                StreamedScalarValues::Int32(&keys.antenna1),
            ),
            (
                scalars.antenna2,
                StreamedScalarValues::Int32(&keys.antenna2),
            ),
            (scalars.field, StreamedScalarValues::Int32(&keys.field)),
            (scalars.scan, StreamedScalarValues::Int32(&keys.scan)),
            (scalars.time, StreamedScalarValues::Float64(&keys.time)),
            (
                scalars.centroid,
                StreamedScalarValues::Float64(&keys.centroid),
            ),
            (
                scalars.interval,
                StreamedScalarValues::Float64(&keys.interval),
            ),
            (
                scalars.exposure,
                StreamedScalarValues::Float64(&keys.exposure),
            ),
        ] {
            self.session
                .push_scalar_column(column, values)
                .map_err(storage)?;
        }
        let output = placement.output;
        let [feed1, feed2] = self.outputs[output].feeds;
        let state = placement
            .state
            .unwrap_or_else(|| self.layout.intent_states[output].unwrap_or(-1));
        let constants = [
            ScalarValue::Int32(0),
            ScalarValue::Int32(self.layout.data_description[output]),
            ScalarValue::Int32(feed1),
            ScalarValue::Int32(feed2),
            ScalarValue::Bool(false),
            ScalarValue::Int32(self.outputs[output].observation),
            ScalarValue::Int32(placement.processor),
            ScalarValue::Int32(state),
        ];
        for (column, value) in scalars.constant.iter().zip(constants) {
            self.session
                .push_repeated_scalar(*column, Some(value), count)
                .map_err(storage)?;
        }
        self.rows += count;
        Ok(())
    }

    /// Save `ms` with MAIN's columns installed; the rows written.
    ///
    /// # Errors
    ///
    /// Fewer rows than planned (an input changed between the passes), or
    /// a write failure.
    pub(super) fn finish(self, ms: &mut MeasurementSet) -> Result<usize, ObservationError> {
        if self.rows != self.planned {
            return Err(request_error(format!(
                "{} rows written of the {} the occupancy pass counted",
                self.rows, self.planned
            )));
        }
        let mut overrides = ColumnOverrides::for_row_count(self.rows);
        for column in self.data_columns.iter().chain(&ARRAY_COLUMNS) {
            overrides.insert_deferred(*column);
        }
        self.session
            .save_and_finish(ms, &overrides)
            .map_err(storage)?;
        Ok(self.rows)
    }
}

/// `view`'s elements in order: the view itself when contiguous, else a
/// copy in `scratch`.
fn contiguous<'a, T: Copy>(view: ArrayView2<'a, T>, scratch: &'a mut Vec<T>) -> &'a [T] {
    match view.to_slice() {
        Some(slice) => slice,
        None => {
            scratch.clear();
            scratch.extend(view.iter().copied());
            scratch
        }
    }
}

/// The variable-shape creation plan: one hypercube per visibility shape
/// and per correlation count, sized by the rows each holds.
fn plan(
    catalogue: &Catalogue,
    data_columns: &[&'static str],
    rows: &[usize],
    total: usize,
) -> Result<MeasurementSetWritePlan, ObservationError> {
    if total == 0 {
        return Err(request_error(
            "the source holds nothing to write as an MSv2 row",
        ));
    }
    let telescope = catalogue
        .observations
        .first()
        .map_or("", |observation| observation.telescope.as_str());
    let mut by_shape = BTreeMap::<(usize, usize), usize>::new();
    let mut by_correlations = BTreeMap::<usize, usize>::new();
    for (entry, rows) in catalogue.partitions.iter().zip(rows) {
        let correlations = entry.correlations.len();
        *by_shape
            .entry((correlations, entry.frequencies_hz.len()))
            .or_default() += rows;
        *by_correlations.entry(correlations).or_default() += rows;
    }
    let visibility: Vec<_> = by_shape
        .iter()
        .map(|(&(correlations, channels), &rows)| {
            MeasurementSetArrayShapePlan::visibility(correlations, channels, rows, telescope)
        })
        .collect();
    let flag_category = by_shape
        .iter()
        .map(|(&(correlations, channels), &rows)| {
            MeasurementSetArrayShapePlan::flag_category(correlations, channels, 1, rows, telescope)
        })
        .collect();
    let weight: Vec<_> = by_correlations
        .iter()
        .map(|(&correlations, &rows)| {
            MeasurementSetArrayShapePlan::weight(correlations, rows, telescope)
        })
        .collect();
    let uvw = MeasurementSetArrayShapePlan {
        cell_shape: vec![3],
        row_count: total,
        tile_shape: crate::ms::casa_uvw_tile_shape(&visibility[0].tile_shape),
    };
    let column = |name: &str,
                  value_type: StreamedTiledShapeValueType,
                  shapes: Vec<MeasurementSetArrayShapePlan>,
                  storage_manager: MeasurementSetColumnStorage| {
        MeasurementSetArrayColumnPlan {
            name: name.to_string(),
            value_type,
            shapes,
            storage_manager,
        }
    };
    let shaped = MeasurementSetColumnStorage::TiledShape;
    let mut columns: Vec<_> = data_columns
        .iter()
        .map(|name| {
            column(
                name,
                StreamedTiledShapeValueType::Complex32,
                visibility.clone(),
                shaped,
            )
        })
        .collect();
    columns.extend([
        column(
            "FLAG",
            StreamedTiledShapeValueType::Bool,
            visibility,
            shaped,
        ),
        column(
            "FLAG_CATEGORY",
            StreamedTiledShapeValueType::Bool,
            flag_category,
            shaped,
        ),
        column(
            "UVW",
            StreamedTiledShapeValueType::Float64,
            vec![uvw],
            MeasurementSetColumnStorage::TiledColumn,
        ),
        column(
            "WEIGHT",
            StreamedTiledShapeValueType::Float32,
            weight.clone(),
            shaped,
        ),
        column(
            "SIGMA",
            StreamedTiledShapeValueType::Float32,
            weight,
            shaped,
        ),
    ]);
    let resources = MeasurementSetWriteResources::from_system_memory(2).map_err(storage)?;
    MeasurementSetWritePlan::variable_array_creation(
        total,
        columns,
        standard_main_scalar_column_plans(),
        resources,
    )
    .map_err(storage)
}

#[cfg(test)]
mod tests {
    use ndarray::array;

    use super::{acquired, sigma};

    #[test]
    fn padding_is_a_sample_of_non_finite_weights_only() {
        assert!(!acquired(array![[f32::NAN, f32::NAN], [f32::NAN, f32::INFINITY]].view()).unwrap());
        assert!(acquired(array![[1.0, f32::NAN], [1.0, f32::NAN]].view()).unwrap());
        assert!(acquired(array![[0.0, 0.0], [0.0, 0.0]].view()).unwrap());
        assert!(
            acquired(array![[1.0, 2.0], [1.0, 3.0]].view()).is_err(),
            "weights varying over channels are refused"
        );
    }

    #[test]
    fn sigma_is_casas_inverse_of_the_weight() {
        assert_eq!(sigma(4.0), 0.5);
        assert_eq!(sigma(0.0), -1.0);
        assert_eq!(sigma(-1.0), -1.0);
        assert_eq!(sigma(f32::NAN), -1.0);
    }
}
