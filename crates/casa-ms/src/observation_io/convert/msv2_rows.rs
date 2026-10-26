// SPDX-License-Identifier: LGPL-3.0-or-later
//! MAIN's rows of an MSv2 a conversion creates: one per acquisition, in
//! batch order. A sample's array cells go to the variable-shape creation
//! writers as they are read; its keys are gathered per batch and pushed to
//! the scalar columns a column at a time.

use std::collections::BTreeMap;
use std::path::Path;

use casa_observation::{Catalogue, ObservationError, VisibilityBatch};
use casa_tables::{ColumnOverrides, StreamedScalarValues, StreamedTiledShapeValueType};
use casa_types::ScalarValue;
use casa_types::measures::MjdHighPrec;
use ndarray::{ArrayView2, ArrayView3, Axis};
use num_complex::Complex32;

use super::msv2_tables::Layout;
use crate::MeasurementSet;
use crate::observation_io::update::{request_error, storage};
use crate::write_session::{
    MeasurementSetArrayColumnPlan, MeasurementSetArrayShapePlan, MeasurementSetColumnStorage,
    MeasurementSetWritePlan, MeasurementSetWriteResources, MeasurementSetWriteSession,
    ScalarColumnId, standard_main_scalar_column_plans,
};

/// The array columns every converted MSv2 holds besides its data columns.
const ARRAY_COLUMNS: [&str; 5] = ["FLAG", "FLAG_CATEGORY", "UVW", "WEIGHT", "SIGMA"];

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
    /// Columns constant over a partition: `ARRAY_ID`, `DATA_DESC_ID`,
    /// `FEED1`, `FEED2`, `FLAG_ROW`, `OBSERVATION_ID`, `PROCESSOR_ID`,
    /// `STATE_ID`.
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
/// payload.
#[derive(Default)]
struct Scratch {
    values: Vec<Complex32>,
    flags: Vec<bool>,
    weights: Vec<f32>,
    sigmas: Vec<f32>,
}

/// The creation session of a converted MSv2's MAIN.
pub(super) struct MainRows {
    session: MeasurementSetWriteSession,
    layout: Layout,
    data_columns: Vec<&'static str>,
    scalars: Scalars,
    planned: usize,
    rows: usize,
    keys: Keys,
    scratch: Scratch,
}

impl MainRows {
    /// Plan and start MAIN's writers at `staging` for `rows` acquisitions
    /// of each of `catalogue`'s partitions, with `data_columns` (`DATA`,
    /// `CORRECTED_DATA`, `MODEL_DATA`) and the subtables' `layout`.
    pub(super) fn start(
        staging: &Path,
        catalogue: &Catalogue,
        data_columns: Vec<&'static str>,
        rows: &[usize],
        layout: Layout,
    ) -> Result<Self, ObservationError> {
        let planned = rows.iter().sum();
        let plan = plan(catalogue, &data_columns, rows, planned)?;
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
        Ok(Self {
            session,
            layout,
            data_columns,
            scalars,
            planned,
            rows: 0,
            keys: Keys::default(),
            scratch: Scratch::default(),
        })
    }

    /// Write a row for every acquisition of `batches`, one batch per data
    /// column of the same times, the first with every role.
    pub(super) fn push(&mut self, batches: &[VisibilityBatch<'_>]) -> Result<(), ObservationError> {
        let batch = &batches[0];
        let missing = |role: &str| request_error(format!("a converted batch carries its {role}"));
        let weights = batch.group.weights.ok_or_else(|| missing("weights"))?;
        let flags = batch.group.flags.ok_or_else(|| missing("flags"))?;
        let uvw = batch.group.uvw.ok_or_else(|| missing("UVW"))?;
        let centroid = batch
            .timing
            .centroid
            .ok_or_else(|| missing("time centroids"))?;
        let values = batches
            .iter()
            .map(|each| each.group.values.ok_or_else(|| missing("visibilities")))
            .collect::<Result<Vec<_>, _>>()?;
        let shape = [batch.correlations.len(), batch.channel_count()];
        let origin = MjdHighPrec::new(0.0, 0.0);
        self.keys.clear();
        for sample in 0..batch.samples() {
            let sample_weights = weights.index_axis(Axis(0), sample);
            if !acquired(sample_weights)? {
                continue;
            }
            self.push_cells(sample, shape, &values, flags, sample_weights)?;
            self.session
                .push_f64_row("UVW", &[3], &uvw.metres_at(sample))
                .map_err(storage)?;
            let coordinate = batch.sample(sample);
            let scan = batch.scan(sample).ok_or_else(|| missing("scans"))?;
            let keys = &mut self.keys;
            keys.antenna1.push(coordinate.baseline.antenna1.0 as i32);
            keys.antenna2.push(coordinate.baseline.antenna2.0 as i32);
            keys.field.push(coordinate.field.0 as i32);
            keys.scan.push(self.layout.scan_number[scan.0 as usize]);
            keys.time.push(
                batch
                    .time
                    .seconds_since(sample / batch.samples_per_time(), origin),
            );
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
        }
        self.push_keys(batch.partition.0 as usize, batch)
    }

    /// One acquisition's visibility, flag, weight and sigma cells.
    fn push_cells(
        &mut self,
        sample: usize,
        shape: [usize; 2],
        values: &[ArrayView3<'_, Complex32>],
        flags: ArrayView3<'_, bool>,
        weights: ArrayView2<'_, f32>,
    ) -> Result<(), ObservationError> {
        // A sample's `[channel, correlation]` cells, correlation fastest,
        // are an MSv2 cell of shape `[correlation, channel]` as casacore
        // stores it.
        let scratch = &mut self.scratch;
        for (column, values) in self.data_columns.iter().zip(values) {
            let cell = contiguous(values.index_axis(Axis(0), sample), &mut scratch.values);
            self.session
                .push_complex32_row(column, &shape, cell)
                .map_err(storage)?;
        }
        let cell = contiguous(flags.index_axis(Axis(0), sample), &mut scratch.flags);
        self.session
            .push_bool_row("FLAG", &shape, cell)
            .map_err(storage)?;
        self.session
            .push_undefined_row("FLAG_CATEGORY")
            .map_err(storage)?;
        scratch.weights.clear();
        scratch.weights.extend(weights.row(0).iter().copied());
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

    /// The batch's gathered keys, and its partition's constant ones.
    fn push_keys(
        &mut self,
        partition: usize,
        batch: &VisibilityBatch<'_>,
    ) -> Result<(), ObservationError> {
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
        let entry = &batch.catalogue.partitions[partition];
        let [feed1, feed2] = entry.feeds.unwrap_or([0, 0]);
        let constants = [
            ScalarValue::Int32(0),
            ScalarValue::Int32(self.layout.data_description[partition]),
            ScalarValue::Int32(feed1 as i32),
            ScalarValue::Int32(feed2 as i32),
            ScalarValue::Bool(false),
            ScalarValue::Int32(entry.observation.0 as i32),
            ScalarValue::Int32(0),
            ScalarValue::Int32(self.layout.state[partition]),
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
    /// Fewer rows than planned (the source changed between the passes), or
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
            "the source holds no acquisition to write as an MSv2 row",
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
