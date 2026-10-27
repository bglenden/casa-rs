// SPDX-License-Identifier: LGPL-3.0-or-later
//! Imaging port step 3b's equality law (#932): the row geometry today's
//! selected-observation route gives one imaging request, beside the batch
//! geometry ([`GeometryPlan`]) of the same MeasurementSet read through the
//! observation boundary, compared sample for sample. Evidence for the port
//! only: it goes with today's route at the cutover.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;

use casa_imaging_model::{
    BatchGeometry, CompiledProblem, GeometryPlan, GeometryTraversal, PointingCentreLaw,
    PointingDirectionColumn, SelectedAntennaResponses, SelectedPointingDirections, SkyDirection,
};
use casa_imaging_runtime::{Admitted, finalize_source_access, row_layout};
use casa_ms::SelectedObservationNumericGeometry;
use casa_ms::observation_io::{Msv2Observation, OpenOptions};
use casa_observation::{
    AccessRequest, AntennaId, BatchBuffers, CorrelationRequest, FieldSelection, Grouping,
    Observation, PartitionId, PointingDirection, PointingRequest, ReadEvent, Roles, TimeSelection,
    VisibilityBatch, WeightRequest,
};
use casa_types::measures::{MeasuresProvider, MjdHighPrec};

use crate::{ApplicationError, ImagingRequest, RunContext, compile_run, imaging};

/// One request's two geometries compared.
#[derive(Clone, Debug, Default)]
pub struct GeometryComparison {
    /// Rows today's route delivered.
    pub rows: usize,
    /// Of them, rows no batch sample matched.
    pub unmatched: usize,
    /// Per quantity, the largest difference relative to its scale and the
    /// physical row where it occurred.
    pub differences: BTreeMap<&'static str, (f64, u64)>,
    /// Per quantity, the values today's route gave and the batch geometry
    /// was compared on.
    pub compared: BTreeMap<&'static str, usize>,
}

impl GeometryComparison {
    /// The largest relative difference of any quantity.
    #[must_use]
    pub fn largest(&self) -> f64 {
        self.differences
            .values()
            .map(|(difference, _)| *difference)
            .fold(0.0, f64::max)
    }

    fn record(&mut self, quantity: &'static str, difference: f64, row: u64) {
        *self.compared.entry(quantity).or_default() += 1;
        let entry = self.differences.entry(quantity).or_insert((0.0, row));
        // A NaN difference is the largest.
        if difference.is_nan() || difference > entry.0 {
            *entry = (difference, row);
        }
    }
}

/// A row's identity in both routes: spectral window, antennas, and time in
/// milliseconds (today's route delivers its time as days).
type Key = (u32, u32, u32, i64);

fn key(spectral_window: u32, antennas: [u32; 2], time_s: f64) -> Key {
    (
        spectral_window,
        antennas[0],
        antennas[1],
        (time_s * 1.0e3).round() as i64,
    )
}

/// One row's geometry as either route gives it.
#[derive(Clone, Debug)]
struct Row {
    physical_row: u64,
    field: u32,
    raw_uvw_m: [f64; 3],
    projections: Vec<([f64; 3], f64)>,
    aw_pixels: Vec<Option<[f64; 2]>>,
    pointing: SelectedPointingDirections,
    parallactic: Option<[f64; 2]>,
    responses: Option<SelectedAntennaResponses>,
    /// Native channel index and output-frame frequency.
    frequencies: Vec<(u32, f64)>,
}

/// Compare the geometry of `request`'s rows on today's route with the
/// batch geometry of its MeasurementSet.
///
/// # Errors
///
/// When either route cannot be prepared or read.
pub fn compare_geometry(
    request: &ImagingRequest,
    context: &RunContext,
) -> Result<GeometryComparison, ApplicationError> {
    let Prepared {
        problem,
        path,
        measures,
        today,
    } = prepare(request, context)?;
    let observation = Msv2Observation::open_with(&path, &OpenOptions::default())?;
    let batches = batch_rows(
        &problem,
        &observation,
        |partition| Some(observation.spectral_window_id(partition)),
        measures,
        &today,
    )?;
    Ok(compare(&today, &batches))
}

/// Compare the batch geometry of `request`'s rows in its MeasurementSet
/// with their batch geometry in `converted`, the same observation in
/// another store (an MSv4 conversion): what the store alone changes. A
/// converted window is matched to the MeasurementSet's by its first
/// channel frequency.
///
/// # Errors
///
/// When either store cannot be opened or read.
pub fn compare_stores(
    request: &ImagingRequest,
    context: &RunContext,
    converted: &Path,
) -> Result<GeometryComparison, ApplicationError> {
    let Prepared {
        problem,
        path,
        measures,
        today,
    } = prepare(request, context)?;
    let observation = Msv2Observation::open_with(&path, &OpenOptions::default())?;
    let windows = observation
        .catalogue()
        .partition_ids()
        .into_iter()
        .map(|partition| {
            let entry = observation.catalogue().partition(partition);
            (
                entry.frequencies_hz.first().map(|hz| hz.to_bits()),
                observation.spectral_window_id(partition),
            )
        })
        .collect::<HashMap<_, _>>();
    let reference = batch_rows(
        &problem,
        &observation,
        |partition| Some(observation.spectral_window_id(partition)),
        Arc::clone(&measures),
        &today,
    )?;
    let other = casa_ms::observation_io::open(converted, &OpenOptions::default())?;
    let catalogue = Arc::clone(other.catalogue());
    let rows = batch_rows(
        &problem,
        other.as_ref(),
        |partition| {
            let first = catalogue.partition(partition).frequencies_hz.first();
            windows.get(&first.map(|hz| hz.to_bits())).copied()
        },
        measures,
        &today,
    )?;
    Ok(compare(&reference, &rows))
}

/// What both comparisons start from: the compiled problem of a request,
/// its MeasurementSet, the run's Measures provider and today's route's
/// rows.
struct Prepared {
    problem: CompiledProblem,
    path: std::path::PathBuf,
    measures: Arc<dyn MeasuresProvider>,
    today: HashMap<Key, Row>,
}

fn prepare(request: &ImagingRequest, context: &RunContext) -> Result<Prepared, ApplicationError> {
    let resolved = std::cell::Cell::new(None);
    let (problem, input, _native, _resident) = compile_run(request, context, &resolved)?;
    let measures = input.observation.measures_provider();
    let path = std::path::PathBuf::from(input.observation.locator());
    let Admitted { value: access, .. } = finalize_source_access(
        &problem,
        input.initial_access,
        imaging::row_samples(&problem)?,
        &context.host,
        &context.policy,
    )?;
    let today = today_rows(&problem, access)?;
    Ok(Prepared {
        problem,
        path,
        measures,
        today,
    })
}

/// `actual`'s rows against `expected`'s.
fn compare(expected: &HashMap<Key, Row>, actual: &HashMap<Key, Row>) -> GeometryComparison {
    let mut comparison = GeometryComparison {
        rows: expected.len(),
        ..GeometryComparison::default()
    };
    for (key, want) in expected {
        match actual.get(key) {
            Some(got) => compare_row(want, got, &mut comparison),
            None => comparison.unmatched += 1,
        }
    }
    comparison
}

/// Today's route's rows of `problem`, by key.
fn today_rows(
    problem: &CompiledProblem,
    access: casa_ms::ResolvedSelectedObservationAccess,
) -> Result<HashMap<Key, Row>, ApplicationError> {
    let selected = access.into_deferred().open(problem)?;
    let mut stream = selected.into_block_stream(problem);
    let mut block = stream.create_storage();
    let mut geometry = SelectedObservationNumericGeometry::new(
        stream.maximum_rows_per_block(),
        row_layout(problem).channels(),
    )?;
    let mut rows = HashMap::new();
    while let Some(filled) = stream.fill_next(&mut block)? {
        let projected = filled.project_numeric_geometry(problem, &mut geometry)?;
        for index in 0..projected.row_count() {
            let numeric = projected.numeric_row(index);
            let row = numeric.row;
            let channels = numeric.channels.len();
            let frequencies = &projected.frequencies_hz()[index * channels..(index + 1) * channels];
            let antennas = [row.metadata.antenna1, row.metadata.antenna2].map(|id| id as u32);
            let time_s = row.coordinates.time.mjd_days() * 86_400.0;
            rows.insert(
                key(row.spectral_window_id, antennas, time_s),
                Row {
                    physical_row: row.physical_row,
                    field: row.metadata.field_id as u32,
                    raw_uvw_m: row.coordinates.raw_uvw_m,
                    projections: row
                        .domain_projections()
                        .iter()
                        .map(|projection| {
                            let model = projection.model();
                            (model.transformed_uvw_m(), model.phase_shift_m())
                        })
                        .collect(),
                    aw_pixels: row
                        .domain_projections()
                        .iter()
                        .map(|projection| projection.aw_pointing_pixel())
                        .collect(),
                    pointing: row.coordinates.pointing_directions,
                    parallactic: row.coordinates.parallactic_angles_rad,
                    responses: row.metadata.antenna_responses,
                    frequencies: numeric
                        .channels
                        .iter()
                        .zip(frequencies)
                        .map(|(channel, frequency)| (channel.channel_index, *frequency))
                        .collect(),
                },
            );
        }
    }
    Ok(rows)
}

/// The batch geometry of the rows of `today` in `observation`, by key;
/// `window` gives a partition's spectral window number, `None` skipping it.
fn batch_rows(
    problem: &CompiledProblem,
    observation: &dyn Observation,
    window: impl Fn(PartitionId) -> Option<u32>,
    measures: Arc<dyn MeasuresProvider>,
    today: &HashMap<Key, Row>,
) -> Result<HashMap<Key, Row>, ApplicationError> {
    let catalogue = Arc::clone(observation.catalogue());
    let plan = GeometryPlan::compile(problem, &catalogue, measures)?;
    let mut traversal = GeometryTraversal::default();
    let mut geometry = BatchGeometry::default();
    let mut found = HashMap::new();
    let centroids = matches!(
        problem.geometry().centres().pointing(),
        PointingCentreLaw::Observation(law)
            if law.time_sampling() == casa_imaging_model::PointingTimeSampling::VisibilityTimeCentroid
    );
    for partition in catalogue.partition_ids() {
        let Some(spectral_window) = window(partition) else {
            continue;
        };
        let entry = catalogue.partition(partition);
        let request = AccessRequest {
            partitions: vec![partition],
            data_group: entry.data_groups[0].name.clone(),
            roles: Roles {
                values: false,
                weights: false,
                time_centroid: centroids,
                ..Roles::PAYLOAD
            },
            channels: 0..entry.frequencies_hz.len(),
            times: TimeSelection::All,
            fields: FieldSelection::All,
            weights: WeightRequest::Stored,
            correlations: CorrelationRequest::ALL,
            grouping: Grouping::None,
            batch_bytes: 1 << 26,
        };
        let mut reader = observation.bind_read(&request)?;
        let mut buffers = BatchBuffers::default();
        while let Some(event) = reader.next(&mut buffers)? {
            let ReadEvent::Batch(batch) = event else {
                continue;
            };
            let rows = batch_keys(&batch, spectral_window)
                .map(|key| today.contains_key(&key))
                .collect::<Vec<_>>();
            let pointing = plan
                .reads_pointing()
                .then(|| observation.pointing(&pointing_request(problem, &batch, partition)))
                .transpose()?;
            plan.apply(
                &batch,
                &rows,
                pointing.as_ref(),
                &mut traversal,
                &mut geometry,
            )?;
            for (sample, key) in batch_keys(&batch, spectral_window).enumerate() {
                if let Some(expected) = rows[sample].then(|| &today[&key]) {
                    found.insert(
                        key,
                        batch_row(&batch, &geometry, sample, plan.charts(), expected),
                    );
                }
            }
        }
    }
    Ok(found)
}

/// Each sample's key.
fn batch_keys<'a>(
    batch: &'a VisibilityBatch<'_>,
    spectral_window: u32,
) -> impl Iterator<Item = Key> + 'a {
    let origin = MjdHighPrec::new(0.0, 0.0);
    let per_time = batch.samples_per_time();
    (0..batch.samples()).map(move |sample| {
        let baseline = batch.baselines[sample % per_time];
        key(
            spectral_window,
            [baseline.antenna1.0, baseline.antenna2.0],
            batch.time.seconds_since(sample / per_time, origin),
        )
    })
}

/// The pointing request covering `batch`'s times for its partition's
/// antennas.
fn pointing_request(
    problem: &CompiledProblem,
    batch: &VisibilityBatch<'_>,
    partition: PartitionId,
) -> PointingRequest {
    let mut antennas: Vec<AntennaId> = batch
        .baselines
        .iter()
        .flat_map(|baseline| [baseline.antenna1, baseline.antenna2])
        .collect();
    antennas.sort_unstable();
    antennas.dedup();
    let times = batch.time.offsets_s();
    let mut span = times[0]..=times[times.len() - 1];
    if let Some(centroid) = batch.timing.centroid {
        let offsets = centroid.offsets_s();
        let low = offsets.iter().copied().fold(*span.start(), f64::min);
        let high = offsets.iter().copied().fold(*span.end(), f64::max);
        span = low..=high;
    }
    let direction = match problem.geometry().centres().pointing() {
        PointingCentreLaw::Observation(law)
            if law.direction_column() == PointingDirectionColumn::Target =>
        {
            PointingDirection::Target
        }
        _ => PointingDirection::Beam,
    };
    PointingRequest {
        partition,
        direction,
        antennas,
        times: span,
        budget_bytes: 1 << 34,
    }
}

/// Sample `sample`'s geometry as a row; `expected` gives the native
/// channel indices of today's row.
fn batch_row(
    batch: &VisibilityBatch<'_>,
    geometry: &BatchGeometry,
    sample: usize,
    charts: usize,
    expected: &Row,
) -> Row {
    let time = sample / batch.samples_per_time();
    let frequencies = geometry.frequencies_hz(time);
    Row {
        physical_row: expected.physical_row,
        field: batch.field[time].0,
        raw_uvw_m: batch
            .group
            .uvw
            .map_or([f64::NAN; 3], |uvw| uvw.metres_at(sample)),
        projections: (0..charts)
            .map(|chart| {
                let projection = geometry.projection(sample, chart);
                (projection.uvw_m, projection.phase_shift_m)
            })
            .collect(),
        aw_pixels: (0..charts)
            .map(|chart| geometry.aw_pointing_pixel(sample, chart))
            .collect(),
        pointing: geometry.pointing(sample),
        parallactic: geometry.parallactic_angles(sample),
        responses: geometry.antenna_responses(sample),
        frequencies: expected
            .frequencies
            .iter()
            .map(|(channel, _)| {
                let index = *channel as usize - batch.first_channel;
                (
                    *channel,
                    frequencies.get(index).copied().unwrap_or(f64::NAN),
                )
            })
            .collect(),
    }
}

/// Record the differences of one row.
fn compare_row(expected: &Row, actual: &Row, comparison: &mut GeometryComparison) {
    let row = expected.physical_row;
    let length = norm(expected.raw_uvw_m).max(1.0);
    let exact = |same: bool| if same { 0.0 } else { f64::INFINITY };
    comparison.record("field", exact(expected.field == actual.field), row);
    comparison.record(
        "raw uvw",
        norm(difference(expected.raw_uvw_m, actual.raw_uvw_m)) / length,
        row,
    );
    comparison.record(
        "charts",
        exact(expected.projections.len() == actual.projections.len()),
        row,
    );
    for (want, got) in expected.projections.iter().zip(&actual.projections) {
        comparison.record("uvw", norm(difference(want.0, got.0)) / length, row);
        comparison.record("phase shift", (want.1 - got.1).abs() / length, row);
    }
    for (want, got) in expected.aw_pixels.iter().zip(&actual.aw_pixels) {
        let difference = match (want, got) {
            (None, None) => continue,
            (Some(want), Some(got)) => {
                ((want[0] - got[0]).hypot(want[1] - got[1])) / want[0].hypot(want[1]).max(1.0)
            }
            _ => f64::INFINITY,
        };
        comparison.record("AW pointing pixel", difference, row);
    }
    let angle = |want: SkyDirection, got: SkyDirection| {
        exact(want.frame() == got.frame()).max(
            (want.longitude_rad() - got.longitude_rad())
                .abs()
                .max((want.latitude_rad() - got.latitude_rad()).abs()),
        )
    };
    comparison.record(
        "pointing",
        angle(expected.pointing.antenna1, actual.pointing.antenna1)
            .max(angle(expected.pointing.antenna2, actual.pointing.antenna2)),
        row,
    );
    match (expected.parallactic, actual.parallactic) {
        (None, None) => {}
        (Some(want), Some(got)) => comparison.record(
            "parallactic angle",
            (want[0] - got[0]).abs().max((want[1] - got[1]).abs()),
            row,
        ),
        _ => comparison.record("parallactic angle", f64::INFINITY, row),
    }
    if expected.responses.is_some() || actual.responses.is_some() {
        comparison.record(
            "aperture classes",
            exact(expected.responses == actual.responses),
            row,
        );
    }
    for ((_, want), (_, got)) in expected.frequencies.iter().zip(&actual.frequencies) {
        comparison.record("frequency", (want - got).abs() / want.abs(), row);
    }
}

fn difference(a: [f64; 3], b: [f64; 3]) -> [f64; 3] {
    [a[0] - b[0], a[1] - b[1], a[2] - b[2]]
}

fn norm(vector: [f64; 3]) -> f64 {
    vector.iter().map(|value| value * value).sum::<f64>().sqrt()
}
