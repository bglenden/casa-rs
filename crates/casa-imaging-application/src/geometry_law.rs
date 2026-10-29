// SPDX-License-Identifier: LGPL-3.0-or-later
//! Imaging port step 3b's equality law (#932): the row geometry today's
//! selected-observation route gives one imaging request, beside the batch
//! geometry ([`GeometryPlan`]) of the same MeasurementSet read through the
//! observation boundary, compared sample for sample. Evidence for the port
//! only: it goes with today's route at the cutover.
//!
//! Rows are matched by MAIN row number: a MeasurementSet batch sample's
//! through its grid ([`Msv2Observation::main_row`]), so padding matches
//! nothing. A sample of another store is matched through its data
//! description, its antennas by name and its time to the millisecond, a
//! key two of the MeasurementSet's rows may share; such rows, and rows two
//! samples claim, are counted as duplicates and fail the law.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;

use casa_imaging_model::{
    BatchGeometry, CompiledProblem, GeometryPlan, GeometryTraversal, PointingCentreLaw,
    SelectedAntennaResponses, SelectedPointingDirections, SkyDirection,
};
use casa_imaging_runtime::{Admitted, finalize_source_access, row_layout};
use casa_ms::SelectedObservationNumericGeometry;
use casa_ms::observation_io::{Msv2Observation, OpenOptions};
use casa_observation::{
    AccessRequest, AntennaEntry, AntennaId, BatchBuffers, Catalogue, CorrelationRequest,
    FieldEphemeris, FieldSelection, Grouping, Observation, PartitionId, PointingRequest, ReadEvent,
    Roles, TimeSelection, VisibilityBatch, WeightRequest,
};
use casa_types::measures::{MeasuresProvider, MjdHighPrec};

use crate::{ApplicationError, ImagingRequest, RunContext, compile_run, imaging};

/// The law's tolerance: every quantity relative to its scale.
pub const TOLERANCE: f64 = 1.0e-12;

/// One request's two geometries compared.
#[derive(Clone, Debug, Default)]
pub struct GeometryComparison {
    /// Rows today's route delivered.
    pub rows: usize,
    /// Of them, rows no batch sample matched.
    pub unmatched: usize,
    /// Rows today's route delivered twice, rows two batch samples matched,
    /// and MeasurementSet rows that share another store's key.
    pub duplicates: usize,
    /// Per quantity, the largest difference relative to its scale and the
    /// physical row where it occurred.
    pub differences: BTreeMap<&'static str, (f64, u64)>,
    /// Per quantity, the values today's route gave and the batch geometry
    /// was compared on.
    pub compared: BTreeMap<&'static str, usize>,
}

impl GeometryComparison {
    /// Whether the law holds: there were rows, every one matched exactly
    /// once, and no quantity differs by more than [`TOLERANCE`].
    #[must_use]
    pub fn holds(&self) -> bool {
        self.rows > 0 && self.unmatched == 0 && self.duplicates == 0 && self.largest() <= TOLERANCE
    }

    /// The largest relative difference of any quantity.
    #[must_use]
    pub fn largest(&self) -> f64 {
        self.differences
            .values()
            .map(|(difference, _)| *difference)
            .fold(0.0, worst)
    }

    /// Record one difference; a NaN one (either side absent or not finite)
    /// counts as infinite.
    fn record(&mut self, quantity: &'static str, difference: f64, row: u64) {
        *self.compared.entry(quantity).or_default() += 1;
        let difference = worst(difference, 0.0);
        let entry = self.differences.entry(quantity).or_insert((0.0, row));
        if difference > entry.0 {
            *entry = (difference, row);
        }
    }
}

/// The larger of two differences, a NaN infinite: `f64::max` would drop it.
fn worst(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() {
        f64::INFINITY
    } else {
        a.max(b)
    }
}

/// Rows by MAIN row number.
type Rows = HashMap<u64, Row>;

/// A row's identity in another store: data description, antennas, and
/// time in milliseconds (today's route delivers its time as days).
type Key = (u32, u32, u32, i64);

fn key(data_description: u32, antennas: [u32; 2], time_s: f64) -> Key {
    (
        data_description,
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
        inputs,
        today,
    } = prepare(request, context)?;
    let observation = Msv2Observation::open_with(&path, &OpenOptions::default())?;
    let batches = msv2_rows(&problem, &observation, inputs, &today.rows)?;
    let mut comparison = compare(&today.rows, &batches);
    comparison.duplicates += today.repeated;
    Ok(comparison)
}

/// Compare the batch geometry of `request`'s rows in its MeasurementSet
/// with their batch geometry in `converted`, the same observation in
/// another store (an MSv4 conversion): what the store alone changes. A
/// converted partition is matched to the data description of the
/// MeasurementSet's partitions of the same content
/// ([`PartitionEntry::same_content`](casa_observation::PartitionEntry::same_content)),
/// and a converted antenna to the MeasurementSet's of its name.
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
        inputs,
        today,
    } = prepare(request, context)?;
    let observation = Msv2Observation::open_with(&path, &OpenOptions::default())?;
    let reference = msv2_rows(&problem, &observation, inputs.clone(), &today.rows)?;
    let other = casa_ms::observation_io::open(converted, &OpenOptions::default())?;
    let bridge = Bridge::new(&observation, other.catalogue());
    let rows = batch_rows(
        &problem,
        other.as_ref(),
        &bridge.partitions(),
        |batch, sample| bridge.row(batch, sample, &today.keys),
        inputs,
        &reference.rows,
    )?;
    let mut comparison = compare(&reference.rows, &rows);
    comparison.rows = today.rows.len();
    comparison.unmatched += today.rows.len() - reference.rows.len();
    comparison.duplicates += today.repeated + reference.duplicates + today.shared;
    Ok(comparison)
}

/// What both comparisons start from: the compiled problem of a request,
/// its MeasurementSet, what compiles a store's geometry and today's
/// route's rows.
struct Prepared {
    problem: CompiledProblem,
    path: std::path::PathBuf,
    inputs: PlanInputs,
    today: Today,
}

/// What compiles a store's geometry beside its catalogue: the run's
/// Measures provider and the ephemeris table a moving phase centre names.
#[derive(Clone)]
struct PlanInputs {
    measures: Arc<dyn MeasuresProvider>,
    phase_table: Option<Arc<FieldEphemeris>>,
}

/// Today's route's rows.
struct Today {
    rows: Rows,
    /// Each row's [`Key`].
    keys: HashMap<Key, u64>,
    /// Rows delivered a second time.
    repeated: usize,
    /// Rows whose key another, earlier row has.
    shared: usize,
}

/// The rows a store's batch samples matched.
struct Found {
    rows: Rows,
    /// Samples that matched a row an earlier sample had.
    duplicates: usize,
}

fn prepare(request: &ImagingRequest, context: &RunContext) -> Result<Prepared, ApplicationError> {
    let resolved = std::cell::Cell::new(None);
    let (problem, input, _native, _resident) = compile_run(request, context, &resolved)?;
    let inputs = PlanInputs {
        measures: input.observation.measures_provider(),
        phase_table: input
            .observation
            .ephemeris()
            .and_then(casa_ms::SelectedObservationEphemeris::external_series)
            .cloned(),
    };
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
        inputs,
        today,
    })
}

/// `actual`'s rows against `expected`'s.
fn compare(expected: &Rows, actual: &Found) -> GeometryComparison {
    let mut comparison = GeometryComparison {
        rows: expected.len(),
        duplicates: actual.duplicates,
        ..GeometryComparison::default()
    };
    for (row, want) in expected {
        match actual.rows.get(row) {
            Some(got) => compare_row(want, got, &mut comparison),
            None => comparison.unmatched += 1,
        }
    }
    comparison
}

/// Today's route's rows of `problem`.
fn today_rows(
    problem: &CompiledProblem,
    access: casa_ms::ResolvedSelectedObservationAccess,
) -> Result<Today, ApplicationError> {
    let selected = access.into_deferred().open(problem)?;
    let mut stream = selected.into_block_stream(problem);
    let mut block = stream.create_storage();
    let mut geometry = SelectedObservationNumericGeometry::new(
        stream.maximum_rows_per_block(),
        row_layout(problem).channels(),
    )?;
    let mut today = Today {
        rows: HashMap::new(),
        keys: HashMap::new(),
        repeated: 0,
        shared: 0,
    };
    while let Some(filled) = stream.fill_next(&mut block)? {
        let projected = filled.project_numeric_geometry(problem, &mut geometry)?;
        for index in 0..projected.row_count() {
            let numeric = projected.numeric_row(index);
            let row = numeric.row;
            let channels = numeric.channels.len();
            let frequencies = &projected.frequencies_hz()[index * channels..(index + 1) * channels];
            let antennas = [row.metadata.antenna1, row.metadata.antenna2].map(|id| id as u32);
            let time_s = row.coordinates.time.mjd_days() * 86_400.0;
            let identity = key(row.data_description_id as u32, antennas, time_s);
            let earlier = today.keys.insert(identity, row.physical_row);
            if earlier.is_some_and(|earlier| earlier != row.physical_row) {
                today.shared += 1;
            }
            let repeated = today.rows.insert(
                row.physical_row,
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
            if repeated.is_some() {
                today.repeated += 1;
            }
        }
    }
    Ok(today)
}

/// The batch geometry of `expected`'s rows in a MeasurementSet, each
/// sample matched to its MAIN row through the partition's grid.
fn msv2_rows(
    problem: &CompiledProblem,
    observation: &Msv2Observation,
    inputs: PlanInputs,
    expected: &Rows,
) -> Result<Found, ApplicationError> {
    batch_rows(
        problem,
        observation,
        &observation.catalogue().partition_ids(),
        |batch, sample| {
            let per_time = batch.samples_per_time();
            observation
                .main_row(
                    batch.partition,
                    batch.time.offsets_s()[sample / per_time],
                    batch.baselines[sample % per_time],
                )
                .map(u64::from)
        },
        inputs,
        expected,
    )
}

/// The batch geometry of `expected`'s rows in `partitions` of
/// `observation`; `row_of` gives a sample's row, `None` for one that is
/// no row of `expected`'s.
fn batch_rows(
    problem: &CompiledProblem,
    observation: &dyn Observation,
    partitions: &[PartitionId],
    row_of: impl Fn(&VisibilityBatch<'_>, usize) -> Option<u64>,
    inputs: PlanInputs,
    expected: &Rows,
) -> Result<Found, ApplicationError> {
    let catalogue = Arc::clone(observation.catalogue());
    let plan = GeometryPlan::compile(problem, &catalogue, inputs.measures, inputs.phase_table)?;
    let mut traversal = GeometryTraversal::default();
    let mut geometry = BatchGeometry::default();
    let mut found = Found {
        rows: HashMap::new(),
        duplicates: 0,
    };
    let centroids = matches!(
        problem.geometry().centres().pointing(),
        PointingCentreLaw::Observation(law)
            if law.time_sampling() == casa_imaging_model::PointingTimeSampling::VisibilityTimeCentroid
    );
    for &partition in partitions {
        let entry = catalogue.partition(partition);
        let data_group = entry
            .data_groups
            .first()
            .ok_or_else(|| format!("partition {} has no data group", partition.0))?;
        let request = AccessRequest {
            partitions: vec![partition],
            data_group: data_group.name.clone(),
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
            if batch.samples() == 0 {
                continue;
            }
            let samples = (0..batch.samples())
                .map(|sample| row_of(&batch, sample).filter(|row| expected.contains_key(row)))
                .collect::<Vec<_>>();
            let rows = samples.iter().map(Option::is_some).collect::<Vec<_>>();
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
            for (sample, row) in samples.iter().enumerate() {
                let Some(row) = row else {
                    continue;
                };
                let got = batch_row(&batch, &geometry, sample, plan.charts(), &expected[row]);
                if found.rows.insert(*row, got).is_some() {
                    found.duplicates += 1;
                }
            }
        }
    }
    Ok(found)
}

/// How another store's samples find the MeasurementSet's rows.
struct Bridge {
    /// Each matched converted partition's data description.
    data_descriptions: BTreeMap<PartitionId, u32>,
    /// Each converted antenna's MeasurementSet antenna.
    antennas: Vec<Option<u32>>,
}

impl Bridge {
    fn new(observation: &Msv2Observation, converted: &Catalogue) -> Self {
        let reference = observation.catalogue();
        let data_descriptions = converted
            .partition_ids()
            .into_iter()
            .filter_map(|partition| {
                let entry = converted.partition(partition);
                let mut matching = reference
                    .partition_ids()
                    .into_iter()
                    .filter(|candidate| reference.partition(*candidate).same_content(entry))
                    .map(|candidate| observation.data_description_id(candidate));
                let first = matching.next()?;
                matching
                    .all(|other| other == first)
                    .then_some((partition, first))
            })
            .collect();
        let antennas = converted
            .antennas
            .iter()
            .map(|antenna| {
                let mut matching = reference
                    .antennas
                    .iter()
                    .enumerate()
                    .filter(|(_, candidate)| same_antenna(candidate, antenna))
                    .map(|(index, _)| index as u32);
                let first = matching.next()?;
                matching.next().is_none().then_some(first)
            })
            .collect();
        Self {
            data_descriptions,
            antennas,
        }
    }

    /// The converted partitions matched to a data description.
    fn partitions(&self) -> Vec<PartitionId> {
        self.data_descriptions.keys().copied().collect()
    }

    /// The MeasurementSet row `keys` gives `sample` of `batch`.
    fn row(
        &self,
        batch: &VisibilityBatch<'_>,
        sample: usize,
        keys: &HashMap<Key, u64>,
    ) -> Option<u64> {
        let data_description = *self.data_descriptions.get(&batch.partition)?;
        let per_time = batch.samples_per_time();
        let baseline = batch.baselines[sample % per_time];
        let antenna = |id: AntennaId| self.antennas.get(id.0 as usize).copied().flatten();
        let time_s = batch
            .time
            .seconds_since(sample / per_time, MjdHighPrec::new(0.0, 0.0));
        let antennas = [antenna(baseline.antenna1)?, antenna(baseline.antenna2)?];
        keys.get(&key(data_description, antennas, time_s)).copied()
    }
}

/// Whether `converted` names `reference`: as stored, or as XRADIO names an
/// MSv4 antenna (`NAME_STATION`).
fn same_antenna(reference: &AntennaEntry, converted: &AntennaEntry) -> bool {
    converted.name == reference.name
        || converted.name == format!("{}_{}", reference.name, reference.station)
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
    let direction = crate::imaging::pointing_direction(problem);
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
        worst(
            exact(want.frame() == got.frame()),
            worst(
                (want.longitude_rad() - got.longitude_rad()).abs(),
                (want.latitude_rad() - got.latitude_rad()).abs(),
            ),
        )
    };
    comparison.record(
        "pointing",
        worst(
            angle(expected.pointing.antenna1, actual.pointing.antenna1),
            angle(expected.pointing.antenna2, actual.pointing.antenna2),
        ),
        row,
    );
    match (expected.parallactic, actual.parallactic) {
        (None, None) => {}
        (Some(want), Some(got)) => comparison.record(
            "parallactic angle",
            worst((want[0] - got[0]).abs(), (want[1] - got[1]).abs()),
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
