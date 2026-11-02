// SPDX-License-Identifier: LGPL-3.0-or-later
//! The store law (#932, #942): the batch geometry ([`GeometryPlan`]) of an
//! observation's samples in its MeasurementSet equals their batch geometry
//! in another store of the same observation (an MSv4 conversion), sample
//! for sample, to [`TOLERANCE`]: what the store alone changes. Every sample
//! of every partition is compared, on every channel; the request gives the
//! geometry (phase centre, image domains, pointing), not the samples.
//!
//! A MeasurementSet sample is its MAIN row
//! ([`Msv2Observation::main_row`]), so padding matches nothing. A sample of
//! the other store is matched through its partition's content, its
//! antennas by name and its time to the millisecond, a key two of the
//! MeasurementSet's rows may share; such rows, and rows two samples claim,
//! are counted as duplicates and fail the law.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;

use casa_imaging_model::{
    BatchGeometry, CompiledProblem, GeometryPlan, GeometryTraversal, PointingCentreLaw,
    SelectedAntennaResponses, SelectedPointingDirections, SkyDirection,
};
use casa_ms::observation_io::{Msv2Observation, OpenOptions};
use casa_observation::{
    AccessRequest, AntennaEntry, AntennaId, BatchBuffers, Catalogue, CorrelationRequest,
    FieldEphemeris, FieldSelection, Grouping, Observation, PartitionId, PointingRequest, ReadEvent,
    Roles, TimeSelection, VisibilityBatch, WeightRequest,
};
use casa_types::measures::{MeasuresProvider, MjdHighPrec};

use crate::compile::OpenedStore;
use crate::native::OpenedObservation;
use crate::{ApplicationError, ImagingRequest, RunContext, compile_run};

/// The law's tolerance: every quantity relative to its scale.
pub const TOLERANCE: f64 = 1.0e-12;

/// One request's two geometries compared.
#[derive(Clone, Debug, Default)]
pub struct GeometryComparison {
    /// Rows the MeasurementSet's batches delivered.
    pub rows: usize,
    /// Of them, rows no sample of the other store matched.
    pub unmatched: usize,
    /// Rows two samples of the other store matched, and MeasurementSet
    /// rows that share another's key.
    pub duplicates: usize,
    /// Per quantity, the largest difference relative to its scale and the
    /// physical row where it occurred.
    pub differences: BTreeMap<&'static str, (f64, u64)>,
    /// Per quantity, the values compared.
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
/// time in milliseconds.
type Key = (u32, u32, u32, i64);

fn key(data_description: u32, antennas: [u32; 2], time_s: f64) -> Key {
    (
        data_description,
        antennas[0],
        antennas[1],
        (time_s * 1.0e3).round() as i64,
    )
}

/// One row's geometry as a store's batches give it.
#[derive(Clone, Debug)]
struct Row {
    physical_row: u64,
    /// The row's time, MJD days in the observation's time scale.
    time_mjd_days: f64,
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

/// Compare the batch geometry of every sample of `request`'s
/// MeasurementSet with its batch geometry in `converted`, the same
/// observation in another store (an MSv4 conversion). A converted partition
/// is matched to the data description of the MeasurementSet's partitions of
/// the same content
/// ([`PartitionEntry::same_content`](casa_observation::PartitionEntry::same_content)),
/// and a converted antenna to the MeasurementSet's of its name.
///
/// # Errors
///
/// When the request does not compile, or either store cannot be opened or
/// read.
pub fn compare_stores(
    request: &ImagingRequest,
    context: &RunContext,
    converted: &Path,
) -> Result<GeometryComparison, ApplicationError> {
    let resolved = std::cell::Cell::new(None);
    let (problem, input, _native, _resident) = compile_run(request, context, &resolved)?;
    let OpenedObservation {
        store,
        measures,
        phase_table,
    } = input.observation;
    let observation = measurement_set(&store.value)?;
    let inputs = PlanInputs {
        measures,
        phase_table,
    };
    let mut keys = HashMap::new();
    let mut shared = 0;
    let reference = batch_rows(
        &problem,
        observation,
        (
            &observation.catalogue().partition_ids(),
            &FieldSelection::All,
        ),
        |batch, sample| {
            let per_time = batch.samples_per_time();
            let baseline = batch.baselines[sample % per_time];
            let row = main_row(observation, batch, sample)?;
            let identity = key(
                observation.data_description_id(batch.partition),
                [baseline.antenna1.0, baseline.antenna2.0],
                batch
                    .time
                    .seconds_since(sample / per_time, MjdHighPrec::new(0.0, 0.0)),
            );
            if keys
                .insert(identity, row)
                .is_some_and(|earlier| earlier != row)
            {
                shared += 1;
            }
            Some(row)
        },
        inputs.clone(),
        None,
    )?;
    let other = casa_ms::observation_io::open(converted, &OpenOptions::default())?;
    let bridge = Bridge::new(observation, other.catalogue());
    let rows = batch_rows(
        &problem,
        other.as_ref(),
        (&bridge.partitions(), &FieldSelection::All),
        |batch, sample| bridge.row(batch, sample, &keys),
        inputs,
        Some(&reference.rows),
    )?;
    let mut comparison = compare(&reference.rows, &rows);
    comparison.duplicates += reference.duplicates + shared;
    Ok(comparison)
}

/// The time (MJD days) and first antenna's pointing direction of each MAIN
/// row `request` selects (its partitions, fields and scans, as the run
/// tests them; a UV-distance interval is not applied), from its batch
/// geometry: under a
/// pointing law that follows the phase-tracking centre, the phase centre
/// at the row's time. What CASA oracles of a moving phase centre compare
/// (T41's `TRACKFIELD`).
///
/// # Errors
///
/// When the request does not compile or the MeasurementSet cannot be read.
pub fn row_pointing(
    request: &ImagingRequest,
    context: &RunContext,
) -> Result<BTreeMap<u64, (f64, SkyDirection)>, ApplicationError> {
    let resolved = std::cell::Cell::new(None);
    let (problem, input, _native, _resident) = compile_run(request, context, &resolved)?;
    let OpenedObservation {
        store,
        measures,
        phase_table,
    } = input.observation;
    let observation = measurement_set(&store.value)?;
    let selection = crate::compile::resolve_partitions(&problem, observation)?;
    let partitions: Vec<PartitionId> = selection
        .partitions
        .iter()
        .map(|partition| partition.id)
        .collect();
    let found = batch_rows(
        &problem,
        observation,
        (&partitions, &selection.fields),
        |batch, sample| {
            let time = sample / batch.samples_per_time();
            if !selection.scan_holds(batch, time).ok()? {
                return None;
            }
            main_row(observation, batch, sample)
        },
        PlanInputs {
            measures,
            phase_table,
        },
        None,
    )?;
    Ok(found
        .rows
        .into_iter()
        .map(|(row, geometry)| (row, (geometry.time_mjd_days, geometry.pointing.antenna1)))
        .collect())
}

/// The MeasurementSet `store` holds: the law's reference, whose samples are
/// MAIN rows.
fn measurement_set(store: &OpenedStore) -> Result<&Msv2Observation, ApplicationError> {
    store
        .msv2()
        .ok_or_else(|| "the store law's reference must be a MeasurementSet".into())
}

/// The MAIN row of `sample` of `batch`; `None` for padding.
fn main_row(
    observation: &Msv2Observation,
    batch: &VisibilityBatch<'_>,
    sample: usize,
) -> Option<u64> {
    let per_time = batch.samples_per_time();
    let baseline = batch.baselines[sample % per_time];
    let time_s = batch.time.offsets_s()[sample / per_time];
    observation
        .main_row(batch.partition, time_s, baseline)
        .map(u64::from)
}

/// What compiles a store's geometry beside its catalogue: the run's
/// Measures provider and the ephemeris table a moving phase centre names.
#[derive(Clone)]
struct PlanInputs {
    measures: Arc<dyn MeasuresProvider>,
    phase_table: Option<Arc<FieldEphemeris>>,
}

/// The rows a store's batch samples matched.
struct Found {
    rows: Rows,
    /// Samples that matched a row an earlier sample had.
    duplicates: usize,
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

/// The batch geometry of the rows of `partitions` of `observation` of
/// `fields`; `row_of` gives a sample's row, `None` for padding or a sample
/// no row matches. With `expected`, only its rows, on its channels.
fn batch_rows(
    problem: &CompiledProblem,
    observation: &dyn Observation,
    (partitions, fields): (&[PartitionId], &FieldSelection),
    mut row_of: impl FnMut(&VisibilityBatch<'_>, usize) -> Option<u64>,
    inputs: PlanInputs,
    expected: Option<&Rows>,
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
                // Each time's scan, for a law that tests the run's scan
                // selection ([`row_pointing`]).
                scan: true,
                ..Roles::PAYLOAD
            },
            channels: 0..entry.frequencies_hz.len(),
            times: TimeSelection::All,
            fields: fields.clone(),
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
                .map(|sample| {
                    row_of(&batch, sample)
                        .filter(|row| expected.is_none_or(|rows| rows.contains_key(row)))
                })
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
                let got = batch_row(
                    &batch,
                    &geometry,
                    (sample, *row),
                    plan.charts(),
                    expected.map(|rows| &rows[row]),
                );
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

/// Sample `sample`'s geometry as MAIN row `row`, on every channel the
/// batch holds or, with `expected`, on its channels.
fn batch_row(
    batch: &VisibilityBatch<'_>,
    geometry: &BatchGeometry,
    (sample, row): (usize, u64),
    charts: usize,
    expected: Option<&Row>,
) -> Row {
    let time = sample / batch.samples_per_time();
    let frequencies = geometry.frequencies_hz(time);
    Row {
        physical_row: row,
        time_mjd_days: batch.time.seconds_since(time, MjdHighPrec::new(0.0, 0.0)) / 86_400.0,
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
        frequencies: match expected {
            Some(expected) => expected
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
            None => frequencies
                .iter()
                .enumerate()
                .map(|(index, frequency)| ((batch.first_channel + index) as u32, *frequency))
                .collect(),
        },
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
