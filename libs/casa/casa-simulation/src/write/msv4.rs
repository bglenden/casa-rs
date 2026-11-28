// SPDX-License-Identifier: LGPL-3.0-or-later
//! A simulated acquisition goes from the same geometry, prediction and
//! noise pipeline to a processing-set dataset, in whole integration batches.

use std::collections::{BTreeSet, HashSet};
use std::time::SystemTime;
use std::{slice, sync::Arc};

use casa_core::measures::direction::DirectionRef;
use casa_core::measures::{EpochRef, MjdHighPrec};
use casa_imaging_runtime::{Demand, Reservation, admit, free_memory};
use casa_ms::observation_io::creation::{
    Loss, Msv2CreationPartition, catalogue_for_creation, losses,
};
use casa_ms::observation_io::{
    Msv4Codec, Msv4ProcessingSetWriter, Msv4StorageOptions, Msv4WriteScratch, Msv4Writer,
};
use casa_observation::{
    AntennaId, AntennaPointing, BatchBuffers, BatchHeader, Catalogue, CorrelationAxis, FieldId,
    ObservationId, OrderedBaseline, PartitionId, PointingSeries, Roles, ScanEntry, ScanId, Sharing,
    TimeBaselineGrid, WeightRule,
};

use super::*;
use crate::session::SessionResources;

const UNIX_EPOCH_MJD: f64 = 40_587.0;
const UNIX_EPOCH_S: f64 = UNIX_EPOCH_MJD * 86_400.0;

/// A scan's row range in the global simulation, and its independently
/// writable time × baseline grid. A mosaic can revisit the same scan ID.
struct DatasetPlan {
    scan_index: usize,
    first_row: usize,
    grid: TimeBaselineGrid,
}

pub(crate) fn write_processing_set(session: &Simulator) -> SimulatorResult<Vec<Loss>> {
    if session.data_plan().predicts_single_dish() {
        return Err(unsupported(
            "single-dish spectra require the XRADIO spectrum layout",
        ));
    }
    let resources = &session.options().resources;
    let _metadata = admit_metadata(session, resources)?;
    let planned = PlannedScan::of_all(session.scans())
        .ok_or_else(|| unsupported("creation requires planned scans"))?;
    let plans = dataset_plans(&planned)?;
    let catalogue = describe(session, &planned, &plans)?;
    let set = Msv4ProcessingSetWriter::create(session.path())
        .map_err(creation_error("prepare the processing set"))?;
    let stem = session
        .path()
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("simulation");
    let stem = stem.strip_suffix(".ps.zarr").unwrap_or(stem);
    let digits = (plans.len() - 1).to_string().len();
    let date = casa_core::quanta::time::utc_now_iso8601();
    let mut report = known_losses(session, &catalogue, &plans);
    let mut flagged = false;
    for (index, plan) in plans.iter().enumerate() {
        let id = PartitionId(index as u32);
        let scan = planned[plan.scan_index];
        let dataset_name = format!("{stem}_{index:0digits$}");
        let options = (dataset_name, date.as_str());
        flagged |= write_dataset(
            session,
            (&planned, scan),
            plan,
            (&set, &catalogue, id),
            options,
        )?;
    }
    set.finish()
        .map_err(creation_error("publish the processing set"))?;
    if flagged {
        report.push(Loss::RowFlags);
    }
    Ok(report)
}

fn unsupported(reason: &str) -> SimulatorError {
    SimulatorError::not_yet_implemented(format!("simulator outformat='msv4': {reason} (#1055)"))
}

fn msv4_error(context: &str) -> impl Fn(casa_observation::ObservationError) -> SimulatorError + '_ {
    move |error| match error {
        casa_observation::ObservationError::Unsupported { what } => unsupported(what),
        _ => creation_error(context)(error),
    }
}

fn admit_metadata(
    session: &Simulator,
    resources: &SessionResources,
) -> SimulatorResult<Reservation> {
    let integrations = session.scans().iter().filter_map(|scan| scan.planned());
    let bytes = integrations.fold(0_u64, |total, scan| {
        // The grid, writer's checked copy and the coordinate vectors. The
        // catalogue also owns an independent copy of the baseline axis.
        total
            .saturating_add((scan.integration_count as u64).saturating_mul(64))
            .saturating_add((scan.baseline_count() as u64).saturating_mul(48))
            .saturating_add((scan.antenna_count as u64).saturating_mul(512))
    });
    let catalogue_channels = session
        .spectral_windows()
        .iter()
        .fold(0_u64, |total, window| {
            // The descriptor holds CHAN_FREQ, CHAN_WIDTH, EFFECTIVE_BW and
            // RESOLUTION, with scratch while building them.
            total.saturating_add((window.channel_count as u64).saturating_mul(64))
        });
    let axes = session
        .scans()
        .iter()
        .fold(catalogue_channels, |total, scan| {
            let channels = session.spectral_windows()[scan.spectral_window_id].channel_count as u64;
            total
                .saturating_add(channels.saturating_mul(96))
                .saturating_add(4096)
        });
    let metadata_rows = session.antennas().len()
        + session.fields().len()
        + session.feeds().len()
        + session.observations().len()
        + session.states().len()
        + session.data_descriptions().len()
        + session.spectral_windows().len();
    let descriptor = (metadata_rows as u64).saturating_mul(2048);
    admit(
        resources.host(),
        resources.policy(),
        &Demand {
            phase: "the simulator's MSv4 catalogue and grid coordinates",
            memory: bytes
                .saturating_add(axes)
                .saturating_add(descriptor)
                .saturating_add(256 << 10),
        },
    )
    .map_err(SimulatorError::admission)
}

fn dataset_plans(planned: &[PlannedScan<'_>]) -> SimulatorResult<Vec<DatasetPlan>> {
    let mut offset = 0_usize;
    let mut result = Vec::new();
    for (scan_index, planned) in planned.iter().enumerate() {
        let rows = planned.rows;
        let count = rows
            .baseline_count()
            .checked_mul(rows.integration_count)
            .ok_or_else(|| unsupported("the dataset row count exceeds the addressable size"))?;
        if count == 0 {
            continue;
        }
        let with_autocorrelations = rows.autocorrelation_weight > 0.0;
        let baselines = (0..rows.antenna_count)
            .flat_map(|a| {
                let first = a + usize::from(!with_autocorrelations);
                (first..rows.antenna_count).map(move |b| OrderedBaseline {
                    antenna1: AntennaId(a as u32),
                    antenna2: AntennaId(b as u32),
                })
            })
            .collect();
        let scan = u32::try_from(planned.scan.scan_number.saturating_sub(1))
            .map_err(|_| unsupported("a scan number must be positive"))?;
        result.push(DatasetPlan {
            scan_index,
            first_row: offset,
            grid: TimeBaselineGrid {
                baselines,
                time_scale: EpochRef::UTC,
                time_origin: MjdHighPrec::new(UNIX_EPOCH_MJD, 0.0),
                times_s: rows
                    .integration_times_s()
                    .map(|time| time - UNIX_EPOCH_S)
                    .collect(),
                field_per_time: vec![FieldId(planned.scan.field_id as u32); rows.integration_count],
                scan_per_time: vec![ScanId(scan); rows.integration_count],
                interval_s: rows.integration_s,
            },
        });
        offset = offset
            .checked_add(count)
            .ok_or_else(|| unsupported("the observation row count exceeds the addressable size"))?;
    }
    if result.is_empty() {
        return Err(unsupported(
            "an XRADIO processing set needs a populated dataset",
        ));
    }
    Ok(result)
}

fn describe(
    session: &Simulator,
    planned: &[PlannedScan<'_>],
    plans: &[DatasetPlan],
) -> SimulatorResult<Arc<Catalogue>> {
    let mut descriptor = MeasurementSet::create_memory(ms_builder()).map_err(|error| {
        SimulatorError::raised(format!("describe the simulated observation: {error}"))
    })?;
    subtables::write_subtables(&mut descriptor, session)?;
    subtables::populate_descriptions(&mut descriptor, session)?;
    let scans = scan_entries(session, planned);
    let specs = plans
        .iter()
        .map(|plan| {
            let scan = planned[plan.scan_index];
            Msv2CreationPartition {
                observation: ObservationId(scan.rows.observation_id as u32),
                data_description: scan.scan.data_description_id,
                feeds: [scan.rows.feed_id as u32; 2],
                grid: &plan.grid,
            }
        })
        .collect::<Vec<_>>();
    let mut catalogue = catalogue_for_creation(&descriptor, scans, &specs)
        .map_err(msv4_error("describe the simulated observation"))?;
    // Distinguish equal-project observing sessions and keep this identity
    // when the output processing set is moved or copied.
    let created = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos());
    for (ordinal, observation) in catalogue.observations.iter_mut().enumerate() {
        observation.session_reference = Some(format!("urn:casa-rs:simulator:{created}:{ordinal}"));
    }
    for (part, plan) in catalogue.partitions.iter_mut().zip(plans) {
        let scan = planned[plan.scan_index];
        part.intents = state_intents(session, scan).map(str::to_owned).collect();
    }
    let ids = catalogue
        .partitions
        .iter()
        .flat_map(|part| {
            part.baselines
                .iter()
                .flat_map(|pair| [pair.antenna1, pair.antenna2])
        })
        .collect::<BTreeSet<_>>();
    let mut names = HashSet::new();
    if ids
        .iter()
        .any(|id| !names.insert(&catalogue.antennas[id.0 as usize].name))
    {
        return Err(unsupported("XRADIO requires distinct antenna names"));
    }
    Ok(Arc::new(catalogue))
}

fn scan_entries(session: &Simulator, planned: &[PlannedScan<'_>]) -> Vec<ScanEntry> {
    let count = planned
        .iter()
        .map(|scan| scan.scan.scan_number)
        .max()
        .unwrap_or(0);
    let mut scans = (1..=count)
        .map(|number| ScanEntry {
            name: number.to_string(),
            number: Some(number),
            intents: Vec::new(),
        })
        .collect::<Vec<_>>();
    for scan in planned {
        let entry = &mut scans[(scan.scan.scan_number - 1) as usize];
        for intent in state_intents(session, *scan) {
            if !entry.intents.iter().any(|known| known == intent) {
                entry.intents.push(intent.to_owned());
            }
        }
    }
    scans
}

fn state_intents<'a>(
    session: &'a Simulator,
    scan: PlannedScan<'_>,
) -> impl Iterator<Item = &'a str> {
    session.states()[scan.rows.state_id]
        .obs_mode
        .split(',')
        .map(str::trim)
        .filter(|intent| !intent.is_empty())
}

fn known_losses(session: &Simulator, catalogue: &Catalogue, plans: &[DatasetPlan]) -> Vec<Loss> {
    let mut report = vec![Loss::Sigma];
    report.extend(losses::unused_antennas(catalogue));
    // These columns exist on the simulator's native product. XRADIO
    // carries the scan intents and the actual beam direction.
    for name in [
        "OBSERVATION.TIME_RANGE",
        "SOURCE.TIME",
        "SPECTRAL_WINDOW.FREQ_GROUP_NAME",
        "POINTING.TARGET",
        "POINTING.INTERVAL",
        "POINTING.TIME_ORIGIN",
    ] {
        report.push(Loss::Column(name.into()));
    }
    if session.states().iter().any(|state| {
        state.sig || state.reference || state.cal != 0. || state.load != 0. || state.sub_scan != 0
    }) {
        report.push(Loss::Column("STATE.SIG/REF/CAL/LOAD/SUB_SCAN".into()));
    }
    if session.fields().iter().any(|field| !field.code.is_empty()) {
        report.push(Loss::Column("FIELD.CODE and SOURCE.CODE".into()));
    }
    if plans
        .iter()
        .any(|plan| session.scans()[plan.scan_index].array_id != 0)
    {
        report.push(Loss::Column("MAIN.ARRAY_ID".into()));
    }
    report
}

fn write_dataset<'s>(
    session: &'s Simulator,
    (planned, scan): (&[PlannedScan<'s>], PlannedScan<'s>),
    plan: &DatasetPlan,
    (set, catalogue, id): (&Msv4ProcessingSetWriter, &Arc<Catalogue>, PartitionId),
    (dataset_name, creation_date): (String, &str),
) -> SimulatorResult<bool> {
    let resources = &session.options().resources;
    let entry = catalogue.partition(id);
    let cell = (entry.correlations.len(), entry.frequencies_hz.len());
    let baselines = plan.grid.baselines.len();
    let shape = main_shape(session, slice::from_ref(&scan), cell);
    let scope = plan.scan_index..plan.scan_index + 1;
    let prediction = DataStage::for_scans(
        session,
        cell,
        resources.prediction_backend(),
        scope.clone(),
        false,
    )?;
    let (times, bytes, writer_memory) =
        reserve_payload(resources, shape, baselines, plan.grid.times_s.len())?;
    let rows = times * baselines;
    let admission =
        resources::admit_pipeline(resources, &shape, prediction.as_ref(), rows, writer_memory)?;
    let options = Msv4StorageOptions {
        dataset_name,
        creator: "casa-rs".into(),
        creation_date: creation_date.into(),
        chunks: [times as u64, baselines as u64, cell.1 as u64, cell.0 as u64],
        codec: Msv4Codec::BloscLz4,
    };
    let destination = set
        .dataset(catalogue, id, &["base"], &plan.grid, &options)
        .map_err(msv4_error("create the simulated MSv4 dataset"))?;
    let mut writer = DatasetWriter {
        destination,
        plan,
        catalogue,
        partition: id,
        time_index: 0,
        columns: None,
        buffers: BatchBuffers::with_envelope(bytes),
        scratch: Msv4WriteScratch::default(),
        flagged: false,
    };
    let range = plan.first_row..plan.first_row + scan.rows.row_count();
    run_main(
        session,
        planned,
        &shape,
        (&admission, prediction),
        &mut writer,
        scope,
        range,
    )?;
    // Prediction and its batch buffers leave before building pointing.
    let (destination, flagged) = writer.into_destination();
    drop(admission);
    write_pointing(&destination, scan, &plan.grid, catalogue, resources)?;
    destination
        .finish()
        .map_err(creation_error("finish the simulated dataset"))?;
    Ok(flagged)
}

fn reserve_payload(
    resources: &SessionResources,
    shape: MainShape<'_>,
    baselines: usize,
    times: usize,
) -> SimulatorResult<(usize, usize, Reservation)> {
    let (host, policy) = (resources.host(), resources.policy());
    let free = free_memory(host, policy);
    let cells = shape.channels.saturating_mul(shape.correlations);
    // The batch arrays, expanded spectral weights, timing arrays and
    // codec input/output for the largest contiguous array. The pipeline
    // separately admits the column-shaped producer and prediction.
    let row_bytes = cells
        .saturating_mul(8 + 1 + 4 + 2 * 8)
        .saturating_add(shape.correlations.saturating_mul(4))
        .saturating_add(3 * 8 + 8 + 2 * 8 + 32);
    let times_bytes = row_bytes.saturating_mul(baselines).saturating_add(32);
    let cap = (free / 2).min(casa_ms::TARGET_BATCH_BYTES as u64) as usize;
    let chunk_times = (cap / times_bytes).clamp(1, times);
    let bytes = times_bytes.saturating_mul(chunk_times);
    let reservation = admit(
        host,
        policy,
        &Demand {
            phase: "the simulator's MSv4 visibility chunk",
            memory: bytes as u64,
        },
    )
    .map_err(SimulatorError::admission)?;
    Ok((chunk_times, bytes, reservation))
}

struct DatasetWriter<'a> {
    destination: Msv4Writer,
    plan: &'a DatasetPlan,
    catalogue: &'a Arc<Catalogue>,
    partition: PartitionId,
    time_index: usize,
    columns: Option<VisibilityColumns>,
    buffers: BatchBuffers,
    scratch: Msv4WriteScratch,
    flagged: bool,
}

impl DatasetWriter<'_> {
    fn into_destination(self) -> (Msv4Writer, bool) {
        (self.destination, self.flagged)
    }
}

impl BatchWriter for DatasetWriter<'_> {
    fn visibility_columns(&mut self) -> VisibilityColumns {
        self.columns.take().unwrap_or_default()
    }

    fn push_scalars(
        &mut self,
        _: PlannedScan<'_>,
        _: f64,
        _: &[RowGeometry],
    ) -> SimulatorResult<()> {
        // The coordinates are already declared by the scan's grid.
        Ok(())
    }

    fn send(&mut self, mut columns: VisibilityColumns) -> SimulatorResult<()> {
        let entry = self.catalogue.partition(self.partition);
        let baselines = self.plan.grid.baselines.len();
        let rows = columns.row_count();
        if !rows.is_multiple_of(baselines) {
            return Err(SimulatorError::raised(
                "MSv4 simulation batch split an integration",
            ));
        }
        let channels = entry.frequencies_hz.len();
        let correlations = entry.correlations.len();
        let times = rows / baselines;
        let interval = Sharing::Constant(self.plan.grid.interval_s);
        let error = creation_error("write the simulated MSv4 batch");
        let mut fill = self
            .buffers
            .begin(
                BatchHeader {
                    partition: self.partition,
                    data_group: 0,
                    times,
                    first_channel: 0,
                    channels,
                    correlations: CorrelationAxis::All,
                    roles: Roles::ALL,
                    weights_per_channel: false,
                    weight_rule: WeightRule::Stored,
                    interval,
                    exposure: interval,
                },
                self.catalogue,
            )
            .map_err(&error)?;
        let arrays = fill.arrays();
        let grid = &self.plan.grid;
        let range = self.time_index..self.time_index + times;
        arrays
            .time_offsets_s
            .extend_from_slice(&grid.times_s[range.clone()]);
        arrays
            .field
            .extend_from_slice(&grid.field_per_time[range.clone()]);
        arrays.scan.extend_from_slice(&grid.scan_per_time[range]);
        arrays.values.extend_from_slice(&columns.data);
        arrays.uvw.extend_from_slice(columns.uvw.as_flattened());
        for (row, flag) in columns.flag_row.iter().copied().enumerate() {
            let weight = columns.weight.get(row).copied().unwrap_or(1.);
            self.flagged |= flag;
            arrays
                .flags
                .extend(std::iter::repeat_n(flag, channels * correlations));
            arrays
                .weights
                .extend(std::iter::repeat_n(weight, correlations));
        }
        for time in &arrays.time_offsets_s {
            arrays
                .centroid_offsets_s
                .extend(std::iter::repeat_n(*time, baselines));
        }
        fill.finish().map_err(&error)?;
        let batch = self.buffers.batch().expect("finished simulated batch");
        self.destination
            .write_groups_reusing(&[batch], &mut self.scratch)
            .map_err(error)?;
        self.time_index += times;
        columns.data.clear();
        columns.uvw.clear();
        columns.weight.clear();
        columns.sigma.clear();
        columns.flag_row.clear();
        self.columns = Some(columns);
        Ok(())
    }
}

fn write_pointing(
    writer: &Msv4Writer,
    scan: PlannedScan<'_>,
    grid: &TimeBaselineGrid,
    catalogue: &Catalogue,
    resources: &SessionResources,
) -> SimulatorResult<()> {
    let ids = grid
        .baselines
        .iter()
        .flat_map(|baseline| [baseline.antenna1, baseline.antenna2])
        .collect::<BTreeSet<_>>();
    let times = grid.times_s.len();
    let antenna_samples = (ids.len() as u64).saturating_mul(times as u64);
    // At most one scan's samples, the merged time coordinates and the
    // common pointing writer's currently encoded chunk are live.
    let bytes = antenna_samples.saturating_mul(64).saturating_add(
        (times.min(4096) as u64)
            .saturating_mul(ids.len() as u64)
            .saturating_mul(48),
    );
    let _reservation = admit(
        resources.host(),
        resources.policy(),
        &Demand {
            phase: "the simulator's MSv4 pointing samples",
            memory: bytes,
        },
    )
    .map_err(SimulatorError::admission)?;
    let error = msv4_error("write the simulated antenna pointings");
    let antennas = ids
        .into_iter()
        .map(|id| {
            let coefficients = scan.scan.pointing_rad.repeat(times);
            AntennaPointing::new(
                grid.times_s.clone(),
                None,
                grid.times_s.clone(),
                1,
                coefficients,
            )
            .map(|samples| (id, samples))
        })
        .collect::<Result<_, _>>()
        .map_err(&error)?;
    writer
        .write_pointing(
            catalogue,
            &PointingSeries {
                frame: DirectionRef::J2000,
                terms: 1,
                antennas,
                store_has_pointing: true,
            },
        )
        .map_err(error)
}
