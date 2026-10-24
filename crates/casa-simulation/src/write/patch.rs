// SPDX-License-Identifier: LGPL-3.0-or-later
//! Patching the MeasurementSet `openfromms` opened, at `close`.
//!
//! Every DATA step after `openfromms` covers every scan of the
//! MeasurementSet, so `close` rewrites DATA of every MAIN row in one pass
//! through the bounded selected-row mutation
//! (`MeasurementSetWritePlan::selected_row_mutation`), a batch of rows at a
//! time:
//! 1. **Rows.** The batch's ANTENNA1, ANTENNA2, FLAG_ROW and UVW are read,
//!    and its DATA when the fold starts from it: when no replacing
//!    `predict` cleared the plan. A `tsys-manual` term's σ also reads TIME
//!    and EXPOSURE, and each row's antennas' airmasses at its scan's phase
//!    centre are computed once per integration, as a new MeasurementSet's
//!    geometry computes them; gains and leakage read TIME, where they take
//!    their solution.
//! 2. **Prediction and corruption**, as for a new MeasurementSet
//!    ([`super::pipeline`]): the steps covering the batch's scans are
//!    predicted into zeroed DATA and the component lists' and total
//!    power's visibilities added; then the DATA read from disk; then each
//!    scan's corruptions, gains and leakage then noise, whose draws depend
//!    only on the term, the MAIN row, the channel and the correlation.
//! 3. **Write.** DATA, and the scratch columns MAIN has, as CASA's steps
//!    leave them: CORRECTED_DATA equal to DATA, and MODEL_DATA 1 after a
//!    `predict`. casa-rs creates neither where CASA's `VisSet` would add
//!    them (plan decision 6). Nothing else is written: UVW, FLAG,
//!    FLAG_ROW, WEIGHT and SIGMA stay as they are, as CASA's simple noise
//!    leaves WEIGHT.
//!
//! Before anything is written, FLAG is read through once: a row whose
//! samples are flagged without FLAG_ROW is not yet predicted as CASA
//! predicts it, and is refused, so a refusal leaves the MeasurementSet as
//! it was. A session with no DATA step writes nothing.
//!
//! On the CPU a row's prediction depends only on the row and the steps, and
//! its noise on its MAIN row, so a MeasurementSet created and then patched
//! after `openfromms` holds the DATA a single `close` writes for both
//! sessions' steps, bit for bit; an incremental `predict` adds to the DATA
//! on disk rather than within the predictor, which rounds apart.

use std::time::Instant;

use casa_imaging_runtime::pass::{BackendChoice, WorkerTeam};
use casa_imaging_runtime::{
    CoreAllocation, Demand, Reservation, ReservationWindow, admit, charge_process_caches,
    free_memory,
};
use casa_ms::{
    MeasurementSet, MeasurementSetColumnStorage, MeasurementSetColumnWriteMode,
    MeasurementSetMutationBatch, MeasurementSetMutationColumnBatch,
    MeasurementSetMutationColumnValues, MeasurementSetWriteColumnPlan, MeasurementSetWritePlan,
    MeasurementSetWriteResources, MeasurementSetWriteSession, TARGET_BATCH_BYTES,
};
use casa_tables::{
    RequiredScalarColumnDestination, RequiredScalarColumnValuesMut,
    STREAMING_TILED_COLUMN_BUFFER_BYTES, SelectedArray1DCellsMut, SelectedArray2DCellsMut,
};
use casa_types::measures::MPosition;
use casa_types::{ArrayValue, Complex32};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

use super::components::{self, add_components};
use super::geometry::{ArrayGeometry, RowGeometry};
use super::pipeline::{MINIMUM_NOISE_JOB_ROWS, NoiseContext, NoiseJob, noise_jobs};
use super::prediction::{PredictionStage, device_noise};
use super::{cell_shape, noise_varies_by_row, write_error};
use crate::session::SessionResources;
use crate::session::Simulator;
use crate::session::error::{SimulatorError, SimulatorResult};
use crate::session::observe::Scan;

/// The patch writer's phase, as an admission refusal names it.
const WRITER_PHASE: &str = "the simulator's MeasurementSet patch";

/// The patch pipeline's phase, as an admission refusal names it.
const PIPELINE_PHASE: &str = "the simulator's reads, prediction and noise for the patch";

/// What the patch rewrites.
#[derive(Debug, Clone, Copy)]
struct PatchShape {
    rows: usize,
    scans: usize,
    correlations: usize,
    channels: usize,
    /// Whether the fold starts from the DATA on disk.
    reads_data: bool,
    /// Columns written: DATA and the scratch columns kept in step.
    columns: usize,
    /// Antennas of the MeasurementSet.
    antennas: usize,
    /// Whether the noise needs each row's airmasses and exposure
    /// (`tsys-manual`).
    airmass: bool,
    /// Whether the corruption needs each row's TIME: gains, leakage or
    /// `tsys-manual`. (The gain and leakage terms it reads hold their own
    /// admission, from `setgain` and `setleakage`.)
    times: bool,
}

impl PatchShape {
    fn samples(&self) -> usize {
        self.correlations * self.channels
    }
}

/// What the patch was admitted: the mutation plan, the workers that fit,
/// the rows of a prediction block, and the memory, held until it ends.
#[derive(Debug)]
struct PatchAdmission {
    plan: MeasurementSetWritePlan,
    cores: CoreAllocation,
    block_rows: usize,
    reservation: Reservation,
}

/// Patch the session's opened MeasurementSet with its DATA steps.
pub(crate) fn patch_measurement_set(session: &Simulator) -> SimulatorResult<()> {
    let plan = session.data_plan();
    let Some(opened) = session.opened() else {
        return Err(SimulatorError::raised(
            "only a MeasurementSet openfromms opened is patched",
        ));
    };
    if plan.is_empty() {
        tracing::info!("No DATA step after openfromms: the MeasurementSet is unchanged");
        return Ok(());
    }
    if session.scans().iter().any(|scan| scan.on_disk().is_none()) {
        return Err(SimulatorError::raised(
            "a scan observed after openfromms cannot be patched",
        ));
    }
    let Some((correlations, channels)) = cell_shape(session)? else {
        return Ok(());
    };
    let window = ReservationWindow::open();
    let started = Instant::now();
    let resources = &session.options().resources;
    let mut prediction = PredictionStage::new(
        session,
        (correlations, channels),
        resources.prediction_backend(),
    )?;
    let mut columns = vec!["DATA"];
    if opened.corrected_data {
        columns.push("CORRECTED_DATA");
    }
    if opened.model_data && plan.predicts() {
        columns.push("MODEL_DATA");
    }
    let shape = PatchShape {
        rows: session.scans().iter().map(|scan| scan.row_count()).sum(),
        scans: session.scans().len(),
        correlations,
        channels,
        reads_data: !plan.replaced(),
        columns: columns.len(),
        antennas: session.antennas().len(),
        airmass: noise_varies_by_row(plan),
        times: noise_varies_by_row(plan) || plan.corrupts_by_jones(),
    };
    let admission = admit_patch(resources, &shape, &columns, prediction.as_ref())?;
    let mut ms = MeasurementSet::open(session.path())
        .map_err(|error| SimulatorError::raised(format!("reopen the MeasurementSet: {error}")))?;
    if ms.row_count() != shape.rows {
        return Err(SimulatorError::raised(format!(
            "the MeasurementSet has {} MAIN rows, where openfromms read {}",
            ms.row_count(),
            shape.rows
        )));
    }
    refuse_partly_flagged_rows(&ms, &shape, admission.plan.batch_rows().max(1))?;
    if let Some(stage) = &mut prediction {
        stage.start(admission.block_rows, admission.cores.streaming_workers);
    }
    let team = WorkerTeam::new(admission.cores.streaming_workers)
        .map_err(|error| SimulatorError::raised(format!("the worker team: {error}")))?;
    let batch_rows = admission.plan.batch_rows().max(1);
    let array = shape
        .airmass
        .then(|| {
            ArrayGeometry::new(
                session.telescope(),
                session.antennas(),
                session.options().measures.clone(),
            )
        })
        .transpose()?;
    let mut patch = Patch {
        session,
        shape,
        columns: &columns,
        team: &team,
        prediction,
        array,
        reader: BatchReader::default(),
        data: Vec::new(),
        // As many runs as a batch can have, as admitted.
        runs: Vec::with_capacity(most_runs(shape.scans, batch_rows)),
        receptors: super::window_receptors(session),
        cursor: 0,
        batches: 0,
    };
    let mut writer =
        MeasurementSetWriteSession::start_selected_row_mutation(&mut ms, admission.plan)
            .map_err(write_error("start the MAIN patch"))?;
    loop {
        let rows = writer
            .next_mutation_rows()
            .map_err(write_error("the MAIN patch's rows"))?
            .to_vec();
        if rows.is_empty() {
            break;
        }
        let batch = patch.batch(&ms, rows)?;
        writer
            .write_mutation_batch(&mut ms, batch)
            .map_err(write_error("write a MAIN patch batch"))?;
    }
    let written = writer
        .finish_mutation()
        .map_err(write_error("finish the MAIN patch"))?;
    tracing::info!(
        rows = shape.rows,
        columns = ?columns,
        reads_data = shape.reads_data,
        batches = patch.batches,
        backend = ?patch.prediction.as_ref().map(PredictionStage::backend),
        workers = admission.cores.streaming_workers,
        block_rows = admission.block_rows,
        reserved_bytes = admission.reservation.memory(),
        peak_reserved_bytes = window.close(),
        bytes_written = written.bytes_written,
        writer_write_s = written.write_seconds,
        total_s = started.elapsed().as_secs_f64(),
        "simulation patch"
    );
    Ok(())
}

/// The most runs of one scan a batch of `batch_rows` rows of `scans` scans
/// has: one a row at most, as when each row is a scan of its own.
fn most_runs(scans: usize, batch_rows: usize) -> usize {
    scans.min(batch_rows).max(1)
}

/// A batch's run of one scan: (scan, first row in the batch, rows).
type Run = (usize, usize, usize);

/// The patch's state across batches.
struct Patch<'s> {
    session: &'s Simulator,
    shape: PatchShape,
    columns: &'s [&'static str],
    team: &'s WorkerTeam,
    prediction: Option<PredictionStage<'s>>,
    /// The array's geometry, which gives each row's airmasses when the
    /// noise needs them.
    array: Option<ArrayGeometry<'s>>,
    reader: BatchReader,
    /// The batch's new DATA, `[row][channel][correlation]`.
    data: Vec<Complex32>,
    /// The batch's runs.
    runs: Vec<Run>,
    /// `[window]`: each correlation's receptors.
    receptors: Vec<Vec<[usize; 2]>>,
    /// The first scan the next batch can reach.
    cursor: usize,
    batches: usize,
}

impl Patch<'_> {
    /// The new DATA, and the scratch columns, of MAIN rows `rows`, which
    /// follow one another.
    fn batch(
        &mut self,
        ms: &MeasurementSet,
        rows: Vec<usize>,
    ) -> SimulatorResult<MeasurementSetMutationBatch> {
        let first = rows[0];
        debug_assert!(
            rows.windows(2).all(|pair| pair[1] == pair[0] + 1),
            "the patch's rows follow one another"
        );
        let count = rows.len();
        let samples = self.shape.samples();
        self.reader
            .read(ms, &rows, (self.session.antennas().len(), self.shape))?;
        self.data.clear();
        self.data.resize(count * samples, Complex32::new(0.0, 0.0));
        self.find_runs(first, count);
        if let Some(array) = &self.array {
            self.reader.fill_airmass(
                array,
                (self.session.scans(), &self.runs),
                self.shape.antennas,
            )?;
        }
        let runs = &self.runs;
        let (session, team) = (self.session, self.team);
        let (scans, plan) = (session.scans(), session.data_plan());
        let geometry = &self.reader.geometry;
        // The noise follows the DATA read from disk.
        let reads_data = self.shape.reads_data;
        if let Some(stage) = &mut self.prediction {
            stage.predict(
                team,
                (scans, geometry),
                runs,
                first as u64,
                &mut self.data,
                |scan| device_noise(plan, scan, reads_data),
            )?;
        }
        if plan.predicts_components() {
            add_components(
                team,
                (scans, geometry),
                plan,
                runs,
                (&mut self.data, samples),
            )?;
        }
        if plan.predicts_single_dish() {
            super::single_dish::add_single_dish(
                (scans, geometry),
                plan,
                runs,
                (&mut self.data, samples),
            );
        }
        if reads_data {
            for (value, stored) in self.data.iter_mut().zip(&self.reader.data) {
                *value += *stored;
            }
        }
        let prediction = &self.prediction;
        let noised = |scan| {
            prediction
                .as_ref()
                .is_some_and(|stage| stage.noised(scan, device_noise(plan, scan, reads_data)))
        };
        let mut jobs = noise_jobs(
            (plan, NoiseContext::of(session), team),
            &self.receptors,
            (geometry, runs),
            (first as u64, &mut self.data),
            samples,
            &noised,
        );
        let correlations = self.shape.correlations;
        team.for_each_mut(&mut jobs, |_, job| {
            job.run(correlations, samples);
            Ok::<_, SimulatorError>(())
        })?;
        self.batches += 1;
        Ok(self.mutation(rows))
    }

    /// The batch's runs of one scan from MAIN row `first`, `count` rows,
    /// into `self.runs`. The scans' rows follow one another in MAIN order.
    fn find_runs(&mut self, first: usize, count: usize) {
        let end = first + count;
        let scans = self.session.scans();
        let rows = |scan: &Scan| scan.on_disk().expect("checked before the patch started");
        while scans
            .get(self.cursor)
            .is_some_and(|scan| rows(scan).end <= first)
        {
            self.cursor += 1;
        }
        self.runs.clear();
        for (index, scan) in scans.iter().enumerate().skip(self.cursor) {
            let range = rows(scan);
            if range.start >= end {
                break;
            }
            let (start, stop) = (range.start.max(first), range.end.min(end));
            self.runs.push((index, start - first, stop - start));
        }
    }

    /// The mutation writing the batch's DATA and scratch columns.
    fn mutation(&self, rows: Vec<usize>) -> MeasurementSetMutationBatch {
        let samples = self.shape.samples();
        let shape = IxDyn(&[self.shape.correlations, self.shape.channels]).f();
        let cell = |values: Vec<Complex32>| {
            ArrayValue::Complex32(
                ArrayD::from_shape_vec(shape.clone(), values).expect("a cell's samples"),
            )
        };
        let data: Vec<ArrayValue> = self
            .data
            .chunks_exact(samples)
            .map(|values| cell(values.to_vec()))
            .collect();
        let one = cell(vec![Complex32::new(1.0, 0.0); samples]);
        // DATA is first; the scratch columns follow it.
        let mut columns: Vec<MeasurementSetMutationColumnBatch> = self.columns[1..]
            .iter()
            .map(|&name| MeasurementSetMutationColumnBatch {
                name: name.to_string(),
                values: MeasurementSetMutationColumnValues::Arrays(match name {
                    "MODEL_DATA" => vec![one.clone(); rows.len()],
                    _ => data.clone(),
                }),
            })
            .collect();
        columns.insert(
            0,
            MeasurementSetMutationColumnBatch {
                name: "DATA".to_string(),
                values: MeasurementSetMutationColumnValues::Arrays(data),
            },
        );
        MeasurementSetMutationBatch {
            row_indices: rows,
            columns,
        }
    }
}

/// The columns of a batch's rows read from disk, in buffers kept across
/// batches.
#[derive(Debug, Default)]
struct BatchReader {
    antenna1: Vec<i32>,
    antenna2: Vec<i32>,
    flag_row: Vec<bool>,
    uvw: Vec<f64>,
    /// TIME and EXPOSURE, when the noise needs them.
    time: Vec<f64>,
    exposure: Vec<f64>,
    /// DATA, `[row][channel][correlation]`, when the fold starts from it.
    data: Vec<Complex32>,
    geometry: Vec<RowGeometry>,
    /// The antennas' airmasses at the last integration `fill_airmass`
    /// computed, and its scan and TIME.
    airmasses: Vec<f32>,
    airmass_at: Option<(usize, u64)>,
}

impl BatchReader {
    /// Read MAIN rows `rows` of `ms`, whose antennas are below `antennas`.
    fn read(
        &mut self,
        ms: &MeasurementSet,
        rows: &[usize],
        (antennas, shape): (usize, PatchShape),
    ) -> SimulatorResult<()> {
        let main = ms.main_table();
        let error = |what: &str| {
            let what = what.to_string();
            move |error: casa_tables::TableError| {
                SimulatorError::raised(format!("read MAIN {what}: {error}"))
            }
        };
        for column in [&mut self.antenna1, &mut self.antenna2] {
            column.clear();
        }
        self.flag_row.clear();
        main.required_scalar_columns_for_rows_into(
            rows,
            &mut [
                RequiredScalarColumnDestination::new(
                    "ANTENNA1",
                    RequiredScalarColumnValuesMut::Int32(&mut self.antenna1),
                ),
                RequiredScalarColumnDestination::new(
                    "ANTENNA2",
                    RequiredScalarColumnValuesMut::Int32(&mut self.antenna2),
                ),
                RequiredScalarColumnDestination::new(
                    "FLAG_ROW",
                    RequiredScalarColumnValuesMut::Bool(&mut self.flag_row),
                ),
            ],
        )
        .map_err(error("ANTENNA1, ANTENNA2 and FLAG_ROW"))?;
        self.time.clear();
        self.exposure.clear();
        if shape.airmass {
            main.required_scalar_columns_for_rows_into(
                rows,
                &mut [
                    RequiredScalarColumnDestination::new(
                        "TIME",
                        RequiredScalarColumnValuesMut::Float64(&mut self.time),
                    ),
                    RequiredScalarColumnDestination::new(
                        "EXPOSURE",
                        RequiredScalarColumnValuesMut::Float64(&mut self.exposure),
                    ),
                ],
            )
            .map_err(error("TIME and EXPOSURE"))?;
        } else if shape.times {
            main.required_scalar_columns_for_rows_into(
                rows,
                &mut [RequiredScalarColumnDestination::new(
                    "TIME",
                    RequiredScalarColumnValuesMut::Float64(&mut self.time),
                )],
            )
            .map_err(error("TIME"))?;
        }
        self.uvw.clear();
        main.column_accessor("UVW")
            .and_then(|column| {
                column.fill_array_cells_1d_typed_uncached(
                    rows,
                    SelectedArray1DCellsMut::Float64(&mut self.uvw),
                )
            })
            .map_err(error("UVW"))?;
        if self.uvw.len() != 3 * rows.len() {
            return Err(SimulatorError::raised(
                "read MAIN UVW: a cell does not hold 3 values",
            ));
        }
        if shape.reads_data {
            self.data.clear();
            main.column_accessor("DATA")
                .and_then(|column| {
                    column.fill_array_cells_2d_channel_range_typed_uncached(
                        rows,
                        0,
                        shape.channels,
                        SelectedArray2DCellsMut::RowChannelComplex32(&mut self.data),
                    )
                })
                .map_err(error("DATA"))?;
            if self.data.len() != rows.len() * shape.samples() {
                return Err(SimulatorError::raised(
                    "read MAIN DATA: a cell is not of the shape its window gives",
                ));
            }
        }
        self.geometry.clear();
        for (index, &row) in rows.iter().enumerate() {
            let antenna = |value: i32| {
                usize::try_from(value)
                    .ok()
                    .filter(|&antenna| antenna < antennas)
                    .ok_or_else(|| {
                        SimulatorError::raised(format!(
                            "MAIN row {row} names antenna {value}, which ANTENNA does not have"
                        ))
                    })
            };
            let uvw = &self.uvw[3 * index..3 * index + 3];
            self.geometry.push(RowGeometry {
                antenna1: antenna(self.antenna1[index])?,
                antenna2: antenna(self.antenna2[index])?,
                uvw_m: [uvw[0], uvw[1], uvw[2]],
                flagged: self.flag_row[index],
                time_s: self.time.get(index).copied().unwrap_or(0.0),
                airmass: [f32::NAN; 2],
                exposure_s: self.exposure.get(index).copied().unwrap_or(0.0),
                ..RowGeometry::default()
            });
        }
        Ok(())
    }

    /// Give each row of the batch's `runs` the airmasses of its antennas,
    /// of the first `antennas`, at its scan's phase centre at its TIME,
    /// from `array`: computed once for each integration (scan and TIME)
    /// the rows follow one another in.
    fn fill_airmass(
        &mut self,
        array: &ArrayGeometry<'_>,
        (scans, runs): (&[Scan], &[Run]),
        antennas: usize,
    ) -> SimulatorResult<()> {
        for &(scan, start, count) in runs {
            for index in start..start + count {
                let time_s = self.time[index];
                if self.airmass_at != Some((scan, time_s.to_bits())) {
                    self.airmasses =
                        array.airmasses(scans[scan].phase_center_rad, time_s, antennas)?;
                    self.airmass_at = Some((scan, time_s.to_bits()));
                }
                let row = &mut self.geometry[index];
                row.airmass = [self.airmasses[row.antenna1], self.airmasses[row.antenna2]];
            }
        }
        Ok(())
    }
}

/// Refuse, before anything is written, a row with flagged samples but no
/// FLAG_ROW: CASA predicts its flagged samples zero, and the prediction
/// takes each row's flag alone. Both simulators flag whole rows.
fn refuse_partly_flagged_rows(
    ms: &MeasurementSet,
    shape: &PatchShape,
    batch_rows: usize,
) -> SimulatorResult<()> {
    let main = ms.main_table();
    let error =
        |error: casa_tables::TableError| SimulatorError::raised(format!("read MAIN FLAG: {error}"));
    let (mut flag_row, mut flags, mut rows) = (Vec::new(), Vec::new(), Vec::new());
    let samples = shape.samples();
    for first in (0..shape.rows).step_by(batch_rows) {
        rows.clear();
        rows.extend(first..shape.rows.min(first + batch_rows));
        flag_row.clear();
        main.required_scalar_columns_for_rows_into(
            &rows,
            &mut [RequiredScalarColumnDestination::new(
                "FLAG_ROW",
                RequiredScalarColumnValuesMut::Bool(&mut flag_row),
            )],
        )
        .map_err(error)?;
        flags.clear();
        main.column_accessor("FLAG")
            .and_then(|column| {
                column.fill_array_cells_2d_channel_range_typed_uncached(
                    &rows,
                    0,
                    shape.channels,
                    SelectedArray2DCellsMut::RowChannelBool(&mut flags),
                )
            })
            .map_err(error)?;
        let partly = flags
            .chunks_exact(samples)
            .zip(&flag_row)
            .position(|(cell, &row_flag)| !row_flag && cell.iter().any(|&flag| flag));
        if let Some(offset) = partly {
            return Err(SimulatorError::not_yet_implemented(format!(
                "predict or corrupt after openfromms on MAIN row {}, whose samples are \
                 flagged without FLAG_ROW, is not yet implemented in casa-rs (#841)",
                first + offset
            )));
        }
    }
    Ok(())
}

/// Admit the patch for `shape`, writing `columns`, within `resources`, the
/// pipeline predicting with `prediction` when a step predicts.
fn admit_patch(
    resources: &SessionResources,
    shape: &PatchShape,
    columns: &[&str],
    prediction: Option<&PredictionStage<'_>>,
) -> SimulatorResult<PatchAdmission> {
    let (host, policy) = (resources.host(), resources.policy());
    // The patch reads the MeasurementSet through the process's table cache.
    charge_process_caches(host, policy).map_err(SimulatorError::admission)?;
    let free = free_memory(host, policy);
    // The plan holds the selected rows, and copies them once to check them.
    // Its batches aim at casa-ms's batch size, whatever the budget allows,
    // as a new MeasurementSet's tile bands do.
    let index_bytes = 2 * (shape.rows * size_of::<usize>()) as u64;
    let writer_bytes = usize::try_from((free / 2).saturating_sub(index_bytes))
        .unwrap_or(usize::MAX)
        .min(TARGET_BATCH_BYTES);
    let plan = patch_plan(shape, columns, writer_bytes).map_err(|_| {
        SimulatorError::admission(casa_imaging_runtime::Admission {
            phase: WRITER_PHASE,
            required: index_bytes
                + 2 * (shape.columns * shape.samples() * size_of::<Complex32>()) as u64,
            available: free,
        })
    })?;
    let mut reservation = admit(
        host,
        policy,
        &Demand {
            phase: WRITER_PHASE,
            memory: plan.maximum_resident_bytes() as u64 + index_bytes,
        },
    )
    .map_err(SimulatorError::admission)?;
    let policy_cores = resources
        .close_cores(prediction.is_some_and(|stage| stage.backend() == BackendChoice::Metal));
    let free = free_memory(host, policy);
    let batch_rows = plan.batch_rows().max(1);
    let demand = |cores: CoreAllocation, block_rows: usize| {
        pipeline_bytes(shape, columns.len(), batch_rows)
            + prediction.map_or(0, |stage| stage.bytes(block_rows, cores.streaming_workers))
    };
    // As `close` admits a new MeasurementSet's pipeline: a batch predicted
    // as one block when that fits with the policy's workers, else small
    // blocks with the workers shrinking first.
    let (cores, block_rows) = if demand(policy_cores, batch_rows) <= free {
        (policy_cores, batch_rows)
    } else {
        let small = prediction.map_or(batch_rows, |stage| stage.small_block_rows(batch_rows));
        let cores = (1..=policy_cores.streaming_workers)
            .rev()
            .map(|workers| policy_cores.at_most(workers))
            .find(|cores| demand(*cores, small) <= free)
            .unwrap_or_else(|| policy_cores.at_most(1));
        (cores, small)
    };
    let pipeline = admit(
        host,
        policy,
        &Demand {
            phase: PIPELINE_PHASE,
            memory: demand(cores, block_rows),
        },
    )
    .map_err(SimulatorError::admission)?;
    reservation.join(pipeline);
    Ok(PatchAdmission {
        plan,
        cores,
        block_rows,
        reservation,
    })
}

/// The mutation of every row of `shape` writing `columns`, with
/// `available_bytes` for its batch.
fn patch_plan(
    shape: &PatchShape,
    columns: &[&str],
    available_bytes: usize,
) -> Result<MeasurementSetWritePlan, casa_ms::MeasurementSetWriteError> {
    // A row's cell of each column: its samples and the array holding them.
    let bytes_per_row = shape.samples() * size_of::<Complex32>() + size_of::<ArrayValue>();
    MeasurementSetWritePlan::selected_row_mutation(
        (0..shape.rows).collect(),
        columns
            .iter()
            .map(|name| MeasurementSetWriteColumnPlan {
                name: (*name).to_string(),
                bytes_per_row,
                mode: MeasurementSetColumnWriteMode::Replace,
                storage_manager: MeasurementSetColumnStorage::Persisted,
                tile_shape: None,
                create_source_column: None,
            })
            .collect(),
        MeasurementSetWriteResources {
            available_bytes,
            maximum_live_batches: 1,
            tiled_column_buffer_bytes: 0,
        },
    )
}

/// The tiled managers' I/O while the patch reads one column of a batch, or
/// writes the batch: tiles read, decoded and rewritten, which the tile
/// layout bounds and the rows do not (`casa_tables::SelectedReadFootprint`
/// leaves them out). It is charged as `close`'s writer charges a tiled
/// column's buffer; the reads and the write come one after another.
const TILED_IO_BYTES: u64 = STREAMING_TILED_COLUMN_BUFFER_BYTES as u64;

/// Copies of a written batch's cells casa-ms's mutation holds beside the
/// batch itself (measured by `tests/session_memory/patch.rs`):
/// `write_mutation_batch` clones each cell into the table's pending cells,
/// the in-place save clones the changed rows' cells again to collect them,
/// and the tiled manager stages them into its tiles.
const MUTATION_CELL_COPIES: u64 = 3;

/// Copies of a column a typed row-major read stages beside its output: the
/// cells it reorders into the output's layout (measured as above).
const READ_CELL_COPIES: u64 = 1;

/// Per antenna, when the noise needs airmasses: the array geometry's
/// position, and one integration's elevation and airmass, with room for the
/// measures frame of the conversion (as `close`'s geometry charges its
/// antennas' scratch).
const AIRMASS_ANTENNA_BYTES: u64 = size_of::<MPosition>() as u64 + 64;

/// Bytes the patch holds beside its writer for batches of `batch_rows`
/// rows writing `columns` columns: the columns read (ANTENNA1, ANTENNA2,
/// FLAG_ROW, UVW, TIME when the corruption needs it, EXPOSURE when the
/// noise needs airmasses, and DATA when the fold starts from it, or FLAG
/// while it is checked) and
/// their staging, the rows' geometry, the new DATA, the mutation's copies
/// of the cells written, the tiled managers' I/O, the batch's runs and
/// jobs, and the airmasses' antennas.
fn pipeline_bytes(shape: &PatchShape, columns: usize, batch_rows: usize) -> u64 {
    let samples = shape.samples() as u64;
    let cell = samples * size_of::<Complex32>() as u64;
    let staged = 1 + READ_CELL_COPIES;
    let read = staged * if shape.reads_data { cell } else { 0 }.max(samples);
    let times = staged * 8 * (u64::from(shape.times) + u64::from(shape.airmass));
    let written = columns as u64 * MUTATION_CELL_COPIES * (cell + size_of::<ArrayValue>() as u64);
    let row = 4
        + 4
        + 1
        + staged * 3 * 8
        + times
        + read
        + size_of::<RowGeometry>() as u64
        + cell
        + written;
    let antennas = if shape.airmass {
        shape.antennas as u64 * AIRMASS_ANTENNA_BYTES
    } else {
        0
    };
    batch_rows as u64 * row + TILED_IO_BYTES + run_bytes(shape.scans, batch_rows) + antennas
}

/// Bytes of a batch's runs, and of the noise and component-list jobs they
/// split into, for batches of `batch_rows` rows of `scans` scans: a run of
/// each scan the batch reaches, which may be one a row (#886 review), and
/// of each kind a job a run and one more every [`MINIMUM_NOISE_JOB_ROWS`]
/// and [`components::JOB_ROWS`] rows.
fn run_bytes(scans: usize, batch_rows: usize) -> u64 {
    let runs = most_runs(scans, batch_rows);
    let noise_jobs = runs + batch_rows / MINIMUM_NOISE_JOB_ROWS;
    let component_jobs = runs + batch_rows / components::JOB_ROWS;
    (runs * size_of::<Run>()
        + noise_jobs * size_of::<NoiseJob<'static>>()
        + component_jobs * components::JOB_BYTES) as u64
}
