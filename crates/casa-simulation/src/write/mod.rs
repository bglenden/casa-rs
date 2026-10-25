// SPDX-License-Identifier: LGPL-3.0-or-later
//! Writing a session's MeasurementSet at `close`.
//!
//! One pass creates the MeasurementSet in a staging directory, fills the
//! subtables, streams MAIN through the bounded write session a batch of
//! column-shaped rows at a time, and publishes the result. The layout is
//! casa-rs's conventional one: IncrementalStMan and StandardStMan scalars,
//! TiledShapeStMan visibility columns with CASA's tile shapes, and
//! TiledColumnStMan for UVW. Following plan decision 6, MAIN has DATA but
//! no MODEL_DATA or CORRECTED_DATA. DATA is the sum of the predictions of
//! every `predict` step covering the row's scan (zero without one) plus the
//! noise of every `corrupt` step covering it.
//!
//! Before anything is written, what the pass holds is admitted against the
//! session's resource policy ([`resources`]); each batch's geometry,
//! prediction ([`prediction`]) and noise are then computed on the session's
//! worker team ([`pipeline`]).
//!
//! A session `openfromms` opened is patched in place instead ([`patch`]).

mod components;
mod geometry;
mod patch;
mod pipeline;
mod prediction;
mod resources;
mod single_dish;
mod subtables;
#[cfg(test)]
mod tests;

use std::time::Instant;

use casa_imaging_runtime::ReservationWindow;
use casa_imaging_runtime::pass::WorkerTeam;
use casa_ms::schema::SubtableId;
use casa_ms::{
    MeasurementSet, MeasurementSetBuilder, MeasurementSetCreateTarget, MeasurementSetWritePlan,
    MeasurementSetWriteSession, MeasurementSetWriteTelemetry, OptionalMainColumn, ScalarColumnId,
    VisibilityColumns,
};
use casa_tables::{ColumnOverrides, StreamedScalarValues, TableInfo};
use casa_types::ScalarValue;

use crate::session::Simulator;
use crate::session::error::{SimulatorError, SimulatorResult};
use crate::session::observe::PlannedScan;
use crate::session::plan::DataPlan;
use geometry::{ArrayGeometry, RowGeometry};
pub(crate) use patch::patch_measurement_set;
use pipeline::{NoiseContext, Pipeline};
use prediction::PredictionStage;
use resources::{MainAdmission, MainShape, NoiseScales};

/// The readme CASA's simulator gives MAIN.
const MAIN_README: [&str; 2] = [
    "This is a MeasurementSet Table holding measurements from a Telescope",
    "This is a MeasurementSet Table holding simulated astronomical observations",
];

fn write_error(what: &str) -> impl Fn(casa_ms::MeasurementSetWriteError) -> SimulatorError + '_ {
    move |error| SimulatorError::raised(format!("{what}: {error}"))
}

/// Create the session's MeasurementSet.
pub(crate) fn write_measurement_set(session: &Simulator) -> SimulatorResult<()> {
    let window = ReservationWindow::open();
    let planned = PlannedScan::of_all(session.scans()).ok_or_else(|| {
        SimulatorError::raised("only a session's planned scans make a new MeasurementSet")
    })?;
    let cell = cell_shape(session)?;
    let shape = cell.map(|cell| main_shape(session, &planned, cell));
    let resources = &session.options().resources;
    let prediction = match cell {
        Some(cell) => PredictionStage::new(session, cell, resources.prediction_backend())?,
        None => None,
    };
    // Admitted before anything is written, so a refusal leaves nothing.
    let admission = shape
        .as_ref()
        .map(|shape| resources::admit_main_write(resources, shape, prediction.as_ref()))
        .transpose()?;
    let target = MeasurementSetCreateTarget::prepare(session.path(), true)
        .map_err(write_error("prepare the MeasurementSet"))?;
    let staging = target.staging_path().to_path_buf();
    let mut ms = MeasurementSet::create(
        &staging,
        MeasurementSetBuilder::new()
            .with_main_column(OptionalMainColumn::Data)
            .with_optional_subtable(SubtableId::Source),
    )
    .map_err(|error| SimulatorError::raised(format!("create the MeasurementSet: {error}")))?;
    std::fs::create_dir_all(&staging).map_err(|error| {
        SimulatorError::raised(format!("create {}: {error}", staging.display()))
    })?;
    let info = ms.main_table().info().clone();
    ms.main_table_mut().set_info(TableInfo {
        table_type: info.table_type,
        sub_type: "simulator".to_string(),
        readme: MAIN_README.iter().map(|line| (*line).to_string()).collect(),
    });
    subtables::write_subtables(&mut ms, session)?;
    let generated = subtables::subtable_overrides(&planned)?;
    match shape.zip(admission) {
        Some((shape, admission)) => {
            let main = MainTarget {
                staging: &staging,
                generated: &generated,
                window: &window,
            };
            write_main(
                &mut ms,
                (session, &planned),
                &main,
                &shape,
                (admission, prediction),
            )?;
        }
        None => ms
            .save()
            .map_err(|error| SimulatorError::raised(format!("save the MeasurementSet: {error}")))?,
    }
    target
        .commit()
        .map_err(write_error("publish the MeasurementSet"))
}

/// The cell shape every scan shares: (correlations, channels).
fn cell_shape(session: &Simulator) -> SimulatorResult<Option<(usize, usize)>> {
    let mut shape = None;
    for scan in session.scans() {
        let window = &session.spectral_windows()[scan.spectral_window_id];
        let this = (window.correlations.len(), window.channel_count);
        match shape {
            None => shape = Some(this),
            Some(first) if first != this => {
                return Err(SimulatorError::raised(
                    "observing spectral windows of different shapes in one MeasurementSet is not \
                     yet implemented in casa-rs",
                ));
            }
            Some(_) => {}
        }
    }
    Ok(shape)
}

/// `[window]`: the receptors of each correlation of the session's spectral
/// windows (`Stokes::receptor1`, `receptor2`), which gains and leakage take.
pub(super) fn window_receptors(session: &Simulator) -> Vec<Vec<[usize; 2]>> {
    session
        .spectral_windows()
        .iter()
        .map(|window| {
            window
                .correlation_products()
                .iter()
                .map(|pair| pair.map(|receptor| usize::try_from(receptor).unwrap_or(0)))
                .collect()
        })
        .collect()
}

/// MAIN as the session's planned scans make it, with cells of
/// `(correlations, channels)`.
fn main_shape<'s>(
    session: &'s Simulator,
    planned: &[PlannedScan<'_>],
    (correlations, channels): (usize, usize),
) -> MainShape<'s> {
    MainShape {
        rows: planned.iter().map(|scan| scan.rows.row_count()).sum(),
        correlations,
        channels,
        telescope: session.telescope(),
        largest_integration: planned
            .iter()
            .map(|scan| scan.rows.baseline_count())
            .max()
            .unwrap_or(0),
        antennas: planned
            .iter()
            .map(|scan| scan.rows.antenna_count)
            .max()
            .unwrap_or(0),
        generated_bytes: subtables::generated_bytes(planned),
        noise_scales: NoiseScales::of(
            session.data_plan(),
            channels,
            Some((
                planned
                    .iter()
                    .map(|scan| scan.rows.baseline_count())
                    .filter(|&rows| rows > 0)
                    .min()
                    .unwrap_or(1),
                planned
                    .iter()
                    .map(|scan| scan.rows.antenna_count)
                    .max()
                    .unwrap_or(0),
            )),
        ),
    }
}

/// Where MAIN goes: the staging directory, the subtable columns the save
/// generates (POINTING), and the window whose peak the pass reports.
struct MainTarget<'a> {
    staging: &'a std::path::Path,
    generated: &'a casa_ms::SubtableColumnOverrides,
    window: &'a ReservationWindow,
}

/// Write MAIN and save the MeasurementSet, holding `admission` until done,
/// predicting with `prediction` when the session predicts.
fn write_main<'s>(
    ms: &mut MeasurementSet,
    (session, planned): (&'s Simulator, &[PlannedScan<'_>]),
    target: &MainTarget<'_>,
    shape: &MainShape<'_>,
    (admission, mut prediction): (MainAdmission, Option<PredictionStage<'s>>),
) -> SimulatorResult<()> {
    let started = Instant::now();
    let batch_rows = admission.plan.batch_rows().max(1);
    if let Some(stage) = &mut prediction {
        stage.start(admission.block_rows, admission.cores.streaming_workers);
    }
    let backend = prediction.as_ref().map_or_else(
        || session.options().resources.resolved_backend(false),
        PredictionStage::backend,
    );
    let mut writer = MainWriter::start(target.staging, admission.plan.clone())?;
    let geometry = ArrayGeometry::new(
        session.telescope(),
        session.antennas(),
        session.options().measures.clone(),
    )?
    .with_airmass(noise_varies_by_row(session.data_plan()));
    let team = WorkerTeam::new(admission.cores.streaming_workers)
        .map_err(|error| SimulatorError::raised(format!("the worker team: {error}")))?;
    let mut pipeline = Pipeline::new(
        (session.scans(), planned),
        &geometry,
        &team,
        (
            prediction,
            session.data_plan(),
            NoiseContext::of(session),
            shape.noise_scales,
        ),
        (
            shape.correlations,
            shape.channels,
            window_receptors(session),
        ),
    );
    let integrations = planned
        .iter()
        .enumerate()
        .filter(|(_, scan)| scan.rows.baseline_count() > 0)
        .flat_map(|(index, scan)| {
            scan.rows
                .integration_times_s()
                .map(move |time| (index, time))
        });
    let setup = started.elapsed();
    pipeline.write_rows(&mut writer, integrations, shape.rows, batch_rows)?;
    let finishing = Instant::now();
    let written = writer.finish(ms, shape.rows, target.generated)?;
    let finish = finishing.elapsed();
    let times = &pipeline.times;
    tracing::info!(
        rows = shape.rows,
        batch_rows,
        block_rows = admission.block_rows,
        batches = times.batches,
        workers = admission.cores.streaming_workers,
        policy_workers = admission.policy_cores.streaming_workers,
        backend = ?backend,
        reserved_bytes = admission.reservation.memory(),
        peak_reserved_bytes = target.window.close(),
        geometry_s = times.geometry.as_secs_f64(),
        zero_s = times.zero.as_secs_f64(),
        predict_s = times.predict.as_secs_f64(),
        block_fill_s = pipeline.block_fill_time().as_secs_f64(),
        cells_s = times.cells.as_secs_f64(),
        send_wait_s = times.send.as_secs_f64(),
        setup_s = setup.as_secs_f64(),
        finish_s = finish.as_secs_f64(),
        writer_write_s = written.write_seconds,
        writer_finalize_s = written.finalize_seconds,
        bytes_written = written.bytes_written,
        total_s = started.elapsed().as_secs_f64(),
        "simulation pass"
    );
    Ok(())
}

/// Whether any noise of `plan` has a σ that varies by row (the Tsys
/// modes), whose rows then need their antennas' airmasses and exposure.
/// Every `corrupt` covers scan 0, so its noise is all the plan's.
fn noise_varies_by_row(plan: &DataPlan) -> bool {
    plan.noise_for_scan(0)
        .iter()
        .any(crate::corrupt::NoiseApplication::varies_by_row)
}

/// The resolved MAIN scalar columns.
struct ScalarColumns {
    antenna1: ScalarColumnId,
    antenna2: ScalarColumnId,
    flag_row: ScalarColumnId,
    time: ScalarColumnId,
    time_centroid: ScalarColumnId,
    constants: [(ScalarColumnId, &'static str); 11],
}

/// MAIN's write session and the scalar runs of the integration being
/// pushed.
struct MainWriter {
    session: MeasurementSetWriteSession,
    columns: ScalarColumns,
    antenna1: Vec<i32>,
    antenna2: Vec<i32>,
    flag_row: Vec<bool>,
}

impl MainWriter {
    fn start(staging: &std::path::Path, plan: MeasurementSetWritePlan) -> SimulatorResult<Self> {
        let session = MeasurementSetWriteSession::start(staging, plan)
            .map_err(write_error("start the MAIN write"))?;
        let id = |name: &str| {
            session
                .scalar_column(name)
                .map_err(write_error("MAIN scalar column"))
        };
        let constant = |name: &'static str| id(name).map(|column| (column, name));
        let columns = ScalarColumns {
            antenna1: id("ANTENNA1")?,
            antenna2: id("ANTENNA2")?,
            flag_row: id("FLAG_ROW")?,
            time: id("TIME")?,
            time_centroid: id("TIME_CENTROID")?,
            constants: [
                constant("ARRAY_ID")?,
                constant("DATA_DESC_ID")?,
                constant("EXPOSURE")?,
                constant("FEED1")?,
                constant("FEED2")?,
                constant("FIELD_ID")?,
                constant("INTERVAL")?,
                constant("OBSERVATION_ID")?,
                constant("PROCESSOR_ID")?,
                constant("SCAN_NUMBER")?,
                constant("STATE_ID")?,
            ],
        };
        Ok(Self {
            session,
            columns,
            antenna1: Vec::new(),
            antenna2: Vec::new(),
            flag_row: Vec::new(),
        })
    }

    /// Append the scalar columns of one integration of `scan` at stored
    /// TIME `time_s`, whose rows are `rows`.
    fn push_scalars(
        &mut self,
        PlannedScan {
            scan,
            rows: planned,
        }: PlannedScan<'_>,
        time_s: f64,
        rows: &[RowGeometry],
    ) -> SimulatorResult<()> {
        self.antenna1.clear();
        self.antenna2.clear();
        self.flag_row.clear();
        for row in rows {
            self.antenna1.push(row.antenna1 as i32);
            self.antenna2.push(row.antenna2 as i32);
            self.flag_row.push(row.flagged);
        }
        let count = rows.len();
        let error = write_error("MAIN scalars");
        let columns = &self.columns;
        let session = &mut self.session;
        session
            .push_scalar_column(
                columns.antenna1,
                StreamedScalarValues::Int32(&self.antenna1),
            )
            .map_err(&error)?;
        session
            .push_scalar_column(
                columns.antenna2,
                StreamedScalarValues::Int32(&self.antenna2),
            )
            .map_err(&error)?;
        session
            .push_scalar_column(columns.flag_row, StreamedScalarValues::Bool(&self.flag_row))
            .map_err(&error)?;
        for column in [columns.time, columns.time_centroid] {
            session
                .push_repeated_scalar(column, Some(ScalarValue::Float64(time_s)), count)
                .map_err(&error)?;
        }
        for (column, name) in columns.constants {
            let value = match name {
                "DATA_DESC_ID" => ScalarValue::Int32(scan.spectral_window_id as i32),
                "EXPOSURE" | "INTERVAL" => ScalarValue::Float64(planned.integration_s),
                "FIELD_ID" => ScalarValue::Int32(scan.field_id as i32),
                "OBSERVATION_ID" => ScalarValue::Int32(planned.observation_id),
                "SCAN_NUMBER" => ScalarValue::Int32(scan.scan_number),
                "STATE_ID" => ScalarValue::Int32(planned.state_id as i32),
                // ARRAY_ID, FEED1, FEED2 and PROCESSOR_ID.
                _ => ScalarValue::Int32(0),
            };
            session
                .push_repeated_scalar(column, Some(value), count)
                .map_err(&error)?;
        }
        Ok(())
    }

    /// An empty batch, with the buffers of one the writer has finished with
    /// when there is one.
    fn visibility_columns(&self) -> VisibilityColumns {
        self.session.visibility_columns()
    }

    /// Queue a full batch for the writer's thread; blocks while its queue is
    /// full.
    fn send(&self, batch: VisibilityColumns) -> SimulatorResult<()> {
        self.session
            .send_visibility_columns(batch)
            .map_err(write_error("MAIN visibility batch"))
    }

    fn finish(
        self,
        ms: &mut MeasurementSet,
        row_count: usize,
        generated: &casa_ms::SubtableColumnOverrides,
    ) -> SimulatorResult<MeasurementSetWriteTelemetry> {
        let mut overrides = ColumnOverrides::for_row_count(row_count);
        for column in ["DATA", "FLAG", "FLAG_CATEGORY", "UVW", "WEIGHT", "SIGMA"] {
            overrides.insert_deferred(column);
        }
        self.session
            .save_and_finish_with_subtables(ms, &overrides, generated)
            .map_err(write_error("finish the MAIN write"))
    }
}
