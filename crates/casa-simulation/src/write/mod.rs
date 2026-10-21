// SPDX-License-Identifier: LGPL-3.0-or-later
//! Writing a session's MeasurementSet at `close`.
//!
//! One pass creates the MeasurementSet in a staging directory, fills the
//! subtables, streams MAIN through the bounded write session a batch of
//! column-shaped rows at a time, and publishes the result. The layout is
//! casa-rs's conventional one: IncrementalStMan and StandardStMan scalars,
//! TiledShapeStMan visibility columns with CASA's tile shapes, and
//! TiledColumnStMan for UVW. Following plan decision 6, MAIN has DATA but
//! no MODEL_DATA or CORRECTED_DATA. DATA is zero (until `predict`, Phase 1d)
//! plus the noise of every `corrupt` step covering the row's scan, added as
//! the row is written: noise depends only on the row's MAIN index, so the
//! batching does not change it.

mod geometry;
mod subtables;

use casa_ms::schema::SubtableId;
use casa_ms::{
    MeasurementSet, MeasurementSetBuilder, MeasurementSetCreateTarget, MeasurementSetWritePlan,
    MeasurementSetWriteSession, OptionalMainColumn, ScalarColumnId,
};
use casa_tables::{ColumnOverrides, StreamedScalarValues, TableInfo};
use casa_types::{Complex32, ScalarValue};

use crate::corrupt::NoiseApplication;
use crate::session::Simulator;
use crate::session::error::{SimulatorError, SimulatorResult};
use crate::session::observe::Scan;
use geometry::{ArrayGeometry, RowGeometry};

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
    let generated = subtables::subtable_overrides(session)?;
    write_main(&mut ms, session, &staging, &generated)?;
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

/// Write MAIN and save the MeasurementSet, with the subtable columns
/// `generated` holds (POINTING).
fn write_main(
    ms: &mut MeasurementSet,
    session: &Simulator,
    staging: &std::path::Path,
    generated: &casa_ms::SubtableColumnOverrides,
) -> SimulatorResult<()> {
    let row_count = session.scans().iter().map(Scan::row_count).sum::<usize>();
    let Some((correlations, channels)) = cell_shape(session)? else {
        ms.save()
            .map_err(|error| SimulatorError::raised(format!("save the MeasurementSet: {error}")))?;
        return Ok(());
    };
    let plan = MeasurementSetWritePlan::visibility_creation(
        row_count,
        correlations,
        channels,
        session.telescope(),
        session.options().write_resources,
    )
    .map_err(write_error("plan the MAIN write"))?;
    let batch_rows = plan.batch_rows().max(1);
    let mut writer = MainWriter::start(staging, plan, correlations, channels)?;
    let geometry = ArrayGeometry::new(
        session.telescope(),
        session.antennas(),
        session.options().measures.clone(),
    )?;
    let mut rows = Vec::new();
    for (index, scan) in session.scans().iter().enumerate() {
        let noise = session.data_plan().noise_for_scan(index);
        for time_s in scan.integration_times_s() {
            geometry.integration_rows(scan, time_s, &mut rows)?;
            writer.push_integration(scan, time_s, &rows, &noise, batch_rows)?;
        }
    }
    writer.finish(ms, row_count, generated)
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

/// MAIN's write session, its batch being filled, and the scalar runs of the
/// integration being pushed.
struct MainWriter {
    session: MeasurementSetWriteSession,
    columns: ScalarColumns,
    correlations: usize,
    samples: usize,
    /// The MAIN index of the next row pushed.
    next_row: u64,
    batch: casa_ms::VisibilityColumns,
    antenna1: Vec<i32>,
    antenna2: Vec<i32>,
    flag_row: Vec<bool>,
}

impl MainWriter {
    fn start(
        staging: &std::path::Path,
        plan: MeasurementSetWritePlan,
        correlations: usize,
        channels: usize,
    ) -> SimulatorResult<Self> {
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
        let batch = session.visibility_columns();
        Ok(Self {
            session,
            columns,
            correlations,
            samples: correlations * channels,
            next_row: 0,
            batch,
            antenna1: Vec::new(),
            antenna2: Vec::new(),
            flag_row: Vec::new(),
        })
    }

    /// Append one integration's rows: scalars as runs, visibility columns
    /// into batches of at most `batch_rows`, each row's DATA zero plus
    /// `noise`.
    fn push_integration(
        &mut self,
        scan: &Scan,
        time_s: f64,
        rows: &[RowGeometry],
        noise: &[NoiseApplication],
        batch_rows: usize,
    ) -> SimulatorResult<()> {
        self.antenna1.clear();
        self.antenna2.clear();
        self.flag_row.clear();
        for row in rows {
            self.antenna1.push(row.antenna1 as i32);
            self.antenna2.push(row.antenna2 as i32);
            self.flag_row.push(row.flagged);
            let start = self.batch.data.len();
            self.batch
                .data
                .resize(start + self.samples, Complex32::new(0.0, 0.0));
            let cell = &mut self.batch.data[start..];
            for application in noise {
                application.add_to_row(
                    self.next_row,
                    row.antenna1 == row.antenna2,
                    self.correlations,
                    cell,
                );
            }
            self.next_row += 1;
            self.batch.flag_row.push(row.flagged);
            self.batch.uvw.push(row.uvw_m);
            self.batch.weight.push(row.weight);
            self.batch.sigma.push(row.sigma);
            if self.batch.row_count() == batch_rows {
                self.send_batch()?;
            }
        }
        self.push_scalars(scan, time_s, rows.len())
    }

    fn push_scalars(&mut self, scan: &Scan, time_s: f64, count: usize) -> SimulatorResult<()> {
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
                "EXPOSURE" | "INTERVAL" => ScalarValue::Float64(scan.integration_s),
                "FIELD_ID" => ScalarValue::Int32(scan.field_id as i32),
                "OBSERVATION_ID" => ScalarValue::Int32(scan.observation_id),
                "SCAN_NUMBER" => ScalarValue::Int32(scan.scan_number),
                "STATE_ID" => ScalarValue::Int32(scan.state_id as i32),
                // ARRAY_ID, FEED1, FEED2 and PROCESSOR_ID.
                _ => ScalarValue::Int32(0),
            };
            session
                .push_repeated_scalar(column, Some(value), count)
                .map_err(&error)?;
        }
        Ok(())
    }

    fn send_batch(&mut self) -> SimulatorResult<()> {
        let full = std::mem::replace(&mut self.batch, self.session.visibility_columns());
        self.session
            .send_visibility_columns(full)
            .map_err(write_error("MAIN visibility batch"))
    }

    fn finish(
        mut self,
        ms: &mut MeasurementSet,
        row_count: usize,
        generated: &casa_ms::SubtableColumnOverrides,
    ) -> SimulatorResult<()> {
        if self.batch.row_count() > 0 {
            self.send_batch()?;
        }
        let mut overrides = ColumnOverrides::for_row_count(row_count);
        for column in ["DATA", "FLAG", "FLAG_CATEGORY", "UVW", "WEIGHT", "SIGMA"] {
            overrides.insert_deferred(column);
        }
        self.session
            .save_and_finish_with_subtables(ms, &overrides, generated)
            .map_err(write_error("finish the MAIN write"))?;
        Ok(())
    }
}
