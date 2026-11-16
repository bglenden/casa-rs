// SPDX-License-Identifier: LGPL-3.0-or-later
//! Different visibility shapes through the common calculation pipeline.
use std::collections::BTreeMap;

use casa_imaging_runtime::{Demand, Reservation, ReservationWindow, admit, free_memory};
use casa_ms::{
    MeasurementSetArrayColumnPlan, MeasurementSetArrayShapePlan, MeasurementSetColumnStorage,
    MeasurementSetCreateTarget, MeasurementSetWritePlan, MeasurementSetWriteResources,
    standard_main_scalar_column_plans,
};
use casa_tables::{STREAMING_TILED_COLUMN_BUFFER_BYTES, StreamedTiledShapeValueType};

use super::*;

fn cell(session: &Simulator, scan: &crate::session::observe::Scan) -> (usize, usize) {
    (
        session.correlations_for_scan(scan).len(),
        session.spectral_windows()[scan.spectral_window_id].channel_count,
    )
}

pub(super) fn needed(session: &Simulator) -> bool {
    session.scans().first().is_some_and(|first| {
        session
            .scans()
            .iter()
            .any(|scan| cell(session, scan) != cell(session, first))
    })
}

pub(super) fn write(session: &Simulator) -> SimulatorResult<()> {
    let started = Instant::now();
    let window = ReservationWindow::open();
    let resources = &session.options().resources;
    // Histograms and the two live plan copies are bounded by scan count.
    let _metadata = admit(
        resources.host(),
        resources.policy(),
        &Demand {
            phase: "the simulator's mixed-shape creation plan",
            memory: session.scans().len() as u64 * 2048 + (64 << 10),
        },
    )
    .map_err(SimulatorError::admission)?;
    let planned = PlannedScan::of_all(session.scans())
        .ok_or_else(|| SimulatorError::raised("only planned scans make a new MeasurementSet"))?;
    let mut position = 0;
    let runs = super::patch_ranges::plan(
        session.scans().iter().map(|scan| {
            let first = position;
            position += scan.row_count();
            (first..position, cell(session, scan))
        }),
        resources.host(),
        resources.policy(),
    )
    .map_err(SimulatorError::admission)?;
    if position == 0 {
        return empty(session, &planned);
    }
    let free = free_memory(resources.host(), resources.policy());
    let plan = plan(
        session,
        &runs.value,
        position,
        usize::try_from(free / 2).unwrap_or(usize::MAX),
    )
    .map_err(|error| {
        let required = resources::minimum_writer_bytes_for(|budget| {
            plan(session, &runs.value, position, budget)
        })
        .map_or(u64::MAX, |bytes| (bytes as u64).saturating_mul(2));
        resources::refusal_or(error, resources::WRITER_PHASE, required, free)
    })?;
    let reservation = admit(
        resources.host(),
        resources.policy(),
        &Demand {
            phase: resources::WRITER_PHASE,
            memory: plan.maximum_resident_bytes() as u64 + subtables::generated_bytes(&planned),
        },
    )
    .map_err(SimulatorError::admission)?;
    let generated = subtables::subtable_overrides(session, &planned)?;
    let target = MeasurementSetCreateTarget::prepare(session.path(), true)
        .map_err(write_error("prepare the MeasurementSet"))?;
    let mut ms = create_staged_ms(session, target.staging_path())?;
    let mut writer = MainWriter::start(target.staging_path(), plan.clone())?;
    writer._reservation = reservation;
    let (mut first_scan, mut rows_seen) = (0, 0);
    for (rows, cell) in &runs.value {
        if rows.is_empty() {
            continue;
        }
        let mut last_scan = first_scan;
        while rows_seen < rows.end {
            rows_seen += session.scans()[last_scan].row_count();
            last_scan += 1;
        }
        let scope = first_scan..last_scan;
        let prediction = DataStage::for_scans(
            session,
            *cell,
            resources.prediction_backend(),
            scope.clone(),
            false,
        )?;
        let shape = main_shape(session, &planned[scope.clone()], *cell);
        let admission = resources::admit_pipeline(
            resources,
            &shape,
            prediction.as_ref(),
            plan.clone(),
            Reservation::none(),
        )?;
        writer.variable = Some(*cell);
        run_main(
            session,
            &planned,
            &shape,
            (&admission, prediction),
            &mut writer,
            scope,
            rows.clone(),
        )?;
        first_scan = last_scan;
    }
    let written = writer.finish(&mut ms, position, &generated)?;
    target
        .commit()
        .map_err(write_error("publish the MeasurementSet"))?;
    tracing::info!(
        rows = position,
        runs = runs.value.len(),
        peak_reserved_bytes = window.close(),
        bytes_written = written.bytes_written,
        total_s = started.elapsed().as_secs_f64(),
        "simulation mixed-shape pass"
    );
    Ok(())
}

fn empty(session: &Simulator, planned: &[PlannedScan<'_>]) -> SimulatorResult<()> {
    let resources = &session.options().resources;
    let _reservation = admit(
        resources.host(),
        resources.policy(),
        &Demand {
            phase: resources::WRITER_PHASE,
            memory: subtables::generated_bytes(planned) + (256 << 10),
        },
    )
    .map_err(SimulatorError::admission)?;
    let target = MeasurementSetCreateTarget::prepare(session.path(), true)
        .map_err(write_error("prepare the empty MeasurementSet"))?;
    let mut ms = create_staged_ms(session, target.staging_path())?;
    let generated = subtables::subtable_overrides(session, planned)?;
    // The ordinary empty-table save preserves declared array dimensionality.
    // Streamed TiledShape columns need a populated shape to define a hypercube.
    ms.save_with_column_overrides(&ColumnOverrides::for_row_count(0), &generated)
        .map_err(|error| {
            SimulatorError::raised(format!("save the empty MeasurementSet: {error}"))
        })?;
    target
        .commit()
        .map_err(write_error("publish the empty MeasurementSet"))
}

fn plan(
    session: &Simulator,
    runs: &[super::patch_ranges::Run],
    rows: usize,
    available_bytes: usize,
) -> Result<MeasurementSetWritePlan, casa_ms::MeasurementSetWriteError> {
    let mut counts = BTreeMap::new();
    let mut weights = BTreeMap::new();
    for (range, (correlations, channels)) in runs {
        if !range.is_empty() {
            *counts.entry((*correlations, *channels)).or_insert(0) += range.len();
            *weights.entry(*correlations).or_insert(0) += range.len();
        }
    }
    let visibility: Vec<_> = counts
        .iter()
        .map(|(&(c, h), &n)| MeasurementSetArrayShapePlan::visibility(c, h, n, session.telescope()))
        .collect();
    let category = counts
        .iter()
        .map(|(&(c, h), &n)| {
            MeasurementSetArrayShapePlan::flag_category(c, h, 3, n, session.telescope())
        })
        .collect();
    let weight: Vec<_> = weights
        .iter()
        .map(|(&c, &n)| MeasurementSetArrayShapePlan::weight(c, n, session.telescope()))
        .collect();
    let column = |name: &str, value_type, shapes, storage_manager| MeasurementSetArrayColumnPlan {
        name: name.into(),
        value_type,
        shapes,
        storage_manager,
    };
    let tiled = MeasurementSetColumnStorage::TiledShape;
    let columns = vec![
        column(
            "DATA",
            StreamedTiledShapeValueType::Complex32,
            visibility.clone(),
            tiled,
        ),
        column("FLAG", StreamedTiledShapeValueType::Bool, visibility, tiled),
        column(
            "FLAG_CATEGORY",
            StreamedTiledShapeValueType::Bool,
            category,
            tiled,
        ),
        column(
            "UVW",
            StreamedTiledShapeValueType::Float64,
            vec![MeasurementSetArrayShapePlan {
                cell_shape: vec![3],
                row_count: rows,
                tile_shape: vec![3, 1024],
            }],
            MeasurementSetColumnStorage::TiledColumn,
        ),
        column(
            "WEIGHT",
            StreamedTiledShapeValueType::Float32,
            weight.clone(),
            tiled,
        ),
        column("SIGMA", StreamedTiledShapeValueType::Float32, weight, tiled),
    ];
    MeasurementSetWritePlan::variable_array_creation(
        rows,
        columns,
        standard_main_scalar_column_plans(),
        MeasurementSetWriteResources {
            available_bytes,
            maximum_live_batches: 2,
            tiled_column_buffer_bytes: STREAMING_TILED_COLUMN_BUFFER_BYTES,
        },
    )
}

pub(super) fn send(
    writer: &mut MainWriter,
    mut batch: VisibilityColumns,
    (correlations, channels): (usize, usize),
) -> SimulatorResult<()> {
    let error = write_error("mixed-shape MAIN cells");
    let samples = correlations * channels;
    let mut flags = vec![false; samples];
    let mut weights = vec![0.; correlations];
    for row in 0..batch.row_count() {
        writer
            .session
            .push_complex32_row(
                "DATA",
                &[correlations, channels],
                &batch.data[row * samples..(row + 1) * samples],
            )
            .map_err(&error)?;
        flags.fill(batch.flag_row[row]);
        writer
            .session
            .push_bool_row("FLAG", &[correlations, channels], &flags)
            .map_err(&error)?;
        writer
            .session
            .push_undefined_row("FLAG_CATEGORY")
            .map_err(&error)?;
        writer
            .session
            .push_f64_row("UVW", &[3], &batch.uvw[row])
            .map_err(&error)?;
        for (name, values) in [("WEIGHT", &batch.weight), ("SIGMA", &batch.sigma)] {
            weights.fill(values.get(row).copied().unwrap_or(1.));
            writer
                .session
                .push_f32_row(name, &[correlations], &weights)
                .map_err(&error)?;
        }
    }
    batch.data.clear();
    batch.flag_row.clear();
    batch.uvw.clear();
    batch.weight.clear();
    batch.sigma.clear();
    writer.recycled = Some(batch);
    Ok(())
}
