// SPDX-License-Identifier: LGPL-3.0-or-later
//! Streamed MAIN-table array columns for synthetic observations.

use std::collections::BTreeMap;
use std::path::Path;

use casa_tables::StreamedTiledShapeValueType;
use num_complex::Complex32;

use super::SyntheticObservationRequest;
use crate::error::{MsError, MsResult};
use crate::write_session::{
    MeasurementSetArrayColumnPlan, MeasurementSetArrayShapePlan, MeasurementSetColumnStorage,
    MeasurementSetWriteBatch, MeasurementSetWriteError, MeasurementSetWritePlan,
    MeasurementSetWriteResources, MeasurementSetWriteSession, standard_main_scalar_column_plans,
};

/// Streamed MAIN array columns (`DATA`, `FLAG`, `FLAG_CATEGORY`, `UVW`,
/// `WEIGHT`, `SIGMA`) plus the scalar-column session they share.
///
/// When every window has one channel count, all rows share one cell shape and
/// stream through the bounded background visibility writer. Mixed channel
/// counts need one `TiledShapeStMan` hypercube per shape, which only the
/// variable-shape writer provides; it writes each row directly and is several
/// times slower, so it is used only when the shapes require it.
pub(super) struct MainArrayWriter {
    pub(super) session: MeasurementSetWriteSession,
    route: MainArrayRoute,
}

enum MainArrayRoute {
    UniformShape,
    MixedShapes { unit_weights: Vec<f32> },
}

impl MainArrayWriter {
    /// Plan and start the writers for `rows_per_window` rows in every window.
    pub(super) fn start(
        request: &SyntheticObservationRequest,
        rows_per_window: usize,
        output: &Path,
    ) -> MsResult<Self> {
        let io_error =
            |error: MeasurementSetWriteError| MsError::SyntheticObservation(error.to_string());
        let correlation_count = request.polarization_setup.correlation_count;
        let telescope_name = request.telescope_name.as_str();
        let row_count = rows_per_window * request.spectral_windows.len();
        let mut rows_by_channel_count = BTreeMap::<usize, usize>::new();
        for window in &request.spectral_windows {
            *rows_by_channel_count
                .entry(window.channel_count)
                .or_default() += rows_per_window;
        }
        let resources = MeasurementSetWriteResources::from_system_memory(2).map_err(io_error)?;
        let (plan, route) = if let [channel_count] =
            rows_by_channel_count.keys().copied().collect::<Vec<_>>()[..]
        {
            let plan = MeasurementSetWritePlan::visibility_creation(
                row_count,
                correlation_count,
                channel_count,
                telescope_name,
                resources,
            )
            .map_err(io_error)?;
            (plan, MainArrayRoute::UniformShape)
        } else {
            let plan = mixed_shape_write_plan(
                correlation_count,
                telescope_name,
                row_count,
                &rows_by_channel_count,
                resources,
            )
            .map_err(io_error)?;
            let unit_weights = vec![1.0f32; correlation_count];
            (plan, MainArrayRoute::MixedShapes { unit_weights })
        };
        let session = MeasurementSetWriteSession::start(output, plan).map_err(io_error)?;
        Ok(Self { session, route })
    }

    /// Append one window's rows with unit weights and an empty `FLAG_CATEGORY`.
    pub(super) fn push_rows(
        &mut self,
        cell_shape: [usize; 2],
        data_rows: Vec<Vec<Complex32>>,
        flag_rows: &[bool],
        uvw_rows: &[[f64; 3]],
    ) -> MsResult<()> {
        let io_error =
            |error: MeasurementSetWriteError| MsError::SyntheticObservation(error.to_string());
        let unit_weights = match &self.route {
            MainArrayRoute::UniformShape => {
                return self
                    .session
                    .send_batch(MeasurementSetWriteBatch::Rows {
                        data_rows,
                        flag_rows: flag_rows.to_vec(),
                        uvw_rows: uvw_rows.to_vec(),
                    })
                    .map_err(io_error);
            }
            MainArrayRoute::MixedShapes { unit_weights } => unit_weights,
        };
        let sample_count = cell_shape[0] * cell_shape[1];
        let flagged = vec![true; sample_count];
        let unflagged = vec![false; sample_count];
        let weight_shape = [unit_weights.len()];
        let session = &mut self.session;
        for ((data, &flag_row), uvw) in data_rows.iter().zip(flag_rows).zip(uvw_rows) {
            let flags = if flag_row { &flagged } else { &unflagged };
            session
                .push_complex32_row("DATA", &cell_shape, data)
                .map_err(io_error)?;
            session
                .push_bool_row("FLAG", &cell_shape, flags)
                .map_err(io_error)?;
            session
                .push_undefined_row("FLAG_CATEGORY")
                .map_err(io_error)?;
            session.push_f64_row("UVW", &[3], uvw).map_err(io_error)?;
            session
                .push_f32_row("WEIGHT", &weight_shape, unit_weights)
                .map_err(io_error)?;
            session
                .push_f32_row("SIGMA", &weight_shape, unit_weights)
                .map_err(io_error)?;
        }
        Ok(())
    }
}

/// Variable-shape plan with one visibility hypercube per distinct channel count.
fn mixed_shape_write_plan(
    correlation_count: usize,
    telescope_name: &str,
    row_count: usize,
    rows_by_channel_count: &BTreeMap<usize, usize>,
    resources: MeasurementSetWriteResources,
) -> Result<MeasurementSetWritePlan, MeasurementSetWriteError> {
    let visibility = rows_by_channel_count
        .iter()
        .map(|(&channel_count, &rows)| {
            MeasurementSetArrayShapePlan::visibility(
                correlation_count,
                channel_count,
                rows,
                telescope_name,
            )
        })
        .collect::<Vec<_>>();
    let flag_category = rows_by_channel_count
        .iter()
        .map(|(&channel_count, &rows)| {
            MeasurementSetArrayShapePlan::flag_category(
                correlation_count,
                channel_count,
                1,
                rows,
                telescope_name,
            )
        })
        .collect();
    let weight = MeasurementSetArrayShapePlan::weight(correlation_count, row_count, telescope_name);
    let uvw = MeasurementSetArrayShapePlan {
        cell_shape: vec![3],
        row_count,
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
    let columns = vec![
        column(
            "DATA",
            StreamedTiledShapeValueType::Complex32,
            visibility.clone(),
            MeasurementSetColumnStorage::TiledShape,
        ),
        column(
            "FLAG",
            StreamedTiledShapeValueType::Bool,
            visibility,
            MeasurementSetColumnStorage::TiledShape,
        ),
        column(
            "FLAG_CATEGORY",
            StreamedTiledShapeValueType::Bool,
            flag_category,
            MeasurementSetColumnStorage::TiledShape,
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
            vec![weight.clone()],
            MeasurementSetColumnStorage::TiledShape,
        ),
        column(
            "SIGMA",
            StreamedTiledShapeValueType::Float32,
            vec![weight],
            MeasurementSetColumnStorage::TiledShape,
        ),
    ];
    MeasurementSetWritePlan::variable_array_creation(
        row_count,
        columns,
        standard_main_scalar_column_plans(),
        resources,
    )
}
