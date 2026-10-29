// SPDX-License-Identifier: LGPL-3.0-or-later
//! What a variable-shape creation's writers need, for a caller that admits
//! its memory before it plans ([`MeasurementSetWritePlan::variable_array_creation`]):
//! each array column's writer works in its largest row and tile row, and
//! assembles its bands in memory when its share holds them
//! ([`StreamingTiledShapeWriter::resident_bytes`]).

use casa_tables::{
    STREAMING_TILED_COLUMN_BUFFER_BYTES, StreamedTiledShapeCubeLayout, StreamingTiledShapeWriter,
    streamed_scalar_columns_resident_bytes,
};

use super::{
    MeasurementSetArrayColumnPlan, MeasurementSetWriteError, MeasurementSetWritePlan,
    MeasurementSetWriteResources,
};

impl MeasurementSetArrayColumnPlan {
    /// The column's hypercubes, one per cell shape, as its writer lays
    /// them out.
    pub(super) fn layouts(&self) -> Vec<StreamedTiledShapeCubeLayout> {
        self.shapes
            .iter()
            .map(|shape| {
                StreamedTiledShapeCubeLayout::new(
                    shape.cell_shape.clone(),
                    shape.row_count,
                    shape.tile_shape.clone(),
                )
            })
            .collect()
    }
}

impl MeasurementSetWritePlan {
    /// The share each array column of a variable-shape creation of
    /// `array_columns` needs: the least its writer is created with, and the
    /// share from which it assembles its bands in memory, the largest of
    /// any column's (every column gets one share).
    ///
    /// # Errors
    ///
    /// A column's layout its writer refuses.
    pub fn variable_array_column_bytes(
        array_columns: &[MeasurementSetArrayColumnPlan],
    ) -> Result<(usize, usize), MeasurementSetWriteError> {
        array_columns
            .iter()
            .try_fold((0, 0), |(least, buffered), column| {
                let (working, banded) =
                    StreamingTiledShapeWriter::resident_bytes(column.value_type, &column.layouts())
                        .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
                Ok((least.max(working), buffered.max(banded)))
            })
    }

    /// The resources under which [`Self::variable_array_creation`] gives
    /// each of `array_columns` array columns a share of at least
    /// `column_bytes` beside the buffers of `scalar_columns` scalar columns:
    /// it plans writer-owned batches of up to half its streaming budget,
    /// which a creation that pushes its rows to the column writers does not
    /// fill, and splits the rest among the array columns. `None` when the
    /// bytes overflow.
    #[must_use]
    pub fn variable_array_creation_resources(
        array_columns: usize,
        scalar_columns: usize,
        column_bytes: usize,
    ) -> Option<MeasurementSetWriteResources> {
        let live = 2;
        let streaming = column_bytes.checked_mul(array_columns)?.checked_mul(live)?;
        Some(MeasurementSetWriteResources {
            available_bytes: streamed_scalar_columns_resident_bytes(scalar_columns)?
                .checked_add(streaming)?,
            maximum_live_batches: live,
            tiled_column_buffer_bytes: STREAMING_TILED_COLUMN_BUFFER_BYTES,
        })
    }
}
