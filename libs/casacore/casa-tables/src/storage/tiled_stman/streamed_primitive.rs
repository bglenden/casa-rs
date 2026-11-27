// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[derive(Debug, Clone, Copy)]
pub(super) struct StreamedShapeCubeWrite {
    pub(super) bytes: usize,
    pub(super) assemble_seconds: f64,
    pub(super) write_seconds: f64,
}

/// Primitive element type for the streaming tiled-column writer.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum StreamedTiledPrimitiveType {
    /// `TpBool`, stored bit-packed in tiled storage.
    Bool,
    /// `TpFloat`.
    Float32,
    /// `TpDouble`.
    Float64,
}

impl StreamedTiledPrimitiveType {
    pub(super) fn casacore_type(self) -> CasacoreDataType {
        match self {
            Self::Bool => CasacoreDataType::TpBool,
            Self::Float32 => CasacoreDataType::TpFloat,
            Self::Float64 => CasacoreDataType::TpDouble,
        }
    }

    fn elem_size(self) -> usize {
        tile_element_size(self.casacore_type())
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) enum StreamedTiledPrimitiveVariant {
    Column,
    Shape,
}

#[derive(Debug, Clone)]
pub(super) struct StreamedPrimitiveTilePlan {
    pub(super) cell_start: Vec<usize>,
    pub(super) actual_extent: Vec<usize>,
}

/// Completed payload for one streamed primitive tiled column.
#[derive(Debug, Clone)]
pub struct StreamedTiledPrimitiveColumn {
    pub(super) temp_data_path: PathBuf,
    pub(super) row_count: usize,
    pub(super) cell_shape: Vec<usize>,
    pub(super) tile_shape: Vec<usize>,
    pub(super) primitive_type: StreamedTiledPrimitiveType,
    pub(super) variant: StreamedTiledPrimitiveVariant,
    pub(super) bytes_written: usize,
    pub(super) big_endian: bool,
    pub(super) assemble_seconds: f64,
    pub(super) write_seconds: f64,
}

impl StreamedTiledPrimitiveColumn {
    /// Number of completed table rows in the streamed column.
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Number of bytes written to the TSM data file.
    pub fn bytes_written(&self) -> usize {
        self.bytes_written
    }

    /// Time spent packing row cells into tile buffers.
    pub fn assemble_seconds(&self) -> f64 {
        self.assemble_seconds
    }

    /// Time spent writing tile buffers to the temporary TSM file.
    pub fn write_seconds(&self) -> f64 {
        self.write_seconds
    }
}

/// Streaming writer for one sequential primitive tiled column.
///
/// Rows must be pushed in table-row order. Row byte payloads are the unpacked
/// casacore array memory order for `cell_shape`; bool rows use one byte per
/// logical flag and are bit-packed when tile buffers are flushed.
pub struct StreamingTiledPrimitiveWriter {
    pub(super) temp_data_path: PathBuf,
    pub(super) writer: BufWriter<CountingFile>,
    pub(super) row_count: usize,
    pub(super) rows_written: usize,
    pub(super) cell_shape: Vec<usize>,
    pub(super) tile_shape: Vec<usize>,
    pub(super) tile_plans: Vec<StreamedPrimitiveTilePlan>,
    pub(super) rows_per_tile: usize,
    pub(super) bucket_size: usize,
    pub(super) tile_nelem: usize,
    pub(super) tile_buffers: Vec<Vec<u8>>,
    pub(super) primitive_type: StreamedTiledPrimitiveType,
    pub(super) variant: StreamedTiledPrimitiveVariant,
    pub(super) big_endian: bool,
    pub(super) assemble_seconds: f64,
    pub(super) write_seconds: f64,
    pub(super) bytes_written: usize,
    pub(super) logical_write_calls: usize,
    pub(super) max_logical_write_bytes: usize,
    pub(super) tile_row_blocks_flushed: usize,
    pub(super) writer_buffer_bytes: usize,
    pub(super) encoded_row: Vec<u8>,
}

impl StreamingTiledPrimitiveWriter {
    /// Create a sequential `TiledShapeStMan` primitive writer.
    pub fn create_shape(
        temp_data_path: impl Into<PathBuf>,
        row_count: usize,
        cell_shape: Vec<usize>,
        tile_shape: Vec<usize>,
        primitive_type: StreamedTiledPrimitiveType,
        writer_buffer_bytes: usize,
        big_endian: bool,
    ) -> Result<Self, StorageError> {
        Self::create(
            temp_data_path,
            row_count,
            cell_shape,
            tile_shape,
            primitive_type,
            StreamedTiledPrimitiveVariant::Shape,
            writer_buffer_bytes,
            big_endian,
        )
    }

    /// Create a sequential `TiledColumnStMan` primitive writer.
    pub fn create_column(
        temp_data_path: impl Into<PathBuf>,
        row_count: usize,
        cell_shape: Vec<usize>,
        tile_shape: Vec<usize>,
        primitive_type: StreamedTiledPrimitiveType,
        writer_buffer_bytes: usize,
        big_endian: bool,
    ) -> Result<Self, StorageError> {
        Self::create(
            temp_data_path,
            row_count,
            cell_shape,
            tile_shape,
            primitive_type,
            StreamedTiledPrimitiveVariant::Column,
            writer_buffer_bytes,
            big_endian,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn create(
        temp_data_path: impl Into<PathBuf>,
        row_count: usize,
        cell_shape: Vec<usize>,
        tile_shape: Vec<usize>,
        primitive_type: StreamedTiledPrimitiveType,
        variant: StreamedTiledPrimitiveVariant,
        writer_buffer_bytes: usize,
        big_endian: bool,
    ) -> Result<Self, StorageError> {
        if tile_shape.len() != cell_shape.len() + 1 {
            return Err(StorageError::FormatMismatch(format!(
                "streamed primitive tile rank must be cell rank + row axis; cell={cell_shape:?} tile={tile_shape:?}"
            )));
        }
        if tile_shape.contains(&0) {
            return Err(StorageError::FormatMismatch(
                "streamed primitive tile dimensions must be positive".to_string(),
            ));
        }
        if cell_shape.contains(&0) && variant != StreamedTiledPrimitiveVariant::Shape {
            return Err(StorageError::FormatMismatch(
                "streamed primitive TiledColumnStMan cell dimensions must be positive".to_string(),
            ));
        }

        let mut cube_shape = cell_shape.clone();
        cube_shape.push(row_count);
        let tiles_per_dim: Vec<usize> = cube_shape
            .iter()
            .zip(tile_shape.iter())
            .map(|(&cube, &tile)| cube.div_ceil(tile))
            .collect();
        let cell_rank = cell_shape.len();
        let cell_tiles_per_dim = &tiles_per_dim[..cell_rank];
        let non_row_tile_count: usize = cell_tiles_per_dim.iter().product();
        let elem_size = primitive_type.elem_size();
        let rows_per_tile = *tile_shape.last().expect("validated non-empty tile shape");
        let tile_plans = (0..non_row_tile_count)
            .map(|tile_index| {
                let tile_pos = linear_to_nd(tile_index, cell_tiles_per_dim);
                let cell_start: Vec<usize> = tile_pos
                    .iter()
                    .enumerate()
                    .map(|(axis, &pos)| pos * tile_shape[axis])
                    .collect();
                let actual_extent: Vec<usize> = cell_start
                    .iter()
                    .enumerate()
                    .map(|(axis, &start)| std::cmp::min(tile_shape[axis], cell_shape[axis] - start))
                    .collect();
                StreamedPrimitiveTilePlan {
                    cell_start,
                    actual_extent,
                }
            })
            .collect();

        let (bucket_size, _offsets) =
            compute_tile_layout(&[primitive_type.casacore_type()], &tile_shape);
        let tile_nelem: usize = tile_shape.iter().product();
        let tile_buffers = (0..non_row_tile_count)
            .map(|_| vec![0u8; tile_nelem * elem_size])
            .collect();

        let temp_data_path = temp_data_path.into();
        if let Some(parent) = temp_data_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let writer =
            BufWriter::with_capacity(writer_buffer_bytes, CountingFile::create(&temp_data_path)?);
        let encoded_row = vec![0u8; cell_shape.iter().product::<usize>() * elem_size];

        Ok(Self {
            temp_data_path,
            writer,
            row_count,
            rows_written: 0,
            cell_shape,
            tile_shape,
            tile_plans,
            rows_per_tile,
            bucket_size,
            tile_nelem,
            tile_buffers,
            primitive_type,
            variant,
            big_endian,
            assemble_seconds: 0.0,
            write_seconds: 0.0,
            bytes_written: 0,
            logical_write_calls: 0,
            max_logical_write_bytes: 0,
            tile_row_blocks_flushed: 0,
            writer_buffer_bytes,
            encoded_row,
        })
    }

    /// Append one zero-valued row. This avoids touching tile buffers that are
    /// already zero-filled.
    pub fn push_zero_row(&mut self) -> Result<(), StorageError> {
        self.advance_row_after_copy()
    }

    /// Append several zero rows with one bounded writer call.
    pub fn push_zero_rows(&mut self, row_count: usize) -> Result<(), StorageError> {
        self.ensure_additional_rows(row_count)?;
        let mut remaining = row_count;
        while remaining > 0 {
            let row_in_tile = self.rows_written % self.rows_per_tile;
            let rows = remaining.min(self.rows_per_tile - row_in_tile);
            self.rows_written += rows;
            remaining -= rows;
            if self.rows_written.is_multiple_of(self.rows_per_tile) {
                self.flush_tile_row_block()?;
            }
        }
        Ok(())
    }

    /// Append one bool row.
    pub fn push_bool_row(&mut self, values: &[bool]) -> Result<(), StorageError> {
        if self.primitive_type != StreamedTiledPrimitiveType::Bool {
            return Err(StorageError::FormatMismatch(
                "streamed primitive writer was not configured for bool rows".to_string(),
            ));
        }
        let mut encoded = vec![0u8; values.len()];
        for (dst, value) in encoded.iter_mut().zip(values.iter()) {
            *dst = u8::from(*value);
        }
        self.push_encoded_row(&encoded)
    }

    /// Append one bool row with every logical value set to `value`.
    pub fn push_bool_fill_row(&mut self, value: bool) -> Result<(), StorageError> {
        if self.primitive_type != StreamedTiledPrimitiveType::Bool {
            return Err(StorageError::FormatMismatch(
                "streamed primitive writer was not configured for bool rows".to_string(),
            ));
        }
        if !value {
            return self.push_zero_row();
        }
        if self.rows_written >= self.row_count {
            return Err(StorageError::FormatMismatch(format!(
                "streamed primitive writer received too many rows: expected {}",
                self.row_count
            )));
        }
        let assemble_started = Instant::now();
        let row_in_tile = self.rows_written % self.rows_per_tile;
        self.fill_row_into_tile_buffers(row_in_tile, 1);
        self.assemble_seconds += assemble_started.elapsed().as_secs_f64();
        self.advance_row_after_copy()
    }

    /// Append rows whose cells are each filled from the corresponding value.
    pub fn push_bool_fill_rows(&mut self, values: &[bool]) -> Result<(), StorageError> {
        let assemble_started = Instant::now();
        for &value in values {
            self.push_bool_fill_row_untimed(value)?;
        }
        self.assemble_seconds += assemble_started.elapsed().as_secs_f64();
        Ok(())
    }

    /// Append one bool-filled row repeatedly.
    pub fn push_repeated_bool_fill_row(
        &mut self,
        value: bool,
        row_count: usize,
    ) -> Result<(), StorageError> {
        if !value {
            return self.push_zero_rows(row_count);
        }
        self.ensure_additional_rows(row_count)?;
        let assemble_started = Instant::now();
        let mut remaining = row_count;
        while remaining > 0 {
            let row_in_tile = self.rows_written % self.rows_per_tile;
            let rows = remaining.min(self.rows_per_tile - row_in_tile);
            self.fill_rows_into_tile_buffers(row_in_tile, rows, 1);
            self.rows_written += rows;
            remaining -= rows;
            if self.rows_written.is_multiple_of(self.rows_per_tile) {
                self.flush_tile_row_block()?;
            }
        }
        self.assemble_seconds += assemble_started.elapsed().as_secs_f64();
        Ok(())
    }

    /// Append one float row.
    pub fn push_f32_row(&mut self, values: &[f32]) -> Result<(), StorageError> {
        if self.primitive_type != StreamedTiledPrimitiveType::Float32 {
            return Err(StorageError::FormatMismatch(
                "streamed primitive writer was not configured for f32 rows".to_string(),
            ));
        }
        self.encode_f32_row(values)?;
        self.push_current_encoded_row()
    }

    /// Append one f32 row repeatedly without reallocating its encoding.
    pub fn push_repeated_f32_row(
        &mut self,
        values: &[f32],
        row_count: usize,
    ) -> Result<(), StorageError> {
        self.encode_f32_row(values)?;
        let assemble_started = Instant::now();
        self.push_repeated_encoded_rows(row_count)?;
        self.assemble_seconds += assemble_started.elapsed().as_secs_f64();
        Ok(())
    }

    /// Append one double row.
    pub fn push_f64_row(&mut self, values: &[f64]) -> Result<(), StorageError> {
        if self.primitive_type != StreamedTiledPrimitiveType::Float64 {
            return Err(StorageError::FormatMismatch(
                "streamed primitive writer was not configured for f64 rows".to_string(),
            ));
        }
        self.encode_f64_row(values)?;
        self.push_current_encoded_row()
    }

    /// Append a batch of f64 rows without per-row encoding allocations.
    pub fn push_f64_rows<T: AsRef<[f64]>>(&mut self, rows: &[T]) -> Result<(), StorageError> {
        let assemble_started = Instant::now();
        if self.tile_plans.len() == 1
            && self.tile_plans[0]
                .cell_start
                .iter()
                .all(|&start| start == 0)
            && self.tile_plans[0].actual_extent == self.cell_shape
        {
            self.ensure_additional_rows(rows.len())?;
            let row_bytes = self.encoded_row.len();
            let mut source_start = 0usize;
            while source_start < rows.len() {
                let row_in_tile = self.rows_written % self.rows_per_tile;
                let batch_rows = (rows.len() - source_start).min(self.rows_per_tile - row_in_tile);
                for (batch_row, source) in rows[source_start..source_start + batch_rows]
                    .iter()
                    .enumerate()
                {
                    let source = source.as_ref();
                    if source.len() * 8 != row_bytes {
                        return Err(StorageError::FormatMismatch(format!(
                            "streamed primitive f64 row has {} values for {row_bytes} encoded bytes",
                            source.len()
                        )));
                    }
                    let destination_start = (row_in_tile + batch_row) * row_bytes;
                    let destination =
                        &mut self.tile_buffers[0][destination_start..destination_start + row_bytes];
                    if self.big_endian {
                        for (idx, &value) in source.iter().enumerate() {
                            write_f64_be(&mut destination[idx * 8..], value);
                        }
                    } else {
                        destination.copy_from_slice(tile_values_as_bytes(source));
                    }
                }
                self.rows_written += batch_rows;
                source_start += batch_rows;
                if self.rows_written.is_multiple_of(self.rows_per_tile) {
                    self.flush_tile_row_block()?;
                }
            }
        } else {
            for row in rows {
                self.encode_f64_row(row.as_ref())?;
                self.push_current_encoded_row()?;
            }
        }
        self.assemble_seconds += assemble_started.elapsed().as_secs_f64();
        Ok(())
    }

    /// Append one f64 row repeatedly without reallocating its encoding.
    pub fn push_repeated_f64_row(
        &mut self,
        values: &[f64],
        row_count: usize,
    ) -> Result<(), StorageError> {
        self.encode_f64_row(values)?;
        let assemble_started = Instant::now();
        self.push_repeated_encoded_rows(row_count)?;
        self.assemble_seconds += assemble_started.elapsed().as_secs_f64();
        Ok(())
    }

    fn push_bool_fill_row_untimed(&mut self, value: bool) -> Result<(), StorageError> {
        if self.primitive_type != StreamedTiledPrimitiveType::Bool {
            return Err(StorageError::FormatMismatch(
                "streamed primitive writer was not configured for bool rows".to_string(),
            ));
        }
        if !value {
            return self.advance_row_after_copy();
        }
        if self.rows_written >= self.row_count {
            return Err(StorageError::FormatMismatch(format!(
                "streamed primitive writer received too many rows: expected {}",
                self.row_count
            )));
        }
        let row_in_tile = self.rows_written % self.rows_per_tile;
        self.fill_row_into_tile_buffers(row_in_tile, 1);
        self.advance_row_after_copy()
    }

    fn encode_f32_row(&mut self, values: &[f32]) -> Result<(), StorageError> {
        if self.primitive_type != StreamedTiledPrimitiveType::Float32
            || values.len() * 4 != self.encoded_row.len()
        {
            return Err(StorageError::FormatMismatch(format!(
                "streamed primitive f32 row has {} values for {} encoded bytes",
                values.len(),
                self.encoded_row.len()
            )));
        }
        for (idx, &value) in values.iter().enumerate() {
            if self.big_endian {
                write_f32_be(&mut self.encoded_row[idx * 4..], value);
            } else {
                write_f32_le(&mut self.encoded_row[idx * 4..], value);
            }
        }
        Ok(())
    }

    fn encode_f64_row(&mut self, values: &[f64]) -> Result<(), StorageError> {
        if self.primitive_type != StreamedTiledPrimitiveType::Float64
            || values.len() * 8 != self.encoded_row.len()
        {
            return Err(StorageError::FormatMismatch(format!(
                "streamed primitive f64 row has {} values for {} encoded bytes",
                values.len(),
                self.encoded_row.len()
            )));
        }
        for (idx, &value) in values.iter().enumerate() {
            if self.big_endian {
                write_f64_be(&mut self.encoded_row[idx * 8..], value);
            } else {
                write_f64_le(&mut self.encoded_row[idx * 8..], value);
            }
        }
        Ok(())
    }

    fn push_current_encoded_row(&mut self) -> Result<(), StorageError> {
        let encoded = std::mem::take(&mut self.encoded_row);
        let result = self.push_encoded_row(&encoded);
        self.encoded_row = encoded;
        result
    }

    fn push_repeated_encoded_rows(&mut self, row_count: usize) -> Result<(), StorageError> {
        self.ensure_additional_rows(row_count)?;
        if self.tile_plans.len() != 1
            || self.tile_plans[0]
                .cell_start
                .iter()
                .any(|&start| start != 0)
            || self.tile_plans[0].actual_extent != self.cell_shape
        {
            for _ in 0..row_count {
                self.push_current_encoded_row()?;
            }
            return Ok(());
        }

        let row_bytes = self.encoded_row.len();
        let mut remaining = row_count;
        while remaining > 0 {
            let row_in_tile = self.rows_written % self.rows_per_tile;
            let rows = remaining.min(self.rows_per_tile - row_in_tile);
            let start = row_in_tile * row_bytes;
            let end = start + rows * row_bytes;
            self.tile_buffers[0][start..start + row_bytes].copy_from_slice(&self.encoded_row);
            let mut filled_rows = 1usize;
            while filled_rows < rows {
                let copy_rows = filled_rows.min(rows - filled_rows);
                self.tile_buffers[0].copy_within(
                    start..start + copy_rows * row_bytes,
                    start + filled_rows * row_bytes,
                );
                filled_rows += copy_rows;
            }
            debug_assert_eq!(end, start + rows * row_bytes);
            self.rows_written += rows;
            remaining -= rows;
            if self.rows_written.is_multiple_of(self.rows_per_tile) {
                self.flush_tile_row_block()?;
            }
        }
        Ok(())
    }

    fn ensure_additional_rows(&self, row_count: usize) -> Result<(), StorageError> {
        if self.rows_written.saturating_add(row_count) > self.row_count {
            return Err(StorageError::FormatMismatch(format!(
                "streamed primitive writer received too many rows: {} + {row_count}, expected {}",
                self.rows_written, self.row_count
            )));
        }
        Ok(())
    }

    fn push_encoded_row(&mut self, row_bytes: &[u8]) -> Result<(), StorageError> {
        if self.rows_written >= self.row_count {
            return Err(StorageError::FormatMismatch(format!(
                "streamed primitive writer received too many rows: expected {}",
                self.row_count
            )));
        }
        let expected_len =
            self.cell_shape.iter().product::<usize>() * self.primitive_type.elem_size();
        if row_bytes.len() != expected_len {
            return Err(StorageError::FormatMismatch(format!(
                "streamed primitive row has {} bytes, expected {expected_len}",
                row_bytes.len()
            )));
        }

        let assemble_started = Instant::now();
        let row_in_tile = self.rows_written % self.rows_per_tile;
        self.copy_row_into_tile_buffers(row_in_tile, row_bytes);
        self.assemble_seconds += assemble_started.elapsed().as_secs_f64();
        self.advance_row_after_copy()
    }

    fn advance_row_after_copy(&mut self) -> Result<(), StorageError> {
        if self.rows_written >= self.row_count {
            return Err(StorageError::FormatMismatch(format!(
                "streamed primitive writer received too many rows: expected {}",
                self.row_count
            )));
        }
        let row_in_tile = self.rows_written % self.rows_per_tile;
        self.rows_written += 1;
        if row_in_tile + 1 == self.rows_per_tile {
            self.flush_tile_row_block()?;
        }
        Ok(())
    }

    fn copy_row_into_tile_buffers(&mut self, row_in_tile: usize, row_bytes: &[u8]) {
        let cell_rank = self.cell_shape.len();
        let tile_cell_shape = &self.tile_shape[..cell_rank];
        let tile_cell_nelem: usize = tile_cell_shape.iter().product();
        let elem_size = self.primitive_type.elem_size();
        let row_block_bytes = tile_cell_nelem * elem_size;

        for (tile_index, plan) in self.tile_plans.iter().enumerate() {
            let row_slice_start = row_in_tile * row_block_bytes;
            let row_slice_end = row_slice_start + row_block_bytes;
            let row_tile = &mut self.tile_buffers[tile_index][row_slice_start..row_slice_end];
            copy_cube_to_tile(
                row_bytes,
                &self.cell_shape,
                row_tile,
                tile_cell_shape,
                &plan.cell_start,
                &plan.actual_extent,
                elem_size,
            );
        }
    }

    fn fill_row_into_tile_buffers(&mut self, row_in_tile: usize, value: u8) {
        let cell_rank = self.cell_shape.len();
        let tile_cell_shape = &self.tile_shape[..cell_rank];
        let tile_cell_nelem: usize = tile_cell_shape.iter().product();
        let elem_size = self.primitive_type.elem_size();
        let row_block_bytes = tile_cell_nelem * elem_size;

        for (tile_index, plan) in self.tile_plans.iter().enumerate() {
            let row_slice_start = row_in_tile * row_block_bytes;
            let row_slice_end = row_slice_start + row_block_bytes;
            let row_tile = &mut self.tile_buffers[tile_index][row_slice_start..row_slice_end];
            fill_cube_tile_region(
                row_tile,
                tile_cell_shape,
                &plan.actual_extent,
                elem_size,
                value,
            );
        }
    }

    fn fill_rows_into_tile_buffers(&mut self, row_in_tile: usize, row_count: usize, value: u8) {
        let cell_rank = self.cell_shape.len();
        let tile_cell_shape = &self.tile_shape[..cell_rank];
        let tile_cell_nelem: usize = tile_cell_shape.iter().product();
        let row_block_bytes = tile_cell_nelem * self.primitive_type.elem_size();
        for tile in &mut self.tile_buffers {
            let start = row_in_tile * row_block_bytes;
            let end = start + row_count * row_block_bytes;
            tile[start..end].fill(value);
        }
    }

    /// Finish writing and return an installable streamed-column payload.
    pub fn finish(mut self) -> Result<StreamedTiledPrimitiveColumn, StorageError> {
        if self.rows_written != self.row_count {
            return Err(StorageError::FormatMismatch(format!(
                "streamed primitive writer finished with {} rows, expected {}",
                self.rows_written, self.row_count
            )));
        }
        if self.rows_written > 0 && !self.rows_written.is_multiple_of(self.rows_per_tile) {
            self.flush_tile_row_block()?;
        }
        let write_started = Instant::now();
        self.writer.flush()?;
        self.write_seconds += write_started.elapsed().as_secs_f64();
        if trace_streamed_tiled_writes() {
            self.trace_write_stats();
        }

        Ok(StreamedTiledPrimitiveColumn {
            temp_data_path: self.temp_data_path,
            row_count: self.row_count,
            cell_shape: self.cell_shape,
            tile_shape: self.tile_shape,
            primitive_type: self.primitive_type,
            variant: self.variant,
            bytes_written: self.bytes_written,
            big_endian: self.big_endian,
            assemble_seconds: self.assemble_seconds,
            write_seconds: self.write_seconds,
        })
    }

    fn flush_tile_row_block(&mut self) -> Result<(), StorageError> {
        let write_started = Instant::now();
        self.tile_row_blocks_flushed += 1;
        if self.primitive_type == StreamedTiledPrimitiveType::Bool {
            let mut bucket = vec![0u8; self.bucket_size];
            for tile in &self.tile_buffers {
                bucket.fill(0);
                if tile.iter().all(|&value| value == 0) {
                    // Already zero-filled.
                } else if tile.iter().all(|&value| value != 0) {
                    write_bool_bits_fill(&mut bucket, self.tile_nelem);
                } else {
                    write_tile_storage(
                        &mut bucket,
                        self.primitive_type.casacore_type(),
                        tile,
                        self.tile_nelem,
                    );
                }
                self.writer.write_all(&bucket)?;
                self.logical_write_calls += 1;
                self.max_logical_write_bytes = self.max_logical_write_bytes.max(bucket.len());
                self.bytes_written += bucket.len();
            }
        } else {
            for tile in &self.tile_buffers {
                debug_assert_eq!(tile.len(), self.bucket_size);
                self.writer.write_all(tile)?;
                self.logical_write_calls += 1;
                self.max_logical_write_bytes = self.max_logical_write_bytes.max(tile.len());
                self.bytes_written += tile.len();
            }
        }
        self.write_seconds += write_started.elapsed().as_secs_f64();

        for tile in &mut self.tile_buffers {
            tile.fill(0);
        }
        Ok(())
    }

    fn trace_write_stats(&self) {
        let stats = self.writer.get_ref().stats();
        tracing::debug!(
            target: STREAMED_TILED_TRACE,
            "casa-rs streamed tiled write column={} variant={:?} type={:?} rows={} cell_shape={:?} tile_shape={:?} row_tile={} tile_buffers={} tile_row_blocks={} logical_write_calls={} logical_max_write_bytes={} buffered_write_calls={} buffered_max_write_bytes={} buffered_flush_calls={} writer_buffer_bytes={} bytes_written={} assemble_seconds={:.6} write_seconds={:.6}",
            self.temp_data_path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("<unknown>"),
            self.variant,
            self.primitive_type,
            self.rows_written,
            self.cell_shape,
            self.tile_shape,
            self.rows_per_tile,
            self.tile_buffers.len(),
            self.tile_row_blocks_flushed,
            self.logical_write_calls,
            self.max_logical_write_bytes,
            stats.write_calls,
            stats.max_write_bytes,
            stats.flush_calls,
            self.writer_buffer_bytes,
            self.bytes_written,
            self.assemble_seconds,
            self.write_seconds,
        );
    }
}

/// Install a completed streamed primitive column as a `TiledShapeStMan` data manager.
pub fn install_streamed_tiled_shape_primitive_column(
    table_path: &Path,
    dm_seq_nr: u32,
    dm_name: &str,
    streamed: StreamedTiledPrimitiveColumn,
) -> Result<(), StorageError> {
    if streamed.variant != StreamedTiledPrimitiveVariant::Shape {
        return Err(StorageError::FormatMismatch(
            "streamed primitive payload is not a TiledShapeStMan column".to_string(),
        ));
    }
    install_streamed_tiled_primitive_column(table_path, dm_seq_nr, dm_name, streamed)
}

/// Install a completed streamed primitive column as a `TiledColumnStMan` data manager.
pub fn install_streamed_tiled_column_primitive_column(
    table_path: &Path,
    dm_seq_nr: u32,
    dm_name: &str,
    streamed: StreamedTiledPrimitiveColumn,
) -> Result<(), StorageError> {
    if streamed.variant != StreamedTiledPrimitiveVariant::Column {
        return Err(StorageError::FormatMismatch(
            "streamed primitive payload is not a TiledColumnStMan column".to_string(),
        ));
    }
    install_streamed_tiled_primitive_column(table_path, dm_seq_nr, dm_name, streamed)
}

pub(super) fn install_streamed_tiled_primitive_column(
    table_path: &Path,
    dm_seq_nr: u32,
    dm_name: &str,
    streamed: StreamedTiledPrimitiveColumn,
) -> Result<(), StorageError> {
    std::fs::create_dir_all(table_path)?;
    let tsm_path = tsm_data_path(table_path, dm_seq_nr, 0);
    match std::fs::rename(&streamed.temp_data_path, &tsm_path) {
        Ok(()) => {}
        Err(_) => {
            std::fs::copy(&streamed.temp_data_path, &tsm_path)?;
            std::fs::remove_file(&streamed.temp_data_path)?;
        }
    }

    let mut cube_shape = streamed.cell_shape.clone();
    cube_shape.push(streamed.row_count);
    let data_type = streamed.primitive_type.casacore_type();
    let files = if streamed.row_count == 0 {
        vec![]
    } else {
        vec![Some(TsmFileInfo {
            seq_nr: 0,
            length: streamed.bytes_written as i64,
        })]
    };
    let real_cube = TsmCubeInfo {
        values: RecordValue::default(),
        extensible: true,
        cube_shape: cube_shape.clone(),
        tile_shape: streamed.tile_shape.clone(),
        file_seq_nr: if streamed.row_count == 0 { -1 } else { 0 },
        file_offset: 0,
    };
    let header_path = table_path.join(format!("table.f{dm_seq_nr}"));

    match streamed.variant {
        StreamedTiledPrimitiveVariant::Column => {
            let header = TiledStManHeader {
                big_endian: streamed.big_endian,
                seq_nr: dm_seq_nr,
                nrrow: streamed.row_count as u64,
                col_data_types: vec![data_type],
                hypercolumn_name: dm_name.to_string(),
                max_cache_size: 0,
                nrdim: (streamed.cell_shape.len() + 1) as u32,
                files,
                cubes: if streamed.row_count == 0 {
                    vec![]
                } else {
                    vec![real_cube]
                },
            };
            let variant = TiledVariant::Column {
                default_tile_shape: streamed
                    .tile_shape
                    .iter()
                    .map(|&value| value as i32)
                    .collect(),
            };
            write_tiled_header(&header_path, &variant, &header)?;
        }
        StreamedTiledPrimitiveVariant::Shape => {
            let header = TiledStManHeader {
                big_endian: streamed.big_endian,
                seq_nr: dm_seq_nr,
                nrrow: streamed.row_count as u64,
                col_data_types: vec![data_type],
                hypercolumn_name: dm_name.to_string(),
                max_cache_size: 0,
                nrdim: (streamed.cell_shape.len() + 1) as u32,
                files,
                cubes: if streamed.row_count == 0 {
                    vec![TsmCubeInfo {
                        values: RecordValue::default(),
                        extensible: false,
                        cube_shape: vec![],
                        tile_shape: vec![],
                        file_seq_nr: -1,
                        file_offset: 0,
                    }]
                } else {
                    vec![
                        TsmCubeInfo {
                            values: RecordValue::default(),
                            extensible: false,
                            cube_shape: vec![],
                            tile_shape: vec![],
                            file_seq_nr: -1,
                            file_offset: 0,
                        },
                        real_cube,
                    ]
                },
            };
            let variant = TiledVariant::Shape {
                default_tile_shape: streamed
                    .tile_shape
                    .iter()
                    .map(|&value| value as i32)
                    .collect(),
                nr_used_row_map: if streamed.row_count == 0 { 0 } else { 1 },
                row_map: if streamed.row_count == 0 {
                    vec![]
                } else {
                    vec![(streamed.row_count - 1) as u32]
                },
                cube_map: if streamed.row_count == 0 {
                    vec![]
                } else {
                    vec![1]
                },
                pos_map: if streamed.row_count == 0 {
                    vec![]
                } else {
                    vec![(streamed.row_count - 1) as u32]
                },
            };
            write_tiled_header(&header_path, &variant, &header)?;
        }
    }

    invalidate_shared_tile_cache_for_table(table_path);
    Ok(())
}
