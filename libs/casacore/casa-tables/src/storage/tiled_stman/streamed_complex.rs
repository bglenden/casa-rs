// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// Completed payload for one streamed `TiledShapeStMan` Complex32 column.
///
/// This is a narrow high-throughput write primitive for MeasurementSet-like
/// writers that produce rows in table order and want to overlap simulation or
/// transformation work with tiled-column I/O.  The caller streams row cells into
/// a temporary TSM data file, saves the rest of the table normally, then
/// installs this payload over the placeholder tiled manager written by the
/// table save path.
#[derive(Debug, Clone)]
pub struct StreamedTiledShapeComplex32Column {
    pub(super) temp_data_path: PathBuf,
    pub(super) row_count: usize,
    pub(super) cell_shape: Vec<usize>,
    pub(super) tile_shape: Vec<usize>,
    pub(super) bytes_written: usize,
    pub(super) big_endian: bool,
    pub(super) assemble_seconds: f64,
    pub(super) write_seconds: f64,
}

impl StreamedTiledShapeComplex32Column {
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

/// Streaming writer for one sequential `TiledShapeStMan` Complex32 column.
///
/// Rows must be pushed in table-row order, with each row stored in casacore
/// array memory order for the supplied `cell_shape`.  For MS visibility cells
/// that means correlation is the fastest-varying axis: `[corr, chan]`.
pub struct StreamingTiledShapeComplex32Writer {
    pub(super) temp_data_path: PathBuf,
    /// The thread that writes filled bands while the next is laid out;
    /// taken when the writer finishes.
    pub(super) bands: Option<band_writer::BandWriter>,
    pub(super) row_count: usize,
    pub(super) rows_written: usize,
    pub(super) cell_shape: Vec<usize>,
    pub(super) tile_shape: Vec<usize>,
    pub(super) tile_plans: Vec<StreamedPrimitiveTilePlan>,
    pub(super) rows_per_tile: usize,
    pub(super) bucket_size: usize,
    pub(super) tile_buffers: Vec<Vec<u8>>,
    pub(super) big_endian: bool,
    pub(super) assemble_seconds: f64,
    pub(super) write_seconds: f64,
    pub(super) bytes_written: usize,
    pub(super) logical_write_calls: usize,
    pub(super) max_logical_write_bytes: usize,
    pub(super) tile_row_blocks_flushed: usize,
    pub(super) writer_buffer_bytes: usize,
}

impl StreamingTiledShapeComplex32Writer {
    /// Create a sequential Complex32 tiled-shape writer.
    ///
    /// The first two axes of `cell_shape` are currently required to be
    /// `[corr, chan]`, matching MeasurementSet DATA-like columns.  `tile_shape`
    /// must include the row axis as its final dimension, for example CASA's
    /// common visibility tile shape `[4, 128, 256]`.
    ///
    /// The writer holds one band of tiles (one row of tiles), written on a
    /// thread of its own while the caller waits.
    pub fn create(
        temp_data_path: impl Into<PathBuf>,
        row_count: usize,
        cell_shape: Vec<usize>,
        tile_shape: Vec<usize>,
        writer_buffer_bytes: usize,
        big_endian: bool,
    ) -> Result<Self, StorageError> {
        let target = (
            temp_data_path.into(),
            row_count,
            writer_buffer_bytes,
            big_endian,
        );
        Self::with_bands(target, cell_shape, tile_shape, false)
    }

    /// [`Self::create`], holding two bands: the caller lays rows into one
    /// while the other is written, so laying rows out takes nothing off
    /// the rate the file is written at (#855).
    pub fn create_overlapped(
        temp_data_path: impl Into<PathBuf>,
        row_count: usize,
        cell_shape: Vec<usize>,
        tile_shape: Vec<usize>,
        writer_buffer_bytes: usize,
        big_endian: bool,
    ) -> Result<Self, StorageError> {
        let target = (
            temp_data_path.into(),
            row_count,
            writer_buffer_bytes,
            big_endian,
        );
        Self::with_bands(target, cell_shape, tile_shape, true)
    }

    /// The writer of [`Self::create`], with a second band when `overlapped`.
    fn with_bands(
        (temp_data_path, row_count, writer_buffer_bytes, big_endian): (PathBuf, usize, usize, bool),
        cell_shape: Vec<usize>,
        tile_shape: Vec<usize>,
        overlapped: bool,
    ) -> Result<Self, StorageError> {
        if cell_shape.len() != 2 {
            return Err(StorageError::FormatMismatch(format!(
                "streamed Complex32 TiledShape writer currently supports rank-2 cells, got rank {}",
                cell_shape.len()
            )));
        }
        if tile_shape.len() != cell_shape.len() + 1 {
            return Err(StorageError::FormatMismatch(format!(
                "streamed TiledShape tile rank must be cell rank + row axis; cell={cell_shape:?} tile={tile_shape:?}"
            )));
        }
        if cell_shape.contains(&0) || tile_shape.contains(&0) {
            return Err(StorageError::FormatMismatch(
                "streamed TiledShape dimensions must be positive".to_string(),
            ));
        }

        let mut cube_shape = cell_shape.clone();
        cube_shape.push(row_count);
        let tiles_per_dim: Vec<usize> = cube_shape
            .iter()
            .zip(tile_shape.iter())
            .map(|(&cube, &tile)| cube.div_ceil(tile))
            .collect();
        let rows_per_tile = *tile_shape.last().expect("validated non-empty tile shape");
        let cell_rank = cell_shape.len();
        let cell_tiles_per_dim = &tiles_per_dim[..cell_rank];
        let non_row_tile_count: usize = cell_tiles_per_dim.iter().product();
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
            compute_tile_layout(&[CasacoreDataType::TpComplex], &tile_shape);
        let tile_buffers: Vec<Vec<u8>> = (0..non_row_tile_count)
            .map(|_| vec![0u8; bucket_size])
            .collect();

        if let Some(parent) = temp_data_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let writer =
            BufWriter::with_capacity(writer_buffer_bytes, CountingFile::create(&temp_data_path)?);
        let spare = overlapped.then(|| tile_buffers.clone());

        Ok(Self {
            temp_data_path,
            bands: Some(band_writer::BandWriter::start(writer, spare)?),
            row_count,
            rows_written: 0,
            cell_shape,
            tile_shape,
            tile_plans,
            rows_per_tile,
            bucket_size,
            tile_buffers,
            big_endian,
            assemble_seconds: 0.0,
            write_seconds: 0.0,
            bytes_written: 0,
            logical_write_calls: 0,
            max_logical_write_bytes: 0,
            tile_row_blocks_flushed: 0,
            writer_buffer_bytes,
        })
    }

    /// Append one row cell in table-row order.
    pub fn push_row(&mut self, values: &[Complex32]) -> Result<(), StorageError> {
        let assemble_started = Instant::now();
        self.push_row_untimed(values)?;
        self.assemble_seconds += assemble_started.elapsed().as_secs_f64();
        Ok(())
    }

    /// Append row cells stored back to back, with one timing and dispatch
    /// boundary.
    pub fn push_rows(&mut self, rows: &[Complex32]) -> Result<(), StorageError> {
        let cell_values: usize = self.cell_shape.iter().product();
        if cell_values == 0 || !rows.len().is_multiple_of(cell_values) {
            return Err(StorageError::FormatMismatch(format!(
                "streamed TiledShape rows hold {} values, not a whole number of {cell_values}-value cells",
                rows.len()
            )));
        }
        let assemble_started = Instant::now();
        for row in rows.chunks_exact(cell_values) {
            self.push_row_untimed(row)?;
        }
        self.assemble_seconds += assemble_started.elapsed().as_secs_f64();
        Ok(())
    }

    /// Append one row cell repeatedly with one timing and dispatch boundary.
    pub fn push_repeated_row(
        &mut self,
        values: &[Complex32],
        row_count: usize,
    ) -> Result<(), StorageError> {
        if self.rows_written.saturating_add(row_count) > self.row_count {
            return Err(StorageError::FormatMismatch(format!(
                "streamed TiledShape writer received too many rows: {} + {row_count}, expected {}",
                self.rows_written, self.row_count
            )));
        }
        let expected_values: usize = self.cell_shape.iter().product();
        if values.len() != expected_values {
            return Err(StorageError::FormatMismatch(format!(
                "streamed TiledShape row has {} values, expected {expected_values} for shape {:?}",
                values.len(),
                self.cell_shape
            )));
        }
        let assemble_started = Instant::now();
        let row_bytes = self.bucket_size / self.rows_per_tile;
        let mut remaining = row_count;
        while remaining > 0 {
            let row_in_tile = self.rows_written % self.rows_per_tile;
            let rows = remaining.min(self.rows_per_tile - row_in_tile);
            self.copy_complex32_row_into_tile_buffers(row_in_tile, values)?;
            for tile in &mut self.tile_buffers {
                let source_start = row_in_tile * row_bytes;
                let mut filled_rows = 1usize;
                while filled_rows < rows {
                    let copy_rows = filled_rows.min(rows - filled_rows);
                    tile.copy_within(
                        source_start..source_start + copy_rows * row_bytes,
                        source_start + filled_rows * row_bytes,
                    );
                    filled_rows += copy_rows;
                }
            }
            self.rows_written += rows;
            remaining -= rows;
            if self.rows_written.is_multiple_of(self.rows_per_tile) {
                self.flush_tile_row_block()?;
            }
        }
        self.assemble_seconds += assemble_started.elapsed().as_secs_f64();
        Ok(())
    }

    fn push_row_untimed(&mut self, values: &[Complex32]) -> Result<(), StorageError> {
        if self.rows_written >= self.row_count {
            return Err(StorageError::FormatMismatch(format!(
                "streamed TiledShape writer received too many rows: expected {}",
                self.row_count
            )));
        }
        let expected_values: usize = self.cell_shape.iter().product();
        if values.len() != expected_values {
            return Err(StorageError::FormatMismatch(format!(
                "streamed TiledShape row has {} values, expected {} for shape {:?}",
                values.len(),
                expected_values,
                self.cell_shape
            )));
        }

        let row_in_tile = self.rows_written % self.rows_per_tile;
        self.copy_complex32_row_into_tile_buffers(row_in_tile, values)?;
        self.rows_written += 1;

        if row_in_tile + 1 == self.rows_per_tile {
            self.flush_tile_row_block()?;
        }
        Ok(())
    }

    /// Finish writing and return an installable streamed-column payload.
    pub fn finish(mut self) -> Result<StreamedTiledShapeComplex32Column, StorageError> {
        if self.rows_written != self.row_count {
            return Err(StorageError::FormatMismatch(format!(
                "streamed TiledShape writer finished with {} rows, expected {}",
                self.rows_written, self.row_count
            )));
        }
        if self.rows_written > 0 && !self.rows_written.is_multiple_of(self.rows_per_tile) {
            self.flush_tile_row_block()?;
        }
        let bands = self
            .bands
            .take()
            .expect("the writer finishes once")
            .finish()?;
        let (mut writer, written) = bands;
        let write_started = Instant::now();
        writer.flush()?;
        self.write_seconds += written.write_seconds + write_started.elapsed().as_secs_f64();
        self.bytes_written += written.bytes_written;
        self.logical_write_calls += written.logical_write_calls;
        self.max_logical_write_bytes = written.max_logical_write_bytes;
        if trace_streamed_tiled_writes() {
            self.trace_write_stats(&writer.get_ref().stats());
        }

        Ok(StreamedTiledShapeComplex32Column {
            temp_data_path: self.temp_data_path,
            row_count: self.row_count,
            cell_shape: self.cell_shape,
            tile_shape: self.tile_shape,
            bytes_written: self.bytes_written,
            big_endian: self.big_endian,
            assemble_seconds: self.assemble_seconds,
            write_seconds: self.write_seconds,
        })
    }

    fn copy_complex32_row_into_tile_buffers(
        &mut self,
        row_in_tile: usize,
        values: &[Complex32],
    ) -> Result<(), StorageError> {
        let corr_count = self.cell_shape[0];
        let chan_count = self.cell_shape[1];
        let tile_corr = self.tile_shape[0];
        let tile_chan = self.tile_shape[1];
        let tile_row_stride = tile_corr * tile_chan;

        for (tile_index, plan) in self.tile_plans.iter().enumerate() {
            let corr_start = plan.cell_start[0];
            let chan_start = plan.cell_start[1];
            if corr_start >= corr_count || chan_start >= chan_count {
                continue;
            }
            let corr_extent = plan.actual_extent[0];
            let chan_extent = plan.actual_extent[1];
            let tile = &mut self.tile_buffers[tile_index];

            if !self.big_endian && tile_corr == corr_count && corr_start == 0 {
                let src_start = chan_start * corr_count;
                let src_end = (chan_start + chan_extent) * corr_count;
                let dst_start = row_in_tile * tile_row_stride * std::mem::size_of::<Complex32>();
                let dst_end = dst_start + std::mem::size_of_val(&values[src_start..src_end]);
                copy_complex32_native_bytes(
                    &mut tile[dst_start..dst_end],
                    &values[src_start..src_end],
                );
                continue;
            }

            for chan_offset in 0..chan_extent {
                let channel = chan_start + chan_offset;
                for corr_offset in 0..corr_extent {
                    let corr = corr_start + corr_offset;
                    let src_idx = channel * corr_count + corr;
                    let tile_idx =
                        row_in_tile * tile_row_stride + chan_offset * tile_corr + corr_offset;
                    let dst_start = tile_idx * 8;
                    write_complex32_component_bytes(
                        &mut tile[dst_start..dst_start + 8],
                        values[src_idx],
                        self.big_endian,
                    );
                }
            }
        }

        Ok(())
    }

    /// Hand the filled band to the band writer ([`band_writer`]) and lay the
    /// next rows into the band it wrote before, cleared.
    fn flush_tile_row_block(&mut self) -> Result<(), StorageError> {
        self.tile_row_blocks_flushed += 1;
        let band = std::mem::take(&mut self.tile_buffers);
        let mut written = self
            .bands
            .as_mut()
            .expect("bands are written only before the writer finishes")
            .swap(band)?;
        for tile in &mut written {
            tile.fill(0);
        }
        self.tile_buffers = written;
        Ok(())
    }

    fn trace_write_stats(&self, stats: &CountingWriteStats) {
        tracing::debug!(
            target: STREAMED_TILED_TRACE,
            "casa-rs streamed tiled write column={} variant=Shape type=Complex32 rows={} cell_shape={:?} tile_shape={:?} row_tile={} tile_buffers={} tile_row_blocks={} logical_write_calls={} logical_max_write_bytes={} buffered_write_calls={} buffered_max_write_bytes={} buffered_flush_calls={} writer_buffer_bytes={} bytes_written={} assemble_seconds={:.6} write_seconds={:.6}",
            self.temp_data_path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("<unknown>"),
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

/// Install a completed streamed Complex32 column as a one-shape
/// `TiledShapeStMan` data manager.
pub fn install_streamed_tiled_shape_complex32_column(
    table_path: &Path,
    dm_seq_nr: u32,
    dm_name: &str,
    streamed: StreamedTiledShapeComplex32Column,
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
    let header = TiledStManHeader {
        big_endian: streamed.big_endian,
        seq_nr: dm_seq_nr,
        nrrow: streamed.row_count as u64,
        col_data_types: vec![CasacoreDataType::TpComplex],
        hypercolumn_name: dm_name.to_string(),
        max_cache_size: 0,
        nrdim: (streamed.cell_shape.len() + 1) as u32,
        files: if streamed.row_count == 0 {
            vec![]
        } else {
            vec![Some(TsmFileInfo {
                seq_nr: 0,
                length: streamed.bytes_written as i64,
            })]
        },
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
                TsmCubeInfo {
                    values: RecordValue::default(),
                    extensible: true,
                    cube_shape,
                    tile_shape: streamed.tile_shape.clone(),
                    file_seq_nr: 0,
                    file_offset: 0,
                },
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
    let header_path = table_path.join(format!("table.f{dm_seq_nr}"));
    write_tiled_header(&header_path, &variant, &header)?;
    invalidate_shared_tile_cache_for_table(table_path);
    Ok(())
}
