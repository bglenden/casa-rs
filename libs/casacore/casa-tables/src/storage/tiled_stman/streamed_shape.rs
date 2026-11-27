// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// Element type for a bounded, heterogeneous `TiledShapeStMan` column.
///
/// This is intentionally a storage-side construction type. Table users keep
/// using row, column, and cell accessors; the type exists so higher-level
/// writers can build a canonical casacore data-manager payload without first
/// materializing every array cell.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum StreamedTiledShapeValueType {
    Bool,
    Int32,
    Float32,
    Float64,
    Complex32,
}

impl StreamedTiledShapeValueType {
    pub(super) fn casacore_type(self) -> CasacoreDataType {
        match self {
            Self::Bool => CasacoreDataType::TpBool,
            Self::Int32 => CasacoreDataType::TpInt,
            Self::Float32 => CasacoreDataType::TpFloat,
            Self::Float64 => CasacoreDataType::TpDouble,
            Self::Complex32 => CasacoreDataType::TpComplex,
        }
    }

    pub(super) fn elem_size(self) -> usize {
        tile_element_size(self.casacore_type())
    }
}

/// Predeclared layout for one distinct cell shape in a streamed shape column.
#[doc(hidden)]
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StreamedTiledShapeCubeLayout {
    pub(super) cell_shape: Vec<usize>,
    pub(super) row_count: usize,
    pub(super) tile_shape: Vec<usize>,
}

impl StreamedTiledShapeCubeLayout {
    pub fn new(cell_shape: Vec<usize>, row_count: usize, tile_shape: Vec<usize>) -> Self {
        Self {
            cell_shape,
            row_count,
            tile_shape,
        }
    }

    pub fn cell_shape(&self) -> &[usize] {
        &self.cell_shape
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    pub fn tile_shape(&self) -> &[usize] {
        &self.tile_shape
    }
}

#[derive(Debug, Clone)]
pub(super) struct DirectShapeCubePayload {
    pub(super) temp_data_path: PathBuf,
    pub(super) cell_shape: Vec<usize>,
    pub(super) row_count: usize,
    pub(super) tile_shape: Vec<usize>,
    pub(super) bytes_written: usize,
}

/// Completed canonical heterogeneous `TiledShapeStMan` payload.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct StreamedTiledShapeColumn {
    pub(super) row_count: usize,
    pub(super) value_type: StreamedTiledShapeValueType,
    pub(super) big_endian: bool,
    pub(super) cell_rank: usize,
    pub(super) default_tile_shape: Vec<usize>,
    pub(super) cubes: Vec<DirectShapeCubePayload>,
    pub(super) row_map: Vec<u32>,
    pub(super) cube_map: Vec<u32>,
    pub(super) pos_map: Vec<u32>,
    pub(super) max_resident_bytes: usize,
    pub(super) assemble_seconds: f64,
    pub(super) write_seconds: f64,
}

impl StreamedTiledShapeColumn {
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    pub(crate) fn matches_descriptor(&self, desc: &ColumnDescContents) -> bool {
        desc.is_array
            && desc.shape.is_empty()
            && desc.data_type == self.value_type.casacore_type()
            && (self.cell_rank == 0 || desc.nrdim < 1 || desc.nrdim as usize == self.cell_rank)
    }

    pub fn bytes_written(&self) -> usize {
        self.cubes.iter().map(|cube| cube.bytes_written).sum()
    }

    pub fn max_resident_bytes(&self) -> usize {
        self.max_resident_bytes
    }

    pub fn shape_count(&self) -> usize {
        self.cubes.len()
    }

    pub fn assemble_seconds(&self) -> f64 {
        self.assemble_seconds
    }

    pub fn write_seconds(&self) -> f64 {
        self.write_seconds
    }
}

/// Direct, bounded writer for one heterogeneous `TiledShapeStMan` column.
///
/// The shape histogram is immutable. Every distinct shape maps to exactly one
/// extensible hypercube, matching casacore's `TiledShapeStMan` convention.
/// Each row is written once to its final tile offsets; no full-payload spool,
/// snapshot, or second assembly pass is used.
#[doc(hidden)]
#[derive(Debug)]
pub struct StreamingTiledShapeWriter {
    pub(super) value_type: StreamedTiledShapeValueType,
    pub(super) big_endian: bool,
    pub(super) cell_rank: Option<usize>,
    pub(super) default_tile_shape: Vec<usize>,
    pub(super) row_count: usize,
    pub(super) rows_written: usize,
    pub(super) shape_to_cube: HashMap<Vec<usize>, usize>,
    pub(super) cubes: Vec<DirectShapeCubeWriter>,
    /// The tile-row scratch every cube writes its rows through, sized for
    /// the largest cube's tile row.
    pub(super) tile_row: Vec<u8>,
    pub(super) row_map: Vec<u32>,
    pub(super) cube_map: Vec<u32>,
    pub(super) pos_map: Vec<u32>,
    pub(super) memory_budget_bytes: usize,
    pub(super) working_bytes: usize,
    pub(super) max_resident_bytes: usize,
    pub(super) assemble_seconds: f64,
    pub(super) write_seconds: f64,
}

impl StreamingTiledShapeWriter {
    /// Set the manager's default tile for future hypercubes. The explicit
    /// layout of each existing cube remains the one passed at creation.
    pub fn set_default_tile_shape(&mut self, default: Vec<usize>) -> Result<(), StorageError> {
        if default.len() != self.default_tile_shape.len()
            || default.iter().any(|&extent| extent > i32::MAX as usize)
        {
            return Err(StorageError::FormatMismatch(
                "streamed TiledShape default tile has an invalid rank or extent".into(),
            ));
        }
        self.default_tile_shape = default;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create(
        temp_data_path: impl Into<PathBuf>,
        value_type: StreamedTiledShapeValueType,
        layouts: Vec<StreamedTiledShapeCubeLayout>,
        memory_budget_bytes: usize,
        big_endian: bool,
    ) -> Result<Self, StorageError> {
        if memory_budget_bytes == 0 {
            return Err(StorageError::FormatMismatch(
                "streamed TiledShape memory budget must be positive".to_string(),
            ));
        }

        let temp_data_path = temp_data_path.into();
        if let Some(parent) = temp_data_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let mut shape_to_cube = HashMap::with_capacity(layouts.len());
        let mut cubes = Vec::with_capacity(layouts.len());
        let mut row_count = 0usize;
        let mut cell_rank = None;
        let mut default_tile_shape = Vec::new();
        let mut working_set = ShapeCubeWorkingSet::default();

        // Each layout's extents are checked as its cube is created.
        for (cube_index, layout) in layouts.into_iter().enumerate() {
            match cell_rank {
                Some(rank) if rank != layout.cell_shape.len() => {
                    return Err(StorageError::FormatMismatch(
                        "streamed TiledShape layouts must have one cell rank".to_string(),
                    ));
                }
                None => cell_rank = Some(layout.cell_shape.len()),
                _ => {}
            }
            if default_tile_shape.is_empty() {
                default_tile_shape = layout.tile_shape.clone();
            }
            if shape_to_cube
                .insert(layout.cell_shape.clone(), cube_index)
                .is_some()
            {
                return Err(StorageError::FormatMismatch(format!(
                    "streamed TiledShape has duplicate layout for shape {:?}",
                    layout.cell_shape
                )));
            }

            row_count = row_count.checked_add(layout.row_count).ok_or_else(|| {
                StorageError::FormatMismatch(
                    "streamed TiledShape row count overflows usize".to_string(),
                )
            })?;
            if row_count > u32::MAX as usize || layout.row_count > u32::MAX as usize {
                return Err(StorageError::FormatMismatch(
                    "streamed TiledShape row maps support at most u32::MAX rows".to_string(),
                ));
            }
            let (cube, cube_working_set) = DirectShapeCubeWriter::create(
                streamed_shape_cube_temp_path(&temp_data_path, cube_index),
                layout,
                value_type,
            )?;
            working_set = working_set.max(cube_working_set);
            cubes.push(cube);
        }

        // The largest encoded row, tile row and packed Bool tile row are
        // held at once, though they may belong to different cubes: the
        // tile-row scratch is shared and sized for the largest.
        let mut working_bytes = working_set.bytes().ok_or_else(|| {
            StorageError::FormatMismatch(
                "streamed TiledShape working set overflows usize".to_string(),
            )
        })?;
        if working_bytes > memory_budget_bytes {
            return Err(StorageError::FormatMismatch(format!(
                "streamed TiledShape working set {working_bytes} exceeds memory budget {memory_budget_bytes}"
            )));
        }
        // Each cube assembles its bands in memory when they fit the budget
        // beside the working set and the largest row map the rows can need
        // ([`Self::resident_bytes`]).
        let band_bytes = cubes
            .iter()
            .map(DirectShapeCubeWriter::band_bytes)
            .sum::<usize>();
        if shape_cube::buffered_bytes(working_bytes, band_bytes, row_count) <= memory_budget_bytes {
            cubes
                .iter_mut()
                .for_each(DirectShapeCubeWriter::buffer_bands);
            working_bytes += band_bytes;
        }

        Ok(Self {
            value_type,
            big_endian,
            cell_rank,
            default_tile_shape,
            row_count,
            rows_written: 0,
            shape_to_cube,
            cubes,
            tile_row: vec![0; working_set.tile_row_bytes],
            row_map: Vec::new(),
            cube_map: Vec::new(),
            pos_map: Vec::new(),
            memory_budget_bytes,
            working_bytes,
            max_resident_bytes: working_bytes,
            assemble_seconds: 0.0,
            write_seconds: 0.0,
        })
    }

    pub fn push_bool_row(
        &mut self,
        cell_shape: &[usize],
        values: &[bool],
    ) -> Result<(), StorageError> {
        self.require_value_type(StreamedTiledShapeValueType::Bool)?;
        let assemble_started = Instant::now();
        let encoded: Vec<u8> = values.iter().map(|&value| u8::from(value)).collect();
        self.assemble_seconds += assemble_started.elapsed().as_secs_f64();
        self.push_encoded_row(cell_shape, &encoded)
    }

    pub fn push_f32_row(
        &mut self,
        cell_shape: &[usize],
        values: &[f32],
    ) -> Result<(), StorageError> {
        self.require_value_type(StreamedTiledShapeValueType::Float32)?;
        let assemble_started = Instant::now();
        let mut encoded = vec![0u8; values.len() * 4];
        for (idx, &value) in values.iter().enumerate() {
            if self.big_endian {
                write_f32_be(&mut encoded[idx * 4..], value);
            } else {
                write_f32_le(&mut encoded[idx * 4..], value);
            }
        }
        self.assemble_seconds += assemble_started.elapsed().as_secs_f64();
        self.push_encoded_row(cell_shape, &encoded)
    }

    /// Append one Int32 cell in casacore axis order.
    pub fn push_i32_row(
        &mut self,
        cell_shape: &[usize],
        values: &[i32],
    ) -> Result<(), StorageError> {
        self.require_value_type(StreamedTiledShapeValueType::Int32)?;
        let assemble_started = Instant::now();
        let mut encoded = Vec::with_capacity(size_of_val(values));
        for &value in values {
            encoded.extend_from_slice(&if self.big_endian {
                value.to_be_bytes()
            } else {
                value.to_le_bytes()
            });
        }
        self.assemble_seconds += assemble_started.elapsed().as_secs_f64();
        self.push_encoded_row(cell_shape, &encoded)
    }

    pub fn push_f64_row(
        &mut self,
        cell_shape: &[usize],
        values: &[f64],
    ) -> Result<(), StorageError> {
        self.require_value_type(StreamedTiledShapeValueType::Float64)?;
        let assemble_started = Instant::now();
        let mut encoded = vec![0u8; values.len() * 8];
        for (idx, &value) in values.iter().enumerate() {
            if self.big_endian {
                write_f64_be(&mut encoded[idx * 8..], value);
            } else {
                write_f64_le(&mut encoded[idx * 8..], value);
            }
        }
        self.assemble_seconds += assemble_started.elapsed().as_secs_f64();
        self.push_encoded_row(cell_shape, &encoded)
    }

    pub fn push_complex32_row(
        &mut self,
        cell_shape: &[usize],
        values: &[Complex32],
    ) -> Result<(), StorageError> {
        self.require_value_type(StreamedTiledShapeValueType::Complex32)?;
        let assemble_started = Instant::now();
        let mut encoded = vec![0u8; values.len() * 8];
        if self.big_endian {
            for (idx, &value) in values.iter().enumerate() {
                write_complex32_component_bytes(&mut encoded[idx * 8..], value, true);
            }
        } else {
            copy_complex32_native_bytes(&mut encoded, values);
        }
        self.assemble_seconds += assemble_started.elapsed().as_secs_f64();
        self.push_encoded_row(cell_shape, &encoded)
    }

    /// Append an undefined variable-shape cell using casacore's dummy cube 0.
    pub fn push_undefined_row(&mut self) -> Result<(), StorageError> {
        if self.rows_written >= self.row_count {
            return Err(StorageError::FormatMismatch(format!(
                "streamed TiledShape writer received too many rows: expected {}",
                self.row_count
            )));
        }
        let global_row = self.rows_written as u32;
        let position = if self.cube_map.last() == Some(&0) {
            self.pos_map.last().copied().unwrap_or(0).saturating_add(1)
        } else {
            0
        };
        self.append_row_mapping(global_row, 0, position)?;
        self.rows_written += 1;
        Ok(())
    }

    pub fn finish(mut self) -> Result<StreamedTiledShapeColumn, StorageError> {
        if self.rows_written != self.row_count {
            return Err(StorageError::FormatMismatch(format!(
                "streamed TiledShape writer finished with {} rows, expected {}",
                self.rows_written, self.row_count
            )));
        }
        let mut cubes = Vec::with_capacity(self.cubes.len());
        let mut remapped_cube_indices = vec![0u32; self.cubes.len() + 1];
        for (old_cube_index, mut cube) in self.cubes.into_iter().enumerate() {
            if cube.rows_written > cube.row_count {
                return Err(StorageError::FormatMismatch(format!(
                    "streamed TiledShape shape {:?} finished with {} rows, capacity {}",
                    cube.cell_shape, cube.rows_written, cube.row_count
                )));
            }
            if cube.rows_written == 0 {
                drop(cube.file);
                std::fs::remove_file(&cube.temp_data_path)?;
                continue;
            }
            let row_tile_count = cube.rows_written.div_ceil(cube.rows_per_tile);
            let actual_bytes_written = row_tile_count
                .checked_mul(cube.non_row_tile_count)
                .and_then(|tiles| tiles.checked_mul(cube.bucket_size))
                .ok_or_else(|| {
                    StorageError::FormatMismatch(
                        "streamed TiledShape final file size overflows usize".to_string(),
                    )
                })?;
            cube.finish_band()?;
            cube.file.flush()?;
            cube.file.set_len(actual_bytes_written as u64)?;
            remapped_cube_indices[old_cube_index + 1] = (cubes.len() + 1) as u32;
            cubes.push(DirectShapeCubePayload {
                temp_data_path: cube.temp_data_path,
                cell_shape: cube.cell_shape,
                row_count: cube.rows_written,
                tile_shape: cube.tile_shape,
                bytes_written: actual_bytes_written,
            });
        }
        for cube_index in &mut self.cube_map {
            if *cube_index != 0 {
                *cube_index = remapped_cube_indices[*cube_index as usize];
                debug_assert_ne!(*cube_index, 0);
            }
        }
        if cubes.is_empty() {
            self.row_map.clear();
            self.cube_map.clear();
            self.pos_map.clear();
        }
        Ok(StreamedTiledShapeColumn {
            row_count: self.row_count,
            value_type: self.value_type,
            big_endian: self.big_endian,
            cell_rank: self.cell_rank.unwrap_or(0),
            default_tile_shape: self.default_tile_shape,
            cubes,
            row_map: self.row_map,
            cube_map: self.cube_map,
            pos_map: self.pos_map,
            max_resident_bytes: self.max_resident_bytes,
            assemble_seconds: self.assemble_seconds,
            write_seconds: self.write_seconds,
        })
    }

    fn require_value_type(
        &self,
        expected: StreamedTiledShapeValueType,
    ) -> Result<(), StorageError> {
        if self.value_type == expected {
            Ok(())
        } else {
            Err(StorageError::FormatMismatch(format!(
                "streamed TiledShape writer is {:?}, not {:?}",
                self.value_type, expected
            )))
        }
    }

    fn push_encoded_row(
        &mut self,
        cell_shape: &[usize],
        encoded: &[u8],
    ) -> Result<(), StorageError> {
        if self.rows_written >= self.row_count {
            return Err(StorageError::FormatMismatch(format!(
                "streamed TiledShape writer received too many rows: expected {}",
                self.row_count
            )));
        }
        let cube_index = self.shape_to_cube.get(cell_shape).copied().ok_or_else(|| {
            StorageError::FormatMismatch(format!(
                "streamed TiledShape row has undeclared shape {cell_shape:?}"
            ))
        })?;
        let cube = &mut self.cubes[cube_index];
        if cube.rows_written >= cube.row_count {
            return Err(StorageError::FormatMismatch(format!(
                "streamed TiledShape received too many rows for shape {cell_shape:?}: expected {}",
                cube.row_count
            )));
        }
        let expected_bytes =
            cube.cell_shape.iter().product::<usize>() * self.value_type.elem_size();
        if encoded.len() != expected_bytes {
            return Err(StorageError::FormatMismatch(format!(
                "streamed TiledShape row has {} bytes, expected {expected_bytes} for shape {cell_shape:?}",
                encoded.len()
            )));
        }

        let global_row = self.rows_written as u32;
        let pos_in_cube = cube.rows_written as u32;
        let write_started = Instant::now();
        cube.write_row(self.value_type, encoded, &mut self.tile_row)?;
        self.write_seconds += write_started.elapsed().as_secs_f64();
        self.rows_written += 1;
        self.append_row_mapping(global_row, (cube_index + 1) as u32, pos_in_cube)?;
        Ok(())
    }

    fn append_row_mapping(
        &mut self,
        global_row: u32,
        cube: u32,
        position: u32,
    ) -> Result<(), StorageError> {
        let extends_last = self.row_map.last().is_some_and(|&last_row| {
            self.cube_map.last() == Some(&cube)
                && last_row + 1 == global_row
                && self
                    .pos_map
                    .last()
                    .is_some_and(|&last_pos| last_pos + 1 == position)
        });
        let map_entries_after = self.row_map.len() + usize::from(!extends_last);
        let map_bytes_after = map_entries_after
            .checked_mul(3 * std::mem::size_of::<u32>())
            .ok_or_else(|| {
                StorageError::FormatMismatch(
                    "streamed TiledShape row-map size overflows usize".to_string(),
                )
            })?;
        if self.working_bytes + map_bytes_after > self.memory_budget_bytes {
            return Err(StorageError::FormatMismatch(format!(
                "streamed TiledShape row-map working set exceeds memory budget {}",
                self.memory_budget_bytes
            )));
        }
        if extends_last {
            *self.row_map.last_mut().expect("checked row-map entry") = global_row;
            *self.pos_map.last_mut().expect("checked position entry") = position;
        } else {
            self.row_map.push(global_row);
            self.cube_map.push(cube);
            self.pos_map.push(position);
        }
        self.max_resident_bytes = self
            .max_resident_bytes
            .max(self.working_bytes + self.row_map.len() * 3 * std::mem::size_of::<u32>());
        Ok(())
    }
}

pub(super) fn streamed_shape_cube_temp_path(base: &Path, cube_index: usize) -> PathBuf {
    let mut path = base.as_os_str().to_os_string();
    path.push(format!(".cube{cube_index}"));
    PathBuf::from(path)
}

/// Install a completed heterogeneous payload using casacore's canonical
/// `TiledShapeStMan` header and row-map conventions.
#[doc(hidden)]
pub fn install_streamed_tiled_shape_column(
    table_path: &Path,
    dm_seq_nr: u32,
    dm_name: &str,
    streamed: StreamedTiledShapeColumn,
) -> Result<(), StorageError> {
    std::fs::create_dir_all(table_path)?;
    let mut files = Vec::with_capacity(streamed.cubes.len());
    let mut cubes = vec![TsmCubeInfo {
        values: RecordValue::default(),
        extensible: false,
        cube_shape: vec![],
        tile_shape: vec![],
        file_seq_nr: -1,
        file_offset: 0,
    }];
    let cell_rank = streamed.cell_rank;
    let default_tile_shape = streamed.default_tile_shape.clone();
    for (file_index, cube) in streamed.cubes.into_iter().enumerate() {
        let target = tsm_data_path(table_path, dm_seq_nr, file_index as u32);
        match std::fs::rename(&cube.temp_data_path, &target) {
            Ok(()) => {}
            Err(_) => {
                std::fs::copy(&cube.temp_data_path, &target)?;
                std::fs::remove_file(&cube.temp_data_path)?;
            }
        }
        files.push(Some(TsmFileInfo {
            seq_nr: file_index as u32,
            length: cube.bytes_written as i64,
        }));
        let mut cube_shape = cube.cell_shape;
        cube_shape.push(cube.row_count);
        cubes.push(TsmCubeInfo {
            values: RecordValue::default(),
            extensible: true,
            cube_shape,
            tile_shape: cube.tile_shape,
            file_seq_nr: file_index as i32,
            file_offset: 0,
        });
    }

    let header = TiledStManHeader {
        big_endian: streamed.big_endian,
        seq_nr: dm_seq_nr,
        nrrow: streamed.row_count as u64,
        col_data_types: vec![streamed.value_type.casacore_type()],
        hypercolumn_name: dm_name.to_string(),
        max_cache_size: 0,
        nrdim: if cell_rank == 0 {
            0
        } else {
            (cell_rank + 1) as u32
        },
        files,
        cubes,
    };
    let variant = TiledVariant::Shape {
        default_tile_shape: default_tile_shape
            .iter()
            .map(|&value| value as i32)
            .collect(),
        nr_used_row_map: streamed.row_map.len() as u32,
        row_map: streamed.row_map,
        cube_map: streamed.cube_map,
        pos_map: streamed.pos_map,
    };
    write_tiled_header(
        &table_path.join(format!("table.f{dm_seq_nr}")),
        &variant,
        &header,
    )?;
    invalidate_shared_tile_cache_for_table(table_path);
    Ok(())
}

/// Install a completed single-shape payload using casacore's canonical
/// `TiledColumnStMan` header conventions.
///
/// The direct writer is shared with heterogeneous `TiledShapeStMan` output,
/// but a column manager has exactly one defined shape and no row map.
#[doc(hidden)]
pub fn install_streamed_tiled_column(
    table_path: &Path,
    dm_seq_nr: u32,
    dm_name: &str,
    mut streamed: StreamedTiledShapeColumn,
) -> Result<(), StorageError> {
    if streamed.row_count == 0
        || streamed.cubes.len() != 1
        || streamed.row_map != vec![(streamed.row_count - 1) as u32]
        || streamed.cube_map != vec![1]
        || streamed.pos_map != vec![(streamed.row_count - 1) as u32]
    {
        return Err(StorageError::FormatMismatch(
            "streamed TiledColumnStMan payload must contain one defined shape for every row"
                .to_string(),
        ));
    }

    std::fs::create_dir_all(table_path)?;
    let cube = streamed.cubes.pop().expect("validated one cube");
    let target = tsm_data_path(table_path, dm_seq_nr, 0);
    match std::fs::rename(&cube.temp_data_path, &target) {
        Ok(()) => {}
        Err(_) => {
            std::fs::copy(&cube.temp_data_path, &target)?;
            std::fs::remove_file(&cube.temp_data_path)?;
        }
    }
    let mut cube_shape = cube.cell_shape;
    cube_shape.push(cube.row_count);
    let header = TiledStManHeader {
        big_endian: streamed.big_endian,
        seq_nr: dm_seq_nr,
        nrrow: streamed.row_count as u64,
        col_data_types: vec![streamed.value_type.casacore_type()],
        hypercolumn_name: dm_name.to_string(),
        max_cache_size: 0,
        nrdim: (streamed.cell_rank + 1) as u32,
        files: vec![Some(TsmFileInfo {
            seq_nr: 0,
            length: cube.bytes_written as i64,
        })],
        cubes: vec![TsmCubeInfo {
            values: RecordValue::default(),
            extensible: true,
            cube_shape,
            tile_shape: cube.tile_shape,
            file_seq_nr: 0,
            file_offset: 0,
        }],
    };
    let variant = TiledVariant::Column {
        default_tile_shape: streamed
            .default_tile_shape
            .iter()
            .map(|&value| value as i32)
            .collect(),
    };
    write_tiled_header(
        &table_path.join(format!("table.f{dm_seq_nr}")),
        &variant,
        &header,
    )?;
    invalidate_shared_tile_cache_for_table(table_path);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn write_single_column_shape_cube_direct_to_file(
    tsm_path: &Path,
    values: &[Option<&Value>],
    cell_shape: &[usize],
    tile_shape: &[usize],
    cell_nelem: usize,
    elem_size: usize,
    nr_tiles: usize,
    bucket_size: usize,
    col_offset: usize,
    col_data_type: CasacoreDataType,
    big_endian: bool,
) -> Result<Option<StreamedShapeCubeWrite>, StorageError> {
    if tile_shape.len() != cell_shape.len() + 1 {
        return Ok(None);
    }
    if tile_shape[..cell_shape.len()] != *cell_shape {
        return Ok(None);
    }
    let rows_per_tile = tile_shape[cell_shape.len()];
    if rows_per_tile == 0 {
        return Ok(None);
    }
    if col_data_type != CasacoreDataType::TpBool && col_data_type != CasacoreDataType::TpComplex {
        return Ok(None);
    }

    for value in values.iter().flatten() {
        match (col_data_type, *value) {
            (CasacoreDataType::TpBool, Value::Array(ArrayValue::Bool(array))) => {
                if array.len() != cell_nelem {
                    return Ok(None);
                }
            }
            (CasacoreDataType::TpComplex, Value::Array(ArrayValue::Complex32(array))) => {
                if array.len() != cell_nelem || array.shape() != cell_shape {
                    return Ok(None);
                }
            }
            _ => return Ok(None),
        }
    }

    let row_bytes = cell_nelem * elem_size;
    let tile_nelem: usize = tile_shape.iter().product();
    let tile_storage_len = tile_storage_bytes(col_data_type, tile_nelem);
    let tile_end = col_offset + tile_storage_len;
    if tile_end > bucket_size {
        return Ok(None);
    }

    let mut file = std::fs::File::create(tsm_path)?;
    let mut tile_data = vec![0u8; bucket_size];
    let mut assemble_seconds = 0.0;
    let mut write_seconds = 0.0;
    for tile_idx in 0..nr_tiles {
        tile_data.fill(0);
        let assemble_started = Instant::now();
        for row_in_tile in 0..rows_per_tile {
            let pos_in_cube = tile_idx * rows_per_tile + row_in_tile;
            let Some(value) = values.get(pos_in_cube).and_then(|value| *value) else {
                continue;
            };
            if col_data_type == CasacoreDataType::TpBool {
                let Value::Array(ArrayValue::Bool(array)) = value else {
                    unreachable!("preflight checked bool tiled shape values");
                };
                if array.iter().all(|value| !*value) {
                    continue;
                }
                let bools = array_memory_order_values(array);
                write_bool_bits(
                    &mut tile_data[col_offset..tile_end],
                    row_in_tile * cell_nelem,
                    &bools,
                );
            } else {
                let Value::Array(ArrayValue::Complex32(array)) = value else {
                    unreachable!("preflight checked complex tiled shape values");
                };
                let dst_start = col_offset + row_in_tile * row_bytes;
                let dst_end = dst_start + row_bytes;
                if dst_end > tile_data.len() {
                    return Ok(None);
                }
                encode_complex32_array_into_tile_row(
                    &mut tile_data[dst_start..dst_end],
                    array,
                    cell_shape,
                    big_endian,
                )?;
            }
        }
        assemble_seconds += assemble_started.elapsed().as_secs_f64();
        let write_started = Instant::now();
        file.write_all(&tile_data)?;
        write_seconds += write_started.elapsed().as_secs_f64();
    }

    Ok(Some(StreamedShapeCubeWrite {
        bytes: nr_tiles * bucket_size,
        assemble_seconds,
        write_seconds,
    }))
}

pub(super) fn encode_complex32_array_into_tile_row(
    dst: &mut [u8],
    array: &ArrayD<Complex32>,
    cell_shape: &[usize],
    big_endian: bool,
) -> Result<(), StorageError> {
    if dst.len() != array.len() * 8 {
        return Err(StorageError::FormatMismatch(
            "complex tile row has unexpected byte length".to_string(),
        ));
    }

    if !big_endian
        && is_fortran_contiguous(array.shape(), array.strides())
        && let Some(slice) = array.as_slice_memory_order()
    {
        copy_complex32_native_bytes(dst, slice);
        return Ok(());
    }

    if let Some(slice) = array.as_slice_memory_order()
        && cell_shape.len() == 2
        && array.strides() == [cell_shape[1] as isize, 1]
    {
        let n0 = cell_shape[0];
        let n1 = cell_shape[1];
        for axis1 in 0..n1 {
            for axis0 in 0..n0 {
                let value = slice[axis0 * n1 + axis1];
                let dst_offset = (axis1 * n0 + axis0) * 8;
                write_complex32_component_bytes(
                    &mut dst[dst_offset..dst_offset + 8],
                    value,
                    big_endian,
                );
            }
        }
        return Ok(());
    }

    let slice = array_memory_order_values(array);
    for (idx, &value) in slice.iter().enumerate() {
        let dst_offset = idx * 8;
        write_complex32_component_bytes(&mut dst[dst_offset..dst_offset + 8], value, big_endian);
    }
    Ok(())
}

pub(super) fn copy_complex32_native_bytes(dst: &mut [u8], values: &[Complex32]) {
    debug_assert_eq!(dst.len(), std::mem::size_of_val(values));
    // `num_complex::Complex32` is the same in-memory pair of f32 values that
    // casacore writes for little-endian complex cells. This fast path is only
    // used after the caller has already established the array's MS storage
    // order, so no logical transposition happens here.
    let src = unsafe {
        std::slice::from_raw_parts(values.as_ptr().cast::<u8>(), std::mem::size_of_val(values))
    };
    dst.copy_from_slice(src);
}

pub(super) fn tile_values_as_bytes<T: TilePixel>(values: &[T]) -> &[u8] {
    unsafe {
        std::slice::from_raw_parts(values.as_ptr().cast::<u8>(), std::mem::size_of_val(values))
    }
}

pub(super) fn tile_values_as_bytes_mut<T: TilePixel>(values: &mut [T]) -> &mut [u8] {
    unsafe {
        std::slice::from_raw_parts_mut(
            values.as_mut_ptr().cast::<u8>(),
            std::mem::size_of_val(values),
        )
    }
}

pub(super) fn write_complex32_component_bytes(dst: &mut [u8], value: Complex32, big_endian: bool) {
    if big_endian {
        write_f32_be(dst, value.re);
        write_f32_be(&mut dst[4..], value.im);
    } else {
        write_f32_le(dst, value.re);
        write_f32_le(&mut dst[4..], value.im);
    }
}
