// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// Default fixed cache budget for the typed tiled-array storage seam.
pub const DEFAULT_TILED_ARRAY_CACHE_BYTES: usize = 64 * 1024 * 1024;

/// Typed, byte-bounded storage seam for one casacore tiled array cell.
///
/// This is the deliberate image/lattice boundary. It preserves casacore's
/// persisted TiledCellStMan layout while preventing callers from selecting the
/// legacy whole-cube flat cache. Memory owned by the handle is bounded by the
/// configured cache (or one oversized tile) plus a reusable direct-I/O buffer.
pub struct TiledArrayStorage {
    pub(super) inner: TiledFileIO,
    pub(super) cache_budget_bytes: usize,
}

/// Prepared physical layout for one typed tiled array and its standard table metadata.
///
/// Preparation performs no filesystem I/O and allocates no pixel payload. The
/// encoded control files are produced by the same serializers as ordinary
/// tables; creation writes those bytes directly without an eager placeholder
/// row. Cache and index capacities are separate from caller-owned slice arrays.
#[derive(Debug, Clone)]
pub struct TiledArrayStorageLayout {
    pub(super) cube_shape: Vec<usize>,
    pub(super) tile_shape: Vec<usize>,
    pub(super) pixel_type: PrimitiveType,
    pub(super) big_endian: bool,
    pub(super) dm_seq_nr: u32,
    pub(super) cache_budget_bytes: usize,
    pub(super) tile_elements: usize,
    pub(super) tile_count: usize,
    pub(super) tile_bytes: usize,
    pub(super) file_tile_bytes: usize,
    pub(super) cache_slots: usize,
    pub(super) use_lru: bool,
    pub(super) control: Box<[u8]>,
    pub(super) header: Box<[u8]>,
    pub(super) info: Box<[u8]>,
}

impl TiledArrayStorageLayout {
    pub(super) fn write_new_data_file(&self, path: &Path, byte: u8) -> Result<(), StorageError> {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        let bytes = checked_layout_mul(self.tile_count, self.file_tile_bytes)?;
        if byte == 0 {
            file.set_len(u64::try_from(bytes).map_err(|_| {
                StorageError::FormatMismatch("typed array file length exceeds u64".into())
            })?)?;
        } else {
            let buffer = vec![byte; bytes.min(64 * 1024)];
            let mut remaining = bytes;
            while remaining > 0 {
                let count = remaining.min(buffer.len());
                file.write_all(&buffer[..count])?;
                remaining -= count;
            }
        }
        Ok(())
    }

    /// Prepare an exact array shape, physical tile shape and explicit cache.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cube_shape: &[usize],
        tile_shape: &[usize],
        pixel_type: PrimitiveType,
        big_endian: bool,
        dm_seq_nr: u32,
        column_name: &str,
        cache_budget_bytes: usize,
    ) -> Result<Self, StorageError> {
        TiledArrayStorage::validate_pixel_type(pixel_type)?;
        validate_typed_cache_budget(cache_budget_bytes)?;
        let dt = CasacoreDataType::from_primitive_type(pixel_type, false);
        let element_bytes = tile_element_size(dt);
        crate::storage::TileLayoutPlanner::repository_default()
            .plan_explicit_array(cube_shape, element_bytes, tile_shape)
            .map_err(|error| StorageError::FormatMismatch(error.to_string()))?;
        if cube_shape.len() > MAX_NDIM
            || cube_shape
                .iter()
                .chain(tile_shape)
                .any(|&n| n > i32::MAX as usize)
        {
            return Err(StorageError::FormatMismatch(
                "typed array shape exceeds casacore metadata bounds".into(),
            ));
        }
        let tile_elements = checked_storage_product(tile_shape, "tile elements")?;
        let tile_bytes = checked_layout_mul(tile_elements, element_bytes)?;
        let file_tile_bytes = if pixel_type == PrimitiveType::Bool {
            tile_elements.div_ceil(8)
        } else {
            tile_bytes
        };
        let tile_count = cube_shape
            .iter()
            .zip(tile_shape)
            .try_fold(1usize, |count, (&shape, &tile)| {
                checked_layout_mul(count, shape.div_ceil(tile))
            })?;
        let resident_payload = checked_layout_mul(tile_count, tile_bytes)?;
        let file_payload = checked_layout_mul(tile_count, file_tile_bytes)?;
        let file_length = i64::try_from(file_payload).map_err(|_| {
            StorageError::FormatMismatch("casacore tiled file size exceeds i64".into())
        })?;
        let use_lru = cache_budget_bytes < resident_payload;
        let cache_slots = if use_lru {
            (cache_budget_bytes / tile_bytes).max(1)
        } else {
            tile_count
        };
        if use_lru && cache_slots > i32::MAX as usize {
            return Err(StorageError::FormatMismatch(
                "tile cache slot count exceeds i32 index".into(),
            ));
        }
        let header = TiledStManHeader {
            big_endian,
            seq_nr: dm_seq_nr,
            nrrow: 1,
            col_data_types: vec![dt],
            hypercolumn_name: column_name.into(),
            max_cache_size: 0,
            nrdim: cube_shape.len() as u32,
            files: vec![Some(TsmFileInfo {
                seq_nr: 0,
                length: file_length,
            })],
            cubes: vec![TsmCubeInfo {
                values: RecordValue::default(),
                extensible: false,
                cube_shape: cube_shape.to_vec(),
                tile_shape: tile_shape.to_vec(),
                file_seq_nr: 0,
                file_offset: 0,
            }],
        };
        let variant = TiledVariant::Cell {
            default_tile_shape: tile_shape.iter().map(|&n| n as i32).collect(),
        };
        let header = serialize_tiled_header(&variant, &header)?;
        let schema = crate::TableSchema::new(vec![crate::ColumnSchema::array_variable(
            column_name,
            pixel_type,
            Some(cube_shape.len()),
        )])
        .map_err(|error| StorageError::FormatMismatch(error.to_string()))?;
        let mut control = super::super::table_control::TableDatContents::from_snapshot(
            &schema,
            &RecordValue::default(),
            &HashMap::new(),
            1,
            "TiledCellStMan",
            &[],
            big_endian,
        );
        control.column_set.data_managers[0].seq_nr = dm_seq_nr;
        control.column_set.columns[0].dm_seq_nr = dm_seq_nr;
        control.column_set.seq_count = dm_seq_nr
            .checked_add(1)
            .ok_or_else(|| StorageError::FormatMismatch("data manager sequence overflow".into()))?;
        for column in &mut control.table_desc.columns {
            column.data_manager_type = "TiledCellStMan".into();
            column.data_manager_group = column_name.into();
        }
        let control = super::super::table_control::serialize_table_dat(&control)?;
        let info = super::super::TableInfo::default()
            .to_string()
            .into_bytes()
            .into_boxed_slice();
        let layout = Self {
            cube_shape: cube_shape.to_vec(),
            tile_shape: tile_shape.to_vec(),
            pixel_type,
            big_endian,
            dm_seq_nr,
            cache_budget_bytes,
            tile_elements,
            tile_count,
            tile_bytes,
            file_tile_bytes,
            cache_slots,
            use_lru,
            control,
            header,
            info,
        };
        layout.cache_metadata_bytes()?;
        layout.storage_bytes()?;
        Ok(layout)
    }

    /// Logical array dimensions, excluding tile padding.
    pub fn cube_shape(&self) -> &[usize] {
        &self.cube_shape
    }
    /// Physical tile dimensions.
    pub fn tile_shape(&self) -> &[usize] {
        &self.tile_shape
    }
    /// Pixel representation bound by the plan.
    pub fn pixel_type(&self) -> PrimitiveType {
        self.pixel_type
    }
    /// Explicit requested cache budget, before tile rounding.
    pub fn cache_budget_bytes(&self) -> usize {
        self.cache_budget_bytes
    }
    /// Maximum allocated unpacked tile payload, including edge-tile padding.
    pub fn cache_payload_bytes(&self) -> Result<usize, StorageError> {
        checked_layout_mul(self.cache_slots, self.tile_bytes)
    }
    /// Index/dirty/access arrays retained in addition to the cache payload.
    pub fn cache_metadata_bytes(&self) -> Result<usize, StorageError> {
        if self.use_lru {
            checked_layout_add(
                checked_layout_mul(self.tile_count, size_of::<i32>() + size_of::<bool>())?,
                checked_layout_mul(
                    self.cache_slots,
                    size_of::<usize>() + size_of::<u64>() + size_of::<bool>(),
                )?,
            )
        } else {
            checked_layout_mul(self.tile_count, 2 * size_of::<bool>())
        }
    }
    /// Retained dimension/stride vector contents in one storage handle.
    pub fn shape_metadata_bytes(&self) -> Result<usize, StorageError> {
        checked_layout_mul(self.cube_shape.len(), 5 * size_of::<usize>())
    }
    /// Exact path payload retained by the three storage-owned boxed paths.
    pub fn path_metadata_bytes(&self, table_path: &Path) -> Result<usize, StorageError> {
        checked_layout_add(
            checked_layout_add(
                table_path.as_os_str().len(),
                tsm_data_path(table_path, self.dm_seq_nr, 0)
                    .as_os_str()
                    .len(),
            )?,
            table_path
                .join(format!("table.f{}", self.dm_seq_nr))
                .as_os_str()
                .len(),
        )
    }
    /// Cache, index, dimension and path heap at the largest retained state.
    pub fn retained_heap_bytes(&self, table_path: &Path) -> Result<usize, StorageError> {
        checked_layout_add(
            checked_layout_add(self.cache_payload_bytes()?, self.cache_metadata_bytes()?)?,
            checked_layout_add(
                self.shape_metadata_bytes()?,
                self.path_metadata_bytes(table_path)?,
            )?,
        )
    }
    /// Encoded standard control/header/info bytes retained by this prepared plan.
    pub fn encoded_metadata_bytes(&self) -> Result<usize, StorageError> {
        checked_layout_add(
            checked_layout_add(self.control.len(), self.header.len())?,
            self.info.len(),
        )
    }
    /// Heap owned by this plan, excluding its inline owner and allocator bookkeeping.
    pub fn owned_heap_bytes(&self) -> Result<usize, StorageError> {
        checked_layout_add(
            self.encoded_metadata_bytes()?,
            checked_layout_mul(
                self.cube_shape.capacity() + self.tile_shape.capacity(),
                size_of::<usize>(),
            )?,
        )
    }
    /// Exact logical file lengths, including packed/padded tiles and all metadata files.
    pub fn storage_bytes(&self) -> Result<usize, StorageError> {
        checked_layout_add(
            checked_layout_mul(self.tile_count, self.file_tile_bytes)?,
            self.encoded_metadata_bytes()?,
        )
    }
    /// Packed conversion scratch used while reading or writing one boolean tile.
    pub fn packed_tile_scratch_bytes(&self) -> usize {
        if self.file_tile_bytes != self.tile_bytes {
            self.file_tile_bytes
        } else {
            0
        }
    }
    /// Conversion bytes that can overlap a slice result or input array.
    pub fn conversion_scratch_bytes(&self) -> Result<usize, StorageError> {
        if self.big_endian != cfg!(target_endian = "big") {
            if self.use_lru {
                self.cache_payload_bytes()
            } else {
                Ok(self.tile_bytes)
            }
        } else {
            Ok(self.packed_tile_scratch_bytes())
        }
    }
    /// Heap scratch for one slice operation, excluding its ndarray payload.
    pub fn slice_scratch_bytes(&self) -> Result<usize, StorageError> {
        checked_layout_add(
            self.conversion_scratch_bytes()?,
            checked_layout_mul(self.cube_shape.len(), size_of::<usize>())?,
        )
    }
    /// Flush scratch in addition to retained cache and metadata.
    pub fn flush_scratch_bytes(&self) -> Result<usize, StorageError> {
        let buffer = if self.use_lru {
            0
        } else {
            std::io::BufWriter::new(std::io::sink()).capacity()
        };
        checked_layout_add(buffer, self.conversion_scratch_bytes()?)
    }
    /// Maximum file descriptors across creation, access, flush and reopen.
    /// A reopened flat cache retains a read descriptor while flush opens a writer.
    pub fn file_handles(&self) -> usize {
        if self.use_lru { 1 } else { 2 }
    }
    /// Whether one complete unpacked array fits the explicit cache.
    pub fn uses_flat_cache(&self) -> bool {
        !self.use_lru
    }
}

pub(super) fn checked_layout_mul(left: usize, right: usize) -> Result<usize, StorageError> {
    left.checked_mul(right)
        .ok_or_else(|| StorageError::FormatMismatch("typed array layout size overflow".into()))
}

pub(super) fn checked_layout_add(left: usize, right: usize) -> Result<usize, StorageError> {
    left.checked_add(right)
        .ok_or_else(|| StorageError::FormatMismatch("typed array layout size overflow".into()))
}

impl std::fmt::Debug for TiledArrayStorage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TiledArrayStorage")
            .field("table_path", &self.inner.table_path())
            .field("cube_shape", &self.inner.cube_shape())
            .field("tile_shape", &self.inner.tile_shape())
            .field("cache_budget_bytes", &self.cache_budget_bytes)
            .finish()
    }
}

impl TiledArrayStorage {
    /// Materialize a previously prepared one-column CASA table without constructing rows.
    ///
    /// The encoded metadata remains borrowed from `layout` during creation and
    /// is not retained by the returned handle. The caller owns its overlap with
    /// the cache/index allocation and any other live storage.
    pub fn create_planned_table(
        table_path: &Path,
        layout: &TiledArrayStorageLayout,
    ) -> Result<Self, StorageError> {
        Self::write_new_table_metadata(table_path, layout)?;
        let inner = TiledFileIO::create_planned(table_path, layout)?;
        Ok(Self {
            inner,
            cache_budget_bytes: layout.cache_budget_bytes,
        })
    }

    /// Materialize a new one-column CASA boolean table with a uniform value.
    ///
    /// The caller owns the destination and handles publication. Every physical
    /// tile, including its padding, is initialized without a tile cache or an
    /// unpacked image-sized array.
    pub fn create_filled_bool_table(
        table_path: &Path,
        layout: &TiledArrayStorageLayout,
        value: bool,
    ) -> Result<(), StorageError> {
        if layout.pixel_type != PrimitiveType::Bool {
            return Err(StorageError::FormatMismatch(
                "uniform boolean table requires a boolean layout".into(),
            ));
        }
        Self::write_new_table_metadata(table_path, layout)?;
        let data_path = tsm_data_path(table_path, layout.dm_seq_nr, 0);
        let header_path = table_path.join(format!("table.f{}", layout.dm_seq_nr));
        // casacore packs boolean tiles LSB first, padding to a byte boundary.
        // A one-pixel tile is also read as one unpacked Rust bool.
        let fill = match (value, layout.tile_elements) {
            (false, _) => 0,
            (true, 1) => 1,
            (true, _) => u8::MAX,
        };
        layout.write_new_data_file(&data_path, fill)?;
        std::fs::write(header_path, &layout.header)?;
        Ok(())
    }

    fn write_new_table_metadata(
        table_path: &Path,
        layout: &TiledArrayStorageLayout,
    ) -> Result<(), StorageError> {
        std::fs::create_dir_all(table_path)?;
        std::fs::write(table_path.join("table.dat"), &layout.control)?;
        std::fs::write(table_path.join("table.info"), &layout.info)?;
        Ok(())
    }

    /// Complete allocated heap owned by this direct handle, excluding allocator bookkeeping.
    /// Includes cache/index vectors, dimensions, paths and direct-write buffers.
    pub fn owned_heap_bytes(&self) -> Result<usize, StorageError> {
        let inner = &self.inner;
        let mut bytes = checked_layout_add(
            inner.tsm_path.as_os_str().len(),
            inner.header_path.as_os_str().len(),
        )?;
        bytes = checked_layout_add(bytes, inner.table_path.as_os_str().len())?;
        for values in [
            &inner.cube_shape,
            &inner.tile_shape,
            &inner.tiles_per_dim,
            &inner.tile_strides,
            &inner.tiles_per_dim_strides,
        ] {
            bytes = checked_layout_add(
                bytes,
                checked_layout_mul(values.capacity(), size_of::<usize>())?,
            )?;
        }
        bytes = checked_layout_add(bytes, inner.tile_on_disk.capacity())?;
        bytes = checked_layout_add(bytes, inner.direct_write_buffer.capacity())?;
        match &inner.cache {
            TileCache::Flat(flat) => {
                bytes = checked_layout_add(bytes, flat.data.capacity())?;
                bytes = checked_layout_add(bytes, flat.dirty.capacity())?;
            }
            TileCache::Lru(lru) => {
                bytes = checked_layout_add(bytes, lru.data.capacity())?;
                bytes = checked_layout_add(
                    bytes,
                    checked_layout_mul(lru.slot_tile.capacity(), size_of::<usize>())?,
                )?;
                bytes = checked_layout_add(bytes, lru.slot_dirty.capacity())?;
                bytes = checked_layout_add(
                    bytes,
                    checked_layout_mul(lru.tile_to_slot.capacity(), size_of::<i32>())?,
                )?;
                bytes = checked_layout_add(
                    bytes,
                    checked_layout_mul(lru.slot_access.capacity(), size_of::<u64>())?,
                )?;
            }
        }
        Ok(bytes)
    }

    /// File descriptors currently owned by the array, excluding transient flush handles.
    pub fn owned_file_handles(&self) -> usize {
        usize::from(matches!(self.inner.cache, TileCache::Lru(_)))
            + usize::from(self.inner.read_file.is_some())
    }

    fn validate_pixel_type(pixel_type: PrimitiveType) -> Result<(), StorageError> {
        if matches!(
            pixel_type,
            PrimitiveType::Bool
                | PrimitiveType::UInt8
                | PrimitiveType::Int16
                | PrimitiveType::UInt16
                | PrimitiveType::Int32
                | PrimitiveType::UInt32
                | PrimitiveType::Int64
                | PrimitiveType::Float32
                | PrimitiveType::Float64
                | PrimitiveType::Complex32
                | PrimitiveType::Complex64
        ) {
            Ok(())
        } else {
            Err(StorageError::FormatMismatch(format!(
                "unsupported tiled-array pixel type: {pixel_type:?}"
            )))
        }
    }

    /// Creates storage with the repository's fixed bounded cache policy.
    pub fn create<T: TilePixel>(
        table_path: &Path,
        cube_shape: &[usize],
        tile_shape: &[usize],
        big_endian: bool,
        dm_seq_nr: u32,
        dm_name: &str,
    ) -> Result<Self, StorageError> {
        Self::create_with_cache::<T>(
            table_path,
            cube_shape,
            tile_shape,
            big_endian,
            dm_seq_nr,
            dm_name,
            DEFAULT_TILED_ARRAY_CACHE_BYTES,
        )
    }

    /// Creates storage with an explicit positive byte cache budget.
    #[allow(clippy::too_many_arguments)]
    pub fn create_with_cache<T: TilePixel>(
        table_path: &Path,
        cube_shape: &[usize],
        tile_shape: &[usize],
        big_endian: bool,
        dm_seq_nr: u32,
        dm_name: &str,
        cache_budget_bytes: usize,
    ) -> Result<Self, StorageError> {
        validate_typed_cache_budget(cache_budget_bytes)?;
        let inner = TiledFileIO::create_with_cache_limit(
            table_path,
            cube_shape,
            tile_shape,
            T::STORAGE_TYPE,
            big_endian,
            dm_seq_nr,
            dm_name,
            cache_budget_bytes,
        )?;
        Ok(Self {
            inner,
            cache_budget_bytes,
        })
    }

    /// Creates storage for a checked runtime pixel type.
    ///
    /// This exists for generic lattice containers whose element type is
    /// represented by the closed [`PrimitiveType`] model. Reads and writes
    /// remain typed and revalidate the requested Rust type on every call.
    #[allow(clippy::too_many_arguments)]
    pub fn create_for_pixel_type(
        table_path: &Path,
        cube_shape: &[usize],
        tile_shape: &[usize],
        pixel_type: PrimitiveType,
        big_endian: bool,
        dm_seq_nr: u32,
        dm_name: &str,
        cache_budget_bytes: usize,
    ) -> Result<Self, StorageError> {
        Self::validate_pixel_type(pixel_type)?;
        validate_typed_cache_budget(cache_budget_bytes)?;
        let inner = TiledFileIO::create_with_cache_limit(
            table_path,
            cube_shape,
            tile_shape,
            pixel_type,
            big_endian,
            dm_seq_nr,
            dm_name,
            cache_budget_bytes,
        )?;
        Ok(Self {
            inner,
            cache_budget_bytes,
        })
    }

    /// Opens storage with the repository's fixed bounded cache policy.
    pub fn open<T: TilePixel>(table_path: &Path, dm_seq_nr: u32) -> Result<Self, StorageError> {
        Self::open_with_cache::<T>(table_path, dm_seq_nr, DEFAULT_TILED_ARRAY_CACHE_BYTES)
    }

    /// Opens storage with an explicit positive byte cache budget.
    pub fn open_with_cache<T: TilePixel>(
        table_path: &Path,
        dm_seq_nr: u32,
        cache_budget_bytes: usize,
    ) -> Result<Self, StorageError> {
        validate_typed_cache_budget(cache_budget_bytes)?;
        let inner = TiledFileIO::open_with_cache_limit(table_path, dm_seq_nr, cache_budget_bytes)?;
        if inner.pixel_type() != Some(T::STORAGE_TYPE) {
            return Err(StorageError::FormatMismatch(format!(
                "tiled array pixel type mismatch: requested {:?}, found {:?}",
                T::STORAGE_TYPE,
                inner.pixel_type()
            )));
        }
        Ok(Self {
            inner,
            cache_budget_bytes,
        })
    }

    /// Opens storage for a checked runtime pixel type.
    pub fn open_for_pixel_type(
        table_path: &Path,
        dm_seq_nr: u32,
        pixel_type: PrimitiveType,
        cache_budget_bytes: usize,
    ) -> Result<Self, StorageError> {
        Self::validate_pixel_type(pixel_type)?;
        validate_typed_cache_budget(cache_budget_bytes)?;
        let inner = TiledFileIO::open_with_cache_limit(table_path, dm_seq_nr, cache_budget_bytes)?;
        if inner.pixel_type() != Some(pixel_type) {
            return Err(StorageError::FormatMismatch(format!(
                "tiled array pixel type mismatch: requested {pixel_type:?}, found {:?}",
                inner.pixel_type()
            )));
        }
        Ok(Self {
            inner,
            cache_budget_bytes,
        })
    }

    /// Returns the logical array extents in stored axis order.
    pub fn cube_shape(&self) -> &[usize] {
        self.inner.cube_shape()
    }

    /// Returns the physical tile extents in stored axis order.
    pub fn tile_shape(&self) -> &[usize] {
        self.inner.tile_shape()
    }

    /// Reports whether multibyte components are stored most significant byte first.
    pub fn big_endian(&self) -> bool {
        self.inner.big_endian()
    }

    /// Returns the schema's primitive pixel type required for typed I/O.
    pub fn pixel_type(&self) -> PrimitiveType {
        self.inner
            .pixel_type()
            .expect("typed tiled-array storage always has a pixel type")
    }

    /// Returns the casacore data-manager sequence number used in table file naming.
    pub fn dm_seq_nr(&self) -> u32 {
        self.inner.dm_seq_nr()
    }

    /// Borrows the directory of the containing casacore table.
    pub fn table_path(&self) -> &Path {
        self.inner.table_path()
    }

    /// Returns the configured tile cache budget in bytes.
    pub fn cache_budget_bytes(&self) -> usize {
        self.cache_budget_bytes
    }

    /// Maximum currently allocated cache and direct-I/O bytes owned by this handle.
    pub fn owned_resident_bytes(&self) -> usize {
        let cache_bytes = match &self.inner.cache {
            TileCache::Flat(flat) => flat.data.capacity(),
            TileCache::Lru(lru) => lru.data.capacity(),
        };
        cache_bytes.saturating_add(self.inner.direct_write_buffer.capacity())
    }

    /// Returns this handle's accumulated tile I/O counters.
    pub fn io_stats(&self) -> TiledFileIoStats {
        self.inner.io_stats()
    }

    /// Resets this handle's counters without changing cached pixels.
    pub fn reset_io_stats(&mut self) {
        self.inner.reset_io_stats();
    }

    /// Discard cached reads while keeping the current pixel file open.
    /// Pending pixel writes retain their cache until they are flushed.
    pub fn discard_clean_read_cache(&mut self) {
        match &mut self.inner.cache {
            TileCache::Flat(flat) if !flat.dirty.iter().any(|&dirty| dirty) => {
                flat.data = Vec::new();
                flat.allocated = false;
            }
            TileCache::Lru(lru) if !lru.slot_dirty[..lru.used_slots].iter().any(|&dirty| dirty) => {
                for tile in &mut lru.slot_tile[..lru.used_slots] {
                    if *tile != usize::MAX {
                        lru.tile_to_slot[*tile] = -1;
                        *tile = usize::MAX;
                    }
                }
                lru.used_slots = 0;
                lru.access_counter = 0;
            }
            _ => {}
        }
    }

    /// Writes pending dirty tile bytes to the backing file, propagating I/O failures.
    pub fn flush(&mut self) -> Result<(), StorageError> {
        self.inner.flush()
    }

    /// Writes a typed array at a zero-based origin, taking extents from the input. Shape, type, and
    /// storage errors propagate.
    pub fn put_slice<T: TilePixel>(
        &mut self,
        data: &ArrayD<T>,
        start: &[usize],
    ) -> Result<(), StorageError> {
        self.ensure_pixel_type::<T>()?;
        self.inner.put_slice_ndarray(data, start)
    }

    /// Writes contiguous input with the last axis fastest. Start and extents follow stored axis
    /// order.
    pub fn put_slice_c_order<T: TilePixel>(
        &mut self,
        data: &[T],
        start: &[usize],
        shape: &[usize],
    ) -> Result<(), StorageError> {
        self.ensure_pixel_type::<T>()?;
        self.inner.put_slice_c_order(data, start, shape)
    }

    /// Writes contiguous input with the first axis fastest. Start and extents follow stored axis
    /// order.
    pub fn put_slice_fortran<T: TilePixel>(
        &mut self,
        data: &[T],
        start: &[usize],
        shape: &[usize],
    ) -> Result<(), StorageError> {
        self.ensure_pixel_type::<T>()?;
        self.inner.put_slice_fortran(data, start, shape)
    }

    /// Attempts a direct write from a last-axis-fastest buffer. False means the window could not
    /// use the aligned path.
    pub fn put_aligned_c_order_tiles<T: TilePixel>(
        &mut self,
        data: &[T],
        start: &[usize],
        shape: &[usize],
    ) -> Result<bool, StorageError> {
        self.ensure_pixel_type::<T>()?;
        self.inner.put_aligned_c_order_tiles(data, start, shape)
    }

    /// Attempts a direct write from a first-axis-fastest buffer. False means the window could not
    /// use the aligned path.
    pub fn put_aligned_fortran_order_tiles<T: TilePixel>(
        &mut self,
        data: &[T],
        start: &[usize],
        shape: &[usize],
    ) -> Result<bool, StorageError> {
        self.ensure_pixel_type::<T>()?;
        self.inner
            .put_aligned_fortran_order_tiles(data, start, shape)
    }

    /// Read one pixel through the existing bounded tile cache without allocating
    /// a slice array. Coordinates use the same axis order as [`Self::get_slice`].
    /// Pixel type, rank, and coordinate bounds must match the stored array.
    pub fn get_at<T: TilePixel>(&mut self, position: &[usize]) -> Result<T, StorageError> {
        self.ensure_pixel_type::<T>()?;
        if position.len() != self.inner.cube_shape.len()
            || position
                .iter()
                .zip(&self.inner.cube_shape)
                .any(|(index, extent)| index >= extent)
        {
            return Err(StorageError::FormatMismatch(
                "tiled array pixel position is outside the stored shape".to_string(),
            ));
        }
        let mut tile_index = 0;
        let mut pixel_index = 0;
        for (axis, &coordinate) in position.iter().enumerate() {
            tile_index +=
                coordinate / self.inner.tile_shape[axis] * self.inner.tiles_per_dim_strides[axis];
            pixel_index += coordinate % self.inner.tile_shape[axis] * self.inner.tile_strides[axis];
        }
        let tile = self.inner.get_cached_tile(tile_index)?;
        Ok(tile_as_typed::<T>(tile)[pixel_index])
    }

    /// Reads a rectangular section in first-axis-fastest layout. Invalid bounds, pixel types, and
    /// storage failures return an error.
    pub fn get_slice<T: TilePixel>(
        &mut self,
        start: &[usize],
        shape: &[usize],
    ) -> Result<ArrayD<T>, StorageError> {
        self.ensure_pixel_type::<T>()?;
        self.inner.get_slice(start, shape)
    }

    /// Read regularly sampled pixels in Fortran order by visiting only the tiles
    /// containing selected pixels. `shape` is the number of samples per axis.
    ///
    /// # Errors
    /// Returns an error for mismatched rank or pixel type, invalid bounds or
    /// strides, or an unreadable tile.
    pub fn get_strided_slice<T: TilePixel>(
        &mut self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<T>, StorageError> {
        self.ensure_pixel_type::<T>()?;
        self.inner.get_strided_slice(start, shape, stride)
    }

    /// Reads a section into caller-owned first-axis-fastest storage. The buffer must fit the
    /// requested extents and have the stored type.
    pub fn get_slice_into<T: TilePixel>(
        &mut self,
        start: &[usize],
        shape: &[usize],
        destination: &mut [T],
    ) -> Result<(), StorageError> {
        self.ensure_pixel_type::<T>()?;
        self.inner.get_slice_into(start, shape, destination)
    }

    /// Reads an owned array with the full stored shape and first-axis-fastest layout. The requested
    /// Rust element type must match the stored type.
    pub fn get_all<T: TilePixel>(&mut self) -> Result<ArrayD<T>, StorageError> {
        self.ensure_pixel_type::<T>()?;
        self.inner.get_all()
    }

    fn ensure_pixel_type<T: TilePixel>(&self) -> Result<(), StorageError> {
        if self.inner.pixel_type() != Some(T::STORAGE_TYPE) {
            return Err(StorageError::FormatMismatch(format!(
                "tiled array pixel type mismatch: requested {:?}, found {:?}",
                T::STORAGE_TYPE,
                self.inner.pixel_type()
            )));
        }
        Ok(())
    }
}

pub(super) fn validate_typed_cache_budget(cache_budget_bytes: usize) -> Result<(), StorageError> {
    if cache_budget_bytes == 0 {
        return Err(StorageError::FormatMismatch(
            "typed tiled-array cache budget must be positive".to_string(),
        ));
    }
    Ok(())
}

/// Reinterpret a byte slice as a typed slice (read-only).
///
/// # Safety
/// The caller must ensure the byte slice is aligned for `T` and that
/// `bytes.len()` is a multiple of `T::ELEM_SIZE`.
#[inline]
pub(super) fn tile_as_typed<T: TilePixel>(bytes: &[u8]) -> &[T] {
    debug_assert_eq!(bytes.len() % T::ELEM_SIZE, 0);
    debug_assert_eq!(
        (bytes.as_ptr() as usize) % std::mem::align_of::<T>(),
        0,
        "tile buffer misaligned for {}",
        std::any::type_name::<T>()
    );
    unsafe { std::slice::from_raw_parts(bytes.as_ptr() as *const T, bytes.len() / T::ELEM_SIZE) }
}

/// Reinterpret a byte slice as a typed mutable slice.
#[inline]
pub(super) fn tile_as_typed_mut<T: TilePixel>(bytes: &mut [u8]) -> &mut [T] {
    debug_assert_eq!(bytes.len() % T::ELEM_SIZE, 0);
    debug_assert_eq!(
        (bytes.as_ptr() as usize) % std::mem::align_of::<T>(),
        0,
        "tile buffer misaligned for {}",
        std::any::type_name::<T>()
    );
    unsafe {
        std::slice::from_raw_parts_mut(bytes.as_mut_ptr() as *mut T, bytes.len() / T::ELEM_SIZE)
    }
}

/// Byte-swap in place: reverse bytes within each `component_size`-byte chunk.
pub(super) fn swap_bytes_inplace(data: &mut [u8], component_size: usize) {
    match component_size {
        4 => {
            for chunk in data.as_chunks_mut::<4>().0 {
                chunk.swap(0, 3);
                chunk.swap(1, 2);
            }
        }
        8 => {
            for chunk in data.as_chunks_mut::<8>().0 {
                chunk.swap(0, 7);
                chunk.swap(1, 6);
                chunk.swap(2, 5);
                chunk.swap(3, 4);
            }
        }
        _ => {
            for chunk in data.chunks_exact_mut(component_size) {
                chunk.reverse();
            }
        }
    }
}

/// Compute C-order (row-major) strides for a given shape.
/// `stride[last] = 1`, `stride[i]` = product of `shape[i+1..]`.
pub(super) fn c_order_strides(shape: &[usize]) -> Vec<usize> {
    let ndim = shape.len();
    let mut strides = vec![1usize; ndim];
    for i in (0..ndim.saturating_sub(1)).rev() {
        strides[i] = strides[i + 1] * shape[i + 1];
    }
    strides
}

/// Compute Fortran-order (column-major) strides for a given shape.
/// `stride[0] = 1`, `stride[i]` = product of `shape[0..i]`.
pub(super) fn fortran_order_strides(shape: &[usize]) -> Vec<usize> {
    let ndim = shape.len();
    let mut strides = vec![1usize; ndim];
    for i in 1..ndim {
        strides[i] = strides[i - 1] * shape[i - 1];
    }
    strides
}

/// Dot product of two equal-length slices.
pub(super) fn dot_product(a: &[usize], b: &[usize]) -> usize {
    a.iter().zip(b.iter()).map(|(&x, &y)| x * y).sum()
}
