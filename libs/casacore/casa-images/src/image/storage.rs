// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) fn paged_image_put_slice_view<T: ImagePixel>(
    image: &mut PagedImage<T>,
    data: ArrayViewD<'_, T>,
    start: &[usize],
) -> Result<(), LatticeError> {
    image
        .check_writable()
        .map_err(|error| LatticeError::Table(error.to_string()))?;
    if start.len() != image.shape.len() {
        return Err(LatticeError::NdimMismatch {
            expected: image.shape.len(),
            got: start.len(),
        });
    }
    let end: Vec<usize> = start
        .iter()
        .zip(data.shape().iter())
        .map(|(&s, &n)| s + n)
        .collect();
    for (&limit, &dim) in end.iter().zip(image.shape.iter()) {
        if limit > dim {
            return Err(LatticeError::ShapeMismatch {
                expected: image.shape.clone(),
                got: end,
            });
        }
    }
    if let Some(ref tio) = image.tiled_io {
        #[cfg(unix)]
        {
            image.pending_tile_writes = true;
        }
        if view_is_fortran_contiguous(&data)
            && let Some(slice) = data.as_slice_memory_order()
            && tio
                .borrow_mut()
                .put_aligned_fortran_order_tiles::<T>(slice, start, data.shape())
                .map_err(|e| LatticeError::Table(e.to_string()))?
        {
            return Ok(());
        }
        if let Some(slice) = data.as_slice() {
            let mut tiled = tio.borrow_mut();
            if tiled
                .put_aligned_c_order_tiles::<T>(slice, start, data.shape())
                .map_err(|e| LatticeError::Table(e.to_string()))?
            {
                return Ok(());
            }
            tiled
                .put_slice_c_order::<T>(slice, start, data.shape())
                .map_err(|e| LatticeError::Table(e.to_string()))?;
            return Ok(());
        }
        let fortran_view = data.t();
        if let Some(s) = fortran_view.as_slice() {
            tio.borrow_mut()
                .put_slice_fortran::<T>(s, start, data.shape())
                .map_err(|e| LatticeError::Table(e.to_string()))?;
            return Ok(());
        }
        let contiguous = data.as_standard_layout();
        let slice = contiguous.as_slice().expect("contiguous C-order data");
        tio.borrow_mut()
            .put_slice_c_order::<T>(slice, start, data.shape())
            .map_err(|e| LatticeError::Table(e.to_string()))?;
        return Ok(());
    }
    let mut array = image
        .read_array()
        .map_err(|e| LatticeError::Table(e.to_string()))?;
    {
        let mut view = array.slice_each_axis_mut(|axis| {
            let idx = axis.axis.index();
            Slice::from(start[idx] as isize..end[idx] as isize)
        });
        view.assign(&data);
    }
    image
        .write_array(&array)
        .map_err(|e| LatticeError::Table(e.to_string()))
}

pub(super) fn view_is_fortran_contiguous<T>(data: &ArrayViewD<'_, T>) -> bool {
    let mut expected = 1isize;
    for (&dim, &stride) in data.shape().iter().zip(data.strides()) {
        if dim <= 1 {
            continue;
        }
        if stride != expected {
            return false;
        }
        expected = expected.saturating_mul(dim as isize);
    }
    true
}

pub(super) fn to_array_value<T: ImagePixel>(array: &ArrayD<T>) -> ArrayValue {
    if TypeId::of::<T>() == TypeId::of::<f32>() {
        let array = unsafe_cast_ref::<T, f32>(array);
        ArrayValue::Float32(array.clone())
    } else if TypeId::of::<T>() == TypeId::of::<f64>() {
        let array = unsafe_cast_ref::<T, f64>(array);
        ArrayValue::Float64(array.clone())
    } else if TypeId::of::<T>() == TypeId::of::<Complex32>() {
        let array = unsafe_cast_ref::<T, Complex32>(array);
        ArrayValue::Complex32(array.clone())
    } else {
        let array = unsafe_cast_ref::<T, Complex64>(array);
        ArrayValue::Complex64(array.clone())
    }
}

pub(super) fn from_array_value<T: ImagePixel>(value: ArrayValue) -> Result<ArrayD<T>, ImageError> {
    if value.primitive_type() != T::PRIMITIVE_TYPE {
        return Err(ImageError::InvalidMetadata(format!(
            "type mismatch: expected {:?}, found {:?}",
            T::PRIMITIVE_TYPE,
            value.primitive_type()
        )));
    }
    match value {
        ArrayValue::Float32(array) => Ok(unsafe_cast_owned::<f32, T>(array)),
        ArrayValue::Float64(array) => Ok(unsafe_cast_owned::<f64, T>(array)),
        ArrayValue::Complex32(array) => Ok(unsafe_cast_owned::<Complex32, T>(array)),
        ArrayValue::Complex64(array) => Ok(unsafe_cast_owned::<Complex64, T>(array)),
        _ => Err(ImageError::InvalidMetadata(
            "unsupported image array value".to_string(),
        )),
    }
}

pub(super) fn unsafe_cast_ref<From: 'static, To: 'static>(array: &ArrayD<From>) -> &ArrayD<To> {
    assert_eq!(TypeId::of::<From>(), TypeId::of::<To>());
    unsafe { &*(std::ptr::from_ref(array) as *const ArrayD<To>) }
}

pub(super) fn unsafe_cast_owned<From: 'static, To: 'static>(array: ArrayD<From>) -> ArrayD<To> {
    assert_eq!(TypeId::of::<From>(), TypeId::of::<To>());
    unsafe {
        let raw = std::mem::ManuallyDrop::new(array);
        std::ptr::read(std::ptr::from_ref(&*raw) as *const ArrayD<To>)
    }
}

/// Returns the pixel type of an image on disk.
pub fn image_pixel_type(path: impl AsRef<Path>) -> Result<ImagePixelType, ImageError> {
    PagedImage::<f32>::pixel_type(path)
}

impl<T: ImagePixel> PagedImage<T> {
    /// Flushes pixel mutations so an owned expression can safely reopen this
    /// persistent source from worker-local handles.
    pub(crate) fn flush_pixels_for_reopen(&self) -> Result<(), ImageError> {
        if let Some(tiled_io) = &self.tiled_io {
            tiled_io
                .borrow_mut()
                .flush()
                .map_err(|error| ImageError::Io(error.to_string()))
        } else {
            self.flush_table()
        }
    }

    pub(super) fn map_column_primitive_type(
        table: &Table,
        tiled_io: Option<&RefCell<TiledArrayStorage>>,
    ) -> Result<PrimitiveType, ImageError> {
        if let Some(tiled_io) = tiled_io {
            return Ok(tiled_io.borrow().pixel_type());
        }
        if let Some(schema) = table.schema()
            && let Some(column) = schema.column(MAP_COLUMN)
            && let Some(data_type) = column.data_type()
        {
            return Ok(data_type);
        }

        let cell = table
            .cell_accessor(0, MAP_COLUMN)?
            .value()?
            .ok_or_else(|| {
                ImageError::InvalidMetadata("missing 'map' column in row 0".to_string())
            })?;
        match cell {
            Value::Array(array) => Ok(array.primitive_type()),
            _ => Err(ImageError::InvalidMetadata(
                "'map' column is not an array".to_string(),
            )),
        }
    }

    pub(super) fn map_column_shape(table: &Table) -> Result<Vec<usize>, ImageError> {
        let cell = table
            .cell_accessor(0, MAP_COLUMN)?
            .value()?
            .ok_or_else(|| {
                ImageError::InvalidMetadata("missing 'map' column in row 0".to_string())
            })?;
        match cell {
            Value::Array(array) => Ok(array.shape().to_vec()),
            _ => Err(ImageError::InvalidMetadata(
                "'map' column is not an array".to_string(),
            )),
        }
    }

    pub(super) fn open_tiled_io(
        path: &Path,
        max_cache_bytes: usize,
    ) -> Result<TiledArrayStorage, ImageError> {
        let cache_bytes = if max_cache_bytes == 0 {
            DEFAULT_TILED_ARRAY_CACHE_BYTES
        } else {
            max_cache_bytes
        };
        TiledArrayStorage::open_with_cache::<T>(path, 1, cache_bytes)
            .or_else(|_| TiledArrayStorage::open_with_cache::<T>(path, 0, cache_bytes))
            .map_err(|e| ImageError::Io(e.to_string()))
    }

    pub(super) fn tile_pixels(&self) -> usize {
        self.tile_shape.iter().product()
    }

    pub(super) fn element_size_bytes() -> usize {
        T::PRIMITIVE_TYPE.fixed_width_bytes().unwrap_or(0)
    }

    pub(super) fn refresh_tiled_io(&self) -> Result<(), ImageError> {
        if self.path.is_none() || self.tiled_io.is_none() {
            return Ok(());
        }
        let path = self.path.as_ref().expect("persistent image path");
        let tiled_io = Self::open_tiled_io(path, self.max_cache_bytes.get())?;
        *self
            .tiled_io
            .as_ref()
            .expect("tile-aware image")
            .borrow_mut() = tiled_io;
        Ok(())
    }

    pub(super) fn set_cache_bytes_shared(&self, max_cache_bytes: usize) -> Result<(), ImageError> {
        // Cache replacement drops the previous handle. Surface any pending
        // write error before reopening it with a different budget.
        if self.tiled_io.is_some() {
            self.flush_pixels_for_reopen()?;
        }
        let previous = self.max_cache_bytes.replace(if max_cache_bytes == 0 {
            DEFAULT_TILED_ARRAY_CACHE_BYTES
        } else {
            max_cache_bytes
        });
        if let Err(error) = self.refresh_tiled_io() {
            self.max_cache_bytes.set(previous);
            return Err(error);
        }
        self.persistent_masks.borrow_mut().clear();
        Ok(())
    }

    pub(super) fn persistent_mask_relative_path(
        record: &RecordValue,
    ) -> Result<String, ImageError> {
        match record.get("mask") {
            Some(Value::TableRef(path)) => Ok(path.clone()),
            Some(Value::Scalar(ScalarValue::String(path))) => Ok(path.clone()),
            _ => Err(ImageError::InvalidMetadata(
                "mask record is missing table reference".to_string(),
            )),
        }
    }

    pub(super) fn with_persistent_mask<R, F>(
        &self,
        name: &str,
        record: &RecordValue,
        func: F,
    ) -> Result<R, ImageError>
    where
        F: FnOnce(&PagedArray<bool>) -> Result<R, ImageError>,
    {
        if !self.persistent_masks.borrow().contains_key(name) {
            let image_path = self.path.as_deref().ok_or(ImageError::NotPersistent)?;
            let relative_path = Self::persistent_mask_relative_path(record)?;
            let mask = PagedArray::<bool>::open_with_cache(
                casa_tables::add_directory(&relative_path, image_path),
                self.cache_bytes(),
            )?;
            self.persistent_masks
                .borrow_mut()
                .insert(name.to_string(), mask);
        }

        let persistent_masks = self.persistent_masks.borrow();
        let mask = persistent_masks
            .get(name)
            .ok_or_else(|| ImageError::MaskNotFound(name.to_string()))?;
        func(mask)
    }

    /// Creates a new persistent image on disk.
    pub fn create(
        shape: Vec<usize>,
        coords: CoordinateSystem,
        path: impl AsRef<Path>,
    ) -> Result<Self, ImageError> {
        let tile_shape = TiledShape::new(shape.clone(), T::ELEM_SIZE)?
            .tile_shape()
            .to_vec();
        Self::create_with_tile_shape(shape, tile_shape, coords, path)
    }

    /// Creates a new persistent image with direct tile-level I/O.
    ///
    /// Unlike [`create`](Self::create), this method does *not* allocate a full
    /// in-memory default array. Instead, it sets up the table structure and a
    /// zeroed tile data file on disk. Subsequent `put_slice` / `get_slice`
    /// calls operate tile-by-tile via the typed storage seam, making plane-by-plane
    /// writes O(tiles_intersected) instead of O(total_pixels).
    ///
    /// Call [`save`](Self::save) after all writes to flush metadata.
    pub fn create_with_tile_shape(
        shape: Vec<usize>,
        tile_shape: Vec<usize>,
        coords: CoordinateSystem,
        path: impl AsRef<Path>,
    ) -> Result<Self, ImageError> {
        let path = path.as_ref().to_path_buf();

        // Use a variable-shape column schema to avoid allocating the full cube.
        // The actual pixel data lives in the TSM tile file, not in the table cell.
        let schema = TableSchema::new(vec![ColumnSchema::array_variable(
            MAP_COLUMN,
            T::PRIMITIVE_TYPE,
            Some(shape.len()),
        )])
        .map_err(|e| ImageError::Table(e.to_string()))?;
        let mut table = Table::with_schema(schema);
        Self::initialize_keywords(&mut table, &coords);
        table.keywords_mut().upsert(
            LOGTABLE_KEYWORD,
            Value::TableRef(subtable_reference(LOGTABLE_NAME)),
        );
        // Mark this image as using tile-aware I/O so open() can detect it.
        table
            .keywords_mut()
            .upsert("_tiled_io", Value::Scalar(ScalarValue::Bool(true)));
        table.set_info(TableInfo {
            table_type: "Image".into(),
            sub_type: "PAGEDIMAGE".into(),
            readme: Vec::new(),
        });
        // Add a row with a tiny 1-element placeholder array; the actual pixel
        // data lives in the TSM tile file.
        let placeholder = ArrayD::from_elem(IxDyn(&vec![1; shape.len()]), T::default_value());
        table.add_row(RecordValue::new(vec![RecordField::new(
            MAP_COLUMN,
            Value::Array(to_array_value(&placeholder)),
        )]))?;

        // Save the table skeleton with a CASA-visible tiled data manager for
        // the map column. The typed tiled seam overwrites this same DM sequence below
        // with the full target cube shape while avoiding a full in-memory map.
        table.save(
            TableOptions::new(&path)
                .with_data_manager(DataManagerKind::TiledCellStMan)
                .with_tile_shape(tile_shape.clone()),
        )?;

        // Create the typed tiled storage which writes the TSM header + allocates
        // a zeroed data file for the same TiledCellStMan sequence registered
        // in table.dat.
        let tiled_io = TiledArrayStorage::create::<T>(
            &path,
            &shape,
            &tile_shape,
            cfg!(target_endian = "big"), // native endian, matching C++ default
            0,
            MAP_COLUMN,
        )
        .map_err(|e| ImageError::Io(e.to_string()))?;

        Ok(Self {
            table,
            shape,
            tile_shape,
            coords,
            path: Some(path),
            units: String::new(),
            misc_info: RecordValue::default(),
            temp_masks: BTreeMap::new(),
            persistent_masks: RefCell::new(BTreeMap::new()),
            temp_history: Vec::new(),
            tiled_io: Some(RefCell::new(tiled_io)),
            max_cache_bytes: Cell::new(DEFAULT_TILED_ARRAY_CACHE_BYTES),
            #[cfg(unix)]
            cached_modify_counter: None,
            advisory_locks: false,
            #[cfg(unix)]
            pending_tile_writes: false,
            _pixel: PhantomData,
        })
    }

    /// Creates a new on-disk image with an explicit tile shape and cache size limit.
    ///
    /// When `max_cache_bytes > 0` and smaller than the total image data,
    /// tile I/O uses an LRU cache, forcing real disk I/O.
    pub fn create_with_tile_shape_and_cache(
        shape: Vec<usize>,
        tile_shape: Vec<usize>,
        coords: CoordinateSystem,
        path: impl AsRef<Path>,
        max_cache_bytes: usize,
    ) -> Result<Self, ImageError> {
        let path = path.as_ref().to_path_buf();

        let schema = TableSchema::new(vec![ColumnSchema::array_variable(
            MAP_COLUMN,
            T::PRIMITIVE_TYPE,
            Some(shape.len()),
        )])
        .map_err(|e| ImageError::Table(e.to_string()))?;
        let mut table = Table::with_schema(schema);
        Self::initialize_keywords(&mut table, &coords);
        table.keywords_mut().upsert(
            LOGTABLE_KEYWORD,
            Value::TableRef(subtable_reference(LOGTABLE_NAME)),
        );
        table
            .keywords_mut()
            .upsert("_tiled_io", Value::Scalar(ScalarValue::Bool(true)));
        table.set_info(TableInfo {
            table_type: "Image".into(),
            sub_type: "PAGEDIMAGE".into(),
            readme: Vec::new(),
        });
        let placeholder = ArrayD::from_elem(IxDyn(&tile_shape), T::default_value());
        table.add_row(RecordValue::new(vec![RecordField::new(
            MAP_COLUMN,
            Value::Array(to_array_value(&placeholder)),
        )]))?;
        table.save(
            TableOptions::new(&path)
                .with_data_manager(DataManagerKind::TiledCellStMan)
                .with_tile_shape(tile_shape.clone()),
        )?;

        let cache_bytes = if max_cache_bytes == 0 {
            DEFAULT_TILED_ARRAY_CACHE_BYTES
        } else {
            max_cache_bytes
        };
        let tiled_io = TiledArrayStorage::create_with_cache::<T>(
            &path,
            &shape,
            &tile_shape,
            cfg!(target_endian = "big"),
            0,
            MAP_COLUMN,
            cache_bytes,
        )
        .map_err(|e| ImageError::Io(e.to_string()))?;

        Ok(Self {
            table,
            shape,
            tile_shape,
            coords,
            path: Some(path),
            units: String::new(),
            misc_info: RecordValue::default(),
            temp_masks: BTreeMap::new(),
            persistent_masks: RefCell::new(BTreeMap::new()),
            temp_history: Vec::new(),
            tiled_io: Some(RefCell::new(tiled_io)),
            max_cache_bytes: Cell::new(cache_bytes),
            #[cfg(unix)]
            cached_modify_counter: None,
            advisory_locks: false,
            #[cfg(unix)]
            pending_tile_writes: false,
            _pixel: PhantomData,
        })
    }

    /// Opens an existing on-disk image with an explicit cache size limit.
    pub fn open_with_cache(
        path: impl AsRef<Path>,
        max_cache_bytes: usize,
    ) -> Result<Self, ImageError> {
        let path = path.as_ref().to_path_buf();
        let tiled_io = Self::open_tiled_io(&path, max_cache_bytes)
            .ok()
            .map(RefCell::new);
        let table = if tiled_io.is_some() {
            Table::open_metadata_only(TableOptions::new(&path))?
        } else {
            Table::open(TableOptions::new(&path))?
        };
        Self::from_disk(path, table, tiled_io, max_cache_bytes)
    }

    pub(super) fn from_disk(
        path: PathBuf,
        table: Table,
        tiled_io: Option<RefCell<TiledArrayStorage>>,
        max_cache_bytes: usize,
    ) -> Result<Self, ImageError> {
        let (coords, units, misc_info) = Self::read_table_metadata(&table)?;
        let map_primitive = Self::map_column_primitive_type(&table, tiled_io.as_ref())?;
        if map_primitive != T::PRIMITIVE_TYPE {
            return Err(ImageError::InvalidMetadata(format!(
                "image pixel type mismatch: requested {:?}, found {:?}",
                T::PRIMITIVE_TYPE,
                map_primitive
            )));
        }
        let (shape, tile_shape) = if let Some(ref tio) = tiled_io {
            let tio_ref = tio.borrow();
            (tio_ref.cube_shape().to_vec(), tio_ref.tile_shape().to_vec())
        } else {
            let shape = Self::map_column_shape(&table)?;
            let ts = TiledShape::new(shape.clone(), T::ELEM_SIZE)?
                .tile_shape()
                .to_vec();
            (shape, ts)
        };
        Ok(Self {
            #[cfg(unix)]
            cached_modify_counter: table.locked_modify_counter().ok(),
            advisory_locks: false,
            #[cfg(unix)]
            pending_tile_writes: false,
            table,
            shape,
            tile_shape,
            coords,
            path: Some(path),
            units,
            misc_info,
            temp_masks: BTreeMap::new(),
            persistent_masks: RefCell::new(BTreeMap::new()),
            temp_history: Vec::new(),
            tiled_io,
            max_cache_bytes: Cell::new(if max_cache_bytes == 0 {
                DEFAULT_TILED_ARRAY_CACHE_BYTES
            } else {
                max_cache_bytes
            }),
            _pixel: PhantomData,
        })
    }

    pub(super) fn read_table_metadata(
        table: &Table,
    ) -> Result<(CoordinateSystem, String, RecordValue), ImageError> {
        let coords = match table.keywords().get("coords") {
            Some(Value::Record(rec)) => CoordinateSystem::from_record(rec)
                .map_err(|error| ImageError::InvalidMetadata(error.to_string()))?,
            _ => CoordinateSystem::new(),
        };
        let units = match table.keywords().get("units") {
            Some(Value::Scalar(ScalarValue::String(s))) => s.clone(),
            _ => String::new(),
        };
        let misc_info = match table.keywords().get("miscinfo") {
            Some(Value::Record(rec)) => rec.clone(),
            _ => RecordValue::default(),
        };
        Ok((coords, units, misc_info))
    }

    /// Opens an existing image from disk as the requested typed image.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ImageError> {
        Self::open_with_cache(path, 0)
    }
}

impl<T: ImagePixel> PagedImage<T> {
    /// Flushes the image to its current on-disk path.
    ///
    /// When tile-aware I/O is active (via [`create_with_tile_shape`](Self::create_with_tile_shape)),
    /// pixel data is already on disk — only metadata (table.dat, table.info,
    /// keywords, logtable) is written.
    pub fn save(&mut self) -> Result<(), ImageError> {
        self.check_writable()?;
        let Some(path) = self.path.clone() else {
            return Err(ImageError::NotPersistent);
        };
        if let Some(ref tio) = self.tiled_io {
            // Flush dirty tiles to disk.
            tio.borrow_mut()
                .flush()
                .map_err(|e| ImageError::Io(e.to_string()))?;
            // Save only metadata: table.dat keywords/column keywords and
            // table.info. This preserves the existing tiled storage-manager
            // layout instead of rewriting the placeholder map cell.
            self.save_metadata_only(&path)?;
            #[cfg(unix)]
            {
                self.pending_tile_writes = false;
            }
        } else {
            if !self.temp_masks.is_empty() {
                // Flush first so the image directory exists for mask sub-tables.
                self.flush_table()?;
                self.write_all_masks(&path)?;
            }
            if !self.temp_history.is_empty() {
                self.ensure_logtable()?;
                for entry in self.temp_history.drain(..) {
                    Self::append_logtable_row(&Self::logtable_path(&path), &entry)?;
                }
            }
            self.flush_table()?;
        }
        // Ensure logtable directory exists after the image directory is created.
        let logtable_path = Self::logtable_path(&path);
        if !logtable_path.exists() {
            Self::reset_logtable(&logtable_path)?;
        }
        Ok(())
    }

    /// Saves the image to a new path and makes it persistent.
    pub fn save_as(&mut self, path: impl AsRef<Path>) -> Result<(), ImageError> {
        let path = path.as_ref().to_path_buf();
        self.materialize_to_path(&path)?;
        self.path = Some(path);
        self.persistent_masks.borrow_mut().clear();
        Ok(())
    }

    /// Returns a reference to the underlying table metadata.
    ///
    /// For tiled images opened from disk, the table may have been loaded via a
    /// metadata-only fast path. In that case schema, keywords, column
    /// keywords, and table info are available, but row/cell access behaves as
    /// if the table has zero rows because the pixel payload is read through the
    /// image's tiled I/O path instead of materializing the `"map"` cell.
    pub fn table(&self) -> &Table {
        &self.table
    }

    /// Set the subtype of the image's on-disk table.
    pub fn set_table_subtype(&mut self, subtype: impl Into<String>) {
        let mut info = self.table.info().clone();
        info.sub_type = subtype.into();
        self.table.set_info(info);
    }

    pub(super) fn initialize_keywords(table: &mut Table, coords: &CoordinateSystem) {
        table
            .keywords_mut()
            .upsert("coords", Value::Record(coords.to_record()));
        table
            .keywords_mut()
            .upsert("units", Value::Scalar(ScalarValue::String(String::new())));
    }

    pub(super) fn read_array(&self) -> Result<ArrayD<T>, ImageError> {
        let cell = self
            .table
            .cell_accessor(0, MAP_COLUMN)?
            .value()?
            .ok_or_else(|| ImageError::InvalidMetadata("missing map cell".to_string()))?;
        match cell {
            Value::Array(array) => from_array_value(array.clone()),
            _ => Err(ImageError::InvalidMetadata(
                "map cell is not an array".to_string(),
            )),
        }
    }

    pub(super) fn write_array(&mut self, array: &ArrayD<T>) -> Result<(), ImageError> {
        self.check_writable()?;
        self.table
            .cell_accessor_mut(0, MAP_COLUMN)?
            .set(Value::Array(to_array_value(array)))?;
        Ok(())
    }

    /// Save only metadata (table.dat, table.info, keywords) when tile data is
    /// already on disk.
    pub(super) fn save_metadata_only(&mut self, path: &Path) -> Result<(), ImageError> {
        if !self.temp_masks.is_empty() {
            self.write_all_masks(path)?;
        }
        if !self.temp_history.is_empty() {
            self.ensure_logtable()?;
            for entry in self.temp_history.drain(..) {
                Self::append_logtable_row(&Self::logtable_path(path), &entry)?;
            }
        }
        self.table.save_metadata_only(TableOptions::new(path))?;
        Ok(())
    }

    pub(super) fn flush_table(&self) -> Result<(), ImageError> {
        if let Some(path) = &self.path {
            self.table.save(self.save_options(path))?;
        }
        Ok(())
    }

    pub(super) fn save_options(&self, path: &Path) -> TableOptions {
        TableOptions::new(path)
            .with_data_manager(DataManagerKind::TiledCellStMan)
            .with_tile_shape(self.tile_shape.clone())
    }

    pub(super) fn materialize_to_path(&mut self, path: &Path) -> Result<(), ImageError> {
        self.table.keywords_mut().upsert(
            LOGTABLE_KEYWORD,
            Value::TableRef(subtable_reference(LOGTABLE_NAME)),
        );
        if let Some(Value::Record(masks)) = self.table.keywords_mut().get_mut(MASKS_KEYWORD) {
            point_mask_references_inside(masks);
        }
        let has_masks = !self.temp_masks.is_empty();
        let has_history = !self.temp_history.is_empty();
        if has_masks || has_history {
            // First save creates the image dir for mask/history sub-tables.
            self.table.save(self.save_options(path))?;
            self.write_all_masks(path)?;
            self.write_history_to_path(path)?;
        }
        self.table.save(self.save_options(path))?;
        Ok(())
    }
}
