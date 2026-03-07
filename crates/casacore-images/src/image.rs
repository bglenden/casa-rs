// SPDX-License-Identifier: LGPL-3.0-or-later
//! Generic casacore-style images backed by casacore tables.
//!
//! [`PagedImage<T>`] is the typed persistent image abstraction in this crate.
//! It stores the pixel payload in a single fixed-shape `"map"` column and
//! carries the usual casacore image metadata: coordinates, units, image info,
//! misc info, masks, and history.
//!
//! The convenience alias [`Image`] is retained for the common
//! `PagedImage<f32>` case, matching C++ `PagedImage<Float>`.

use std::any::TypeId;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use casacore_coordinates::{CoordinateSystem, CoordinateType};
use casacore_lattices::{
    Lattice, LatticeElement, LatticeError, LatticeMut, PagedArray, TiledShape,
};
use casacore_tables::{
    ColumnSchema, DataManagerKind, Table, TableInfo, TableOptions, TableSchema, TilePixel,
    TiledFileIO,
};
use casacore_types::{
    ArrayD, ArrayValue, Complex32, Complex64, PrimitiveType, RecordField, RecordValue, ScalarValue,
    Value,
};
use ndarray::{IxDyn, Slice, SliceInfoElem};

use crate::error::ImageError;
use crate::image_expr::ImageExpr;
use crate::image_info::ImageInfo;
use crate::subimage::{SubImage, SubImageMut};

const MAP_COLUMN: &str = "map";
const MASKS_KEYWORD: &str = "masks";
const DEFAULT_MASK_KEYWORD: &str = "Image_defaultmask";
const LOGTABLE_KEYWORD: &str = "logtable";
const LOGTABLE_RELATIVE_PATH: &str = "logtable";
const REGION_TYPE_FIELD: &str = "isRegion";
const REGION_NAME_FIELD: &str = "name";
const REGION_COMMENT_FIELD: &str = "comment";
const REGION_TYPE_LC: i32 = 1;
const LCBOX_NAME: &str = "LCBox";
const LCPAGEDMASK_NAME: &str = "LCPagedMask";

mod private {
    pub trait Sealed {}
}

/// Pixel types supported by the image crate.
///
/// This is intentionally smaller than the full `LatticeElement` set and tracks
/// the publicly-instantiated casacore image pixel types we support here.
pub trait ImagePixel: LatticeElement + TilePixel + private::Sealed {}

impl private::Sealed for f32 {}
impl private::Sealed for f64 {}
impl private::Sealed for Complex32 {}
impl private::Sealed for Complex64 {}

impl ImagePixel for f32 {}
impl ImagePixel for f64 {}
impl ImagePixel for Complex32 {}
impl ImagePixel for Complex64 {}

/// Supported image pixel kinds discoverable from on-disk metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ImagePixelType {
    Float32,
    Float64,
    Complex32,
    Complex64,
}

impl ImagePixelType {
    fn from_primitive_type(primitive: PrimitiveType) -> Result<Self, ImageError> {
        match primitive {
            PrimitiveType::Float32 => Ok(Self::Float32),
            PrimitiveType::Float64 => Ok(Self::Float64),
            PrimitiveType::Complex32 => Ok(Self::Complex32),
            PrimitiveType::Complex64 => Ok(Self::Complex64),
            other => Err(ImageError::InvalidMetadata(format!(
                "unsupported image pixel type: {other:?}"
            ))),
        }
    }
}

/// Dynamically opened paged image with runtime pixel-type dispatch.
#[derive(Debug)]
pub enum AnyPagedImage {
    Float32(PagedImage<f32>),
    Float64(PagedImage<f64>),
    Complex32(PagedImage<Complex32>),
    Complex64(PagedImage<Complex64>),
}

impl AnyPagedImage {
    /// Opens an image from disk after detecting its pixel type.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ImageError> {
        let path_ref = path.as_ref();
        match image_pixel_type(path_ref)? {
            ImagePixelType::Float32 => Ok(Self::Float32(PagedImage::open(path_ref)?)),
            ImagePixelType::Float64 => Ok(Self::Float64(PagedImage::open(path_ref)?)),
            ImagePixelType::Complex32 => Ok(Self::Complex32(PagedImage::open(path_ref)?)),
            ImagePixelType::Complex64 => Ok(Self::Complex64(PagedImage::open(path_ref)?)),
        }
    }

    /// Returns the detected pixel type.
    pub fn pixel_type(&self) -> ImagePixelType {
        match self {
            Self::Float32(_) => ImagePixelType::Float32,
            Self::Float64(_) => ImagePixelType::Float64,
            Self::Complex32(_) => ImagePixelType::Complex32,
            Self::Complex64(_) => ImagePixelType::Complex64,
        }
    }

    /// Returns the image shape regardless of pixel type.
    pub fn shape(&self) -> &[usize] {
        match self {
            Self::Float32(image) => image.shape(),
            Self::Float64(image) => image.shape(),
            Self::Complex32(image) => image.shape(),
            Self::Complex64(image) => image.shape(),
        }
    }
}

/// Common image metadata behavior shared by persistent and temporary images.
pub trait ImageInterface<T: ImagePixel>: Lattice<T> {
    fn coordinates(&self) -> &CoordinateSystem;
    fn units(&self) -> &str;
    fn misc_info(&self) -> RecordValue;
    fn image_info(&self) -> Result<ImageInfo, ImageError>;
    fn name(&self) -> Option<&Path>;

    /// Returns the default pixel mask, if one is configured.
    ///
    /// Used by LEL `mask(image)` and `replace(image, val)`.  The default
    /// returns `Ok(None)` (no mask); concrete image types override this
    /// when they store pixel masks.
    fn default_mask(&self) -> Result<Option<ArrayD<bool>>, ImageError> {
        Ok(None)
    }

    fn name_string(&self, strip_path: bool) -> Option<String> {
        self.name().map(|path| {
            if strip_path {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or_default()
                    .to_string()
            } else {
                path.display().to_string()
            }
        })
    }
}

/// Convenience alias for the common `Float` image case.
pub type Image = PagedImage<f32>;

/// A coordinate-aware typed image backed by a casacore table.
///
/// This type is generic in the image pixel type and models both persistent and
/// temporary images. A temporary image is simply an image with no associated
/// filesystem path yet; calling [`save_as`](Self::save_as) materializes it.
pub struct PagedImage<T: ImagePixel> {
    table: Table,
    shape: Vec<usize>,
    tile_shape: Vec<usize>,
    coords: CoordinateSystem,
    path: Option<PathBuf>,
    units: String,
    misc_info: RecordValue,
    temp_masks: BTreeMap<String, ArrayD<bool>>,
    temp_history: Vec<String>,
    /// When `Some`, pixel I/O goes directly through tile-level file access,
    /// bypassing the `Table` cell abstraction. Enabled by `create_with_tile_shape`
    /// or when opening an on-disk image that was created with it.
    /// Wrapped in `RefCell` for interior mutability since the tile cache
    /// needs mutation even through `&self` trait methods.
    tiled_io: Option<RefCell<TiledFileIO>>,
    _pixel: PhantomData<T>,
}

impl<T: ImagePixel> std::fmt::Debug for PagedImage<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PagedImage")
            .field("shape", &self.shape)
            .field("tile_shape", &self.tile_shape)
            .field("path", &self.path)
            .field("pixel_type", &T::PRIMITIVE_TYPE)
            .finish()
    }
}

impl<T: ImagePixel> PagedImage<T> {
    /// Creates a new persistent image on disk.
    pub fn create(
        shape: Vec<usize>,
        coords: CoordinateSystem,
        path: impl AsRef<Path>,
    ) -> Result<Self, ImageError> {
        let path = path.as_ref().to_path_buf();
        let tile_shape = TiledShape::new(shape.clone()).tile_shape();
        let mut table = Self::build_table(&shape, false)?;
        Self::initialize_keywords(&mut table, &coords);
        table.keywords_mut().upsert(
            LOGTABLE_KEYWORD,
            Value::TableRef(LOGTABLE_RELATIVE_PATH.to_string()),
        );
        table.set_info(TableInfo {
            table_type: "Image".into(),
            sub_type: "PAGEDIMAGE".into(),
        });
        table.add_row(RecordValue::new(vec![RecordField::new(
            MAP_COLUMN,
            Value::Array(to_array_value(&ArrayD::from_elem(
                IxDyn(&shape),
                T::default_value(),
            ))),
        )]))?;
        // Don't save to disk yet — save() will write everything in one pass.

        Ok(Self {
            table,
            shape,
            tile_shape,
            coords,
            path: Some(path),
            units: String::new(),
            misc_info: RecordValue::default(),
            temp_masks: BTreeMap::new(),
            temp_history: Vec::new(),
            tiled_io: None,
            _pixel: PhantomData,
        })
    }

    /// Creates a new persistent image with direct tile-level I/O.
    ///
    /// Unlike [`create`](Self::create), this method does *not* allocate a full
    /// in-memory default array. Instead, it sets up the table structure and a
    /// zeroed tile data file on disk. Subsequent `put_slice` / `get_slice`
    /// calls operate tile-by-tile via [`TiledFileIO`], making plane-by-plane
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
            Value::TableRef(LOGTABLE_RELATIVE_PATH.to_string()),
        );
        // Mark this image as using tile-aware I/O so open() can detect it.
        table
            .keywords_mut()
            .upsert("_tiled_io", Value::Scalar(ScalarValue::Bool(true)));
        table.set_info(TableInfo {
            table_type: "Image".into(),
            sub_type: "PAGEDIMAGE".into(),
        });
        // Add a row with a tiny placeholder array.
        // The actual pixel data lives in the tile file.
        let placeholder = ArrayD::from_elem(IxDyn(&vec![1; shape.len()]), T::default_value());
        table.add_row(RecordValue::new(vec![RecordField::new(
            MAP_COLUMN,
            Value::Array(to_array_value(&placeholder)),
        )]))?;

        // Save the table skeleton to create the directory.
        // Use default DM (StManAipsIO) since our pixel data is in the TSM file
        // managed by TiledFileIO, not in the table cell.
        table.save(TableOptions::new(&path))?;

        // Create the TiledFileIO which writes the TSM header + allocates
        // a zeroed data file. Use dm_seq_nr=1 to avoid conflict with the
        // StManAipsIO data manager at seq_nr=0.
        let tiled_io = TiledFileIO::create(
            &path,
            &shape,
            &tile_shape,
            T::PRIMITIVE_TYPE,
            cfg!(target_endian = "big"), // native endian, matching C++ default
            1,                           // dm_seq_nr=1 avoids conflict with StManAipsIO at 0
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
            temp_history: Vec::new(),
            tiled_io: Some(RefCell::new(tiled_io)),
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
            Value::TableRef(LOGTABLE_RELATIVE_PATH.to_string()),
        );
        table
            .keywords_mut()
            .upsert("_tiled_io", Value::Scalar(ScalarValue::Bool(true)));
        table.set_info(TableInfo {
            table_type: "Image".into(),
            sub_type: "PAGEDIMAGE".into(),
        });
        let placeholder = ArrayD::from_elem(IxDyn(&vec![1; shape.len()]), T::default_value());
        table.add_row(RecordValue::new(vec![RecordField::new(
            MAP_COLUMN,
            Value::Array(to_array_value(&placeholder)),
        )]))?;
        table.save(TableOptions::new(&path))?;

        let tiled_io = TiledFileIO::create_with_cache_limit(
            &path,
            &shape,
            &tile_shape,
            T::PRIMITIVE_TYPE,
            cfg!(target_endian = "big"),
            1,
            MAP_COLUMN,
            max_cache_bytes,
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
            temp_history: Vec::new(),
            tiled_io: Some(RefCell::new(tiled_io)),
            _pixel: PhantomData,
        })
    }

    /// Opens an existing on-disk image with an explicit cache size limit.
    pub fn open_with_cache(
        path: impl AsRef<Path>,
        max_cache_bytes: usize,
    ) -> Result<Self, ImageError> {
        let path = path.as_ref().to_path_buf();
        let table = Table::open(TableOptions::new(&path))?;
        let cell = table.cell(0, MAP_COLUMN).ok_or_else(|| {
            ImageError::InvalidMetadata("missing 'map' column in row 0".to_string())
        })?;
        let shape = match &cell {
            Value::Array(array) => {
                if array.primitive_type() != T::PRIMITIVE_TYPE {
                    return Err(ImageError::InvalidMetadata(format!(
                        "image pixel type mismatch: requested {:?}, found {:?}",
                        T::PRIMITIVE_TYPE,
                        array.primitive_type()
                    )));
                }
                array.shape().to_vec()
            }
            _ => {
                return Err(ImageError::InvalidMetadata(
                    "'map' column is not an array".to_string(),
                ));
            }
        };
        let coords = match table.keywords().get("coords") {
            Some(Value::Record(rec)) => CoordinateSystem::from_record(rec)?,
            _ => CoordinateSystem::new(),
        };
        let tiled_io = match table.keywords().get("_tiled_io") {
            Some(Value::Scalar(ScalarValue::Bool(true))) => {
                TiledFileIO::open_with_cache_limit(&path, 1, max_cache_bytes)
                    .or_else(|_| TiledFileIO::open_with_cache_limit(&path, 0, max_cache_bytes))
                    .ok()
                    .map(RefCell::new)
            }
            _ => None,
        };
        let (shape, tile_shape) = if let Some(ref tio) = tiled_io {
            let tio_ref = tio.borrow();
            (tio_ref.cube_shape().to_vec(), tio_ref.tile_shape().to_vec())
        } else {
            let ts = TiledShape::new(shape.clone()).tile_shape();
            (shape, ts)
        };
        Ok(Self {
            table,
            shape: shape.clone(),
            tile_shape,
            coords,
            path: Some(path),
            units: String::new(),
            misc_info: RecordValue::default(),
            temp_masks: BTreeMap::new(),
            temp_history: Vec::new(),
            tiled_io,
            _pixel: PhantomData,
        })
    }

    /// Creates a new temporary image backed by an in-memory table.
    ///
    /// Prefer [`TempImage::new()`](crate::TempImage::new) which avoids table
    /// overhead entirely.
    #[deprecated(note = "Use TempImage::new() instead")]
    pub fn create_temp(shape: Vec<usize>, coords: CoordinateSystem) -> Result<Self, ImageError> {
        let tile_shape = TiledShape::new(shape.clone()).tile_shape();
        let mut table = Self::build_table(&shape, true)?;
        Self::initialize_keywords(&mut table, &coords);
        table.set_info(TableInfo {
            table_type: "Image".into(),
            sub_type: "PAGEDIMAGE".into(),
        });
        table.add_row(RecordValue::new(vec![RecordField::new(
            MAP_COLUMN,
            Value::Array(to_array_value(&ArrayD::from_elem(
                IxDyn(&shape),
                T::default_value(),
            ))),
        )]))?;

        Ok(Self {
            table,
            shape,
            tile_shape,
            coords,
            path: None,
            units: String::new(),
            misc_info: RecordValue::default(),
            temp_masks: BTreeMap::new(),
            temp_history: Vec::new(),
            tiled_io: None,
            _pixel: PhantomData,
        })
    }

    /// Opens an existing image from disk as the requested typed image.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ImageError> {
        let path = path.as_ref().to_path_buf();
        let table = Table::open(TableOptions::new(&path))?;
        let cell = table.cell(0, MAP_COLUMN).ok_or_else(|| {
            ImageError::InvalidMetadata("missing 'map' column in row 0".to_string())
        })?;
        let shape = match &cell {
            Value::Array(array) => {
                if array.primitive_type() != T::PRIMITIVE_TYPE {
                    return Err(ImageError::InvalidMetadata(format!(
                        "image pixel type mismatch: requested {:?}, found {:?}",
                        T::PRIMITIVE_TYPE,
                        array.primitive_type()
                    )));
                }
                array.shape().to_vec()
            }
            _ => {
                return Err(ImageError::InvalidMetadata(
                    "'map' column is not an array".to_string(),
                ));
            }
        };
        let coords = match table.keywords().get("coords") {
            Some(Value::Record(rec)) => CoordinateSystem::from_record(rec)?,
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

        // Enable tile-aware I/O only if the image was created with create_with_tile_shape.
        // Try dm_seq_nr=1 first (new format with variable-shape column), then 0 (old format).
        let tiled_io = match table.keywords().get("_tiled_io") {
            Some(Value::Scalar(ScalarValue::Bool(true))) => TiledFileIO::open(&path, 1)
                .or_else(|_| TiledFileIO::open(&path, 0))
                .ok()
                .map(RefCell::new),
            _ => None,
        };

        // When tile-aware I/O is active, the real shape comes from the TSM header
        // (the table cell may hold only a placeholder array).
        let (shape, tile_shape) = if let Some(ref tio) = tiled_io {
            let tio_ref = tio.borrow();
            (tio_ref.cube_shape().to_vec(), tio_ref.tile_shape().to_vec())
        } else {
            let ts = TiledShape::new(shape.clone()).tile_shape();
            (shape, ts)
        };

        Ok(Self {
            table,
            shape: shape.clone(),
            tile_shape,
            coords,
            path: Some(path),
            units,
            misc_info,
            temp_masks: BTreeMap::new(),
            temp_history: Vec::new(),
            tiled_io,
            _pixel: PhantomData,
        })
    }

    /// Detects the pixel type of an image on disk without opening it as a
    /// specific `PagedImage<T>`.
    pub fn pixel_type(path: impl AsRef<Path>) -> Result<ImagePixelType, ImageError> {
        let table = Table::open(TableOptions::new(path.as_ref()))?;
        let cell = table.cell(0, MAP_COLUMN).ok_or_else(|| {
            ImageError::InvalidMetadata("missing 'map' column in row 0".to_string())
        })?;
        match cell {
            Value::Array(array) => ImagePixelType::from_primitive_type(array.primitive_type()),
            _ => Err(ImageError::InvalidMetadata(
                "'map' column is not an array".to_string(),
            )),
        }
    }

    /// Returns the shape of the image payload.
    pub fn shape(&self) -> &[usize] {
        &self.shape
    }

    /// Returns the number of axes.
    pub fn ndim(&self) -> usize {
        self.shape.len()
    }

    /// Returns the number of pixels.
    pub fn nelements(&self) -> usize {
        self.shape.iter().product()
    }

    /// Returns `true` when the image has been materialized to disk.
    pub fn is_persistent(&self) -> bool {
        self.path.is_some()
    }

    /// Returns `true` for persistent paged images and `false` for temporary images.
    pub fn is_paged(&self) -> bool {
        self.path.is_some()
    }

    /// Returns `true` because `PagedImage<T>` supports mutation.
    pub fn is_writable(&self) -> bool {
        true
    }

    /// Returns the casacore-style class name for this image.
    pub fn image_type_name(&self) -> &'static str {
        if self.path.is_some() {
            "PagedImage"
        } else {
            "TempImage"
        }
    }

    /// Returns the filesystem location if the image is persistent.
    pub fn name(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    /// Returns the tile shape used by the backing table.
    pub fn tile_shape(&self) -> &[usize] {
        &self.tile_shape
    }

    /// Reads the full pixel array.
    pub fn get(&self) -> Result<ArrayD<T>, ImageError> {
        <Self as Lattice<T>>::get(self).map_err(Into::into)
    }

    /// Reads a rectangular slice using unit stride.
    pub fn get_slice(&self, start: &[usize], shape: &[usize]) -> Result<ArrayD<T>, ImageError> {
        <Self as Lattice<T>>::get_slice(self, start, shape, &vec![1; self.ndim()])
            .map_err(Into::into)
    }

    /// Reads a rectangular slice using an explicit stride.
    pub fn get_slice_with_stride(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<T>, ImageError> {
        <Self as Lattice<T>>::get_slice(self, start, shape, stride).map_err(Into::into)
    }

    /// Reads a single pixel value.
    pub fn get_at(&self, pos: &[usize]) -> Result<T, ImageError> {
        <Self as Lattice<T>>::get_at(self, pos).map_err(Into::into)
    }

    /// Writes a full-value slice at the given start position.
    pub fn put_slice(&mut self, data: &ArrayD<T>, start: &[usize]) -> Result<(), ImageError> {
        <Self as LatticeMut<T>>::put_slice(self, data, start).map_err(Into::into)
    }

    /// Writes a single pixel.
    pub fn put_at(&mut self, value: T, pos: &[usize]) -> Result<(), ImageError> {
        <Self as LatticeMut<T>>::put_at(self, value, pos).map_err(Into::into)
    }

    /// Sets all pixels to the same value.
    pub fn set(&mut self, value: T) -> Result<(), ImageError> {
        <Self as LatticeMut<T>>::set(self, value).map_err(Into::into)
    }

    /// Returns the coordinate system.
    pub fn coordinates(&self) -> &CoordinateSystem {
        &self.coords
    }

    /// Replaces the coordinate system and updates the table keyword.
    pub fn set_coordinates(&mut self, coords: CoordinateSystem) -> Result<(), ImageError> {
        self.table
            .keywords_mut()
            .upsert("coords", Value::Record(coords.to_record()));
        self.coords = coords;
        Ok(())
    }

    /// Returns the brightness unit string.
    pub fn units(&self) -> &str {
        &self.units
    }

    /// Replaces the brightness unit string.
    pub fn set_units(&mut self, units: impl Into<String>) -> Result<(), ImageError> {
        let units = units.into();
        self.table
            .keywords_mut()
            .upsert("units", Value::Scalar(ScalarValue::String(units.clone())));
        self.units = units;
        Ok(())
    }

    /// Reads the image info record.
    pub fn image_info(&self) -> Result<ImageInfo, ImageError> {
        match self.table.keywords().get("imageinfo") {
            Some(Value::Record(rec)) => ImageInfo::from_record(rec),
            _ => Ok(ImageInfo::default()),
        }
    }

    /// Replaces the image info record.
    pub fn set_image_info(&mut self, info: &ImageInfo) -> Result<(), ImageError> {
        self.table
            .keywords_mut()
            .upsert("imageinfo", Value::Record(info.to_record()));
        Ok(())
    }

    /// Returns the misc-info record.
    pub fn misc_info(&self) -> RecordValue {
        self.misc_info.clone()
    }

    /// Replaces the misc-info record.
    pub fn set_misc_info(&mut self, rec: RecordValue) -> Result<(), ImageError> {
        self.table
            .keywords_mut()
            .upsert("miscinfo", Value::Record(rec.clone()));
        self.misc_info = rec;
        Ok(())
    }

    /// Returns the coordinate type for each image axis.
    pub fn axis_types(&self) -> Vec<CoordinateType> {
        let mut types = Vec::with_capacity(self.ndim());
        for i in 0..self.coords.n_coordinates() {
            let coord = self.coords.coordinate(i);
            let coord_type = coord.coordinate_type();
            for _ in 0..coord.n_pixel_axes() {
                types.push(coord_type);
            }
        }
        while types.len() < self.ndim() {
            types.push(CoordinateType::Linear);
        }
        types
    }

    /// Returns the axis names for the image.
    pub fn axis_names(&self) -> Vec<String> {
        let mut names = Vec::with_capacity(self.ndim());
        for i in 0..self.coords.n_coordinates() {
            names.extend(self.coords.coordinate(i).axis_names());
        }
        while names.len() < self.ndim() {
            names.push(format!("Axis{}", names.len()));
        }
        names
    }

    /// Finds the first axis belonging to a given coordinate type.
    pub fn find_axis(&self, coord_type: CoordinateType) -> Option<usize> {
        let mut offset = 0;
        for i in 0..self.coords.n_coordinates() {
            let coord = self.coords.coordinate(i);
            if coord.coordinate_type() == coord_type {
                return Some(offset);
            }
            offset += coord.n_pixel_axes();
        }
        None
    }

    /// Finds an axis by case-insensitive name prefix.
    pub fn find_axis_by_name(&self, name: &str) -> Option<usize> {
        let target = name.to_lowercase();
        self.axis_names()
            .iter()
            .position(|axis| axis.to_lowercase().starts_with(&target))
    }

    /// Extracts a single plane with the target axis fixed to `index`.
    pub fn get_plane(&self, axis: usize, index: usize) -> Result<ArrayD<T>, ImageError> {
        if axis >= self.ndim() {
            return Err(ImageError::ShapeMismatch {
                expected: self.shape.clone(),
                got: vec![axis],
            });
        }
        let mut start = vec![0; self.ndim()];
        let mut shape = self.shape.clone();
        start[axis] = index;
        shape[axis] = 1;
        self.get_slice(&start, &shape)
    }

    /// Extracts a spectral-channel plane if the image has a spectral axis.
    pub fn channel_plane(&self, chan: usize) -> Result<Option<ArrayD<T>>, ImageError> {
        match self.find_axis(CoordinateType::Spectral) {
            Some(axis) => Ok(Some(self.get_plane(axis, chan)?)),
            None => Ok(None),
        }
    }

    /// Extracts a Stokes plane if the image has a Stokes axis.
    pub fn stokes_plane(&self, stokes: usize) -> Result<Option<ArrayD<T>>, ImageError> {
        match self.find_axis(CoordinateType::Stokes) {
            Some(axis) => Ok(Some(self.get_plane(axis, stokes)?)),
            None => Ok(None),
        }
    }

    /// Creates a unit-stride subimage view.
    pub fn sub_image(
        &self,
        start: Vec<usize>,
        shape: Vec<usize>,
    ) -> Result<SubImage<'_, T, Self>, ImageError> {
        SubImage::new(self, start, shape)
    }

    /// Creates a strided subimage view.
    pub fn sub_image_with_stride(
        &self,
        start: Vec<usize>,
        shape: Vec<usize>,
        stride: Vec<usize>,
    ) -> Result<SubImage<'_, T, Self>, ImageError> {
        SubImage::with_stride(self, start, shape, stride)
    }

    /// Creates a mutable unit-stride subimage view.
    pub fn sub_image_mut(
        &mut self,
        start: Vec<usize>,
        shape: Vec<usize>,
    ) -> Result<SubImageMut<'_, T, Self>, ImageError> {
        SubImageMut::new(self, start, shape)
    }

    /// Creates a mutable strided subimage view.
    pub fn sub_image_mut_with_stride(
        &mut self,
        start: Vec<usize>,
        shape: Vec<usize>,
        stride: Vec<usize>,
    ) -> Result<SubImageMut<'_, T, Self>, ImageError> {
        SubImageMut::with_stride(self, start, shape, stride)
    }

    /// Creates a read-only expression by mapping a function over this image.
    /// Starts a lazy expression rooted at this image.
    pub fn expr(&self) -> Result<ImageExpr<'_, T>, ImageError>
    where
        T: crate::image_expr::ImageExprValue + PartialOrd,
    {
        ImageExpr::from_image(self)
    }

    /// Creates a lazy read-only expression by mapping a function over this image.
    pub fn expr_map<F>(&self, f: F) -> Result<ImageExpr<'_, T>, ImageError>
    where
        T: crate::image_expr::ImageExprValue + PartialOrd,
        F: Fn(T) -> T + Send + Sync + 'static,
    {
        ImageExpr::map(self, f)
    }

    /// Returns the history entries associated with the image.
    pub fn history(&self) -> Result<Vec<String>, ImageError> {
        match &self.path {
            Some(path) => Self::read_logtable(&Self::logtable_path(path)),
            None => Ok(self.temp_history.clone()),
        }
    }

    /// Appends a history message.
    pub fn add_history(&mut self, msg: impl Into<String>) -> Result<(), ImageError> {
        let msg = msg.into();
        match self.path.clone() {
            Some(path) => {
                self.ensure_logtable()?;
                Self::append_logtable_row(&Self::logtable_path(&path), &msg)?;
                Ok(())
            }
            None => {
                self.temp_history.push(msg);
                Ok(())
            }
        }
    }

    /// Clears the history log.
    pub fn clear_history(&mut self) -> Result<(), ImageError> {
        match self.path.clone() {
            Some(path) => {
                self.ensure_logtable()?;
                Self::reset_logtable(&Self::logtable_path(&path))?;
                Ok(())
            }
            None => {
                self.temp_history.clear();
                Ok(())
            }
        }
    }

    /// Returns `true` if the image has a default pixel mask.
    pub fn has_pixel_mask(&self) -> bool {
        self.default_mask_name().is_some()
    }

    /// Returns the default mask name if present.
    pub fn default_mask_name(&self) -> Option<String> {
        match self.table.keywords().get(DEFAULT_MASK_KEYWORD) {
            Some(Value::Scalar(ScalarValue::String(name))) if !name.is_empty() => {
                Some(name.clone())
            }
            _ => None,
        }
    }

    /// Returns the names of all stored masks.
    pub fn mask_names(&self) -> Vec<String> {
        match self.table.keywords().get(MASKS_KEYWORD) {
            Some(Value::Record(rec)) => rec.fields().iter().map(|f| f.name.clone()).collect(),
            _ => Vec::new(),
        }
    }

    /// Creates a full-image mask with the given name.
    pub fn make_mask(
        &mut self,
        name: impl Into<String>,
        set_default: bool,
        initial: bool,
    ) -> Result<(), ImageError> {
        let name = name.into();
        let mask = ArrayD::from_elem(IxDyn(&self.shape), initial);
        self.put_mask(&name, &mask)?;
        if set_default {
            self.set_default_mask(&name)?;
        }
        Ok(())
    }

    /// Sets the default mask, validating that it exists.
    pub fn set_default_mask(&mut self, name: &str) -> Result<(), ImageError> {
        if !self.mask_names().iter().any(|mask_name| mask_name == name) {
            return Err(ImageError::MaskNotFound(name.to_string()));
        }
        self.table.keywords_mut().upsert(
            DEFAULT_MASK_KEYWORD,
            Value::Scalar(ScalarValue::String(name.to_string())),
        );
        Ok(())
    }

    /// Unsets the default mask.
    pub fn unset_default_mask(&mut self) -> Result<(), ImageError> {
        self.table.keywords_mut().remove(DEFAULT_MASK_KEYWORD);
        Ok(())
    }

    /// Returns the default mask contents if a default mask is configured.
    pub fn get_mask(&self) -> Result<Option<ArrayD<bool>>, ImageError> {
        match self.default_mask_name() {
            Some(name) => Ok(Some(self.get_named_mask(&name)?)),
            None => Ok(None),
        }
    }

    /// Returns a named mask as a full-image boolean array.
    pub fn get_named_mask(&self, name: &str) -> Result<ArrayD<bool>, ImageError> {
        if let Some(mask) = self.temp_masks.get(name) {
            return Ok(mask.clone());
        }
        let masks = match self.table.keywords().get(MASKS_KEYWORD) {
            Some(Value::Record(rec)) => rec,
            _ => return Err(ImageError::MaskNotFound(name.to_string())),
        };
        let entry = masks
            .get(name)
            .ok_or_else(|| ImageError::MaskNotFound(name.to_string()))?;
        read_mask_entry(entry, self.path.as_deref(), &self.shape)
    }

    /// Replaces a named mask.
    pub fn put_mask(&mut self, name: &str, data: &ArrayD<bool>) -> Result<(), ImageError> {
        if data.shape() != self.shape.as_slice() {
            return Err(ImageError::ShapeMismatch {
                expected: self.shape.clone(),
                got: data.shape().to_vec(),
            });
        }
        let table_ref = match &self.path {
            Some(path) => mask_table_reference(path, name),
            None => name.to_string(),
        };
        let record = make_paged_mask_record(&table_ref, &self.shape);
        let mut masks = match self.table.keywords().get(MASKS_KEYWORD) {
            Some(Value::Record(rec)) => rec.clone(),
            _ => RecordValue::default(),
        };
        masks.upsert(name, Value::Record(record));
        self.table
            .keywords_mut()
            .upsert(MASKS_KEYWORD, Value::Record(masks));

        if let Some(path) = &self.path {
            Self::write_mask_table(path, name, data)?;
        } else {
            self.temp_masks.insert(name.to_string(), data.clone());
        }
        Ok(())
    }

    /// Removes a named mask and clears the default mask if it pointed at it.
    pub fn remove_mask(&mut self, name: &str) -> Result<(), ImageError> {
        let mut masks = match self.table.keywords().get(MASKS_KEYWORD) {
            Some(Value::Record(rec)) => rec.clone(),
            _ => return Err(ImageError::MaskNotFound(name.to_string())),
        };
        if masks.remove(name).is_none() {
            return Err(ImageError::MaskNotFound(name.to_string()));
        }
        self.table
            .keywords_mut()
            .upsert(MASKS_KEYWORD, Value::Record(masks));
        self.temp_masks.remove(name);
        if let Some(path) = &self.path {
            let mask_path = path.join(name);
            if mask_path.exists() {
                std::fs::remove_dir_all(mask_path).map_err(|e| ImageError::Io(e.to_string()))?;
            }
        }
        if self.default_mask_name().as_deref() == Some(name) {
            self.table.keywords_mut().remove(DEFAULT_MASK_KEYWORD);
        }
        Ok(())
    }

    /// Flushes the image to its current on-disk path.
    ///
    /// When tile-aware I/O is active (via [`create_with_tile_shape`](Self::create_with_tile_shape)),
    /// pixel data is already on disk — only metadata (table.dat, table.info,
    /// keywords, logtable) is written.
    pub fn save(&mut self) -> Result<(), ImageError> {
        let Some(path) = self.path.clone() else {
            return Err(ImageError::NotPersistent);
        };
        if let Some(ref tio) = self.tiled_io {
            // Flush dirty tiles to disk.
            tio.borrow_mut()
                .flush()
                .map_err(|e| ImageError::Io(e.to_string()))?;
            // Save only metadata: table.dat, table.info, keywords.
            self.save_metadata_only(&path)?;
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
        Ok(())
    }

    /// Returns a reference to the underlying table.
    pub fn table(&self) -> &Table {
        &self.table
    }

    fn build_table(shape: &[usize], memory: bool) -> Result<Table, ImageError> {
        let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
            MAP_COLUMN,
            T::PRIMITIVE_TYPE,
            shape.to_vec(),
        )])
        .map_err(|e| ImageError::Table(e.to_string()))?;
        Ok(if memory {
            Table::with_schema_memory(schema)
        } else {
            Table::with_schema(schema)
        })
    }

    fn initialize_keywords(table: &mut Table, coords: &CoordinateSystem) {
        table
            .keywords_mut()
            .upsert("coords", Value::Record(coords.to_record()));
        table
            .keywords_mut()
            .upsert("units", Value::Scalar(ScalarValue::String(String::new())));
    }

    fn read_array(&self) -> Result<ArrayD<T>, ImageError> {
        let cell = self
            .table
            .cell(0, MAP_COLUMN)
            .ok_or_else(|| ImageError::InvalidMetadata("missing map cell".to_string()))?;
        match cell {
            Value::Array(array) => from_array_value(array.clone()),
            _ => Err(ImageError::InvalidMetadata(
                "map cell is not an array".to_string(),
            )),
        }
    }

    fn write_array(&mut self, array: &ArrayD<T>) -> Result<(), ImageError> {
        self.table
            .set_cell(0, MAP_COLUMN, Value::Array(to_array_value(array)))?;
        Ok(())
    }

    /// Save only metadata (table.dat, table.info, keywords) when tile data is
    /// already on disk.
    fn save_metadata_only(&mut self, path: &Path) -> Result<(), ImageError> {
        // The table directory already exists (created by create_with_tile_shape).
        // Re-save the table to update table.dat, table.info, keywords.
        // The pixel data in the TSM file is untouched.
        if !self.temp_masks.is_empty() {
            self.write_all_masks(path)?;
        }
        if !self.temp_history.is_empty() {
            self.ensure_logtable()?;
            for entry in self.temp_history.drain(..) {
                Self::append_logtable_row(&Self::logtable_path(path), &entry)?;
            }
        }
        // Write table.info.
        let info_path = path.join("table.info");
        let info = self.table.info();
        std::fs::write(&info_path, info.to_string()).map_err(|e| ImageError::Io(e.to_string()))?;
        Ok(())
    }

    fn flush_table(&self) -> Result<(), ImageError> {
        if let Some(path) = &self.path {
            self.table.save(self.save_options(path))?;
        }
        Ok(())
    }

    fn save_options(&self, path: &Path) -> TableOptions {
        TableOptions::new(path)
            .with_data_manager(DataManagerKind::TiledCellStMan)
            .with_tile_shape(self.tile_shape.clone())
    }

    fn materialize_to_path(&mut self, path: &Path) -> Result<(), ImageError> {
        self.table.keywords_mut().upsert(
            LOGTABLE_KEYWORD,
            Value::TableRef(LOGTABLE_RELATIVE_PATH.to_string()),
        );
        self.rewrite_mask_keyword_paths(path);
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

    fn ensure_logtable(&mut self) -> Result<(), ImageError> {
        if let Some(path) = &self.path {
            self.table.keywords_mut().upsert(
                LOGTABLE_KEYWORD,
                Value::TableRef(LOGTABLE_RELATIVE_PATH.to_string()),
            );
            let logtable_path = Self::logtable_path(path);
            if !logtable_path.exists() {
                Self::reset_logtable(&logtable_path)?;
            }
        }
        Ok(())
    }

    fn logtable_path(path: &Path) -> PathBuf {
        path.join(LOGTABLE_RELATIVE_PATH)
    }

    fn reset_logtable(path: &Path) -> Result<(), ImageError> {
        if path.exists() {
            std::fs::remove_dir_all(path).map_err(|e| ImageError::Io(e.to_string()))?;
        }
        let schema = TableSchema::new(vec![
            ColumnSchema::scalar("TIME", PrimitiveType::Float64),
            ColumnSchema::scalar("PRIORITY", PrimitiveType::String),
            ColumnSchema::scalar("MESSAGE", PrimitiveType::String),
            ColumnSchema::scalar("LOCATION", PrimitiveType::String),
            ColumnSchema::scalar("OBJECT_ID", PrimitiveType::String),
        ])
        .map_err(|e| ImageError::Table(e.to_string()))?;
        let table = Table::with_schema(schema);
        table.save(TableOptions::new(path))?;
        Ok(())
    }

    fn append_logtable_row(path: &Path, message: &str) -> Result<(), ImageError> {
        let mut table = Table::open(TableOptions::new(path))?;
        table.add_row(RecordValue::new(vec![
            RecordField::new(
                "TIME",
                Value::Scalar(ScalarValue::Float64(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_err(|e| ImageError::Io(e.to_string()))?
                        .as_secs_f64(),
                )),
            ),
            RecordField::new(
                "PRIORITY",
                Value::Scalar(ScalarValue::String("INFO".into())),
            ),
            RecordField::new(
                "MESSAGE",
                Value::Scalar(ScalarValue::String(message.to_string())),
            ),
            RecordField::new(
                "LOCATION",
                Value::Scalar(ScalarValue::String("casacore-images".into())),
            ),
            RecordField::new(
                "OBJECT_ID",
                Value::Scalar(ScalarValue::String(String::new())),
            ),
        ]))?;
        table.save(TableOptions::new(path))?;
        Ok(())
    }

    fn read_logtable(path: &Path) -> Result<Vec<String>, ImageError> {
        if !path.exists() {
            return Ok(Vec::new());
        }
        let table = Table::open(TableOptions::new(path))?;
        let mut messages = Vec::new();
        for row in 0..table.row_count() {
            if let Some(Value::Scalar(ScalarValue::String(message))) = table.cell(row, "MESSAGE") {
                messages.push(message.clone());
            }
        }
        Ok(messages)
    }

    fn write_history_to_path(&mut self, path: &Path) -> Result<(), ImageError> {
        let logtable_path = Self::logtable_path(path);
        Self::reset_logtable(&logtable_path)?;
        for entry in self.temp_history.drain(..) {
            Self::append_logtable_row(&logtable_path, &entry)?;
        }
        Ok(())
    }

    fn write_all_masks(&mut self, path: &Path) -> Result<(), ImageError> {
        let pending = std::mem::take(&mut self.temp_masks);
        for (name, mask) in pending {
            Self::write_mask_table(path, &name, &mask)?;
        }
        Ok(())
    }

    fn rewrite_mask_keyword_paths(&mut self, path: &Path) {
        let Some(Value::Record(mut masks)) = self.table.keywords().get(MASKS_KEYWORD).cloned()
        else {
            return;
        };

        let mut changed = false;
        for field in masks.fields_mut() {
            let Value::Record(record) = &mut field.value else {
                continue;
            };

            let table_ref = mask_table_reference(path, &field.name);
            match record.get("mask") {
                Some(Value::TableRef(existing)) if existing == &table_ref => {}
                Some(Value::Scalar(ScalarValue::String(existing))) if existing == &table_ref => {}
                Some(Value::TableRef(_)) | Some(Value::Scalar(ScalarValue::String(_))) => {
                    record.upsert("mask", Value::TableRef(table_ref));
                    changed = true;
                }
                _ => {}
            }
        }

        if changed {
            self.table
                .keywords_mut()
                .upsert(MASKS_KEYWORD, Value::Record(masks));
        }
    }

    fn write_mask_table(
        image_path: &Path,
        relative_mask_path: &str,
        data: &ArrayD<bool>,
    ) -> Result<(), ImageError> {
        let mask_path = image_path.join(relative_mask_path);
        if mask_path.exists() {
            std::fs::remove_dir_all(&mask_path).map_err(|e| ImageError::Io(e.to_string()))?;
        }
        let mut mask =
            PagedArray::<bool>::create(TiledShape::new(data.shape().to_vec()), &mask_path)
                .map_err(ImageError::from)?;
        mask.put_slice(data, &vec![0; data.ndim()])?;
        mask.flush().map_err(ImageError::from)
    }
}

impl<T: ImagePixel> ImageInterface<T> for PagedImage<T> {
    fn coordinates(&self) -> &CoordinateSystem {
        self.coordinates()
    }

    fn units(&self) -> &str {
        self.units()
    }

    fn misc_info(&self) -> RecordValue {
        self.misc_info()
    }

    fn image_info(&self) -> Result<ImageInfo, ImageError> {
        self.image_info()
    }

    fn name(&self) -> Option<&Path> {
        self.name()
    }

    fn default_mask(&self) -> Result<Option<ArrayD<bool>>, ImageError> {
        self.get_mask()
    }
}

impl<T: ImagePixel> Lattice<T> for PagedImage<T> {
    fn shape(&self) -> &[usize] {
        &self.shape
    }

    fn is_persistent(&self) -> bool {
        self.is_persistent()
    }

    fn is_paged(&self) -> bool {
        self.path.is_some()
    }

    fn get_at(&self, position: &[usize]) -> Result<T, LatticeError> {
        if position.len() != self.shape.len() {
            return Err(LatticeError::NdimMismatch {
                expected: self.shape.len(),
                got: position.len(),
            });
        }
        for (&idx, &dim) in position.iter().zip(self.shape.iter()) {
            if idx >= dim {
                return Err(LatticeError::IndexOutOfBounds {
                    index: position.to_vec(),
                    shape: self.shape.clone(),
                });
            }
        }
        // Use tiled_io for single-pixel read via a 1-element slice.
        if let Some(ref tio) = self.tiled_io {
            let ones = vec![1; self.shape.len()];
            let arr = tio
                .borrow_mut()
                .get_slice::<T>(position, &ones)
                .map_err(|e| LatticeError::Table(e.to_string()))?;
            return Ok(arr.into_iter().next().unwrap_or_default());
        }
        Ok(self
            .read_array()
            .map_err(|e| LatticeError::Table(e.to_string()))?[IxDyn(position)])
    }

    fn get_slice(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<T>, LatticeError> {
        let ndim = self.shape.len();
        if start.len() != ndim || shape.len() != ndim || stride.len() != ndim {
            return Err(LatticeError::NdimMismatch {
                expected: ndim,
                got: start.len(),
            });
        }
        // Tile-aware path with unit stride.
        if let Some(ref tio) = self.tiled_io {
            let is_unit_stride = stride.iter().all(|&s| s == 1);
            if is_unit_stride {
                let arr = tio
                    .borrow_mut()
                    .get_slice::<T>(start, shape)
                    .map_err(|e| LatticeError::Table(e.to_string()))?;
                return Ok(arr);
            }
        }
        let array = self
            .read_array()
            .map_err(|e| LatticeError::Table(e.to_string()))?;
        let slice_info: Vec<SliceInfoElem> = start
            .iter()
            .zip(shape.iter())
            .zip(stride.iter())
            .map(|((&s, &n), &st)| SliceInfoElem::Slice {
                start: s as isize,
                end: Some((s + n * st) as isize),
                step: st as isize,
            })
            .collect();
        Ok(array.slice(slice_info.as_slice()).to_owned())
    }

    fn get(&self) -> Result<ArrayD<T>, LatticeError> {
        // Tile-aware full read.
        if let Some(ref tio) = self.tiled_io {
            return tio
                .borrow_mut()
                .get_all::<T>()
                .map_err(|e| LatticeError::Table(e.to_string()));
        }
        self.read_array()
            .map_err(|e| LatticeError::Table(e.to_string()))
    }

    fn nice_cursor_shape(&self) -> Vec<usize> {
        self.tile_shape.clone()
    }
}

impl<T: ImagePixel> LatticeMut<T> for PagedImage<T> {
    fn put_at(&mut self, value: T, position: &[usize]) -> Result<(), LatticeError> {
        if position.len() != self.shape.len() {
            return Err(LatticeError::NdimMismatch {
                expected: self.shape.len(),
                got: position.len(),
            });
        }
        let mut array = self
            .read_array()
            .map_err(|e| LatticeError::Table(e.to_string()))?;
        let pixel =
            array
                .get_mut(IxDyn(position))
                .ok_or_else(|| LatticeError::IndexOutOfBounds {
                    index: position.to_vec(),
                    shape: self.shape.clone(),
                })?;
        *pixel = value;
        self.write_array(&array)
            .map_err(|e| LatticeError::Table(e.to_string()))
    }

    fn put_slice(&mut self, data: &ArrayD<T>, start: &[usize]) -> Result<(), LatticeError> {
        if start.len() != self.shape.len() {
            return Err(LatticeError::NdimMismatch {
                expected: self.shape.len(),
                got: start.len(),
            });
        }
        let end: Vec<usize> = start
            .iter()
            .zip(data.shape().iter())
            .map(|(&s, &n)| s + n)
            .collect();
        for (&limit, &dim) in end.iter().zip(self.shape.iter()) {
            if limit > dim {
                return Err(LatticeError::ShapeMismatch {
                    expected: self.shape.clone(),
                    got: end,
                });
            }
        }
        // Tile-aware path.
        if let Some(ref tio) = self.tiled_io {
            // Try Fortran-contiguous first (zero-copy fast path).
            let fortran_view = data.t();
            if let Some(s) = fortran_view.as_slice() {
                tio.borrow_mut()
                    .put_slice_fortran::<T>(s, start, data.shape())
                    .map_err(|e| LatticeError::Table(e.to_string()))?;
                return Ok(());
            }
            // C-order input: use the C-order put method.
            let contiguous = data.as_standard_layout();
            let slice = contiguous.as_slice().expect("contiguous C-order data");
            tio.borrow_mut()
                .put_slice_c_order::<T>(slice, start, data.shape())
                .map_err(|e| LatticeError::Table(e.to_string()))?;
            return Ok(());
        }
        let mut array = self
            .read_array()
            .map_err(|e| LatticeError::Table(e.to_string()))?;
        {
            let mut view = array.slice_each_axis_mut(|axis| {
                let idx = axis.axis.index();
                Slice::from(start[idx] as isize..end[idx] as isize)
            });
            view.assign(data);
        }
        self.write_array(&array)
            .map_err(|e| LatticeError::Table(e.to_string()))
    }

    fn set(&mut self, value: T) -> Result<(), LatticeError> {
        let array = ArrayD::from_elem(IxDyn(&self.shape), value);
        self.write_array(&array)
            .map_err(|e| LatticeError::Table(e.to_string()))
    }
}

fn make_paged_mask_record(relative_mask_path: &str, shape: &[usize]) -> RecordValue {
    let mut record = RecordValue::default();
    record.upsert(
        REGION_TYPE_FIELD,
        Value::Scalar(ScalarValue::Int32(REGION_TYPE_LC)),
    );
    record.upsert(
        REGION_NAME_FIELD,
        Value::Scalar(ScalarValue::String(LCPAGEDMASK_NAME.into())),
    );
    record.upsert(
        REGION_COMMENT_FIELD,
        Value::Scalar(ScalarValue::String(String::new())),
    );
    record.upsert("mask", Value::TableRef(relative_mask_path.to_string()));
    record.upsert("box", Value::Record(make_lcbox_record(shape)));
    record
}

fn make_lcbox_record(shape: &[usize]) -> RecordValue {
    let blc: Vec<f32> = vec![1.0; shape.len()];
    let trc: Vec<f32> = shape.iter().map(|&dim| dim as f32).collect();
    let shape_i32: Vec<i32> = shape.iter().map(|&dim| dim as i32).collect();
    let mut record = RecordValue::default();
    record.upsert(
        REGION_TYPE_FIELD,
        Value::Scalar(ScalarValue::Int32(REGION_TYPE_LC)),
    );
    record.upsert(
        REGION_NAME_FIELD,
        Value::Scalar(ScalarValue::String(LCBOX_NAME.into())),
    );
    record.upsert(
        REGION_COMMENT_FIELD,
        Value::Scalar(ScalarValue::String(String::new())),
    );
    record.upsert("oneRel", Value::Scalar(ScalarValue::Bool(true)));
    record.upsert("blc", Value::Array(ArrayValue::from_f32_vec(blc)));
    record.upsert("trc", Value::Array(ArrayValue::from_f32_vec(trc)));
    record.upsert("shape", Value::Array(ArrayValue::from_i32_vec(shape_i32)));
    record
}

fn mask_table_reference(image_path: &Path, mask_name: &str) -> String {
    image_path.join(mask_name).to_string_lossy().into_owned()
}

fn resolve_mask_table_path(image_path: &Path, stored_path: &str) -> PathBuf {
    let stored = PathBuf::from(stored_path);
    if stored.is_absolute() {
        return stored;
    }

    if let Some(image_name) = image_path.file_name() {
        if stored.starts_with(Path::new(image_name)) {
            return image_path
                .parent()
                .map(|parent| parent.join(&stored))
                .unwrap_or(stored);
        }
    }

    image_path.join(stored)
}

fn read_mask_entry(
    value: &Value,
    image_path: Option<&Path>,
    expected_shape: &[usize],
) -> Result<ArrayD<bool>, ImageError> {
    match value {
        Value::Array(ArrayValue::Bool(array)) => {
            if array.shape() != expected_shape {
                return Err(ImageError::ShapeMismatch {
                    expected: expected_shape.to_vec(),
                    got: array.shape().to_vec(),
                });
            }
            Ok(array.clone())
        }
        Value::Record(record) => {
            let relative_path = match record.get("mask") {
                Some(Value::TableRef(path)) => path.clone(),
                Some(Value::Scalar(ScalarValue::String(path))) => path.clone(),
                _ => {
                    return Err(ImageError::InvalidMetadata(
                        "mask record is missing table reference".to_string(),
                    ));
                }
            };
            let image_path = image_path.ok_or(ImageError::NotPersistent)?;
            let mask =
                PagedArray::<bool>::open(resolve_mask_table_path(image_path, &relative_path))?;
            let array = mask.get()?;
            if array.shape() != expected_shape {
                return Err(ImageError::ShapeMismatch {
                    expected: expected_shape.to_vec(),
                    got: array.shape().to_vec(),
                });
            }
            Ok(array)
        }
        _ => Err(ImageError::InvalidMetadata(
            "mask keyword is neither a bool array nor a paged-mask record".to_string(),
        )),
    }
}

fn to_array_value<T: ImagePixel>(array: &ArrayD<T>) -> ArrayValue {
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

fn from_array_value<T: ImagePixel>(value: ArrayValue) -> Result<ArrayD<T>, ImageError> {
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

fn unsafe_cast_ref<From: 'static, To: 'static>(array: &ArrayD<From>) -> &ArrayD<To> {
    assert_eq!(TypeId::of::<From>(), TypeId::of::<To>());
    unsafe { &*(std::ptr::from_ref(array) as *const ArrayD<To>) }
}

fn unsafe_cast_owned<From: 'static, To: 'static>(array: ArrayD<From>) -> ArrayD<To> {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_coords() -> CoordinateSystem {
        CoordinateSystem::new()
    }

    #[test]
    fn resolves_bare_and_cxx_style_mask_table_refs() {
        let image_path = Path::new("/tmp/demo.image");
        assert_eq!(
            resolve_mask_table_path(image_path, "flags"),
            PathBuf::from("/tmp/demo.image/flags")
        );
        assert_eq!(
            resolve_mask_table_path(image_path, "demo.image/flags"),
            PathBuf::from("/tmp/demo.image/flags")
        );
    }

    #[test]
    fn float32_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f32.image");
        let mut image = PagedImage::<f32>::create(vec![4, 4], make_coords(), &path).unwrap();
        image.put_at(1.5, &[1, 2]).unwrap();
        image.add_history("hello").unwrap();
        image.make_mask("mask0", true, true).unwrap();
        image.save().unwrap();

        let reopened = PagedImage::<f32>::open(&path).unwrap();
        assert_eq!(reopened.get_at(&[1, 2]).unwrap(), 1.5);
        assert_eq!(reopened.history().unwrap(), vec!["hello".to_string()]);
        assert_eq!(reopened.default_mask_name().as_deref(), Some("mask0"));
        assert_eq!(image_pixel_type(&path).unwrap(), ImagePixelType::Float32);
    }

    #[test]
    fn float64_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f64.image");
        let mut image = PagedImage::<f64>::create(vec![2, 2], make_coords(), &path).unwrap();
        image.set(2.5).unwrap();
        image.save().unwrap();
        let reopened = PagedImage::<f64>::open(&path).unwrap();
        assert_eq!(reopened.get_at(&[0, 0]).unwrap(), 2.5);
        assert_eq!(image_pixel_type(&path).unwrap(), ImagePixelType::Float64);
    }

    #[test]
    fn complex32_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c32.image");
        let value = Complex32::new(1.0, -2.0);
        let mut image = PagedImage::<Complex32>::create(vec![2, 2], make_coords(), &path).unwrap();
        image.put_at(value, &[1, 1]).unwrap();
        image.save().unwrap();
        let reopened = PagedImage::<Complex32>::open(&path).unwrap();
        assert_eq!(reopened.get_at(&[1, 1]).unwrap(), value);
        assert_eq!(image_pixel_type(&path).unwrap(), ImagePixelType::Complex32);
    }

    #[test]
    fn complex64_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c64.image");
        let value = Complex64::new(3.0, -4.0);
        let mut image = PagedImage::<Complex64>::create(vec![2, 2], make_coords(), &path).unwrap();
        image.put_at(value, &[0, 1]).unwrap();
        image.save().unwrap();
        let reopened = PagedImage::<Complex64>::open(&path).unwrap();
        assert_eq!(reopened.get_at(&[0, 1]).unwrap(), value);
        assert_eq!(image_pixel_type(&path).unwrap(), ImagePixelType::Complex64);
    }

    #[test]
    fn temp_image_materializes_masks_and_history() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("temp.image");
        let mut image = crate::TempImage::<f32>::new(vec![3, 3], make_coords()).unwrap();
        image.make_mask("quality", true, true).unwrap();
        image.add_history("temp").unwrap();
        let _paged = image.save_as(&path).unwrap();

        let reopened = PagedImage::<f32>::open(&path).unwrap();
        assert_eq!(reopened.default_mask_name().as_deref(), Some("quality"));
        let mask = reopened.get_mask().unwrap().unwrap();
        assert!(mask.iter().all(|&value| value));
        assert_eq!(reopened.history().unwrap(), vec!["temp".to_string()]);
    }

    #[test]
    fn default_mask_requires_existing_name() {
        let mut image = crate::TempImage::<f32>::new(vec![2, 2], make_coords()).unwrap();
        assert!(matches!(
            image.set_default_mask("missing"),
            Err(ImageError::MaskNotFound(_))
        ));
    }

    #[test]
    fn open_any_dispatches_on_pixel_type() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dispatch.image");
        let mut image = PagedImage::<f64>::create(vec![2, 2], make_coords(), &path).unwrap();
        image.save().unwrap();

        let opened = AnyPagedImage::open(&path).unwrap();
        assert_eq!(opened.pixel_type(), ImagePixelType::Float64);
        assert_eq!(opened.shape(), &[2, 2]);
    }

    #[test]
    fn tiled_io_plane_by_plane_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tiled.image");
        let size = 16;
        let shape = vec![size, size, size];
        let tile_shape = vec![8, 8, 8];

        let mut img = PagedImage::<f32>::create_with_tile_shape(
            shape.clone(),
            tile_shape,
            make_coords(),
            &path,
        )
        .unwrap();

        // Write plane by plane (z-planes).
        for z in 0..size {
            let plane = ArrayD::from_shape_fn(IxDyn(&[size, size, 1]), |idx| {
                (idx[0] + idx[1] * size + z * size * size) as f32
            });
            img.put_slice(&plane, &[0, 0, z]).unwrap();
        }
        img.save().unwrap();

        // Read back and verify.
        let img2 = PagedImage::<f32>::open(&path).unwrap();
        for z in 0..size {
            let plane = img2.get_slice(&[0, 0, z], &[size, size, 1]).unwrap();
            for x in 0..size {
                for y in 0..size {
                    let expected = (x + y * size + z * size * size) as f32;
                    assert_eq!(plane[[x, y, 0]], expected, "mismatch at [{x}, {y}, {z}]");
                }
            }
        }

        // Also test full read.
        let full = img2.get().unwrap();
        eprintln!(
            "full shape: {:?}, expected: {:?}",
            full.shape(),
            &[size, size, size]
        );
        assert_eq!(full.shape(), &[size, size, size]);
        for z in 0..size {
            for y in 0..size {
                for x in 0..size {
                    let expected = (x + y * size + z * size * size) as f32;
                    assert_eq!(full[[x, y, z]], expected, "full mismatch at [{x},{y},{z}]");
                }
            }
        }
    }
}
