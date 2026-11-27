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

use std::any::{Any, TypeId};
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use casa_coordinates::{CoordinateSystem, CoordinateType};
use casa_core::{
    ArrayD, ArrayValue, Complex32, Complex64, PrimitiveType, RecordField, RecordValue, ScalarValue,
    Value,
};
use casa_lattices::{
    ArrayLattice, Lattice, LatticeElement, LatticeError, LatticeMut, PagedArray, TiledShape,
    TraversalCacheHint, TraversalCacheScope, TraversalCursorIter, TraversalSpec,
    recommended_tile_cache_size,
};
use casa_tables::{
    ColumnSchema, DEFAULT_TILED_ARRAY_CACHE_BYTES, DataManagerKind, Table, TableInfo, TableOptions,
    TableSchema, TableWriteLock, TilePixel, TiledArrayStorage, TiledFileIoStats,
};
use ndarray::{ArrayViewD, IxDyn, Slice, SliceInfoElem};

use crate::error::ImageError;
use crate::image_expr::ImageExprBuilder;
use crate::image_info::ImageInfo;
use crate::mask_reference::{point_mask_references_inside, subtable_reference};
use crate::subimage::{SubImage, SubImageMut};

mod history;
mod interfaces;
mod masks;
mod metadata;
mod storage;

pub use history::add_image_history;
pub use history::append_image_history_entries;
pub use history::image_history_entries;
pub use history::image_history_rows;
pub(crate) use masks::assign_mask_slice;
pub use masks::make_paged_mask_record;
pub(crate) use masks::slice_mask_array;
pub(crate) use masks::validate_mask_slice_write;
pub(crate) use masks::validate_new_mask_name;
pub use storage::image_pixel_type;
use storage::*;

const MAP_COLUMN: &str = "map";
pub(crate) const REGIONS_KEYWORD: &str = "regions";
const MASKS_KEYWORD: &str = "masks";
const DEFAULT_MASK_KEYWORD: &str = "Image_defaultmask";
const LOGTABLE_KEYWORD: &str = "logtable";
const LOGTABLE_NAME: &str = "logtable";

/// One image history entry, retaining its original CASA log time and origin.
#[derive(Debug, Clone, PartialEq)]
pub struct ImageHistoryEntry {
    /// Time in seconds since MJD zero.
    pub time_mjd_seconds: f64,
    /// Original CASA log priority.
    pub priority: String,
    /// Original CASA log location.
    pub location: String,
    /// History message.
    pub message: String,
}

/// Complete casacore log metadata retained when importing history records.
#[derive(Debug, Clone, PartialEq)]
pub struct ImageHistoryRow {
    /// Time, priority, source location and message of the entry.
    pub entry: ImageHistoryEntry,
    /// Object identifier as stored in the CASA log table.
    pub object_id: String,
}

impl ImageHistoryRow {
    pub(crate) fn informational(entry: ImageHistoryEntry) -> Self {
        Self {
            entry,
            object_id: String::new(),
        }
    }
}

impl ImageHistoryEntry {
    pub fn current_time() -> Result<f64, ImageError> {
        const UNIX_EPOCH_MJD_SECONDS: f64 = 40_587.0 * 86_400.0;
        let duration = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|error| ImageError::Io(error.to_string()))?;
        Ok(UNIX_EPOCH_MJD_SECONDS + duration.as_secs_f64())
    }

    /// Origin used when casacore replays this entry through `ImageHistory`.
    ///
    /// An empty location becomes `ImageHistory::get`. Otherwise casacore
    /// treats the stored location as a global function name, which adds a
    /// `::` prefix when the log origin is rendered.
    #[must_use]
    pub fn replay_location(&self) -> String {
        if self.location.is_empty() {
            "ImageHistory::get".into()
        } else {
            format!("::{}", self.location)
        }
    }
}

#[cfg(unix)]
mod locking;

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
        Self::open_with_cache(path, 0)
    }

    /// Opens an image from disk with an explicit cache size limit.
    pub fn open_with_cache(
        path: impl AsRef<Path>,
        max_cache_bytes: usize,
    ) -> Result<Self, ImageError> {
        let path = path.as_ref().to_path_buf();
        let table = Table::open_metadata_only(TableOptions::new(&path))?;
        let pixel_type = ImagePixelType::from_primitive_type(
            PagedImage::<f32>::map_column_primitive_type(&table, None)?,
        )?;
        match pixel_type {
            ImagePixelType::Float32 => Ok(Self::Float32(PagedImage::open_with_cache(
                &path,
                max_cache_bytes,
            )?)),
            ImagePixelType::Float64 => Ok(Self::Float64(PagedImage::open_with_cache(
                &path,
                max_cache_bytes,
            )?)),
            ImagePixelType::Complex32 => Ok(Self::Complex32(PagedImage::open_with_cache(
                &path,
                max_cache_bytes,
            )?)),
            ImagePixelType::Complex64 => Ok(Self::Complex64(PagedImage::open_with_cache(
                &path,
                max_cache_bytes,
            )?)),
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

    pub fn coordinates(&self) -> &CoordinateSystem {
        match self {
            Self::Float32(image) => image.coordinates(),
            Self::Float64(image) => image.coordinates(),
            Self::Complex32(image) => image.coordinates(),
            Self::Complex64(image) => image.coordinates(),
        }
    }

    /// Returns the names of all stored image-attached regions.
    pub fn region_names(&self) -> Vec<String> {
        match self {
            Self::Float32(image) => image.region_names(),
            Self::Float64(image) => image.region_names(),
            Self::Complex32(image) => image.region_names(),
            Self::Complex64(image) => image.region_names(),
        }
    }

    /// Returns a named stored image-attached region record.
    pub fn get_region_record(&self, name: &str) -> Result<RecordValue, ImageError> {
        match self {
            Self::Float32(image) => image.get_region_record(name),
            Self::Float64(image) => image.get_region_record(name),
            Self::Complex32(image) => image.get_region_record(name),
            Self::Complex64(image) => image.get_region_record(name),
        }
    }

    /// Returns the configured default mask name, if any.
    pub fn default_mask_name(&self) -> Option<String> {
        match self {
            Self::Float32(image) => image.default_mask_name(),
            Self::Float64(image) => image.default_mask_name(),
            Self::Complex32(image) => image.default_mask_name(),
            Self::Complex64(image) => image.default_mask_name(),
        }
    }

    /// Returns the names of all stored masks.
    pub fn mask_names(&self) -> Vec<String> {
        match self {
            Self::Float32(image) => image.mask_names(),
            Self::Float64(image) => image.mask_names(),
            Self::Complex32(image) => image.mask_names(),
            Self::Complex64(image) => image.mask_names(),
        }
    }

    /// Returns a slice from the default mask, if one is configured.
    pub fn get_mask_slice(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<Option<ArrayD<bool>>, ImageError> {
        match self {
            Self::Float32(image) => image.get_mask_slice(start, shape, stride),
            Self::Float64(image) => image.get_mask_slice(start, shape, stride),
            Self::Complex32(image) => image.get_mask_slice(start, shape, stride),
            Self::Complex64(image) => image.get_mask_slice(start, shape, stride),
        }
    }

    /// Sets the named default mask.
    pub fn set_default_mask(&mut self, name: &str) -> Result<(), ImageError> {
        match self {
            Self::Float32(image) => image.set_default_mask(name),
            Self::Float64(image) => image.set_default_mask(name),
            Self::Complex32(image) => image.set_default_mask(name),
            Self::Complex64(image) => image.set_default_mask(name),
        }
    }

    /// Unsets the configured default mask.
    pub fn unset_default_mask(&mut self) -> Result<(), ImageError> {
        match self {
            Self::Float32(image) => image.unset_default_mask(),
            Self::Float64(image) => image.unset_default_mask(),
            Self::Complex32(image) => image.unset_default_mask(),
            Self::Complex64(image) => image.unset_default_mask(),
        }
    }

    /// Removes a named mask.
    pub fn remove_mask(&mut self, name: &str) -> Result<(), ImageError> {
        match self {
            Self::Float32(image) => image.remove_mask(name),
            Self::Float64(image) => image.remove_mask(name),
            Self::Complex32(image) => image.remove_mask(name),
            Self::Complex64(image) => image.remove_mask(name),
        }
    }

    /// Flushes the image metadata and payload to disk.
    pub fn save(&mut self) -> Result<(), ImageError> {
        match self {
            Self::Float32(image) => image.save(),
            Self::Float64(image) => image.save(),
            Self::Complex32(image) => image.save(),
            Self::Complex64(image) => image.save(),
        }
    }
}

/// Transactional updates to the named mask tables attached to an image.
pub trait StoredImageMasks {
    /// Store a named mask and optionally select it.
    fn define_mask(
        &self,
        requested_name: Option<&str>,
        overwrite: bool,
        set_default: bool,
        image_shape: &[usize],
        data: &ArrayD<bool>,
    ) -> Result<(), ImageError>;
    /// Create a named mask filled with a constant value.
    fn create_filled_mask(
        &self,
        name: &str,
        image_shape: &[usize],
        value: bool,
    ) -> Result<(), ImageError>;
    /// Change or clear the default named mask.
    fn set_default_mask(&self, name: Option<&str>, image_shape: &[usize])
    -> Result<(), ImageError>;
    /// Rename a stored mask, retaining its selection.
    fn rename_mask(&self, from: &str, to: &str) -> Result<(), ImageError>;
    /// Remove a stored mask.
    fn remove_mask(&self, name: &str) -> Result<(), ImageError>;
}

/// Common image metadata behavior shared by persistent and temporary images.
pub trait ImageInterface<T: ImagePixel>: Lattice<T> {
    /// Returns this image as [`Any`] for crate-level downcasting when the
    /// concrete type is `'static`.
    ///
    /// The compiled image-expression runtime uses this to recognize concrete,
    /// `'static` source types such as [`PagedImage<T>`]. Borrowed view-like
    /// sources that are not `'static` still fall back to snapshot compilation.
    fn as_any(&self) -> Option<&dyn Any>;
    /// Directory containing the persistent logtable shared with an attached image view.
    fn history_directory(&self) -> Option<&Path> {
        None
    }

    /// Named mask tables managed by this image.
    fn stored_masks(&self) -> Option<&dyn StoredImageMasks> {
        None
    }

    /// Validate a brightness unit before an attached view changes it.
    fn validate_units(&self, _units: &str) -> Result<(), ImageError> {
        Ok(())
    }

    /// Validate and persist an attached view's beam and image type metadata.
    fn store_image_info(
        &self,
        _info: &ImageInfo,
        _record: Option<&RecordValue>,
    ) -> Result<(), ImageError> {
        Ok(())
    }

    /// Persist an attached view's miscellaneous metadata.
    fn store_misc_info(&self, _record: &RecordValue) -> Result<(), ImageError> {
        Ok(())
    }

    /// Rebuild a virtual image with new coordinates and store its native record.
    fn update_source_coordinates(
        &mut self,
        _coords: CoordinateSystem,
        _record: &RecordValue,
    ) -> Result<(), ImageError> {
        Err(ImageError::CasaSelection(
            "A component-list source is required".into(),
        ))
    }

    /// Configure evaluated-section caching on a virtual source.
    fn set_source_cache(&mut self, _enabled: bool) -> Result<(), ImageError> {
        Err(ImageError::CasaSelection(
            "A component-list source is required".into(),
        ))
    }

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

    /// Returns the configured default mask name, if any.
    fn default_mask_name(&self) -> Option<String> {
        None
    }

    /// Returns the names of stored masks.
    fn mask_names(&self) -> Vec<String> {
        self.default_mask_name().into_iter().collect()
    }

    /// Returns a named mask as a full-image boolean array.
    fn get_named_mask(&self, name: &str) -> Result<ArrayD<bool>, ImageError> {
        if self.default_mask_name().as_deref() == Some(name) {
            return self
                .default_mask()?
                .ok_or_else(|| ImageError::MaskNotFound(name.to_string()));
        }
        Err(ImageError::MaskNotFound(name.to_string()))
    }

    /// Returns a named mask slice, mirroring casacore `getMaskSlice`.
    ///
    /// The default implementation slices the fully materialized named mask.
    /// Persistent images should override this to route through paged storage.
    fn get_named_mask_slice(
        &self,
        name: &str,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<bool>, ImageError> {
        slice_mask_array(&self.get_named_mask(name)?, start, shape, stride)
    }

    /// Returns the history entries associated with the image.
    fn history(&self) -> Result<Vec<String>, ImageError> {
        Ok(Vec::new())
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

    /// Returns the coordinate type for each image axis.
    fn axis_types(&self) -> Vec<CoordinateType> {
        let coords = self.coordinates();
        (0..self.ndim().max(coords.n_pixel_axes()))
            .map(|axis| {
                coords
                    .pixel_axis_owner(axis)
                    .map_or(CoordinateType::Linear, |(index, _)| {
                        coords.coordinate(index).coordinate_type()
                    })
            })
            .collect()
    }

    /// Returns the axis names for the image.
    fn axis_names(&self) -> Vec<String> {
        let coords = self.coordinates();
        (0..self.ndim().max(coords.n_pixel_axes()))
            .map(|axis| {
                coords
                    .pixel_axis_owner(axis)
                    .and_then(|(index, local)| {
                        coords.coordinate(index).axis_names().into_iter().nth(local)
                    })
                    .unwrap_or_else(|| format!("Axis{axis}"))
            })
            .collect()
    }

    /// Finds the first axis belonging to a given coordinate type.
    fn find_axis(&self, coord_type: CoordinateType) -> Option<usize> {
        let coords = self.coordinates();
        (0..coords.n_pixel_axes()).find(|&axis| {
            coords
                .pixel_axis_owner(axis)
                .is_some_and(|(index, _)| coords.coordinate(index).coordinate_type() == coord_type)
        })
    }

    /// Finds an axis by case-insensitive name prefix.
    fn find_axis_by_name(&self, name: &str) -> Option<usize> {
        let target = name.to_lowercase();
        self.axis_names()
            .iter()
            .position(|axis| axis.to_lowercase().starts_with(&target))
    }

    /// Extracts a single plane with the target axis fixed to `index`.
    fn get_plane(&self, axis: usize, index: usize) -> Result<ArrayD<T>, ImageError> {
        if axis >= self.ndim() {
            return Err(ImageError::ShapeMismatch {
                expected: self.shape().to_vec(),
                got: vec![axis],
            });
        }
        let mut start = vec![0; self.ndim()];
        let mut shape = self.shape().to_vec();
        start[axis] = index;
        shape[axis] = 1;
        self.get_slice(&start, &shape, &vec![1; self.ndim()])
            .map_err(Into::into)
    }

    /// Extracts a spectral-channel plane if the image has a spectral axis.
    fn channel_plane(&self, chan: usize) -> Result<Option<ArrayD<T>>, ImageError> {
        match self.find_axis(CoordinateType::Spectral) {
            Some(axis) => Ok(Some(self.get_plane(axis, chan)?)),
            None => Ok(None),
        }
    }

    /// Extracts a Stokes plane if the image has a Stokes axis.
    fn stokes_plane(&self, stokes: usize) -> Result<Option<ArrayD<T>>, ImageError> {
        match self.find_axis(CoordinateType::Stokes) {
            Some(axis) => Ok(Some(self.get_plane(axis, stokes)?)),
            None => Ok(None),
        }
    }

    /// Returns `true` if the image has a default pixel mask.
    fn has_pixel_mask(&self) -> bool {
        self.default_mask_name().is_some()
            || self
                .default_mask()
                .map(|mask| mask.is_some())
                .unwrap_or(false)
    }

    /// Returns the default mask contents if a default mask is configured.
    fn get_mask(&self) -> Result<Option<ArrayD<bool>>, ImageError> {
        match self.default_mask_name() {
            Some(name) => Ok(Some(self.get_named_mask(&name)?)),
            None => self.default_mask(),
        }
    }

    /// Returns a slice from the default mask if one is configured.
    fn get_mask_slice(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<Option<ArrayD<bool>>, ImageError> {
        match self.default_mask_name() {
            Some(name) => Ok(Some(
                self.get_named_mask_slice(&name, start, shape, stride)?,
            )),
            None => self
                .default_mask()?
                .map(|mask| slice_mask_array(&mask, start, shape, stride))
                .transpose(),
        }
    }

    /// Creates a unit-stride subimage view.
    fn sub_image(
        &self,
        start: Vec<usize>,
        shape: Vec<usize>,
    ) -> Result<SubImage<'_, T, Self>, ImageError>
    where
        Self: Sized,
    {
        SubImage::new(self, start, shape)
    }

    /// Creates a strided subimage view.
    fn sub_image_with_stride(
        &self,
        start: Vec<usize>,
        shape: Vec<usize>,
        stride: Vec<usize>,
    ) -> Result<SubImage<'_, T, Self>, ImageError>
    where
        Self: Sized,
    {
        SubImage::with_stride(self, start, shape, stride)
    }

    /// Starts a lazy expression rooted at this image.
    fn expr(&self) -> Result<ImageExprBuilder<'_, T>, ImageError>
    where
        Self: Sized,
        T: crate::image_expr::ImageExprValue + PartialOrd,
    {
        ImageExprBuilder::from_image(self)
    }

    /// Creates a lazy expression by mapping a function over this image.
    fn expr_map<F>(&self, f: F) -> Result<ImageExprBuilder<'_, T>, ImageError>
    where
        Self: Sized,
        T: crate::image_expr::ImageExprValue + PartialOrd,
        F: Fn(T) -> T + Send + Sync + 'static,
    {
        ImageExprBuilder::map(self, f)
    }
}

/// Common mutable image metadata and mask/history behavior.
pub trait MutableImageInterface<T: ImagePixel>: ImageInterface<T> + LatticeMut<T> {
    /// Replaces the coordinate system.
    fn set_coordinates(&mut self, coords: CoordinateSystem) -> Result<(), ImageError>;

    /// Replaces the brightness unit string using an owned string.
    fn set_units_string(&mut self, units: String) -> Result<(), ImageError>;

    /// Replaces the image info record.
    fn set_image_info(&mut self, info: &ImageInfo) -> Result<(), ImageError>;

    /// Replaces image info from its native record. Native storage can retain
    /// saved quantity values and units alongside the numerical model.
    fn set_image_info_record(&mut self, record: RecordValue) -> Result<(), ImageError> {
        self.set_image_info(&ImageInfo::from_record(&record)?)
    }

    /// Replaces the misc-info record.
    fn set_misc_info(&mut self, rec: RecordValue) -> Result<(), ImageError>;

    /// Replaces a named mask.
    fn put_mask(&mut self, name: &str, data: &ArrayD<bool>) -> Result<(), ImageError>;

    /// Creates a named mask with uniform validity.
    fn create_filled_mask(&mut self, name: &str, value: bool) -> Result<(), ImageError> {
        validate_new_mask_name(name, &self.mask_names())?;
        self.put_mask(name, &ArrayD::from_elem(IxDyn(self.shape()), value))
    }

    /// Creates the active pixel mask of a new output with uniform validity.
    /// Returns the name to use for slice writes, empty for an anonymous mask.
    ///
    /// # Errors
    /// The image cannot create or activate the mask.
    fn create_filled_pixel_mask(&mut self, value: bool) -> Result<String, ImageError> {
        let name = "mask0";
        self.create_filled_mask(name, value)?;
        self.set_default_mask(name)?;
        Ok(name.to_owned())
    }

    /// Whether an existing named mask accepts writes.
    fn named_mask_is_writable(&self, name: &str) -> Result<bool, ImageError> {
        Ok(self.mask_names().iter().any(|entry| entry == name))
    }

    /// Mask target for in-place pixel edits. An empty name denotes a
    /// temporary image's writable anonymous mask.
    fn active_pixel_mask_name(&self) -> Option<String> {
        self.default_mask_name()
    }

    /// Edit several sections of an existing mask through one writer.
    fn with_mask_slice_writer(
        &mut self,
        name: &str,
        edit: impl FnOnce(&mut MaskSliceWriter<'_>) -> Result<(), ImageError>,
    ) -> Result<(), ImageError>
    where
        Self: Sized,
    {
        edit(&mut |data, start| self.put_mask_slice(name, data, start))
    }

    /// Write a unit-stride section of a named mask. New masks start false.
    fn put_mask_slice(
        &mut self,
        name: &str,
        data: &ArrayD<bool>,
        start: &[usize],
    ) -> Result<(), ImageError> {
        validate_mask_slice_write(self.shape(), data.shape(), start)?;
        let mut mask = match self.get_named_mask(name) {
            Ok(mask) => mask,
            Err(ImageError::MaskNotFound(_)) => ArrayD::from_elem(IxDyn(self.shape()), false),
            Err(error) => return Err(error),
        };
        assign_mask_slice(&mut mask, data, start)?;
        self.put_mask(name, &mask)
    }

    /// Removes a named mask.
    fn remove_mask(&mut self, name: &str) -> Result<(), ImageError>;

    /// Sets the default mask, validating that it exists.
    fn set_default_mask(&mut self, name: &str) -> Result<(), ImageError>;

    /// Unsets the default mask.
    fn unset_default_mask(&mut self) -> Result<(), ImageError>;

    /// Appends a history message using an owned string.
    fn add_history_entry(&mut self, msg: String) -> Result<(), ImageError>;

    /// Clears the history log.
    fn clear_history(&mut self) -> Result<(), ImageError>;

    /// Replaces the brightness unit string.
    fn set_units(&mut self, units: impl Into<String>) -> Result<(), ImageError>
    where
        Self: Sized,
    {
        self.set_units_string(units.into())
    }

    /// Appends a history message.
    fn add_history(&mut self, msg: impl Into<String>) -> Result<(), ImageError>
    where
        Self: Sized,
    {
        self.add_history_entry(msg.into())
    }

    /// Creates a full-image mask with the given name.
    fn make_mask(
        &mut self,
        name: impl Into<String>,
        set_default: bool,
        initial: bool,
    ) -> Result<(), ImageError>
    where
        Self: Sized,
    {
        let name = name.into();
        let mask = ArrayD::from_elem(IxDyn(self.shape()), initial);
        self.put_mask(&name, &mask)?;
        if set_default {
            self.set_default_mask(&name)?;
        }
        Ok(())
    }

    /// Creates a mutable unit-stride subimage view.
    fn sub_image_mut(
        &mut self,
        start: Vec<usize>,
        shape: Vec<usize>,
    ) -> Result<SubImageMut<'_, T, Self>, ImageError>
    where
        Self: Sized,
    {
        SubImageMut::new(self, start, shape)
    }

    /// Creates a mutable strided subimage view.
    fn sub_image_mut_with_stride(
        &mut self,
        start: Vec<usize>,
        shape: Vec<usize>,
        stride: Vec<usize>,
    ) -> Result<SubImageMut<'_, T, Self>, ImageError>
    where
        Self: Sized,
    {
        SubImageMut::with_stride(self, start, shape, stride)
    }
}

/// Write a contiguous section of a named mask at its image pixel origin.
pub type MaskSliceWriter<'a> = dyn FnMut(&ArrayD<bool>, &[usize]) -> Result<(), ImageError> + 'a;

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
    persistent_masks: RefCell<BTreeMap<String, PagedArray<bool>>>,
    temp_history: Vec<String>,
    /// When `Some`, pixel I/O goes directly through tile-level file access,
    /// bypassing the `Table` cell abstraction. Enabled by `create_with_tile_shape`
    /// or when opening an on-disk image that was created with it.
    /// Wrapped in `RefCell` for interior mutability since the tile cache
    /// needs mutation even through `&self` trait methods.
    tiled_io: Option<RefCell<TiledArrayStorage>>,
    max_cache_bytes: Cell<usize>,
    advisory_locks: bool,
    #[cfg(unix)]
    cached_modify_counter: Option<u32>,
    #[cfg(unix)]
    pending_tile_writes: bool,
    _pixel: PhantomData<T>,
}

struct PagedImageTraversalCacheScope<'a, T: ImagePixel> {
    image: &'a PagedImage<T>,
    previous_cache_bytes: usize,
}

impl<T: ImagePixel> TraversalCacheScope for PagedImageTraversalCacheScope<'_, T> {}

impl<T: ImagePixel> Drop for PagedImageTraversalCacheScope<'_, T> {
    fn drop(&mut self) {
        let _ = self.image.set_cache_bytes_shared(self.previous_cache_bytes);
    }
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
    /// Detects the pixel type of an image on disk without opening it as a
    /// specific `PagedImage<T>`.
    pub fn pixel_type(path: impl AsRef<Path>) -> Result<ImagePixelType, ImageError> {
        let tiled_io = Self::open_tiled_io(path.as_ref(), 0).ok().map(RefCell::new);
        let table = if tiled_io.is_some() {
            Table::open_metadata_only(TableOptions::new(path.as_ref()))?
        } else {
            Table::open(TableOptions::new(path.as_ref()))?
        };
        ImagePixelType::from_primitive_type(Self::map_column_primitive_type(
            &table,
            tiled_io.as_ref(),
        )?)
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

    /// Whether this image can be changed through the current attachment.
    pub fn is_writable(&self) -> bool {
        !self.advisory_locks
    }

    fn check_writable(&self) -> Result<(), ImageError> {
        if !self.is_writable() {
            let path = self.path.as_ref().expect("advisory image has a table path");
            return Err(ImageError::CasaSelection(format!(
                "Table {} cannot be opened for read/write",
                path.display()
            )));
        }
        Ok(())
    }

    /// A read-only directory without a lock file reports advisory locks.
    pub fn uses_advisory_locks(&self) -> bool {
        self.advisory_locks
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

    pub(crate) fn cache_bytes(&self) -> usize {
        self.max_cache_bytes.get()
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

    /// Writes a borrowed full-value slice at the given start position.
    pub fn put_slice_view(
        &mut self,
        data: ArrayViewD<'_, T>,
        start: &[usize],
    ) -> Result<(), ImageError> {
        paged_image_put_slice_view(self, data, start).map_err(Into::into)
    }

    /// Returns current tiled-I/O diagnostic counters for this image.
    pub fn tiled_io_stats(&self) -> Option<TiledFileIoStats> {
        self.tiled_io
            .as_ref()
            .map(|tiled_io| tiled_io.borrow().io_stats())
    }

    /// Bytes currently owned by the bounded tile cache and direct-I/O buffer.
    pub fn tiled_storage_resident_bytes(&self) -> Option<usize> {
        self.tiled_io
            .as_ref()
            .map(|tiled_io| tiled_io.borrow().owned_resident_bytes())
    }

    /// Reload pixel values on their next read while keeping the tiled file open.
    /// Pending pixel writes retain their cache until they are flushed.
    pub fn discard_clean_pixel_cache(&self) {
        if let Some(tiled_io) = &self.tiled_io {
            tiled_io.borrow_mut().discard_clean_read_cache();
        }
    }

    /// Clears tiled-I/O diagnostic counters for this image.
    pub fn reset_tiled_io_stats(&mut self) {
        if let Some(tiled_io) = &self.tiled_io {
            tiled_io.borrow_mut().reset_io_stats();
        }
    }

    /// Writes a single pixel.
    pub fn put_at(&mut self, value: T, pos: &[usize]) -> Result<(), ImageError> {
        <Self as LatticeMut<T>>::put_at(self, value, pos).map_err(Into::into)
    }

    /// Sets all pixels to the same value.
    pub fn set(&mut self, value: T) -> Result<(), ImageError> {
        <Self as LatticeMut<T>>::set(self, value).map_err(Into::into)
    }

    /// Returns the coordinate type for each image axis.
    pub fn axis_types(&self) -> Vec<CoordinateType> {
        <Self as ImageInterface<T>>::axis_types(self)
    }

    /// Returns the axis names for the image.
    pub fn axis_names(&self) -> Vec<String> {
        <Self as ImageInterface<T>>::axis_names(self)
    }

    /// Finds the first axis belonging to a given coordinate type.
    pub fn find_axis(&self, coord_type: CoordinateType) -> Option<usize> {
        <Self as ImageInterface<T>>::find_axis(self, coord_type)
    }

    /// Finds an axis by case-insensitive name prefix.
    pub fn find_axis_by_name(&self, name: &str) -> Option<usize> {
        <Self as ImageInterface<T>>::find_axis_by_name(self, name)
    }

    /// Extracts a single plane with the target axis fixed to `index`.
    pub fn get_plane(&self, axis: usize, index: usize) -> Result<ArrayD<T>, ImageError> {
        <Self as ImageInterface<T>>::get_plane(self, axis, index)
    }

    /// Extracts a spectral-channel plane if the image has a spectral axis.
    pub fn channel_plane(&self, chan: usize) -> Result<Option<ArrayD<T>>, ImageError> {
        <Self as ImageInterface<T>>::channel_plane(self, chan)
    }

    /// Extracts a Stokes plane if the image has a Stokes axis.
    pub fn stokes_plane(&self, stokes: usize) -> Result<Option<ArrayD<T>>, ImageError> {
        <Self as ImageInterface<T>>::stokes_plane(self, stokes)
    }

    /// Creates a unit-stride subimage view.
    pub fn sub_image(
        &self,
        start: Vec<usize>,
        shape: Vec<usize>,
    ) -> Result<SubImage<'_, T, Self>, ImageError> {
        <Self as ImageInterface<T>>::sub_image(self, start, shape)
    }

    /// Creates a strided subimage view.
    pub fn sub_image_with_stride(
        &self,
        start: Vec<usize>,
        shape: Vec<usize>,
        stride: Vec<usize>,
    ) -> Result<SubImage<'_, T, Self>, ImageError> {
        <Self as ImageInterface<T>>::sub_image_with_stride(self, start, shape, stride)
    }

    /// Creates a mutable unit-stride subimage view.
    pub fn sub_image_mut(
        &mut self,
        start: Vec<usize>,
        shape: Vec<usize>,
    ) -> Result<SubImageMut<'_, T, Self>, ImageError> {
        <Self as MutableImageInterface<T>>::sub_image_mut(self, start, shape)
    }

    /// Creates a mutable strided subimage view.
    pub fn sub_image_mut_with_stride(
        &mut self,
        start: Vec<usize>,
        shape: Vec<usize>,
        stride: Vec<usize>,
    ) -> Result<SubImageMut<'_, T, Self>, ImageError> {
        <Self as MutableImageInterface<T>>::sub_image_mut_with_stride(self, start, shape, stride)
    }

    /// Creates a read-only expression by mapping a function over this image.
    /// Starts a lazy expression rooted at this image.
    pub fn expr(&self) -> Result<ImageExprBuilder<'_, T>, ImageError>
    where
        T: crate::image_expr::ImageExprValue + PartialOrd,
    {
        <Self as ImageInterface<T>>::expr(self)
    }

    /// Creates a lazy read-only expression by mapping a function over this image.
    pub fn expr_map<F>(&self, f: F) -> Result<ImageExprBuilder<'_, T>, ImageError>
    where
        T: crate::image_expr::ImageExprValue + PartialOrd,
        F: Fn(T) -> T + Send + Sync + 'static,
    {
        <Self as ImageInterface<T>>::expr_map(self, f)
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
#[path = "observation_date_tests.rs"]
mod observation_date_tests;
