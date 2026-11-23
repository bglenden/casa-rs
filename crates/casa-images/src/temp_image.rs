// SPDX-License-Identifier: LGPL-3.0-or-later
//! Temporary image that stores pixel data in a [`TempLattice`] and metadata
//! in member fields — no casacore `Table` involved.
//!
//! This is the Rust equivalent of C++ `casacore::TempImage<T>`.  Small images
//! stay entirely in memory; larger ones spill to a scratch file via
//! [`PagedArray`](casa_lattices::PagedArray).
//!
//! # Differences from [`PagedImage`]
//!
//! | Aspect                   | `PagedImage`         | `TempImage`               |
//! |--------------------------|----------------------|---------------------------|
//! | Storage                  | casacore `Table`     | `TempLattice` (mem/scratch)|
//! | `is_persistent()`        | `true`               | `false`                   |
//! | `name()`                 | `Some(path)`         | `None`                    |
//! | Metadata I/O             | table keywords       | member fields             |
//! | Save to disk             | `save()`             | `save_as()` / `into_paged()` |
//!
//! # Examples
//!
//! ```rust
//! use casa_coordinates::CoordinateSystem;
//! use casa_images::TempImage;
//! use casa_lattices::{Lattice, LatticeMut};
//!
//! let mut img = TempImage::<f32>::new(vec![8, 8], CoordinateSystem::new(), casa_lattices::TempStoragePolicy::Memory).unwrap();
//! assert!(!img.is_persistent());
//! img.set(3.14).unwrap();
//! assert_eq!(img.get_at(&[0, 0]).unwrap(), 3.14);
//! ```

use std::any::Any;
use std::path::Path;

use casa_coordinates::{CoordinateSystem, CoordinateType};
use casa_lattices::{
    Lattice, LatticeError, LatticeMut, ScratchSpace, TempLattice, TempStoragePolicy,
    TraversalCacheHint, TraversalCacheScope, TraversalCursorIter, TraversalSpec,
};
use casa_types::{ArrayD, RecordValue, Value};
use ndarray::IxDyn;

use crate::component_list_image::{ComponentListImage, ComponentTable};
use crate::error::ImageError;
use crate::image::{
    ImageHistoryRow, ImageInterface, ImagePixel, MutableImageInterface, PagedImage,
    validate_new_mask_name,
};
use crate::image_expr::ImageExprBuilder;
use crate::image_info::ImageInfo;
use crate::subimage::{SubImage, SubImageMut};
use crate::virtual_concat::ConcatImage;

mod history;
mod mask;
mod read_only;

use mask::TempMask;
use read_only::ReadOnlyImage;

/// An image with tool-local metadata and pixels in a [`TempLattice`] or a
/// read-only source.
///
/// Corresponds to C++ `casacore::TempImage<T>`.  Unlike [`PagedImage`],
/// a `TempImage` never writes to a casacore table unless explicitly
/// materialized via [`save_as`](Self::save_as) or
/// [`into_paged`](Self::into_paged).
///
/// The caller chooses memory, scratch paging, or a byte-budgeted automatic
/// policy through [`TempStoragePolicy`].
pub struct TempImage<T: ImagePixel> {
    lattice: TempLattice<T>,
    read_only: Option<ReadOnlyImage<T>>,
    coords: CoordinateSystem,
    units: String,
    misc_info: RecordValue,
    image_info: ImageInfo,
    image_info_record: Option<RecordValue>,
    masks: Vec<(String, TempMask)>,
    mask_scratch: Option<ScratchSpace>,
    default_mask: Option<String>,
    anonymous_mask: Option<TempMask>,
    history: Vec<ImageHistoryRow>,
}

// ---- Constructors -----------------------------------------------------------

impl<T: ImagePixel> TempImage<T> {
    /// Creates a temporary image with an explicit byte-aware storage policy.
    pub fn new(
        shape: Vec<usize>,
        coords: CoordinateSystem,
        policy: TempStoragePolicy,
    ) -> Result<Self, ImageError> {
        let lattice = TempLattice::new(shape, policy.clone())?;
        let mask_scratch = match policy {
            TempStoragePolicy::Paged { scratch } | TempStoragePolicy::Auto { scratch, .. }
                if lattice.is_paged() =>
            {
                Some(scratch)
            }
            _ => None,
        };
        Ok(Self {
            lattice,
            read_only: None,
            coords,
            units: String::new(),
            misc_info: RecordValue::default(),
            image_info: ImageInfo::default(),
            image_info_record: None,
            masks: Vec::new(),
            mask_scratch,
            default_mask: None,
            anonymous_mask: None,
            history: Vec::new(),
        })
    }

    fn pixel_source(&self) -> &dyn Lattice<T> {
        match &self.read_only {
            Some(image) => image.source.as_ref(),
            None => &self.lattice,
        }
    }

    /// Check whether a pixel write is allowed before preparing its values.
    ///
    /// # Errors
    /// Returns the source-specific write error for a read-only image.
    pub fn require_writable_pixels(&self) -> Result<(), LatticeError> {
        match &self.read_only {
            Some(image) if !self.is_writable() => {
                Err(LatticeError::Table(image.write_error.into()))
            }
            Some(_) => Ok(()),
            None => Ok(()),
        }
    }

    /// Returns `true` if the pixel data is held entirely in memory.
    pub fn is_in_memory(&self) -> bool {
        self.read_only.is_none() && self.lattice.is_in_memory()
    }
}

// ---- Pixel access (convenience wrappers returning ImageError) ---------------

impl<T: ImagePixel> TempImage<T> {
    /// Returns the shape of the image.
    pub fn shape(&self) -> &[usize] {
        self.pixel_source().shape()
    }

    /// Returns the number of axes.
    pub fn ndim(&self) -> usize {
        self.pixel_source().ndim()
    }

    /// Returns the total number of pixels.
    pub fn nelements(&self) -> usize {
        self.pixel_source().nelements()
    }

    /// Reports whether the attachment has a persistent image source.
    pub fn is_persistent(&self) -> bool {
        self.name().is_some()
    }

    /// Reports whether the attachment reads pixels from paged storage.
    pub fn is_paged(&self) -> bool {
        self.lattice.is_paged()
            || self
                .read_only
                .as_ref()
                .is_some_and(|image| image.name.is_some() && image.source.is_paged())
    }

    /// Reports whether this image's pixels are writable.
    pub fn is_writable(&self) -> bool {
        self.read_only.is_none() || self.concat_source().is_some_and(Lattice::is_writable)
    }

    /// Return the virtual concatenation supplying this attachment's pixels.
    pub fn concat_source(&self) -> Option<&ConcatImage<T>> {
        self.read_only.as_ref()?.source.as_any()?.downcast_ref()
    }

    /// Returns the casacore-style class name.
    pub fn image_type_name(&self) -> &'static str {
        self.read_only
            .as_ref()
            .map_or("TempImage", |image| image.image_type)
    }

    /// Name of the read-only image source, when attached to a persistent image.
    pub fn name(&self) -> Option<&Path> {
        self.read_only
            .as_ref()
            .and_then(|image| image.name.as_deref())
    }

    pub(crate) fn set_pixel_cache_budget(&mut self, bytes: usize) -> Result<(), ImageError> {
        self.lattice
            .set_maximum_cache_size_pixels((bytes / std::mem::size_of::<T>()).max(1))?;
        Ok(())
    }

    /// Reads the full pixel array.
    pub fn get(&self) -> Result<ArrayD<T>, ImageError> {
        Ok(self.pixel_source().get()?)
    }

    /// Reads a rectangular slice (unit stride).
    pub fn get_slice(&self, start: &[usize], shape: &[usize]) -> Result<ArrayD<T>, ImageError> {
        let stride = vec![1; self.ndim()];
        Ok(self.pixel_source().get_slice(start, shape, &stride)?)
    }

    /// Reads a rectangular slice using an explicit stride.
    pub fn get_slice_with_stride(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<T>, ImageError> {
        Ok(self.pixel_source().get_slice(start, shape, stride)?)
    }

    /// Reads a single pixel.
    pub fn get_at(&self, pos: &[usize]) -> Result<T, ImageError> {
        Ok(self.pixel_source().get_at(pos)?)
    }

    /// Writes a rectangular slice at the given origin.
    pub fn put_slice(&mut self, data: &ArrayD<T>, start: &[usize]) -> Result<(), ImageError> {
        Ok(<Self as LatticeMut<T>>::put_slice(self, data, start)?)
    }

    /// Writes a single pixel.
    pub fn put_at(&mut self, value: T, pos: &[usize]) -> Result<(), ImageError> {
        Ok(<Self as LatticeMut<T>>::put_at(self, value, pos)?)
    }

    /// Sets all pixels to the same value.
    pub fn set(&mut self, value: T) -> Result<(), ImageError> {
        Ok(<Self as LatticeMut<T>>::set(self, value)?)
    }
}

// ---- Metadata ---------------------------------------------------------------

impl<T: ImagePixel> TempImage<T> {
    /// Returns the coordinate system.
    pub fn coordinates(&self) -> &CoordinateSystem {
        &self.coords
    }

    /// Replaces the coordinate system.
    pub fn set_coordinates(&mut self, coords: CoordinateSystem) -> Result<(), ImageError> {
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
        if self.image_type_name() == "ComponentListImage" && units != "Jy/pixel" {
            return Err(ImageError::CasaSelection("units must be Jy/pixel".into()));
        }
        self.units = units;
        Ok(())
    }

    /// Returns the image info.
    pub fn image_info(&self) -> Result<ImageInfo, ImageError> {
        Ok(self.image_info.clone())
    }

    /// Replaces the image info.
    pub fn set_image_info(&mut self, info: &ImageInfo) -> Result<(), ImageError> {
        self.check_beams(info)?;
        if let Some(source) = self.concat_source() {
            source.write_image_info(info, &info.to_record())?;
        }
        if let Some(path) = self.component_table_path() {
            crate::component_list_image::write_component_keyword(
                path,
                "imageinfo",
                Value::Record(info.to_record()),
            )?;
        }
        self.image_info = info.clone();
        self.image_info_record = None;
        Ok(())
    }

    /// Returns the image info record if it was supplied in casacore form.
    pub fn image_info_record(&self) -> Option<&RecordValue> {
        self.image_info_record.as_ref()
    }

    /// Replaces the image info, retaining the supplied casacore record.
    ///
    /// # Errors
    /// Returns an error before changing the image if the record is invalid.
    pub fn set_image_info_record(&mut self, record: RecordValue) -> Result<(), ImageError> {
        let info = ImageInfo::from_record(&record)?;
        self.check_beams(&info)?;
        if let Some(source) = self.concat_source() {
            source.write_image_info(&info, &record)?;
        }
        if let Some(path) = self.component_table_path() {
            crate::component_list_image::write_component_keyword(
                path,
                "imageinfo",
                Value::Record(record.clone()),
            )?;
        }
        self.image_info = info;
        self.image_info_record = Some(record);
        Ok(())
    }

    fn check_beams(&self, info: &ImageInfo) -> Result<(), ImageError> {
        if self.image_type_name() == "ComponentListImage" && !info.beam_set.is_empty() {
            return Err(ImageError::CasaSelection(
                "A ComponentListImage may not have a beam(s)".into(),
            ));
        }
        Ok(())
    }

    fn native_component_source(&self) -> Option<&dyn ImageInterface<T>> {
        let source = self.read_only.as_ref()?;
        if source.image_type != "ComponentListImage" || source.name.is_none() {
            return None;
        }
        Some(source.source.as_ref())
    }

    fn component_table_path(&self) -> Option<&Path> {
        self.native_component_source()?.name()
    }

    fn native_component_table(&self) -> Option<&ComponentTable> {
        self.native_component_source()?
            .as_any()?
            .downcast_ref::<ComponentListImage>()?
            .native_table()
    }

    /// Returns the misc-info record.
    pub fn misc_info(&self) -> RecordValue {
        self.misc_info.clone()
    }

    /// Replaces the misc-info record.
    pub fn set_misc_info(&mut self, rec: RecordValue) -> Result<(), ImageError> {
        if let Some(source) = self.concat_source() {
            source.write_misc_info(&rec)?;
        }
        if let Some(path) = self.component_table_path() {
            crate::component_list_image::write_component_keyword(
                path,
                "miscinfo",
                Value::Record(rec.clone()),
            )?;
        }
        self.misc_info = rec;
        Ok(())
    }
}

// ---- Coordinate / axis helpers ----------------------------------------------

impl<T: ImagePixel> TempImage<T> {
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
}

// ---- temp_close / reopen ----------------------------------------------------

impl<T: ImagePixel> TempImage<T> {
    /// Releases the backing lattice memory (paged variant only).
    ///
    /// Delegates to [`TempLattice::temp_close`].  For in-memory images this
    /// is a no-op.
    pub fn temp_close(&mut self) -> Result<(), ImageError> {
        Ok(self.lattice.temp_close()?)
    }

    /// Reopens a temp-closed image.
    pub fn reopen(&mut self) -> Result<(), ImageError> {
        Ok(self.lattice.reopen()?)
    }
}

// ---- Materialization --------------------------------------------------------

impl<T: ImagePixel> TempImage<T> {
    /// Saves the image to disk as a [`PagedImage`], leaving `self` intact.
    pub fn save_as(&self, path: impl AsRef<Path>) -> Result<PagedImage<T>, ImageError> {
        let path = path.as_ref();
        let mut paged = PagedImage::create(self.shape().to_vec(), self.coords.clone(), path)?;
        if let Some(expression) = &self.read_only {
            let shape = self.shape().to_vec();
            let tile = paged.tile_shape().to_vec();
            let stride = vec![1; self.ndim()];
            for cursor in TraversalCursorIter::new(shape, tile.clone(), TraversalSpec::chunks(tile))
            {
                let cursor = cursor?;
                let data = self.get_slice(&cursor.position, &cursor.shape)?;
                paged.put_slice(&data, &cursor.position)?;
                if expression.has_mask {
                    let mask = self
                        .get_mask_slice(&cursor.position, &cursor.shape, &stride)?
                        .unwrap_or_else(|| ArrayD::from_elem(IxDyn(&cursor.shape), true));
                    paged.put_mask_slice("mask0", &mask, &cursor.position)?;
                }
            }
            if expression.has_mask {
                paged.set_default_mask("mask0")?;
            }
        } else if self.is_paged() {
            paged.copy_data(&self.lattice)?;
        } else {
            let data = self.get()?;
            paged.put_slice(&data, &vec![0; self.ndim()])?;
        }
        paged.set_units(self.units.clone())?;
        if let Some(record) = &self.image_info_record {
            paged.set_image_info_record(record.clone())?;
        } else {
            paged.set_image_info(&self.image_info)?;
        }
        paged.set_misc_info(self.misc_info.clone())?;
        for (name, mask) in &self.masks {
            mask.save(&mut paged, name)?;
        }
        if let Some(mask) = &self.anonymous_mask {
            let name = (0..)
                .map(|n| format!("mask{n}"))
                .find(|name| self.masks.iter().all(|(stored, _)| stored != name))
                .expect("a free name exists");
            mask.save(&mut paged, &name)?;
            paged.set_default_mask(&name)?;
        } else if let Some(ref default) = self.default_mask {
            paged.set_default_mask(default)?;
        }
        paged.append_history_entries(&self.history)?;
        paged.save()?;
        Ok(paged)
    }

    /// Consumes self and saves to disk as a [`PagedImage`].
    pub fn into_paged(self, path: impl AsRef<Path>) -> Result<PagedImage<T>, ImageError> {
        self.save_as(path)
    }
}

// ---- SubImage / Expression convenience --------------------------------------

impl<T: ImagePixel> TempImage<T> {
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

    /// Starts a lazy expression rooted at this image.
    pub fn expr(&self) -> Result<ImageExprBuilder<'_, T>, ImageError>
    where
        T: crate::image_expr::ImageExprValue + PartialOrd,
    {
        <Self as ImageInterface<T>>::expr(self)
    }

    /// Creates a lazy expression by mapping a function over this image.
    pub fn expr_map<F>(&self, f: F) -> Result<ImageExprBuilder<'_, T>, ImageError>
    where
        T: crate::image_expr::ImageExprValue + PartialOrd,
        F: Fn(T) -> T + Send + Sync + 'static,
    {
        <Self as ImageInterface<T>>::expr_map(self, f)
    }
}

// ---- Trait implementations --------------------------------------------------

impl<T: ImagePixel> Lattice<T> for TempImage<T> {
    fn shape(&self) -> &[usize] {
        self.pixel_source().shape()
    }

    fn is_persistent(&self) -> bool {
        TempImage::is_persistent(self)
    }

    fn is_paged(&self) -> bool {
        TempImage::is_paged(self)
    }

    fn is_writable(&self) -> bool {
        TempImage::is_writable(self)
    }

    fn get_at(&self, position: &[usize]) -> Result<T, LatticeError> {
        self.pixel_source().get_at(position)
    }

    fn get_slice(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<T>, LatticeError> {
        self.pixel_source().get_slice(start, shape, stride)
    }

    fn get(&self) -> Result<ArrayD<T>, LatticeError> {
        self.pixel_source().get()
    }

    fn nice_cursor_shape(&self) -> Vec<usize> {
        self.pixel_source().nice_cursor_shape()
    }

    fn enter_traversal_cache_scope<'a>(
        &'a self,
        hint: &TraversalCacheHint,
    ) -> Result<Option<Box<dyn TraversalCacheScope + 'a>>, LatticeError> {
        self.pixel_source().enter_traversal_cache_scope(hint)
    }
}

impl<T: ImagePixel> LatticeMut<T> for TempImage<T> {
    fn with_traversal_cache_hint_mut<R>(
        &mut self,
        hint: &TraversalCacheHint,
        f: impl FnOnce(&mut Self) -> Result<R, LatticeError>,
    ) -> Result<R, LatticeError>
    where
        Self: Sized,
    {
        let previous_cache_pixels = self.lattice.maximum_cache_size_pixels();
        if !self.lattice.is_paged() {
            return f(self);
        }
        let recommended_pixels = {
            let cursor_shape = self.lattice.nice_cursor_shape();
            let recommended_tiles = casa_lattices::recommended_tile_cache_size(
                self.lattice.shape(),
                &cursor_shape,
                hint,
                None,
            )
            .max(1);
            recommended_tiles.saturating_mul(cursor_shape.iter().product::<usize>())
        };
        if previous_cache_pixels == recommended_pixels {
            return f(self);
        }
        self.lattice
            .set_maximum_cache_size_pixels(recommended_pixels)?;
        let result = f(self);
        let restore = self
            .lattice
            .set_maximum_cache_size_pixels(previous_cache_pixels);
        match (result, restore) {
            (Err(err), _) => Err(err),
            (Ok(_), Err(err)) => Err(err),
            (Ok(value), Ok(())) => Ok(value),
        }
    }

    fn put_at(&mut self, value: T, position: &[usize]) -> Result<(), LatticeError> {
        if let Some(source) = self.concat_source() {
            let data = ArrayD::from_elem(IxDyn(&vec![1; self.ndim()]), value);
            return source.write_slice(&data, position);
        }
        self.require_writable_pixels()?;
        self.lattice.put_at(value, position)
    }

    fn put_slice(&mut self, data: &ArrayD<T>, start: &[usize]) -> Result<(), LatticeError> {
        if let Some(source) = self.concat_source() {
            return source.write_slice(data, start);
        }
        self.require_writable_pixels()?;
        self.lattice.put_slice(data, start)
    }

    fn set(&mut self, value: T) -> Result<(), LatticeError> {
        if let Some(source) = self.concat_source() {
            return source.fill(value);
        }
        self.require_writable_pixels()?;
        self.lattice.set(value)
    }
}

impl<T: ImagePixel> ImageInterface<T> for TempImage<T> {
    fn as_any(&self) -> Option<&dyn Any> {
        Some(self)
    }

    fn coordinates(&self) -> &CoordinateSystem {
        TempImage::coordinates(self)
    }

    fn units(&self) -> &str {
        TempImage::units(self)
    }

    fn misc_info(&self) -> RecordValue {
        TempImage::misc_info(self)
    }

    fn image_info(&self) -> Result<ImageInfo, ImageError> {
        TempImage::image_info(self)
    }

    fn name(&self) -> Option<&Path> {
        TempImage::name(self)
    }

    fn default_mask_name(&self) -> Option<String> {
        TempImage::default_mask_name(self)
    }

    fn mask_names(&self) -> Vec<String> {
        TempImage::mask_names(self)
    }

    fn get_named_mask(&self, name: &str) -> Result<ArrayD<bool>, ImageError> {
        TempImage::get_named_mask(self, name)
    }

    fn get_named_mask_slice(
        &self,
        name: &str,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<bool>, ImageError> {
        if let Some(source) = self.native_component_source() {
            return source.get_named_mask_slice(name, start, shape, stride);
        }
        self.find_named_mask(name)?.read_slice(start, shape, stride)
    }

    fn history(&self) -> Result<Vec<String>, ImageError> {
        TempImage::history(self)
    }

    fn default_mask(&self) -> Result<Option<ArrayD<bool>>, ImageError> {
        if let Some(expression) = &self.read_only {
            return expression.source.default_mask();
        }
        match self.default_mask.as_deref() {
            Some(name) => Ok(Some(self.get_named_mask(name)?)),
            None => self.anonymous_mask.as_ref().map(TempMask::read).transpose(),
        }
    }

    fn has_pixel_mask(&self) -> bool {
        if let Some(source) = self.native_component_source() {
            return source.has_pixel_mask();
        }
        self.read_only.as_ref().map_or(
            self.default_mask.is_some() || self.anonymous_mask.is_some(),
            |expression| expression.has_mask,
        )
    }

    fn get_mask_slice(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<Option<ArrayD<bool>>, ImageError> {
        if let Some(expression) = &self.read_only {
            return expression.source.get_mask_slice(start, shape, stride);
        }
        match self.default_mask.as_deref() {
            Some(name) => Ok(Some(self.get_named_mask_slice(name, start, shape, stride)?)),
            None => self
                .anonymous_mask
                .as_ref()
                .map(|mask| mask.read_slice(start, shape, stride))
                .transpose(),
        }
    }
}

impl<T: ImagePixel> MutableImageInterface<T> for TempImage<T> {
    fn set_coordinates(&mut self, coords: CoordinateSystem) -> Result<(), ImageError> {
        TempImage::set_coordinates(self, coords)
    }

    fn set_units_string(&mut self, units: String) -> Result<(), ImageError> {
        TempImage::set_units(self, units)
    }

    fn set_image_info(&mut self, info: &ImageInfo) -> Result<(), ImageError> {
        TempImage::set_image_info(self, info)
    }

    fn set_image_info_record(&mut self, record: RecordValue) -> Result<(), ImageError> {
        TempImage::set_image_info_record(self, record)
    }

    fn set_misc_info(&mut self, rec: RecordValue) -> Result<(), ImageError> {
        TempImage::set_misc_info(self, rec)
    }

    fn put_mask(&mut self, name: &str, data: &ArrayD<bool>) -> Result<(), ImageError> {
        TempImage::put_mask(self, name, data)
    }

    fn create_filled_mask(&mut self, name: &str, value: bool) -> Result<(), ImageError> {
        if let Some(table) = self.native_component_table() {
            return table.create_filled_mask(name, self.shape(), value);
        }
        self.require_writable_pixels()?;
        validate_new_mask_name(name, &self.mask_names())?;
        let mask = TempMask::filled(self.shape(), value, self.mask_scratch.as_ref())?;
        self.masks.push((name.to_owned(), mask));
        Ok(())
    }

    fn create_filled_pixel_mask(&mut self, value: bool) -> Result<String, ImageError> {
        self.require_writable_pixels()?;
        let mask = TempMask::filled(self.shape(), value, self.mask_scratch.as_ref())?;
        self.default_mask = None;
        self.anonymous_mask = Some(mask);
        Ok(String::new())
    }

    fn put_mask_slice(
        &mut self,
        name: &str,
        data: &ArrayD<bool>,
        start: &[usize],
    ) -> Result<(), ImageError> {
        TempImage::put_mask_slice(self, name, data, start)
    }

    fn active_pixel_mask_name(&self) -> Option<String> {
        if let Some(source) = self.concat_source() {
            return source.has_pixel_mask().then(String::new);
        }
        if let Some(source) = self.native_component_source() {
            return source
                .default_mask_name()
                .filter(|_| source.has_pixel_mask());
        }
        self.default_mask
            .clone()
            .or_else(|| self.anonymous_mask.as_ref().map(|_| String::new()))
    }

    fn named_mask_is_writable(&self, name: &str) -> Result<bool, ImageError> {
        if let Some(source) = self.concat_source() {
            return Ok(name.is_empty() && source.mask_writable());
        }
        if let Some(source) = self.native_component_source() {
            return Ok(source.mask_names().iter().any(|stored| stored == name));
        }
        Ok((name.is_empty() && self.anonymous_mask.is_some())
            || self.masks.iter().any(|(stored, _)| stored == name))
    }

    fn remove_mask(&mut self, name: &str) -> Result<(), ImageError> {
        TempImage::remove_mask(self, name)
    }

    fn set_default_mask(&mut self, name: &str) -> Result<(), ImageError> {
        TempImage::set_default_mask(self, name)
    }

    fn unset_default_mask(&mut self) -> Result<(), ImageError> {
        TempImage::unset_default_mask(self)
    }

    fn add_history_entry(&mut self, msg: String) -> Result<(), ImageError> {
        TempImage::add_history(self, msg)
    }

    fn clear_history(&mut self) -> Result<(), ImageError> {
        TempImage::clear_history(self)
    }
}

impl<T: ImagePixel> std::fmt::Debug for TempImage<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TempImage")
            .field("shape", &self.shape())
            .field("is_in_memory", &self.is_in_memory())
            .field("pixel_type", &T::PRIMITIVE_TYPE)
            .finish()
    }
}

// ---- Tests ------------------------------------------------------------------

#[cfg(test)]
mod paged_mask_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use casa_coordinates::CoordinateSystem;
    use casa_lattices::{Lattice, LatticeIterExt, LatticeMut, TraversalSpec};
    use casa_types::{Complex32, Complex64, RecordValue, ScalarValue, Value};
    use ndarray::IxDyn;

    use crate::beam::{GaussianBeam, ImageBeamSet};
    use crate::image_info::ImageType;

    fn cs() -> CoordinateSystem {
        CoordinateSystem::new()
    }

    #[test]
    fn construction_memory() {
        let img = TempImage::<f32>::new(vec![4, 4], cs(), casa_lattices::TempStoragePolicy::Memory)
            .unwrap();
        assert!(img.is_in_memory());
        assert!(!img.is_persistent());
        assert!(img.is_writable());
        assert!(!img.is_paged());
        assert_eq!(img.shape(), &[4, 4]);
        assert_eq!(img.ndim(), 2);
        assert_eq!(img.nelements(), 16);
        assert_eq!(img.image_type_name(), "TempImage");
        assert!(img.name().is_none());
    }

    #[test]
    fn construction_paged() {
        let img = TempImage::<f32>::new(
            vec![4, 4],
            cs(),
            casa_lattices::TempStoragePolicy::Paged {
                scratch: casa_lattices::ScratchSpace::SystemTemp,
            },
        )
        .unwrap();
        assert!(!img.is_in_memory());
        assert!(img.is_paged());
    }

    #[test]
    fn pixel_access() {
        let mut img =
            TempImage::<f32>::new(vec![3, 3], cs(), casa_lattices::TempStoragePolicy::Memory)
                .unwrap();
        img.set(5.0).unwrap();
        assert_eq!(img.get_at(&[1, 1]).unwrap(), 5.0);

        img.put_at(10.0, &[2, 2]).unwrap();
        assert_eq!(img.get_at(&[2, 2]).unwrap(), 10.0);

        let data = img.get().unwrap();
        assert_eq!(data[[2, 2]], 10.0);
        assert_eq!(data[[0, 0]], 5.0);
    }

    #[test]
    fn pixel_slice() {
        let mut img =
            TempImage::<f64>::new(vec![4, 4], cs(), casa_lattices::TempStoragePolicy::Memory)
                .unwrap();
        let ramp = ArrayD::from_shape_fn(IxDyn(&[4, 4]), |idx| (idx[0] * 4 + idx[1]) as f64);
        img.put_slice(&ramp, &[0, 0]).unwrap();

        let slice = img.get_slice(&[1, 1], &[2, 2]).unwrap();
        assert_eq!(slice.shape(), &[2, 2]);
        assert_eq!(slice[[0, 0]], 5.0);
    }

    #[test]
    fn metadata_round_trip() {
        let mut img =
            TempImage::<f32>::new(vec![2, 2], cs(), casa_lattices::TempStoragePolicy::Memory)
                .unwrap();
        img.set_units("Jy/beam").unwrap();
        assert_eq!(img.units(), "Jy/beam");

        let beam = GaussianBeam::new(1e-4, 5e-5, 0.0);
        let info = ImageInfo {
            beam_set: ImageBeamSet::new(beam),
            image_type: ImageType::Intensity,
            object_name: "CasA".into(),
        };
        img.set_image_info(&info).unwrap();
        let read_info = img.image_info().unwrap();
        assert_eq!(read_info.object_name, "CasA");
        assert_eq!(read_info.image_type, ImageType::Intensity);

        let mut misc = RecordValue::default();
        misc.upsert(
            "telescope",
            Value::Scalar(ScalarValue::String("VLA".into())),
        );
        img.set_misc_info(misc).unwrap();
        let read_misc = img.misc_info();
        assert_eq!(
            read_misc.get("telescope"),
            Some(&Value::Scalar(ScalarValue::String("VLA".into())))
        );

        img.set_coordinates(CoordinateSystem::new()).unwrap();
        assert_eq!(img.coordinates().n_coordinates(), 0);
    }

    #[test]
    fn mask_operations() {
        let mut img =
            TempImage::<f32>::new(vec![3, 3], cs(), casa_lattices::TempStoragePolicy::Memory)
                .unwrap();

        // No masks initially.
        assert!(!img.has_pixel_mask());
        assert!(img.mask_names().is_empty());
        assert!(img.get_mask().unwrap().is_none());

        // Create mask.
        img.make_mask("quality", true, true).unwrap();
        assert!(img.has_pixel_mask());
        assert_eq!(img.default_mask_name().as_deref(), Some("quality"));
        assert_eq!(img.mask_names(), vec!["quality".to_string()]);

        // Read mask.
        let mask = img.get_mask().unwrap().unwrap();
        assert!(mask.iter().all(|&v| v));

        // Write mask.
        let mut modified = mask.clone();
        modified[[0, 0]] = false;
        img.put_mask("quality", &modified).unwrap();
        let read = img.get_named_mask("quality").unwrap();
        assert!(!read[[0, 0]]);
        let slice = img
            .get_named_mask_slice("quality", &[0, 0], &[2, 1], &[2, 1])
            .unwrap();
        assert_eq!(slice.shape(), &[2, 1]);
        assert!(!slice[[0, 0]]);
        assert!(slice[[1, 0]]);

        // Remove mask.
        img.remove_mask("quality").unwrap();
        assert!(!img.has_pixel_mask());
        assert!(img.mask_names().is_empty());

        // Error on removing non-existent mask.
        assert!(img.remove_mask("missing").is_err());
    }

    #[test]
    fn mask_default_validation() {
        let mut img =
            TempImage::<f32>::new(vec![2, 2], cs(), casa_lattices::TempStoragePolicy::Memory)
                .unwrap();
        assert!(img.set_default_mask("missing").is_err());

        img.make_mask("m1", false, true).unwrap();
        img.set_default_mask("m1").unwrap();
        assert_eq!(img.default_mask_name().as_deref(), Some("m1"));

        img.unset_default_mask().unwrap();
        assert!(img.default_mask_name().is_none());
    }

    #[test]
    fn named_mask_blocks_preserve_strided_boundary_values() {
        let mut image = TempImage::<f32>::new(vec![5, 3], cs(), TempStoragePolicy::Memory).unwrap();
        let block = ArrayD::from_elem(IxDyn(&[3, 1]), true);
        assert!(image.put_mask_slice("mask", &block, &[3, 0]).is_err());
        assert!(image.mask_names().is_empty());
        image.put_mask_slice("mask", &block, &[2, 1]).unwrap();
        let row = image
            .get_named_mask_slice("mask", &[0, 1], &[3, 1], &[2, 1])
            .unwrap();
        assert_eq!(row.as_slice().unwrap(), &[false, true, true]);
        assert!(
            image
                .get_named_mask_slice("mask", &[0, 1], &[3, 1], &[0, 1])
                .is_err()
        );
    }

    #[test]
    fn history_operations() {
        let mut img =
            TempImage::<f32>::new(vec![2, 2], cs(), casa_lattices::TempStoragePolicy::Memory)
                .unwrap();
        assert!(img.history().unwrap().is_empty());

        img.add_history("created").unwrap();
        img.add_history("edited").unwrap();
        assert_eq!(
            img.history().unwrap(),
            vec!["created".to_string(), "edited".to_string()]
        );

        img.clear_history().unwrap();
        assert!(img.history().unwrap().is_empty());
    }

    #[test]
    fn save_as_materializes_correctly() {
        let mut img =
            TempImage::<f32>::new(vec![4, 4], cs(), casa_lattices::TempStoragePolicy::Memory)
                .unwrap();
        img.set(2.5).unwrap();
        img.set_units("K").unwrap();
        img.make_mask("mymask", true, true).unwrap();
        img.add_history("test entry").unwrap();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("materialized.image");
        let paged = img.save_as(&path).unwrap();

        assert_eq!(paged.shape(), &[4, 4]);
        assert_eq!(paged.get_at(&[0, 0]).unwrap(), 2.5);
        assert_eq!(paged.units(), "K");
        assert_eq!(paged.default_mask_name().as_deref(), Some("mymask"));
        assert_eq!(paged.history().unwrap(), vec!["test entry".to_string()]);

        // Verify original TempImage is still valid.
        assert_eq!(img.get_at(&[0, 0]).unwrap(), 2.5);
    }

    #[test]
    fn save_as_materializes_a_paged_temporary_image_and_mask() {
        let dir = tempfile::tempdir().unwrap();
        let mut img = TempImage::<f32>::new(
            vec![5, 7],
            cs(),
            TempStoragePolicy::Paged {
                scratch: ScratchSpace::SystemTemp,
            },
        )
        .unwrap();
        let values = ArrayD::from_shape_fn(IxDyn(&[5, 7]), |p| (p[0] + 10 * p[1]) as f32);
        let valid = values.mapv(|value| value >= 13.);
        img.put_slice(&values, &[0, 0]).unwrap();
        img.make_mask("mymask", true, true).unwrap();
        img.put_mask_slice("mymask", &valid, &[0, 0]).unwrap();
        let paged = img.save_as(dir.path().join("materialized.im")).unwrap();
        assert_eq!(paged.get().unwrap(), values);
        assert_eq!(paged.get_mask().unwrap(), Some(valid));
    }

    #[test]
    fn expression_attachment_materializes_values_and_validity() {
        let mut source =
            TempImage::<f32>::new(vec![3, 2], cs(), TempStoragePolicy::Memory).unwrap();
        source.set(4.0).unwrap();
        let mut mask = ArrayD::from_elem(IxDyn(&[3, 2]), true);
        mask[[0, 1]] = false;
        source.put_mask("source_mask", &mask).unwrap();
        source.set_default_mask("source_mask").unwrap();
        let expression = ImageExprBuilder::from_image(&source)
            .unwrap()
            .negate()
            .compile()
            .unwrap();
        let mut view = TempImage::from_expression(expression).unwrap();
        assert_eq!(view.image_type_name(), "ImageExpr");
        assert!(view.set(1.0).is_err());
        assert_eq!(view.get_slice(&[1, 0], &[1, 2]).unwrap()[[0, 0]], -4.0);
        view.set_units("K").unwrap();
        let dir = tempfile::tempdir().unwrap();
        let image = view.save_as(dir.path().join("result.im")).unwrap();
        assert!(image.get().unwrap().iter().all(|value| *value == -4.0));
        assert_eq!(image.default_mask_name().as_deref(), Some("mask0"));
        assert_eq!(image.get_mask().unwrap().unwrap(), mask);
        assert_eq!(image.units(), "K");
    }

    #[test]
    fn into_paged_consumes() {
        let mut img =
            TempImage::<f64>::new(vec![2, 2], cs(), casa_lattices::TempStoragePolicy::Memory)
                .unwrap();
        img.set(1.0).unwrap();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("consumed.image");
        let paged = img.into_paged(&path).unwrap();
        assert_eq!(paged.get_at(&[0, 0]).unwrap(), 1.0);
    }

    #[test]
    fn subimage_integration() {
        let mut img =
            TempImage::<f32>::new(vec![4, 4], cs(), casa_lattices::TempStoragePolicy::Memory)
                .unwrap();
        img.set(0.0).unwrap();
        {
            let mut sub = img.sub_image_mut(vec![1, 1], vec![2, 2]).unwrap();
            sub.set(5.0).unwrap();
        }
        assert_eq!(img.get_at(&[0, 0]).unwrap(), 0.0);
        assert_eq!(img.get_at(&[1, 1]).unwrap(), 5.0);

        let sub = img.sub_image(vec![1, 1], vec![2, 2]).unwrap();
        assert_eq!(sub.get_at(&[0, 0]).unwrap(), 5.0);
    }

    #[test]
    fn image_expr_integration() {
        let mut img =
            TempImage::<f32>::new(vec![3, 3], cs(), casa_lattices::TempStoragePolicy::Memory)
                .unwrap();
        img.set(2.0).unwrap();

        let expr = img.expr().unwrap().multiply_scalar(3.0);
        assert_eq!(expr.compile().unwrap().get_at(&[0, 0]).unwrap(), 6.0);
    }

    #[test]
    fn image_iter_integration() {
        let mut img =
            TempImage::<f32>::new(vec![4, 4], cs(), casa_lattices::TempStoragePolicy::Memory)
                .unwrap();
        img.set(1.0).unwrap();

        let chunks: Vec<_> = img
            .traverse(TraversalSpec::chunks(vec![2, 2]))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(chunks.len(), 4);
        assert!(chunks.iter().all(|c| c.data.iter().all(|&v| v == 1.0)));
    }

    #[test]
    fn image_iter_mut_integration() {
        let mut img =
            TempImage::<f32>::new(vec![4, 4], cs(), casa_lattices::TempStoragePolicy::Memory)
                .unwrap();
        img.set(1.0).unwrap();

        img.for_each_chunk_mut(TraversalSpec::chunks(vec![4, 4]), |data, _| {
            data.mapv_inplace(|value| value * 2.0);
            Ok(())
        })
        .unwrap();
        assert_eq!(img.get_at(&[0, 0]).unwrap(), 2.0);
    }

    #[test]
    fn all_pixel_types() {
        // f32
        let mut img =
            TempImage::<f32>::new(vec![2], cs(), casa_lattices::TempStoragePolicy::Memory).unwrap();
        img.set(1.5).unwrap();
        assert_eq!(img.get_at(&[0]).unwrap(), 1.5f32);

        // f64
        let mut img =
            TempImage::<f64>::new(vec![2], cs(), casa_lattices::TempStoragePolicy::Memory).unwrap();
        img.set(2.5).unwrap();
        assert_eq!(img.get_at(&[0]).unwrap(), 2.5f64);

        // Complex32
        let c32 = Complex32::new(1.0, -2.0);
        let mut img =
            TempImage::<Complex32>::new(vec![2], cs(), casa_lattices::TempStoragePolicy::Memory)
                .unwrap();
        img.set(c32).unwrap();
        assert_eq!(img.get_at(&[0]).unwrap(), c32);

        // Complex64
        let c64 = Complex64::new(3.0, -4.0);
        let mut img =
            TempImage::<Complex64>::new(vec![2], cs(), casa_lattices::TempStoragePolicy::Memory)
                .unwrap();
        img.set(c64).unwrap();
        assert_eq!(img.get_at(&[0]).unwrap(), c64);
    }

    #[test]
    fn lattice_trait_properties() {
        let img = TempImage::<f32>::new(vec![4, 4], cs(), casa_lattices::TempStoragePolicy::Memory)
            .unwrap();
        assert!(!<TempImage<f32> as Lattice<f32>>::is_persistent(&img));
        assert!(<TempImage<f32> as Lattice<f32>>::is_writable(&img));
    }

    #[test]
    fn temp_close_reopen() {
        // Memory-backed: temp_close is a no-op.
        let mut img =
            TempImage::<f32>::new(vec![4, 4], cs(), casa_lattices::TempStoragePolicy::Memory)
                .unwrap();
        img.set(5.0).unwrap();
        img.temp_close().unwrap();
        assert_eq!(img.get_at(&[0, 0]).unwrap(), 5.0);

        // Paged-backed: temp_close releases, reopen restores.
        let mut img = TempImage::<f32>::new(
            vec![4, 4],
            cs(),
            casa_lattices::TempStoragePolicy::Paged {
                scratch: casa_lattices::ScratchSpace::SystemTemp,
            },
        )
        .unwrap();
        img.set(7.0).unwrap();
        img.temp_close().unwrap();

        // Reads auto-reopen transparently.
        assert_eq!(img.get_at(&[0, 0]).unwrap(), 7.0);

        // Explicit close + reopen cycle also works.
        img.temp_close().unwrap();
        img.reopen().unwrap();
        assert_eq!(img.get_at(&[0, 0]).unwrap(), 7.0);
    }

    #[test]
    fn debug_impl() {
        let img = TempImage::<f32>::new(vec![3, 3], cs(), casa_lattices::TempStoragePolicy::Memory)
            .unwrap();
        let debug = format!("{img:?}");
        assert!(debug.contains("TempImage"));
        assert!(debug.contains("shape"));
    }

    #[test]
    fn mask_shape_validation() {
        let mut img =
            TempImage::<f32>::new(vec![3, 3], cs(), casa_lattices::TempStoragePolicy::Memory)
                .unwrap();
        let wrong = ArrayD::from_elem(IxDyn(&[2, 2]), true);
        assert!(img.put_mask("bad", &wrong).is_err());
    }

    #[test]
    fn image_interface_trait() {
        let mut img =
            TempImage::<f32>::new(vec![2, 2], cs(), casa_lattices::TempStoragePolicy::Memory)
                .unwrap();
        img.set(1.0).unwrap();
        img.set_units("Jy").unwrap();

        // Use through ImageInterface trait object.
        let iface: &dyn ImageInterface<f32> = &img;
        assert_eq!(iface.units(), "Jy");
        assert!(iface.name().is_none());
        assert_eq!(iface.shape(), &[2, 2]);
    }
}
