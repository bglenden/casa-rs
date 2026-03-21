// SPDX-License-Identifier: LGPL-3.0-or-later
//! Temporary image that stores pixel data in a [`TempLattice`] and metadata
//! in member fields — no casacore `Table` involved.
//!
//! This is the Rust equivalent of C++ `casacore::TempImage<T>`.  Small images
//! stay entirely in memory; larger ones spill to a scratch file via
//! [`PagedArray`](casacore_lattices::PagedArray).
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
//! use casacore_coordinates::CoordinateSystem;
//! use casacore_images::TempImage;
//! use casacore_lattices::{Lattice, LatticeMut};
//!
//! let mut img = TempImage::<f32>::new(vec![8, 8], CoordinateSystem::new()).unwrap();
//! assert!(!img.is_persistent());
//! img.set(3.14).unwrap();
//! assert_eq!(img.get_at(&[0, 0]).unwrap(), 3.14);
//! ```

use std::any::Any;
use std::collections::BTreeMap;
use std::path::Path;

use casacore_coordinates::{CoordinateSystem, CoordinateType};
use casacore_lattices::{
    Lattice, LatticeError, LatticeMut, TempLattice, TraversalCacheHint, TraversalCacheScope,
};
use casacore_types::{ArrayD, RecordValue};

use crate::error::ImageError;
use crate::image::{ImageInterface, ImagePixel, MutableImageInterface, PagedImage};
use crate::image_expr::ImageExpr;
use crate::image_info::ImageInfo;
use crate::subimage::{SubImage, SubImageMut};

/// A temporary image that stores pixels in a [`TempLattice`] and metadata
/// in member fields.
///
/// Corresponds to C++ `casacore::TempImage<T>`.  Unlike [`PagedImage`],
/// a `TempImage` never writes to a casacore table unless explicitly
/// materialized via [`save_as`](Self::save_as) or
/// [`into_paged`](Self::into_paged).
///
/// Small images (below the default 256 Ki element threshold) live entirely
/// in memory.  Larger ones use a scratch [`PagedArray`](casacore_lattices::PagedArray)
/// that auto-manages its own temporary file.
pub struct TempImage<T: ImagePixel> {
    lattice: TempLattice<T>,
    coords: CoordinateSystem,
    units: String,
    misc_info: RecordValue,
    image_info: ImageInfo,
    masks: BTreeMap<String, ArrayD<bool>>,
    default_mask: Option<String>,
    history: Vec<String>,
}

// ---- Constructors -----------------------------------------------------------

impl<T: ImagePixel> TempImage<T> {
    /// Creates a new temporary image with the default memory threshold.
    ///
    /// Images with fewer than 256 Ki elements stay in memory; larger ones
    /// use scratch-disk storage.
    pub fn new(shape: Vec<usize>, coords: CoordinateSystem) -> Result<Self, ImageError> {
        Self::with_threshold(shape, coords, None)
    }

    /// Creates a new temporary image with an explicit memory threshold.
    ///
    /// If `max_memory_elements` is `Some(n)`, images with more than `n`
    /// elements will use scratch-disk storage.
    pub fn with_threshold(
        shape: Vec<usize>,
        coords: CoordinateSystem,
        max_memory_elements: Option<usize>,
    ) -> Result<Self, ImageError> {
        let lattice = TempLattice::new(shape, max_memory_elements)?;
        Ok(Self {
            lattice,
            coords,
            units: String::new(),
            misc_info: RecordValue::default(),
            image_info: ImageInfo::default(),
            masks: BTreeMap::new(),
            default_mask: None,
            history: Vec::new(),
        })
    }

    /// Returns `true` if the pixel data is held entirely in memory.
    pub fn is_in_memory(&self) -> bool {
        self.lattice.is_in_memory()
    }
}

// ---- Pixel access (convenience wrappers returning ImageError) ---------------

impl<T: ImagePixel> TempImage<T> {
    /// Returns the shape of the image.
    pub fn shape(&self) -> &[usize] {
        self.lattice.shape()
    }

    /// Returns the number of axes.
    pub fn ndim(&self) -> usize {
        self.lattice.ndim()
    }

    /// Returns the total number of pixels.
    pub fn nelements(&self) -> usize {
        self.lattice.nelements()
    }

    /// Returns `false` — temporary images are never persistent.
    pub fn is_persistent(&self) -> bool {
        false
    }

    /// Returns `true` if the backing lattice uses paged storage.
    pub fn is_paged(&self) -> bool {
        self.lattice.is_paged()
    }

    /// Returns `true` — temporary images are always writable.
    pub fn is_writable(&self) -> bool {
        true
    }

    /// Returns the casacore-style class name.
    pub fn image_type_name(&self) -> &'static str {
        "TempImage"
    }

    /// Returns `None` — temporary images have no on-disk path.
    pub fn name(&self) -> Option<&Path> {
        None
    }

    /// Reads the full pixel array.
    pub fn get(&self) -> Result<ArrayD<T>, ImageError> {
        Ok(self.lattice.get()?)
    }

    /// Reads a rectangular slice (unit stride).
    pub fn get_slice(&self, start: &[usize], shape: &[usize]) -> Result<ArrayD<T>, ImageError> {
        let stride = vec![1; self.ndim()];
        Ok(self.lattice.get_slice(start, shape, &stride)?)
    }

    /// Reads a rectangular slice using an explicit stride.
    pub fn get_slice_with_stride(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<T>, ImageError> {
        Ok(self.lattice.get_slice(start, shape, stride)?)
    }

    /// Reads a single pixel.
    pub fn get_at(&self, pos: &[usize]) -> Result<T, ImageError> {
        Ok(self.lattice.get_at(pos)?)
    }

    /// Writes a rectangular slice at the given origin.
    pub fn put_slice(&mut self, data: &ArrayD<T>, start: &[usize]) -> Result<(), ImageError> {
        Ok(self.lattice.put_slice(data, start)?)
    }

    /// Writes a single pixel.
    pub fn put_at(&mut self, value: T, pos: &[usize]) -> Result<(), ImageError> {
        Ok(self.lattice.put_at(value, pos)?)
    }

    /// Sets all pixels to the same value.
    pub fn set(&mut self, value: T) -> Result<(), ImageError> {
        Ok(self.lattice.set(value)?)
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
        self.units = units.into();
        Ok(())
    }

    /// Returns the image info.
    pub fn image_info(&self) -> Result<ImageInfo, ImageError> {
        Ok(self.image_info.clone())
    }

    /// Replaces the image info.
    pub fn set_image_info(&mut self, info: &ImageInfo) -> Result<(), ImageError> {
        self.image_info = info.clone();
        Ok(())
    }

    /// Returns the misc-info record.
    pub fn misc_info(&self) -> RecordValue {
        self.misc_info.clone()
    }

    /// Replaces the misc-info record.
    pub fn set_misc_info(&mut self, rec: RecordValue) -> Result<(), ImageError> {
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

// ---- Masks ------------------------------------------------------------------

impl<T: ImagePixel> TempImage<T> {
    /// Creates a full-image mask with the given name.
    pub fn make_mask(
        &mut self,
        name: impl Into<String>,
        set_default: bool,
        initial: bool,
    ) -> Result<(), ImageError> {
        <Self as MutableImageInterface<T>>::make_mask(self, name, set_default, initial)
    }

    /// Returns the default mask contents if a default mask is configured.
    pub fn get_mask(&self) -> Result<Option<ArrayD<bool>>, ImageError> {
        <Self as ImageInterface<T>>::get_mask(self)
    }

    /// Returns a named mask as a full-image boolean array.
    pub fn get_named_mask(&self, name: &str) -> Result<ArrayD<bool>, ImageError> {
        self.masks
            .get(name)
            .cloned()
            .ok_or_else(|| ImageError::MaskNotFound(name.to_string()))
    }

    /// Replaces a named mask.
    pub fn put_mask(&mut self, name: &str, data: &ArrayD<bool>) -> Result<(), ImageError> {
        if data.shape() != self.shape() {
            return Err(ImageError::ShapeMismatch {
                expected: self.shape().to_vec(),
                got: data.shape().to_vec(),
            });
        }
        self.masks.insert(name.to_string(), data.clone());
        Ok(())
    }

    /// Removes a named mask.
    pub fn remove_mask(&mut self, name: &str) -> Result<(), ImageError> {
        if self.masks.remove(name).is_none() {
            return Err(ImageError::MaskNotFound(name.to_string()));
        }
        if self.default_mask.as_deref() == Some(name) {
            self.default_mask = None;
        }
        Ok(())
    }

    /// Sets the default mask name, validating that it exists.
    pub fn set_default_mask(&mut self, name: &str) -> Result<(), ImageError> {
        if !self.masks.contains_key(name) {
            return Err(ImageError::MaskNotFound(name.to_string()));
        }
        self.default_mask = Some(name.to_string());
        Ok(())
    }

    /// Unsets the default mask.
    pub fn unset_default_mask(&mut self) -> Result<(), ImageError> {
        self.default_mask = None;
        Ok(())
    }

    /// Returns `true` if a default pixel mask is configured.
    pub fn has_pixel_mask(&self) -> bool {
        <Self as ImageInterface<T>>::has_pixel_mask(self)
    }

    /// Returns the default mask name if present.
    pub fn default_mask_name(&self) -> Option<String> {
        self.default_mask.clone()
    }

    /// Returns the names of all stored masks.
    pub fn mask_names(&self) -> Vec<String> {
        self.masks.keys().cloned().collect()
    }
}

// ---- History ----------------------------------------------------------------

impl<T: ImagePixel> TempImage<T> {
    /// Returns the history entries.
    pub fn history(&self) -> Result<Vec<String>, ImageError> {
        Ok(self.history.clone())
    }

    /// Appends a history message.
    pub fn add_history(&mut self, msg: impl Into<String>) -> Result<(), ImageError> {
        self.history.push(msg.into());
        Ok(())
    }

    /// Clears the history log.
    pub fn clear_history(&mut self) -> Result<(), ImageError> {
        self.history.clear();
        Ok(())
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
        let data = self.get()?;
        paged.put_slice(&data, &vec![0; self.ndim()])?;
        paged.set_units(self.units.clone())?;
        paged.set_image_info(&self.image_info)?;
        paged.set_misc_info(self.misc_info.clone())?;
        for (name, mask_data) in &self.masks {
            paged.put_mask(name, mask_data)?;
        }
        if let Some(ref default) = self.default_mask {
            paged.set_default_mask(default)?;
        }
        for entry in &self.history {
            paged.add_history(entry.clone())?;
        }
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
    pub fn expr(&self) -> Result<ImageExpr<'_, T>, ImageError>
    where
        T: crate::image_expr::ImageExprValue + PartialOrd,
    {
        <Self as ImageInterface<T>>::expr(self)
    }

    /// Creates a lazy expression by mapping a function over this image.
    pub fn expr_map<F>(&self, f: F) -> Result<ImageExpr<'_, T>, ImageError>
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
        self.lattice.shape()
    }

    fn is_persistent(&self) -> bool {
        false
    }

    fn is_paged(&self) -> bool {
        self.lattice.is_paged()
    }

    fn is_writable(&self) -> bool {
        true
    }

    fn get_at(&self, position: &[usize]) -> Result<T, LatticeError> {
        self.lattice.get_at(position)
    }

    fn get_slice(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<T>, LatticeError> {
        self.lattice.get_slice(start, shape, stride)
    }

    fn get(&self) -> Result<ArrayD<T>, LatticeError> {
        self.lattice.get()
    }

    fn nice_cursor_shape(&self) -> Vec<usize> {
        self.lattice.nice_cursor_shape()
    }

    fn enter_traversal_cache_scope<'a>(
        &'a self,
        hint: &TraversalCacheHint,
    ) -> Result<Option<Box<dyn TraversalCacheScope + 'a>>, LatticeError> {
        self.lattice.enter_traversal_cache_scope(hint)
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
            let recommended_tiles = casacore_lattices::recommended_tile_cache_size(
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
        self.lattice.put_at(value, position)
    }

    fn put_slice(&mut self, data: &ArrayD<T>, start: &[usize]) -> Result<(), LatticeError> {
        self.lattice.put_slice(data, start)
    }

    fn set(&mut self, value: T) -> Result<(), LatticeError> {
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

    fn history(&self) -> Result<Vec<String>, ImageError> {
        TempImage::history(self)
    }

    fn default_mask(&self) -> Result<Option<ArrayD<bool>>, ImageError> {
        match TempImage::default_mask_name(self) {
            Some(name) => Ok(Some(TempImage::get_named_mask(self, &name)?)),
            None => Ok(None),
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

    fn set_misc_info(&mut self, rec: RecordValue) -> Result<(), ImageError> {
        TempImage::set_misc_info(self, rec)
    }

    fn put_mask(&mut self, name: &str, data: &ArrayD<bool>) -> Result<(), ImageError> {
        TempImage::put_mask(self, name, data)
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
            .field("shape", &self.lattice.shape())
            .field("is_in_memory", &self.is_in_memory())
            .field("pixel_type", &T::PRIMITIVE_TYPE)
            .finish()
    }
}

// ---- Tests ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use casacore_coordinates::CoordinateSystem;
    use casacore_lattices::{Lattice, LatticeMut};
    use casacore_types::{Complex32, Complex64, RecordValue, ScalarValue, Value};
    use ndarray::IxDyn;

    use crate::beam::{GaussianBeam, ImageBeamSet};
    use crate::image_info::ImageType;
    use crate::iterator::{ImageIter, ImageIterMut};

    fn cs() -> CoordinateSystem {
        CoordinateSystem::new()
    }

    #[test]
    fn construction_memory() {
        let img = TempImage::<f32>::new(vec![4, 4], cs()).unwrap();
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
        let img = TempImage::<f32>::with_threshold(vec![4, 4], cs(), Some(1)).unwrap();
        assert!(!img.is_in_memory());
        assert!(img.is_paged());
    }

    #[test]
    fn pixel_access() {
        let mut img = TempImage::<f32>::new(vec![3, 3], cs()).unwrap();
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
        let mut img = TempImage::<f64>::new(vec![4, 4], cs()).unwrap();
        let ramp = ArrayD::from_shape_fn(IxDyn(&[4, 4]), |idx| (idx[0] * 4 + idx[1]) as f64);
        img.put_slice(&ramp, &[0, 0]).unwrap();

        let slice = img.get_slice(&[1, 1], &[2, 2]).unwrap();
        assert_eq!(slice.shape(), &[2, 2]);
        assert_eq!(slice[[0, 0]], 5.0);
    }

    #[test]
    fn metadata_round_trip() {
        let mut img = TempImage::<f32>::new(vec![2, 2], cs()).unwrap();
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
        let mut img = TempImage::<f32>::new(vec![3, 3], cs()).unwrap();

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

        // Remove mask.
        img.remove_mask("quality").unwrap();
        assert!(!img.has_pixel_mask());
        assert!(img.mask_names().is_empty());

        // Error on removing non-existent mask.
        assert!(img.remove_mask("missing").is_err());
    }

    #[test]
    fn mask_default_validation() {
        let mut img = TempImage::<f32>::new(vec![2, 2], cs()).unwrap();
        assert!(img.set_default_mask("missing").is_err());

        img.make_mask("m1", false, true).unwrap();
        img.set_default_mask("m1").unwrap();
        assert_eq!(img.default_mask_name().as_deref(), Some("m1"));

        img.unset_default_mask().unwrap();
        assert!(img.default_mask_name().is_none());
    }

    #[test]
    fn history_operations() {
        let mut img = TempImage::<f32>::new(vec![2, 2], cs()).unwrap();
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
        let mut img = TempImage::<f32>::new(vec![4, 4], cs()).unwrap();
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
    fn into_paged_consumes() {
        let mut img = TempImage::<f64>::new(vec![2, 2], cs()).unwrap();
        img.set(1.0).unwrap();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("consumed.image");
        let paged = img.into_paged(&path).unwrap();
        assert_eq!(paged.get_at(&[0, 0]).unwrap(), 1.0);
    }

    #[test]
    fn subimage_integration() {
        let mut img = TempImage::<f32>::new(vec![4, 4], cs()).unwrap();
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
        let mut img = TempImage::<f32>::new(vec![3, 3], cs()).unwrap();
        img.set(2.0).unwrap();

        let expr = img.expr().unwrap().multiply_scalar(3.0);
        assert_eq!(expr.get_at(&[0, 0]).unwrap(), 6.0);
    }

    #[test]
    fn image_iter_integration() {
        let mut img = TempImage::<f32>::new(vec![4, 4], cs()).unwrap();
        img.set(1.0).unwrap();

        let chunks: Vec<_> = ImageIter::new(&img, vec![2, 2])
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(chunks.len(), 4);
        assert!(chunks.iter().all(|c| c.data.iter().all(|&v| v == 1.0)));
    }

    #[test]
    fn image_iter_mut_integration() {
        let mut img = TempImage::<f32>::new(vec![4, 4], cs()).unwrap();
        img.set(1.0).unwrap();

        let mut iter = ImageIterMut::new(&mut img, vec![4, 4]);
        while let Some(Ok(mut chunk)) = iter.next_chunk() {
            chunk.data.mapv_inplace(|v| v * 2.0);
            iter.flush_chunk(&chunk).unwrap();
        }
        assert_eq!(img.get_at(&[0, 0]).unwrap(), 2.0);
    }

    #[test]
    fn all_pixel_types() {
        // f32
        let mut img = TempImage::<f32>::new(vec![2], cs()).unwrap();
        img.set(1.5).unwrap();
        assert_eq!(img.get_at(&[0]).unwrap(), 1.5f32);

        // f64
        let mut img = TempImage::<f64>::new(vec![2], cs()).unwrap();
        img.set(2.5).unwrap();
        assert_eq!(img.get_at(&[0]).unwrap(), 2.5f64);

        // Complex32
        let c32 = Complex32::new(1.0, -2.0);
        let mut img = TempImage::<Complex32>::new(vec![2], cs()).unwrap();
        img.set(c32).unwrap();
        assert_eq!(img.get_at(&[0]).unwrap(), c32);

        // Complex64
        let c64 = Complex64::new(3.0, -4.0);
        let mut img = TempImage::<Complex64>::new(vec![2], cs()).unwrap();
        img.set(c64).unwrap();
        assert_eq!(img.get_at(&[0]).unwrap(), c64);
    }

    #[test]
    fn lattice_trait_properties() {
        let img = TempImage::<f32>::new(vec![4, 4], cs()).unwrap();
        assert!(!<TempImage<f32> as Lattice<f32>>::is_persistent(&img));
        assert!(<TempImage<f32> as Lattice<f32>>::is_writable(&img));
    }

    #[test]
    fn temp_close_reopen() {
        // Memory-backed: temp_close is a no-op.
        let mut img = TempImage::<f32>::new(vec![4, 4], cs()).unwrap();
        img.set(5.0).unwrap();
        img.temp_close().unwrap();
        assert_eq!(img.get_at(&[0, 0]).unwrap(), 5.0);

        // Paged-backed: temp_close releases, reopen restores.
        let mut img = TempImage::<f32>::with_threshold(vec![4, 4], cs(), Some(1)).unwrap();
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
        let img = TempImage::<f32>::new(vec![3, 3], cs()).unwrap();
        let debug = format!("{img:?}");
        assert!(debug.contains("TempImage"));
        assert!(debug.contains("shape"));
    }

    #[test]
    fn mask_shape_validation() {
        let mut img = TempImage::<f32>::new(vec![3, 3], cs()).unwrap();
        let wrong = ArrayD::from_elem(IxDyn(&[2, 2]), true);
        assert!(img.put_mask("bad", &wrong).is_err());
    }

    #[test]
    fn image_interface_trait() {
        let mut img = TempImage::<f32>::new(vec![2, 2], cs()).unwrap();
        img.set(1.0).unwrap();
        img.set_units("Jy").unwrap();

        // Use through ImageInterface trait object.
        let iface: &dyn ImageInterface<f32> = &img;
        assert_eq!(iface.units(), "Jy");
        assert!(iface.name().is_none());
        assert_eq!(iface.shape(), &[2, 2]);
    }
}
