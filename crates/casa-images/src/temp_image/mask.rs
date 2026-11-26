// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_core::ArrayD;
use casa_lattices::{
    Lattice, LatticeMut, ScratchSpace, TempLattice, TempStoragePolicy, TraversalCursorIter,
    TraversalSpec,
};
use ndarray::IxDyn;

use crate::image::{assign_mask_slice, slice_mask_array, validate_mask_slice_write};
use crate::{ImageError, ImageInterface, ImagePixel, MutableImageInterface, PagedImage};

use super::TempImage;

pub(super) enum TempMask {
    Dense(ArrayD<bool>),
    Paged(TempLattice<bool>),
}

impl TempMask {
    pub(super) fn filled(
        shape: &[usize],
        value: bool,
        scratch: Option<&ScratchSpace>,
    ) -> Result<Self, ImageError> {
        let Some(scratch) = scratch else {
            return Ok(Self::Dense(ArrayD::from_elem(IxDyn(shape), value)));
        };
        let mut mask = TempLattice::new(
            shape.to_vec(),
            TempStoragePolicy::Paged {
                scratch: scratch.clone(),
            },
        )?;
        mask.set(value)?;
        Ok(Self::Paged(mask))
    }

    pub(super) fn read(&self) -> Result<ArrayD<bool>, ImageError> {
        match self {
            Self::Dense(data) => Ok(data.clone()),
            Self::Paged(mask) => Ok(mask.get()?),
        }
    }

    pub(super) fn read_slice(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<bool>, ImageError> {
        match self {
            Self::Dense(data) => slice_mask_array(data, start, shape, stride),
            Self::Paged(mask) => Ok(mask.get_slice(start, shape, stride)?),
        }
    }

    pub(super) fn write_slice(
        &mut self,
        data: &ArrayD<bool>,
        start: &[usize],
    ) -> Result<(), ImageError> {
        match self {
            Self::Dense(mask) => assign_mask_slice(mask, data, start),
            Self::Paged(mask) => Ok(mask.put_slice(data, start)?),
        }
    }

    pub(super) fn save<T: ImagePixel>(
        &self,
        image: &mut PagedImage<T>,
        name: &str,
    ) -> Result<(), ImageError> {
        let mask = match self {
            Self::Dense(data) => return image.put_mask(name, data),
            Self::Paged(mask) => mask,
        };
        image.create_filled_mask(name, false)?;
        let stride = vec![1; mask.ndim()];
        for cursor in TraversalCursorIter::new(
            mask.shape().to_vec(),
            mask.nice_cursor_shape(),
            TraversalSpec::tiles(),
        ) {
            let cursor = cursor?;
            let data = mask.get_slice(&cursor.position, &cursor.shape, &stride)?;
            image.put_mask_slice(name, &data, &cursor.position)?;
        }
        Ok(())
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

    /// Set the active pixel mask without registering a named mask.
    ///
    /// # Errors
    /// Returns an error for read-only storage or a mask of the wrong shape.
    pub fn set_anonymous_mask(&mut self, mask: ArrayD<bool>) -> Result<(), ImageError> {
        self.require_writable_pixels()?;
        if mask.shape() != self.shape() {
            return Err(ImageError::ShapeMismatch {
                expected: self.shape().to_vec(),
                got: mask.shape().to_vec(),
            });
        }
        self.default_mask = None;
        self.anonymous_mask = Some(TempMask::Dense(mask));
        Ok(())
    }

    /// Returns a named mask as a full-image boolean array.
    pub fn get_named_mask(&self, name: &str) -> Result<ArrayD<bool>, ImageError> {
        if let Some(source) = self.native_component_source() {
            return source.get_named_mask(name);
        }
        self.find_named_mask(name)?.read()
    }

    pub(super) fn find_named_mask(&self, name: &str) -> Result<&TempMask, ImageError> {
        self.masks
            .iter()
            .find(|(candidate, _)| candidate == name)
            .map(|(_, mask)| mask)
            .ok_or_else(|| ImageError::MaskNotFound(name.to_string()))
    }

    /// Replaces a named mask.
    pub fn put_mask(&mut self, name: &str, data: &ArrayD<bool>) -> Result<(), ImageError> {
        if let Some(table) = self.native_component_table() {
            return table.define_mask(Some(name), true, false, self.shape(), data);
        }
        self.require_writable_pixels()?;
        if data.shape() != self.shape() {
            return Err(ImageError::ShapeMismatch {
                expected: self.shape().to_vec(),
                got: data.shape().to_vec(),
            });
        }
        if let Some((_, mask)) = self
            .masks
            .iter_mut()
            .find(|(candidate, _)| candidate == name)
        {
            match mask {
                TempMask::Dense(mask) => *mask = data.clone(),
                TempMask::Paged(mask) => mask.put_slice(data, &vec![0; data.ndim()])?,
            }
        } else {
            self.masks
                .push((name.to_owned(), TempMask::Dense(data.clone())));
        }
        Ok(())
    }

    /// Define a native component-image mask under the table's write lock.
    /// A missing name selects the first unused generated name. Returns
    /// `false` if this attachment uses another kind of image.
    ///
    /// # Errors
    /// Returns an error for invalid data, conflicting names, or failed I/O.
    pub fn define_component_mask(
        &mut self,
        name: Option<&str>,
        overwrite: bool,
        set_default: bool,
        data: &ArrayD<bool>,
    ) -> Result<bool, ImageError> {
        let Some(table) = self.native_component_table() else {
            return Ok(false);
        };
        table.define_mask(name, overwrite, set_default, self.shape(), data)?;
        Ok(true)
    }

    /// Copies a rectangular block into a named mask, allocating an all-false
    /// mask if this is the first write for that name.
    ///
    /// # Errors
    /// Returns an error for read-only storage or a block outside the image shape.
    pub fn put_mask_slice(
        &mut self,
        name: &str,
        data: &ArrayD<bool>,
        start: &[usize],
    ) -> Result<(), ImageError> {
        if let Some(source) = self.concat_source()
            && name.is_empty()
        {
            return source.write_mask_slice(data, start);
        }
        if self.native_component_table().is_some() {
            return Err(ImageError::CasaSelection(
                "MaskedLattice::pixelMask - no pixelmask available".into(),
            ));
        }
        self.require_writable_pixels()?;
        if name.is_empty()
            && let Some(mask) = self.anonymous_mask.as_mut()
        {
            return mask.write_slice(data, start);
        }
        if let Some((_, mask)) = self
            .masks
            .iter_mut()
            .find(|(candidate, _)| candidate == name)
        {
            return mask.write_slice(data, start);
        }
        validate_mask_slice_write(self.shape(), data.shape(), start)?;
        let mut mask = ArrayD::from_elem(IxDyn(self.shape()), false);
        assign_mask_slice(&mut mask, data, start)?;
        self.masks.push((name.to_owned(), TempMask::Dense(mask)));
        Ok(())
    }

    /// Removes a named mask.
    pub fn remove_mask(&mut self, name: &str) -> Result<(), ImageError> {
        if let Some(table) = self.native_component_table() {
            return table.remove_mask(name);
        }
        let index = self
            .masks
            .iter()
            .position(|(candidate, _)| candidate == name)
            .ok_or_else(|| ImageError::MaskNotFound(name.into()))?;
        self.masks.remove(index);
        if self.default_mask.as_deref() == Some(name) {
            self.default_mask = None;
        }
        Ok(())
    }

    /// Renames a mask in its original order and keeps it selected if default.
    ///
    /// # Errors
    /// Returns an error if the source is absent or the destination exists.
    pub fn rename_mask(&mut self, from: &str, to: &str) -> Result<(), ImageError> {
        if let Some(table) = self.native_component_table() {
            return table.rename_mask(from, to);
        }
        if self.masks.iter().any(|(name, _)| name == to) {
            return Err(ImageError::InvalidMetadata(format!(
                "Mask {to} already exists"
            )));
        }
        let (name, _) = self
            .masks
            .iter_mut()
            .find(|(name, _)| name == from)
            .ok_or_else(|| ImageError::MaskNotFound(from.into()))?;
        *name = to.into();
        if self.default_mask.as_deref() == Some(from) {
            self.default_mask = Some(to.into());
        }
        Ok(())
    }

    /// Sets the default mask name, validating that it exists.
    pub fn set_default_mask(&mut self, name: &str) -> Result<(), ImageError> {
        if let Some(table) = self.native_component_table() {
            return table.set_default_mask(Some(name), self.shape());
        }
        if !self.masks.iter().any(|(candidate, _)| candidate == name) {
            return Err(ImageError::MaskNotFound(name.to_string()));
        }
        self.default_mask = Some(name.to_string());
        self.anonymous_mask = None;
        Ok(())
    }

    /// Unsets the default mask.
    pub fn unset_default_mask(&mut self) -> Result<(), ImageError> {
        if let Some(table) = self.native_component_table() {
            return table.set_default_mask(None, self.shape());
        }
        self.default_mask = None;
        self.anonymous_mask = None;
        Ok(())
    }

    /// Returns `true` if a default pixel mask is configured.
    pub fn has_pixel_mask(&self) -> bool {
        <Self as ImageInterface<T>>::has_pixel_mask(self)
    }

    /// Returns the default mask name if present.
    pub fn default_mask_name(&self) -> Option<String> {
        self.native_component_source().map_or_else(
            || self.default_mask.clone(),
            |source| source.default_mask_name(),
        )
    }

    /// Returns the names of all stored masks.
    pub fn mask_names(&self) -> Vec<String> {
        self.native_component_source().map_or_else(
            || self.masks.iter().map(|(name, _)| name.clone()).collect(),
            |source| source.mask_names(),
        )
    }
}
