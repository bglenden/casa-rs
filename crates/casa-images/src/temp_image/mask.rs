// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_lattices::{
    Lattice, LatticeMut, ScratchSpace, TempLattice, TempStoragePolicy, TraversalCursorIter,
    TraversalSpec,
};
use casa_types::ArrayD;
use ndarray::IxDyn;

use crate::image::{assign_mask_slice, slice_mask_array};
use crate::{ImageError, ImagePixel, MutableImageInterface, PagedImage};

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
