// SPDX-License-Identifier: LGPL-3.0-or-later
//! Image subviews corresponding to C++ `SubImage<T>`.

use std::any::Any;
use std::path::Path;

use casacore_coordinates::CoordinateSystem;
use casacore_lattices::{Lattice, LatticeError, LatticeMut};
use casacore_types::ArrayD;
use ndarray::{Dimension, IxDyn};

use crate::error::ImageError;
use crate::image::{ImageInterface, ImagePixel};

fn validate_subimage_args(
    parent_shape: &[usize],
    start: &[usize],
    shape: &[usize],
    stride: &[usize],
) -> Result<(), ImageError> {
    let ndim = parent_shape.len();
    if start.len() != ndim || shape.len() != ndim || stride.len() != ndim {
        return Err(ImageError::ShapeMismatch {
            expected: parent_shape.to_vec(),
            got: shape.to_vec(),
        });
    }
    for axis in 0..ndim {
        let stop = start[axis]
            .checked_add(shape[axis].saturating_sub(1).saturating_mul(stride[axis]))
            .ok_or_else(|| ImageError::InvalidMetadata("subimage bounds overflow".to_string()))?;
        if shape[axis] > 0 && stop >= parent_shape[axis] {
            return Err(ImageError::ShapeMismatch {
                expected: parent_shape.to_vec(),
                got: shape.to_vec(),
            });
        }
    }
    Ok(())
}

fn map_subimage_position(
    sub_shape: &[usize],
    outer_start: &[usize],
    outer_stride: &[usize],
    position: &[usize],
) -> Result<Vec<usize>, LatticeError> {
    if position.len() != sub_shape.len() {
        return Err(LatticeError::NdimMismatch {
            expected: sub_shape.len(),
            got: position.len(),
        });
    }
    Ok(position
        .iter()
        .zip(outer_start.iter())
        .zip(outer_stride.iter())
        .map(|((&p, &s), &st)| s + p * st)
        .collect())
}

fn map_subimage_slice(
    sub_shape: &[usize],
    outer_start: &[usize],
    outer_stride: &[usize],
    start: &[usize],
    shape: &[usize],
    stride: &[usize],
) -> Result<(Vec<usize>, Vec<usize>), LatticeError> {
    if start.len() != sub_shape.len()
        || shape.len() != sub_shape.len()
        || stride.len() != sub_shape.len()
    {
        return Err(LatticeError::NdimMismatch {
            expected: sub_shape.len(),
            got: start.len().max(shape.len()).max(stride.len()),
        });
    }
    let parent_start: Vec<usize> = start
        .iter()
        .zip(outer_start.iter())
        .zip(outer_stride.iter())
        .map(|((&p, &s), &st)| s + p * st)
        .collect();
    let parent_stride: Vec<usize> = stride
        .iter()
        .zip(outer_stride.iter())
        .map(|(&inner, &outer)| inner * outer)
        .collect();
    Ok((parent_start, parent_stride))
}

/// Read-only image subview over a parent image or image-like lattice.
pub struct SubImage<'a, T: ImagePixel, I: ImageInterface<T>> {
    parent: &'a I,
    start: Vec<usize>,
    shape: Vec<usize>,
    stride: Vec<usize>,
    coords: CoordinateSystem,
    _pixel: std::marker::PhantomData<T>,
}

impl<'a, T: ImagePixel, I: ImageInterface<T>> SubImage<'a, T, I> {
    /// Creates a unit-stride subimage.
    pub fn new(parent: &'a I, start: Vec<usize>, shape: Vec<usize>) -> Result<Self, ImageError> {
        let stride = vec![1; parent.ndim()];
        Self::with_stride(parent, start, shape, stride)
    }

    /// Creates a strided subimage.
    pub fn with_stride(
        parent: &'a I,
        start: Vec<usize>,
        shape: Vec<usize>,
        stride: Vec<usize>,
    ) -> Result<Self, ImageError> {
        validate_subimage_args(parent.shape(), &start, &shape, &stride)?;
        Ok(Self {
            parent,
            start,
            shape,
            stride,
            coords: parent.coordinates().clone(),
            _pixel: std::marker::PhantomData,
        })
    }

    /// Returns the subimage origin in parent-pixel coordinates.
    pub fn start(&self) -> &[usize] {
        &self.start
    }

    /// Returns the subimage stride.
    pub fn stride(&self) -> &[usize] {
        &self.stride
    }
}

impl<'a, T: ImagePixel, I: ImageInterface<T>> Lattice<T> for SubImage<'a, T, I> {
    fn shape(&self) -> &[usize] {
        &self.shape
    }

    fn is_persistent(&self) -> bool {
        self.parent.is_persistent()
    }

    fn is_paged(&self) -> bool {
        self.parent.is_paged()
    }

    fn is_writable(&self) -> bool {
        false
    }

    fn get_at(&self, position: &[usize]) -> Result<T, LatticeError> {
        let parent_pos = map_subimage_position(&self.shape, &self.start, &self.stride, position)?;
        self.parent.get_at(&parent_pos)
    }

    fn get_slice(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<T>, LatticeError> {
        let (parent_start, parent_stride) =
            map_subimage_slice(&self.shape, &self.start, &self.stride, start, shape, stride)?;
        self.parent.get_slice(&parent_start, shape, &parent_stride)
    }
}

impl<'a, T: ImagePixel, I: ImageInterface<T>> ImageInterface<T> for SubImage<'a, T, I> {
    fn as_any(&self) -> Option<&dyn Any> {
        None
    }

    fn coordinates(&self) -> &CoordinateSystem {
        &self.coords
    }

    fn units(&self) -> &str {
        self.parent.units()
    }

    fn misc_info(&self) -> casacore_types::RecordValue {
        self.parent.misc_info()
    }

    fn image_info(&self) -> Result<crate::image_info::ImageInfo, ImageError> {
        self.parent.image_info()
    }

    fn name(&self) -> Option<&Path> {
        self.parent.name()
    }
}

/// Mutable subimage view over a writable parent image.
pub struct SubImageMut<'a, T: ImagePixel, I: ImageInterface<T> + LatticeMut<T>> {
    parent: &'a mut I,
    start: Vec<usize>,
    shape: Vec<usize>,
    stride: Vec<usize>,
    coords: CoordinateSystem,
    _pixel: std::marker::PhantomData<T>,
}

impl<'a, T: ImagePixel, I: ImageInterface<T> + LatticeMut<T>> SubImageMut<'a, T, I> {
    /// Creates a mutable unit-stride subimage.
    pub fn new(
        parent: &'a mut I,
        start: Vec<usize>,
        shape: Vec<usize>,
    ) -> Result<Self, ImageError> {
        let stride = vec![1; parent.ndim()];
        Self::with_stride(parent, start, shape, stride)
    }

    /// Creates a mutable strided subimage.
    pub fn with_stride(
        parent: &'a mut I,
        start: Vec<usize>,
        shape: Vec<usize>,
        stride: Vec<usize>,
    ) -> Result<Self, ImageError> {
        validate_subimage_args(parent.shape(), &start, &shape, &stride)?;
        let coords = parent.coordinates().clone();
        Ok(Self {
            parent,
            start,
            shape,
            stride,
            coords,
            _pixel: std::marker::PhantomData,
        })
    }
}

impl<'a, T: ImagePixel, I: ImageInterface<T> + LatticeMut<T>> Lattice<T> for SubImageMut<'a, T, I> {
    fn shape(&self) -> &[usize] {
        &self.shape
    }

    fn is_persistent(&self) -> bool {
        self.parent.is_persistent()
    }

    fn is_paged(&self) -> bool {
        self.parent.is_paged()
    }

    fn get_at(&self, position: &[usize]) -> Result<T, LatticeError> {
        let parent_pos = map_subimage_position(&self.shape, &self.start, &self.stride, position)?;
        self.parent.get_at(&parent_pos)
    }

    fn get_slice(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<T>, LatticeError> {
        let (parent_start, parent_stride) =
            map_subimage_slice(&self.shape, &self.start, &self.stride, start, shape, stride)?;
        self.parent.get_slice(&parent_start, shape, &parent_stride)
    }
}

impl<'a, T: ImagePixel, I: ImageInterface<T> + LatticeMut<T>> LatticeMut<T>
    for SubImageMut<'a, T, I>
{
    fn put_at(&mut self, value: T, position: &[usize]) -> Result<(), LatticeError> {
        let parent_pos = map_subimage_position(&self.shape, &self.start, &self.stride, position)?;
        self.parent.put_at(value, &parent_pos)
    }

    fn put_slice(&mut self, data: &ArrayD<T>, start: &[usize]) -> Result<(), LatticeError> {
        if start.len() != self.shape.len() {
            return Err(LatticeError::NdimMismatch {
                expected: self.shape.len(),
                got: start.len(),
            });
        }
        if self.stride.iter().all(|&step| step == 1) {
            let parent_start: Vec<usize> = start
                .iter()
                .zip(self.start.iter())
                .map(|(&inner, &outer)| inner + outer)
                .collect();
            return self.parent.put_slice(data, &parent_start);
        }
        for (idx, value) in data.indexed_iter() {
            let parent_pos: Vec<usize> = idx
                .slice()
                .iter()
                .zip(start.iter())
                .zip(self.start.iter())
                .zip(self.stride.iter())
                .map(|(((&i, &inner_start), &outer_start), &step)| {
                    outer_start + (inner_start + i) * step
                })
                .collect();
            self.parent.put_at(*value, &parent_pos)?;
        }
        Ok(())
    }

    fn set(&mut self, value: T) -> Result<(), LatticeError> {
        let fill = ArrayD::from_elem(IxDyn(&self.shape), value);
        self.put_slice(&fill, &vec![0; self.shape.len()])
    }
}

impl<'a, T: ImagePixel, I: ImageInterface<T> + LatticeMut<T>> ImageInterface<T>
    for SubImageMut<'a, T, I>
{
    fn as_any(&self) -> Option<&dyn Any> {
        None
    }

    fn coordinates(&self) -> &CoordinateSystem {
        &self.coords
    }

    fn units(&self) -> &str {
        self.parent.units()
    }

    fn misc_info(&self) -> casacore_types::RecordValue {
        self.parent.misc_info()
    }

    fn image_info(&self) -> Result<crate::image_info::ImageInfo, ImageError> {
        self.parent.image_info()
    }

    fn name(&self) -> Option<&Path> {
        self.parent.name()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::temp_image::TempImage;
    use casacore_coordinates::CoordinateSystem;
    use casacore_types::{RecordField, RecordValue, ScalarValue, Value};

    #[test]
    fn readonly_subimage_reads_parent_region() {
        let mut image = TempImage::<f32>::new(vec![4, 4], CoordinateSystem::new()).unwrap();
        image.put_at(7.0, &[2, 3]).unwrap();
        let sub = SubImage::new(&image, vec![1, 2], vec![3, 2]).unwrap();
        assert_eq!(sub.get_at(&[1, 1]).unwrap(), 7.0);
    }

    #[test]
    fn mutable_subimage_writes_parent_region() {
        let mut image = TempImage::<f32>::new(vec![4, 4], CoordinateSystem::new()).unwrap();
        let mut sub = SubImageMut::new(&mut image, vec![1, 1], vec![2, 2]).unwrap();
        sub.put_at(3.5, &[1, 1]).unwrap();
        drop(sub);
        assert_eq!(image.get_at(&[2, 2]).unwrap(), 3.5);
    }

    #[test]
    fn strided_subimage_reads_every_other_pixel() {
        let mut image = TempImage::<f32>::new(vec![5, 5], CoordinateSystem::new()).unwrap();
        image.put_at(11.0, &[4, 4]).unwrap();
        let sub = SubImage::with_stride(&image, vec![0, 0], vec![3, 3], vec![2, 2]).unwrap();
        assert_eq!(sub.get_at(&[2, 2]).unwrap(), 11.0);
    }

    #[test]
    fn subimage_validation_rejects_mismatched_dimensions_and_bounds() {
        let image = TempImage::<f32>::new(vec![4, 4], CoordinateSystem::new()).unwrap();
        assert!(matches!(
            SubImage::with_stride(&image, vec![0], vec![2, 2], vec![1, 1]),
            Err(ImageError::ShapeMismatch { .. })
        ));
        assert!(matches!(
            SubImage::with_stride(&image, vec![3, 3], vec![2, 2], vec![1, 1]),
            Err(ImageError::ShapeMismatch { .. })
        ));
        assert!(matches!(
            SubImage::with_stride(&image, vec![0, 0], vec![3, 3], vec![2, 2]),
            Err(ImageError::ShapeMismatch { .. })
        ));
    }

    #[test]
    fn readonly_subimage_get_slice_and_metadata_delegate_to_parent() {
        let mut image = TempImage::<f32>::new(vec![4, 4], CoordinateSystem::new()).unwrap();
        image.set_units("Jy/beam").unwrap();
        image
            .set_misc_info(RecordValue::new(vec![RecordField::new(
                "observer",
                Value::Scalar(ScalarValue::String("demo".to_string())),
            )]))
            .unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(IxDyn(&[4, 4]), (0..16).map(|v| v as f32).collect())
                    .unwrap(),
                &[0, 0],
            )
            .unwrap();

        let sub = SubImage::with_stride(&image, vec![0, 0], vec![2, 2], vec![2, 2]).unwrap();
        let got = sub.get_slice(&[0, 0], &[2, 2], &[1, 1]).unwrap();

        assert_eq!(
            got,
            ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![0.0, 2.0, 8.0, 10.0]).unwrap()
        );
        assert_eq!(sub.start(), &[0, 0]);
        assert_eq!(sub.stride(), &[2, 2]);
        assert_eq!(sub.units(), "Jy/beam");
        assert_eq!(sub.misc_info(), image.misc_info());
        assert_eq!(sub.name(), None);
        assert!(!sub.is_writable());
    }

    #[test]
    fn mutable_subimage_put_slice_and_set_cover_parent_mapping() {
        let mut image = TempImage::<f32>::new(vec![5, 5], CoordinateSystem::new()).unwrap();
        let mut sub =
            SubImageMut::with_stride(&mut image, vec![1, 1], vec![2, 2], vec![2, 2]).unwrap();
        sub.put_slice(
            &ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1.0, 2.0, 3.0, 4.0]).unwrap(),
            &[0, 0],
        )
        .unwrap();
        assert_eq!(
            sub.get_slice(&[0, 0], &[2, 2], &[1, 1]).unwrap(),
            ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1.0, 2.0, 3.0, 4.0]).unwrap()
        );
        sub.set(9.0).unwrap();
        drop(sub);

        assert_eq!(image.get_at(&[1, 1]).unwrap(), 9.0);
        assert_eq!(image.get_at(&[3, 1]).unwrap(), 9.0);
        assert_eq!(image.get_at(&[1, 3]).unwrap(), 9.0);
        assert_eq!(image.get_at(&[3, 3]).unwrap(), 9.0);
    }

    #[test]
    fn mutable_subimage_reports_dimension_errors() {
        let mut image = TempImage::<f32>::new(vec![4, 4], CoordinateSystem::new()).unwrap();
        let mut sub = SubImageMut::new(&mut image, vec![0, 0], vec![2, 2]).unwrap();
        assert!(matches!(
            sub.get_at(&[0, 0, 0]),
            Err(LatticeError::NdimMismatch { .. })
        ));
        assert!(matches!(
            sub.get_slice(&[0, 0, 0], &[1, 1], &[1, 1]),
            Err(LatticeError::NdimMismatch { .. })
        ));
        assert!(matches!(
            sub.put_at(1.0, &[0, 0, 0]),
            Err(LatticeError::NdimMismatch { .. })
        ));
        assert!(matches!(
            sub.put_slice(&ArrayD::from_elem(IxDyn(&[1, 1]), 1.0), &[0, 0, 0]),
            Err(LatticeError::NdimMismatch { .. })
        ));
    }
}
