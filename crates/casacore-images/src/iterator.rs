// SPDX-License-Identifier: LGPL-3.0-or-later
//! Chunk iterators for typed images and image-like views.
//!
//! These iterators are compatibility wrappers over the lattice traversal API.
//! They operate on any [`Lattice`] whose element type satisfies
//! [`ImagePixel`].

use ndarray::ArrayD;

use casacore_lattices::{Lattice, LatticeMut, TraversalCursorIter, TraversalIter, TraversalSpec};

use crate::error::ImageError;
use crate::image::ImagePixel;

/// A rectangular chunk of image data together with its origin.
#[derive(Debug, Clone)]
pub struct ImageChunk<T: ImagePixel> {
    /// The pixel data for this chunk.
    pub data: ArrayD<T>,
    /// The starting pixel position within the parent image.
    pub origin: Vec<usize>,
    /// The shape of this chunk, clipped at image boundaries.
    pub shape: Vec<usize>,
}

/// Read-only chunk iterator over any typed image-like lattice.
pub struct ImageIter<'a, T: ImagePixel, I: Lattice<T>> {
    inner: TraversalIter<'a, T, I>,
}

impl<'a, T: ImagePixel, I: Lattice<T>> ImageIter<'a, T, I> {
    /// Creates a new read-only chunk iterator.
    pub fn new(image: &'a I, cursor_shape: Vec<usize>) -> Self {
        Self {
            inner: TraversalIter::new(image, TraversalSpec::chunks(cursor_shape)),
        }
    }
}

impl<'a, T: ImagePixel, I: Lattice<T>> Iterator for ImageIter<'a, T, I> {
    type Item = Result<ImageChunk<T>, ImageError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|chunk| {
            chunk
                .map(|chunk| ImageChunk {
                    data: chunk.data,
                    origin: chunk.cursor.position,
                    shape: chunk.cursor.shape,
                })
                .map_err(ImageError::from)
        })
    }
}

/// Mutable chunk iterator over any writable typed image-like lattice.
pub struct ImageIterMut<'a, T: ImagePixel, I: LatticeMut<T>> {
    image: &'a mut I,
    cursors: TraversalCursorIter,
    _pixel: std::marker::PhantomData<T>,
}

impl<'a, T: ImagePixel, I: LatticeMut<T>> ImageIterMut<'a, T, I> {
    /// Creates a new mutable chunk iterator.
    pub fn new(image: &'a mut I, cursor_shape: Vec<usize>) -> Self {
        let cursors = TraversalCursorIter::new(
            image.shape().to_vec(),
            image.nice_cursor_shape(),
            TraversalSpec::chunks(cursor_shape),
        );
        Self {
            image,
            cursors,
            _pixel: std::marker::PhantomData,
        }
    }

    /// Reads the next chunk and advances the iterator.
    pub fn next_chunk(&mut self) -> Option<Result<ImageChunk<T>, ImageError>> {
        self.cursors.next().map(|cursor| {
            cursor.map_err(ImageError::from).and_then(|cursor| {
                let stride = vec![1; cursor.position.len()];
                self.image
                    .get_slice(&cursor.position, &cursor.shape, &stride)
                    .map(|data| ImageChunk {
                        data,
                        origin: cursor.position,
                        shape: cursor.shape,
                    })
                    .map_err(ImageError::from)
            })
        })
    }

    /// Writes a modified chunk back to the parent image.
    pub fn flush_chunk(&mut self, chunk: &ImageChunk<T>) -> Result<(), ImageError> {
        self.image
            .put_slice(&chunk.data, &chunk.origin)
            .map_err(ImageError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::temp_image::TempImage;
    use casacore_coordinates::CoordinateSystem;
    use casacore_types::Complex32;
    use ndarray::IxDyn;

    fn make_image<T: ImagePixel>(shape: Vec<usize>) -> TempImage<T> {
        TempImage::new(shape, CoordinateSystem::new()).unwrap()
    }

    #[test]
    fn full_scan_sum() {
        let mut img = make_image::<f32>(vec![8, 6]);
        img.set(1.0).unwrap();

        let mut total = 0.0f32;
        for chunk in ImageIter::new(&img, vec![4, 3]) {
            total += chunk.unwrap().data.sum();
        }
        assert_eq!(total, 48.0);
    }

    #[test]
    fn chunk_count_exact_division() {
        let img = make_image::<f32>(vec![8, 6]);
        assert_eq!(ImageIter::new(&img, vec![4, 3]).count(), 4);
    }

    #[test]
    fn chunk_count_non_divisible() {
        let img = make_image::<f32>(vec![7, 5]);
        assert_eq!(ImageIter::new(&img, vec![4, 3]).count(), 4);
    }

    #[test]
    fn hangover_shape() {
        let img = make_image::<f32>(vec![7, 5]);
        let chunks: Vec<_> = ImageIter::new(&img, vec![4, 3])
            .map(|c| c.unwrap())
            .collect();

        assert_eq!(chunks[0].shape, vec![4, 3]);
        assert_eq!(chunks[0].origin, vec![0, 0]);
        assert_eq!(chunks[3].shape, vec![3, 2]);
        assert_eq!(chunks[3].origin, vec![4, 3]);
    }

    #[test]
    fn covers_all_elements() {
        let shape = vec![7, 5];
        let mut img = make_image::<f32>(shape.clone());
        let data = ArrayD::from_shape_fn(IxDyn(&shape), |idx| (idx[0] * 5 + idx[1]) as f32);
        img.put_slice(&data, &[0, 0]).unwrap();

        let mut sum = 0.0f32;
        let mut pixel_count = 0usize;
        for chunk in ImageIter::new(&img, vec![3, 2]) {
            let c = chunk.unwrap();
            sum += c.data.sum();
            pixel_count += c.data.len();
        }
        assert_eq!(pixel_count, 35);
        assert_eq!(sum, data.sum());
    }

    #[test]
    fn mutable_iteration() {
        let mut img = make_image::<f32>(vec![4, 4]);
        img.set(1.0).unwrap();

        let mut iter = ImageIterMut::new(&mut img, vec![2, 2]);
        while let Some(Ok(mut chunk)) = iter.next_chunk() {
            chunk.data.mapv_inplace(|v| v * 2.0);
            iter.flush_chunk(&chunk).unwrap();
        }

        let arr = img.get().unwrap();
        assert!(arr.iter().all(|&v| v == 2.0));
    }

    #[test]
    fn one_d_image() {
        let mut img = make_image::<f32>(vec![10]);
        img.set(3.0).unwrap();
        let mut total = 0.0f32;
        for chunk in ImageIter::new(&img, vec![4]) {
            total += chunk.unwrap().data.sum();
        }
        assert_eq!(total, 30.0);
    }

    #[test]
    fn complex_iteration_preserves_values() {
        let mut img = make_image::<Complex32>(vec![2, 2]);
        img.put_at(Complex32::new(1.0, -1.0), &[0, 0]).unwrap();
        let first = ImageIter::new(&img, vec![1, 2]).next().unwrap().unwrap();
        assert_eq!(first.data[[0, 0]], Complex32::new(1.0, -1.0));
    }

    #[test]
    fn temp_image_iteration() {
        let mut img = TempImage::<f32>::new(vec![2, 2], CoordinateSystem::new()).unwrap();
        img.set(1.0).unwrap();
        assert_eq!(ImageIter::new(&img, vec![1, 1]).count(), 4);
    }
}
